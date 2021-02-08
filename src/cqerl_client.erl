-module(cqerl_client).
-behaviour(gen_statem).
-define(SERVER, ?MODULE).

-include("cqerl_protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3, start_link/4, 
         run_query/2, query_async/2, fetch_more/1, fetch_more_async/1,
         prepare_query/2, batch_ready/2, make_key/2]).

%% ------------------------------------------------------------------
%% gen_statem Function Exports
%% ------------------------------------------------------------------

-define(QUERIES_MAX, 128).
-define(QUERIES_HW, 88).
-define(FSM_TIMEOUT, case application:get_env(cqerl, query_timeout) of
    undefined -> 30000;
    {ok, Val} -> Val
end).

-define(IS_IOLIST(L), is_list(L) orelse is_binary(L)).

-export([init/1, terminate/3, callback_mode/0,
         starting/3,
         live/3,
         sleep/3,
         code_change/4]).

-record(client_state, {
    %% Authentication state (only kept during initialization)
    authmod :: atom(),
    authstate :: any(),
    authargs :: list(any()),

    %% Information about the connection
    inet :: any(),
    trans :: atom(),
    socket :: port() | ssl:sslsocket(), % The port() is actually a
                                        % gen_tcp:socket(), but that type isn't
                                        % currently exported (as of 18.2)
    compression_type :: undefined | snappy | lz4,
    keyspace :: atom(),

    %% Operating state
    sleep :: integer(),
    delayed = <<>> :: binary(),     % Fragmented message continuation
    users = [] :: list({pid(), reference()}) | ets:tab(),
    queries = [] :: list({integer(), term()}),
    queued,
    available_slots = [] :: list(integer()),
    waiting_preparation = [],
    key :: {term(), term()}
}).

-record(client_user, {
    ref :: reference() | '_',
    pid :: pid(),
    monitor :: reference() | '_'
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Inet, Opts, OptGetter) ->
    gen_statem:start_link(?MODULE, [Inet, Opts, OptGetter, undefined], []).

start_link(Inet, Opts, OptGetter, Key) ->
    gen_statem:start_link(?MODULE, [Inet, Opts, OptGetter, Key], []).

run_query(Client, Query) when ?IS_IOLIST(Query) ->
    run_query(Client, #cql_query{statement=Query});
run_query(Client, Query=#cql_query{statement=Statement}) when is_list(Statement) ->
    run_query(Client, Query#cql_query{statement=iolist_to_binary(Statement)});
run_query({ClientPid, ClientRef}, Query) ->
    gen_statem:call(ClientPid, {send_query, ClientRef, Query}, ?FSM_TIMEOUT).

query_async(Client, Query) when ?IS_IOLIST(Query) ->
    query_async(Client, #cql_query{statement=Query});
query_async(Client, Query=#cql_query{statement=Statement}) when is_list(Statement) ->
    query_async(Client, Query#cql_query{statement=iolist_to_binary(Statement)});
query_async({ClientPid, ClientRef}, Query) ->
    QueryRef = make_ref(),
    gen_statem:cast(ClientPid, {send_query, {self(), QueryRef}, ClientRef, Query}),
    QueryRef.

fetch_more(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    gen_statem:call(ClientPid, {fetch_more, ClientRef, Continuation}, ?FSM_TIMEOUT).

fetch_more_async(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    QueryRef = make_ref(),
    gen_statem:cast(ClientPid, {fetch_more, {self(), QueryRef}, ClientRef, Continuation}),
    QueryRef.

prepare_query(ClientPid, Query) ->
    % We don't want the cqerl_cache process to crash if our client has gone away,
    % so wrap in a try-catch
    try
        gen_statem:cast(ClientPid, {prepare_query, Query})
    catch
        _:_ -> ok
    end.

batch_ready({ClientPid, Call}, QueryBatch) ->
    gen_statem:cast(ClientPid, {batch_ready, Call, QueryBatch}).

make_key(Node, Opts) ->
    SafeOpts =
    case lists:keytake(auth, 1, Opts) of
        {value, {auth, Auth}, Opts1} -> [{auth_hash, erlang:phash2(Auth)} | Opts1];
        false -> Opts
    end,
    NormalisedOpts = normalise_keyspace(SafeOpts),
    {Node, lists:usort(NormalisedOpts)}.

normalise_keyspace(Opts) ->
    KS = proplists:get_value(keyspace, Opts),
    [{keyspace, normalise_to_atom(KS)} | proplists:delete(keyspace, Opts)].

normalise_to_atom(KS) when is_list(KS) -> list_to_atom(KS);
normalise_to_atom(KS) when is_binary(KS) -> binary_to_atom(KS, latin1);
normalise_to_atom(KS) when is_atom(KS) -> KS.

%% ------------------------------------------------------------------
%% gen_statem Function Definitions
%% ------------------------------------------------------------------

init([Inet, Opts, OptGetter, Key]) ->
    case create_socket(Inet, Opts) of
        {ok, Socket, Transport} ->
            {AuthHandler, AuthArgs} = OptGetter(auth),
            cqerl:put_protocol_version(OptGetter(protocol_version)),
            {ok, OptionsFrame} = cqerl_protocol:options_frame(#cqerl_frame{}),
            State = #client_state{
                socket=Socket, trans=Transport, inet=Inet,
                authmod=AuthHandler, authargs=AuthArgs,
                users=[],
                sleep=infinity,
                keyspace=normalise_to_atom(proplists:get_value(keyspace, Opts)),
                key=Key
            },
            send_to_db(State, OptionsFrame),
            activate_socket(State),
            cqerl_cluster:node_up(Inet),
            {ok, starting, State};

        {error, Reason} ->
            cqerl_cluster:node_down(Inet),
            {stop, {connection_error, Reason}}
    end.



callback_mode() -> state_functions.




starting(cast, _Event, _State) ->
    keep_state_and_data;

starting({call, From}, {new_user, User}, State=#client_state{users=Users}) ->
    {keep_state, State#client_state{users=[User | Users]}, {reply, From, ok}};

starting({call, From}, _Event, State) ->
    {keep_state, State, {reply, From, unexpected_msg}};

starting(info, { tcp_closed, _Socket }, State) ->
    stop_during_startup({error, connection_closed}, State);

starting(info, { ssl_closed, _Socket }, State) ->
    stop_during_startup({error, connection_closed}, State);

starting(info, { Transport, Socket, BinaryMsg }, State = #client_state{ socket=Socket, trans=Transport, delayed=Delayed0 }) ->
    Resp = case cqerl_protocol:response_frame(#cqerl_frame{}, << Delayed0/binary, BinaryMsg/binary >>) of
        %% The frame is incomplete, so we take the accumulated data so far and store it for the next incoming
        %% fragment
        {delay, Delayed} ->
            {next_state, starting, State};

        %% Server tells us what version and compression algorithm it supports
        {ok, #cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, Payload, Delayed} ->
            Compression = choose_compression_type(proplists:lookup('COMPRESSION', Payload)),
            SelectedVersion = choose_cql_version(proplists:lookup('CQL_VERSION', Payload)),
            {ok, StartupFrame} = cqerl_protocol:startup_frame(#cqerl_frame{}, #cqerl_startup_options{compression=Compression,
                                                                                                     cql_version=SelectedVersion}),
            send_to_db(State, StartupFrame),
            {next_state, starting, State#client_state{compression_type=Compression}};

        %% Server tells us all is clear, we can start to throw queries at it
        {ok, #cqerl_frame{opcode=?CQERL_OP_READY}, _, Delayed} ->
            {StateName, FinalState} = maybe_set_keyspace(State),
            {next_state, StateName, FinalState};

        %% Server tells us we need to authenticate
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTHENTICATE}, Body, Delayed} ->
            #client_state{ authmod=AuthMod, authargs=AuthArgs, inet=Inet } = State,
            case AuthMod:auth_init(AuthArgs, Body, Inet) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);

                {reply, Reply, AuthState} ->
                    {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
                    send_to_db(State, AuthFrame),
                    {next_state, starting, State#client_state{ authstate=AuthState }}
            end;

        %% Server tells us we need to give another piece of data
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTH_CHALLENGE}, Body, Delayed} ->
            #client_state{ authmod=AuthMod, authstate=AuthState } = State,
            case AuthMod:auth_handle_challenge(Body, AuthState) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);
                {reply, Reply, AuthState} ->
                    {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
                    send_to_db(State, AuthFrame),
                    {next_state, starting, State#client_state{ authstate=AuthState }}
            end;

        %% Server tells us something screwed up while authenticating
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR}, {16#0100, AuthErrorDescription, _}, Delayed} ->
            #client_state{ authmod=AuthMod, authstate=AuthState } = State,
            AuthMod:auth_handle_error(AuthErrorDescription, AuthState),
            stop_during_startup({auth_server_refused, AuthErrorDescription}, State);

        %% Server tells us something an error occured
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR}, {ErrorCode, ErrorMessage, _}, Delayed} ->
            stop_during_startup({server_error, ErrorCode, ErrorMessage}, State);

        %% Server tells us the authentication went well, we can start shooting queries
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTH_SUCCESS}, Body, Delayed} ->
            #client_state{ authmod=AuthMod, authstate=AuthState} = State,
            case AuthMod:auth_handle_success(Body, AuthState) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);

                ok ->
                    {StateName, FinalState} = maybe_set_keyspace(State),
                    {next_state, StateName, FinalState }
            end;

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT}, {set_keyspace, _KeySpaceName}, Delayed} ->
            {next_state, live, switch_to_live_state(State) }
    end,
    {next_state, NextState, Data} = Resp,
    activate_socket(Data),
    append_delayed_segment({next_state, NextState, Data}, Delayed).




live(cast, {prepare_query, Query}, State=#client_state{available_slots=[], queued=Queue0}) ->
    {keep_state, State#client_state{queued=queue:in_r({prepare, Query}, Queue0)}};

live(cast, {prepare_query, Query}, State) ->
    {keep_state, process_outgoing_query(prepare, Query, State)};

live(cast, {batch_ready, Call, QueryBatch}, State=#client_state{available_slots=[], queued=Queue0}) ->
    {keep_state, State#client_state{queued=queue:in({Call, QueryBatch}, Queue0)}};

live(cast, {batch_ready, Call, QueryBatch}, State) ->
    {keep_state, process_outgoing_query(Call, QueryBatch, State)};

live(cast, {remove_user, Ref}, State) ->
    {keep_state, remove_user(Ref, State)};

live(cast, {send_query, Tag, Ref, Batch=#cql_query_batch{}}, State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=async, caller=Tag, client=Ref}, Batch),
    {keep_state, State};

live(cast, {Msg, Tag, Ref, Query}, State=#client_state{available_slots=[], queued=Queue0}) when Msg == send_query orelse
                                                                                          Msg == fetch_more ->
    {keep_state, State#client_state{
        queued=queue:in({#cql_call{type=async, caller=Tag, client=Ref}, Query}, Queue0)
    }};

live(cast, {Msg, Tag, Ref, Item}, State) when Msg == send_query orelse
                                              Msg == fetch_more ->
    case Item of
        Query=#cql_query{} -> ok;
        #cql_result{cql_query=Query=#cql_query{}} -> ok
    end,
    CacheResult = cqerl_cache:lookup(Query),
    {keep_state, process_outgoing_query(#cql_call{type=async, caller=Tag, client=Ref}, {CacheResult, Item}, State)};

live(cast, _Event, State) ->
    {keep_state, State};


live({call, From}, {new_user, User}, State=#client_state{users=Users}) ->
    add_user(User, Users),
    {keep_state, State, {reply, From, ok}};

live({call, From}, {send_query, Ref, Batch=#cql_query_batch{}}, _State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=sync, caller=From, client=Ref}, Batch),
    keep_state_and_data;


live({call, From}, {Msg, Ref, Query}, State=#client_state{available_slots=[], queued=Queue0}) when Msg == send_query orelse
                                                                                           Msg == fetch_more ->
    {keep_state, State#client_state{queued=queue:in({#cql_call{type=sync, caller=From, client=Ref}, Query}, Queue0)}};

live({call, From}, {Msg, Ref, Item}, State) when Msg == send_query orelse
                                                 Msg == fetch_more ->
    case Item of
        Query=#cql_query{} -> ok;
        #cql_result{cql_query=Query=#cql_query{}} -> ok
    end,
    CacheResult = cqerl_cache:lookup(Query),
    {keep_state, process_outgoing_query(#cql_call{type=sync, caller=From, client=Ref}, {CacheResult, Item}, State)};


live({call, From}, _Event, State) ->
    {keep_state, State, {reply, From, ok}};

live(info, {processor_threw, {Error, {Query, Call}}},
            State=#client_state{queries=Queries0}) ->
    case Call of
        {send, #cqerl_frame{stream_id=I}, _, _, _} ->
            case orddict:find(I, Queries0) of
                {ok, {UserCall, _}} ->
                    respond_to_user(UserCall, {error, Error});
                error ->
                    ok
            end,
            {next_state, live, release_stream_id(I, State)};

        {rows, _} ->
            {UserCall, _} = Query,
            respond_to_user(UserCall, {error, Error}),
            {next_state, live, State};

        {prepared, _rest} ->
            {next_state, live, State}
    end;

live(info, {prepared, CachedQuery=#cqerl_cached_query{key={_Inet, Statement}}},
            State=#client_state{waiting_preparation=Waiting}) ->
    case orddict:find(Statement, Waiting) of
        {ok, Waiters} ->
            Waiting2 = orddict:erase(Statement, Waiting),
            NewState = lists:foldl(fun
                (Item, StateAcc=#client_state{available_slots=[], queued=Queue0}) ->
                    StateAcc#client_state{queued=queue:in(Item, Queue0)};
                ({Call, Item}, StateAcc) ->
                    process_outgoing_query(Call, {CachedQuery, Item}, StateAcc)
            end, State#client_state{waiting_preparation=Waiting2}, Waiters),
            {next_state, live, NewState};
        error ->
            {next_state, live, State}
    end;

live(info, {preparation_failed, {_Inet, Statement}, Reason},
            State=#client_state{waiting_preparation=Waiting}) ->
    case orddict:find(Statement, Waiting) of
        {ok, Waiters} ->
            Waiting2 = orddict:erase(Statement, Waiting),
            lists:foreach(fun
                ({Call, _Item}) ->
                    respond_to_user(Call, {error, Reason})
            end, Waiters),
            {next_state, live, State#client_state{waiting_preparation=Waiting2}};
        error ->
            {next_state, live, State}
    end;

live(info, { tcp_closed, _Socket }, State = #client_state{ queries = Queries }) ->
    [ respond_to_user(Call, {error, connection_closed}) || {_, {Call, _}} <- Queries ],
    {stop, connection_closed, State};

live(info, { tcp_error, _Socket, _Reason }, State = #client_state{ queries = Queries }) ->
    [ respond_to_user(Call, {error, connection_closed}) || {_, {Call, _}} <- Queries ],
    {stop, connection_closed, State};

live(info, { ssl_closed, _Socket }, State = #client_state{ queries = Queries }) ->
    [ respond_to_user(Call, {error, connection_closed}) || {_, {Call, _}} <- Queries ],
    {stop, connection_closed, State};

live(info, { rows, Call, Result }, _State) ->
    respond_to_user(Call, Result),
    keep_state_and_data;

live(info, { Transport, Socket, BinaryMsg }, State = #client_state{ socket=Socket, trans=Transport, delayed=Delayed0 }) ->
    Resp = case cqerl_protocol:response_frame(base_frame(State), << Delayed0/binary, BinaryMsg/binary >>) of
        {delay, Delayed} ->
            {stop, {next_state, live, State}};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {void, _}, Delayed} ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, void);
                {ok, undefined} -> ok
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {rows, RawMsg}, Delayed} ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, undefined} -> ok;
                {ok, UserQuery} ->
                    cqerl_processor_sup:new_processor(UserQuery, {rows, RawMsg}, cqerl:get_protocol_version())
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, ResponseTerm={set_keyspace, _KeySpaceName}, Delayed} ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, ResponseTerm);
                {ok, undefined} -> ok
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {prepared, RawMsg}, Delayed} ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, {preparing, Query}} ->
                    cqerl_processor_sup:new_processor(Query, {prepared, RawMsg}, cqerl:get_protocol_version());
                {ok, undefined} -> ok
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {schema_change, ResponseTerm}, Delayed} ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, ResponseTerm);
                {ok, undefined} -> ok
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        %% Previously prepared query is absent from server's cache. We need to re-prepare and re-submit it:
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR, stream_id=StreamID}, {16#2500, _ErrString, _QueryID}, Delayed} when StreamID >= 0 ->
            NewState = release_stream_id(StreamID, State),
            FinalState = case orddict:find(StreamID, State#client_state.queries) of
                %% For single queries, just remove from our cache and re-issue it
                {ok, {Call, {Query = #cql_query{}, _ColumnSpecs}}} ->
                    cqerl_cache:remove(Query),
                    CacheResult = cqerl_cache:lookup(Query),
                    process_outgoing_query(Call, {CacheResult, Query}, NewState);
                %% For batch queries, don't bother trying to parse out and match the ID, just
                %% treat all queries as uncached and re-prepare them. This should happen
                %% rarely enough that it shouldn't be a performance issue.
                {ok, {Call, Batch = #cql_query_batch{queries = Queries}}} ->
                    SourceQueries = [Q || #cqerl_query{source_query = Q} <- Queries],
                    cqerl_cache:remove(SourceQueries),
                    RestartedBatch = Batch#cql_query_batch{queries = SourceQueries},
                    cqerl_batch_sup:new_batch_coordinator(Call, RestartedBatch),
                    NewState;
                {ok, undefined} ->
                    NewState
            end,
            {next_state, live, FinalState};

        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR, stream_id=StreamID}, ErrorTerm, Delayed} when StreamID >= 0 ->
            case orddict:find(StreamID, State#client_state.queries) of
                {ok, {preparing, Query}} ->
                    cqerl_cache:query_preparation_failed(Query, ErrorTerm);
                {ok, {Call, _}} -> respond_to_user(Call, {error, ErrorTerm});
                {ok, undefined} -> ok
            end,
            {next_state, live, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_EVENT}, _EventTerm, Delayed} ->
            ok%% TODO Manage incoming server-driven events
    end,

    case Resp of
      {stop, {_, _, State1} = Resp1} ->
        activate_socket(State1),
        append_delayed_segment(Resp1, Delayed);
      {next_state, live, State1} ->
        {keep_state, State1#client_state{delayed = <<>>}, {next_event, info, {Transport, Socket, Delayed}}}
    end;

live(info, {'DOWN', _MonitorRef, process, Pid, _Info}, State=#client_state{users=Users}) ->
    case ets:match_object(Users, #client_user{pid=Pid, _='_'}) of
        [] -> 
            keep_state_and_data;
        UserList ->
            State1 = lists:foldl(fun(#client_user{ref=Ref}, StateAcc) ->
                remove_user(Ref, StateAcc)
            end, State, UserList),
            case ets:info(Users, size) of
                0 ->    
                    {next_state, sleep, State1, State1#client_state.sleep};
                _ ->    
                    {keep_state, State1}
            end
    end.




sleep(timeout, ping, _State) ->
    signal_asleep(),
    keep_state_and_data;

sleep(cast, _Event, State=#client_state{sleep=Duration}) ->
    {keep_state, State, {timeout, Duration, ping}};

sleep({call, From}, {new_user, User}, State=#client_state{users=Users}) ->
    add_user(User, Users),
    {next_state, live, State, {reply, From, ok}};

sleep({call, From}, _Event, State) ->
    {keep_state, State, {reply, From, ok}};

sleep(info, { Transport, Socket, BinaryMsg }, State = #client_state{ socket=Socket, trans=Transport, sleep=Duration, delayed=Delayed0 }) ->
    case cqerl_protocol:response_frame(base_frame(State), << Delayed0/binary, BinaryMsg/binary >>) of
        %% To keep packets coherent, we still need to handle fragmented messages
        {delay, Delayed} ->
            activate_socket(State),
            %% Use a finite timeout if we have a message fragment; otherwise, use infinity.
            Duration1 = case Delayed of
                <<>> -> Duration;
                _ -> infinity
            end,
            {keep_state, State#client_state{delayed=Delayed}, {timeout, Duration1, ping}};

        %% While sleeping, any response to previously sent queries are ignored,
        %% but we still need to manage internal state accordingly
        {ok, #cqerl_frame{stream_id=StreamID}, _ResponseTerm, Delayed} when StreamID < ?QUERIES_MAX, StreamID >= 0 ->
            State1 = release_stream_id(StreamID, State),
            {keep_state, State1#client_state{delayed = <<>>}, {next_event, info, {Transport, Socket, Delayed}}};

        {ok, #cqerl_frame{opcode=?CQERL_OP_EVENT}, _EventTerm, Delayed} ->
            {keep_state, State#client_state{delayed = <<>>}, {next_event, info, {Transport, Socket, Delayed}}}
    end.


terminate(_Reason, sleep, _State) ->
    ok;

terminate(Reason, live, #client_state{queries=Queries}) ->
    lists:foreach(fun
        ({_I, {#cql_call{type=sync, caller=From}, _}}) ->
            gen_statem:reply(From, {error, Reason});
        ({_I, {#cql_call{type=async, caller={Pid, Tag}}, _}}) ->
            Pid ! {cql_error, Tag, Reason};
        ({_I, _}) -> ok
    end, Queries);

terminate(_Reason, starting, _State) ->
    ok.



code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.





%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------




dequeue_query(State0=#client_state{queued=Queue0}) ->
    case queue:out(Queue0) of
        {{value, {Call, Batch=#cql_query_batch{}}}, Queue1} ->
            State1 = process_outgoing_query(Call, Batch, State0),
            {true, State1#client_state{queued=Queue1}};

        {{value, {prepare, Query}}, Queue1} when is_binary(Query) ->
            State1 = process_outgoing_query(prepare, Query, State0),
            {true, State1#client_state{queued=Queue1}};

        {{value, {Call, Item}}, Queue1} ->
            case Item of
                Query=#cql_query{} -> ok;
                #cql_result{cql_query=Query=#cql_query{}} -> ok
            end,
            CacheResult = cqerl_cache:lookup(Query),
            State1 = process_outgoing_query(Call, {CacheResult, Item}, State0),
            {true, State1#client_state{queued=Queue1}};

        {empty, _} ->
            {false, State0}
    end.





maybe_signal_busy(State) ->
    if
        length(State#client_state.available_slots) == ?QUERIES_MAX - ?QUERIES_HW ->
            signal_busy();
        true -> ok
    end.




append_delayed_segment({X, Y, State}, Delayed) ->
    {X, Y, State#client_state{delayed=Delayed}}.



release_stream_id(StreamID, State=#client_state{available_slots=Slots, queries=Queries}) ->
    State2 = State#client_state{
        available_slots=[StreamID | Slots],
        queries=orddict:store(StreamID, undefined, Queries)
    },
    if  length(Slots) - 5 == ?QUERIES_MAX - ?QUERIES_HW -> signal_avail();
        true -> ok
    end,
    {_Dequeued, State3} = dequeue_query(State2),
    State3.





process_outgoing_query(prepare, Query, State=#client_state{queries=Queries0}) ->
    {BaseFrame, State1} = seq_frame(State),
    {ok, PrepareFrame} = cqerl_protocol:prepare_frame(BaseFrame, Query),
    send_to_db(State1, PrepareFrame),
    maybe_signal_busy(State1),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {preparing, Query}, Queries0),
    State1#client_state{queries=Queries1};

process_outgoing_query(Call=#cql_call{}, Batch=#cql_query_batch{}, State=#client_state{queries=Queries0}) ->
    {BaseFrame, State1} = seq_frame(State),
    {ok, BatchFrame} = cqerl_protocol:batch_frame(BaseFrame, Batch),
    send_to_db(State1, BatchFrame),
    maybe_signal_busy(State1),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {Call, Batch}, Queries0),
    State1#client_state{queries=Queries1};

process_outgoing_query(Call,
                       {queued, Continuation=#cql_result{cql_query=#cql_query{statement=Statement}}},
                       State=#client_state{waiting_preparation=Waiting}) ->
    Waiting2 = case orddict:find(Statement, Waiting) of
        error -> orddict:store(Statement, [{Call, Continuation}], Waiting);
        _     -> orddict:append(Statement, {Call, Continuation}, Waiting)
    end,
    State#client_state{waiting_preparation=Waiting2};

process_outgoing_query(Call,
                       {queued, Query=#cql_query{statement=Statement}},
                       State=#client_state{waiting_preparation=Waiting}) ->
    Waiting2 = case orddict:find(Statement, Waiting) of
        error -> orddict:store(Statement, [{Call, Query}], Waiting);
        _     -> orddict:append(Statement, {Call, Query}, Waiting)
    end,
    State#client_state{waiting_preparation=Waiting2};

process_outgoing_query(Call,
                       {CachedResult, Item},
                       State=#client_state{queries=Queries0}) ->

    {BaseFrame, State1} = seq_frame(State),
    I = BaseFrame#cqerl_frame.stream_id,
    case Item of
        Query = #cql_query{values=Values} ->
            ColumnSpecs = undefined,
            SkipMetadata = false;
        #cql_result{cql_query = Query=#cql_query{values=Values},
                    columns=ColumnSpecs} ->
            SkipMetadata = true
    end,
    Queries1 = case CachedResult of
        uncached -> orddict:store(I, {Call, {Query, ColumnSpecs}}, Queries0);
        #cqerl_cached_query{result_metadata=#cqerl_result_metadata{columns=CachedColumnSpecs}} ->
            orddict:store(I, {Call, {Query, CachedColumnSpecs}}, Queries0)
    end,
    cqerl_processor_sup:new_processor(
        { State#client_state.trans, State#client_state.socket, CachedResult },
        { send, BaseFrame, Values, Query, SkipMetadata },
        cqerl:get_protocol_version()
    ),
    maybe_signal_busy(State2 = State1#client_state{queries=Queries1}),
    State2.



respond_to_user(#cql_call{type=sync, caller=From}, Reply={error, _Term}) ->
    gen_statem:reply(From, Reply);

respond_to_user(#cql_call{type=sync, caller=From}, Term) ->
    gen_statem:reply(From, {ok, Term});

respond_to_user(#cql_call{type=async, caller={Pid, QueryRef}}, {error, Term}) ->
    Pid ! {error, QueryRef, Term};

respond_to_user(#cql_call{type=async, caller={Pid, QueryRef}}, Term) ->
    Pid ! {result, QueryRef, Term}.



add_user(From={Pid, _Tag}, Users) ->
    Ref = make_ref(),
    Monitor = monitor(process, Pid),
    ets:insert(Users, #client_user{ref=Ref, pid=Pid, monitor=Monitor}),
    gen_server:reply(From, {ok, {self(), Ref}}).





remove_user(Ref, State=#client_state{users=Users, queued=Queue0, queries=Queries0}) ->
    case ets:lookup(Users, Ref) of
        [] -> State;
        [#client_user{monitor=Monitor}] ->
            demonitor(Monitor, [flush]),
            ets:delete(Users, Ref),

            %% Remove in-flight queries from this user
            %% we leave slots as though they're being used, since they can't yet be reused
            Queries1 = lists:map(fun
                ({I, {{_, _, CRef}, _, _}}) when Ref == CRef -> {I, undefined};
                (Entry) -> Entry
            end, Queries0),
            State#client_state{queued=Queue0, queries=Queries1}
    end.




maybe_set_keyspace(State=#client_state{keyspace=undefined}) ->
    {live, switch_to_live_state(State)};
maybe_set_keyspace(State=#client_state{keyspace=Keyspace}) ->
    KeyspaceName = atom_to_binary(Keyspace, latin1),
    BaseFrame = base_frame(State),
    {ok, Frame} = cqerl_protocol:query_frame(BaseFrame,
        #cqerl_query_parameters{},
        #cqerl_query{statement = <<"USE ", KeyspaceName/binary>>}
    ),
    send_to_db(State, Frame),
    {starting, State}.

switch_to_live_state(State=#client_state{keyspace=Keyspace,
                                         inet=Inet, key=Key}) ->
    signal_alive(Inet, Keyspace),
    cqerl_hash:client_started(Key),
    Queries = create_queries_dict(),
    State1 = State#client_state{
        authstate=undefined, authargs=undefined, delayed = <<>>,
        queued=queue:new(),
        queries=Queries,
        available_slots = orddict:fetch_keys(Queries)
    },
    State1.




send_to_db(#client_state{trans=tcp, socket=Socket}, Data) when is_binary(Data) ->
    gen_tcp:send(Socket, Data);
send_to_db(#client_state{trans=ssl, socket=Socket}, Data) when is_binary(Data) ->
    ssl:send(Socket, Data).




create_socket({Addr, Port}, Opts) ->
    BaseOpts = [{active, false}, {mode, binary}],
    Result = case proplists:lookup(ssl, Opts) of
        {ssl, false} ->
            Transport = tcp,
            case proplists:lookup(tcp_opts, Opts) of
                none ->
                    gen_tcp:connect(Addr, Port, BaseOpts, 2000);
                {tcp_opts, TCPOpts} ->
                    gen_tcp:connect(Addr, Port, BaseOpts ++ TCPOpts, 2000)
            end;

        {ssl = Transport, true} ->
            ssl:connect(Addr, Port, BaseOpts, 2000);

        {ssl = Transport, SSLOpts} when is_list(SSLOpts) ->
            ssl:connect(Addr, Port, SSLOpts ++ BaseOpts, 2000)
    end,
    case Result of
        {ok, Socket} -> {ok, Socket, Transport};
        Other -> Other
    end.



activate_socket(#client_state{socket=undefined}) ->
    ok;
activate_socket(#client_state{trans=ssl, socket=Socket}) ->
    ssl:setopts(Socket, [{active, once}]);
activate_socket(#client_state{trans=tcp, socket=Socket}) ->
    inet:setopts(Socket, [{active, once}]).




signal_asleep() ->
    gen_server:cast(cqerl, {client_asleep, self()}).

signal_busy() ->
    gen_server:cast(cqerl, {client_busy, self()}).

signal_avail() ->
    gen_server:cast(cqerl, {client_avail, self()}).

signal_alive(Inet, Keyspace) ->
    gen_server:cast(cqerl, {client_alive, self(), Inet, Keyspace}).



choose_compression_type({'COMPRESSION', Choice}) ->
    SupportedCompression = lists:map(fun (CompressionNameBin) -> binary_to_atom(CompressionNameBin, latin1) end, Choice),
    case lists:member(lz4, SupportedCompression) andalso module_exists(lz4) of
        true -> lz4;
        _ -> case lists:member(snappy, SupportedCompression) andalso module_exists(snappyer) of
            true -> snappy;
            _ -> undefined
        end
    end;

choose_compression_type(none) -> undefined.




choose_cql_version({'CQL_VERSION', Versions}) ->
    SemVersions = lists:sort(
        fun (SemVersion1, SemVersion2) ->
            case semver:compare(SemVersion1, SemVersion2) of
                -1 -> false;
                _ -> true
            end
        end,
        lists:map(fun (Version) -> semver:parse(Version) end, Versions)
    ),
    case application:get_env(cqerl, preferred_cql_version, undefined) of
        undefined ->
            [GreaterVersion|_] = SemVersions;
        Version1 ->
            [GreaterVersion|_] = lists:dropwhile(fun (SemVersion) ->
                case semver:compare(SemVersion, Version1) of
                    1 -> true;
                    _ -> false
                end
            end, SemVersions)
    end,
    [_v | Version] = semver:vsn_string(GreaterVersion),
    list_to_binary(Version).




base_frame(#client_state{compression_type=CompressionType}) ->
    #cqerl_frame{compression_type=CompressionType}.

seq_frame(State=#client_state{compression_type=CompressionType, available_slots=[Slot | Rest]}) ->
    {#cqerl_frame{compression_type=CompressionType, stream_id=Slot}, State#client_state{available_slots=Rest}}.


module_exists(Module) ->
    case code:is_loaded(Module) of
        {file, _} -> true;
        false -> false
    end.


create_queries_dict() ->
    create_queries_dict(?QUERIES_MAX-1, []).

create_queries_dict(0, Acc) ->
    [{0, undefined} | Acc];
create_queries_dict(N, Acc) ->
    create_queries_dict(N-1, [{N, undefined} | Acc]).



stop_during_startup(Reason, State = #client_state{users = Users}) ->
    lists:foreach(fun (From) -> gen_server:reply(From, {error, Reason}) end, Users),
    {stop, normal, State#client_state{socket=undefined}}.
