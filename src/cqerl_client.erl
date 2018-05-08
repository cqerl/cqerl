-module(cqerl_client).
-behaviour(gen_fsm).
-define(SERVER, ?MODULE).

-include("cqerl_protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3, start_link/4, new_user/2, remove_user/1,
         run_query/2, query_async/2, fetch_more/1, fetch_more_async/1,
         prepare_query/2, batch_ready/2, make_key/2]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-define(QUERIES_MAX, 128).
-define(QUERIES_HW, 88).
-define(FSM_TIMEOUT, case application:get_env(cqerl, query_timeout) of
    undefined -> 30000;
    {ok, Val} -> Val
end).

-define(IS_IOLIST(L), is_list(L) orelse is_binary(L)).

-export([init/1, terminate/3,
         starting/2,    starting/3,
         live/2,        live/3,
         sleep/2,       sleep/3,
         handle_event/3, handle_sync_event/4, handle_info/3,
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
    mode :: hash | pooler,
    key :: {term(), term()},
    heartbeat_interval  :: non_neg_integer(), % in millisecond
    last_socket_send    :: non_neg_integer()  % in millisecond
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
    gen_fsm:start_link(?MODULE, [Inet, Opts, OptGetter, undefined], []).

start_link(Inet, Opts, OptGetter, Key) ->
    gen_fsm:start_link(?MODULE, [Inet, Opts, OptGetter, Key], []).

new_user(Pid, From) ->
    case cqerl_app:mode() of
        pooler ->
            try
                gen_fsm:sync_send_event(Pid, {new_user, From}, infinity)
            catch
                exit:_ -> {error, {closed, process_died}}
            end;
        hash ->
            ok
    end.

remove_user({ClientPid, ClientRef}) ->
    cqerl_app:mode() =:= pooler andalso
    gen_fsm:send_event(ClientPid, {remove_user, ClientRef}).

run_query(Client, Query) when ?IS_IOLIST(Query) ->
    run_query(Client, #cql_query{statement=Query});
run_query(Client, Query=#cql_query{statement=Statement}) when is_list(Statement) ->
    run_query(Client, Query#cql_query{statement=iolist_to_binary(Statement)});
run_query({ClientPid, ClientRef}, Query) ->
    gen_fsm:sync_send_event(ClientPid, {send_query, ClientRef, Query}, ?FSM_TIMEOUT).

query_async(Client, Query) when ?IS_IOLIST(Query) ->
    query_async(Client, #cql_query{statement=Query});
query_async(Client, Query=#cql_query{statement=Statement}) when is_list(Statement) ->
    query_async(Client, Query#cql_query{statement=iolist_to_binary(Statement)});
query_async({ClientPid, ClientRef}, Query) ->
    QueryRef = make_ref(),
    gen_fsm:send_event(ClientPid, {send_query, {self(), QueryRef}, ClientRef, Query}),
    QueryRef.

fetch_more(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    gen_fsm:sync_send_event(ClientPid, {fetch_more, ClientRef, Continuation}, ?FSM_TIMEOUT).

fetch_more_async(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    QueryRef = make_ref(),
    gen_fsm:send_event(ClientPid, {fetch_more, {self(), QueryRef}, ClientRef, Continuation}),
    QueryRef.

prepare_query(ClientPid, Query) ->
    % We don't want the cqerl_cache process to crash if our client has gone away,
    % so wrap in a try-catch
    try
        gen_fsm:send_event(ClientPid, {prepare_query, Query})
    catch
        _:_ -> ok
    end.

batch_ready({ClientPid, Call}, QueryBatch) ->
    gen_fsm:send_event(ClientPid, {batch_ready, Call, QueryBatch}).

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
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init([Inet, Opts, OptGetter, Key]) ->
    case create_socket(Inet, OptGetter) of
        {ok, Socket, Transport} ->
            {AuthHandler, AuthArgs} = OptGetter(auth),
            HeartbeatInterval = OptGetter(heartbeat_interval),
            cqerl:put_protocol_version(OptGetter(protocol_version)),
            {ok, OptionsFrame} = cqerl_protocol:options_frame(#cqerl_frame{}),
            State0 = #client_state{
                socket=Socket, trans=Transport, inet=Inet,
                authmod=AuthHandler, authargs=AuthArgs,
                users=[],
                sleep=get_sleep_duration(Opts),
                keyspace=normalise_to_atom(proplists:get_value(keyspace, Opts)),
                key=Key,
                mode=cqerl_app:mode(),
                heartbeat_interval = HeartbeatInterval
            },
            State = send_to_db(State0, OptionsFrame),
            activate_socket(State),
            erlang:send_after(HeartbeatInterval, self(), heartbeat_check),
            {ok, starting, State};

        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.




starting(_Event, State) ->
    {next_state, starting, State}.

starting({new_user, User}, _From, State=#client_state{users=Users}) ->
    {reply, ok, starting, State#client_state{users=[User | Users]}};

starting(_Event, _From, State) ->
    {reply, unexpected_msg, starting, State}.


live({prepare_query, Query}, State=#client_state{available_slots=[], queued=Queue0}) ->
    {next_state, live, State#client_state{queued=queue:in_r({prepare, Query}, Queue0)}};

live({prepare_query, Query}, State) ->
    {next_state, live, process_outgoing_query(prepare, Query, State)};

live({batch_ready, Call, QueryBatch}, State=#client_state{available_slots=[], queued=Queue0}) ->
    {next_state, live, State#client_state{queued=queue:in({Call, QueryBatch}, Queue0)}};

live({batch_ready, Call, QueryBatch}, State) ->
    {next_state, live, process_outgoing_query(Call, QueryBatch, State)};

live({remove_user, Ref}, State) ->
    {next_state, live, remove_user(Ref, State)};

live({send_query, Tag, Ref, Batch=#cql_query_batch{}}, State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=async, caller=Tag, client=Ref}, Batch),
    {next_state, live, State};

live({Msg, Tag, Ref, Query}, State=#client_state{available_slots=[], queued=Queue0}) when Msg == send_query orelse
                                                                                          Msg == fetch_more ->
    {next_state, live, State#client_state{
        queued=queue:in({#cql_call{type=async, caller=Tag, client=Ref}, Query}, Queue0)
    }};

live({Msg, Tag, Ref, Item}, State) when Msg == send_query orelse
                                        Msg == fetch_more ->
    case Item of
        Query=#cql_query{} -> ok;
        #cql_result{cql_query=Query=#cql_query{}} -> ok
    end,
    CacheResult = cqerl_cache:lookup(Query),
    {next_state, live, process_outgoing_query(#cql_call{type=async, caller=Tag, client=Ref}, {CacheResult, Item}, State)};

live(_Event, State) ->
    {next_state, live, State}.


live({new_user, User}, _From, State=#client_state{users=Users}) ->
    add_user(User, Users),
    {reply, ok, live, State};

live({send_query, Ref, Batch=#cql_query_batch{}}, From, State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=sync, caller=From, client=Ref}, Batch),
    {next_state, live, State};


live({Msg, Ref, Query}, From, State=#client_state{available_slots=[], queued=Queue0}) when Msg == send_query orelse
                                                                                           Msg == fetch_more ->
    {next_state, live, State#client_state{queued=queue:in({#cql_call{type=sync, caller=From, client=Ref}, Query}, Queue0)}};

live({Msg, Ref, Item}, From, State) when Msg == send_query orelse
                                         Msg == fetch_more ->
    case Item of
        Query=#cql_query{} -> ok;
        #cql_result{cql_query=Query=#cql_query{}} -> ok
    end,
    CacheResult = cqerl_cache:lookup(Query),
    {next_state, live, process_outgoing_query(#cql_call{type=sync, caller=From, client=Ref}, {CacheResult, Item}, State)};


live(_Event, _From, State) ->
    {reply, ok, live, State}.




sleep(timeout, State) ->
    signal_asleep(),
    {next_state, sleep, State};

sleep(_Event, State=#client_state{sleep=Duration}) ->
    {next_state, sleep, State, Duration}.

sleep({new_user, User}, _From, State=#client_state{users=Users}) ->
    add_user(User, Users),
    {reply, ok, live, State};

sleep(_Event, _From, State) ->
    {reply, ok, sleep, State}.




handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({processor_threw, {Error, {Query, Call}}}, live,
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

handle_info({prepared, CachedQuery=#cqerl_cached_query{key={_Inet, Statement}}}, live,
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

handle_info({preparation_failed, {_Inet, Statement}, Reason}, live,
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

handle_info({tcp_closed, _Socket}, starting, State) ->
    stop_during_startup({error, {connection_closed, normal}}, State);

handle_info({tcp_closed, _Socket}, _, State = #client_state{queries = Queries}) ->
    [ case Call of
          #cql_call{} -> respond_to_user(Call, {error, {connection_closed, normal}});
          _ -> ok
      end || {_, {Call, _}} <- Queries ],
    {stop, {connection_closed, normal}, State};

handle_info({tcp_error, _Socket, Reason}, _, State = #client_state{queries = Queries}) ->
    [ case Call of
          #cql_call{} -> respond_to_user(Call, {error, {connection_closed, Reason}});
          _ -> ok
      end || {_, {Call, _}} <- Queries ],
    {stop, {connection_closed, Reason}, State};

handle_info({ssl_closed, _Socket}, starting, State) ->
    stop_during_startup({error, {connection_closed, normal}}, State);

handle_info({ssl_closed, _Socket}, _, State = #client_state{queries = Queries}) ->
    [ case Call of
          #cql_call{} -> respond_to_user(Call, {error, {connection_closed, normal}});
          _ -> ok
      end || {_, {Call, _}} <- Queries ],
    {stop, connection_closed, State};

handle_info({ssl_error, _Socket, Reason}, _, State = #client_state{queries = Queries}) ->
    [ case Call of
          #cql_call{} -> respond_to_user(Call, {error, {connection_closed, Reason}});
          _ -> ok
      end || {_, {Call, _}} <- Queries ],
    {stop, {connection_closed, Reason}, State};

handle_info({ Transport, Socket, BinaryMsg }, starting, State = #client_state{ socket=Socket, trans=Transport, delayed=Delayed0 }) ->
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
            State1 = send_to_db(State, StartupFrame),
            {next_state, starting, State1#client_state{compression_type=Compression}};

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
                    State1 = send_to_db(State, AuthFrame),
                    {next_state, starting, State1#client_state{ authstate=AuthState }}
            end;

        %% Server tells us we need to give another piece of data
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTH_CHALLENGE}, Body, Delayed} ->
            #client_state{ authmod=AuthMod, authstate=AuthState } = State,
            case AuthMod:auth_handle_challenge(Body, AuthState) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);
                {reply, Reply, AuthState} ->
                    {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
                    State1 = send_to_db(State, AuthFrame),
                    {next_state, starting, State1#client_state{ authstate=AuthState }}
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
    {_, _, State2} = Resp,
    activate_socket(State2),
    append_delayed_segment(Resp, Delayed);

handle_info({ rows, Call, Result }, live, State) ->
    respond_to_user(Call, Result),
    {next_state, live, State};

handle_info({ Transport, Socket, BinaryMsg }, live, State = #client_state{ socket=Socket, trans=Transport, delayed=Delayed0 }) ->
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
            ok; %% TODO Manage incoming server-driven events
        {ok, #cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, _Payload, Delayed} ->
            %% SUPPORTED is the message received from Cassandra for OPTIONS request,
            %% which we use for heartbeats. We need to handle that for live state without any State modifications.
            {next_state, live, State}
    end,

    case Resp of
      {stop, {_, _, State1} = Resp1} ->
        activate_socket(State1),
        append_delayed_segment(Resp1, Delayed);
      {next_state, live, State1} ->
        handle_info({Transport, Socket, Delayed}, live, State1#client_state{delayed = <<>>})
    end;


handle_info({ Transport, Socket, BinaryMsg }, sleep, State = #client_state{ socket=Socket, trans=Transport, sleep=Duration, delayed=Delayed0 }) ->
    case cqerl_protocol:response_frame(base_frame(State), << Delayed0/binary, BinaryMsg/binary >>) of
        %% To keep packets coherent, we still need to handle fragmented messages
        {delay, Delayed} ->
            activate_socket(State),
            %% Use a finite timeout if we have a message fragment; otherwise, use infinity.
            Duration1 = case Delayed of
                <<>> -> Duration;
                _ -> infinity
            end,
            {next_state, sleep, State#client_state{delayed=Delayed}, Duration1};

        {ok, #cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, _Payload, Delayed} ->
            %% SUPPORTED is the message received from Cassandra for OPTIONS request,
            %% which we use for heartbeats. We need to handle that for sleep state
            %% in the same way as in live state.
            handle_info({Transport, Socket, Delayed}, sleep, State#client_state{delayed = <<>>});

        %% While sleeping, any response to previously sent queries are ignored,
        %% but we still need to manage internal state accordingly
        {ok, #cqerl_frame{stream_id=StreamID}, _ResponseTerm, Delayed} when StreamID < ?QUERIES_MAX, StreamID >= 0 ->
            State1 = release_stream_id(StreamID, State),
            handle_info({Transport, Socket, Delayed}, sleep, State1#client_state{delayed = <<>>});

        {ok, #cqerl_frame{opcode=?CQERL_OP_EVENT}, _EventTerm, Delayed} ->
            handle_info({Transport, Socket, Delayed}, sleep, State#client_state{delayed = <<>>})
    end;

handle_info({'DOWN', _MonitorRef, process, Pid, _Info}, live, State=#client_state{users=Users}) ->
    case ets:match_object(Users, #client_user{pid=Pid, _='_'}) of
        [] -> {next_state, live, State};
        UserList ->
            State1 = lists:foldl(fun(#client_user{ref=Ref}, StateAcc) ->
                remove_user(Ref, StateAcc)
            end, State, UserList),
            case ets:info(Users, size) of
                0 ->    {next_state, sleep, State1, State1#client_state.sleep};
                _ ->    {next_state, live, State1}
            end
    end;

handle_info(heartbeat_check, StateName, State = #client_state{heartbeat_interval = 0}) ->
    {next_state, StateName, State}; %% Disable heartbeat if interval is set to 0
handle_info(heartbeat_check, StateName, State = #client_state{heartbeat_interval = HeartbeatInterval}) ->
    TimeMargin = timer:seconds(1),
    State1 =
        case next_heartbeat_within(State) of
            TimeLeft when TimeLeft < TimeMargin ->
                %% OPTIONS frame is what Java driver sends as a heartbeat in case of idle connection
                {ok, OptionsFrame} = cqerl_protocol:options_frame(#cqerl_frame{}),
                erlang:send_after(HeartbeatInterval, self(), heartbeat_check),
                send_to_db(State, OptionsFrame);
            TimeLeft ->
                erlang:send_after(TimeLeft, self(), heartbeat_check),
                State
        end,

    {next_state, StateName, State1};

handle_info(Info, StateName, State) ->
    io:format("Received message ~w while in state ~w~n", [Info, StateName]),
    {next_state, StateName, State}.



terminate(_Reason, sleep, _State) ->
    ok;

terminate(Reason, live, #client_state{queries=Queries}) ->
    lists:foreach(fun
        ({_I, {#cql_call{type=sync, caller=From}, _}}) ->
            gen_fsm:reply(From, {error, Reason});
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
    State2 = send_to_db(State1, PrepareFrame),
    maybe_signal_busy(State2),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {preparing, Query}, Queries0),
    State2#client_state{queries=Queries1};

process_outgoing_query(Call=#cql_call{}, Batch=#cql_query_batch{}, State=#client_state{queries=Queries0}) ->
    {BaseFrame, State1} = seq_frame(State),
    {ok, BatchFrame} = cqerl_protocol:batch_frame(BaseFrame, Batch),
    State2 = send_to_db(State1, BatchFrame),
    maybe_signal_busy(State2),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {Call, Batch}, Queries0),
    State2#client_state{queries=Queries1};

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
    gen_fsm:reply(From, Reply);

respond_to_user(#cql_call{type=sync, caller=From}, Term) ->
    gen_fsm:reply(From, {ok, Term});

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
    State1 = send_to_db(State, Frame),
    {starting, State1}.

switch_to_live_state(State=#client_state{users=Users, keyspace=Keyspace,
                                         inet=Inet, key=Key, mode=Mode}) ->
    signal_alive(Inet, Keyspace),
    UsersTab =
    case Mode of
        hash ->
            cqerl_hash:client_started(Key),
            undefined;
        pooler ->
            T = ets:new(users, [set, private, {keypos, #client_user.ref}]),
            lists:foreach(fun(From) -> add_user(From, T) end, Users),
            T
    end,
    Queries = create_queries_dict(),
    State1 = State#client_state{
        authstate=undefined, authargs=undefined, delayed = <<>>,
        queued=queue:new(),
        queries=Queries,
        available_slots = orddict:fetch_keys(Queries),
        users=UsersTab
    },
    State1.




send_to_db(ClientState, Data) when is_binary(Data) ->
    do_send_to_db(ClientState, Data),
    LastSocketSend = os:system_time(millisecond),
    ClientState#client_state{last_socket_send = LastSocketSend}.

do_send_to_db(#client_state{trans=tcp, socket=Socket}, Data) when is_binary(Data) ->
    gen_tcp:send(Socket, Data);
do_send_to_db(#client_state{trans=ssl, socket=Socket}, Data) when is_binary(Data) ->
    ssl:send(Socket, Data).




create_socket({Addr, Port}, OptGetter) ->
    BaseOpts = [{active, false}, {mode, binary}],
    Result = case {ssl, OptGetter(ssl)} of
        {ssl, false} ->
            Transport = tcp,
            Opts = BaseOpts ++ OptGetter(tcp_opts),
            gen_tcp:connect(Addr, Port, Opts, 2000);

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
        _ -> case lists:member(snappy, SupportedCompression) andalso module_exists(snappy) of
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



get_sleep_duration(Opts) ->
    case cqerl_app:mode() of
        hash -> infinity;
        _ ->
            round(case proplists:get_value(sleep_duration, Opts) of
                      {Amount, sec} -> Amount * 1000;
                      {Amount, min} -> Amount * 1000 * 60;
                      {Amount, hour} -> Amount * 1000 * 60 * 60;
                      Amount when is_integer(Amount) -> Amount
                  end)
    end.

stop_during_startup(Reason, State = #client_state{users = Users}) ->
    lists:foreach(fun (From) -> gen_server:reply(From, {error, Reason}) end, Users),
    % In pooler mode, do a short sleep here before dying - pooler immediately (and
    % continiously) restarts the process, and in failure states such as setting an
    % invalid keyspace this leads very quickly to use of all the local ports and
    % eaddrinuse errors on further attempts (including valid ones) by clients to
    % connect.
    cqerl_app:mode() =:= pooler andalso timer:sleep(200),
    {stop, normal, State#client_state{socket=undefined}}.


next_heartbeat_within(#client_state{last_socket_send = LastSocketSend,
                                    heartbeat_interval = Interval}) ->
    CurrentTime = os:system_time(millisecond),
    TimeLeft = Interval - (CurrentTime - LastSocketSend),
    max(0, TimeLeft).
