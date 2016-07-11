-module(cqerl_client).
-behaviour(gen_server).

-include("cqerl_protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/3,
         run_query/2, query_async/2,
         fetch_more/1, fetch_more_async/1,
         prepare_query/2,
         batch_ready/2]).

-define(QUERIES_MAX, 128).
-define(DEFAULT_QUERY_TIMEOUT, 30000).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    group_name :: term(),

    %% Initialisation options
    opts :: proplists:proplist(),

    %% Authentication state (only kept during initialization)
    authmod :: atom(),
    authstate :: any(),
    authargs :: list(any()),

    %% Information about the connection
    node :: cqerl_node(),
    trans :: atom(),
    socket :: gen_tcp:socket() | ssl:sslsocket(),
    compression_type :: undefined | snappy | lz4,
    keyspace :: atom(),
    setup_tasks = [] :: [register | set_keyspace],

    %% Operating state
    delayed = <<>> :: binary(),     % Fragmented message continuation
    queries = [] :: list({integer(), term()}),
    queued = queue:new() :: queue:queue(),
    available_slots = [] :: list(integer()),
    waiting_preparation = [],
    state :: starting | live
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Name, Inet, Opts) ->
    gen_server:start_link(?MODULE, [Name, Inet, Opts], []).

run_query({ClientPid, ClientRef}, Query) ->
    gen_server:call(ClientPid, {send_query, ClientRef, Query}, query_timeout()).

query_async({ClientPid, ClientRef}, Query) ->
    QueryRef = make_ref(),
    gen_server:cast(ClientPid, {send_query, {self(), QueryRef}, ClientRef, Query}),
    QueryRef.

fetch_more(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    gen_server:call(ClientPid, {fetch_more, ClientRef, Continuation}, query_timeout()).

fetch_more_async(Continuation=#cql_result{client={ClientPid, ClientRef}}) ->
    QueryRef = make_ref(),
    gen_server:cast(ClientPid, {fetch_more, {self(), QueryRef}, ClientRef, Continuation}),
    QueryRef.

prepare_query(ClientPid, Query) ->
    % We don't want the cqerl_cache process to crash if our client has gone away,
    % so wrap in a try-catch
    try
        gen_server:cast(ClientPid, {prepare_query, Query})
    catch
        _:_ -> ok
    end.

batch_ready({ClientPid, Call}, QueryBatch) ->
    gen_server:cast(ClientPid, {batch_ready, Call, QueryBatch}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Name, Node, Opts]) ->
    {auth, {AuthHandler, AuthArgs}} = proplists:lookup(auth, Opts),
    SetupTasks = case proplists:get_value(register_for_events, Opts, false) of
                     false -> [];
                     true -> [register]
                 end,

    Keyspace = proplists:get_value(keyspace, Opts),
    SetupTasks2 = case Keyspace of
                      undefined -> SetupTasks;
                      _ -> [set_keyspace | SetupTasks]
                  end,

    State = #state{
                   group_name = Name,
                   node = Node,
                   opts = Opts,
                   keyspace = cqerl:normalise_keyspace(Keyspace),
                   authmod = AuthHandler,
                   authargs = AuthArgs,
                   setup_tasks = SetupTasks2,
                   state = starting
                  },
    self() ! do_startup,
    {ok, State}.

handle_cast(_, State=#state{state = starting}) ->
    {noreply, State};

handle_cast({prepare_query, Query}, State=#state{available_slots=[], queued=Queue0}) ->
    {noreply, State#state{queued=queue:in_r({prepare, Query}, Queue0)}};

handle_cast({prepare_query, Query}, State) ->
    {noreply, process_outgoing_query(prepare, Query, State)};

handle_cast({batch_ready, Call, QueryBatch}, State=#state{available_slots=[], queued=Queue0}) ->
    {noreply, State#state{queued=queue:in({Call, QueryBatch}, Queue0)}};

handle_cast({batch_ready, Call, QueryBatch}, State) ->
    {noreply, process_outgoing_query(Call, QueryBatch, State)};

handle_cast({send_query, Tag, Ref, Batch=#cql_query_batch{}}, State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=async, caller=Tag, client=Ref}, Batch),
    {noreply, State};

handle_cast({Msg, Tag, Ref, Query}, State=#state{available_slots=[], queued=Queue0})
  when Msg =:= send_query; Msg =:= fetch_more ->
    {noreply, State#state{
        queued=queue:in({#cql_call{type=async, caller=Tag, client=Ref}, Query}, Queue0)
    }};

handle_cast({Msg, Tag, Ref, Item}, State) when Msg == send_query orelse
                                               Msg == fetch_more ->
    Query = case Item of
        #cql_query{} -> Item;
        #cql_result{cql_query=Q} -> Q
    end,
    CacheResult = cqerl_cache:lookup(Query),
    {noreply, process_outgoing_query(#cql_call{type=async, caller=Tag, client=Ref}, {CacheResult, Item}, State)};

handle_cast(_Event, State) ->
    {noreply, State}.

handle_call(_, _, State = #state{state = starting}) ->
    {reply, {error, not_ready}, State};

handle_call({send_query, Ref, Batch=#cql_query_batch{}}, From, State) ->
    cqerl_batch_sup:new_batch_coordinator(#cql_call{type=sync, caller=From, client=Ref}, Batch),
    {noreply, State};

handle_call({Msg, Ref, Query}, From, State=#state{available_slots=[], queued=Queue0})
  when Msg == send_query; Msg =:= fetch_more ->
    NewQueue = queue:in({#cql_call{type=sync, caller=From, client=Ref}, Query}, Queue0),
    {noreply, State#state{queued = NewQueue}};

handle_call({Msg, Ref, Item}, From, State) when Msg == send_query orelse
                                                Msg == fetch_more ->
    Query = case Item of
        Q=#cql_query{} -> Q;
        #cql_result{cql_query=Q=#cql_query{}} -> Q
    end,
    CacheResult = cqerl_cache:lookup(Query),
    NewState = process_outgoing_query(#cql_call{type=sync, caller=From, client=Ref},
                                      {CacheResult, Item},
                                      State),
    {noreply, NewState};


handle_call(_Event, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_info(do_startup, State) ->
    {noreply, do_startup(State)};

handle_info({processor_threw, {Error, {Query, Call}}},
            State=#state{queries=Queries0, state = live}) ->
    case Call of
        {send, #cqerl_frame{stream_id=I}, _, _, _} ->
            case orddict:find(I, Queries0) of
                {ok, {UserCall, _}} ->
                    respond_to_user(UserCall, {error, Error});
                error ->
                    ok
            end,
            {noreply, State};

        {rows, _} ->
            {UserCall, _} = Query,
            respond_to_user(UserCall, {error, Error}),
            {noreply, State};

        {prepared, _rest} ->
            {noreply, State}
    end;

handle_info({prepared, CachedQuery=#cqerl_cached_query{key={_Node, Statement}}},
            State=#state{waiting_preparation=Waiting, state = live}) ->
    case orddict:find(Statement, Waiting) of
        {ok, Waiters} ->
            Waiting2 = orddict:erase(Statement, Waiting),
            NewState = lists:foldl(fun
                (Item, StateAcc=#state{available_slots=[], queued=Queue0}) ->
                    StateAcc#state{queued=queue:in(Item, Queue0)};
                ({Call, Item}, StateAcc) ->
                    process_outgoing_query(Call, {CachedQuery, Item}, StateAcc)
            end, State#state{waiting_preparation=Waiting2}, Waiters),
            {noreply, NewState};
        error ->
            {noreply, State}
    end;

handle_info({preparation_failed, {_Node, Statement}, Reason},
            State=#state{waiting_preparation=Waiting, state = live}) ->
    case orddict:find(Statement, Waiting) of
        {ok, Waiters} ->
            Waiting2 = orddict:erase(Statement, Waiting),
            lists:foreach(fun
                ({Call, _Item}) ->
                    respond_to_user(Call, {error, Reason})
            end, Waiters),
            {noreply, State#state{waiting_preparation=Waiting2}};
        error ->
            {noreply, State}
    end;

handle_info({ tcp_error, Socket, _Reason }, State = #state{socket = Socket}) ->
    {noreply, do_retry(State)};

handle_info({ tcp_closed, _Socket }, State = #state{state = starting}) ->
    {noreply, do_retry(State)};

handle_info({ tcp_error, _Socket }, State = #state{state = live}) ->
    {noreply, do_restart(State)};

handle_info({ tcp_closed, _Socket }, State = #state{state = live }) ->
    {noreply, do_restart(State)};

handle_info({ Transport, Socket, BinaryMsg }, State = #state{ socket=Socket, trans=Transport, delayed=Delayed0, state = starting}) ->
    {Result, Rest} = cqerl_protocol:response_frame(#cqerl_frame{}, << Delayed0/binary, BinaryMsg/binary >>),
    Resp = case Result of
        %% fragment
        incomplete ->
            {continue, State};

        %% Server tells us what version and compression algorithm it supports
        {ok, #cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, Payload} ->
            Compression = choose_compression_type(proplists:lookup('COMPRESSION', Payload)),
            SelectedVersion = choose_cql_version(proplists:lookup('CQL_VERSION', Payload)),
            {ok, StartupFrame} = cqerl_protocol:startup_frame(#cqerl_frame{}, #cqerl_startup_options{compression=Compression,
                                                                                                     cql_version=SelectedVersion}),
            send_to_db(State, StartupFrame),
            {continue, State#state{compression_type=Compression}};

        %% Server tells us all is clear, we can start to throw queries at it
        {ok, #cqerl_frame{opcode=?CQERL_OP_READY}, _} ->
            FinalState = maybe_do_setup_tasks(State),
            {continue, FinalState};

        %% Server tells us we need to authenticate
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTHENTICATE}, Body} ->
            #state{ authmod=AuthMod, authargs=AuthArgs, node=Node } = State,
            case AuthMod:auth_init(AuthArgs, Body, Node) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);

                {reply, Reply, AuthState} ->
                    {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
                    send_to_db(State, AuthFrame),
                    {continue, State#state{ authstate=AuthState }}
            end;

        %% Server tells us we need to give another piece of data
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTH_CHALLENGE}, Body} ->
            #state{ authmod=AuthMod, authstate=AuthState } = State,
            case AuthMod:auth_handle_challenge(Body, AuthState) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);
                {reply, Reply, AuthState} ->
                    {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
                    send_to_db(State, AuthFrame),
                    {continue, State#state{ authstate=AuthState }}
            end;

        %% Server tells us something screwed up while authenticating
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR}, {16#0100, AuthErrorDescription, _}} ->
            #state{ authmod=AuthMod, authstate=AuthState } = State,
            AuthMod:auth_handle_error(AuthErrorDescription, AuthState),
            stop_during_startup({auth_server_refused, AuthErrorDescription}, State);

        %% Server tells us something an error occured
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR}, {ErrorCode, ErrorMessage, _}} ->
            stop_during_startup({server_error, ErrorCode, ErrorMessage}, State);

        %% Server tells us the authentication went well, we can start shooting queries
        {ok, #cqerl_frame{opcode=?CQERL_OP_AUTH_SUCCESS}, Body} ->
            #state{ authmod=AuthMod, authstate=AuthState} = State,
            case AuthMod:auth_handle_success(Body, AuthState) of
                {close, Reason} ->
                    stop_during_startup({auth_client_closed, Reason}, State);

                ok ->
                    FinalState = maybe_do_setup_tasks(State),
                    {continue, FinalState }
            end;

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT}, {set_keyspace, _KeySpaceName}} ->
            {continue, switch_to_live_state(State)};

        {ok, _, unhandled} ->
            {continue, State}
    end,
    case Resp of
        {continue, State1} ->
            activate_socket(State1),
            {noreply, State1#state{delayed = Rest}};
        {stop, _, _} ->
            Resp
    end;


handle_info({ rows, Call, Result }, State) ->
    respond_to_user(Call, Result),
    {noreply, State};

handle_info({ Transport, Socket, BinaryMsg }, State = #state{ socket=Socket, trans=Transport, delayed=Delayed0, state = live}) ->
    {Result, Rest} = cqerl_protocol:response_frame(base_frame(State), << Delayed0/binary, BinaryMsg/binary >>),
    Resp = case Result of
        incomplete ->
            {stop, State};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {void, _}} ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, void);
                {ok, undefined} -> ok
            end,
            {continue, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {rows, RawMsg}} ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, undefined} -> ok;
                {ok, UserQuery} ->
                    cqerl_processor_sup:new_processor(UserQuery, {rows, RawMsg}, cqerl:get_protocol_version())
            end,
            {continue, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, ResponseTerm={set_keyspace, _KeySpaceName}} ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, ResponseTerm);
                {ok, undefined} -> ok
            end,
            {continue, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {prepared, RawMsg}} ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, {preparing, Query}} ->
                    cqerl_processor_sup:new_processor(Query, {prepared, RawMsg}, cqerl:get_protocol_version());
                {ok, undefined} -> ok
            end,
            {continue, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_RESULT, stream_id=StreamID}, {schema_change, ResponseTerm}} ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, {Call, _}} -> respond_to_user(Call, ResponseTerm);
                {ok, undefined} -> ok
            end,
            {continue, release_stream_id(StreamID, State)};

        %% Previously prepared query is absent from server's cache. We need to re-prepare and re-submit it:
        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR, stream_id=StreamID}, {16#2500, _ErrString, _QueryID}} when StreamID >= 0 ->
            NewState = release_stream_id(StreamID, State),
            FinalState = case orddict:find(StreamID, State#state.queries) of
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
            {continue, FinalState};

        {ok, #cqerl_frame{opcode=?CQERL_OP_ERROR, stream_id=StreamID}, ErrorTerm} when StreamID >= 0 ->
            case orddict:find(StreamID, State#state.queries) of
                {ok, {preparing, Query}} ->
                    cqerl_cache:query_preparation_failed(Query, ErrorTerm);
                {ok, {Call, _}} -> respond_to_user(Call, {error, ErrorTerm});
                {ok, undefined} -> ok
            end,
            {continue, release_stream_id(StreamID, State)};

        {ok, #cqerl_frame{opcode=?CQERL_OP_EVENT}, EventTerm} ->
            cqerl_schema:handle_event(EventTerm),
            {continue, State};

        {ok, _, unhandled} ->
            {continue, State}
    end,

    case Resp of
        {continue, State1} ->
            handle_info({Transport, Socket, Rest}, State1#state{delayed = <<>>});
        {stop, State1} ->
            activate_socket(State1),
            {noreply, State1#state{delayed = Rest}}
    end;

handle_info(_, State) ->
    {ok, State}.

terminate(Reason, #state{queries=Queries, state = live}) ->
    lists:foreach(fun
        ({_I, {#cql_call{type=sync, caller=From}, _}}) ->
            gen_server:reply(From, {error, Reason});
        ({_I, {#cql_call{type=async, caller={Pid, Tag}}, _}}) ->
            Pid ! {cql_error, Tag, Reason};
        ({_I, _}) -> ok
    end, Queries);

terminate(_Reason, _State) ->
    ok.



code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------




dequeue_query(State0=#state{queued=Queue0}) ->
    case queue:out(Queue0) of
        {{value, {Call, Batch=#cql_query_batch{}}}, Queue1} ->
            State1 = process_outgoing_query(Call, Batch, State0),
            {true, State1#state{queued=Queue1}};

        {{value, {prepare, Query}}, Queue1} when is_binary(Query) ->
            State1 = process_outgoing_query(prepare, Query, State0),
            {true, State1#state{queued=Queue1}};

        {{value, {Call, Item}}, Queue1} ->
            Query = case Item of
                #cql_query{} -> Item;
                #cql_result{cql_query=Q} -> Q
            end,
            CacheResult = cqerl_cache:lookup(Query),
            State1 = process_outgoing_query(Call, {CacheResult, Item}, State0),
            {true, State1#state{queued=Queue1}};

        {empty, _} ->
            {false, State0}
    end.

release_stream_id(StreamID, State=#state{available_slots=Slots, queries=Queries}) ->
    State2 = State#state{
        available_slots=[StreamID | Slots],
        queries=orddict:store(StreamID, undefined, Queries)
    },
    {_Dequeued, State3} = dequeue_query(State2),
    State3.


process_outgoing_query(prepare, Query, State=#state{queries=Queries0}) ->
    {BaseFrame, State1} = seq_frame(State),
    {ok, PrepareFrame} = cqerl_protocol:prepare_frame(BaseFrame, Query),
    send_to_db(State1, PrepareFrame),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {preparing, Query}, Queries0),
    State1#state{queries=Queries1};

process_outgoing_query(Call=#cql_call{}, Batch=#cql_query_batch{}, State=#state{queries=Queries0}) ->
    {BaseFrame, State1} = seq_frame(State),
    {ok, BatchFrame} = cqerl_protocol:batch_frame(BaseFrame, Batch),
    send_to_db(State1, BatchFrame),
    Queries1 = orddict:store(BaseFrame#cqerl_frame.stream_id, {Call, Batch}, Queries0),
    State1#state{queries=Queries1};

process_outgoing_query(Call,
                       {queued, Continuation=#cql_result{cql_query=#cql_query{statement=Statement}}},
                       State=#state{waiting_preparation=Waiting}) ->
    Waiting2 = case orddict:find(Statement, Waiting) of
        error -> orddict:store(Statement, [{Call, Continuation}], Waiting);
        _     -> orddict:append(Statement, {Call, Continuation}, Waiting)
    end,
    State#state{waiting_preparation=Waiting2};

process_outgoing_query(Call,
                       {queued, Query=#cql_query{statement=Statement}},
                       State=#state{waiting_preparation=Waiting}) ->
    Waiting2 = case orddict:find(Statement, Waiting) of
        error -> orddict:store(Statement, [{Call, Query}], Waiting);
        _     -> orddict:append(Statement, {Call, Query}, Waiting)
    end,
    State#state{waiting_preparation=Waiting2};

process_outgoing_query(Call,
                       {CachedResult, Item},
                       State=#state{queries=Queries}) ->

    {BaseFrame, State1} = seq_frame(State),
    I = BaseFrame#cqerl_frame.stream_id,
    {ColumnSpecs, SkipMetadata, Query, Values, Tracing} =
    case Item of
        Q = #cql_query{values=V, tracing=T} ->
            {undefined, false, Q, V, T};
        #cql_result{cql_query = Q = #cql_query{values=V, tracing=T}, columns = CS} ->
            {CS, true, Q, V, T}
    end,
    NewQueries = case CachedResult of
        uncached -> orddict:store(I, {Call, {Query, ColumnSpecs}}, Queries);
        #cqerl_cached_query{result_metadata=#cqerl_result_metadata{columns=CachedColumnSpecs}} ->
            orddict:store(I, {Call, {Query, CachedColumnSpecs}}, Queries)
    end,
    cqerl_processor_sup:new_processor(
        { State1#state.trans, State1#state.socket, CachedResult },
        { send, BaseFrame, Values, Query, SkipMetadata, Tracing },
        cqerl:get_protocol_version()
    ),
    State1#state{queries = NewQueries}.

respond_to_user(#cql_call{type=sync, caller=From}, Reply={error, _Term}) ->
    gen_server:reply(From, Reply);

respond_to_user(#cql_call{type=sync, caller=From}, Term) ->
    gen_server:reply(From, {ok, Term});

respond_to_user(#cql_call{type=async, caller={Pid, QueryRef}}, {error, Term}) ->
    Pid ! {error, QueryRef, Term};

respond_to_user(#cql_call{type=async, caller={Pid, QueryRef}}, Term) ->
    Pid ! {cqerl_result, QueryRef, Term}.


maybe_do_setup_tasks(State=#state{setup_tasks = []}) ->
    switch_to_live_state(State);
maybe_do_setup_tasks(State=#state{keyspace=Keyspace,
                                  setup_tasks = [set_keyspace | Rest]}) ->
    KeyspaceName = atom_to_binary(Keyspace, latin1),
    BaseFrame = base_frame(State),

    {ok, Frame} = cqerl_protocol:query_frame(BaseFrame,
        #cqerl_query_parameters{},
        #cqerl_query{statement = <<"USE ", KeyspaceName/binary>>}
    ),
    send_to_db(State, Frame),
    State#state{setup_tasks = Rest};
maybe_do_setup_tasks(State=#state{setup_tasks = [register | Rest]}) ->
    BaseFrame = base_frame(State),
    {ok, Frame} = cqerl_protocol:register_frame(BaseFrame,
                                                [?CQERL_EVENT_TOPOLOGY_CHANGE,
                                                 ?CQERL_EVENT_STATUS_CHANGE,
                                                 ?CQERL_EVENT_SCHEMA_CHANGE]),
    send_to_db(State, Frame),
    State#state{setup_tasks = Rest}.

switch_to_live_state(State=#state{group_name = GroupName, opts = Opts,
                                  node=Node, keyspace = Keyspace}) ->
    cqerl_client_pool:client_started(GroupName, Node, Keyspace, Opts),
    Queries = create_queries_dict(),
    State#state{
        authstate=undefined, authargs=undefined, delayed = <<>>,
        queries=Queries,
        available_slots = orddict:fetch_keys(Queries),
        state = live
    }.

send_to_db(#state{trans=tcp, socket=Socket}, Data) when is_binary(Data) ->
    ok = gen_tcp:send(Socket, Data);
send_to_db(#state{trans=ssl, socket=Socket}, Data) when is_binary(Data) ->
    ok = ssl:send(Socket, Data).

create_socket({Addr, Port}, Opts) ->
    BaseOpts = [{active, false}, {mode, binary}],
    {Transport, Result} =
    case proplists:get_value(ssl, Opts, false) of
        false ->
            TCPOpts = proplists:get_value(tcp_opts, Opts, []),
            {tcp, create_tcp_socket(Addr, Port, BaseOpts, TCPOpts)};
        SSLOpts ->
            {ssl, create_ssl_socket(Addr, Port, BaseOpts, SSLOpts)}
    end,
    case Result of
        {ok, Socket} -> {ok, Socket, Transport};
        Other -> Other
    end.

create_tcp_socket(Addr, Port, BaseOpts, TCPOpts) ->
    gen_tcp:connect(Addr, Port, BaseOpts ++ TCPOpts, 2000).

create_ssl_socket(Addr, Port, BaseOpts, true) ->
    create_ssl_socket(Addr, Port, BaseOpts, []);
create_ssl_socket(Addr, Port, BaseOpts, Opts) ->
    ssl:connect(Addr, Port, Opts ++ BaseOpts, 2000).

activate_socket(#state{socket=undefined}) ->
    ok;
activate_socket(#state{trans=ssl, socket=Socket}) ->
    ok = ssl:setopts(Socket, [{active, once}]);
activate_socket(#state{trans=tcp, socket=Socket}) ->
    ok = inet:setopts(Socket, [{active, once}]).




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
    GreaterVersion =
    case application:get_env(cqerl, preferred_cql_version, undefined) of
        undefined ->
            hd(SemVersions);
        Version1 ->
            hd(lists:dropwhile(fun (SemVersion) ->
                case semver:compare(SemVersion, Version1) of
                    1 -> true;
                    _ -> false
                end
            end, SemVersions))
    end,
    Version = tl(semver:vsn_string(GreaterVersion)),
    list_to_binary(Version).



base_frame(#state{compression_type=CompressionType}) ->
    #cqerl_frame{compression_type=CompressionType}.

seq_frame(State=#state{compression_type=CompressionType, available_slots=[Slot | Rest]}) ->
    {#cqerl_frame{compression_type=CompressionType, stream_id=Slot}, State#state{available_slots=Rest}}.


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

stop_during_startup(_Reason, State) ->
    {stop, normal, State#state{socket=undefined}}.

do_restart(State = #state{node = Node, keyspace = Keyspace, queries = Queries}) ->
    lists:foreach(fun({_, {Call, _}}) ->
                          respond_to_user(Call, {error, connection_failed});
                     ({_, undefined}) ->
                          ok
           end, Queries),
    cqerl_client_pool:remove_client(Node, Keyspace),
    do_retry(State).

do_retry(State) ->
    % TODO: Delay
    self() ! do_startup,
    State#state{state = starting}.

do_startup(State = #state{node = Node, opts = Opts}) ->
    case create_socket(Node, Opts) of
        {ok, Socket, Transport} ->
            NewState = State#state{socket = Socket, trans = Transport},
            handshake(NewState),
            activate_socket(NewState),
            NewState;

        {error, _Reason} ->
            do_retry(State)
    end.

handshake(State = #state{opts = Opts}) ->
    cqerl:put_protocol_version(proplists:get_value(
                                 protocol_version, Opts,
                                 cqerl:get_protocol_version())),
    {ok, OptionsFrame} = cqerl_protocol:options_frame(#cqerl_frame{}),
    send_to_db(State, OptionsFrame).

query_timeout() ->
    application:get_env(cqerl, query_timeout, ?DEFAULT_QUERY_TIMEOUT).
