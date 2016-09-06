-module(cqerl_cache).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("cqerl_protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, lookup/1, lookup/2, lookup_many/2, remove/1,
         query_was_prepared/2, query_preparation_failed/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(QUERIES_TAB, cqerl_cached_queries).
-define(NAMED_BINDINGS_RE_KEY, cqerl_cache_named_bindings_re).
-define(NAMED_BINDINGS_RE, "'*(\\?|:\\w+)'*(?:(?:[^\"]*\"[^\"]*\")*[^\"]*$)").

-record(state, {
    cached_queries :: ets:tid(),
    queued = [] :: list(tuple()),
    monitored_clients = [] :: list(pid())
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec lookup(Query :: #cql_query{}) -> queued | uncached | #cqerl_cached_query{}.

lookup(Query) ->
    lookup(self(), Query).

lookup(ClientPid, #cql_query{reusable=true, statement=Statement}) ->
    case ets:lookup(?QUERIES_TAB, {ClientPid, Statement}) of
        [] ->
            gen_server:cast(?SERVER, {lookup, Statement, ClientPid, self()}),
            queued;
        [CachedQuery] ->
            CachedQuery
    end;
lookup(ClientPid, Query = #cql_query{named=true}) ->
    lookup(ClientPid, Query#cql_query{reusable=true});
lookup(_ClientPid, #cql_query{reusable=false}) ->
    uncached;
lookup(ClientPid, Query = #cql_query{statement=Statement}) ->
    case get(?NAMED_BINDINGS_RE_KEY) of
        undefined ->
            {ok, RE} = re:compile(?NAMED_BINDINGS_RE),
            put(?NAMED_BINDINGS_RE_KEY, RE);
        RE -> ok
    end,
    case re:run(Statement, RE) of
        nomatch ->
            lookup(ClientPid, Query#cql_query{reusable=false, named=false});

        %% In the case reusable is not set, and the query contains ? bindings,
        %% we make it reusable
        {match, [{_, 1}]} when Query#cql_query.reusable == undefined ->
            lookup(ClientPid, Query#cql_query{reusable=true, named=false});

        %% In the case the query contains :named bindings,
        %% we make it reusable no matter what
        {match, _} ->
            lookup(ClientPid, Query#cql_query{reusable=true, named=true})
    end.


lookup_many(ClientPid, Queries) ->
    { States, _ } = lists:foldr(fun
        (#cql_query{reusable=false}, { States0, Statements }) ->
            { [uncached | States0], Statements };

        (Query=#cql_query{statement=Statement}, { States0, Statements }) ->
            case orddict:find(Statement, Statements) of
                error ->
                    Value = lookup(ClientPid, Query),
                    { [Value | States0],
                       orddict:store(Statement, Value, Statements)};

                {ok, Value} ->
                    { [Value | States0], Statements }
            end
    end, {[], orddict:new()}, Queries),
    States.

-spec remove(#cql_query{} | [#cql_query{}]) -> ok.
remove(Q = #cql_query{}) ->
    remove([Q]);
remove(Queries) ->
    Statements = [S || #cql_query{statement = S} <- Queries],
    gen_server:call(?SERVER, {remove, self(), Statements}).

query_was_prepared({Pid, _Query}=Key, Result) when is_pid(Pid) ->
    gen_server:cast(?SERVER, {query_prepared, Key, Result});

query_was_prepared(Query, Result) ->
    query_was_prepared({self(), Query}, Result).


query_preparation_failed(Query, Reason) ->
    gen_server:cast(?SERVER, {preparation_failed, {self(), Query}, Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    {ok, #state{
        queued=[],
        cached_queries=ets:new(?QUERIES_TAB, [set, named_table, protected,
                                              {read_concurrency, true},
                                              {keypos, #cqerl_cached_query.key}
                                              ])
    }}.

handle_call({remove, ClientPid, Statements}, _From, State = #state{cached_queries = Cache}) ->
    lists:foreach(fun(Statement) -> ets:delete(Cache, {ClientPid, Statement}) end, Statements),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({preparation_failed, Key, Reason}, State=#state{queued=Queue}) ->
    case orddict:find(Key, Queue) of
        {ok, Waiting} ->
            lists:foreach(fun (Client) -> Client ! {preparation_failed, Key, Reason} end, Waiting),
            {noreply, State#state{queued=orddict:erase(Key, Queue)}};
        error ->
            {noreply, State}
    end;

handle_cast({query_prepared, Key = {ClientPid, _Query},
             {QueryID, QueryMetadata, ResultMetadata}},
            State=#state{queued=Queue, cached_queries=Cache}) ->

    CachedQuery = #cqerl_cached_query{key=Key, query_ref=QueryID,
                                      params_metadata=QueryMetadata,
                                      result_metadata=ResultMetadata},

    case orddict:find(Key, Queue) of
        {ok, Waiting} ->
            NewState = maybe_monitor(ClientPid, State),
            lists:foreach(fun (Client) -> Client ! {prepared, CachedQuery} end, Waiting),
            ets:insert(Cache, CachedQuery),
            {noreply, NewState#state{queued=orddict:erase(Key, Queue)}};
        error ->
            {noreply, State}
    end;

handle_cast({lookup, Query, ClientPid, Sender}, State=#state{queued=Queue, cached_queries=Cache}) ->
    Key = {ClientPid, Query},
    case orddict:find(Key, Queue) of
        {ok, _} ->
            Queue2 = orddict:append(Key, Sender, Queue);
        error ->
            case ets:lookup(Cache, Key) of
                [CachedQuery] ->
                    Queue2 = Queue,
                    Sender ! {prepared, CachedQuery};
                [] ->
                    Queue2 = orddict:store(Key, [Sender], Queue),
                    cqerl_client:prepare_query(ClientPid, Query)
            end
    end,
    {noreply, State#state{queued=Queue2}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _},
            State=#state{cached_queries=Cache, monitored_clients = MonitoredClients}) ->
    ets:match_delete(Cache, #cqerl_cached_query{key = {Pid, '_'}, _='_'}),
    {noreply, State#state{monitored_clients = lists:delete(Pid, MonitoredClients)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

maybe_monitor(Pid, State = #state{monitored_clients = Monitored}) ->
    case lists:member(Pid, Monitored) of
        true ->
            State;
        false ->
            monitor(process, Pid),
            State#state{monitored_clients = [Pid | Monitored]}
    end.

