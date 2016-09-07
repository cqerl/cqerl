-module(cqerl_cache).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-include("cqerl_protocol.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, lookup/2, lookup/3, lookup_many/3, remove/2,
         query_was_prepared/3, query_preparation_failed/3]).

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
    queued = [] :: list(tuple())
}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec lookup(Node :: cqerl:cqerl_node(), Query :: #cql_query{}) ->
    queued | uncached | #cqerl_cached_query{}.

lookup(Node, Query) ->
    lookup(self(), Node, Query).

lookup(ClientPid, Node, #cql_query{reusable=true, statement=Statement}) ->
    case ets:lookup(?QUERIES_TAB, {Node, Statement}) of
        [] ->
            gen_server:cast(?SERVER,
                            {lookup, Statement, Node, ClientPid, self()}),
            queued;
        [CachedQuery] ->
            CachedQuery
    end;
lookup(ClientPid, Node, Query = #cql_query{named=true}) ->
    lookup(ClientPid, Node, Query#cql_query{reusable=true});
lookup(_ClientPid, _Node, #cql_query{reusable=false}) ->
    uncached;
lookup(ClientPid, Node, Query = #cql_query{statement=Statement}) ->
    RegEx = case get(?NAMED_BINDINGS_RE_KEY) of
        undefined ->
            {ok, RE} = re2:compile(?NAMED_BINDINGS_RE),
            put(?NAMED_BINDINGS_RE_KEY, RE),
            RE;
        RE -> RE
    end,
    case re2:match(Statement, RegEx) of
        nomatch ->
            lookup(ClientPid, Node,
                   Query#cql_query{reusable=false, named=false});

        %% In the case reusable is not set, and the query contains ? bindings,
        %% we make it reusable
        {match, [{_, 1}]} when Query#cql_query.reusable == undefined ->
            lookup(ClientPid, Node,
                   Query#cql_query{reusable=true, named=false});

        %% In the case the query contains :named bindings,
        %% we make it reusable no matter what
        {match, _} ->
            lookup(ClientPid, Node, Query#cql_query{reusable=true, named=true})
    end.


lookup_many(ClientPid, Node, Queries) ->
    { States, _ } = lists:foldr(fun
        (#cql_query{reusable=false}, { States0, Statements }) ->
            { [uncached | States0], Statements };

        (Query=#cql_query{statement=Statement}, { States0, Statements }) ->
            case orddict:find(Statement, Statements) of
                error ->
                    Value = lookup(ClientPid, Node, Query),
                    { [Value | States0],
                       orddict:store(Statement, Value, Statements)};

                {ok, Value} ->
                    { [Value | States0], Statements }
            end
    end, {[], orddict:new()}, Queries),
    States.

-spec remove(cqerl:cqerl_node(), #cql_query{} | [#cql_query{}]) -> ok.
remove(Node, Q = #cql_query{}) ->
    remove(Node, [Q]);
remove(Node, Queries) ->
    Statements = [S || #cql_query{statement = S} <- Queries],
    gen_server:call(?SERVER, {remove, Node, Statements}).

-spec query_was_prepared(cqerl:cqerl_node(), #cql_query{}, term()) -> ok.
query_was_prepared(Node, Query, Result) ->
    gen_server:cast(?SERVER, {query_prepared, {Node, Query}, Result}).

-spec query_preparation_failed(cqerl:cqerl_node(), #cql_query{}, term()) -> ok.
query_preparation_failed(Node, Query, Reason) ->
    gen_server:cast(?SERVER, {preparation_failed, {Node, Query}, Reason}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
    {ok,
     #state{
        queued=[],
        cached_queries = ets:new(?QUERIES_TAB,
                                 [set, named_table, protected,
                                  {read_concurrency, true},
                                  {keypos, #cqerl_cached_query.key}
                                 ])
    }}.

handle_call({remove, Node, Statements}, _From, State = #state{cached_queries = Cache}) ->
    lists:foreach(fun(Statement) ->
                          ets:delete(Cache, {Node, Statement})
                  end, Statements),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast({preparation_failed, Key, Reason}, State=#state{queued=Queue}) ->
    case orddict:find(Key, Queue) of
        {ok, Waiting} ->
            lists:foreach(fun (Client) ->
                                  Client ! {preparation_failed, Key, Reason}
                          end, Waiting),
            {noreply, State#state{queued=orddict:erase(Key, Queue)}};
        error ->
            {noreply, State}
    end;

handle_cast({query_prepared, Key,
             {QueryID, QueryMetadata, ResultMetadata}},
            State=#state{queued=Queue, cached_queries=Cache}) ->

    CachedQuery = #cqerl_cached_query{key=Key, query_ref=QueryID,
                                      params_metadata=QueryMetadata,
                                      result_metadata=ResultMetadata},

    case orddict:find(Key, Queue) of
        {ok, Waiting} ->
            lists:foreach(fun (Client) -> Client ! {prepared, CachedQuery} end, Waiting),
            ets:insert(Cache, CachedQuery),
            {noreply, State#state{queued=orddict:erase(Key, Queue)}};
        error ->
            {noreply, State}
    end;

handle_cast({lookup, Query, Node, ClientPid, Sender},
            State=#state{queued=Queue, cached_queries=Cache}) ->
    Key = {Node, Query},
    NewQueue = case orddict:find(Key, Queue) of
        {ok, _} ->
            orddict:append(Key, Sender, Queue);
        error ->
            case ets:lookup(Cache, Key) of
                [CachedQuery] ->
                    Sender ! {prepared, CachedQuery},
                    Queue;
                [] ->
                    cqerl_client:prepare_query(ClientPid, Query),
                    orddict:store(Key, [Sender], Queue)
            end
    end,
    {noreply, State#state{queued=NewQueue}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
