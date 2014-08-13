-module(cqerl_batch).
-include("cqerl_protocol.hrl").

-export([start_link/3, init/4]).
-export([system_continue/3, system_terminate/4]).

start_link(Call, Inet, Batch=#cql_query_batch{}) ->
    proc_lib:start_link(?MODULE, init, [Call, Inet, Batch, self()]).

init(Call={ClientPid, _}, Inet, Batch=#cql_query_batch{queries=Queries0}, Parent) ->
    Debug = sys:debug_options([]),
    proc_lib:init_ack(Parent, {ok, self()}),
    Queries = lists:map(fun
        (Query=#cql_query{statement=Statement}) when is_list(Statement) ->
            Query#cql_query{statement=list_to_binary(Statement)};
        (Query) -> Query
    end, Queries0),
    QueryStates = lists:zip(
        Queries,
        lists:map(fun (Query) -> cqerl_cache:lookup(ClientPid, Query) end, Queries)
    ),
    loop(Call, Batch#cql_query_batch{queries=QueryStates}, Debug, Parent).

loop(Call, Batch=#cql_query_batch{queries=QueryStates}, Debug, Parent) ->
    case lists:all(fun ({_, queued}) -> false;
                       (_)           -> true end, QueryStates) of
        true ->
            terminate(Call, Batch);
        
        false ->
            receive
                {prepared, CachedQuery=#cqerl_cached_query{key={_Pid, Statement}}} ->
                    NewQueries = lists:map(fun
                        ({Query=#cql_query{statement=Statement1}, queued}) when Statement1 == Statement ->
                            {Query, CachedQuery};
                        (Other) -> Other
                    end, Batch#cql_query_batch.queries),
                    loop(Call, Batch#cql_query_batch{queries=NewQueries}, Debug, Parent);
                
                {preparation_failed, Reason} ->
                    cqerl_client:batch_failed(Call, Batch, Reason),
                    exit({failed, {Reason, Call, Batch}});
                
                {system, From, Request} ->
                    sys:handle_system_msg(Request, From, Parent, ?MODULE, Debug, {Call, Batch})
            end
    end.

terminate(Call, Batch) ->
    Queries = lists:map(fun
        ({Query = #cql_query{statement=Statement, values=Values}, uncached}) ->
            #cqerl_query{statement=Statement, kind=normal, 
                         values=cqerl_protocol:encode_query_values(Values, Query)};
                         
        ({Query = #cql_query{values=Values}, 
          #cqerl_cached_query{query_ref=Ref, params_metadata=Metadata}}) ->
            #cqerl_query{statement=Ref, kind=prepared,
                         values=cqerl_protocol:encode_query_values(Values, Query, Metadata#cqerl_result_metadata.columns)}
                         
    end, Batch#cql_query_batch.queries),
    cqerl_client:batch_ready(Call, Batch#cql_query_batch{queries=Queries}),
    exit(normal).

system_continue(Parent, Debug, {Call, Batch}) ->
    loop(Call, Batch, Debug, Parent).

system_terminate(Reason, _Parent, _Debug, _State) ->
    exit(Reason).
