-module(cqerl_batch).
-include("cqerl_protocol.hrl").

-export([start_link/3]).

start_link(Call, Inet, Batch=#cql_query_batch{}) ->
    spawn_link(fun () -> init(Call, Inet, Batch) end).

init(Call={ClientPid, _}, Inet, Batch=#cql_query_batch{queries=Queries0}) ->
    Queries = lists:map(fun
        (Query=#cql_query{statement=Statement}) when is_list(Statement) ->
            Query#cql_query{statement=list_to_binary(Statement)};
        (Query) -> Query
    end, Queries0),
    QueryStates = lists:zip(
        Queries,
        lists:map(fun (Query) -> cqerl_cache:lookup(ClientPid, Inet, Query) end, Queries)
    ),
    loop(Call, Inet, Batch#cql_query_batch{queries=QueryStates}).

loop(Call, Inet, Batch=#cql_query_batch{queries=QueryStates}) ->
    case lists:all(fun ({_, queued}) -> false;
                       (_)           -> true end, QueryStates) of
        true -> 
            terminate(Call, Batch);
        false ->
            receive
                {prepared, CachedQuery=#cqerl_cached_query{key={Inet, Statement}}} ->
                    NewQueries = lists:map(fun
                        ({Query=#cql_query{statement=Statement1}, queued}) when Statement1 == Statement ->
                            {Query, CachedQuery};
                        (Other) -> Other
                    end, Batch#cql_query_batch.queries),
                    loop(Call, Inet, Batch#cql_query_batch{queries=NewQueries});
                
                {preparation_failed, Reason} ->
                    cqerl_client:batch_failed(Call, Batch, Reason)
            end
    end.

terminate(Call, Batch) ->
    Queries = lists:map(fun
        ({#cql_query{statement=Statement, values=Values}, uncached}) ->
            #cqerl_query{statement=Statement, kind=normal, 
                         values=cqerl_protocol:encode_query_values(Values)};
                         
        ({#cql_query{values=Values}, 
          #cqerl_cached_query{query_ref=Ref, params_metadata=Metadata}}) ->
            #cqerl_query{statement=Ref, kind=prepared,
                         values=cqerl_protocol:encode_query_values(Values, Metadata#cqerl_result_metadata.columns)}
                         
    end, Batch#cql_query_batch.queries),
    cqerl_client:batch_ready(Call, Batch#cql_query_batch{queries=Queries}).
