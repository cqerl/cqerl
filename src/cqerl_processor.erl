-module(cqerl_processor).
-include("cqerl_protocol.hrl").

-define(INT,   32/big-signed-integer).

-export([start_link/3, process/3]).

start_link(ClientPid, UserQuery, Msg) ->
    Pid = spawn_link(?MODULE, process, [ClientPid, UserQuery, Msg]),
    {ok, Pid}.

process(ClientPid, Query, { prepared, Msg }) ->
    {ok, QueryID, Rest0} = cqerl_datatypes:decode_short_bytes(Msg),
    {ok, QueryMetadata, Rest1} = cqerl_protocol:decode_result_metadata(Rest0),
    {ok, ResultMetadata, _Rest} = cqerl_protocol:decode_result_metadata(Rest1),
    cqerl_cache:query_was_prepared({ClientPid, Query}, {QueryID, QueryMetadata, ResultMetadata}),
    ok;

process(ClientPid, UserQuery, { rows, Msg }) ->
    {Call=#cql_call{client=ClientRef}, {Query, ColumnSpecs}} = UserQuery,
    {ok, Metadata, << RowsCount:?INT, Rest0/binary >>} = cqerl_protocol:decode_result_metadata(Msg),
    {ok, ResultSet, _Rest} = cqerl_protocol:decode_result_matrix(RowsCount, Metadata#cqerl_result_metadata.columns_count, Rest0, []),
    ResultMetadata = Metadata#cqerl_result_metadata{rows_count=RowsCount},
    Result = #cql_result{
        client = {ClientPid, ClientRef},
        cql_query = Query#cql_query{page_state = ResultMetadata#cqerl_result_metadata.page_state},
        columns = case ColumnSpecs of
            undefined   -> ResultMetadata#cqerl_result_metadata.columns;
            []          -> ResultMetadata#cqerl_result_metadata.columns;
            _           -> ColumnSpecs
        end,
        dataset = ResultSet
    },
    ClientPid ! {rows, Call, Result},
    ok.
