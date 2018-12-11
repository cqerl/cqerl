-module(cqerl_processor).
-include("cqerl_protocol.hrl").

-define(INT,   32/big-signed-integer).

-export([start_link/4, process/4]).

start_link(ClientPid, UserQuery, Msg, ProtocolVersion) ->
    Pid = spawn_link(?MODULE, process, [ClientPid, UserQuery, Msg, ProtocolVersion]),
    {ok, Pid}.

process(ClientPid, A, B, ProtocolVersion) ->
    cqerl:put_protocol_version(ProtocolVersion),
    try do_process(ClientPid, A, B) of
        Result -> Result
    catch
        ?EXCEPTION(ErrorClass, Error, Stacktrace) ->
            ClientPid ! {processor_threw, {{ErrorClass, {Error, ?GET_STACK(Stacktrace)}}, {A, B}}}
    end.

do_process(ClientPid, Query, { prepared, Msg }) ->
    {ok, QueryID, Rest0} = cqerl_datatypes:decode_short_bytes(Msg),
    {ok, QueryMetadata, Rest1} = cqerl_protocol:decode_prepared_metadata(Rest0),
    {ok, ResultMetadata, _Rest} = cqerl_protocol:decode_result_metadata(Rest1),
    cqerl_cache:query_was_prepared({ClientPid, Query}, {QueryID, QueryMetadata, ResultMetadata}),
    ok;

do_process(ClientPid, UserQuery, { rows, Msg }) ->
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
    ok;

do_process(_ClientPid, {Trans, Socket, CachedResult}, { send, BaseFrame, Values, Query, SkipMetadata }) ->
    {ok, Frame} = case CachedResult of
        uncached ->
            cqerl_protocol:query_frame(BaseFrame,
                #cqerl_query_parameters{
                    skip_metadata       = SkipMetadata,
                    consistency         = Query#cql_query.consistency,
                    page_state          = Query#cql_query.page_state,
                    page_size           = Query#cql_query.page_size,
                    serial_consistency  = Query#cql_query.serial_consistency
                },
                #cqerl_query{
                    kind    = normal,
                    statement = Query#cql_query.statement,
                    values  = cqerl_protocol:encode_query_values(Values, Query)
                }
            );

        #cqerl_cached_query{query_ref=Ref, result_metadata=#cqerl_result_metadata{columns=CachedColumnSpecs}, params_metadata=PMetadata} ->
            cqerl_protocol:execute_frame(BaseFrame,
                #cqerl_query_parameters{
                    skip_metadata       = length(CachedColumnSpecs) > 0,
                    consistency         = Query#cql_query.consistency,
                    page_state          = Query#cql_query.page_state,
                    page_size           = Query#cql_query.page_size,
                    serial_consistency  = Query#cql_query.serial_consistency
                },
                #cqerl_query{
                    kind    = prepared,
                    statement = Ref,
                    values  = cqerl_protocol:encode_query_values(Values, Query, PMetadata#cqerl_result_metadata.columns)
                }
            )
    end,
    case Trans of
        tcp -> gen_tcp:send(Socket, Frame);
        ssl -> ssl:send(Socket, Frame)
    end.
