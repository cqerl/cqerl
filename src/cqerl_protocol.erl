-module(cqerl_protocol).

-include("cqerl_protocol.hrl").

-define(DATA, cqerl_datatypes).
-define(CHAR,  8/big-integer).
-define(SHORT, 16/big-unsigned-integer).
-define(INT,   32/big-signed-integer).

-export([startup_frame/2, options_frame/1, auth_frame/2, prepare_frame/2, register_frame/2,
         query_frame/3, execute_frame/3, batch_frame/2,
         response_frame/2,
         decode_result_metadata/1,
         decode_result_matrix/4,
         decode_row/2,
         encode_query_values/2,
         encode_query_values/3]).

%% =======================
%% DATA ENCODING FUNCTIONS
%% =======================

%% @doc Encode request frames options for compression and tracing into a single byte integer.

-spec encode_frame_flags(Compression :: boolean(), Tracing :: boolean()) -> integer().

encode_frame_flags(true,     true) -> 3;
encode_frame_flags(false,    true) -> 2;
encode_frame_flags(true,     false) -> 1;
encode_frame_flags(_,    _) -> 0.




encode_query_valuelist([]) ->
    {ok, << 0:?SHORT >>};

encode_query_valuelist(Values) when is_list(Values) ->
    BytesSequence = << <<(element(2, ?DATA:encode_bytes(Value)))/binary>> || Value <- Values >>,
    ValuesLength = length(Values),
    {ok, << ValuesLength:?SHORT, BytesSequence/binary >>}.




encode_query_parameters(#cqerl_query_parameters{consistency=Consistency,
                                                skip_metadata=SkipMetadata,
                                                page_state=PageState,
                                                page_size=PageSize,
                                                serial_consistency=SerialConsistency}, Values) ->

    case Values of
        List when is_list(List), length(List) > 0 ->
            {ok, ValueBin} = encode_query_valuelist(Values),
            ValuesFlag = 1;
        _ ->
            ValueBin = <<>>,
            ValuesFlag = 0
    end,

    SkipMetadataFlag = case SkipMetadata of
        true -> 1;
        _ -> 0
    end,

    case PageSize of
        PageSize when is_integer(PageSize), PageSize > 0 ->
            PageSizeBin = << PageSize:?INT >>,
            PageSizeFlag = 1;
        _ ->
            PageSizeBin = <<>>,
            PageSizeFlag = 0
    end,

    case PageState of
        PageState when is_binary(PageState) ->
            {ok, PageStateBin} = ?DATA:encode_bytes(PageState),
            PageStateFlag = 1;
        _ ->
            PageStateBin = <<>>,
            PageStateFlag = 0
    end,

    case SerialConsistency of
        SerialConsistency when SerialConsistency == ?CQERL_CONSISTENCY_SERIAL ;
                                                     SerialConsistency == ?CQERL_CONSISTENCY_LOCAL_SERIAL ->
            SerialConsistencyBin = << SerialConsistency:?SHORT >>,
            SerialConsistencyFlag = 1;
        _ ->
            SerialConsistencyBin = <<>>,
            SerialConsistencyFlag = 0
    end,
    Flags = << 0:3, SerialConsistencyFlag:1, PageStateFlag:1, PageSizeFlag:1, SkipMetadataFlag:1, ValuesFlag:1 >>,
    {ok, iolist_to_binary([ << Consistency:?SHORT >>, Flags, ValueBin, PageSizeBin, PageStateBin, SerialConsistencyBin ])}.




encode_batch_queries([#cqerl_query{kind=Kind, statement=Statement, values=Values} | Rest], Acc) ->
    case Kind of
        prepared ->
            {ok, QueryBin} = ?DATA:encode_short_bytes(Statement),
            KindNum = 1;
        _ ->
            {ok, QueryBin} = ?DATA:encode_long_string(Statement),
            KindNum = 0
    end,
    {ok, ValueBin} = encode_query_valuelist(Values),
    encode_batch_queries(Rest, [[ << KindNum:?CHAR >>, QueryBin, ValueBin ] | Acc]);

encode_batch_queries([], Acc) ->
    Length = length(Acc),
    QueriesBin = iolist_to_binary(lists:reverse(Acc)),
    {ok, << Length:?SHORT , QueriesBin/binary >>}.





maybe_compress_body(false, _, Body) ->      {ok, Body};
maybe_compress_body(true, snappy, Body) ->  snappy:compress(Body);
maybe_compress_body(true, lz4, Body) ->     lz4:pack(Body).


maybe_decompress_body(false, _, Body) ->        {ok, Body};
maybe_decompress_body(true, snappy, Body) ->    snappy:decompress(Body);
maybe_decompress_body(true, lz4, Body) ->       lz4:unpack(Body).


%% =======================
%% DATA DECODING FUNCTIONS
%% =======================

decode_frame_flags(3) -> {true, true};
decode_frame_flags(2) -> {false, true};
decode_frame_flags(1) -> {true, false};
decode_frame_flags(0) -> {false, false}.




decode_flags(Flags, ListOfMasks) ->
    decode_flags(Flags, ListOfMasks, []).

decode_flags(_Flags, [], Acc) ->
    {ok, lists:reverse(Acc)};

decode_flags(Flags, [Mask | Rest], Acc) ->
    BitMask = 1 bsl Mask,
    FlagSet = case Flags band BitMask of
        0 -> false;
        _ -> true
    end,
    decode_flags(Flags, Rest, [FlagSet | Acc]).




decode_type(<< 0:?SHORT, Rest/binary >>) ->
    {ok, CustomType, Rest1} = ?DATA:decode_string(Rest),
    {ok, {custom, CustomType}, Rest1};

decode_type(<< 16#20:?SHORT, Rest/binary >>) ->
    {ok, Type, Rest1} = decode_type(Rest),
    {ok, {list, Type}, Rest1};

decode_type(<< 16#21:?SHORT, Rest/binary >>) ->
    {ok, KeyType, Rest1} = decode_type(Rest),
    {ok, ValueType, Rest2} = decode_type(Rest1),
    {ok, {map, KeyType, ValueType}, Rest2};

decode_type(<< 16#22:?SHORT, Rest/binary >>) ->
    {ok, Type, Rest1} = decode_type(Rest),
    {ok, {set, Type}, Rest1};

decode_type(<< Type:?SHORT, Rest/binary >>) when Type > 0, Type =< 16 ->
    TypeName = case Type of
        1 -> ascii;
        2 -> bigint;
        3 -> blob;
        4 -> boolean;
        5 -> counter;
        6 -> decimal;
        7 -> double;
        8 -> float;
        9 -> int;
        11 -> timestamp;
        12 -> uuid;
        13 -> varchar;
        14 -> varint;
        15 -> timeuuid;
        16 -> inet;
        _ -> unknown
    end,
    {ok, TypeName, Rest}.




decode_result_metadata(<< Flags:?INT, ColumnCount:?INT, Rest/binary >>) ->
    {ok, [GlobalTableSpec, HasMorePages, NoMetadata]} = decode_flags(Flags, [0, 1, 2]),

    case HasMorePages of
        true ->
            {ok, PageStateBin, Rest1} = ?DATA:decode_bytes(Rest);
        false ->
            Rest1 = Rest,
            PageStateBin = undefined
    end,

    case NoMetadata of
        true ->
            {ok, #cqerl_result_metadata{page_state=PageStateBin, columns_count=ColumnCount}, Rest1};

        false ->
            case GlobalTableSpec of
                true ->
                    {ok, KeySpaceName, Rest2} = ?DATA:decode_string(Rest1),
                    {ok, TableName, Rest3} = ?DATA:decode_string(Rest2),
                    GlobalSpec = {KeySpaceName, TableName};
                false ->
                    GlobalSpec = undefined,
                    Rest3 = Rest1
            end,
            {ok, Columns, Rest4} = decode_columns_metadata(GlobalSpec, Rest3, ColumnCount, []),
            {ok, #cqerl_result_metadata{page_state=PageStateBin,
                                                                    columns_count=ColumnCount,
                                                                    columns=Columns}, Rest4}
    end.

decode_columns_metadata(_GlobalSpec, Binary, 0, Acc) ->
    {ok, lists:reverse(Acc), Binary};

decode_columns_metadata(GlobalSpec, Binary, Remainder, Acc) when is_list(Acc), Remainder > 0 ->
    Record = case GlobalSpec of
        {KeySpaceName, TableName} ->
            Binary1 = Binary,
            #cqerl_result_column_spec{keyspace=KeySpaceName, table_name=TableName};
        undefined ->
            {ok, KeySpaceName, Binary0} = ?DATA:decode_string(Binary),
            {ok, TableName, Binary1} = ?DATA:decode_string(Binary0),
            #cqerl_result_column_spec{keyspace=KeySpaceName, table_name=TableName}
    end,
    {ok, NameBin, Binary2} = ?DATA:decode_string(Binary1),
    Name = binary_to_atom(NameBin, utf8),
    {ok, Type, Binary3} = decode_type(Binary2),
    decode_columns_metadata(GlobalSpec, Binary3, Remainder-1, [Record#cqerl_result_column_spec{name=Name, type=Type} | Acc]).




decode_result_matrix(0, _ColumnCount, Binary, Acc) ->
    {ok, lists:reverse(Acc), Binary};

decode_result_matrix(RowCount, ColumnCount, Binary, Acc) ->
    {ok, RowContent, Rest} = decode_result_row(ColumnCount, Binary, []),
    decode_result_matrix(RowCount-1, ColumnCount, Rest, [RowContent|Acc]).


decode_result_row(0, Binary, Acc) ->
    {ok, lists:reverse(Acc), Binary};

decode_result_row(ColumnCount, << NullSize:?INT, Rest/binary >>, Acc) when NullSize < 0 ->
    decode_result_row(ColumnCount-1, Rest, [ << NullSize:?INT >> | Acc]);

decode_result_row(ColumnCount, Binary, Acc) ->
    << Size:?INT, CellValueBin:Size/binary, Rest/binary >> = Binary,
    decode_result_row(ColumnCount-1, Rest, [ << Size:?INT, CellValueBin/binary >> | Acc]).





%% ================================
%% REQUEST FRAME ENCODING FUNCTIONS
%% ================================


request_frame(Frame) ->
    request_frame(Frame, <<>>).


request_frame(#cqerl_frame{tracing=Tracing,
                           compression=Compression,
                           compression_type=CompressionType,
                           stream_id=ID,
                           opcode=OpCode}, Body) when is_binary(Body) ->

    FrameFlags = encode_frame_flags(Compression, Tracing),
    {ok, MaybeCompressedBody} = maybe_compress_body(Compression, CompressionType, Body),
    Size = size(MaybeCompressedBody),
    {ok, iolist_to_binary([ << ?CQERL_FRAME_REQ:?CHAR, FrameFlags:?CHAR, ID:8/big-signed-integer, OpCode:?CHAR >>,
                            << Size:?INT >>,
                            MaybeCompressedBody ])}.




%% @doc Given frame and startup options, produce a 'STARTUP' request frame encoded in the protocol format.

-spec startup_frame(RequestFrame :: #cqerl_frame{}, StartupOptions :: #cqerl_startup_options{}) ->
    {ok, binary()} | {error, badarg}.

startup_frame(Frame, #cqerl_startup_options{cql_version=CQLVersion, compression=Compression}) ->
    {ok, Map} = ?DATA:encode_proplist_to_map([ {'CQL_VERSION', CQLVersion},
                                               {'COMPRESSION', Compression} ]),
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_STARTUP}, Map).




%% @doc Given frame options, produce a 'OPTIONS' request frame encoded in the protocol format.

-spec options_frame(RequestFrame :: #cqerl_frame{}) ->
    {ok, binary()} | {error, badarg}.

options_frame(Frame=#cqerl_frame{}) ->
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_OPTIONS}).




%% @doc Given frame options and authentication data, produce a 'AUTH_RESPONSE' request
%%            frame encoded in the protocol format.

-spec auth_frame(RequestFrame :: #cqerl_frame{}, AuthData :: binary()) ->
    {ok, binary()} | {error, badarg}.

auth_frame(Frame=#cqerl_frame{}, Data) when is_binary(Data) ->
    {ok, Bytes} = ?DATA:encode_bytes(Data),
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_AUTH_RESPONSE}, Bytes).




%% @doc Given frame options and a CQL query, produce a 'PREPARE' request
%%            frame encoded in the protocol format.

-spec prepare_frame(RequestFrame :: #cqerl_frame{}, CQLStatement :: binary()) ->
    {ok, binary()} | {error, badarg}.

prepare_frame(Frame, CQLStatement) when is_binary(CQLStatement) ->
    {ok, Payload} = ?DATA:encode_long_string(CQLStatement),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_PREPARE}, Payload).




%% @doc Given frame options and the list of events, produce a 'REGISTER' request
%%            frame encoded in the protocol format.

-spec register_frame(RequestFrame :: #cqerl_frame{}, EventList :: [atom() | {atom(), boolean()}]) ->
    {ok, binary()} | {error, badarg}.

register_frame(Frame=#cqerl_frame{}, EventList) when is_list(EventList) ->
    RegisteredEvents0 = [],
    RegisteredEvents1 = case proplists:get_value(topology_change, EventList, false) of
        true -> [?CQERL_EVENT_TOPOLOGY_CHANGE | RegisteredEvents0];
        _ ->        RegisteredEvents0
    end,
    RegisteredEvents2 = case proplists:get_value(status_change, EventList, false) of
        true -> [?CQERL_EVENT_STATUS_CHANGE | RegisteredEvents1];
        _ ->        RegisteredEvents1
    end,
    RegisteredEvents3 = case proplists:get_value(schema_change, EventList, false) of
        true -> [?CQERL_EVENT_SCHEMA_CHANGE | RegisteredEvents2];
        _ ->        RegisteredEvents2
    end,
    {ok, EventStringList} = ?DATA:encode_string_list(RegisteredEvents3),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_REGISTER}, EventStringList).




%% @doc Given frame options, query parameters and a query, produce a 'QUERY' request
%%            frame encoded in the protocol format.

-spec query_frame(RequestFrame :: #cqerl_frame{}, QueryParameters :: #cqerl_query_parameters{}, Query :: #cqerl_query{}) ->
    {ok, binary()} | {error, badarg}.

query_frame(Frame=#cqerl_frame{},
            QueryParameters=#cqerl_query_parameters{},
            #cqerl_query{values=Values, statement=Query, kind=normal}) ->

    {ok, QueryParametersBin} = encode_query_parameters(QueryParameters, Values),
    {ok, QueryBin} = ?DATA:encode_long_string(Query),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_QUERY},
                                << QueryBin/binary, QueryParametersBin/binary >>).




%% @doc Given frame options, query parameters and a query, produce a 'EXECUTE' request
%%            frame encoded in the protocol format.

-spec execute_frame(RequestFrame :: #cqerl_frame{}, QueryParameters :: #cqerl_query_parameters{}, Query :: #cqerl_query{}) ->
    {ok, binary()} | {error, badarg}.

execute_frame(Frame=#cqerl_frame{},
              QueryParameters=#cqerl_query_parameters{},
              #cqerl_query{values=Values, statement=QueryID, kind=prepared}) ->

    {ok, QueryParametersBin} = encode_query_parameters(QueryParameters, Values),
    {ok, QueryIDBin} = ?DATA:encode_short_bytes(QueryID),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_EXECUTE},
                                << QueryIDBin/binary, QueryParametersBin/binary >>).




%% @doc Given frame options and batch record (containing a set of queries), produce a
%%            'BATCH' request frame encoded in the protocol format.

-spec batch_frame(RequestFrame :: #cqerl_frame{}, BatchParameters :: #cql_query_batch{}) ->
    {ok, binary()} | {error, badarg}.

batch_frame(Frame=#cqerl_frame{}, #cql_query_batch{consistency=Consistency,
                                                   mode=Mode,
                                                   queries=Queries}) ->
    {ok, QueriesBin} = encode_batch_queries(Queries, []),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_BATCH},
                  << Mode:?CHAR, QueriesBin/binary, Consistency:?SHORT >>).





%% =================================
%% RESPONSE FRAME DECODING FUNCTIONS
%% =================================


%% @doc Decode a response frame coming from the server, expanding the response options and response term.

-spec response_frame(ResponseFrame :: #cqerl_frame{}, Response :: bitstring()) ->
    {ok, #cqerl_frame{}, any(), binary()} | {error, badarg}.

response_frame(_Response, Binary) when size(Binary) < 8 ->
    {delay, Binary};

response_frame(_Response, Binary = << _:4/binary, Size:?INT, Body/binary >>) when size(Body) < Size ->
    {delay, Binary};

response_frame(Response0=#cqerl_frame{compression_type=CompressionType},
               << ?CQERL_FRAME_RESP:?CHAR, FrameFlags:?CHAR, ID:8/big-signed-integer, OpCode:?CHAR, Size:?INT, Body0/binary >>)
                                     when is_binary(Body0), FrameFlags < 5 andalso FrameFlags >= 0 ->

    {Compression, Tracing} = decode_frame_flags(FrameFlags),
    Response = Response0#cqerl_frame{opcode=OpCode, stream_id=ID, compression=Compression, tracing=Tracing},
    << Body1:Size/binary, Rest/binary >> = Body0,
    {ok, UncompressedBody} = maybe_decompress_body(Compression, CompressionType, Body1),
    {ok, ResponseTerm} = decode_response_term(Response, UncompressedBody),
    {ok, Response, ResponseTerm, Rest};

response_frame(_, Binary) ->
    {delay, Binary}.


-spec decode_response_term(#cqerl_frame{}, binary()) -> {ok, any()} | {error, badarg}.

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_ERROR}, << ErrorCode:?INT, Body/binary >>) ->
    {ok, ErrorDescription, Rest} = ?DATA:decode_string(Body),
    case ErrorCode of
        _EmptyError when ErrorCode == 0;              % Server Error
                         ErrorCode == 16#000A;        % Protocol Error
                         ErrorCode == 16#0100;        % Bad credentials
                         ErrorCode >= 16#1001 andalso ErrorCode =< 16#1003;    % Overloaded, bootstrapping or truncate error
                         ErrorCode >= 16#2000 andalso ErrorCode =< 16#2300 ->  % Syntactically incorrect, unauthorized or incorrect query
            {ok, {ErrorCode, ErrorDescription, undefined}};

        16#1000 -> % Unavailability Exception
            << Availability:?SHORT, Required:?INT, Alive:?INT, _Rest/binary >> = Rest,
            {ok, {ErrorCode, ErrorDescription, {Availability, Required, Alive}}};

        16#1100 -> % Write Timeout Exception
            << Availability:?SHORT, Received:?INT, Required:?INT, Rest1/binary >> = Rest,
            {ok, WriteType, _Rest} = ?DATA:decode_string(Rest1),
            {ok, {ErrorCode, ErrorDescription, {Availability, Received, Required, WriteType}}};

        16#1200 -> % Read Timeout Exception
            << Availability:?SHORT, Received:?INT, Required:?INT, DataPresent:?CHAR, _Rest/binary >> = Rest,
            {ok, {ErrorCode, ErrorDescription, {Availability, Received, Required, DataPresent}}};

        16#2400 -> % Already Existing Key Space or Table
            {ok, KeySpace, Rest1} = ?DATA:decode_string(Rest),
            {ok, Table, _Rest} = ?DATA:decode_string(Rest1),
            ErrorData = case Table of
                <<>> -> {key_space, KeySpace};
                _ -> {table, KeySpace, Table}
            end,
            {ok, {ErrorCode, ErrorDescription, ErrorData}};

        16#2500 -> % Unprepared Query
            {ok, QueryID, _Rest} = ?DATA:decode_short_bytes(Rest),
            {ok, {ErrorCode, ErrorDescription, QueryID}}
    end;

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_READY}, _Body) ->
    {ok, undefined};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_AUTHENTICATE}, Body) ->
    {ok, String, _Rest} = ?DATA:decode_string(Body),
    {ok, String};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, Body) ->
    {ok, Proplist, _Rest} = ?DATA:decode_multimap_to_proplist(Body),
    {ok, Proplist};

%% Void result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 1:?INT, _Body/binary >>) ->
    {ok, {void, undefined}};

%% Rows result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 2:?INT, Body/binary >>) ->
    {ok, {rows, Body}};

%% Set_keyspace result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 3:?INT, Body/binary >>) ->
    {ok, KeySpaceName, _Rest} = ?DATA:decode_string(Body),
    {ok, {set_keyspace, KeySpaceName}};

%% Prepared result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 4:?INT, Body/binary >>) ->
    {ok, {prepared, Body}};

%% Schema_change result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 5:?INT, Body/binary >>) ->
    {ok, ChangeName, Rest1} = ?DATA:decode_string(Body),
    ChangeType = case binary_to_atom(ChangeName, latin1) of
        'CREATED' -> created;
        'DROPPED' -> dropped;
        'UPDATED' -> updated
    end,
    {ok, KeySpaceName, Rest2} = ?DATA:decode_string(Rest1),
    {ok, TableName, _Rest} = ?DATA:decode_string(Rest2),
    {ok, {schema_change, #cql_schema_changed{change_type=ChangeType,
                                             keyspace=KeySpaceName,
                                             table=TableName}
                                             }};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_EVENT}, Body) ->
    {ok, EventNameBin, Rest0} = ?DATA:decode_string(Body),
    EventName = binary_to_atom(EventNameBin, latin1),
    if
        EventName == ?CQERL_EVENT_TOPOLOGY_CHANGE orelse
        EventName == ?CQERL_EVENT_STATUS_CHANGE ->
            {ok, TopologyChangeName, Rest1} = ?DATA:decode_string(Rest0),
            TopologyChangeType = binary_to_atom(TopologyChangeName, latin1),
            {ok, ChangedNodeInet, _Rest} = ?DATA:decode_inet(Rest1),
            {ok, {EventName, {TopologyChangeType, ChangedNodeInet}}};

        EventName == ?CQERL_EVENT_SCHEMA_CHANGE ->
            {ok, SchemaChangeName, Rest1} = ?DATA:decode_string(Rest0),
            SchemaChangeType = binary_to_atom(SchemaChangeName, latin1),
            {ok, KeySpaceName, Rest2} = ?DATA:decode_string(Rest1),
            {ok, TableName, _Rest} = ?DATA:decode_string(Rest2),
            {ok, {EventName, {SchemaChangeType, KeySpaceName, TableName}}}
    end;

decode_response_term(#cqerl_frame{opcode=AuthCode}, Body) when AuthCode == ?CQERL_OP_AUTH_CHALLENGE;
                                                               AuthCode == ?CQERL_OP_AUTH_SUCCESS ->
    {ok, Bytes, _Rest} = ?DATA:decode_bytes(Body),
    {ok, Bytes}.



encode_query_values(Values, Query) ->
    [cqerl_datatypes:encode_data(Value, Query) || Value <- Values].

encode_query_values(Values, Query, []) ->
    encode_query_values(Values, Query);
encode_query_values(Values, Query, ColumnSpecs) ->
    lists:map(fun
        (#cqerl_result_column_spec{name=ColumnName, type=Type}) ->
            case proplists:get_value(ColumnName, Values) of
                undefined -> throw({missing_parameter, {parameter, ColumnName}, {in, Values}, {specs, ColumnSpecs}});
                Value -> cqerl_datatypes:encode_data({Type, Value}, Query)
            end
    end, ColumnSpecs).

decode_row(Row, ColumnSpecs) ->
    lists:map(fun
        ({<< Size:?INT, ValueBin/binary >>, #cqerl_result_column_spec{name=Name, type=Type}}) ->
            {Data, _Rest} = cqerl_datatypes:decode_data({Type, Size, ValueBin}),
            {Name, Data}
    end, lists:zip(Row, ColumnSpecs)).
