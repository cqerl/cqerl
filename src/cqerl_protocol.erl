-module(cqerl_protocol).

-include("cqerl_protocol.hrl").

-define(CHAR,  8/big-integer).
-define(SHORT, 16/big-unsigned-integer).
-define(INT,   32/big-signed-integer).

-export([startup_frame/2, options_frame/1, auth_frame/2, prepare_frame/2, register_frame/2,
         query_frame/3, execute_frame/3, batch_frame/2,
         response_frame/2,
         decode_result_metadata/1,
         decode_prepared_metadata/1,
         decode_result_matrix/4,
         decode_row/3,
         encode_query_values/2,
         encode_query_values/3]).

%% =======================
%% DATA ENCODING FUNCTIONS
%% =======================

%% @doc Encode request frames options for compression and tracing into a single byte integer.

-spec encode_frame_flags(Compression :: boolean(), Tracing :: boolean()) -> 0 .. 3.

encode_frame_flags(true,  true)  -> 3;
encode_frame_flags(false, true)  -> 2;
encode_frame_flags(true,  false) -> 1;
encode_frame_flags(_,     _)     -> 0.




encode_query_valuelist([]) ->
    << 0:?SHORT >>;

encode_query_valuelist(Values) when is_list(Values) ->
    BytesSequence = << <<(cqerl_datatypes:encode_bytes(Value))/binary>> || Value <- Values >>,
    ValuesLength = length(Values),
    << ValuesLength:?SHORT, BytesSequence/binary >>.


-spec encode_consistency_name(cqerl:consistency_level() | cqerl:consistency_level_int()) -> cqerl:consistency_level_int().
encode_consistency_name(Name) when is_integer(Name) -> Name;
encode_consistency_name(any)          -> ?CQERL_CONSISTENCY_ANY;
encode_consistency_name(one)          -> ?CQERL_CONSISTENCY_ONE;
encode_consistency_name(two)          -> ?CQERL_CONSISTENCY_TWO;
encode_consistency_name(three)        -> ?CQERL_CONSISTENCY_THREE;
encode_consistency_name(quorum)       -> ?CQERL_CONSISTENCY_QUORUM;
encode_consistency_name(all)          -> ?CQERL_CONSISTENCY_ALL;
encode_consistency_name(local_quorum) -> ?CQERL_CONSISTENCY_LOCAL_QUORUM;
encode_consistency_name(each_quorum)  -> ?CQERL_CONSISTENCY_EACH_QUORUM;
encode_consistency_name(local_one)    -> ?CQERL_CONSISTENCY_LOCAL_ONE.

-spec encode_serial_consistency_name(cqerl:serial_consistency() | cqerl:serial_consistency_int() | undefined) -> 0 | cqerl:serial_consistency_int().
encode_serial_consistency_name(Name) when is_integer(Name) -> Name;
encode_serial_consistency_name(undefined)    -> 0;
encode_serial_consistency_name(serial)       -> ?CQERL_CONSISTENCY_SERIAL;
encode_serial_consistency_name(local_serial) -> ?CQERL_CONSISTENCY_LOCAL_SERIAL.


encode_query_parameters(#cqerl_query_parameters{consistency=Consistency,
                                                skip_metadata=SkipMetadata,
                                                page_state=PageState,
                                                page_size=PageSize,
                                                serial_consistency=SerialConsistency}, Values) ->

    {ValueBin, ValuesFlag} =
    case Values of
        List when is_list(List), length(List) > 0 ->
            {encode_query_valuelist(Values), 1};
        _ ->
            {<<>>, 0}
    end,

    SkipMetadataFlag = case SkipMetadata of
        true -> 1;
        _ -> 0
    end,

    {PageSizeBin, PageSizeFlag} =
    case PageSize of
        PageSize when is_integer(PageSize), PageSize > 0 ->
            {<< PageSize:?INT >>, 1};
        _ ->
            {<<>>, 0}
    end,

    {PageStateBin, PageStateFlag} =
    case PageState of
        PageState when is_binary(PageState) ->
            {cqerl_datatypes:encode_bytes(PageState), 1};
        _ ->
            {<<>>, 0}
    end,

    SerialConsistencyInt = encode_serial_consistency_name(SerialConsistency),

    {SerialConsistencyBin, SerialConsistencyFlag} =
    case SerialConsistencyInt of
        SerialConsistencyInt when SerialConsistencyInt == ?CQERL_CONSISTENCY_SERIAL;
                                  SerialConsistencyInt == ?CQERL_CONSISTENCY_LOCAL_SERIAL ->
            {<< SerialConsistencyInt:?SHORT >>, 1};
        _ ->
            {<<>>, 0}
    end,

    ConsistencyInt = encode_consistency_name(Consistency),
    Flags = << 0:3, SerialConsistencyFlag:1, PageStateFlag:1, PageSizeFlag:1, SkipMetadataFlag:1, ValuesFlag:1 >>,
    {ok, iolist_to_binary([ << ConsistencyInt:?SHORT >>, Flags, ValueBin, PageSizeBin, PageStateBin, SerialConsistencyBin ])}.




encode_batch_queries([#cqerl_query{kind=Kind, statement=Statement, values=Values} | Rest], Acc) ->
    {QueryBin, KindNum} =
    case Kind of
        prepared ->
            {cqerl_datatypes:encode_short_bytes(Statement), 1};
        _ ->
            {cqerl_datatypes:encode_long_string(Statement), 0}
    end,
    ValueBin = encode_query_valuelist(Values),
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
    {ok, CustomType, Rest1} = cqerl_datatypes:decode_string(Rest),
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

decode_type(<< 16#30:?SHORT, Rest/binary >>) ->
    {ok, _KeyspaceName, Rest1} = cqerl_datatypes:decode_string(Rest),
    {ok, _TypeName, Rest2} = cqerl_datatypes:decode_string(Rest1),
    <<Size:?SHORT, Rest3/binary>> = Rest2,
    {ok, Types, Rest4} = decode_udt_type(Size, Rest3),
    {ok, {udt, Types}, Rest4};

decode_type(<< 16#31:?SHORT, Size:?SHORT, Rest/binary >>) ->
    {ok, Types, Rest1} = decode_tuple_type(Size, Rest),
    {ok, {tuple, Types}, Rest1};

decode_type(<< Type:?SHORT, Rest/binary >>) when Type > 0, Type =< 20 ->
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
        17 -> date;
        18 -> time;
        19 -> smallint;
        20 -> tinyint;
        _ -> unknown
    end,
    {ok, TypeName, Rest}.


decode_tuple_type(Size, Rest) ->
    {ok, Types, Rest1} = decode_tuple_type(Size, [], Rest),
    {ok, lists:reverse(Types), Rest1}.

decode_tuple_type(0, Acc, Rest) ->
    {ok, Acc, Rest};
decode_tuple_type(Size, Acc, Rest) ->
    {ok, Type, Rest1} = decode_type(Rest),
    decode_tuple_type(Size-1, [Type | Acc], Rest1).


decode_udt_type(Size, Rest) ->
    {ok, Types, Rest1} = decode_udt_type(Size, [], Rest),
    {ok, lists:reverse(Types), Rest1}.

decode_udt_type(0, Acc, Rest) ->
    {ok, Acc, Rest};
decode_udt_type(Size, Acc, Rest) ->
    {ok, ColName, Rest1} = cqerl_datatypes:decode_string(Rest),
    {ok, ColType, Rest2} = decode_type(Rest1),
    decode_udt_type(Size-1, [{ColName, ColType} | Acc], Rest2).


decode_prepared_metadata(<< Flags:?INT, ColumnCount:?INT, Rest/binary >>) ->
    {ok, [GlobalTableSpec]} = decode_flags(Flags, [0]),

    Rest1 = skip_primarykey_indices(Rest),

    {GlobalSpec, Rest2} = decode_global_spec(GlobalTableSpec, Rest1),

    {ok, Columns, Rest3} = decode_columns_metadata(GlobalSpec, Rest2, ColumnCount, []),
    {ok, #cqerl_result_metadata{columns_count=ColumnCount,
                                columns=Columns}, Rest3}.

skip_primarykey_indices(Data) ->
    case get(protocol_version) of
        3 ->
            Data;
        4 ->
            % TODO: Take the following into account.
            % Currently, we ignore the pk indices. We're not going eager routing yet.
            << PKCount:?INT, Rest0/binary >> = Data,
            {ok, _PKIndices, Rest1} = decode_primarykey_indices(Rest0, PKCount),
            Rest1
    end.

decode_global_spec(false, Data) ->
    {undefined, Data};
decode_global_spec(true, Data) ->
    {ok, KeySpaceName, Rest0} = cqerl_datatypes:decode_string(Data),
    {ok, TableName, Rest1} = cqerl_datatypes:decode_string(Rest0),
    {{KeySpaceName, TableName}, Rest1}.


decode_primarykey_indices(Binary, NumIndices) ->
    decode_primarykey_indices(Binary, NumIndices, []).

decode_primarykey_indices(Binary, 0, Acc) ->
    {ok, lists:reverse(Acc), Binary};
decode_primarykey_indices(<< PKIndex:?SHORT, Rest/binary >>, RemindingIndices, Acc) ->
    decode_primarykey_indices(Rest, RemindingIndices-1, [PKIndex | Acc]).

decode_result_metadata(<< Flags:?INT, ColumnCount:?INT, Rest/binary >>) ->
    {ok, [GlobalTableSpec, HasMorePages, NoMetadata]} = decode_flags(Flags, [0, 1, 2]),

    {PageStateBin, Rest1} = decode_page_state(HasMorePages, Rest),

    case NoMetadata of
        true ->
            {ok, #cqerl_result_metadata{page_state=PageStateBin, columns_count=ColumnCount}, Rest1};

        false ->
            {GlobalSpec, Rest2} = decode_global_spec(GlobalTableSpec, Rest1),
            {ok, Columns, Rest3} = decode_columns_metadata(GlobalSpec, Rest2, ColumnCount, []),
            {ok, #cqerl_result_metadata{page_state=PageStateBin,
                                        columns_count=ColumnCount,
                                        columns=Columns}, Rest3}
    end.

decode_page_state(false, Data) ->
    {undefined, Data};
decode_page_state(true, Data) ->
    {ok, PageStateBin, Rest} = cqerl_datatypes:decode_bytes(Data),
    {PageStateBin, Rest}.


decode_columns_metadata(_GlobalSpec, Binary, 0, Acc) ->
    {ok, lists:reverse(Acc), Binary};

decode_columns_metadata(GlobalSpec, Binary, Remainder, Acc) when is_list(Acc), Remainder > 0 ->
    {Binary1, Record} = case GlobalSpec of
        {KeySpaceName, TableName} ->
            {Binary, #cqerl_result_column_spec{keyspace=KeySpaceName, table_name=TableName}};
        undefined ->
            {ok, KeySpaceName, Bin0} = cqerl_datatypes:decode_string(Binary),
            {ok, TableName, Bin1} = cqerl_datatypes:decode_string(Bin0),
            {Bin1, #cqerl_result_column_spec{keyspace=KeySpaceName, table_name=TableName}}
    end,
    {ok, NameBin, Binary2} = cqerl_datatypes:decode_string(Binary1),
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
    Req = ?CQERL_FRAME_REQ + cqerl:get_protocol_version(),
    {ok, iolist_to_binary([ << Req:?CHAR, FrameFlags:?CHAR, ID:?SHORT, OpCode:?CHAR >>,
                            << Size:?INT >>,
                            MaybeCompressedBody ])}.




%% @doc Given frame and startup options, produce a 'STARTUP' request frame encoded in the protocol format.

-spec startup_frame(RequestFrame :: #cqerl_frame{}, StartupOptions :: #cqerl_startup_options{}) ->
    {ok, binary()}.

startup_frame(Frame, #cqerl_startup_options{cql_version=CQLVersion, compression=Compression}) ->
    Map = cqerl_datatypes:encode_proplist_to_map([ {'CQL_VERSION', CQLVersion},
                                                   {'COMPRESSION', Compression} ]),
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_STARTUP}, Map).




%% @doc Given frame options, produce a 'OPTIONS' request frame encoded in the protocol format.

-spec options_frame(RequestFrame :: #cqerl_frame{}) ->
    {ok, binary()}.

options_frame(Frame=#cqerl_frame{}) ->
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_OPTIONS}).




%% @doc Given frame options and authentication data, produce a 'AUTH_RESPONSE' request
%%            frame encoded in the protocol format.

-spec auth_frame(RequestFrame :: #cqerl_frame{}, AuthData :: binary()) ->
    {ok, binary()}.

auth_frame(Frame=#cqerl_frame{}, Data) when is_binary(Data) ->
    Bytes = cqerl_datatypes:encode_bytes(Data),
    request_frame(Frame#cqerl_frame{compression=false, opcode=?CQERL_OP_AUTH_RESPONSE}, Bytes).




%% @doc Given frame options and a CQL query, produce a 'PREPARE' request
%%            frame encoded in the protocol format.

-spec prepare_frame(RequestFrame :: #cqerl_frame{}, CQLStatement :: binary()) ->
    {ok, binary()}.

prepare_frame(Frame, CQLStatement) when is_binary(CQLStatement) ->
    Payload = cqerl_datatypes:encode_long_string(CQLStatement),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_PREPARE}, Payload).




%% @doc Given frame options and the list of events, produce a 'REGISTER' request
%%            frame encoded in the protocol format.

-spec register_frame(RequestFrame :: #cqerl_frame{}, EventList :: [cqerl:event_type()]) ->
    {ok, binary()}.

register_frame(Frame=#cqerl_frame{}, EventList) when is_list(EventList) ->
    EventStrings = [atom_to_list(E) || E <- EventList],
    EventStringList = cqerl_datatypes:encode_string_list(EventStrings),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_REGISTER}, EventStringList).


%% @doc Given frame options, query parameters and a query, produce a 'QUERY' request
%%            frame encoded in the protocol format.

-spec query_frame(RequestFrame :: #cqerl_frame{}, QueryParameters :: #cqerl_query_parameters{}, Query :: #cqerl_query{}) ->
    {ok, binary()}.

query_frame(Frame=#cqerl_frame{},
            QueryParameters=#cqerl_query_parameters{},
            #cqerl_query{values=Values, statement=Query, kind=normal, tracing=Tracing}) ->

    {ok, QueryParametersBin} = encode_query_parameters(QueryParameters, Values),
    QueryBin = cqerl_datatypes:encode_long_string(Query),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_QUERY, tracing=Tracing},
                                << QueryBin/binary, QueryParametersBin/binary >>).




%% @doc Given frame options, query parameters and a query, produce a 'EXECUTE' request
%%            frame encoded in the protocol format.

-spec execute_frame(RequestFrame :: #cqerl_frame{}, QueryParameters :: #cqerl_query_parameters{}, Query :: #cqerl_query{}) ->
    {ok, binary()}.

execute_frame(Frame=#cqerl_frame{},
              QueryParameters=#cqerl_query_parameters{},
              #cqerl_query{values=Values, statement=QueryID, kind=prepared, tracing=Tracing}) ->

    {ok, QueryParametersBin} = encode_query_parameters(QueryParameters, Values),
    QueryIDBin = cqerl_datatypes:encode_short_bytes(QueryID),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_EXECUTE, tracing=Tracing},
                                << QueryIDBin/binary, QueryParametersBin/binary >>).


-spec encode_mode_name(cqerl:batch_mode() | cqerl:batch_mode_int()) -> cqerl:batch_mode_int().
encode_mode_name(Mode) when is_integer(Mode) -> Mode;
encode_mode_name(logged)   -> ?CQERL_BATCH_LOGGED;
encode_mode_name(unlogged) -> ?CQERL_BATCH_UNLOGGED;
encode_mode_name(counter)  -> ?CQERL_BATCH_COUNTER.


%% @doc Given frame options and batch record (containing a set of queries), produce a
%%            'BATCH' request frame encoded in the protocol format.

-spec batch_frame(RequestFrame :: #cqerl_frame{}, BatchParameters :: #cql_query_batch{}) ->
    {ok, binary()}.

batch_frame(Frame=#cqerl_frame{}, #cql_query_batch{consistency=Consistency,
                                                   mode=Type,
                                                   queries=Queries}) ->
    {ok, QueriesBin} = encode_batch_queries(Queries, []),
    Flags = 0,
    TypeInt = encode_mode_name(Type),
    ConsistencyInt = encode_consistency_name(Consistency),
    request_frame(Frame#cqerl_frame{opcode=?CQERL_OP_BATCH},
                  << TypeInt:?CHAR, QueriesBin/binary, ConsistencyInt:?SHORT, Flags:?CHAR >>).





%% =================================
%% RESPONSE FRAME DECODING FUNCTIONS
%% =================================


%% @doc Decode a response frame coming from the server, expanding the response options and response term.

-spec response_frame(ResponseFrame :: #cqerl_frame{}, Response :: bitstring()) ->
    {{ok, #cqerl_frame{}, any()} | incomplete, binary()}.

response_frame(_Response, Binary) when size(Binary) < 9 ->
    {incomplete, Binary};

response_frame(_Response, Binary = << _:5/binary, Size:?INT, Body/binary >>) when size(Body) < Size ->
    {incomplete, Binary};

response_frame(Response0=#cqerl_frame{compression_type=CompressionType},
               << Resp:?CHAR, FrameFlags:?CHAR, ID:?SHORT, OpCode:?CHAR, Size:?INT, Body0/binary >>)
                                     when is_binary(Body0),
                                          Resp >= ?MIN_CQERL_FRAME_RESP,
                                          Resp =< ?MAX_CQERL_FRAME_RESP ->

    {ok, [Compression, Tracing, HasWarnings]} = decode_flags(FrameFlags, [0, 1, 3]),
    Response = Response0#cqerl_frame{opcode=OpCode, stream_id=ID, compression=Compression, tracing=Tracing},
    << Body1:Size/binary, Rest/binary >> = Body0,
    {ok, UncompressedBody} = maybe_decompress_body(Compression, CompressionType, Body1),
    Body2 = case HasWarnings of
        true ->
            {ok, Warnings, Body_} = cqerl_datatypes:decode_string_list(UncompressedBody),
            io:format("Warning from Cassandra: ~p~n", [Warnings]),
            Body_;
        false ->
            UncompressedBody
    end,
    {ok, ResponseTerm} = decode_response_term(Response, Body2),
    {{ok, Response, ResponseTerm}, Rest};

response_frame(_, Binary) ->
    {incomplete, Binary}.


decode_response_term(#cqerl_frame{opcode=?CQERL_OP_ERROR}, << ErrorCode:?INT, Body/binary >>) ->
    {ok, ErrorDescription, Rest} = cqerl_datatypes:decode_string(Body),
    Data = decode_error(ErrorCode, Rest),
    {ok, {ErrorCode, ErrorDescription, Data}};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_READY}, _Body) ->
    {ok, undefined};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_AUTHENTICATE}, Body) ->
    {ok, String, _Rest} = cqerl_datatypes:decode_string(Body),
    {ok, String};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, Body) ->
    {ok, Proplist, _Rest} = cqerl_datatypes:decode_multimap_to_proplist(Body),
    {ok, Proplist};

decode_response_term(Frame = #cqerl_frame{opcode=?CQERL_OP_RESULT, tracing=true},
                    <<_UUID:16/binary, Body/binary >>) ->
    decode_response_term(Frame#cqerl_frame{tracing=false}, Body);

%% Void result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 1:?INT, _Body/binary >>) ->
    {ok, {void, undefined}};

%% Rows result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 2:?INT, Body/binary >>) ->
    {ok, {rows, Body}};

%% Set_keyspace result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 3:?INT, Body/binary >>) ->
    {ok, KeySpaceName, _Rest} = cqerl_datatypes:decode_string(Body),
    {ok, {set_keyspace, KeySpaceName}};

%% Prepared result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 4:?INT, Body/binary >>) ->
    {ok, {prepared, Body}};

%% Schema_change result
decode_response_term(#cqerl_frame{opcode=?CQERL_OP_RESULT}, << 5:?INT, Body/binary >>) ->
    {ok, ChangeName, Rest1} = cqerl_datatypes:decode_string(Body),
    {ok, TargetString, Rest2} = cqerl_datatypes:decode_string(Rest1),
    SchemaChange0 = #cql_schema_changed{change_type=list_to_atom(string:to_lower(binary_to_list(ChangeName))),
                                        target=list_to_atom(string:to_lower(binary_to_list(TargetString)))},
    SchemaChange1 = if
        SchemaChange0#cql_schema_changed.target == keyspace ->
            {ok, KeyspaceName, _Rest} = cqerl_datatypes:decode_string(Rest2),
            SchemaChange0#cql_schema_changed{keyspace=KeyspaceName};

        SchemaChange0#cql_schema_changed.target == table orelse
        SchemaChange0#cql_schema_changed.target == type ->
            {ok, KeyspaceName, Rest3} = cqerl_datatypes:decode_string(Rest2),
            {ok, EntityName, _Rest} = cqerl_datatypes:decode_string(Rest3),
            SchemaChange0#cql_schema_changed{keyspace=KeyspaceName, name=EntityName};

        SchemaChange0#cql_schema_changed.target == function orelse
        SchemaChange0#cql_schema_changed.target == aggregate ->
            {ok, KeyspaceName, Rest3} = cqerl_datatypes:decode_string(Rest2),
            {ok, EntityName, Rest4} = cqerl_datatypes:decode_string(Rest3),
            {ok, Args, _Rest} = cqerl_datatypes:decode_string_list(Rest4),
            SchemaChange0#cql_schema_changed{keyspace=KeyspaceName, name=EntityName, args=Args}
    end,

    {ok, {schema_change, SchemaChange1}};

decode_response_term(#cqerl_frame{opcode=?CQERL_OP_EVENT}, Body) ->
    {ok, EventName, Rest0} = cqerl_datatypes:decode_string(Body),
    decode_event(binary_to_atom(EventName, utf8), Rest0);

decode_response_term(#cqerl_frame{opcode=AuthCode}, Body) when AuthCode == ?CQERL_OP_AUTH_CHALLENGE;
                                                               AuthCode == ?CQERL_OP_AUTH_SUCCESS ->
    {ok, Bytes, _Rest} = cqerl_datatypes:decode_bytes(Body),
    {ok, Bytes}.

decode_event(?CQERL_EVENT_TOPOLOGY_CHANGE, Data) ->
    {ok, Type, Rest} = cqerl_datatypes:decode_string(Data),
    {ok, Node, <<>>} = cqerl_datatypes:decode_inet(Rest),
    {ok, #topology_change{type = binary_to_atom(Type, utf8),
                          node = Node}};

decode_event(?CQERL_EVENT_STATUS_CHANGE, Data) ->
    {ok, Type, Rest} = cqerl_datatypes:decode_string(Data),
    {ok, Node, <<>>} = cqerl_datatypes:decode_inet(Rest),
    {ok, #status_change{type = binary_to_atom(Type, utf8),
                        node = Node}};

decode_event(?CQERL_EVENT_SCHEMA_CHANGE, Data) ->
    {ok, Type, Rest1} = cqerl_datatypes:decode_string(Data),
    {ok, Target, Rest2} = cqerl_datatypes:decode_string(Rest1),
    decode_schema_change_options(binary_to_atom(Type, utf8),
                                 binary_to_atom(Target, utf8),
                                 Rest2).

decode_schema_change_options(Type, ?CQERL_EVENT_CHANGE_TARGET_KEYSPACE, Data) ->
    {ok, Keyspace, <<>>} = cqerl_datatypes:decode_string(Data),
    {ok, #keyspace_change{type = Type,
                          keyspace = cqerl:normalise_keyspace(Keyspace)}};

decode_schema_change_options(Type, ?CQERL_EVENT_CHANGE_TARGET_TABLE, Data) ->
    {ok, Keyspace, Rest} = cqerl_datatypes:decode_string(Data),
    {ok, Table, <<>>} = cqerl_datatypes:decode_string(Rest),
    {ok, #table_change{type = Type,
                       keyspace = cqerl:normalise_keyspace(Keyspace),
                       table = Table}};

decode_schema_change_options(_Type, ?CQERL_EVENT_CHANGE_TARGET_FUNCTION, _Data) ->
    {ok, unhandled};
decode_schema_change_options(_Type, ?CQERL_EVENT_CHANGE_TARGET_AGGREGATE, _Data) ->
    {ok, unhandled}.

encode_query_values(Values, Query) when is_list(Values) ->
    [cqerl_datatypes:encode_data(Value, Query) || Value <- Values];
encode_query_values(ValueMap, Query) ->
    encode_query_values(maps:to_list(ValueMap), Query).

encode_query_values(Values, Query, []) ->
    encode_query_values(Values, Query);
encode_query_values(Values, Query, ColumnSpecs) when is_list(Values) ->
    lists:map(fun
        (#cqerl_result_column_spec{name=ColumnName, type=Type}) ->
            case proplists:get_value(ColumnName, Values, not_found) of
                not_found ->
                    throw({missing_parameter, {parameter, ColumnName}, {in, Values}, {specs, ColumnSpecs}});
                undefined ->
                    cqerl_datatypes:encode_data({Type, null}, Query);
                Value ->
                    cqerl_datatypes:encode_data({Type, Value}, Query)
            end
    end, ColumnSpecs);

encode_query_values(ValueMap, Query, ColumnSpecs) ->
    encode_query_values(maps:to_list(ValueMap), Query, ColumnSpecs).

decode_row(Row, ColumnSpecs, Opts) ->
    maps:from_list(lists:map(fun
        ({<< Size:?INT, ValueBin/binary >>, #cqerl_result_column_spec{name=Name, type=Type}}) ->
            {Data, _Rest} = cqerl_datatypes:decode_data({Type, Size, ValueBin}, Opts),
            {Name, Data}
    end, lists:zip(Row, ColumnSpecs))).

% Errors with no additional data
decode_error(ErrorCode, _Body)
  when ErrorCode =:= ?CQERL_ERROR_SERVER;
       ErrorCode =:= ?CQERL_ERROR_PROTOCOL;
       ErrorCode =:= ?CQERL_ERROR_AUTH;
       ErrorCode =:= ?CQERL_ERROR_PROTOCOL;
       ErrorCode =:= ?CQERL_ERROR_OVERLOADED;
       ErrorCode =:= ?CQERL_ERROR_BOOTSTRAPPING;
       ErrorCode =:= ?CQERL_ERROR_TRUNCATE;
       ErrorCode =:= ?CQERL_ERROR_SYNTAX;
       ErrorCode =:= ?CQERL_ERROR_UNAUTHORIZED;
       ErrorCode =:= ?CQERL_ERROR_INVALID;
       ErrorCode =:= ?CQERL_ERROR_CONFIG ->
    undefined;

decode_error(?CQERL_ERROR_UNAVAILABLE, Body) ->
    << Availability:?SHORT, Required:?INT, Alive:?INT, _Rest/binary >> = Body,
    {Availability, Required, Alive};

decode_error(?CQERL_ERROR_WTIMEOUT, Body) ->
    << Consistency:?SHORT, Received:?INT, BlockFor:?INT, Rest/binary >> = Body,
    {ok, WriteType, _Rest} = cqerl_datatypes:decode_string(Rest),
    {Consistency, Received, BlockFor, WriteType};

decode_error(?CQERL_ERROR_RTIMEOUT, Body) ->
    << Consistency:?SHORT, Received:?INT, BlockFor:?INT,
       DataPresent:?CHAR, _Rest/binary >> = Body,
    {Consistency, Received, BlockFor, DataPresent};

decode_error(?CQERL_ERROR_RFAILURE, Body) ->
    << Consistency:?SHORT, Received:?INT, BlockFor:?INT,
       NumFailures:?INT, DataPresent:?CHAR, _Rest/binary >> = Body,
    {Consistency, Received, BlockFor, NumFailures, DataPresent};

decode_error(?CQERL_ERROR_FUN_FAILURE, Body) ->
    {ok, Keyspace, Rest1} = cqerl_datatypes:decode_string(Body),
    {ok, Function, Rest2} = cqerl_datatypes:decode_string(Rest1),
    {ok, ArgTypes, _Rest} = cqerl_datatypes:decode_string_list(Rest2),
    {Keyspace, Function, ArgTypes};

decode_error(?CQERL_ERROR_WFAILURE, Body) ->
    << Consistency:?SHORT, Received:?INT, BlockFor:?INT,
       NumFailures:?INT, Rest/binary >> = Body,
    {ok, WriteType, _Rest} = cqerl_datatypes:decode_string(Rest),
    {Consistency, Received, BlockFor, NumFailures, WriteType};

decode_error(?CQERL_ERROR_ALREADY_EXISTS, Body) ->
    {ok, KeySpace, Rest} = cqerl_datatypes:decode_string(Body),
    {ok, Table, _Rest} = cqerl_datatypes:decode_string(Rest),
    case Table of
        <<>> -> {key_space, KeySpace};
        _ -> {table, KeySpace, Table}
    end;

decode_error(?CQERL_ERROR_UNPREPARED, Body) ->
    {ok, QueryID, _Rest} = cqerl_datatypes:decode_short_bytes(Body),
    QueryID.

