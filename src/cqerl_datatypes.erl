-module(cqerl_datatypes).

-include("cqerl_protocol.hrl").

-define(CHAR,  8/big-integer).
-define(SHORT, 16/big-integer).
-define(INT,   32/big-signed-integer).
-define(MAX_SHORT, 65535).

-export([encode_string/1,
         encode_long_string/1,
         encode_bytes/1,
         encode_short_bytes/1,
         encode_string_list/1,
         encode_proplist_to_map/1,
         encode_proplist_to_multimap/1,

         encode_data/2,
         decode_data/1,

         decode_string/1,
         decode_long_string/1,
         decode_bytes/1,
         decode_short_bytes/1,
         decode_string_list/1,
         decode_map_to_proplist/1,
         decode_multimap_to_proplist/1]).


%% @doc Encode a UTF8 binary or string (max length of 2^16) into the wire format required by the protocol

-spec encode_string(String :: string() | binary()) -> {ok, bitstring()} | {error, badarg}.

encode_string(String) when is_list(String) ->
    Binary = list_to_binary(String),
    encode_string(Binary);

encode_string(Binary) when is_binary(Binary), size(Binary) =< ?MAX_SHORT ->
    Size = size(Binary),
    {ok, << Size:?SHORT, Binary/binary >>}.




%% @doc Encode a long UTF8 binary or string (max length 2^32) into the wire format required by the protocol

-spec encode_long_string(String :: string() | binary()) -> {ok, bitstring()} | {error, badarg}.

encode_long_string(String) when is_list(String) ->
    Binary = list_to_binary(String),
    encode_long_string(Binary);

encode_long_string(Binary) when is_binary(Binary) ->
    Size = size(Binary),
    {ok, << Size:?INT, Binary/binary >>}.




%% @doc Encode a binary (max length 2^32) into the wire format required by the protocol

-spec encode_bytes(String :: binary()) -> {ok, bitstring()} | {error, badarg}.

encode_bytes(null) ->
    {ok, << 255, 255, 255, 255 >>};
encode_bytes(Bytes) when is_binary(Bytes) ->
    Size = size(Bytes),
    {ok, << Size:?INT, Bytes/binary >>}.




%% @doc Encode a binary (max length 2^16) into the wire format required by the protocol

-spec encode_short_bytes(String :: binary()) -> {ok, bitstring()} | {error, badarg}.

encode_short_bytes(null) ->
    {ok, << 0:?SHORT >> };
encode_short_bytes(Bytes) when is_binary(Bytes), size(Bytes) =< ?MAX_SHORT ->
    Size = size(Bytes),
    {ok, << Size:?SHORT, Bytes/binary >>}.




%% @doc Encode a string list into the wire format required by the protocol.

-spec encode_string_list(StringList :: [binary() | string()]) -> {ok, bitstring()} | {error, badarg}.

encode_string_list(StringList) when is_list(StringList) ->
    {ok, EncodedStringList} = encode_string_list(StringList, []),
    Length = length(StringList),
    Binary = iolist_to_binary(EncodedStringList),
    {ok, << Length:?SHORT, Binary/binary >>}.

encode_string_list([], Acc) ->
    {ok, lists:reverse(Acc)};
encode_string_list([String | Rest], Acc) when is_list(String); is_binary(String) ->
    {ok, EncodedString} = encode_string(String),
    encode_string_list(Rest, [ EncodedString | Acc ]).




%% @doc Encode a proplist into a string map (<code>[string] -> [string]</code>), in the wire format required by the protocol.

-spec encode_proplist_to_map(PropList :: [{atom() | binary(), binary()}]) -> {ok, bitstring()} | {error, badarg}.

encode_proplist_to_map(PropList) ->
    {ok, IOList} = encode_proplist_to_map(PropList, []),
    Binary = iolist_to_binary(IOList),
    Length = length(IOList),
    {ok, << Length:?SHORT, Binary/binary >>}.


encode_proplist_to_map([{Key, Value}|Rest], Acc) when is_binary(Value) ->
    KeyBin0 = case Key of
        Atom when is_atom(Atom) -> atom_to_binary(Atom, latin1);
        String when is_list(String) -> list_to_binary(String);
        String when is_binary(String) -> String
    end,
    {ok, KeyBin1} = encode_string(KeyBin0),
    {ok, ValueBin} = encode_string(Value),
    encode_proplist_to_map(Rest, [[KeyBin1, ValueBin] | Acc]);

encode_proplist_to_map([_|Rest], Acc) ->
    encode_proplist_to_map(Rest, Acc);

encode_proplist_to_map([], Acc) ->
    {ok, lists:reverse(Acc)}.




%% @doc Encode a proplist into a string multimap (<code>[string] -> { [string], [string], ... }</code>), in the wire format required by the protocol.

-spec encode_proplist_to_multimap(PropList :: [{atom() | binary(), [binary()]}]) -> {ok, bitstring()} | {error, badarg}.

encode_proplist_to_multimap(PropList) ->
    {ok, IOList} = encode_proplist_to_multimap(PropList, []),
    Binary = iolist_to_binary(IOList),
    Length = length(IOList),
    {ok, << Length:?SHORT, Binary/binary >>}.


encode_proplist_to_multimap([], Acc) ->
    {ok, lists:reverse(Acc)};

encode_proplist_to_multimap([{Key, Value}|Rest], Acc) when is_list(Value) ->
    KeyBin0 = case Key of
        Atom when is_atom(Atom) -> atom_to_binary(Atom, latin1);
        String when is_list(String) -> list_to_binary(String);
        String when is_binary(String) -> String
    end,
    {ok, KeyBin1} = encode_string(KeyBin0),
    {ok, ValueBin} = encode_string_list(Value),
    encode_proplist_to_multimap(Rest, [[KeyBin1, ValueBin] | Acc]);

encode_proplist_to_multimap([{Key, Value}|Rest], Acc) when is_binary(Value) ->
    encode_proplist_to_multimap([{Key, [Value]}|Rest], Acc);

encode_proplist_to_multimap([_|Rest], Acc) ->
    encode_proplist_to_multimap(Rest, Acc).




decode_string(<< Length:?SHORT, Rest/binary >>) when size(Rest) >= Length ->
    << String:Length/binary, Rest1/binary >> = Rest,
    {ok, String, Rest1};

decode_string(Bin = << Length:?SHORT, Rest/binary >>) when size(Rest) < Length ->
    {error, malformed_binary, Bin}.




decode_long_string(<< Length:?INT, Rest/binary >>) when size(Rest) >= Length ->
    << String:Length/binary, Rest1/binary >> = Rest,
    {ok, String, Rest1};

decode_long_string(Bin = << Length:?INT, Rest/binary >>) when size(Rest) < Length ->
    {error, malformed_binary, Bin}.



decode_bytes(<< NegativeLength:?INT, _Rest/binary >>) when NegativeLength < 0 ->
    {ok, undefined, <<>>};

decode_bytes(<< Length:?INT, Rest/binary >>) when size(Rest) >= Length ->
    << Bytes:Length/binary, Rest1/binary >> = Rest,
    {ok, Bytes, Rest1};

decode_bytes(Bin = << Length:?INT, Rest/binary >>) when size(Rest) < Length ->
    {error, malformed_binary, Bin}.




decode_short_bytes(<< Length:?SHORT, Rest/binary >>) when size(Rest) >= Length ->
    << Bytes:Length/binary, Rest1/binary >> = Rest,
    {ok, Bytes, Rest1};

decode_short_bytes(Bin = << Length:?SHORT, Rest/binary >>) when size(Rest) < Length ->
    {error, malformed_binary, Bin}.




decode_string_list(<< ListLength:?SHORT, Rest/binary >>) ->
    decode_string_list(Rest, ListLength, []).

decode_string_list(Binary, 0, Acc) when is_binary(Binary) ->
    {ok, lists:reverse(Acc), Binary};

decode_string_list(Binary, Num, Acc) when is_binary(Binary) ->
    {ok, String, Rest} = decode_string(Binary),
    decode_string_list(Rest, Num-1, [String|Acc]).




decode_map_to_proplist(<< MapLength:?SHORT, Rest/binary >>) ->
    decode_map_to_proplist(Rest, MapLength, []).

decode_map_to_proplist(Binary, 0, Acc) when is_binary(Binary) ->
    {ok, lists:reverse(Acc), Binary};

decode_map_to_proplist(Binary, Num, Acc) when is_binary(Binary) ->
    {ok, KeyString, Rest0} = decode_string(Binary),
    {ok, StringList, Rest1} = decode_string(Rest0),
    Key = binary_to_atom(KeyString, utf8),
    decode_map_to_proplist(Rest1, Num-1, [{Key, StringList} | Acc]).




decode_multimap_to_proplist(<< MapLength:?SHORT, Rest/binary >>) ->
    decode_multimap_to_proplist(Rest, MapLength, []).

decode_multimap_to_proplist(Binary, 0, Acc) when is_binary(Binary) ->
    {ok, lists:reverse(Acc), Binary};

decode_multimap_to_proplist(Binary, Num, Acc) when is_binary(Binary) ->
    {ok, KeyString, Rest0} = decode_string(Binary),
    {ok, StringList, Rest1} = decode_string_list(Rest0),
    Key = binary_to_atom(KeyString, utf8),
    decode_multimap_to_proplist(Rest1, Num-1, [{Key, StringList} | Acc]).



-spec encode_data({Type :: datatype(), Value :: term()}, Query :: #cql_query{}) -> binary().

encode_data({_Type, null}, _Query) ->
    null;

encode_data({timeuuid, now}, _Query) ->
    uuid:get_v1(uuid:new(self(), os));

encode_data({uuid, new}, _Query) ->
    uuid:get_v4(strong);
encode_data({uuid, strong}, _Query) ->
    uuid:get_v4(strong);
encode_data({uuid, weak}, _Query) ->
    uuid:get_v4(weak);

encode_data({UuidType, Uuid}, _Query) when UuidType == uuid orelse UuidType == timeuuid ->
    case Uuid of
        << _:128 >> -> Uuid
    end;

encode_data({ascii, Data}, _Query) when is_list(Data) ->
    case lists:all(fun
        (Int) when is_integer(Int) -> Int >= 0 andalso Int < 128;
        (_) -> false
    end, Data) of
        false -> throw(invalid_ascii);
        true -> list_to_binary(Data)
    end;

encode_data({ascii, Atom}, _Query) when is_atom(Atom) ->
    atom_to_binary(Atom, latin1);

encode_data({ascii, Data}, _Query) when is_binary(Data) ->
    Data;

encode_data({BigIntType, Number}, _Query) when is_integer(Number),
                                       BigIntType == bigint orelse
                                       BigIntType == counter orelse
                                       BigIntType == timestamp ->
    <<Number:64/big-signed-integer>>;

encode_data({BigIntType, Number}, _Query) when is_float(Number),
                                       BigIntType == bigint orelse
                                       BigIntType == counter orelse
                                       BigIntType == timestamp ->
    Int = trunc(Number),
    <<Int:64/big-signed-integer>>;

encode_data({blob, Data}, _Query) when is_binary(Data) ->
    Data;

encode_data({boolean, true}, _Query) ->
    <<1>>;
encode_data({boolean, false}, _Query) ->
    <<0>>;

%% Arbitrary precision decimal value, given as {UnscaledValue, Scale} tuple where
%% DecimalValue = UnscaledValue * 10^(-Scale)
%% - UnscaledValue being an integer or arbitrary-precision
%% - Scale being a 32-bit signed integer
%% e.g. 1.234e-3 == 1234e-6 is equivalent to {1234, -6} in the expected notation
encode_data({decimal, {UnscaledVal, Scale}}, _Query) ->
    EncodedUnscaledVal = encode_data({varint, UnscaledVal}, _Query),
    << Scale:?INT, EncodedUnscaledVal/binary >>;

encode_data({float, Val}, _Query) ->
    << Val:32/big-float >>;

encode_data({double, Val}, _Query) ->
    << Val:64/big-float >>;

encode_data({int, Val}, _Query) when is_integer(Val) ->
    << Val:32/big-signed-integer >>;

encode_data({int, Val}, _Query) when is_float(Val) ->
    Int = trunc(Val),
    << Int:32/big-signed-integer >>;

encode_data({TextType, Val}, _Query) when TextType == text orelse TextType == varchar ->
    if  is_binary(Val) -> Val;
        is_list(Val) -> list_to_binary(Val);
        is_atom(Val) -> atom_to_binary(Val, latin1)
    end;

encode_data({timestamp, now}, _Query) ->
    {MS, S, McS} = os:timestamp(),
    MlS = MS * 1000000000 + S * 1000 + trunc(McS/1000),
    encode_data({timestamp, MlS}, _Query);

encode_data({varint, Val}, _Query) when is_integer(Val) ->
    ByteCountF = math:log(Val) / math:log(2) / 8,
    ByteCount0 = trunc(ByteCountF),
    ByteCount = if  ByteCount0 == ByteCountF -> ByteCount0;
                    true -> ByteCount0 + 1
                end,
    << Val:ByteCount/big-signed-integer-unit:8 >>;

encode_data({inet, Addr}, _Query) when is_tuple(Addr) ->
    if
        tuple_size(Addr) == 4 -> %% IPv4
            {A, B, C, D} = Addr,
            << A:?CHAR, B:?CHAR, C:?CHAR, D:?CHAR >>;

        tuple_size(Addr) == 8 -> %% IPv6 (erlang way)
            {A, B, C, D, E, F, G, H} = Addr,
            << A:?SHORT, B:?SHORT, C:?SHORT, D:?SHORT,
               E:?SHORT, F:?SHORT, G:?SHORT, H:?SHORT >>;

        tuple_size(Addr) == 16 -> %% IPv6 (16 bytes)
            {A, B, C, D, E, F, G, H,
             I, J, K, L, M, N, O, P} = Addr,
            << A:?CHAR, B:?CHAR, C:?CHAR, D:?CHAR,
               E:?CHAR, F:?CHAR, G:?CHAR, H:?CHAR,
               I:?CHAR, J:?CHAR, K:?CHAR, L:?CHAR,
               M:?CHAR, N:?CHAR, O:?CHAR, P:?CHAR >>
    end;

encode_data({inet, Addr}, _Query) when is_list(Addr) ->
    {ok, AddrTuple} = ?CQERL_PARSE_ADDR(Addr),
    encode_data({inet, AddrTuple}, _Query);

encode_data({{ColType, Type}, List}, _Query) when ColType == list; ColType == set ->
    List2 = case ColType of
        list -> List;
        set -> ordsets:from_list(List)
    end,
    Length = length(List2),
    GetValueBinary = fun(Value) ->
        Bin = encode_data({Type, Value}, _Query),
        {ok, ShortBytes} = encode_short_bytes(Bin),
        ShortBytes
    end,
    Entries = << << (GetValueBinary(Value))/binary >> || Value <- List2 >>,
    << Length:?SHORT, Entries/binary >>;

encode_data({{map, KeyType, ValType}, List}, _Query) ->
    Length = length(List),
    GetElementBinary = fun(Type, Value) ->
        Bin = encode_data({Type, Value}, _Query),
        {ok, ShortBytes} = encode_short_bytes(Bin),
        ShortBytes
    end,
    Entries = << << (GetElementBinary(KeyType, Key))/binary,
                    (GetElementBinary(ValType, Value))/binary >> || {Key, Value} <- List >>,
    << Length:?SHORT, Entries/binary >>;

encode_data(Val, Query = #cql_query{ value_encode_handler = Handler }) when is_function(Handler) ->
    Handler(Val, Query);

encode_data({Type, _}, _Query) -> throw({bad_param_type, Type}).

-spec decode_data({Type :: datatype(), Buffer :: binary()}) -> {Value :: term(), Rest :: binary()}.

decode_data({_Type, NullSize, Bin}) when NullSize < 0 ->
    {null, Bin};

decode_data({UuidType, 16, Bin}) when UuidType == uuid orelse UuidType == timeuuid ->
    << Uuid:16/binary, Rest/binary >> = Bin,
    {Uuid, Rest};

decode_data({BigIntType, 8, Bin}) when BigIntType == bigint orelse
                                       BigIntType == counter orelse
                                       BigIntType == timestamp ->
    << Number:64/big-signed-integer, Rest/binary >> = Bin,
    {Number, Rest};

decode_data({int, 4, Bin}) ->
    << Number:32/big-signed-integer, Rest/binary >> = Bin,
    {Number, Rest};

decode_data({double, 8, Bin}) ->
    << Val:64/big-float, Rest/binary >> = Bin,
    {Val, Rest};

decode_data({float, 4, Bin}) ->
    << Val:32/big-float, Rest/binary >> = Bin,
    {Val, Rest};

decode_data({TextType, Size, Bin}) when TextType == ascii orelse
                                        TextType == varchar ->
    << Text:Size/binary, Rest/binary >> = Bin,
    {Text, Rest};

decode_data({blob, Size, Bin}) when Size < 0 ->
    {<<>>, Bin};

decode_data({blob, Size, Bin}) ->
    << Text:Size/binary, Rest/binary >> = Bin,
    {Text, Rest};

decode_data({boolean, 1, Bin}) ->
    << Bool:8, Rest/binary >> = Bin,
    {Bool /= 0, Rest};

decode_data({varint, Size, Bin}) ->
    << Number:Size/big-signed-integer-unit:8, Rest/binary >> = Bin,
    {Number, Rest};

decode_data({decimal, Size, Bin}) ->
    << Scale:?INT, Bin1/binary >> = Bin,
    IntSize = Size - 4,
    << Unscaled:IntSize/big-signed-integer-unit:8, Rest/binary >> = Bin1,
    {{Unscaled, Scale}, Rest};

decode_data({inet, 4, << Addr:4/binary, Rest/binary >>}) ->
    << A:?CHAR, B:?CHAR, C:?CHAR, D:?CHAR >> = Addr,
    {{A, B, C, D}, Rest};

decode_data({inet, 16, << Addr:16/binary, Rest/binary >>}) ->
    << A:?SHORT, B:?SHORT, C:?SHORT, D:?SHORT,
       E:?SHORT, F:?SHORT, G:?SHORT, H:?SHORT >> = Addr,
    {{A, B, C, D, E, F, G, H}, Rest};

decode_data({{ColType, ValueType}, Size, Bin}) when ColType == set; ColType == list ->
    << CollectionBin:Size/binary, Rest/binary>> = Bin,
    << _N:?SHORT, EntriesBin/binary >> = CollectionBin,
    List = [ element(1, decode_data({ValueType, ShortSize, ValueBin})) || << ShortSize:?SHORT, ValueBin:ShortSize/binary >> <= EntriesBin ],
    List2 = case ColType of
        set -> ordsets:from_list(List);
        list -> List
    end,
    {List2, Rest};

decode_data({{map, KeyType, ValueType}, Size, Bin}) ->
    << CollectionBin:Size/binary, Rest/binary>> = Bin,
    << _N:?SHORT, EntriesBin/binary >> = CollectionBin,
    List = [ { element(1, decode_data({KeyType, KShortSize, KeyBin})),
               element(1, decode_data({ValueType, VShortSize, ValueBin})) } ||
        << KShortSize:?SHORT, KeyBin:KShortSize/binary, VShortSize:?SHORT, ValueBin:VShortSize/binary >> <= EntriesBin ],
    {List, Rest};

decode_data({_, Size, << Size:?INT, Data/binary >>}) ->
    << Data:Size/binary, Rest/binary >> = Data,
    {{unknown_type, Data}, Rest}.
