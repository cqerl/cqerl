-module(cqerl_datatypes).

-include("cqerl_protocol.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

encode_bytes(Bytes) when is_binary(Bytes) ->
  Size = size(Bytes),
  {ok, << Size:?INT, Bytes/binary >>}.
  
  
  

%% @doc Encode a binary (max length 2^16) into the wire format required by the protocol

-spec encode_short_bytes(String :: binary()) -> {ok, bitstring()} | {error, badarg}.

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
  
  
-ifdef(TEST).

-define(CHAR_LENGTH, 1).
-define(SHORT_LENGTH, 2).
-define(INT_LENGTH, 4).

string_test() ->
  
  % Test that a list string produces the correct output
  LString1 = "Hello there",
  {ok, StringBinary} = encode_string(LString1),
  case length(LString1) + ?SHORT_LENGTH of
    Size when Size == size(StringBinary) -> ok
  end,
  
  % Test that decoding it produces the binary version of the same string
  {ok, BString1, <<>>} = decode_string(StringBinary),
  BString1 = list_to_binary(LString1),
  
  % Test that a binary string of cyrillic characters produces the correct output
  BString2 = <<"Юникод">>,
  {ok, StringBinary2} = encode_string(BString2),
  case size(BString2) + ?SHORT_LENGTH of
    Size2 when Size2 == size(StringBinary2) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, BString2, <<>>} = decode_string(StringBinary2).

long_string_test() -> 

  % Test that a list string produces the correct output
  LString1 = "Hello there",
  {ok, StringBinary} = encode_long_string(LString1),
  case length(LString1) + ?INT_LENGTH of
    Size when Size == size(StringBinary) -> ok
  end,
  
  % Test that decoding it produces the binary version of the same string
  {ok, BString1, <<>>} = decode_long_string(StringBinary),
  BString1 = list_to_binary(LString1),
  
  % Test that a binary string of cyrillic characters produces the correct output
  BString2 = <<"Юникод">>,
  {ok, StringBinary2} = encode_long_string(BString2),
  case size(BString2) + ?INT_LENGTH of
    Size2 when Size2 == size(StringBinary2) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, BString2, <<>>} = decode_long_string(StringBinary2).

bytes_test() ->
  % Test that encoding a proper byte sequence returns a properly encoded binary
  Bytes1 = crypto:rand_bytes(100000),
  {ok, EncodedBytes1} = encode_bytes(Bytes1),
  case size(Bytes1) + ?INT_LENGTH of
    Size2 when Size2 == size(EncodedBytes1) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, Bytes1, <<>>} = decode_bytes(EncodedBytes1).

short_bytes_test() ->
  
  % Test that encoding a proper byte sequence returns a properly encoded binary
  Bytes1 = crypto:rand_bytes(5000),
  {ok, EncodedBytes1} = encode_short_bytes(Bytes1),
  case size(Bytes1) + ?SHORT_LENGTH of
    Size2 when Size2 == size(EncodedBytes1) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, Bytes1, <<>>} = decode_short_bytes(EncodedBytes1).
  
string_list_test() ->
  % Test that encoding a proper string list returns a properly encoded binary
  StringList = ["Hello", <<", World! ">>, "How are ", <<"you?">>],
  {ok, EncodedStringList} = encode_string_list(StringList),
  CumulLength = lists:foldl(fun % Calculate the correct length of the output
    (String, Sum) when is_list(String) -> Sum + length(String) + ?SHORT_LENGTH;
    (Binary, Sum) when is_binary(Binary) -> Sum + size(Binary) + ?SHORT_LENGTH
  end, 0, StringList) + ?SHORT_LENGTH,
  CumulLength = size(EncodedStringList), % Assess the length
  
  % Verify correct reversibility
  AllBinariesList = lists:map(fun
    (String) when is_list(String) -> list_to_binary(String);
    (Other) -> Other
  end, StringList),
  {ok, AllBinariesList, <<>>} = decode_string_list(EncodedStringList).

map_test() ->
  % Test that encoding a proplist as a string map yields a properly encoded binary
  Proplist = [{foo, <<"bar">>}, {<<"Hello, ">>, <<"World!">>}, {"Another", <<"one">>}],
  {ok, EncodedMap} = encode_proplist_to_map(Proplist),
  CumulLength = lists:foldl(fun
    ({Key, Value}, Sum) when is_atom(Key) -> Sum + size(atom_to_binary(Key, latin1)) 
                                              + ?SHORT_LENGTH + size(Value) + ?SHORT_LENGTH;
    ({Key, Value}, Sum) when is_list(Key) -> Sum + size(list_to_binary(Key)) 
                                              + ?SHORT_LENGTH + size(Value) + ?SHORT_LENGTH;
    ({Key, Value}, Sum) when is_binary(Key) -> Sum + size(Key) + ?SHORT_LENGTH + size(Value) + ?SHORT_LENGTH
  end, 0, Proplist) + ?SHORT_LENGTH,
  CumulLength = size(EncodedMap),
  
  % Verify correct reversibility
  ReverseList = lists:map(fun
    ({String, Value}) when is_list(String) -> {list_to_atom(String), Value};
    ({String, Value}) when is_binary(String) -> {binary_to_atom(String, latin1), Value};
    (Other) -> Other
  end, Proplist),
  {ok, ReverseList, <<>>} = decode_map_to_proplist(EncodedMap).
  
multimap_test() ->
  % Test that encoding a proplist as a string multimap yields a properly encoded binary
  Proplist = [{foo, [<<"bar">>, <<"baz">>]}, 
              {<<"Hello, ">>, [<<"World!">>, "everyone", "dear"]}, 
              {"Another", [<<"one">>, "Dog"]}],
              
  {ok, EncodedMMap} = encode_proplist_to_multimap(Proplist),
  StringListLength = fun (StringList) ->
    lists:foldl(fun
      (String, Sum) when is_list(String) -> Sum + length(String) + ?SHORT_LENGTH;
      (Binary, Sum) when is_binary(Binary) -> Sum + size(Binary) + ?SHORT_LENGTH
    end, 0, StringList) + ?SHORT_LENGTH
  end,
  CumulLength = lists:foldl(fun
    ({Key, ValueList}, Sum) when is_atom(Key) -> Sum + size(atom_to_binary(Key, latin1)) 
                                              + ?SHORT_LENGTH + StringListLength(ValueList);
    ({Key, ValueList}, Sum) when is_list(Key) -> Sum + size(list_to_binary(Key)) 
                                              + ?SHORT_LENGTH + StringListLength(ValueList);
    ({Key, ValueList}, Sum) when is_binary(Key) -> Sum + size(Key) + ?SHORT_LENGTH + StringListLength(ValueList)
  end, 0, Proplist) + ?SHORT_LENGTH,
  CumulLength = size(EncodedMMap),
  
  % Verify correct reversibility
  StandardizeList = fun (StringList) ->
    lists:map(fun
      (String) when is_list(String) -> list_to_binary(String);
      (Other) -> Other
    end, StringList)
  end,
  ReverseList = lists:map(fun
    ({String, Value}) when is_list(String) -> {list_to_atom(String), StandardizeList(Value)};
    ({String, Value}) when is_binary(String) -> {binary_to_atom(String, latin1), StandardizeList(Value)};
    (Other) -> Other
  end, Proplist),
  
  {ok, ReverseList, <<>>} = decode_multimap_to_proplist(EncodedMMap).

-endif.