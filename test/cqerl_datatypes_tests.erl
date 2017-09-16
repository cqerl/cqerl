-module(cqerl_datatypes_tests).

-include("cqerl_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(CHAR,  8/big-integer).
-define(SHORT, 16/big-integer).
-define(INT,   32/big-signed-integer).
-define(MAX_SHORT, 65535).

-define(CHAR_LENGTH, 1).
-define(SHORT_LENGTH, 2).
-define(INT_LENGTH, 4).

string_test() ->
  
  % Test that a list string produces the correct output
  LString1 = "Hello there",
  {ok, StringBinary} = cqerl_datatypes:encode_string(LString1),
  case length(LString1) + ?SHORT_LENGTH of
    Size when Size == size(StringBinary) -> ok
  end,
  
  % Test that decoding it produces the binary version of the same string
  {ok, BString1, <<>>} = cqerl_datatypes:decode_string(StringBinary),
  BString1 = list_to_binary(LString1),
  
  % Test that a binary string of cyrillic characters produces the correct output
  BString2 = <<"Юникод">>,
  {ok, StringBinary2} = cqerl_datatypes:encode_string(BString2),
  case size(BString2) + ?SHORT_LENGTH of
    Size2 when Size2 == size(StringBinary2) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, BString2, <<>>} = cqerl_datatypes:decode_string(StringBinary2).

long_string_test() -> 

  % Test that a list string produces the correct output
  LString1 = "Hello there",
  {ok, StringBinary} = cqerl_datatypes:encode_long_string(LString1),
  case length(LString1) + ?INT_LENGTH of
    Size when Size == size(StringBinary) -> ok
  end,
  
  % Test that decoding it produces the binary version of the same string
  {ok, BString1, <<>>} = cqerl_datatypes:decode_long_string(StringBinary),
  BString1 = list_to_binary(LString1),
  
  % Test that a binary string of cyrillic characters produces the correct output
  BString2 = <<"Юникод">>,
  {ok, StringBinary2} = cqerl_datatypes:encode_long_string(BString2),
  case size(BString2) + ?INT_LENGTH of
    Size2 when Size2 == size(StringBinary2) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, BString2, <<>>} = cqerl_datatypes:decode_long_string(StringBinary2).

bytes_test() ->
  % Test that encoding a proper byte sequence returns a properly encoded binary
  Bytes1 = crypto:strong_rand_bytes(100000),
  {ok, EncodedBytes1} = cqerl_datatypes:encode_bytes(Bytes1),
  case size(Bytes1) + ?INT_LENGTH of
    Size2 when Size2 == size(EncodedBytes1) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, Bytes1, <<>>} = cqerl_datatypes:decode_bytes(EncodedBytes1),
  
  % Negative size marker should yield undefined
  {ok, undefined, <<>>} = cqerl_datatypes:decode_bytes(<< 255, 255, 255, 255 >>).

short_bytes_test() ->
  
  % Test that encoding a proper byte sequence returns a properly encoded binary
  Bytes1 = crypto:strong_rand_bytes(5000),
  {ok, EncodedBytes1} = cqerl_datatypes:encode_short_bytes(Bytes1),
  case size(Bytes1) + ?SHORT_LENGTH of
    Size2 when Size2 == size(EncodedBytes1) -> ok
  end,
  
  % ... and that's its reverse is correct
  {ok, Bytes1, <<>>} = cqerl_datatypes:decode_short_bytes(EncodedBytes1).
  
string_list_test() ->
  % Test that encoding a proper string list returns a properly encoded binary
  StringList = ["Hello", <<", World! ">>, "How are ", <<"you?">>],
  {ok, EncodedStringList} = cqerl_datatypes:encode_string_list(StringList),
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
  {ok, AllBinariesList, <<>>} = cqerl_datatypes:decode_string_list(EncodedStringList).

map_test() ->
  % Test that encoding a proplist as a string map yields a properly encoded binary
  Proplist = [{foo, <<"bar">>}, {<<"Hello, ">>, <<"World!">>}, {"Another", <<"one">>}],
  {ok, EncodedMap} = cqerl_datatypes:encode_proplist_to_map(Proplist),
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
  {ok, ReverseList, <<>>} = cqerl_datatypes:decode_map_to_proplist(EncodedMap).
  
multimap_test() ->
  % Test that encoding a proplist as a string multimap yields a properly encoded binary
  Proplist = [{foo, [<<"bar">>, <<"baz">>]}, 
              {<<"Hello, ">>, [<<"World!">>, "everyone", "dear"]}, 
              {"Another", [<<"one">>, "Dog"]}],
              
  {ok, EncodedMMap} = cqerl_datatypes:encode_proplist_to_multimap(Proplist),
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
  
  {ok, ReverseList, <<>>} = cqerl_datatypes:decode_multimap_to_proplist(EncodedMMap).
