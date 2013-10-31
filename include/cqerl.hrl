-define(CQERL_CONSISTENCY_ANY,          0).
-define(CQERL_CONSISTENCY_ONE,          1).
-define(CQERL_CONSISTENCY_TWO,          2).
-define(CQERL_CONSISTENCY_THREE,        3).
-define(CQERL_CONSISTENCY_QUORUM,       4).
-define(CQERL_CONSISTENCY_ALL,          5).
-define(CQERL_CONSISTENCY_LOCAL_QUORUM, 6).
-define(CQERL_CONSISTENCY_EACH_QUORUM,  7).
-define(CQERL_CONSISTENCY_SERIAL,       8).
-define(CQERL_CONSISTENCY_LOCAL_SERIAL, 9).
-define(CQERL_CONSISTENCY_LOCAL_ONE,    10).

-type consistency_level() :: 0 .. 10.
-type column_type() :: {custom, binary()} | {map, column_type(), column_type()} | {set | list, column_type()} |
  ascii | bigint | blob | boolean | counter | decimal | double | 
  float | int | timestamp | uuid | varchat | varint | timeuuid | inet.

-record(cql_query, {
  query       = <<>> :: binary(),   
  reusable    = undefined :: undefined | boolean(),
  consistency = ?CQERL_CONSISTENCY_ANY :: consistency_level(),
  named       = false :: boolean(),
  bindings    = [] :: list(any())
}).

-record(cql_result, {
  
}).