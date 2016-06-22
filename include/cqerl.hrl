-ifndef(_CQERL_HRL_).
-define(_CQERL_HRL_, 1).

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

-define(CQERL_BATCH_LOGGED,   0).
-define(CQERL_BATCH_UNLOGGED, 1).
-define(CQERL_BATCH_COUNTER,  2).

-define(CQERL_EVENT_TOPOLOGY_CHANGE,  'TOPOLOGY_CHANGE').
-define(CQERL_EVENT_STATUS_CHANGE,    'STATUS_CHANGE').
-define(CQERL_EVENT_SCHEMA_CHANGE,    'SCHEMA_CHANGE').

-type event_type() :: ?CQERL_EVENT_TOPOLOGY_CHANGE |
                      ?CQERL_EVENT_STATUS_CHANGE |
                      ?CQERL_EVENT_SCHEMA_CHANGE.

-define(CQERL_TOPOLOGY_CHANGE_TYPE_NEW_NODE, 'NEW_NODE').
-define(CQERL_TOPOLOGY_CHANGE_TYPE_REMOVED_NODE, 'REMOVED_NODE').

-type topology_change() :: ?CQERL_TOPOLOGY_CHANGE_TYPE_NEW_NODE |
                           ?CQERL_TOPOLOGY_CHANGE_TYPE_REMOVED_NODE.

-define(CQERL_STATUS_CHANGE_TYPE_UP, 'UP').
-define(CQERL_STATUS_CHANGE_TYPE_DOWN, 'DOWN').

-type status_change() :: ?CQERL_STATUS_CHANGE_TYPE_UP |
                         ?CQERL_STATUS_CHANGE_TYPE_DOWN.

-define(CQERL_EVENT_CHANGE_TYPE_CREATED, 'CREATED').
-define(CQERL_EVENT_CHANGE_TYPE_DROPPED, 'DROPPED').
-define(CQERL_EVENT_CHANGE_TYPE_UPDATED, 'UPDATED').

-type schema_change() :: ?CQERL_EVENT_CHANGE_TYPE_CREATED |
                         ?CQERL_EVENT_CHANGE_TYPE_DROPPED |
                         ?CQERL_EVENT_CHANGE_TYPE_UPDATED.

-define(CQERL_EVENT_CHANGE_TARGET_KEYSPACE,  'KEYSPACE').
-define(CQERL_EVENT_CHANGE_TARGET_TABLE,     'TABLE').
-define(CQERL_EVENT_CHANGE_TARGET_TYPE,      'TYPE').
-define(CQERL_EVENT_CHANGE_TARGET_FUNCTION,  'FUNCTION').
-define(CQERL_EVENT_CHANGE_TARGET_AGGREGATE, 'AGGREGATE').

-type change_target() :: ?CQERL_EVENT_CHANGE_TARGET_KEYSPACE |
                         ?CQERL_EVENT_CHANGE_TARGET_TABLE |
                         ?CQERL_EVENT_CHANGE_TARGET_TYPE |
                         ?CQERL_EVENT_CHANGE_TARGET_FUNCTION |
                         ?CQERL_EVENT_CHANGE_TARGET_AGGREGATE.

-define(DEFAULT_PORT, 9042).

-define(DEFAULT_PROTOCOL_VERSION, 4).

-define(CQERL_PARSE_ADDR (Addr), case erlang:function_exported(inet, parse_address, 1) of
    true -> inet:parse_address(Addr);
    false -> inet_parse:address(Addr)
  end).

-type consistency_level_int() :: ?CQERL_CONSISTENCY_ANY .. ?CQERL_CONSISTENCY_EACH_QUORUM | ?CQERL_CONSISTENCY_LOCAL_ONE.
-type consistency_level() :: any | one | two | three | quorum | all | local_quorum | each_quorum | local_one.

-type serial_consistency_int() :: ?CQERL_CONSISTENCY_SERIAL | ?CQERL_CONSISTENCY_LOCAL_SERIAL.
-type serial_consistency() :: serial | local_serial.

-type batch_mode_int() :: ?CQERL_BATCH_LOGGED | ?CQERL_BATCH_UNLOGGED | ?CQERL_BATCH_COUNTER.
-type batch_mode() :: logged | unlogged | counter.

-type column_type() ::
  {custom, binary()} |
  {map, column_type(), column_type()} |
  {set | list, column_type()} | datatype().

-type datatype() :: ascii | bigint | blob | boolean | counter | decimal | double | 
                    float | int | timestamp | uuid | varchar | varint | timeuuid | inet.

-type parameter_val() :: number() | binary() | list() | atom() | boolean().
-type parameter() :: { datatype(), parameter_val() }.
-type named_parameter() :: { atom(), parameter_val() }.

-type group_name() :: term().
-type cqerl_node() :: {inet:ip_address() | inet:hostname(), inet:port_number()}.
-type host() :: cqerl_node() | inet:hostname().

-type keyspace() :: binary() | atom() | string().

-record(cql_query, {
    statement   = <<>>      :: iodata(),
    keyspace    = undefined :: keyspace(),
    values      = #{}       :: map(),

    reusable    = undefined :: undefined | boolean(),
    named       = false     :: boolean(),

    page_size   = 100       :: integer(),
    page_state              :: binary() | undefined,

    consistency = one :: consistency_level() | consistency_level_int(),
    serial_consistency = undefined :: serial_consistency() | serial_consistency_int() | undefined,

    value_encode_handler = undefined :: function() | undefined,
    tracing     = false     :: boolean()
}).

-record(cql_call, {
    type :: sync | async,
    caller :: {pid(), reference()},
    client :: reference()
}).

-record(cql_query_batch, {
    keyspace    = undefined :: keyspace(),
    consistency = one       :: consistency_level() | consistency_level_int(),
    mode        = logged    :: batch_mode() | batch_mode_int(),
    queries     = []        :: list(tuple())
}).

-record(cql_result, {
    columns         = []        :: list(tuple()),
    dataset         = []        :: list(list(term())),
    cql_query                   :: #cql_query{},
    client                      :: {pid(), reference()}
}).

-record(cql_schema_changed, {
    change_type :: created | updated | dropped,
    target      :: atom(),
    keyspace    :: keyspace(),
    name        :: binary(),
    args        :: [ binary() ]
}).

-type query_statement() :: binary() | string().
-type query() :: query_statement() | #cql_query{} | #cql_query_batch{}.
-type query_result() :: {ok, void | #cql_result{}} | {error, term()}.
-type async_query_result() :: {ok, reference()} | {error, term()}.

-endif. % _CQERL_HRL_
