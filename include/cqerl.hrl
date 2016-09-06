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

-define(CQERL_TOPOLOGY_CHANGE_TYPE_NEW_NODE, 'NEW_NODE').
-define(CQERL_TOPOLOGY_CHANGE_TYPE_REMOVED_NODE, 'REMOVED_NODE').

-define(CQERL_STATUS_CHANGE_TYPE_UP, 'UP').
-define(CQERL_STATUS_CHANGE_TYPE_DOWN, 'DOWN').

-define(CQERL_EVENT_CHANGE_TYPE_CREATED, 'CREATED').
-define(CQERL_EVENT_CHANGE_TYPE_DROPPED, 'DROPPED').
-define(CQERL_EVENT_CHANGE_TYPE_UPDATED, 'UPDATED').

-define(CQERL_EVENT_CHANGE_TARGET_KEYSPACE,  'KEYSPACE').
-define(CQERL_EVENT_CHANGE_TARGET_TABLE,     'TABLE').
-define(CQERL_EVENT_CHANGE_TARGET_TYPE,      'TYPE').
-define(CQERL_EVENT_CHANGE_TARGET_FUNCTION,  'FUNCTION').
-define(CQERL_EVENT_CHANGE_TARGET_AGGREGATE, 'AGGREGATE').

-define(DEFAULT_PORT, 9042).

-define(DEFAULT_PROTOCOL_VERSION, 4).

-record(cql_query, {
    statement   = <<>>      :: cqerl:query_statement(),
    keyspace    = undefined :: cqerl:keyspace(),
    values      = #{}       :: map(),

    reusable    = undefined :: undefined | boolean(),
    named       = false     :: boolean(),

    page_size   = 100       :: integer(),
    page_state              :: binary() | undefined,

    consistency = one :: cqerl:consistency_level() | cqerl:consistency_level_int(),
    serial_consistency = undefined :: cqerl:serial_consistency() | cqerl:serial_consistency_int() | undefined,

    value_encode_handler = undefined :: function() | undefined,
    tracing     = false     :: boolean()
}).

-record(cql_call, {
    type :: sync | async,
    caller :: {pid(), reference()},
    client :: reference()
}).

-record(cql_query_batch, {
    keyspace    = undefined :: cqerl:keyspace(),
    consistency = one       :: cqerl:consistency_level() | cqerl:consistency_level_int(),
    mode        = logged    :: cqerl:batch_mode() | cqerl:batch_mode_int(),
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
    keyspace    :: cqerl:keyspace(),
    name        :: binary(),
    args        :: [ binary() ]
}).

-endif. % _CQERL_HRL_
