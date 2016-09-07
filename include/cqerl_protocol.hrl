-ifndef(_CQERL_PROTOCOL_HRL_).
-define(_CQERL_PROTOCOL_HRL_, 1).

%% Native protocol v3/4 constants
-define(MIN_CQERL_FRAME_RESP,     16#83).
-define(MAX_CQERL_FRAME_RESP,     16#84).
-define(CQERL_FRAME_REQ,          16#00).
-define(CQERL_FRAME_COMPRESSION,  16#01).
-define(CQERL_FRAME_TRACING,      16#02).

%% Operations
-define(CQERL_OP_ERROR,           16#00).
-define(CQERL_OP_STARTUP,         16#01).
-define(CQERL_OP_READY,           16#02).
-define(CQERL_OP_AUTHENTICATE,    16#03).
-define(CQERL_OP_OPTIONS,         16#05).
-define(CQERL_OP_SUPPORTED,       16#06).
-define(CQERL_OP_QUERY,           16#07).
-define(CQERL_OP_RESULT,          16#08).
-define(CQERL_OP_PREPARE,         16#09).
-define(CQERL_OP_EXECUTE,         16#0A).
-define(CQERL_OP_REGISTER,        16#0B).
-define(CQERL_OP_EVENT,           16#0C).
-define(CQERL_OP_BATCH,           16#0D).
-define(CQERL_OP_AUTH_CHALLENGE,  16#0E).
-define(CQERL_OP_AUTH_RESPONSE,   16#0F).
-define(CQERL_OP_AUTH_SUCCESS,    16#10).

%% Errors
-define(CQERL_ERROR_SERVER,         16#0000).
-define(CQERL_ERROR_PROTOCOL,       16#000A).
-define(CQERL_ERROR_AUTH,           16#0100).

-define(CQERL_ERROR_UNAVAILABLE,    16#1000).
-define(CQERL_ERROR_OVERLOADED,     16#1001).
-define(CQERL_ERROR_BOOTSTRAPPING,  16#1002).
-define(CQERL_ERROR_TRUNCATE,       16#1003).
-define(CQERL_ERROR_WTIMEOUT,       16#1100).
-define(CQERL_ERROR_RTIMEOUT,       16#1200).
-define(CQERL_ERROR_RFAILURE,       16#1300).
-define(CQERL_ERROR_FUN_FAILURE,    16#1400).
-define(CQERL_ERROR_WFAILURE,       16#1500).

-define(CQERL_ERROR_SYNTAX,         16#2000).
-define(CQERL_ERROR_UNAUTHORIZED,   16#2100).
-define(CQERL_ERROR_INVALID,        16#2200).
-define(CQERL_ERROR_CONFIG,         16#2300).
-define(CQERL_ERROR_ALREADY_EXISTS, 16#2400).
-define(CQERL_ERROR_UNPREPARED,     16#2500).

-include("cqerl.hrl").

-type compression_type() :: lz4 | snappy | undefined.

-record(cqerl_frame, {
    compression = false           :: boolean(),
    compression_type = undefined  :: compression_type(),
    tracing = false               :: boolean(),
    opcode                        :: byte(),
    stream_id = 0                 :: char()
}).

-record(cqerl_startup_options, {
    cql_version = <<"3.0.0">> :: binary(),
    compression = undefined   :: compression_type()
}).

-record(cqerl_query_parameters, {
    consistency         = any :: cqerl:consistency_level() | cqerl:consistency_level_int(),
    skip_metadata       = false :: boolean(),
    page_state          = undefined :: binary() | undefined,
    page_size           = undefined :: integer() | undefined,
    serial_consistency  = undefined :: cqerl:serial_consistency() | cqerl:serial_consistency_int() | undefined
}).

-record(cqerl_query, {
    kind                = normal :: normal | prepared,
    statement           = <<>>   :: binary(),
    values              = []     :: list(binary()),
    source_query                 :: #cql_query{},
    tracing             = false  :: boolean()
}).

-record(cqerl_result_column_spec, {
    keyspace = <<>>        :: binary(),
    table_name = <<>>      :: binary(),
    name = undefined       :: atom(),
    type = undefined       :: cqerl:column_type()
}).

-record(cqerl_result_metadata, {
    page_state = undefined :: undefined | binary(),
    columns_count = 0      :: integer(),
    rows_count = 0         :: integer(),
    columns = []           :: list(#cqerl_result_column_spec{})
}).

-record(cqerl_cached_query, {
    key :: {cqerl:cqerl_node(), binary() | '_'},
    query_ref = <<>> :: binary() | '_',
    params_metadata :: #cqerl_result_metadata{} | '_',
    result_metadata :: #cqerl_result_metadata{} | '_'
}).

-record(topology_change, {
    type :: cqerl:topology_change(),
    node :: cqerl:cqerl_node()
}).

-record(status_change, {
    type :: cqerl:status_change(),
    node :: cqerl:cqerl_node()
}).

-record(keyspace_change, {
    type :: cqerl:schema_change(),
    keyspace :: cqerl:keyspace()
}).

-record(table_change, {
    type :: cqerl:schema_change(),
    keyspace :: cqerl:keyspace(),
    table :: binary()
}).

-endif. % _CQERL_PROTOCOL_HRL_
