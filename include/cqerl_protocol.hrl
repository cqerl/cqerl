-define(CQERL_FRAME_RESP,         16#82).
-define(CQERL_FRAME_REQ,          16#02).
-define(CQERL_FRAME_COMPRESSION,  16#01).
-define(CQERL_FRAME_TRACING,      16#02).

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

-define(CQERL_EVENT_TOPOLOGY_CHANGE,  'TOPOLOGY_CHANGE').
-define(CQERL_EVENT_STATUS_CHANGE,    'STATUS_CHANGE').
-define(CQERL_EVENT_SCHEMA_CHANGE,    'SCHEMA_CHANGE').

-define(CQERL_BATCH_LOGGED,   0).
-define(CQERL_BATCH_UNLOGGED, 1).
-define(CQERL_BATCH_COUNTER,  2).

-include("cqerl.hrl").

-record(cqerl_frame, {
  compression = false           :: boolean(), 
  compression_type = undefined  :: lz4 | snappy | undefined, 
  tracing = false               :: boolean(), 
  opcode                        :: integer(), 
  stream_id = 0                 :: integer() 
}).

-record(cqerl_startup_options, { 
  cql_version = <<"3.0.0">> :: binary(), 
  compression = <<>>        :: binary() 
}).

-record(cqerl_query_parameters, {
  consistency         = ?CQERL_CONSISTENCY_ANY :: ?CQERL_CONSISTENCY_ANY .. ?CQERL_CONSISTENCY_LOCAL_SERIAL,
  skip_metadata       = false :: boolean(),
  page_state          = undefined :: binary() | undefined,
  page_size           = undefined :: integer() | undefined,
  serial_consistency  = undefined :: ?CQERL_CONSISTENCY_SERIAL | ?CQERL_CONSISTENCY_LOCAL_SERIAL | undefined
}).

-record(cqerl_query, {
  kind                = normal :: normal | prepared,
  query               = <<>> :: binary(),
  values              = [] :: list(binary())
}).

-record(cqerl_query_batch_parameters, {
  consistency         = ?CQERL_CONSISTENCY_ANY :: ?CQERL_CONSISTENCY_ANY .. ?CQERL_CONSISTENCY_LOCAL_SERIAL,
  mode                = ?CQERL_BATCH_LOGGED :: ?CQERL_BATCH_LOGGED .. ?CQERL_BATCH_COUNTER,
  queries             = [] :: list(#cqerl_query{})
}).

-record(cqerl_result_column_spec, {
  keyspace = <<>>        :: binary(),
  table_name = <<>>      :: binary(),
  name = <<>>            :: binary(),
  type = undefined       :: column_type()
}).

-record(cqerl_result_metadata, {
  page_state = undefined :: undefined | binary(),
  columns_count = 0       :: integer(),
  rows_count = 0         :: integer(),
  columns = []           :: list(#cqerl_result_column_spec{})
}).