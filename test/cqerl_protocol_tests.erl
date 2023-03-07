-module(cqerl_protocol_tests).

-import(cqerl_protocol, [query_frame/3, batch_frame/2]).

-include("cqerl_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(F, #cqerl_frame{}).
-define(Q, #cqerl_query{}).

assert_query_consistency(X, Y) ->
    ?assertEqual(
       query_frame(?F, #cqerl_query_parameters{consistency = X}, ?Q),
       query_frame(?F, #cqerl_query_parameters{consistency = Y}, ?Q)).

assert_query_serial_consistency(X, Y) ->
    ?assertEqual(
       query_frame(?F, #cqerl_query_parameters{serial_consistency = X}, ?Q),
       query_frame(?F, #cqerl_query_parameters{serial_consistency = Y}, ?Q)).

assert_batch_consistency(X, Y) ->
    ?assertEqual(
       batch_frame(?F, #cql_query_batch{consistency = X}),
       batch_frame(?F, #cql_query_batch{consistency = Y})).

assert_batch_mode(X, Y) ->
    ?assertEqual(
       batch_frame(?F, #cql_query_batch{mode = X}),
       batch_frame(?F, #cql_query_batch{mode = Y})).

query_consistency_level_encoding_test() ->
    assert_query_consistency(?CQERL_CONSISTENCY_ANY,          any),
    assert_query_consistency(?CQERL_CONSISTENCY_ONE,          one),
    assert_query_consistency(?CQERL_CONSISTENCY_TWO,          two),
    assert_query_consistency(?CQERL_CONSISTENCY_THREE,        three),
    assert_query_consistency(?CQERL_CONSISTENCY_QUORUM,       quorum),
    assert_query_consistency(?CQERL_CONSISTENCY_ALL,          all),
    assert_query_consistency(?CQERL_CONSISTENCY_LOCAL_QUORUM, local_quorum),
    assert_query_consistency(?CQERL_CONSISTENCY_EACH_QUORUM,  each_quorum),
    assert_query_consistency(?CQERL_CONSISTENCY_LOCAL_ONE,    local_one),
    assert_query_consistency(?CQERL_CONSISTENCY_LOCAL_SERIAL, local_serial),
    assert_query_consistency(?CQERL_CONSISTENCY_SERIAL,       serial).

query_serial_consistency_encoding_test() ->
    assert_query_serial_consistency(0,                               undefined),
    assert_query_serial_consistency(?CQERL_CONSISTENCY_SERIAL,       serial),
    assert_query_serial_consistency(?CQERL_CONSISTENCY_LOCAL_SERIAL, local_serial).

batch_consistency_level_encoding_test() ->
    assert_batch_consistency(?CQERL_CONSISTENCY_ANY,          any),
    assert_batch_consistency(?CQERL_CONSISTENCY_ONE,          one),
    assert_batch_consistency(?CQERL_CONSISTENCY_TWO,          two),
    assert_batch_consistency(?CQERL_CONSISTENCY_THREE,        three),
    assert_batch_consistency(?CQERL_CONSISTENCY_QUORUM,       quorum),
    assert_batch_consistency(?CQERL_CONSISTENCY_ALL,          all),
    assert_batch_consistency(?CQERL_CONSISTENCY_LOCAL_QUORUM, local_quorum),
    assert_batch_consistency(?CQERL_CONSISTENCY_EACH_QUORUM,  each_quorum),
    assert_batch_consistency(?CQERL_CONSISTENCY_LOCAL_ONE,    local_one),
    assert_batch_consistency(?CQERL_CONSISTENCY_LOCAL_SERIAL, local_serial),
    assert_batch_consistency(?CQERL_CONSISTENCY_SERIAL,       serial).

batch_mode_encoding_test() ->
    assert_batch_mode(?CQERL_BATCH_COUNTER,  counter),
    assert_batch_mode(?CQERL_BATCH_LOGGED,   logged),
    assert_batch_mode(?CQERL_BATCH_UNLOGGED, unlogged).
