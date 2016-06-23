-module(tap_SUITE).
-include_lib("common_test/include/ct.hrl").
-include("cqerl.hrl").

-compile(export_all).

-define(KEYSPACE, <<"test_keyspace_tap">>).

all() ->
    [
     create_table
     ,{group, tap}
     ,{group, simple}
    ].

groups() ->
    [{simple, [
               create_rows,
               lookup_rows
              ]},
     {tap,    [
               create_rows,
               lookup_rows
              ]}
    ].

init_per_suite(Config) ->
    application:ensure_started(cqerl),
    Config2 = test_helper:standard_setup(Config),
    cqerl:add_group(["localhost"], Config, 1),
    test_helper:create_keyspace(?KEYSPACE, Config2),
    cqerl:add_group(["localhost", "10.1.1.32"],
                    [{keyspace, ?KEYSPACE} | Config], 10),
    UUIDs = [uuid:get_v4() || _ <- lists:seq(1, 10000)],
    [{uuids, UUIDs} | Config2].

end_per_suite(_Config) ->
    ok.

init_per_group(simple, Config) ->
    application:set_env(cqerl, strategy, simple),
    truncate_table(),
    Config;

init_per_group(tap, Config) ->
    application:set_env(cqerl, strategy, token_aware),
    truncate_table(),
    Config.

end_per_group(_Group, _Config) ->
    ct:log("Schema: ~p", [ets:tab2list(cqerl_schema)]),

    Ts = ets:tab2list(cqerl_client_tables),
    ct:log("client_table ~p", [Ts]),
    lists:foreach(fun(T) ->
                          ct:log("table: ~p ~p", [element(2, T), ets:tab2list(element(3, T))])
                  end, Ts),
    ok.

truncate_table() ->
    _ = cqerl:run_query(#cql_query{statement = "TRUNCATE test",
                                   keyspace = ?KEYSPACE}).

create_table(_Config) ->
    Q = "CREATE TABLE test(id uuid PRIMARY KEY, value text)",
    {ok, #cql_schema_changed{change_type=created,
                             keyspace = ?KEYSPACE,
                             name = <<"test">>}} =
    cqerl:run_query(#cql_query{statement = Q,
                               keyspace = ?KEYSPACE}),
    cqerl_schema:force_refresh().

create_rows(Config) ->
    UUIDs = proplists:get_value(uuids, Config),
    Q = <<"INSERT INTO test(id, value) VALUES (?, ?)">>,
    lists:foreach(
      fun(UUID) ->
              {ok, void} =
              cqerl:run_query(#cql_query{statement = Q,
                                         keyspace = ?KEYSPACE,
                                         values =
                                         #{id => UUID,
                                           value => uuid:uuid_to_string(UUID)}})
      end,
      UUIDs).

lookup_rows(Config) ->
    UUIDs = proplists:get_value(uuids, Config),
    Q = <<"SELECT * FROM test WHERE id = ?">>,
    lists:foreach(
      fun(UUID) ->
              {ok, _R} =
              cqerl:run_query(#cql_query{statement = Q,
                                         keyspace = ?KEYSPACE,
                                         values = #{id => UUID}})
      end,
      UUIDs).
