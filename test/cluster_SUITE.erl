%% common_test suite for cluster

-module (cluster_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("cqerl.hrl").


-import(test_helper, [
                    %  maybe_get_client/1,
                      get_client/1
                     ]).

-compile(export_all).

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
  [{timetrap, {seconds, 30}} | test_helper:requirements()].

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------

groups() ->
    [
     {single, [sequence], [test_get_clients]},
     {multi, [sequence], [test_get_clients_from_cluster]}
    ].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%
%%      NB: By default, we export all 1-arity user defined functions
%%--------------------------------------------------------------------

all() ->
    [
     {group, single},
     {group, multi}
    ].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    test_helper:set_mode(pooler, Config),

    Config0 = test_helper:standard_setup("test_keyspace_1", Config),
    test_helper:create_keyspace(<<"test_keyspace_1">>, Config0),

    Config1 = test_helper:standard_setup("test_keyspace_2", Config0),
    test_helper:create_keyspace(<<"test_keyspace_2">>, Config1),

    Client = get_client(Config0),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_1">>,
                             name = <<"entries1">>}} =
    cqerl:run_query(Client, "CREATE TABLE entries1 (id int PRIMARY KEY, name text);"),
    cqerl:close_client(Client),

    test_helper:set_mode(hash, Config1).

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------


init_per_group(single, Config) ->
    test_helper:set_mode(hash, Config),

    Host = proplists:get_value(host, Config),
    SSL = proplists:get_value(prepared_ssl, Config),
    Auth = proplists:get_value(auth, Config, undefined),
    Keyspace = proplists:get_value(keyspace, Config),
    ProtocolVersion = proplists:get_value(protocol_version, Config),
    
    Opts = [{ssl, SSL}, {auth, Auth}, {keyspace, Keyspace},
            {protocol_version, ProtocolVersion} ],

    cqerl_cluster:add_nodes([ Host ], Opts);

init_per_group(multi, Config) ->
    test_helper:set_mode(hash, Config),

    Host = proplists:get_value(host, Config),
    SSL = proplists:get_value(prepared_ssl, Config),
    Auth = proplists:get_value(auth, Config, undefined),
    ProtocolVersion = proplists:get_value(protocol_version, Config),
    
    Opts = [{ssl, SSL}, {auth, Auth}, {protocol_version, ProtocolVersion} ],

    cqerl_cluster:add_nodes(cluster1, [ Host ], [{keyspace, test_keyspace_1} | Opts]),
    cqerl_cluster:add_nodes(cluster2, [ Host ], [{keyspace, test_keyspace_2} | Opts]);

init_per_group(_, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    Config.

test_get_clients(_Config) ->
    {ok, {Pid, Ref1}} = cqerl:get_client(),
    {ok, {Pid, Ref2}} = cqerl:get_client(),
    if
      Ref1 == Ref2 -> 
        ct:fail("Two subsequent calls to get_client/0 generated exactly the same client handle", []);
      true ->
        ok
    end,
    ok.

test_get_clients_from_cluster(_Config) ->
    {ok, {Pid1, Ref1}} = cqerl:get_client(cluster1),
    {ok, {Pid1, Ref2}} = cqerl:get_client(cluster1),
    {ok, {Pid2, Ref3}} = cqerl:get_client(cluster2),
    {ok, {Pid2, Ref4}} = cqerl:get_client(cluster2),
    if
      Ref1 == Ref2; Ref3 == Ref4 -> 
        ct:fail("Two subsequent calls to get_client/1 for the same cluster generated exactly the same client handle", []);

      Pid1 == Pid2 -> 
        ct:fail("Two calls to get_client/1 for different cluster generated exactly the same connection pid", []);

      true ->
        ok
    end,
    ok.