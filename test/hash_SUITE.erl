%% common_test suite for cqerl hash system

-module(hash_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include("cqerl.hrl").

-compile(export_all).

-import(test_helper, [
                    %  maybe_get_client/1,
                      get_client/1
                     ]).

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
  [{timetrap, {seconds, 20}} | test_helper:requirements()].

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
groups() -> [
    {clients, [sequence], [
        create_keyspace,
        create_clients,
        crash_recovery,
        outage_recovery
    ]}
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
     %{group, clients}
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

-define(KEYSPACE, "test_keyspace_3").

init_per_suite(Config) ->
    test_helper:standard_setup(?KEYSPACE, Config).

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

init_per_group(_Group, Config) ->
    [{keyspace, "test_keyspace_3"} | Config].

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

create_keyspace(Config) ->
    test_helper:create_keyspace(<<"test_keyspace_3">>, Config).

create_clients(Config) ->
    {ClientPid, _} = get_client(Config),
    % Should get the same client pid each time:
    {ClientPid, _} = get_client(Config),
    % Should be two table entries (one for undefined keyspace, used to
    % create test_keyspace_3)
    ClientTables = ets:tab2list(cqerl_client_tables),
    ?assertEqual(2, length(ClientTables)),
    % Each with the default number of elements:
    lists:foreach(fun({client_table, _, _, T}) ->
        ?assertEqual(20, ets:info(T, size)) end,
        ClientTables).

crash_recovery(Config) ->
    {ClientPid, _} = get_client(Config),
    % Let's crash one:
    exit(ClientPid, kill),
    % Give it a moment to get itself together:
    timer:sleep(500),
    {ClientPid2, _} = get_client(Config),
    ?assertNotEqual(ClientPid, ClientPid2),
    ?assert(is_process_alive(ClientPid2)).

outage_recovery(Config) ->
    {ClientPid, _} = get_client(Config),
    ClientTables = ets:tab2list(cqerl_client_tables),
    lists:foreach(fun({client_table, _, Sup, _}) ->
                          kill_children(Sup)
                  end, ClientTables),
    timer:sleep(500),
    % Everything should have died and been cleaned up:
    ?assertEqual([], ets:tab2list(cqerl_client_tables)),

    % Fire up some new clients:
    {ClientPid2, _} = get_client(Config),
    ?assertNotEqual(ClientPid, ClientPid2),
    ?assertEqual(1, length(ets:tab2list(cqerl_client_tables))),
    ok.

kill_children(Sup) ->
    lists:foreach(fun({_, Child, _, _}) -> exit(Child, kill) end,
                  supervisor:which_children(Sup)).
