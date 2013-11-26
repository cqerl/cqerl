%% common_test suite for test

-module(test_SUITE).
-include_lib("common_test/include/ct.hrl").

-include("cqerl.hrl").

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
  [{timetrap, {seconds, 20}},
   {require, ssl, cqerl_test_ssl},
   {require, auth, cqerl_test_auth},
   {require, keyspace, cqerl_test_keyspace},
   {require, host, cqerl_host}].

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
    {database, [sequence], [ 
        connect, create_keyspace, create_table, simple_insertion_roundtrip 
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
    [datatypes_test, {group, database}].

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
    application:ensure_all_started(cqerl),
    [ {auth, ct:get_config(auth)}, 
      {ssl, ct:get_config(ssl)}, 
      {keyspace, ct:get_config(keyspace)},
      {host, ct:get_config(host)} ] ++ Config.

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

init_per_group(_group, Config) ->
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
init_per_testcase(TestCase, Config) ->
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
end_per_testcase(TestCase, Config) ->
    Config.

datatypes_test(_Config) ->
    ok = eunit:test(cqerl_datatypes).

connect(Config) ->
    {Pid, Ref} = get_client(Config),
    true = is_pid(Pid),
    true = is_reference(Ref),
    cqerl:close_client({Pid, Ref}),
    ok.

create_keyspace(Config) ->
    Client = get_client(Config),
    Q = <<"CREATE KEYSPACE test_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};">>,
    D = <<"DROP KEYSPACE test_keyspace;">>,
    case cqerl:run_query(Client, #cql_query{query=Q}) of
        {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>}} -> ok;
        {error, {16#2400, _, {key_space, <<"test_keyspace">>}}} ->
            {ok, #cql_schema_changed{change_type=dropped, keyspace = <<"test_keyspace">>}} = cqerl:run_query(Client, D),
            {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>}} = cqerl:run_query(Client, Q)
    end,
    cqerl:close_client(Client).
        
create_table(Config) ->
    Client = get_client(Config),
    Q = "CREATE TABLE entries1(id varchar, age int, email varchar, PRIMARY KEY(id));",
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>, table = <<"entries1">>}} =
        cqerl:run_query(Client, Q),
    cqerl:close_client(Client).

simple_insertion_roundtrip(Config) ->
    Client = get_client(Config),
    Q = <<"INSERT INTO entries1(id, age, email) VALUES (?, ?, ?)">>,
    {ok, ok} = cqerl:run_query(Client, #cql_query{query=Q, values=[
        {varchar, "hello"},
        {int, 18},
        {varchar, <<"mathieu@damours.org">>}
    ]}),
    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{query = <<"SELECT * FROM entries1;">>}),
    Row = cqerl:head(Result),
    <<"hello">> = proplists:get_value(id, Row),
    18 = proplists:get_value(age, Row),
    <<"mathieu@damours.org">> = proplists:get_value(email, Row),
    cqerl:close_client(Client).

get_client(Config) ->
    Host = proplists:get_value(host, Config),
    DataDir = proplists:get_value(data_dir, Config),

    %% To relative file paths for SSL, prepend the path of
    %% the test data directory. To bypass this behavior, provide
    %% an absolute path.

    SSL = case proplists:get_value(ssl, Config, undefined) of
        undefined -> false;
        false -> false;
        Options ->
            io:format("Options : ~w~n", [Options]),
            lists:map(fun
                ({FileOpt, Path}) when FileOpt == cacertfile;
                                       FileOpt == certfile;
                                       FileOpt == keyfile ->
                    case Path of
                        [$/ | _Rest] -> {FileOpt, Path};
                        _ -> {FileOpt, filename:join([DataDir, Path])}
                    end;
    
                (Opt) -> Opt
            end, Options)
    end,
    Auth = proplists:get_value(auth, Config, undefined),
    Keyspace = proplists:get_value(keyspace, Config),
    cqerl:new_client(Host, [{ssl, SSL}, {auth, Auth}, {keyspace, Keyspace}]).
