%% common_test suite for load

-module(load_SUITE).

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
tests() ->
    [
     single_client, many_concurrent_clients
    ].

groups() ->
    [
     {hash, [sequence], tests()}
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
     {group, hash}
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
    Config2 = test_helper:standard_setup(Config),
    cqerl:add_group(["localhost"], Config, 1),

    test_helper:create_keyspace(<<"test_keyspace_1">>, Config2),

    cqerl:add_group(["localhost"], [{keyspace, "test_keyspace_1"} | Config], 10),

    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_1">>,
                             name = <<"entries1">>}} =
    cqerl:run_query(test_keyspace_1, "CREATE TABLE entries1 (id int PRIMARY KEY, name text);"),

    Config2.

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

single_client(_Config) ->
    Num = 500,    % # of requests
    Dt = 2,    % in ms, yielding (1000/Dt) req/s
    Iterator = fun
                   (_F, 0, _M) -> ok;
    (F, N, M) ->
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
               end,
    Iterator(Iterator, Num, 0),

    Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);",
                   keyspace = test_keyspace_1
                  },

    T1 = os:timestamp(),
    DelayLooper = fun
                      (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
          {cqerl_result, Tag, void} ->
              {ok, T} = orddict:find(Tag, Acc),
              F(F, N, M-1, orddict:store(Tag, timer:now_diff(os:timestamp(), T), Acc));
          send_request ->
              {ok, Tag} = cqerl:send_query(Q#cql_query{values=#{id => N, name => "test"}}),
              F(F, N-1, M, orddict:store(Tag, os:timestamp(), Acc));
          OtherMsg ->
              ct:fail("Unexpected response ~p", [OtherMsg])

      after 1000 ->
                ct:fail("All delayed messages did not arrive in time")
      end
                  end,
    Deltas = DelayLooper(DelayLooper, Num, Num, []),
    Sum = lists:foldr(fun ({_Tag, T}, Acc) -> Acc + T end, 0, Deltas),
    ct:log("~w requests sent over ~w seconds -- mean request roundtrip : ~w microseconds",
           [Num, (timer:now_diff(os:timestamp(), T1))/1.0e6, Sum/Num]).

sync_insert() ->
    receive
        {Pid, {Client, Q}, Tag, N} ->
            case cqerl:run_query(Client, Q#cql_query{values=#{id => N, name => "defaultvaluehere"}}) of
                {ok, void} ->
                    Pid ! {result, Tag},
                    ok;
                Fail ->
                    ct:fail("FAILED ~p~n",[Fail]),
                    ok
            end;
        OtherMsg ->
            ct:fail("Wrong msg ~p~n",[OtherMsg]),
            ok
    after 1000 ->
              ct:fail("Timeout~n",[]),
              timeout
    end.

many_concurrent_clients(_Config) ->
    Procs = 200,
    Count = lists:seq(1, Procs),
    Me = self(),
    lists:foreach(fun(I) ->
                          spawn_link(fun() -> concurrent_client(Me, I) end)
                  end,
                  Count),
    lists:foreach(fun(_) ->
                          receive
                              done -> ok
                          end
                  end,
                  Count).

concurrent_client(ReportTo, ID) ->
    Iters = 500,
    Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);",
                   consistency=1,
                   keyspace = test_keyspace_1},
    lists:foreach(fun(I) ->
                          {ok, void} = cqerl:run_query(Q#cql_query{values=#{id => ID, name => integer_to_list(I)}})
                  end,
                  lists:seq(1, Iters)),
    ReportTo ! done.
