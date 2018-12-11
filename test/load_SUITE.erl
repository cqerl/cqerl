%% common_test suite for load

-module(load_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("cqerl.hrl").

-compile(export_all).

-import(test_helper, [get_client/1]).

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
  [{timetrap, {seconds, 60}} | test_helper:requirements()].

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
     single_client, n_clients, many_clients, many_sync_clients, many_concurrent_clients
    ].

groups() ->
    [
     {pooler, [sequence], tests()},
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
     {group, pooler},
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
    test_helper:set_mode(pooler, Config),
    Config1 = test_helper:standard_setup("test_keyspace_1", Config),
    test_helper:create_keyspace(<<"test_keyspace_1">>, Config1),

    Client = get_client(Config1),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_1">>,
                             name = <<"entries1">>}} =
    cqerl:run_query(Client, "CREATE TABLE entries1 (id int PRIMARY KEY, name text);"),
    cqerl:close_client(Client),

    [{pool_min_size, 10}, {pool_max_size, 100} | Config1].

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
init_per_group(pooler, Config) ->
    test_helper:set_mode(pooler, Config);
init_per_group(hash, Config) ->
    test_helper:set_mode(hash, Config);

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

single_client(Config) ->
  Client = get_client(Config),
  Num = 500,    % # of requests
  Dt = 2,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, Num, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = os:timestamp(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        {result, Tag, void} -> 
          {ok, T} = orddict:find(Tag, Acc),
          F(F, N, M-1, orddict:store(Tag, timer:now_diff(os:timestamp(), T), Acc));
          
        send_request -> 
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
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
  

get_n_clients(_Config, 0, Acc) -> Acc;
get_n_clients(Config, N, Acc) ->
  get_n_clients(Config, N-1, [get_client(Config)|Acc]).

n_clients(Config) ->
  NC = 10,
  Clients = get_n_clients(Config, NC, []),
  
  Num = 1500,    % # of requests
  Dt = 2,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, Num, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = os:timestamp(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        {result, Tag, void} -> 
          {ok, T} = orddict:find(Tag, Acc),
          F(F, N, M-1, orddict:store(Tag, timer:now_diff(os:timestamp(), T), Acc));
          
        send_request ->
          Client = lists:nth((N rem 5) + 1, Clients),
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
          F(F, N-1, M, orddict:store(Tag, os:timestamp(), Acc));
          
        OtherMsg ->
          ct:fail("Unexpected response ~p", [OtherMsg])
        
      after 30000 -> 
        ct:fail("All delayed messages did not arrive in time")
      end
  end,
  Deltas = DelayLooper(DelayLooper, Num, Num, []),
  
  Sum = lists:foldr(fun ({_Tag, T}, Acc) -> Acc + T end, 0, Deltas),
  ct:log("~w requests sent over ~w seconds -- mean request roundtrip : ~w microseconds", 
    [Num, (timer:now_diff(os:timestamp(), T1))/1.0e6, Sum/Num]).

many_clients(Config) ->
  Num = 75000,    % # of requests
  Dt = 0.1,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(trunc(Dt*M), self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, Num, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = os:timestamp(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        {result, Tag, void} -> 
          {T, Client} = gb_trees:get(Tag, Acc),
          cqerl:close_client(Client),
          {Pid, _} = Client,
          F(F, N, M-1, gb_trees:update(Tag, {Pid, timer:now_diff(os:timestamp(), T)}, Acc));
          
        send_request ->
          Client = get_client(Config),
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
          F(F, N-1, M, gb_trees:insert(Tag, {os:timestamp(), Client}, Acc));
          
        OtherMsg ->
          ct:fail("Unexpected response ~p", [OtherMsg])
        
      after 30000 -> 
        ct:fail("All delayed messages did not arrive in time")
      end
  end,
  Deltas = gb_trees:to_list(DelayLooper(DelayLooper, Num, Num, gb_trees:empty())),
  Pids = lists:foldr(fun
      ({_, {Pid, _}}, Acc) ->
          case lists:member(Pid, Acc) of
              true -> Acc;
              false -> [Pid | Acc]
          end
  end, [], Deltas),
    
  DistinctPids = length(Pids),
  Sum = lists:foldr(fun ({_Tag, {_, T}}, Acc) -> Acc + T end, 0, Deltas),
  ct:log("~w requests sent to ~w clients over ~w seconds -- mean request roundtrip : ~w seconds", 
    [Num, DistinctPids, (timer:now_diff(os:timestamp(), T1))/1.0e6, (Sum/Num)/1.0e6]).

sync_insert() ->
    receive
        {Pid, {Client, Q}, Tag, N} ->
            case cqerl:run_query(Client, Q#cql_query{values=[{id, N}, {name, "defaultvaluehere"}]}) of
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

many_sync_clients(Config) ->
    Num = 75000,
    Dt = 0.01,
    Iterator = fun
        (_F, 0, _M) -> ok;
        (F, N, M) ->
            Client = get_client(Config),
            erlang:send_after(trunc(Dt*N), self(), {sync_request, self(),Client}),
            F(F, N-1, M+1)
        end,
    Iterator(Iterator, Num, 0),

    T1 = os:timestamp(),
    Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);", consistency=1},
    DelayLooper = fun
        (_F, 0, 0, Acc) ->
            Acc;
        (F, N, M, Acc) ->
            receive
                {result, Tag} ->
                    {T, Client} = gb_trees:get(Tag, Acc),
                    {Pid, _} = Client,
                    cqerl:close_client(Client),
                    F(F, N, M-1, gb_trees:update(Tag, {Pid, timer:now_diff(os:timestamp(), T)}, Acc));
                {sync_request, P, Client} ->
                    Tag = make_ref(),
                    Now = os:timestamp(),
                    Pid = spawn(fun sync_insert/0),
                    Pid ! {P, {Client, Q}, Tag, N},
                    F(F, N-1, M, gb_trees:insert(Tag, {Now, Client}, Acc));
                OtherMsg ->
                    ct:fail("Unexpected response ~p", [OtherMsg])
            after 30000 ->
                ct:fail("All delayed messages did not arrive in time~n")
            end
    end,
    Deltas = gb_trees:to_list(DelayLooper(DelayLooper, Num, Num, gb_trees:empty())),
    Pids = lists:foldr(fun
        ({_, {Pid, _}}, Acc) ->
            case lists:member(Pid, Acc) of
                true -> Acc;
                false -> [Pid | Acc]
            end
    end, [], Deltas),

    DistinctPids = length(Pids),
    Sum = lists:foldr(fun ({_Tag, {_, T}}, Acc) -> Acc + T end, 0, Deltas),
    ct:log("~w requests sent to ~w clients over ~w seconds -- mean request roundtrip : ~w seconds --~n",
    [Num, DistinctPids, (timer:now_diff(os:timestamp(), T1))/1.0e6, (Sum/Num)/1.0e6]).

many_concurrent_clients(Config) ->
    Procs = 100,
    Count = lists:seq(1, Procs),
    Me = self(),
    lists:foreach(fun(I) ->
                          spawn_link(fun() -> concurrent_client(Me, I, Config) end)
                  end,
                  Count),
    lists:foreach(fun(_) ->
                          receive
                              done -> ok
                          end
                  end,
                  Count).

concurrent_client(ReportTo, ID, Config) ->
    Iters = 200,
    Client = get_client(Config),
    Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);", consistency=1},
    lists:foreach(fun(I) ->
                          {ok, void} = cqerl:run_query(Client, Q#cql_query{values=[{id, ID}, {name, integer_to_list(I)}]})
                  end,
                  lists:seq(1, Iters)),
    ReportTo ! done.
