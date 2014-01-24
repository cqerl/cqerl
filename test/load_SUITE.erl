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
groups() -> [].

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
    [single_client, n_clients, many_clients].

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
    case erlang:function_exported(application, ensure_all_started, 1) of
      true -> application:ensure_all_started(cqerl);
      false ->
        application:start(crypto),
        application:start(public_key),
        application:start(ssl),
        application:start(pooler),
        application:start(cqerl)
    end,
    
    application:start(sasl),
    RawSSL = ct:get_config(ssl),
    DataDir = proplists:get_value(data_dir, Config),
    SSL = case RawSSL of
        undefined -> false;
        false -> false;
        _ ->
            %% To relative file paths for SSL, prepend the path of
            %% the test data directory. To bypass this behavior, provide
            %% an absolute path.
            lists:map(fun
                ({FileOpt, Path}) when FileOpt == cacertfile;
                                       FileOpt == certfile;
                                       FileOpt == keyfile ->
                    case Path of
                        [$/ | _Rest] -> {FileOpt, Path};
                        _ -> {FileOpt, filename:join([DataDir, Path])}
                    end;

                (Opt) -> Opt
            end, RawSSL)
    end,
    Config1 = [ {auth, ct:get_config(auth)}, 
      {ssl, RawSSL},
      {prepared_ssl, SSL},
      {keyspace, ct:get_config(keyspace)},
      {host, ct:get_config(host)} ] ++ Config,
    
    Client = get_client([{keyspace, undefined}|Config1]),
    Q = <<"CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};">>,
    D = <<"DROP KEYSPACE test_keyspace;">>,
    case cqerl:run_query(Client, #cql_query{statement=Q}) of
        {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>}} -> ok;
        {error, {16#2400, _, {key_space, <<"test_keyspace">>}}} ->
            {ok, #cql_schema_changed{change_type=dropped, keyspace = <<"test_keyspace">>}} = cqerl:run_query(Client, D),
            {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>}} = cqerl:run_query(Client, Q)
    end,
    cqerl:run_query(Client, "USE test_keyspace;"),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace">>, table = <<"entries1">>}} =
      cqerl:run_query(Client, "CREATE TABLE entries1 (id int PRIMARY KEY, name text);"),
    cqerl:close_client(Client),
    
    Config1.

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

single_client(Config) ->
  Client = get_client(Config),
  N = 500,    % # of requests
  Dt = 2,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, N, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = erlang:now(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        Msg = {result, Tag, void} -> 
          {ok, T} = orddict:find(Tag, Acc),
          F(F, N, M-1, orddict:store(Tag, timer:now_diff(erlang:now(), T), Acc));
          
        send_request -> 
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
          F(F, N-1, M, orddict:store(Tag, erlang:now(), Acc));
          
        OtherMsg ->
          ct:fail("Unexpected response ~p", [OtherMsg])
        
      after 1000 -> 
        ct:fail("All delayed messages did not arrive in time")
      end
  end,
  Deltas = DelayLooper(DelayLooper, N, N, []),
  
  Sum = lists:foldr(fun ({Tag, T}, Acc) -> Acc + T end, 0, Deltas),
  ct:log("~w requests sent over ~w seconds -- mean request roundtrip : ~w microseconds", 
    [N, (timer:now_diff(erlang:now(), T1))/1.0e6, Sum/N]).
  

get_n_clients(_Config, 0, Acc) -> Acc;
get_n_clients(Config, N, Acc) ->
  get_n_clients(Config, N-1, [get_client(Config)|Acc]).

n_clients(Config) ->
  NC = 10,
  Clients = get_n_clients(Config, NC, []),
  
  N = 1500,    % # of requests
  Dt = 2,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, N, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = erlang:now(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        Msg = {result, Tag, void} -> 
          {ok, T} = orddict:find(Tag, Acc),
          F(F, N, M-1, orddict:store(Tag, timer:now_diff(erlang:now(), T), Acc));
          
        send_request ->
          Client = lists:nth((N rem 5) + 1, Clients),
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
          F(F, N-1, M, orddict:store(Tag, erlang:now(), Acc));
          
        OtherMsg ->
          ct:fail("Unexpected response ~p", [OtherMsg])
        
      after 1000 -> 
        ct:fail("All delayed messages did not arrive in time")
      end
  end,
  Deltas = DelayLooper(DelayLooper, N, N, []),
  
  Sum = lists:foldr(fun ({Tag, T}, Acc) -> Acc + T end, 0, Deltas),
  ct:log("~w requests sent over ~w seconds -- mean request roundtrip : ~w microseconds", 
    [N, (timer:now_diff(erlang:now(), T1))/1.0e6, Sum/N]).

many_clients(Config) ->
  N = 1500,    % # of requests
  Dt = 2,    % in ms, yielding (1000/Dt) req/s
  
  Iterator = fun
    (_F, 0, _M) -> ok;
    (F, N, M) -> 
      erlang:send_after(Dt*M, self(), send_request),
      F(F, N-1, M+1)
  end,
  Iterator(Iterator, N, 0),
  
  Q = #cql_query{statement="INSERT INTO entries1 (id, name) values (?, ?);"},
  
  T1 = erlang:now(),
  DelayLooper = fun
    (_F, 0, 0, Acc) -> Acc;
    (F, N, M, Acc) ->
      receive
        Msg = {result, Tag, void} -> 
          {ok, {T, Client}} = orddict:find(Tag, Acc),
          cqerl:close_client(Client),
          F(F, N, M-1, orddict:store(Tag, timer:now_diff(erlang:now(), T), Acc));
          
        send_request ->
          Client = get_client(Config),
          Tag = cqerl:send_query(Client, Q#cql_query{values=[{id, N}, {name, "test"}]}),
          F(F, N-1, M, orddict:store(Tag, {erlang:now(), Client}, Acc));
          
        OtherMsg ->
          ct:fail("Unexpected response ~p", [OtherMsg])
        
      after 1000 -> 
        ct:fail("All delayed messages did not arrive in time")
      end
  end,
  Deltas = DelayLooper(DelayLooper, N, N, []),
  
  Sum = lists:foldr(fun ({Tag, T}, Acc) -> Acc + T end, 0, Deltas),
  ct:log("~w requests sent over ~w seconds -- mean request roundtrip : ~w microseconds", 
    [N, (timer:now_diff(erlang:now(), T1))/1.0e6, Sum/N]).

get_client(Config) ->
    Host = proplists:get_value(host, Config),
    SSL = proplists:get_value(prepared_ssl, Config),
    Auth = proplists:get_value(auth, Config, undefined),
    Keyspace = proplists:get_value(keyspace, Config),
    
    % io:format("Options : ~w~n", [[
    %     {ssl, SSL}, {auth, Auth}, {keyspace, Keyspace},
    %     {pool_min_size, 5}, {pool_max_size, 5}
    %     ]]),
        
    {ok, Client} = cqerl:new_client(Host, [{ssl, SSL}, {auth, Auth}, {keyspace, Keyspace}, 
                                           {pool_min_size, 5}, {pool_max_size, 5} ]),
    Client.
