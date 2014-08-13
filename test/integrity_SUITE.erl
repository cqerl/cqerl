%% common_test suite for test

-module(integrity_SUITE).
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
   % {require, keyspace, cqerl_test_keyspace},
   {require, host, cqerl_host},
   {require, pool_min_size, pool_min_size},
   {require, pool_max_size, pool_max_size}].

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
    {connection, [sequence], [
        random_selection
    ]},
    {database, [sequence], [ 
        {initial, [sequence], [connect, create_keyspace]}, 
        create_table,
        simple_insertion_roundtrip, async_insertion_roundtrip, 
        emptiness,
        {transactions, [parallel], [
            {types, [parallel], [all_datatypes, custom_encoders, collection_types, counter_type]},
            batches_and_pages
        ]}
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
    [datatypes_test, 
     {group, connection}, 
     {group, database}
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
    case erlang:function_exported(application, ensure_all_started, 1) of
      true -> application:ensure_all_started(cqerl);
      false ->
        application:start(crypto),
        application:start(asn1),
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
    [ {auth, ct:get_config(auth)}, 
      {ssl, RawSSL},
      {prepared_ssl, SSL},
      {keyspace, "test_keyspace_2"},
      {host, ct:get_config(host)},
      {pool_min_size, ct:get_config(pool_min_size)},
      {pool_max_size, ct:get_config(pool_max_size)} ] ++ Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(Config) ->
    Client = get_client([{keyspace, undefined}|Config]),
    % {ok, #cql_schema_changed{change_type=dropped, keyspace = <<"test_keyspace_2">>}} = 
    %     cqerl:run_query(Client, #cql_query{statement = <<"DROP KEYSPACE test_keyspace_2;">>}),
    cqerl:close_client(Client).

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

init_per_group(NoKeyspace, Config) when NoKeyspace == connection;
                                        NoKeyspace == initial ->
    %% Here we remove the keyspace configuration, since we're going to drop it
    %% Otherwise, subsequent requests would sometimes fail saying that no keyspace was specified
    [{keyspace, undefined} | Config];
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
    
get_multiple_clients(Config, 0, Acc) -> Acc;
get_multiple_clients(Config, N, Acc) ->
    get_multiple_clients(Config, N-1, [get_client(Config) | Acc]).

random_selection(Config) ->
    Clients = get_multiple_clients(Config, 200, []),
    DistinctPids = lists:foldl(fun({Pid, _Ref}, Pids) ->
        case lists:member(Pid, Pids) of
            true -> Pids;
            false -> [Pid | Pids]
        end
    end, [], Clients),
    MaxSize = proplists:get_value(pool_min_size, Config),
    MaxSize = length(DistinctPids),
    lists:foreach(fun(Client) -> cqerl:close_client(Client) end, Clients).

connect(Config) ->
    {Pid, Ref} = get_client(Config),
    true = is_pid(Pid),
    true = is_reference(Ref),
    cqerl:close_client({Pid, Ref}),
    ok.

create_keyspace(Config) ->
    Client = get_client(Config),
    Q = <<"CREATE KEYSPACE test_keyspace_2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};">>,
    D = <<"DROP KEYSPACE test_keyspace_2;">>,
    case cqerl:run_query(Client, #cql_query{statement=Q}) of
        {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>}} -> ok;
        {error, {16#2400, _, {key_space, <<"test_keyspace_2">>}}} ->
            {ok, #cql_schema_changed{change_type=dropped, keyspace = <<"test_keyspace_2">>}} = cqerl:run_query(Client, D),
            {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>}} = cqerl:run_query(Client, Q)
    end,
    cqerl:close_client(Client).
        
create_table(Config) ->
    Client = get_client(Config),
    ct:log("Got client ~w~n", [Client]),
    Q = "CREATE TABLE entries1(id varchar, age int, email varchar, PRIMARY KEY(id));",
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>, table = <<"entries1">>}} =
        cqerl:run_query(Client, Q),
    cqerl:close_client(Client).

simple_insertion_roundtrip(Config) ->
    Client = get_client(Config),
    Q = <<"INSERT INTO entries1(id, age, email) VALUES (?, ?, ?)">>,
    {ok, void} = cqerl:run_query(Client, #cql_query{statement=Q, values=[
        {id, "hello"},
        {age, 18},
        {email, <<"mathieu@damours.org">>}
    ]}),
    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{statement = <<"SELECT * FROM entries1;">>}),
    Row = cqerl:head(Result),
    <<"hello">> = proplists:get_value(id, Row),
    18 = proplists:get_value(age, Row),
    <<"mathieu@damours.org">> = proplists:get_value(email, Row),
    cqerl:close_client(Client),
    Result.

emptiness(Config) ->
    Client = get_client(Config),
    {ok, void} = cqerl:run_query(Client, "update entries1 set email = null where id = 'hello';"),
    {ok, Result} = cqerl:run_query(Client, "select * from entries1 where id = 'hello';"),
    Row = cqerl:head(Result),
    null = proplists:get_value(email, Row),
    {ok, void} = cqerl:run_query(Client, #cql_query{statement="update entries1 set age = ? where id = 'hello';",
                                                    values=[{age, null}]}),
    {ok, Result2} = cqerl:run_query(Client, "select * from entries1 where id = 'hello';"),
    Row2 = cqerl:head(Result2),
    null = proplists:get_value(age, Row2),
    cqerl:close_client(Client).
    

async_insertion_roundtrip(Config) ->
    Client = get_client(Config),
    
    Ref = cqerl:send_query(Client, #cql_query{
        statement = <<"INSERT INTO entries1(id, age, email) VALUES (?, ?, ?)">>, 
        values = [
            {id, "1234123"},
            {age, 45},
            {email, <<"yvon@damours.org">>}
        ]
    }),
    receive {result, Ref, void} -> ok end,
    
    Ref2 = cqerl:send_query(Client, #cql_query{statement = <<"SELECT * FROM entries1;">>}),
    receive
        {result, Ref2, Result=#cql_result{}} ->
            {_Row, Result2} = cqerl:next(Result),
            Row = cqerl:head(Result2),
            <<"1234123">> = proplists:get_value(id, Row),
            45 = proplists:get_value(age, Row),
            <<"yvon@damours.org">> = proplists:get_value(email, Row),
            Res = Row;
            
        Other ->
            Res = undefined,
            ct:fail("Received: ~p~n", [Other])
            
    end,
    
    cqerl:close_client(Client),
    Res.


datatypes_columns(Cols) ->
    datatypes_columns(1, Cols, <<>>).

datatypes_columns(_I, [], Bin) -> Bin;
datatypes_columns(I, [ColumnType|Rest], Bin) ->
    Column = list_to_binary(io_lib:format("col~B ~s, ", [I, ColumnType])),
    datatypes_columns(I+1, Rest, << Bin/binary, Column/binary >>).

all_datatypes(Config) ->
    Client = get_client(Config),
    Cols = datatypes_columns([ascii, bigint, blob, boolean, decimal, double, float, int, timestamp, uuid, varchar, varint, timeuuid, inet]),
    CreationQ = <<"CREATE TABLE entries2(",  Cols/binary, " PRIMARY KEY(col1));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>, table = <<"entries2">>}} =
        cqerl:run_query(Client, CreationQ),
    
    InsQ = #cql_query{statement = <<"INSERT INTO entries2(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11, col12, col13, col14) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)">>},
    {ok, void} = cqerl:run_query(Client, InsQ#cql_query{values=RRow1=[
        {col1, "hello"},
        {col2, 9223372036854775807},
        {col3, <<1,2,3,4,5,6,7,8,9,10>>},
        {col4, true},
        {col5, {1234, 5}},
        {col6, 5.1235131241221e-6},
        {col7, 5.12351e-6},
        {col8, 2147483647},
        {col9, now},
        {col10, new},
        {col11, <<"Юникод"/utf8>>},
        {col12, 1928301970128391280192830198049113123},
        {col13, now},
        {col14, {127, 0, 0, 1}}
    ]}),
    {ok, void} = cqerl:run_query(Client, InsQ#cql_query{values=RRow2=[
        {col1, <<"foobar">>},
        {col2, -9223372036854775806},
        {col3, <<1,2,3,4,5,6,7,8,9,10>>},
        {col4, false},
        {col5, {1234, -5}},
        {col6, -5.1235131241220e-6},
        {col7, -5.12351e-6},
        {col8, -2147483646},
        {col9, 1984336643},
        {col10, <<22,6,195,126,110,122,64,242,135,15,38,179,46,108,22,64>>},
        {col11, <<"åäö"/utf8>>},
        {col12, 123124211928301970128391280192830198049113123},
        {col13, <<250,10,224,94,87,197,17,227,156,99,146,79,0,0,0,195>>},
        {col14, {0,0,0,0,0,0,0,0}}
    ]}),
    {ok, void} = cqerl:run_query(Client, InsQ#cql_query{
        statement="INSERT INTO entries2(col1, col11) values (?, ?);",
        values=RRow3=[ {col1, foobaz}, {col11, 'åäö'} ]
    }),
    
    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{statement = <<"SELECT * FROM entries2;">>}),
    {Row1, Result1} = cqerl:next(Result),
    {Row2, Result2} = cqerl:next(Result1),
    {Row3, Result3} = cqerl:next(Result2),
    lists:foreach(fun
        (Row) -> 
            ReferenceRow = case proplists:get_value(col1, Row) of
                <<"hello">> -> RRow1;
                <<"foobar">> -> RRow2;
                <<"foobaz">> -> RRow3
            end,
            lists:foreach(fun
                ({col13, _}) -> true = uuid:is_v1(proplists:get_value(col13, Row));
                ({col10, _}) -> true = uuid:is_v4(proplists:get_value(col10, Row));
                ({col9, _}) -> ok; %% Yeah, I know...
                
                ({col1, Key}) when is_list(Key) ->
                    Val = list_to_binary(Key),
                    Val = proplists:get_value(col1, Row);

                ({Col, Key}) when is_atom(Key), Col == col1 orelse Col == col11 ->
                    Val = atom_to_binary(Key, latin1),
                    Val = proplists:get_value(Col, Row);
                    
                ({col7, Val0}) ->
                    Val = round(Val0 * 1.0e11),
                    Val = round(proplists:get_value(col7, Row) * 1.0e11);
                ({Key, Val}) ->
                    Val = proplists:get_value(Key, Row, null)
            end, ReferenceRow)
    end, [Row1, Row2, Row3]),
    cqerl:close_client(Client),
    [Row1, Row2, Row3].

custom_encoders(Config) ->
    Client = get_client(Config),

    Cols = datatypes_columns([varchar, varchar, varchar]),
    CreationQ = <<"CREATE TABLE entries2_1(",  Cols/binary, " PRIMARY KEY(col1, col2, col3));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>, table = <<"entries2_1">>}} =
        cqerl:run_query(Client, CreationQ),

    InsQ = #cql_query{statement = <<"INSERT INTO entries2_1(col1, col2, col3) VALUES (?, ?, ?)">>},
    {ok, void} = cqerl:run_query(Client, InsQ#cql_query{values=RRow1=[
        {col1, <<"test">>},
        {col2, <<"hello">>},
        {col3, <<"testing tuples">>}
    ]}),
    {ok, void} = cqerl:run_query(Client, InsQ#cql_query{values=RRow2=[
        {col1, <<"test">>},
        {col2, <<"nice to have">>},
        {col3, <<"custom encoder">>}
    ]}),

    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{
        statement = <<"SELECT * FROM entries2_1 WHERE col1 = ? AND (col2,col3) IN ?;">>,

        values = [
            {col1, <<"test">>},
            {'in(col2,col3)', [
                [<<"hello">>,<<"testing tuples">>],
                [<<"nice to have">>,<<"custom encoder">>]
            ]}
        ],

        % provide custom encoder for TupleType
        value_encode_handler = fun({Type={custom, <<"org.apache.cassandra.db.marshal.TupleType", _Rest/binary>>}, List}, Query) ->
            GetElementBinary = fun(Value) -> 
                Bin = cqerl_datatypes:encode_data({text, Value}, Query),
                Size = size(Bin),
                <<Size:32/big-signed-integer, Bin/binary>>
            end,

            << << (GetElementBinary(Value))/binary >> || Value <- List >>
        end
    }),

    [RRow1, RRow2] = cqerl:all_rows(Result),

    cqerl:close_client(Client),
    ok.

collection_types(Config) ->
    Client = get_client(Config),
    
    CreationQ = <<"CREATE TABLE entries3(key varchar, numbers list<int>, names set<varchar>, phones map<varchar, varchar>, PRIMARY KEY(key));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>, table = <<"entries3">>}} =
        cqerl:run_query(Client, CreationQ),
        
    {ok, void} = cqerl:run_query(Client, #cql_query{
        statement = <<"INSERT INTO entries3(key, numbers, names, phones) values (?, ?, ?, ?);">>,
        values = [
            {key, "First collection"},
            {numbers, [1,2,3,4,5]},
            {names, ["matt", "matt", "yvon"]},
            {phones, [{<<"home">>, <<"418-123-4545">>}, {"work", "555-555-5555"}]}
        ]
    }),
    
    {ok, void} = cqerl:run_query(Client, #cql_query{
        statement = "UPDATE entries3 SET names = names + {'martin'} WHERE key = ?",
        values = [{key, "First collection"}]
    }),
    
    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{statement = "SELECT * FROM entries3;"}),
    Row = cqerl:head(Result),
    <<"First collection">> = proplists:get_value(key, Row),
    [1,2,3,4,5] = proplists:get_value(numbers, Row),
    Names = proplists:get_value(names, Row),
    3 = length(Names),
    true = lists:member(<<"matt">>, Names),
    true = lists:member(<<"yvon">>, Names),
    true = lists:member(<<"martin">>, Names),
    lists:foreach(fun
        ({<<"home">>, <<"418-123-4545">>}) -> ok;
        ({<<"work">>, <<"555-555-5555">>}) -> ok;
        (_) -> throw(unexpected_value)
    end, proplists:get_value(phones, Row)),
    cqerl:close_client(Client),
    Row.

counter_type(Config) ->
    Client = get_client(Config),
    
    CreationQ = <<"CREATE TABLE entries4(key varchar, count counter, PRIMARY KEY(key));">>,
    ct:log("Executing : ~s~n", [CreationQ]),
    {ok, #cql_schema_changed{change_type=created, keyspace = <<"test_keyspace_2">>, table = <<"entries4">>}} =
        cqerl:run_query(Client, CreationQ),
    
    {ok, void} = cqerl:run_query(Client, #cql_query{
        statement = <<"UPDATE entries4 SET count = count + ? WHERE key = ?;">>,
        values = [
            {key, "First counter"},
            {count, 18}
        ]
    }),
    
    {ok, void} = cqerl:run_query(Client, #cql_query{
        statement = "UPDATE entries4 SET count = count + 10 WHERE key = ?;",
        values = [{key, "First counter"}]
    }),
    
    {ok, Result=#cql_result{}} = cqerl:run_query(Client, #cql_query{statement = "SELECT * FROM entries4;"}),
    Row = cqerl:head(Result),
    <<"First counter">> = proplists:get_value(key, Row),
    28 = proplists:get_value(count, Row),
    cqerl:close_client(Client),
    Row.


inserted_rows(0, Q, Acc) ->
    lists:reverse(Acc);
inserted_rows(N, Q, Acc) ->
    ID = list_to_binary(io_lib:format("~B", [N])),
    inserted_rows(N-1, Q, [ Q#cql_query{values=[
        {id, ID}, 
        {age, 10+N}, 
        {email, list_to_binary(["test", ID, "@gmail.com"])}
    ]} | Acc ]).

batches_and_pages(Config) ->
    Client = get_client(Config),
    T1 = now(),
    N = 100,
    Bsz = 25,
    {ok, void} = cqerl:run_query(Client, "TRUNCATE entries1;"),
    Q = #cql_query{statement = <<"INSERT INTO entries1(id, age, email) VALUES (?, ?, ?)">>},
    Batch = #cql_query_batch{queries=inserted_rows(N, Q, [])},
    ct:log("Batch : ~w~n", [Batch]),
    {ok, void} = cqerl:run_query(Client, Batch),
    AddIDs = fun (Result, IDs0) ->
        lists:foldr(fun (Row, IDs) ->
                        ID = proplists:get_value(id, Row),
                        {IDint, _} = string:to_integer(binary_to_list(ID)),
                        IDint = proplists:get_value(age, Row) - 10,
                        IDsize = size(ID),
                        << _:4/binary, ID:IDsize/binary, _Rest/binary >> = proplists:get_value(email, Row),
                        gb_sets:add(ID, IDs) 
                    end, 
                    IDs0, cqerl:all_rows(Result))
    end,
    {ok, Result} = cqerl:run_query(Client, #cql_query{page_size=Bsz, statement="SELECT * FROM entries1;"}),
    IDs1 = AddIDs(Result, gb_sets:new()),
    
    {ok, Result2} = cqerl:fetch_more(Result),
    Ref1 = cqerl:fetch_more_async(Result2),
    IDs2 = AddIDs(Result2, IDs1),
    receive
        {result, Ref1, Result3} ->
            Ref2 = cqerl:fetch_more_async(Result3),
            IDs3 = AddIDs(Result3, IDs2)
    end,
    receive
        {result, Ref2, Result4} ->
            IDs4 = AddIDs(Result4, IDs3),
            N = gb_sets:size(IDs4)
    end,
    ct:log("Time elapsed inserting ~B entries and fetching in batches of ~B: ~B ms", [N, Bsz, round(timer:now_diff(now(), T1)/1000)]),
    cqerl:close_client(Client).

get_client(Config) ->
    Host = proplists:get_value(host, Config),
    SSL = proplists:get_value(prepared_ssl, Config),
    Auth = proplists:get_value(auth, Config, undefined),
    Keyspace = proplists:get_value(keyspace, Config),
    PoolMinSize = proplists:get_value(pool_min_size, Config),
    PoolMaxSize = proplists:get_value(pool_max_size, Config),
    
    io:format("Options : ~w~n", [[
        {ssl, SSL}, {auth, Auth}, {keyspace, Keyspace},
        {pool_min_size, PoolMinSize}, {pool_max_size, PoolMaxSize}
        ]]),
        
    {ok, Client} = cqerl:new_client(Host, [{ssl, SSL}, {auth, Auth}, {keyspace, Keyspace}, 
                                           {pool_min_size, PoolMinSize}, {pool_max_size, PoolMaxSize} ]),
    Client.
