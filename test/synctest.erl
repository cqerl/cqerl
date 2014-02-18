-module(synctest).

-compile(export_all).

-include("cqerl.hrl").

start(N, Dt) ->
    {ok, Config} = application:get_env(cqerl, cqerl),
    sync_test(N, Dt, Config).

sync_test(N, Dt, Config) ->
    Iterator = fun
        (_F, 0, _M) -> ok;
        (F, N, M) ->
            Client = get_client(Config),
            erlang:send_after(trunc(Dt*N), self(), {sync_request, self(),Client}),
            F(F, N-1, M+1)
        end,
    Iterator(Iterator, N, 0),

    T1 = erlang:now(),
    DelayLooper = fun
        (_F, 0, 0, Acc) ->
            Acc;
        (F, N, M, Acc) ->
            receive
                {result, Tag} ->
                    {T, Client} = gb_trees:get(Tag, Acc),
                    {Pid, _} = Client,
                    cqerl:close_client(Client),
                    F(F, N, M-1, gb_trees:update(Tag, {Pid, timer:now_diff(erlang:now(), T)}, Acc));
                {sync_request, P, Client} ->
                    Tag = make_ref(),
                    Now = erlang:now(),
                    Pid = spawn(?MODULE, insert, []),
                    Pid ! {P, Client, Tag, N},
                    F(F, N-1, M, gb_trees:insert(Tag, {Now, Client}, Acc));
                OtherMsg ->
                    io:format("Unexpected response ~p", [OtherMsg])
            after 1000 ->
                io:format("All delayed messages did not arrive in time~n")
            end
    end,
    Deltas = gb_trees:to_list(DelayLooper(DelayLooper, N, N, gb_trees:empty())),
    Pids = lists:foldr(fun
        ({_, {Pid, _}}, Acc) ->
            case lists:member(Pid, Acc) of
                true -> Acc;
                false -> [Pid | Acc]
            end
    end, [], Deltas),

    DistinctPids = length(Pids),
    Sum = lists:foldr(fun ({_Tag, {_, T}}, Acc) -> Acc + T end, 0, Deltas),
    io:format("~w requests sent to ~w clients over ~w seconds -- mean request roundtrip : ~w seconds --~n",
    [N, DistinctPids, (timer:now_diff(erlang:now(), T1))/1.0e6, (Sum/N)/1.0e6]).
    
insert() ->
    receive
        {Pid, Client, Tag, N} ->
            case cqerl:run_query(Client, #cql_query{statement = list_to_binary("INSERT INTO synctable(key, value) VALUES('keyprefix"++ integer_to_list(N) ++"','defaultvaluehere');"), consistency = 1}) of
                {ok, void} ->
                    Pid ! {result, Tag},
                    ok;
                Fail ->
                    io:format("FAILED ~p~n",[Fail]),
                    ok
            end;
        OtherMsg ->
            io:format("Wrong msg ~p~n",[OtherMsg]),
            ok
    after 1000 ->
        io:format("Timeout~n",[]),
        timeout
    end.


get_client(Config) ->
    Host = proplists:get_value(host, Config),
    SSL = proplists:get_value(prepared_ssl, Config),
    Auth = proplists:get_value(auth, Config, undefined),
    Keyspace = proplists:get_value(keyspace, Config),
    PoolMinSize = proplists:get_value(pool_min_size, Config),
    PoolMaxSize = proplists:get_value(pool_max_size, Config),

    {ok, Client} = cqerl:new_client(Host, [{ssl, SSL}, {auth, Auth}, {keyspace, Keyspace},
                                           {pool_min_size, PoolMinSize}, {pool_max_size, PoolMaxSize} ]),
    Client.
