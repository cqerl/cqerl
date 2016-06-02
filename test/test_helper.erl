-module(test_helper).

-include("cqerl.hrl").

-export([standard_setup/1,
         create_keyspace/2,
         requirements/0
        ]).


standard_setup(Config) ->
    application:ensure_all_started(cqerl),
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
      {host, ct:get_config(host)},
      {protocol_version, ct:get_config(protocol_version)} ] ++ Config.

create_keyspace(KS, _Config) ->
    Q = <<"CREATE KEYSPACE ", KS/binary, " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};">>,
    D = <<"DROP KEYSPACE ", KS/binary>>,
    case cqerl:run_query(#cql_query{statement=Q}) of
        {ok, #cql_schema_changed{change_type=created, keyspace = KS}} -> ok;
        {error, {16#2400, _, {key_space, KS}}} ->
            {ok, #cql_schema_changed{change_type=dropped, keyspace = KS}} = cqerl:run_query(D),
            {ok, #cql_schema_changed{change_type=created, keyspace = KS}} = cqerl:run_query(Q)
    end.

requirements() ->
    [
     {require, ssl, cqerl_test_ssl},
     {require, protocol_version, cqerl_protocol_version},
     {require, auth, cqerl_test_auth},
     % {require, keyspace, cqerl_test_keyspace},
     {require, host, cqerl_host}
    ].

