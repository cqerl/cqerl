-module(cqerl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    {ok, Pid} = cqerl_sup:start_link(),
    apply_config(),
    {ok, Pid}.

stop(_State) ->
    ok.

apply_config() ->
    Groups = application:get_env(cqerl, client_groups, []),
    lists:foreach(fun(G) -> start_group(G) end, Groups).

start_group({client_group, Opts}) ->
    Name = proplists:get_value(name, Opts, undefined),
    Hosts = proplists:get_value(hosts, Opts),
    ClientsPerServer = proplists:get_value(clients_per_server, Opts),
    GroupOpts = proplists:get_value(opts, Opts, []),
    G = cqerl:add_group(Name, Hosts, GroupOpts, ClientsPerServer),
    cqerl:wait_for_group(G);

start_group(_) ->
    error(bad_group_config).
