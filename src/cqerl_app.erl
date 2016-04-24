-module(cqerl_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([mode/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    application:start(hash),
    cqerl_sup:start_link().

stop(_State) ->
    ok.

mode() ->
    application:get_env(cqerl, mode, hash).
