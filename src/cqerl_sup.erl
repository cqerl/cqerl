-module(cqerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok,
     {
      #{strategy =>  one_for_all,
        intensity => 5,
        period =>    10},
      [
       child(cqerl_cache, worker),
       child(cqerl_batch_sup, supervisor),
       child(cqerl_processor_sup, supervisor),
       child(cqerl_schema, worker),
       child(cqerl_hash, worker),
       child(cqerl_client_sup, supervisor)
      ]}}.

child(Module, Type) ->
    #{id =>       Module,
      start =>    {Module, start_link, []},
      restart =>  permanent,
      shutdown => 5000,
      type =>     Type,
      modules =>  [Module]}.
