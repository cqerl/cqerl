
-module(cqerl_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {one_for_all, 5, 10}, [
      ?CHILD(cqerl_cache, worker),
      ?CHILD(cqerl_batch_sup, supervisor),
      ?CHILD(cqerl_processor_sup, supervisor),
      ?CHILD(cqerl_client_sup, supervisor),
      ?CHILD(cqerl_hash, worker),
      ?CHILD(cqerl_cluster, worker)
    ]}}.
