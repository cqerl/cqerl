-module(cqerl_processor_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1, new_processor/3]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I), {I, {I, start_link, []}, transient, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

new_processor(UserQuery, Msg, ProtocolVersion) ->
    supervisor:start_child(?MODULE, [self(), UserQuery, Msg, ProtocolVersion]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {simple_one_for_one, 5, 10}, [ ?CHILD(cqerl_processor) ]}}.
