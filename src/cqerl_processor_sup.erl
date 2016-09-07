-module(cqerl_processor_sup).

-include("cqerl_protocol.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1, new_processor/4]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I), {I, {I, start_link, []}, transient, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec new_processor(Node :: cqerl:cqerl_node(),
                    UserQuery :: term(),
                    Msg :: cqerl_processor:processor_message(),
                    ProtocolVersion :: non_neg_integer()) -> ok.
new_processor(Node, UserQuery, Msg, ProtocolVersion) ->
    {ok, _Pid} = supervisor:start_child(
                   ?MODULE, [self(), Node, UserQuery, Msg, ProtocolVersion]),
    ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, { {simple_one_for_one, 5, 10}, [ ?CHILD(cqerl_processor) ]}}.
