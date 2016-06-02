-module(cqerl_client_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         add_clients/4]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor

-define(DEFAULT_NUM_CLIENTS, 20).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, main).

add_clients(Name, Node, Opts, Count) ->
    case supervisor:start_child(?MODULE, client_sup_spec(Name, Node, Opts, Count)) of
        {ok, SupPid} -> {ok, {Count, SupPid}};
        {error, E} -> {error, E}
    end.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(main) ->
    {ok,
     {
      #{},
      []
     }};

init({clients, Name, Node, Opts, ChildCount}) ->
    {ok,
     {
      #{},
      [
        client_spec(Name, Node, Opts, I) ||
        I <- lists:seq(1, ChildCount)
      ]
     }}.

%% ===================================================================
%% Private functions
%% ===================================================================

client_sup_spec(Name, Node, Opts, Count) ->
    ID = {Name, Node},
    #{
      id => {cqerl_client_sup, ID},
      start => {supervisor, start_link, [?MODULE, {clients, Name, Node, Opts, Count}]},
      type => supervisor
     }.

client_spec(Name, Node, Opts, I) ->
    #{
       id => {cqerl_client, Node, I},
       start => {cqerl_client, start_link, [Name, Node, Opts]}
     }.
