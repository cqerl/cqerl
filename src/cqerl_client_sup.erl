-module(cqerl_client_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         add_clients/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor

-define(DEFAULT_NUM_CLIENTS, 20).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, main).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(main) ->
    {ok,
     {
      #{
       strategy => simple_one_for_one
      },
      [#{
        id => key_sup,
        start => {supervisor, start_link, [?MODULE]},
        restart => transient,
        type => supervisor
      }]
     }};

init([key, Key = {Node, _Opts}, FullOpts, OptGetter, ChildCount]) ->
    {ok,
     {
      #{
       strategy => one_for_one,
       intensity => ChildCount + 5,
       period => 10
      },
      [
        client_spec(Key, Node, FullOpts, OptGetter, I) ||
        I <- lists:seq(1, ChildCount)
      ]}}.

client_spec(Key, Node, FullOpts, OptGetter, I) ->
    #{
       id => {cqerl_client, Key, I},
       start => {cqerl_client, start_link, [Node, FullOpts, OptGetter, Key]}
     }.

add_clients(Node, Opts) ->
    Key = cqerl_client:make_key(Node, Opts),
    ChildCount = child_count(Key),
    GlobalOpts = cqerl:get_global_opts(),
    OptGetter = cqerl:make_option_getter(Opts, GlobalOpts),
    FullOpts = [ {ssl, OptGetter(ssl)}, {keyspace, OptGetter(keyspace)} ],

    case supervisor:start_child(?MODULE, [[key, Key, FullOpts, OptGetter, ChildCount]]) of
        {ok, SupPid} -> {ok, {ChildCount, SupPid}};
        {error, E} -> {error, E}
    end.


child_count(_Key) ->
    application:get_env(cqerl, num_clients, 20).
