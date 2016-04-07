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

init(Args = [key, Key = {Node, _Opts}, FullOpts, ChildCount]) ->
    ct:log("BJD Got start ~p", [Args]),
    {ok,
     {
      #{
       strategy => one_for_one,
       intensity => 5,
       period => 10
      },
      [
        client_spec(Key, Node, FullOpts, I) ||
        I <- lists:seq(1, ChildCount)
      ]}}.

client_spec(Key, Node, FullOpts, I) ->
    #{
       id => {cqerl_client, Key, I},
       start => {cqerl_client, start_link, [Node, FullOpts, Key]}
     }.

add_clients(Node, Opts) ->
    Key = {Node, Opts},
    ChildCount = child_count(Key),
    GlobalOpts = cqerl:get_global_opts(),
    OptGetter = cqerl:make_option_getter(Opts, GlobalOpts),
    FullOpts = [ {auth, OptGetter(auth)}, {ssl, OptGetter(ssl)},
                 {keyspace, OptGetter(keyspace)} ],

    case supervisor:start_child(?MODULE, [[key, Key, FullOpts, ChildCount]]) of
        {ok, SupPid} -> {ok, {ChildCount, SupPid}};
        {error, E} -> {error, E}
    end.


child_count(_Key) ->
    application:get_env(cqerl, num_clients, 20).
