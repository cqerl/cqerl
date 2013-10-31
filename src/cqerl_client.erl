-module(cqerl_client).
-behaviour(gen_fsm).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2, new_user/3]).

%% ------------------------------------------------------------------
%% gen_fsm Function Exports
%% ------------------------------------------------------------------

-export([init/1, terminate/3,
         starting/2,  starting/3, 
         handle_event/3, handle_sync_event/4, handle_info/3, 
         code_change/4]).
         
-record(record, {field = value}).

new_user(Pid, Ref, UserPid) -> ok.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Inet, Opts) ->
    gen_fsm:start_link(?MODULE, [Inet, Opts], []).

%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init([{Ip, Port}, Opts]) ->
    io:format("Initiating a connection with cassandra with data ~w~n", [{Ip, Port}]),
    {ok, state_name, starting}.

starting(_Event, State) ->
    {next_state, starting, State}.

starting(_Event, _From, State) ->
    {reply, ok, starting, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

signal_asleep() ->
  gen_server:cast(cqerl, {client_asleep, self()}).

signal_busy() ->
  gen_server:cast(cqerl, {client_busy, self()}).
