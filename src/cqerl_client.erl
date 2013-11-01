-module(cqerl_client).
-behaviour(gen_fsm).
-define(SERVER, ?MODULE).

-include("cqerl_protocol.hrl").

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
         
-record(client_state, {
  authmod :: atom(),
  authstate :: any(),
  authargs :: list(any()),
  inet :: any(),
  socket :: gen_tcp:socket() | ssl:sslsocket(),
  compression_type = undefined :: undefined | snappy | lz4,
  users = [] :: list({pid(), reference()}),
  trans :: atom()
}).

new_user(Pid, Ref, UserPid) -> ok.

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Inet, Opts) ->
    gen_fsm:start_link(?MODULE, [Inet, Opts], []).

%% ------------------------------------------------------------------
%% gen_fsm Function Definitions
%% ------------------------------------------------------------------

init([Inet, Opts]) ->
  io:format("Initiating a connection with cassandra with data ~w~n", [Inet]),
  case create_socket(Inet, Opts) of
    {ok, Socket, Transport} ->
      {auth, {AuthHandler, AuthArgs}} = proplists:lookup(auth, Opts),
      {ok, OptionsFrame} = cqerl_protocol:options_frame(#cqerl_frame{}),
      send(State = #client_state{ socket=Socket, authmod=AuthHandler, trans=Transport, inet=Inet }, OptionsFrame),
      activate_socket(State),
      {ok, starting, State};
    
    {error, Reason} ->
      {stop, {connection_error, Reason}}
  end.

starting(_Event, State) ->
  {next_state, starting, State}.

starting(_Event, _From, State) ->
  {reply, ok, starting, State}.

handle_event(_Event, StateName, State) ->
  {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
  {reply, ok, StateName, State}.

handle_info({ Transport, Socket, BinaryMsg }, starting, State = #client_state{ socket=Socket, trans=Transport }) ->
  << _:3/binary, OpCode:8/big-integer, _/binary >> = BinaryMsg,
  io:format("Got message with op code : ~w, <~w>~n", [OpCode, BinaryMsg]),
  
  case cqerl_protocol:response_frame(#cqerl_frame{}, BinaryMsg) of
    
    %% Server tells us what version and compression algorithm it supports
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_SUPPORTED}, Payload, _} ->
      Compression = choose_compression_type(proplists:lookup('COMPRESSION', Payload)),
      SelectedVersion = choose_cql_version(proplists:lookup('CQL_VERSION', Payload)),
      {ok, StartupFrame} = cqerl_protocol:startup_frame(#cqerl_frame{}, #cqerl_startup_options{compression=Compression, 
                                                                                               cql_version=SelectedVersion}),
      send(State, StartupFrame),
      activate_socket(State),
      {next_state, starting, State#client_state{compression_type=Compression}};
    
    %% Server tells us all is clear, we can start to throw it queries
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_READY}, _, _} ->
      io:format("Ready!", []),
      activate_socket(State),
      {next_state, live, State};
    
    %% Server tells us we need to authenticate
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_AUTHENTICATE}, Body, _} ->
      #client_state{ authmod=AuthMod, authargs=AuthArgs, inet=Inet } = State,
      case AuthMod:auth_init(AuthArgs, Body, Inet) of
        {close, Reason} ->
          close_socket(Socket),
          {stop, {auth_client_closed, Reason}, State};
        
        {reply, Reply, AuthState} ->
          {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
          send(State, AuthFrame),
          activate_socket(State),
          {next_state, starting, State#client_state{ authstate=AuthState }}
      end;
    
    %% Server tells us we need to give another piece of data
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_AUTH_CHALLENGE}, Body, _} ->
      #client_state{ authmod=AuthMod, authstate=AuthState } = State,
      case AuthMod:auth_handle_challenge(Body, AuthState) of
        {close, Reason} ->
          close_socket(Socket),
          {stop, {auth_client_closed, Reason}, State};
        
        {reply, Reply, AuthState} ->
          {ok, AuthFrame} = cqerl_protocol:auth_frame(base_frame(State), Reply),
          send(State, AuthFrame),
          activate_socket(State),
          {next_state, starting, State#client_state{ authstate=AuthState }}
      end;
    
    %% Server tells us something screwed up while authenticating
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_ERROR}, {16#0100, AuthErrorDescription, _}, _} ->
      #client_state{ authmod=AuthMod, authstate=AuthState } = State,
      AuthMod:auth_handle_error(AuthErrorDescription, AuthState),
      close_socket(Socket),
      {stop, {auth_server_refused, AuthErrorDescription}, State};
    
    %% Server tells us the authentication went well, we can start shooting queries
    {ok, R=#cqerl_frame{opcode=?CQERL_OP_AUTH_SUCCESS}, Body, _} ->
      #client_state{ authmod=AuthMod, authstate=AuthState } = State,
      case AuthMod:auth_handle_success(Body, AuthState) of
        {close, Reason} ->
          close_socket(Socket),
          {stop, {auth_client_closed, Reason}, State};
        
        ok ->
          io:format("Ready!", []),
          activate_socket(State),
          {next_state, live, State#client_state{ authstate=undefined }}
      end
      
  end;

handle_info(Info, StateName, State) ->
  io:format("Received message ~w~n", [{StateName, Info, State#client_state.trans, State#client_state.socket}]),
  {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
  ok.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.





%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------



send(#client_state{trans=tcp, socket=Socket}, Data) ->
  gen_tcp:send(Socket, Data);
send(#client_state{trans=ssl, socket=Socket}, Data) ->
  ssl:send(Socket, Data).




create_socket({Addr, Port}, Opts) ->
  BaseOpts = [{active, false}, {nodelay, true}, binary],
  Result = case proplists:lookup(ssl, Opts) of
    {ssl, false} ->
      Transport = tcp,
      gen_tcp:connect(Addr, Port, BaseOpts, 2000);
    {ssl = Transport, true} -> 
      ssl:connect(Addr, Port, BaseOpts, 2000);
    {ssl = Transport, SSLOpts} when is_list(SSLOpts) ->
      ssl:connect(Addr, Port, SSLOpts ++ BaseOpts, 2000)
  end,
  case Result of
    {ok, Socket} -> {ok, Socket, Transport};
    Other -> Other
  end.




close_socket(#client_state{trans=ssl, socket=Socket}) ->
  ssl:close(Socket);
close_socket(#client_state{trans=tcp, socket=Socket}) ->
  gen_tcp:close(Socket).




activate_socket(#client_state{trans=ssl, socket=Socket}) ->
  ssl:setopts(Socket, [{active, once}]);
activate_socket(#client_state{trans=tcp, socket=Socket}) ->
  inet:setopts(Socket, [{active, once}]).




signal_asleep() ->
  gen_server:cast(cqerl, {client_asleep, self()}).

signal_busy() ->
  gen_server:cast(cqerl, {client_busy, self()}).




choose_compression_type({'COMPRESSION', Choice}) ->
  SupportedCompression = lists:map(fun (CompressionNameBin) -> binary_to_atom(CompressionNameBin, latin1) end, Choice),
  case lists:member(lz4, SupportedCompression) andalso module_exists(lz4) of
    true -> lz4;
    _ -> case lists:member(snappy, SupportedCompression) andalso module_exists(snappy) of
      true -> snappy;
      _ -> undefined
    end
  end;

choose_compression_type(none) -> undefined.




choose_cql_version({'CQL_VERSION', Versions}) ->
  SemVersions = lists:sort(
    fun (SemVersion1, SemVersion2) ->
        case semver:compare(SemVersion1, SemVersion2) of
          -1 -> false;
          _ -> true
        end
    end,
    lists:map(fun (Version) -> semver:parse(Version) end, Versions)
  ),
  case application:get_env(cqerl, preferred_cql_version, none) of
    none -> 
      [GreaterVersion|_] = SemVersions;
    
    Version1 ->
      [GreaterVersion|_] = lists:dropwhile(fun (SemVersion) ->
          case semver:compare(SemVersion, Version1) of
            1 -> true;
            _ -> false
          end
      end, SemVersions)
      
  end,
  [_v | Version] = semver:vsn_string(GreaterVersion),
  list_to_binary(Version).




base_frame(#client_state{compression_type=CompressionType}) ->
  #cqerl_frame{compression_type=CompressionType}.




module_exists(Module) ->
    case is_atom(Module) of
        true ->
            try Module:module_info() of
                _InfoList ->
                    true
            catch
                _:_ ->
                    false
            end;
 
        false ->
            false
    end.
