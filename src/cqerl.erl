%% @author Mathieu D'Amours <matt@forest.io>
%% @doc Main interface to CQErl cassandra client.

-module(cqerl).
-behaviour(gen_server).

-export([
  new_client/0, new_client/1, close_client/1,
  run_query/2, send_query/2,
  fetch_more/1, fetch_more_async/1,
  
  start_link/0
]).

-export([
  init/1, terminate/2, code_change/3,
  handle_call/3, handle_cast/2, handle_info/2
]).

-include("cqerl.hrl").

-opaque client() :: {pid(), reference()}.
-opaque result_continuation() :: {pid(), binary()}.
-opaque query_tag() :: {pid(), reference()}.

-type inet() :: { inet:ip_address() | string(), Port :: integer() }.

-export_type([client/0, result_continuation/0, query_tag/0, inet/0]).

-record(cqerl_state, {
  checked_env = false :: boolean(),
  clients             :: ets:tab(),
  nodes               :: list(tuple()),
  retrying = false    :: boolean()
}).
-record(cql_client, {
  node  :: tuple(),
  pid   :: pid(),
  busy  :: boolean()
}).

-define(RETRY_INITIAL_DELAY, 10).
-define(RETRY_MAX_DELAY, 1000).
-define(RETRY_EXP_FACT, 1.15).

-spec prepare_client(Inet :: inet(), Opts :: list(tuple() | atom())) -> ok.
prepare_client(Inet, Opts) ->
  gen_server:cast(?MODULE, {prepare_client, prepare_node_info(Inet), Opts}).

-spec prepare_client(Inet :: inet()) -> ok.
prepare_client(Inet) -> prepare_client(Inet, []).




-spec new_client() -> client().
new_client() ->
  gen_server:call(?MODULE, get_any_client).

-spec new_client(Inet :: inet()) ->  client().
new_client({}) ->
  new_client({{127, 0, 0, 1}, 9160}, []);
new_client(Inet) -> 
  new_client(Inet, []).

-spec new_client(Inet :: inet(), Opts :: list(tuple() | atom())) -> client() | {error, no_client_available}.
new_client(Inet, Opts) ->
  gen_server:call(?MODULE, {get_client, prepare_node_info(Inet), Opts}).




%% @doc Close a client that was previously allocated with {@link new_client/0} or {@link new_client/1}.
%%
%% This function will return immediately no matter if the client has already been closed or not.

-spec close_client(ClientRef :: client()) -> no_return().
close_client(ClientRef) ->
  cqerl_client:remove_user(ClientRef).
  
  
  

%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%      return with the result from Cassandra (synchronously).
%% The <code>Query</code> parameter can be a string, a binary UTF8 string or a <code>#cql_query{}</code> record
%%
%% <pre>#cql_query{
%%   query :: binary(),
%%   reusable :: boolean(),
%%   consistency :: consistency_level(),
%%   named :: boolean(),
%%   bindings :: list(number() | boolean() | binary() | list() | inet() | ) | list(tuple())
%% }</pre>
%%
%% <em>Reusable</em> is a boolean indicating whether the query should be reused. <em>Reusing a query</em> means sending it to Cassandra to be prepared, which allows
%% later executions of the <strong>same query</strong> to be performed faster. This parameter is <code>true</code> by default when you provide bindings in the query (positional <code>?</code>
%% parameters or named <code>:var</code> parameters), and <code>false</code> by default when you don't. You can override the defaults.
%%
%% <em>Consistency</em> one of constants defined in <code>cqerl.hrl</code>, namely <code>?CQERL_CONSISTENCY_[Const]</code> where <code>Const</code> can be <code>ANY</code>, <code>ONE</code>,
%% <code>TWO</code>, <code>THREE</code>, <code>QUORUM</code>, <code>ALL</code>, <code>LOCAL_QUORUM</code>, <code>EACH_QUORUM</code>, <code>SERIAL</code>,
%% <code>LOCAL_SERIAL</code> or <code>LOCAL_ONE</code>.
%%
%% How <em>bindings</em> is used depends on the <em>named</em> value. <em>Named</em> is a boolean value indicating whether the parameters in the query are named parameters (<code>:var1</code>). Otherwise,
%% they are assumed to be positional (<code>?</code>). In the first case, <em>bindings</em> is a property list (see <a href="http://www.erlang.org/doc/man/proplists.html">proplists</a>) where keys match the
%% parameter names. In the latter case, <em>bindings</em> should be a simple list of values.

-spec run_query(ClientRef :: client(), Query :: binary() | string() | #cql_query{}) -> #cql_result{}.
run_query(ClientRef, Query) -> 
  cqerl_client:query(ClientRef, Query).




%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%      return with the result from Cassandra (synchronously).

-spec fetch_more(Continuation :: result_continuation()) -> #cql_result{}.
fetch_more(Continuation) -> 
  cqerl_client:fetch_more(Continuation).




%% @doc Send a query to be executed asynchronously. This method returns immediately with a unique tag. 
%%
%% When a successful response is received from cassandra, a <code>{result, Tag, Result :: #cql_result{}}</code> 
%% message is sent to the calling process. 
%%
%% If there is an error with the query, a <code>{error, Tag, Error :: #cql_error{}}</code> will be sent to the calling process.
%%
%% Neither of these messages will be called if the connection is dropped before receiving a response (see {@link new_client/0} for
%% how to handle this case).

-spec send_query(ClientRef :: client(), Query :: binary() | string() | #cql_query{}) -> query_tag().
send_query(ClientRef, Query) ->
  cqerl_client:query_async(ClientRef, Query, self()).




%% @doc Asynchronously fetch the next page of result from cassandra for a given continuation.
%%
%% A success or error message will be sent in response some time later (see {@link send_query/2} for details) unless the
%% connection is dropped.

-spec fetch_more_async(Continuation :: result_continuation()) -> query_tag().
fetch_more_async(Continuation) -> 
  cqerl_client:fetch_more_async(Continuation, self()).




%% ====================
%% Gen_Server Callbacks
%% ====================

start_link() ->
  random:seed(now()),
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
  io:format("== Starting CQErl frontend. ==~n", []),
  {ok, #cqerl_state{nodes=[], clients = ets:new(clients, [set, private, {keypos, #cql_client.pid}])}, 0}.




handle_call(get_any_client, _From, State=#cqerl_state{nodes=[]}) ->
  {reply, {error, no_configured_node}, State};

handle_call(get_any_client, From = {UserPid, _Tag}, State = #cqerl_state{nodes=Nodes, clients=Clients, retrying=Retrying}) ->
  case select_client(Clients, #cql_client{busy=false, _='_'}, UserPid) of
    no_available_client when Retrying ->
      retry;
      
    no_available_client ->
      erlang:send_after(?RETRY_INITIAL_DELAY, self(), {retry, get_any_client, From, ?RETRY_INITIAL_DELAY}),
      {noreply, State};
    
    Client ->
      {reply, Client, State}
  end;

handle_call({get_client, Node, Opts}, From = {UserPid, _Tag}, State = #cqerl_state{clients=Clients, nodes=Nodes, retrying=Retrying}) ->
  case lists:member(Node, Nodes) of
    false ->
      GlobalOpts = application:get_env(cqerl, default_options, []),
      new_pool(Node, Opts, GlobalOpts),
      case pooler:take_member(pool_from_node(Node)) of
        error_no_members -> 
          {reply, {error, no_available_clients}, State};
        Pid ->
          ets:insert(Clients, #cql_client{node=Node, busy=false, pid=Pid}),
          {reply, add_client(UserPid, Pid), State#cqerl_state{nodes=[Node|Nodes]}}
      end;
    
    _ ->
      case select_client(Clients, #cql_client{node=Node, busy=false, pid='_'}, UserPid) of
        no_available_client when Retrying ->
          retry;
        
        no_available_client ->
          erlang:send_after(?RETRY_INITIAL_DELAY, self(), {retry, {get_client, Node, Opts}, From, ?RETRY_INITIAL_DELAY}),
          {noreply, State};
        
        Client ->
          {reply, Client, State}
      end
  end;

handle_call(_Msg, _From, State) ->
  {reply, ok, State}.




handle_cast({prepare_client, Node, Opts}, State=#cqerl_state{nodes=Nodes}) ->
  case lists:member(Node, Nodes) of
    true -> ok;
    false -> 
      GlobalOpts = application:get_env(cqerl, default_options, []),
      new_pool(Node, Opts, GlobalOpts),
      {noreply, State#cqerl_state{nodes=[Node|Nodes]}}
  end;

handle_cast({client_busy, Pid}, State=#cqerl_state{clients=Clients}) ->
  case ets:lookup(Clients, Pid) of
    [Client] ->
      ets:insert(Clients, Client#cql_client{busy=true});
    _ -> ok
  end,
  {noreply, State};

handle_cast({client_asleep, Pid}, State=#cqerl_state{clients=Clients}) ->
  case ets:lookup(Clients, Pid) of
    [Client = #cql_client{node=Node}] ->
      Pool = pool_from_node(Node),
      pooler:return_member(Pool, Pid, ok),
      ets:delete(Clients, Pid);
    _ -> ok
  end,
  {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.




handle_info(timeout, State=#cqerl_state{checked_env=false}) ->
  GlobalOpts = application:get_env(cqerl, default_options, []),
  Nodes = application:get_env(cqerl, cassandra_nodes, []),
  lists:foreach(fun
    ({Inet, Opts}) -> new_pool(Inet, Opts, GlobalOpts);
    (Inet) ->         new_pool(Inet, [], GlobalOpts)
  end, Nodes),
  {noreply, State#cqerl_state{checked_env=true}};

handle_info({retry, Msg, From, Delay}, State=#cqerl_state{clients=Clients}) ->
  case handle_call(Msg, From, State#cqerl_state{retrying=true}) of
    retry -> 
      case erlang:round(math:pow(Delay, ?RETRY_EXP_FACT)) of
        NewDelay when NewDelay < ?RETRY_MAX_DELAY ->
          erlang:send_after(NewDelay, self(), {retry, Msg, From, NewDelay});
        _ ->
          gen_server:reply(From, {error, no_available_clients})
      end;
    {reply, Client, State} ->
      gen_server:reply(From, Client)
  end;

handle_info(_Msg, State) ->
  {noreply, State}.




terminate(Reason, State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.




%% =================
%% Private functions
%% =================

new_pool(Node, LocalOpts, GlobalOpts) ->
  OptGetter = option_getter(GlobalOpts, LocalOpts),
  pooler:new_pool([{name,           pool_from_node(Node)},
                   {group,          cqerl},
                   {init_count,     OptGetter(pool_min_size)},
                   {max_count,      OptGetter(pool_max_size)},
                   {cull_interval,  OptGetter(pool_cull_interval)},
                   {max_age,        OptGetter(client_max_age)},
                   {start_mfa,      {cqerl_client, start_link, [Node, 
                         [  {auth_handler, OptGetter(auth_handler)},
                            {ssl, OptGetter(ssl)} ]]} }]).

option_getter(Local, Global) ->
  fun (Key) ->
    case proplists:lookup(Key, Local) of
      none ->
        case proplists:lookup(Key, Global) of
          none ->
            case Key of
              pool_max_size -> 5;
              pool_min_size -> 2;
              pool_cull_interval -> {1, min};
              client_max_age -> {30, sec};
              auth_handler -> cqerl_auth_plain_handler;
              ssl -> false
            end;
          GlobalVal -> GlobalVal
        end;
      LocalVal -> LocalVal
    end
  end.


select_client(Clients, MatchClient = #cql_client{node=Node}, UserPid) ->
  case ets:match_object(Clients, MatchClient) of
    AvailableClients when length(AvailableClients) > 0 ->
      RandIdx = random:uniform(length(AvailableClients)),
      #cql_client{pid=Pid} = lists:nth(RandIdx, AvailableClients),
      add_client(UserPid, Pid);
    
    [] ->
      NewMember = case Node of
        '_' -> pooler:take_group_member(cqerl);
        _   -> pooler:take_member(pool_from_node(Node))
      end,
      case NewMember of
        error_no_members -> no_available_clients;
        {Pool, Pid} ->
          Node = node_from_pool(Pool),
          ets:insert(Clients, #cql_client{node=Node, busy=false, pid=Pid}),
          add_client(UserPid, Pid);
        Pid ->
          ets:insert(Clients, #cql_client{node=Node, busy=false, pid=Pid}),
          add_client(UserPid, Pid)
      end
  end.

-spec add_client(UserPid :: pid(), ClientPid :: pid()) -> {pid(), reference()}.
  
add_client(UserPid, ClientPid) ->
  ClientRef = make_ref(),
  cqerl_client:new_user(ClientPid, ClientRef, UserPid),
  {ClientPid, ClientRef}.

-spec prepare_node_info(NodeInfo :: any()) -> Node :: inet().

prepare_node_info({Localhost, Port}) when Localhost == localhost;
                                          Localhost == "localhost" ->
  {{127, 0, 0, 1}, Port};
prepare_node_info({Addr, Port}) when is_list(Addr) ->
  {ok, IpAddr} = inet:parse_address(Addr),
  {IpAddr, Port};
prepare_node_info(Inet) ->  
  Inet.

-spec pool_from_node(Node :: inet()) -> atom().

pool_from_node(Node = { _Addr , _Port }) when is_tuple(_Addr), is_integer(_Port) ->
  binary_to_atom(base64:encode(term_to_binary(Node)), latin1).

-spec node_from_pool(Pool :: atom()) -> inet().

node_from_pool(PoolName) when is_atom(PoolName) ->
  binary_to_term(base64:decode(atom_to_binary(PoolName, latin1))).

