-module(cqerl_hash).

-include("cqerl.hrl").

-behaviour(gen_server).

-export([
    start_link/0,
    client_started/3,
    remove_client/2,
    get_client/2,
    get_random_client/1,
    wait_for_client/1
]).

-export([
    init/1,
    terminate/2,
    code_change/3,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(client_key, {
          node     :: cqerl_node(),
          keyspace :: keyspace()
         }).

-record(client_table, {
          key :: #client_key{},
          table :: ets:tid()
         }).

-record(client, {
          index :: non_neg_integer() | '_',
          pid :: pid(),
          mon_ref :: reference() | '_'
         }).

-record(state, {
          active_groups = sets:new() :: sets:set(),
          waiters = [] :: [{term(), term()}]
         }).


%% =================
%% Public API
%% =================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec client_started(term(), cqerl_node(), keyspace()) -> ok.
client_started(Name, Node, Keyspace) ->
    Key = make_client_key(Node, Keyspace),
    gen_server:cast(?MODULE, {add_client, Name, Key, self()}).

remove_client(Node, Keyspace) ->
    Key = make_client_key(Node, Keyspace),
    gen_server:cast(?MODULE, {remove_client, Key, self()}).

get_client(Node, Keyspace) ->
    Key = make_client_key(Node, Keyspace),
    case get_existing_table(Key) of
        {ok, T} ->
            get_client_from_table(T);
        _ ->
            {ok, client_not_configured}
    end.

get_random_client(Keyspace) ->
    Tables = ets:tab2list(cqerl_client_tables),
    ValidTables =
    lists:filter(fun(#client_table{key = Key}) ->
                         Key#client_key.keyspace =:= Keyspace
                 end, Tables),
    select_random_from_valid(ValidTables).

wait_for_client(GroupName) ->
    gen_server:call(?MODULE, {wait_for_client, GroupName}, infinity).

%% =================
%% gen_server functions
%% =================

init(_) ->
    ets:new(cqerl_client_tables, [named_table, {read_concurrency, true}, protected,
                                  {keypos, #client_table.key}]),
    {ok, #state{}}.

handle_call({wait_for_client, GroupName}, From,
            State = #state{active_groups = Groups}) ->
    case sets:is_element(GroupName, Groups) of
        true -> {reply, ok, State};
        false -> {noreply, add_waiter(GroupName, From, State)}
    end;

handle_call(_, _, State) ->
    {reply, {error, bad_call}, State}.

handle_cast({add_client, Name, Key, Pid}, State) ->
    NewState = add_client(Name, Key, Pid, State),
    {noreply, NewState};

handle_cast({remove_client, Key, Pid}, State) ->
    {ok, T} = get_existing_table(Key),
    remove_client_from_table(Pid, T),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    clear_existing(Pid),
    {noreply, State};

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% =================
%% Private functions
%% =================

make_client_key(Node, Keyspace) ->
    #client_key{node = cqerl:normalise_node(Node),
                keyspace = cqerl:normalise_keyspace(Keyspace)}.

get_existing_table(Key) ->
    case ets:lookup(cqerl_client_tables, Key) of
        [#client_table{table = T}] -> {ok, T};
        [] -> {error, clients_not_started}
    end.

add_client(Name, Key, Pid, State = #state{active_groups = Groups}) ->
    T = get_create_table(Key),
    add_client_to_table(Pid, T),
    NewState = notify_waiters(Name, State),
    NewState#state{active_groups = sets:add_element(Name, Groups)}.

add_client_to_table(Pid, Table) ->
    MonRef = monitor(process, Pid),
    Index = find_empty_index(Table),
    Record = #client{index = Index,
                     pid = Pid,
                     mon_ref = MonRef},
    ets:insert(Table, Record).

find_empty_index(Table) ->
    UsedIndices = [I || #client{index = I} <- ets:tab2list(Table)],
    PossibleIndices = lists:seq(1, length(UsedIndices)+1),
    find_empty(lists:sort(UsedIndices), PossibleIndices).

find_empty([A|Tail], [A|Tail2]) -> find_empty(Tail, Tail2);
find_empty(_, [A|_]) -> A.

clear_existing(Pid) ->
    Tables = ets:tab2list(cqerl_client_tables),
    lists:foreach(
      fun(#client_table{table = T}) ->
              remove_client_from_table(Pid, T)
      end,
      Tables).

remove_client_from_table(Pid, Table) when is_pid(Pid) ->
    case ets:match_object(Table, #client{pid = Pid, _ = '_'}) of
        [] -> ok;
        [Client] -> remove_client_from_table(Client, Table)
    end;

remove_client_from_table(Client = #client{mon_ref = MonRef}, Table) ->
    demonitor(MonRef),
    ets:match_delete(Table, Client).

get_create_table(Key) ->
    case ets:lookup(cqerl_client_tables, Key) of
        [] -> new_client_table(Key);
        [#client_table{table = T}] -> T
    end.

new_client_table(Key) ->
    ClientTable = ets:new(cqerl_clients, [{read_concurrency, true},
                                          protected,
                                          {keypos, #client.index}
                                         ]),
    ClientTableEntry = #client_table{
                          key = Key,
                          table = ClientTable},
    ets:insert(cqerl_client_tables, ClientTableEntry),
    ClientTable.

get_client_from_table(Table) ->
    N = erlang:phash2(self(), ets:info(Table, size)) + 1,
    case ets:lookup(Table, N) of
        [#client{pid = Pid}] -> {ok, {Pid, make_ref()}};
        [] -> {error, no_clients}
    end.

select_random_from_valid([]) ->
    {error, no_clients};

select_random_from_valid(Tables) ->
    #client_table{table = T} = lists:nth(rand:uniform(length(Tables)), Tables),
    case get_client_from_table(T) of
        {ok, Client} -> {ok, Client};
        {error, _} -> select_random_from_valid(Tables -- [T])
    end.

add_waiter(Name, Waiter, State = #state{waiters = Waiters}) ->
    State#state{waiters = [{Name, Waiter} | Waiters]}.

notify_waiters(Name, State = #state{waiters = Waiters}) ->
    ToNotify = lists:filter(fun({N, _}) -> N =:= Name end, Waiters),
    lists:foreach(fun({_, Waiter}) ->
                          gen_server:reply(Waiter, ok)
                  end, ToNotify),
    State#state{waiters = Waiters -- ToNotify}.


