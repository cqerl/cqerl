-module(cqerl_hash).

-behaviour(gen_server).

-export([
    start_link/0,
    client_started/1,
    get_client/1
]).

-export([
    init/1,
    terminate/2,
    code_change/3,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-type key() :: {Node :: term(), Opts :: list()}.

-record(pending, {
          key :: key(),
          reply_to :: [term()],
          remaining :: non_neg_integer(),
          sup_pid :: pid(),
          table :: ets:tid()
         }).

-record(client_table, {
          key :: key(),
          sup_pid :: pid(),
          table :: ets:tid()
         }).

-record(state, {
          pending = [] :: [#pending{}]
         }).

start_link() ->
    gen_server:start_link({local, cqerl}, ?MODULE, [], []).

init(_) ->
    ets:new(cqerl_client_tables, [named_table, {read_concurrency, true}, protected,
                                  {keypos, #client_table.key}]),
    {ok, #state{}}.

client_started(Key) ->
    gen_server:cast(cqerl, {add_client, Key, self()}).

handle_call({start_clients, Key}, From, State = #state{pending = Pending}) ->
    case ets:lookup(cqerl_client_tables, Key) of
        [#client_table{}] ->
            ct:log("BJD Found existing table ~p", [Key]),
            {reply, ok, State};
        [] ->
            case lists:keytake(Key, #pending.key, Pending) of
                false ->
                    ct:log("BJD No existing table - starting clients ~p", [Key]),
                    start_clients_impl(Key, From, State);
                {value, PendingItem, OtherPending} ->
                    ct:log("BJD No existing table but pending request - adding to queue ~p", [Key]),
                    NewPending = PendingItem#pending{reply_to = [From | PendingItem#pending.reply_to]},
                    {noreply, State#state{pending = [NewPending | OtherPending]}}
            end
    end;


handle_call(_, _, State) ->
    {noreply, State}.

handle_cast({add_client, Key, Pid}, State = #state{pending = Pending}) ->
    ct:log("BJD Got add_client for ~p / ~p", [Key, Pending]),
    NewState =
    case lists:keytake(Key, #pending.key, Pending) of
        {value, PendingItem, OtherPending} ->
            add_new_client(Pid, PendingItem, State#state{pending = OtherPending});
        false ->
            add_replacement_client(Key, Pid, State)
    end,
    {noreply, NewState};


handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, Pid, Reason}, State) ->
    NewState = clear_pending(Pid, Reason, State),
    clear_existing(Pid),
    {noreply, NewState};

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

get_client(Key) ->
    NKey = normlaise_keyspace(Key),
    case get_table(NKey) of
        {ok, T} ->
            N = erlang:phash2(self(), ets:info(T, size)),
            [{_, Pid}] = ets:lookup(T, N+1),
            {ok, {Pid, make_ref()}};
        _ ->
            start_clients(NKey)
    end.

start_clients(Key) ->
    case gen_server:call(cqerl, {start_clients, Key}, infinity) of
        ok -> get_client(Key);
        {error, E} -> {error, E}
    end.

get_table(Key) ->
    case ets:lookup(cqerl_client_tables, Key) of
        [#client_table{table = T}] -> {ok, T};
        [] -> {error, clients_not_started}
    end.

start_clients_impl({Node, Opts}, From, State) ->
    ClientTable = ets:new(cqerl_clients, [{read_concurrency, true}, protected]),
    case cqerl_client_sup:add_clients(Node, Opts) of
        {error, E} -> {reply, {error, E}, State};
        {ok, {Num, SupPid}} ->
            monitor(process, SupPid),
            NewPending = #pending{key = {Node, Opts}, reply_to = [From], remaining = Num, table = ClientTable, sup_pid = SupPid},
            {noreply, State#state{pending = [NewPending  | State#state.pending]}}
    end.

add_new_client(Pid, PendingItem = #pending{reply_to = ReplyTo, key = Key, table = Table, remaining = Remaining, sup_pid = SupPid}, State = #state{pending = OtherPending}) ->
    add_client(Pid, Table),
    NewPending = case Remaining of
        1 ->
                         ct:log("BJD final client started for ~p", [Key]),
            ets:insert(cqerl_client_tables,
                       #client_table{
                          key = Key,
                          table = Table,
                          sup_pid = SupPid}),
            lists:foreach(fun(R) -> gen_server:reply(R, ok) end, ReplyTo),
            OtherPending;
        N ->
                         ct:log("BJD client ~p started for ~p", [N, Key]),
            [PendingItem#pending{remaining = N-1} | OtherPending]
    end,
    State#state{pending = NewPending}.

add_replacement_client(Key, Pid, State) ->
    {ok, T} = get_table(Key),
    add_client(Pid, T),
    State.

add_client(Pid, Table) ->
    monitor(process, Pid),
    Index = find_empty_index(Table),
    ets:insert(Table, {Index, Pid}).

find_empty_index(Table) ->
    {UsedIndices, _} = lists:unzip(ets:tab2list(Table)),
    PossibleIndices = lists:seq(1, length(UsedIndices)+1),
    find_empty(lists:sort(UsedIndices), PossibleIndices).

find_empty([A|Tail], [A|Tail2]) -> find_empty(Tail, Tail2);
find_empty(_, [A|_]) -> A.

normlaise_keyspace({Node, Opts}) ->
    KS = proplists:get_value(keyspace, Opts),
    NewOpts = [{keyspace, normalise_to_atom(KS)} | proplists:delete(keyspace, Opts)],
    {Node, NewOpts}.

normalise_to_atom(KS) when is_list(KS) -> list_to_atom(KS);
normalise_to_atom(KS) when is_binary(KS) -> binary_to_atom(KS, latin1);
normalise_to_atom(KS) when is_atom(KS) -> KS.

clear_pending(Pid, Reason, State = #state{pending = Pending}) ->
    NewPending =
    case lists:keytake(Pid, #pending.sup_pid, Pending) of
        false -> Pending;
        {value, #pending{reply_to = ReplyTo}, OtherPending} ->
            lists:foreach(fun(R) -> gen_server:reply(R, {error, Reason}) end, ReplyTo),
            OtherPending
    end,
    State#state{pending = NewPending}.

clear_existing(Pid) ->
    Tables = ets:tab2list(cqerl_client_tables),
    lists:foreach(
      fun(#client_table{key = K, sup_pid = SupPid, table = T}) when SupPid =:= Pid ->
              ets:delete(cqerl_client_tables, K),
              ets:delete(T);
         (#client_table{table = T}) ->
              ets:match_delete(T, {'_', Pid})
      end,
      Tables).
