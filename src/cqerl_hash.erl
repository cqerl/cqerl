-module(cqerl_hash).

-behaviour(gen_server).

%% This module performs the heavy lifting for the 'hash' application mode.
%% Below is some background behind the creation of this mode as well as an
%% overview of its operation.
%%
%% A note on terminology: When I say "client" I mean a cqerl_client process.
%% When I say "user" I mean any process in an application that calls
%% cqerl:get_client or cqerl:new_client to get a client to use to make a query.
%%
%% So - we've been having a few intermittent problems with the existing
%% pooler-based implementation. Fundamentally, it doesn't seem to be an ideal
%% choice for managing a pool of clients where each client can have multiple
%% users, as is the case with cqerl clients. Additionally, as has been noted in
%% #49, it has scalability issues with bottlenecks (that may not be endemic to
%% pooler per se, but are certainly present in our use of it). I looked over
%% dispcount as an alternative, but as I explained in my comment on #49, that
%% doesn't seem ideal either.
%%
%% What I've ended up doing, then, is to invent my own solution (which steals a
%% couple of dispcount's neat ideas that fit our problem). Basically, for each
%% "key" (see below), we have a set of client processes started on the first
%% attempt to get a client using that key. Those clients, once started, are
%% available to all users, and are allocated from a pool using a
%% dispcount-style hash of the user's PID. Because they're multi-user clients,
%% however, we don't need dispcount's exclusivity guarantees and so can
%% allocate multiple users to each client.
%%
%% The "key" is essentially the NodeKey used by the pooler implementation - a
%% tuple of the Node and Opts used to originally set up the clients. I think
%% this could probably stand to be simplified a bit, but for now it's what I've
%% got. Every unique key currently starts num_clients (default 20) client
%% processes - I'd also like to make that configurable on a per-keyspace basis.
%%
%% Each set of clients is monitored by a one_for_one supervisor (call it the
%% "set supervisor"). And these set supervisors are, in turn, monitored under a
%% simple_one_for_one supervisor called cqerl_client_sup. What this means is
%% that individual failures/crashes amongst a set of clients will be restarted
%% by their set supervisor and only cause transient error conditions for any
%% user trying to access them. If, on the other hand, the whole Cassandra
%% server goes down (or something else equally catastrophic happens) then the
%% clients retrying will quickly hit the set supervisor's restart intensity and
%% cause it to shutdown too. Since cqerl_client_sup is a simple_one_for_one,
%% however, the failure won't cascade any further upwards. Further, the next
%% attempt by a user to access a client for that key will create a new set of
%% clients. If they connect successfully, everything is good and we keep going.
%% If they don't, the user (and any other uses waiting on their creation) will
%% again receive error responses.
%%
%% In addition to the set supervisor, each key has its own ets table which is
%% used to map the hash values to a client PID. These tables are maintained by
%% the cqerl_hash module/process (registered as cqerl) which monitors all the
%% client processes and their immediate supervisors. They are further indexed
%% by a "root" ets table named cqerl_client_tables which maps keys to their
%% respective tables. Thus in the normal course of events, any request for a
%% client requires two ets lookups to read-optimised tables (in much the same
%% manner as dispcount): First to get the client table for a key, then to get
%% the client allocated to the hash of the user process pid.
%%
%% I've tried, as far as possible, to make this change backwards compatible. If
%% you change nothing, the behaviour still uses the existing pooler system.
%% Adding {mode, hash} to the cqerl app config enables the new system.
%%
%% The other change required is to call cqerl:get_client/2 rather than
%% cqerl:new_client. I made that distinction mostly so that users would not
%% accidentally end up using the wrong mode, but also because it would have
%% required fiddly and otherwise unnecessary checks in new_client to have it
%% work for both.
%%
%% Calling cqerl:close_client/1 in hash mode is not necessary, but nor is it
%% harmful.
%%
%% I've changed the integration and load tests to run in both modes, and
%% everything seems to work okay. Load tests for hash mode are marginally
%% (maybe ~20%) faster on my system, but it's a VM with only 2 cores - I hope
%% on beefier machines that the improvement will be more pronounced, but I've
%% yet to have a chance to test that.



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
            {reply, ok, State};
        [] ->
            case lists:keytake(Key, #pending.key, Pending) of
                false ->
                    start_clients_impl(Key, From, State);
                {value, PendingItem, OtherPending} ->
                    NewPending = PendingItem#pending{reply_to = [From | PendingItem#pending.reply_to]},
                    {noreply, State#state{pending = [NewPending | OtherPending]}}
            end
    end;


handle_call(_, _, State) ->
    {noreply, State}.

handle_cast({add_client, Key, Pid}, State = #state{pending = Pending}) ->
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
            ets:insert(cqerl_client_tables,
                       #client_table{
                          key = Key,
                          table = Table,
                          sup_pid = SupPid}),
            lists:foreach(fun(R) -> gen_server:reply(R, ok) end, ReplyTo),
            OtherPending;
        N ->
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
