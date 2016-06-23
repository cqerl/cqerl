-module(cqerl_schema).

-include("cqerl.hrl").
-include("cqerl_protocol.hrl").

-behaviour(gen_server).

-compile({parse_transform, do}).

-define(SCHEMA_POLL_INTERVAL, 500).

-export([start_link/0,
         add_node/2,
         remove_node/1,
         select_client/1,
         force_refresh/0,
         handle_event/1,
         wait_for_schema_agreement/0
        ]).

-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(token_node, {
          token :: integer() | '_',
          node :: cqerl_node()
         }).

-record(column, {
          key :: {atom(), atom()}, % {keyspace, table}
          col :: atom() | '_',
          pos :: non_neg_integer() | '_',
          type :: atom() | '_'
         }).

-record(node, {
          node :: cqerl_node(),
          schema_version :: binary(),
          peers = [] :: [{cqerl_node(), binary()}]
         }).

-record(state, {
          schema_waiters = [] :: [term()]
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

add_node(Node, Opts) ->
    gen_server:cast(?MODULE, {add_node, Node, Opts}).

remove_node(Node) ->
    gen_server:cast(?MODULE, {remove_node, Node}).

force_refresh() ->
    gen_server:cast(?MODULE, refresh).

select_client(Query = #cql_query{statement = Statement, keyspace = Keyspace,
                                 values = Values}) ->
    do([cqerl_error_m ||
        Table <- cql_parser:get_table(Statement),
        Keys  <- get_partition_keys(Keyspace, Table),
        Value <- get_partition_key_value(Keys, Values, Query),
        Token <- get_token(Value),
        NodeKey <- get_node_key(Token),
        Node  <- get_node(NodeKey),
        cqerl_client_pool:get_client(Node, Keyspace)
       ]).

handle_event(#table_change{keyspace = Keyspace, table = Table}) ->
    gen_server:cast(?MODULE, {refresh_table, Keyspace, Table});
handle_event(#keyspace_change{keyspace = Keyspace}) ->
    gen_server:cast(?MODULE, {refresh_keyspace, Keyspace});
handle_event(#topology_change{type = ?CQERL_TOPOLOGY_CHANGE_TYPE_REMOVED_NODE,
                              node = Node}) ->
    gen_server:cast(?MODULE, {remove_node, Node});
% Ignore all others for now
handle_event(_) ->
    ok.

wait_for_schema_agreement() ->
    gen_server:call(?MODULE, wait_for_schema_agreement, infinity).

init(_) ->
    _ = ets:new(token_nodes, [protected, named_table, ordered_set,
                              {read_concurrency, true},
                              {keypos, #token_node.token}]),
    _ = ets:new(cqerl_schema, [protected, named_table, bag,
                               {read_concurrency, true},
                               {keypos, #column.key}]),
    _ = ets:new(cqerl_nodes, [protected, named_table, set,
                              {keypos, #node.node}]),
    {ok, #state{}}.

handle_cast({add_node, Node, Opts}, State = #state{}) ->
    NewState = case ets:lookup(cqerl_nodes, Node) of
        [] ->
            do_add_node(Node, Opts, State);
        _ ->
            State
    end,
    {noreply, NewState};

handle_cast(refresh, State) ->
    NewState = refresh_metadata(State),
    {noreply, NewState};

handle_cast({refresh_keyspace, Keyspace}, State) ->
    ets:match_delete(cqerl_schema, #column{key = {Keyspace, '_'}, _='_'}),
    handle_cast(refresh, State);

handle_cast({refresh_table, Keyspace, Table}, State) ->
    ets:match_delete(cqerl_schema, #column{key = {Keyspace, Table}, _='_'}),
    handle_cast(refresh, State);

handle_cast({remove_node, Node}, State) ->
    ets:match_delete(token_nodes, #token_node{node = Node, _='_'}),
    ets:delete(cqerl_nodes, Node),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_call(wait_for_schema_agreement, From,
            State = #state{schema_waiters = Waiters}) ->
    Waiters =:= [] andalso self() ! poll_schema,
    {noreply, State#state{schema_waiters = [From | Waiters]}};

handle_call(_, _, State) ->
    {reply, {error, bad_call}, State}.

handle_info(poll_schema, State = #state{schema_waiters = []}) ->
    {noreply, State};

handle_info(poll_schema, State) ->
    NewState = refresh_metadata(State),
    {noreply, NewState};

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_add_node(Node, Opts, State) ->
    GroupName = schema_group_name(Node),
    NoKeyspace = proplists:delete(keyspace, Opts),
    FullOpts = [register_for_events | NoKeyspace],
    cqerl:add_group(GroupName, [Node], FullOpts, 1),
    read_metadata(Node, State).

refresh_metadata(State) ->
    lists:foldl(fun read_metadata/2, State, known_nodes()).

schema_group_name(Node) ->
    {schema_reader, Node}.

read_metadata(Node, State) ->
    {ok, Client} = cqerl_client_pool:get_client(schema_group_name(Node)),
    read_schema(Client),
    read_node_data(Node, Client, State).

read_node_data(Node, Client, State) ->
    Q = "SELECT partitioner, tokens, schema_version FROM system.local",
    {ok, Result} = cqerl_client:run_query(
                     Client, #cql_query{statement = Q}),
    Row = cqerl:head(Result),
    Q2 = "SELECT peer, schema_version FROM system.peers",
    {ok, Result2} = cqerl_client:run_query(
                     Client, #cql_query{statement = Q2}),
    Peers = cqerl:all_rows(Result2),
    process_tokens(Node, Row),
    update_node_data(Node, Row, Peers, State).

process_tokens(Node, #{tokens := Tokens}) ->
    lists:foreach(
      fun(T) ->
              TokenInt = binary_to_integer(T),
              ets:insert(token_nodes,
                         #token_node{token = TokenInt, node = Node})
      end, Tokens).

update_node_data(Node, #{schema_version := SchemaVersion}, Peers, State) ->
    PeerList = [{Peer, PeerSchemaVersion} ||
                #{peer := Peer, schema_version := PeerSchemaVersion} <- Peers],
    ets:insert(cqerl_nodes, #node{node = Node,
                                  schema_version = SchemaVersion,
                                  peers = PeerList}),
    process_schema_waiters(State).

process_schema_waiters(State = #state{schema_waiters = Waiters}) ->
    case schemas_agree() of
        true ->
            notify_waiters(Waiters),
            State#state{schema_waiters = []};
        false ->
            erlang:send_after(?SCHEMA_POLL_INTERVAL, self(), poll_schema),
            State
    end.

notify_waiters(Waiters) ->
    lists:foreach(fun(W) -> gen_server:reply(W, ok) end,
                  Waiters).

read_schema(Client) ->
    Q = "SELECT keyspace_name, table_name, column_name, position, type FROM "
        "system_schema.columns WHERE kind = 'partition_key' ALLOW FILTERING",
    {ok, Result} = cqerl_client:run_query(
                     Client, #cql_query{statement = Q, keyspace = system}),
    Rows = cqerl:all_rows(Result),
    process_columns(Rows).

process_columns(Columns) ->
    lists:foreach(fun process_column/1, Columns).

process_column(#{keyspace_name := KS,
                 table_name := Table,
                 column_name := Col,
                 position := Pos,
                 type := Type}) ->
    R = #column{key = {cqerl:normalise_keyspace(KS), Table},
                col = binary_to_atom(Col, utf8), pos = Pos,
                type = binary_to_atom(Type, utf8)},
    ets:insert(cqerl_schema, R).

get_partition_keys(Keyspace, Table) ->
    case ets:lookup(cqerl_schema, {Keyspace, Table}) of
        [] -> {error, no_key};
        Keys -> {ok, Keys}
    end.

get_partition_key_value([#column{col = Col, type = Type}], Values, Query) ->
    case maps:get(Col, Values, undefined) of
        undefined -> {error, no_key_value};
        V -> {ok, cqerl_datatypes:encode_data({Type, V}, Query)}
    end;

get_partition_key_value(Keys, Values, Query) ->
    OrderedKeys = lists:keysort(#column.pos, Keys),
    build_array_bin(OrderedKeys, Values, Query, <<>>).

build_array_bin([], _Values, _Query, BinAcc) ->
    {ok, BinAcc};
build_array_bin([#column{col = Col, type = Type} | RestCols],
                     Values, Query, BinAcc) ->
    case maps:get(Col, Values, undefined) of
        undefined ->
            {error, no_key_value};
        Val ->
            Bin = cqerl_datatypes:encode_data({Type, Val}, Query),
            NewAcc = <<BinAcc/binary, (size(Bin)):16/big-signed, Bin/binary, 0>>,
            build_array_bin(RestCols, Values, Query, NewAcc)
    end.

get_token(Value) ->
    {ok, erlang_murmurhash:murmurhash3_x64_128_cass(Value)}.

get_node_key(Token) ->
    case ets:next(token_nodes, Token-1) of
        '$end_of_table' ->
            case ets:first(token_nodes) of
                '$end_of_table' -> {error, no_nodes};
                K -> {ok, K}
            end;
        K -> {ok, K}
    end.

get_node(Key) ->
    case ets:lookup(token_nodes, Key) of
        [] -> {error, no_node};
        [#token_node{node = Node} | _] -> {ok, Node}
    end.

known_nodes() ->
    [Node#node.node || Node <- ets:tab2list(cqerl_nodes)].

schemas_agree() ->
    Nodes = ets:tab2list(cqerl_nodes),
    LocalSchemas = [Node#node.schema_version || Node <- Nodes],
    Peers = lists:append([Node#node.peers || Node <- Nodes]),
    RemoteSchemas = [Schema || {_Node, Schema} <- Peers],
    length(lists:usort(LocalSchemas ++ RemoteSchemas)) =:= 1.
