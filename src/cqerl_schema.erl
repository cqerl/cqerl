-module(cqerl_schema).

-include("cqerl.hrl").
-include("cqerl_protocol.hrl").

-compile(export_all). % TODO: remove

-behaviour(gen_server).

-compile({parse_transform, do}).

-export([start_link/0,
         add_node/2,
         select_client/1,
         force_refresh/0,
         handle_event/1
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

-record(state, {
          nodes = [] :: [cqerl_node()]
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
    gen_server:cast(?MODULE, {node_removed, Node});
% Ignore all others for now
handle_event(_) ->
    ok.

init(_) ->
    _ = ets:new(token_nodes, [protected, named_table, ordered_set,
                              {read_concurrency, true},
                              {keypos, #token_node.token}]),
    _ = ets:new(cqerl_schema, [protected, named_table, bag,
                               {read_concurrency, true}, {keypos, #column.key}]),
    {ok, #state{}}.

handle_cast({add_node, Node, Opts}, State = #state{nodes = Nodes}) ->
    NewNodes =
    case lists:member(Node, Nodes) of
        true ->
            Nodes;
        false ->
            do_add_node(Node, Opts),
            [Node | Nodes]
    end,
    {noreply, State#state{nodes = NewNodes}};

handle_cast(refresh, State = #state{nodes = Nodes}) ->
    lists:foreach(fun read_metadata/1, Nodes),
    {noreply, State};

handle_cast({refresh_keyspace, Keyspace}, State) ->
    ets:match_delete(cqerl_schema, #column{key = {Keyspace, '_'}, _='_'}),
    handle_cast(refresh, State);

handle_cast({refresh_table, Keyspace, Table}, State) ->
    ets:match_delete(cqerl_schema, #column{key = {Keyspace, Table}, _='_'}),
    handle_cast(refresh, State);

handle_cast({node_removed, Node}, State) ->
    ets:match_delete(token_nodes, #token_node{node = Node, _='_'}),
    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.


handle_call(_, _, State) ->
    {reply, {error, bad_call}, State}.


handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

do_add_node(Node, Opts) ->
    GroupName = schema_group_name(Node),
    NoKeyspace = proplists:delete(keyspace, Opts),
    FullOpts = [register_for_events | NoKeyspace],
    cqerl:add_group(GroupName, [Node], FullOpts, 1),
    cqerl:wait_for_group(GroupName),
    read_metadata(Node).

schema_group_name(Node) ->
    {schema_reader, Node}.

read_metadata(Node) ->
    {ok, Client} = cqerl_client_pool:get_client(schema_group_name(Node)),
    read_tokens(Node, Client),
    read_schema(Client).

read_tokens(Node, Client) ->
    Q = "SELECT partitioner, tokens FROM system.local",
    {ok, Result} = cqerl_client:run_query(
                     Client, #cql_query{statement = Q, keyspace = system}),
    Row = cqerl:head(Result),
    process_tokens(Node, Row).

process_tokens(Node, #{tokens := Tokens}) ->
    lists:foreach(
      fun(T) ->
              TokenInt = binary_to_integer(T),
              ets:insert(token_nodes,
                         #token_node{token = TokenInt, node = Node})
      end, Tokens).

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

get_partition_key_value(Keys, Values, Query) ->
    %TODO: Compound keys
    #column{col = C, type = Type} = hd(Keys),
    case maps:get(C, Values, undefined) of
        undefined -> {error, no_key_value};
        V -> {ok, cqerl_datatypes:encode_data({Type, V}, Query)}
    end.

get_token(Value) ->
    Hash = erlang_murmurhash:murmurhash3_x64_128(Value),
    % Zero pad:
    Encoded = binary:encode_unsigned(Hash, little),
    Padding = 16 - size(Encoded),
    PaddedHash = <<0:(Padding*8), Encoded/binary>>,
    <<_:8/binary, LowerHalf:8/binary>> = PaddedHash,
    <<Token:64/signed-little>> = LowerHalf,
    {ok, Token}.

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

