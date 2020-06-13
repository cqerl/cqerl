%% @author Mathieu D'Amours <matt@forest.io>
%% @doc Main interface to CQErl cassandra client.

-module(cqerl).
-behaviour(gen_server).

-export([
    run_query/2,
    send_query/2,

    has_more_pages/1,
    fetch_more/1,
    fetch_more_async/1,

    size/1,

    head/1,
    head/2,

    tail/1,
    next/1,

    all_rows/1,
    all_rows/2,

    start_link/0,

    get_client/0,
    get_client/1,
    get_client/2,

    get_global_opts/0,
    make_option_getter/2,

    prepare_node_info/1
]).

-export([
    init/1,
    terminate/2,
    code_change/3,

    handle_call/3,
    handle_cast/2,
    handle_info/2,

    get_protocol_version/0,
    put_protocol_version/1
]).


-include("cqerl.hrl").

-opaque client() :: {pid(), reference()}.

-type inet() :: { inet:ip_address() | string(), Port :: integer() } | inet:ip_address() | string() | binary() | {}.

-export_type([client/0, inet/0]).

-define(IPV4_RE, <<"^([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})(?::([0-9]{1,5}))?$">>).

-record(cql_client_stats, {
    min_count :: integer(),
    max_count :: integer(),
    count = 0 :: integer()
}).

-record(cqerl_state, {
    checked_env = false :: boolean(),
    clients             :: ets:tab(),
    client_stats        :: list({atom(), #cql_client_stats{}}),
    globalopts          :: list(any()),
    retrying = false    :: boolean(),
    named_nodes         :: list()
}).

-record(cql_client, {
    node  :: tuple() | '_',
    pid   :: pid() | '_',
    busy  :: boolean()
}).

-define(RETRY_INITIAL_DELAY, 10).
-define(RETRY_MAX_DELAY, 1000).
-define(RETRY_EXP_FACT, 1.15).

-define(DEFAULT_QUERY_RETRY, 2).
-define(DEFAULT_PORT, 9042).
-define(LOCALHOST, "127.0.0.1").


-spec get_client() -> {ok, client()} | {error, term()}.
get_client() ->
    cqerl_cluster:get_any_client().

-spec get_client(ClusterKeyOrInet :: atom() | inet()) -> {ok, client()} | {error, term()}.

get_client(ClusterKey) when is_atom(ClusterKey) ->
    cqerl_cluster:get_any_client(ClusterKey);

get_client({}) ->
    cqerl_hash:get_client(prepare_node_info({?LOCALHOST, ?DEFAULT_PORT}), []);

get_client(Spec) ->
    cqerl_hash:get_client(prepare_node_info(Spec), []).

-spec get_client(Inet :: inet(), Opts :: list(tuple() | atom())) -> {ok, client()} | {error, term()}.
get_client(Spec, Opts) ->
    cqerl_hash:get_client(prepare_node_info(Spec), Opts).



%% @doc Send a query to cassandra for execution. The function will return with the result from Cassandra (synchronously).
%%
%% The <code>Query</code> parameter can be a string, a binary UTF8 string or a <code>#cql_query{}</code> record
%%
%% <pre>#cql_query{
%%     statement :: binary(),
%%     reusable :: boolean(),
%%     consistency :: consistency_level(),
%%     named :: boolean(),
%%     bindings :: list(number() | boolean() | binary() | list() | inet() | ) | list(tuple())
%% }</pre>
%%
%% <em>Reusable</em> is a boolean indicating whether the query should be reused. <em>Reusing a query</em> means sending it to Cassandra to be prepared, which allows
%% later executions of the <strong>same query</strong> to be performed faster. This parameter is <code>true</code> by default when you provide bindings in the query (positional <code>?</code>
%% parameters or named <code>:var</code> parameters), and <code>false</code> by default when you don't. You can override the defaults.
%%
%% <em>Consistency</em> is represented as an atom and can be any of <code>any</code>, <code>one</code>,
%% <code>two</code>, <code>three</code>, <code>quorum</code>, <code>all</code>, <code>local_quorum</code>, <code>each_quorum</code>, <code>serial</code>,
%% <code>local_serial</code> or <code>local_one</code>.
%%
%% How <em>values</em> is used depends on the <em>named</em> value. <em>Named</em> is a boolean value indicating whether the parameters in the query are named parameters (<code>:var1</code>). Otherwise,
%% they are assumed to be positional (<code>?</code>). In both cases, <em>values</em> is a property list (see <a href="http://www.erlang.org/doc/man/proplists.html">proplists</a>) or map, where keys match the
%% parameter names.

-spec run_query(ClientRef :: client(), Query :: binary() | string() | #cql_query{} | #cql_query_batch{}) -> {ok, void} | {ok, #cql_result{}} | {error, term()}.
run_query(ClientRef, Query) ->
    run_query(ClientRef, Query, ?DEFAULT_QUERY_RETRY).

-spec run_query(ClientRef :: client(), Query :: binary() | string() | #cql_query{} | #cql_query_batch{}, non_neg_integer()) -> {ok, void} | {ok, #cql_result{}} | {error, term()}.
run_query(ClientRef, Query, Retry) when Retry =< 0->
    cqerl_client:run_query(ClientRef, Query);

run_query(ClientRef, Query, Retry) ->
    case cqerl_client:run_query(ClientRef, Query) of
        {error, {16#1100, _, _}} -> %% Write timeout
            error_logger:info_msg("Cassandra write timeout, retrying retry=~p", [Retry]),
            run_query(ClientRef, Query, Retry - 1);
        {error, {16#1200, _, _}} -> %% Read timeout
            error_logger:info_msg("Cassandra read timeout, retrying retry=~p", [Retry]),
            run_query(ClientRef, Query, Retry - 1);
        Response ->
            Response
    end.



%% @doc Check to see if there are more result available

-spec has_more_pages(Continuation :: #cql_result{}) -> true | false.
has_more_pages(#cql_result{cql_query=#cql_query{page_state=undefined}}) -> false;
has_more_pages(#cql_result{}) -> true.



%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%            return with the result from Cassandra (synchronously).

-spec fetch_more(Continuation :: #cql_result{}) -> no_more_result | {ok, #cql_result{}}.
fetch_more(#cql_result{cql_query=#cql_query{page_state=undefined}}) ->
    no_more_result;
fetch_more(Continuation) ->
    cqerl_client:fetch_more(Continuation).




%% @doc Send a query to be executed asynchronously. This method returns immediately with a unique tag.
%%
%% When a successful response is received from cassandra, a <code>{result, Tag, Result :: #cql_result{}}</code>
%% message is sent to the calling process.
%%
%% If there is an error with the query, a <code>{error, Tag, Error :: #cql_error{}}</code> will be sent to the calling process.
%%
%% Neither of these messages will be sent if the connection is dropped before receiving a response (see {@link new_client/0} for
%% how to handle this case).

-spec send_query(ClientRef :: client(), Query :: binary() | string() | #cql_query{} | #cql_query_batch{}) -> reference().
send_query(ClientRef, Query) ->
    cqerl_client:query_async(ClientRef, Query).




%% @doc Asynchronously fetch the next page of result from cassandra for a given continuation.
%%
%% A success or error message will be sent in response some time later (see {@link send_query/2} for details) unless the
%% connection is dropped.

-spec fetch_more_async(Continuation :: #cql_result{}) -> reference() | no_more_result.
fetch_more_async(#cql_result{cql_query=#cql_query{page_state=undefined}}) ->
    no_more_result;
fetch_more_async(Continuation) ->
    cqerl_client:fetch_more_async(Continuation).



%% @doc The number of rows in a result set

size(#cql_result{dataset=[]}) -> 0;
size(#cql_result{dataset=Dataset}) -> length(Dataset).

%% @doc Returns the first row of result, as a property list

head(#cql_result{dataset=[]}) -> empty_dataset;
head(Result) ->
    head(Result, get_options_list()).

head(#cql_result{dataset=[]}, _Opts) -> empty_dataset;
head(#cql_result{dataset=[Row|_Rest], columns=ColumnSpecs}, Opts) ->
    cqerl_protocol:decode_row(Row, ColumnSpecs, Opts).

%% @doc Returns all rows of result, except the first one

tail(#cql_result{dataset=[]}) -> empty_dataset;
tail(Result=#cql_result{dataset=[_Row|Rest]}) ->
    Result#cql_result{dataset=Rest}.

%% @doc Returns a tuple of <code>{HeadRow, ResultTail}</code>.
%%
%% This can be used to iterate over a result set efficiently. Successively
%% call this function over the result set to go through all rows, until it
%% returns the <code>empty_dataset</code> atom.

next(#cql_result{dataset=[]}) -> empty_dataset;
next(Result) -> {head(Result), tail(Result)}.

%% @doc Returns a list of rows as property lists

all_rows(#cql_result{dataset=[]}) -> [];
all_rows(Result) ->
    all_rows(Result, get_options_list()).

all_rows(#cql_result{dataset=Rows, columns=ColumnSpecs}, Opts) when is_list(Opts) ->
    [ cqerl_protocol:decode_row(Row, ColumnSpecs, Opts) || Row <- Rows ].

get_options_list() ->
    lists:foldl(fun(Option, Acc) ->
                        case application:get_env(cqerl, Option) of
                            {ok, true} -> [Option | Acc];
                            _ -> Acc
                        end
                end,
                [],
                [maps, text_uuids]).

%% ====================
%% Gen_Server Callbacks
%% ====================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init([]) ->
    rand:seed(exrop),
    process_flag(trap_exit, true),
    BaseState = #cqerl_state{clients = ets:new(clients, [set, private, {keypos, #cql_client.pid}]),
        client_stats = [],
        named_nodes = []
    },
    {ok, BaseState}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', From, Reason}, State=#cqerl_state{clients=Clients, client_stats=Stats}) ->
    case ets:lookup(Clients, From) of
        [#cql_client{node=NodeKey}] ->
            ets:delete(Clients, From),
            {noreply, State#cqerl_state{client_stats = dec_stats_count(NodeKey, Stats)}};
        [] ->
            {stop, Reason, State}
    end;

handle_info({retry, Msg, From, Delay}, State) ->
    case handle_call(Msg, From, State#cqerl_state{retrying=true}) of
        retry ->
            case erlang:round(math:pow(Delay, ?RETRY_EXP_FACT)) of
                NewDelay when NewDelay < ?RETRY_MAX_DELAY ->
                    erlang:send_after(NewDelay, self(), {retry, Msg, From, NewDelay});
                _ ->
                    gen_server:reply(From, {error, no_available_clients})
            end,
            {noreply, State};

        {noreply, State1} ->
            {noreply, State1};

        {reply, Reply, State1} ->
            gen_server:reply(From, Reply),
            {noreply, State1}
    end;

handle_info(_Msg, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




%% =================
%% Private functions
%% =================

get_global_opts() ->
    lists:map(
        fun (Key) ->
            case application:get_env(cqerl, Key) of
                undefined -> {Key, undefined};
                {ok, V} -> {Key, V}
            end
        end,
        [ssl, auth, protocol_version, pool_min_size, pool_max_size, pool_cull_interval, client_max_age, keyspace, name]
    ).

make_option_getter(Local, Global) ->
    fun (Key) ->
        case proplists:get_value(Key, Local) of
            undefined ->
                case proplists:get_value(Key, Global) of
                    undefined ->
                        case Key of
                            pool_max_size -> 5;
                            pool_min_size -> 2;
                            pool_cull_interval -> {1, min};
                            client_max_age -> {30, sec};
                            auth -> {cqerl_auth_plain_handler, []};
                            ssl -> false;
                            keyspace -> undefined;
                            name -> undefined;
                            protocol_version -> ?DEFAULT_PROTOCOL_VERSION
                        end;
                    GlobalVal -> GlobalVal
                end;
            LocalVal -> LocalVal
        end
    end.


-spec prepare_node_info(NodeInfo :: any()) -> Node :: inet().

prepare_node_info({AtomAddr, Port}) when is_list(Port) ->
    {PortInt, []} = string:to_integer(Port),
    prepare_node_info({ AtomAddr, PortInt });
prepare_node_info({AtomAddr, Port}) when is_binary(Port) ->
    prepare_node_info({ AtomAddr, binary_to_list(Port) });
prepare_node_info({AtomAddr, Port}) when is_atom(AtomAddr) ->
  prepare_node_info({atom_to_list(AtomAddr), Port});

prepare_node_info({BinaryArr, Port}) when is_binary(BinaryArr) ->
    prepare_node_info({ binary_to_list(BinaryArr), Port });

prepare_node_info({StringAddr, Port}) when is_list(StringAddr) ->
    case ?CQERL_PARSE_ADDR(StringAddr) of
      {ok, IpAddr} -> {IpAddr, Port};
      {error, _Reason} -> {StringAddr, Port}
    end;

prepare_node_info({TupleAddr, Port}) when is_tuple(TupleAddr) andalso erlang:size(TupleAddr) == 4;    % v4
                                          is_tuple(TupleAddr) andalso erlang:size(TupleAddr) == 8 ->  % v6
    {TupleAddr, Port};

prepare_node_info(Addr) when is_atom(Addr);
                             is_tuple(Addr) andalso erlang:size(Addr) == 4;    % v4
                             is_tuple(Addr) andalso erlang:size(Addr) == 8 ->  % v6
    prepare_node_info({Addr, ?DEFAULT_PORT});

prepare_node_info(Addr) when is_binary(Addr) ->
    case re2:match(Addr, ?IPV4_RE) of
        {match, [_, IP, <<>>]} ->
            prepare_node_info({binary_to_list(IP), ?DEFAULT_PORT});
        {match, [_, IP, Port]} ->
            {PortInt, []} = string:to_integer(binary_to_list(Port)),
            prepare_node_info({binary_to_list(IP), PortInt});
        nomatch ->
            prepare_node_info({binary_to_list(Addr), ?DEFAULT_PORT})
    end;
prepare_node_info(Addr) when is_list(Addr) ->
    case re2:match(Addr, ?IPV4_RE) of
        {match, [_, IP, <<>>]} ->
            prepare_node_info({binary_to_list(IP), ?DEFAULT_PORT});
        {match, [_, IP, Port]} ->
            {PortInt, []} = string:to_integer(binary_to_list(Port)),
            prepare_node_info({binary_to_list(IP), PortInt});
        nomatch ->
            prepare_node_info({Addr, ?DEFAULT_PORT})
    end.


-spec get_protocol_version() -> integer().

get_protocol_version() ->
    case get(protocol_version) of
        undefined ->
            ProtocolVersion0 = application:get_env(cqerl, protocol_version, ?DEFAULT_PROTOCOL_VERSION),
            put(protocol_version, ProtocolVersion0),
            ProtocolVersion0;
        ProtocolVersion0 ->
            ProtocolVersion0
    end.

-spec put_protocol_version(integer()) -> ok.

put_protocol_version(Val) when is_integer(Val) ->
    put(protocol_version, Val).

dec_stats_count(NodeKey, Stats) ->
    case orddict:find(NodeKey, Stats) of
        {ok, #cql_client_stats{count = 1}} -> orddict:erase(NodeKey, Stats);
        {ok, Stat = #cql_client_stats{count = Count}} -> orddict:store(NodeKey, Stat#cql_client_stats{count=Count-1}, Stats);
        error -> Stats
    end.
