%% @author Mathieu D'Amours <matt@forest.io>
%% @doc Main interface to CQErl cassandra client.

-module(cqerl).

-export([
    start/0,
    run_query/1,
    send_query/1,

    run_query/2,
    send_query/2,

    run_query_named/2,
    send_query_named/2,

    has_more_pages/1,
    fetch_more/1,
    fetch_more_async/1,

    size/1,

    head/1,

    tail/1,
    next/1,

    all_rows/1,
    all_rows/2,

    normalise_node/1,
    normalise_keyspace/1,

    add_group/3,
    add_group/4,

    get_protocol_version/0,
    put_protocol_version/1,

    wait_for_schema_agreement/0
]).

-include("cqerl.hrl").

start() ->
    application:ensure_all_started(cqerl).

%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%            return with the result from Cassandra (synchronously).
%% The <code>Query</code> parameter can be a string, a binary UTF8 string or a <code>#cql_query{}</code> record
%%
%% <pre>#cql_query{
%%     statement :: binary(),
%%     reusable :: boolean(),
%%     consistency :: consistency_level(),
%%     named :: boolean(),
%%     bindings :: list(number() | boolean() | binary() | list() | cqerl_node() | ) | list(tuple())
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
%% How <em>bindings</em> is used depends on the <em>named</em> value. <em>Named</em> is a boolean value indicating whether the parameters in the query are named parameters (<code>:var1</code>). Otherwise,
%% they are assumed to be positional (<code>?</code>). In the first case, <em>bindings</em> is a property list (see <a href="http://www.erlang.org/doc/man/proplists.html">proplists</a>) where keys match the
%% parameter names. In the latter case, <em>bindings</em> should be a simple list of values.

-spec run_query(query()) -> query_result().
run_query(Query) ->
    NQuery = normalise_query(Query),
    Client = select_client(NQuery),
    maybe_run_query_with_client(Client, NQuery).

-spec run_query(keyspace(), query_statement()) -> query_result().
run_query(KS, Q) ->
    run_query(#cql_query{keyspace = KS, statement = Q}).

-spec run_query_named(term(), query()) -> query_result().
run_query_named(Name, Query) ->
    NQuery = normalise_query(Query),
    Client = cqerl_client_pool:get_client(Name),
    maybe_run_query_with_client(Client, NQuery).

%% @doc Check to see if there are more result available

-spec has_more_pages(Continuation :: #cql_result{}) -> true | false.
has_more_pages(#cql_result{cql_query=#cql_query{page_state=undefined}}) -> false;
has_more_pages(#cql_result{}) -> true.



%% @doc Fetch the next page of result from Cassandra for a given continuation. The function will
%%            return with the result from Cassandra (synchronously).

-spec fetch_more(Continuation :: #cql_result{}) -> no_more_results | {ok, #cql_result{}}.
fetch_more(#cql_result{cql_query=#cql_query{page_state=undefined}}) ->
    no_more_results;
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

-spec send_query(query()) -> async_query_result().
send_query(Query) ->
    NQuery = normalise_query(Query),
    Client = select_client(NQuery),
    maybe_send_query_with_client(Client, NQuery).

-spec send_query(keyspace(), query_statement()) -> async_query_result().
send_query(KS, Q) ->
    send_query(#cql_query{keyspace = KS, statement = Q}).

-spec send_query_named(term(), query()) -> async_query_result().
send_query_named(Name, Query) ->
    NQuery = normalise_query(Query),
    Client = cqerl_client_pool:get_client(Name),
    maybe_send_query_with_client(Client, NQuery).


%% @doc Asynchronously fetch the next page of result from cassandra for a given continuation.
%%
%% A success or error message will be sent in response some time later (see {@link send_query/1} for details) unless the
%% connection is dropped.

-spec fetch_more_async(Continuation :: #cql_result{}) -> reference() | no_more_results.
fetch_more_async(#cql_result{cql_query=#cql_query{page_state=undefined}}) ->
    no_more_results;
fetch_more_async(Continuation) ->
    cqerl_client:fetch_more_async(Continuation).

%% @doc The number of rows in a result set

size(#cql_result{dataset=Dataset}) -> length(Dataset).

%% @doc Returns the first row of result, as a property list

head(#cql_result{dataset=[]}) -> empty_dataset;
head(Result) ->
    head(Result, get_options_list()).

%% @private
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

-spec normalise_node({string() | tuple(), non_neg_integer()}) -> cqerl_node().
normalise_node({Host, Port}) when is_tuple(Host), is_integer(Port) ->
    {Host, Port};
normalise_node({Host, Port}) when is_list(Host), is_integer(Port) ->
    case inet:parse_address(Host) of
        {ok, Addr} -> {Addr, Port};
        {error, einval} -> {Host, Port} % Presume it's a DNS name.
    end;
normalise_node(Host) when is_tuple(Host); is_list(Host) ->
    normalise_node({Host, ?DEFAULT_PORT}).

normalise_keyspace(KS) when is_list(KS)   -> list_to_atom(KS);
normalise_keyspace(KS) when is_binary(KS) -> binary_to_atom(KS, latin1);
normalise_keyspace(KS) when is_atom(KS)   -> KS.

-spec add_group([host()], proplists:proplist(), pos_integer()) ->
    group_name().
add_group(Hosts, Opts, ClientsPerServer) ->
    add_group(make_ref(), Hosts, Opts, ClientsPerServer).

-spec add_group(group_name(), [host()], proplists:proplist(), pos_integer()) ->
    group_name().
add_group(undefined, Hosts, Opts, ClientsPerServer) ->
    add_group(Hosts, Opts, ClientsPerServer);
add_group(Name, Hosts, Opts, ClientsPerServer) ->
    FullOpts = merge_opts(Opts),
    lists:foreach(
      fun(Host) ->
              {ok, _} = cqerl_client_sup:add_clients(
                          Name, normalise_node(Host), FullOpts, ClientsPerServer)
      end,
      Hosts),
    cqerl_client_pool:wait_for_client(Name),
    Name.

-spec wait_for_schema_agreement() -> ok.
wait_for_schema_agreement() ->
    cqerl_schema:wait_for_schema_agreement().

%% =================
%% Private functions
%% =================

merge_opts(SetOpts) ->
    strip_dups(
      lists:keymerge(1,
                     lists:keysort(1, proplists:unfold(SetOpts)),
                     lists:keysort(1, get_default_opts()))).

get_default_opts() ->
    [
     {auth, {cqerl_auth_plain_handler, []}},
     {ssl, false},
     {keyspace, undefined},
     {protocol_version, ?DEFAULT_PROTOCOL_VERSION}
    ].

strip_dups([]) -> [];
strip_dups([V]) -> [V];
strip_dups([V = {A, _}, {A, _} | Tail]) ->
    strip_dups([V | Tail]);
strip_dups([X, Y | Tail]) ->
    [X | strip_dups([Y | Tail])].

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

maybe_run_query_with_client({error, E}, _) -> {error, E};
maybe_run_query_with_client({ok, Client}, Query) ->
    cqerl_client:run_query(Client, Query).

maybe_send_query_with_client({error, E}, _) -> {error, E};
maybe_send_query_with_client({ok, Client}, Query) ->
    {ok, cqerl_client:query_async(Client, Query)}.

select_client(#cql_query_batch{keyspace = Keyspace}) ->
    random_select_client(Keyspace);

select_client(Query = #cql_query{keyspace = Keyspace}) ->
    case application:get_env(cqerl, strategy, token_aware) of
        token_aware ->
            ta_select_client(Query);
        _ ->
            random_select_client(Keyspace)
    end.

normalise_query(Batch = #cql_query_batch{queries = Queries,
                                         keyspace = Keyspace}) ->
    Batch#cql_query_batch{queries = [normalise_query(Q) || Q <- Queries],
                          keyspace = normalise_keyspace(Keyspace)};

normalise_query(Statement) when is_list(Statement); is_binary(Statement) ->
    normalise_query(#cql_query{statement = iolist_to_binary(Statement)});

normalise_query(Query = #cql_query{statement = Statement})
  when is_list(Statement) ->
    normalise_query(Query#cql_query{statement = iolist_to_binary(Statement)});

normalise_query(Query = #cql_query{statement = Statement, keyspace = Keyspace})
  when is_binary(Statement) ->
    Query#cql_query{keyspace = normalise_keyspace(Keyspace)}.



ta_select_client(#cql_query{keyspace = undefined}) ->
    random_select_client(undefined);
ta_select_client(Query = #cql_query{keyspace = Keyspace}) ->
    case cqerl_schema:select_client(Query) of
        {ok, Client} -> {ok, Client};
        {error, _} -> random_select_client(Keyspace)
    end.

random_select_client(Keyspace) ->
    cqerl_client_pool:get_random_client(Keyspace).


