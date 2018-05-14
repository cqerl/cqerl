# CQErl

Native Erlang client for CQL3 over Cassandra's latest binary protocol v4.

[**Usage**](#usage) &middot; [Connecting](#connecting) &middot; [Clusters](#clusters) &middot; [Performing queries](#performing-queries) &middot; [Query options](#providing-options-along-queries) &middot; [Batched queries](#batched-queries) &middot; [Reusable queries](#reusable-queries) &middot; [Data types](#data-types)

[**Installation**](#installation) &middot; [**Compatibility**](#compatibility) &middot; [**Tests**](#tests) &middot; [**License**](#license)

### At a glance

CQErl offers a simple Erlang interface to Cassandra using the latest CQL version. The main features include:

* Automatic connection pooling, with hash-based allocation (a la [dispcount][9])
* Single or multi-cluster support
* Batched queries
* Variable bindings in CQL queries (named or positional)
* Automatic query reuse when including variable bindings
* Collection types support
* User-defined type support
* Tunable consistency level
* [Automatic pagination support](#pagination)
* Synchronous or asynchronous queries
* Automatic compression (using lz4 or snappy if available)
* SSL support
* Pluggable authentication (as long as it's [SASL][2]-based)

CQErl was designed to be as simple as possible on your side. You just provide the configuration you want as environment variables, and ask to get a client everytime you need to perform a transient piece of work (e.g. handle a web request). You do not need to (and in fact *should not*) keep a client in state for a long time. Under the hood, CQErl maintains a pool of persistent connections with Cassandra and this pattern is the best way to ensure proper load balancing of requests across the pool.

### Usage

##### Connecting

If you installed cassandra and didn't change any configuration related to authentication or SSL, you should be able to connect like this

```erlang
{ok, Client} = cqerl:get_client({}).
```

You do not need to close the connection after you've finished using it.

#### Legacy Mode (pooler)

The default mode of operation uses a hash of the user process's PID to allocate
clients, in a similar way to the system used by [dispcount][9].

The old mode used [pooler][1] to manage connections, and
has been around for quite a while, so is reasonably well tested. It does,
however, have a couple of performance bottlenecks that may limit scalability on
larger or more heaviliy loaded systems.

To use the old mode, and be able to use the legacy `new_client/0,1,2` API to get a hold of a
client process, set

```erlang
{mode, pooler}
```

in your application config (see below). In this mode, rather than calling
`cqerl:get_client/2`, call `cqerl:new_client/2` with the same arguments.
Calling `cqerl:close_client/1` *is* required in legacy mode.

#### All modes

1. The first argument to `cqerl:get_client/2,1` or `cqerl:new_client/2,1` is the node to which you wish to connect as `{Ip, Port}`. If empty, it defaults to `{"127.0.0.1", 9042}`, and `Ip` can be given as a string, or as a tuple of components, either IPv4 or IPv6.

    - If the default port is used, you can provide just the IP address as the first argument, either as a tuple, list or binary.
    - If both the default port and localhost are used, you can just provide an empty tuple as the first argument.

2. The second possible argument (when using `cqerl:get_client/2` or `cqerl:new_client/2`) is a list of options, that include:

    - `keyspace` which determines in which keyspace all subsequent requests operate, on that connection.
    - `auth` (mentionned below)
    - `ssl` (which is `false` by default, but can be set to a list of SSL options) and `keyspace` (string or binary).
    - `protocol_version` to [connect to older Cassandra instances](#connecting-to-older-cassandra-instances).

    Other options include `pool_max_size`, `pool_min_size`, and `pool_cull_interval` which are used to configure [pooler][1] (see its documentation to understand those options)



    If you've set simple username/password authentication scheme on Cassandra, you can provide those to CQErl

    ```erlang
    {ok, Client} = cqerl:get_client({}, [{auth, {cqerl_auth_plain_handler, [{"test", "aaa"}]}}]).
    ```

    Since Cassandra implements pluggable authentication mechanisms, CQErl also allows you to provide custom authentication modules (here `cqerl_auth_plain_handler`). The options you pass along with it are given to the module's `auth_init/3` as its first argument.

3. You can leverage one or more clusters of cassandra nodes by setting up [clusters](#clusters). When set up, you can use

    1. `cqerl:get_client()` if you have just a single main cluster
    2. `cqerl:get_client(ClusterKey)` if you want to get a client from a specific, identified cluster

#### Using environment variables

All the options given above can be provided as environment variables, in which case they are used as default (and overridable) values to any `cqerl:get_client` calls. You can also provide a `cassandra_nodes` variable containing a list of the tuples used as the first argument to `cqerl:get_client` (see [clusters](#clusters) for more explanations). So for example, in your `app.config` or `sys.config` file, you could have the following content:

```erlang
[
  {cqerl, [
            {cassandra_nodes, [ { "127.0.0.1", 9042 } ]},
            {ssl, [ {cacertfile, "cassandra.pem"} ]},
            {auth, {cqerl_auth_plain_handler, [ {"test", "aaa"} ]}}
          ]},
]
```

Doing so will fire up connection pools as soon as the CQErl application is started. So when later on you call `cqerl:get_client`, chances are you will hit a preallocated connection (unless they're so busy that CQErl needs to fire up new ones). In fact, if you provide the `cassandra_nodes` environment variable, you can call `cqerl:get_client/0`, which chooses an available client at random.

#### Clusters

With CQErl clusters, you can configure either a single set of cassandra nodes from which you can draw a client at any time, or multiple sets that serve different purposes.

##### Single cluster

You can prepare a single cluster setup using this structure in your sys.config file:

```erlang
[
  {cqerl, [ {cassandra_nodes, [
                % You can use any of the forms below to specify a cassandra node
                { "127.0.0.1", 9042 },
                { {127, 0, 0, 2}, 9042 },
                "127.0.0.3"
            ]},
            { keyspace, dev_keyspace }
          ]},
]
```

or, equivalently, there's an API you can use to add nodes to the single main cluster:

```erlang
cqerl_cluster:add_nodes([
    { "127.0.0.1", 9042},
    { {127, 0, 0, 2}, 9042 },
    "127.0.0.3"
]).
```

or, with connection options:

```erlang
cqerl_cluster:add_nodes([
    { "127.0.0.1", 9042},
    { {127, 0, 0, 2}, 9042 },
    "127.0.0.3"
], [
    { keyspace, dev_keyspace }
]).
```

When your main cluster is configured, you can just use `cqerl:get_client/0` to get a client random from the cluster.

##### Multiple clusters

You can prepare multiple clusters using this structure in your sys.config file:

```erlang
[
  {cqerl, [ {cassandra_clusters, [
                { config, {
                    [ "127.0.0.1", "127.0.0.3" ],
                    [ { keyspace, config } ]
                }},
                { operations, {
                    [ "127.0.0.1:9042", {"127.0.0.1", 9042} ],
                    [ { keyspace, operations } ]
                }}
            ]},
          ]},
]
```

or, equivalently, there's an API you can use to add nodes to particular clusters:

```erlang
cqerl_cluster:add_nodes(config, [
    { "127.0.0.1", 9042},
    "127.0.0.3"
], [
    { keyspace, config }
]).
cqerl_cluster:add_nodes(operations, [
    { "127.0.0.1", 9042},
    "127.0.0.3"
], [
    { keyspace, operations }
]).
```

##### Options

There are two application environment variables that may be set to change query behaviour:

* `{maps, true}` will cause query result rows to be returned as maps instead of proplists
* `{text_uuids, true}` will cause `timeuuid` and `uuid` fields to be returned as binary strings in canonical form (eg `<<"5620c844-e98d-11e5-b97b-08002719e96e">>`) rather than pure binary.

##### Performing queries

Performing a query can be as simple as this:

```erlang
{ok, Result} = cqerl:run_query(Client, "SELECT * FROM users;").

% Equivalent to
{ok, Result} = cqerl:run_query(Client, <<"SELECT * FROM users;">>).

% Also equivalent to
{ok, Result} = cqerl:run_query(Client, #cql_query{statement = <<"SELECT * FROM users;">>}).
```

It can also be performed asynchronously using

```erlang
Tag = cqerl:send_query(Client, "SELECT * FROM users;"),
receive
    {result, Tag, Result} ->
        ok
end.
```

In situations where you do not need to wait for the response at all, it's perfectly fine to produce this sort of pattern:

```erlang
{ok, Client} = cqerl:get_client(),
cqerl:send_query(Client, #cql_query{statement="UPDATE secrets SET value = null WHERE id = ?;",
                                    values=[{id, <<"42">>}]}),
cqerl:close_client(Client).
```

That is, you can grab a client only the send a query, then you can get rid of it. CQErl will still perform it,
the difference being that no response will be sent back to you.

Here's a rundown of the possible return values

* `SELECT` queries will yield result of type `#cql_result{}` (more details below).
* Queries that change the database schema will yield result of type `#cql_schema_changed{type, keyspace, table}`
* Other queries will yield `void` if everything worked correctly.
* In any case, errors returned by cassandra in response to a query will be the return value (`{error, Reason}` in the synchronous case, and `{error, Tag, Reason}` in the asynchronous case).

###### `#cql_result{}`

The return value of `SELECT` queries will be a `#cql_result{}` record, which can be used to obtain rows as proplists and fetch more result if available

```erlang
{ok, _SchemaChange} = cqerl:run_query(Client, "CREATE TABLE users(id uuid, name varchar, password varchar);"),
{ok, void} = cqerl:run_query(Client, #cql_query{
    statement = "INSERT INTO users(id, name, password) VALUES(?, ?, ?);",
    values = [
        {id, new},
        {name, "matt"},
        {password, "qwerty"}
    ]
}),
{ok, Result} = cqerl:run_query(Client, "SELECT * FROM users;").

Row = cqerl:head(Result),
Tail = cqerl:tail(Result),
{Row, Tail} = cqerl:next(Result),
1 = cqerl:size(Result),
0 = cqerl:size(Tail),
empty_dataset = cqerl:next(Tail),
[Row] = cqerl:all_rows(Result),

<<"matt">> = proplists:get_value(name, Row),
<<"qwerty">> = proplists:get_value(password, Row).
```

##### Pagination

`#cql_result{}` can also be used to fetch the next page of result, if applicable, synchronously or asynchronously. This uses the automatic paging mechanism described [here][10].

```erlang

case cqerl:has_more_pages(Result) of
    true -> {ok, Result2} = cqerl:fetch_more(Result);
    false -> ok
end,

Tag2 = cqerl:fetch_more_async(Result),
receive
    {result, Tag2, Result2} -> ok
end.
```

###### `#cql_schema_changed{}`

`#cql_schema_changed{}` is returned from queries that change the database schema somehow (e.g. `ALTER`, `DROP`, `CREATE`, and so on). It includes:

1. The `type` of change, either `created`, `updated` or `dropped`
2. The name of the `keyspace` where the change happened, as a binary
3. If applicable, the name of `table` on which the change was applied, as a binary

##### Providing options along queries

When performing queries, you can provide more information than just the query statement using the `#cql_query{}` record, which includes the following fields:

1. The query `statement`, as a string or binary
2. `values` for binding variables from the query statement (see next section).

3. You can tell CQErl to consider a query `reusable` or not (see below for what that means). By default, it will detect binding variables and consider it reusable if it contains (named or not) any. Queries containing *named* binding variables will be considered reusable no matter what you set `reusable` to. If you explicitely set `reusable` to `false` on a query having positional variable bindings (`?`), you would provide values with in `{Type, Value}` pairs instead of `{Key, Value}`.
4. You can specify how many rows you want in every result page using the `page_size` (integer) field. The devs at Cassandra recommend a value of 100 (which is the default).
5. You can also specify what `consistency` you want the query to be executed under. Possible values include:

    * `any`
    * `one`
    * `two`
    * `three`
    * `quorum`
    * `all`
    * `local_quorum`
    * `each_quorum`
    * `local_one`

6. In case you want to perform a [lightweight transaction][4] using `INSERT` or `UPDATE`, you can also specify the `serial_consistency` that will be use when performing it. Possible values are:

    * `serial`
    * `local_serial`

##### Variable bindings

In the `#cql_query{}` record, you can provide `values` as a `proplists` or `map`, where the keys are all **atoms** and match the column names or binding variable names in the statement, in **lowercase**.

Example:

```erlang
% Deriving the value key from the column name
#cql_query{statement="SELECT * FROM table1 WHERE id = ?", values=[{id, SomeId}]},

% Explicitly providing a binding variable name
#cql_query{statement="SELECT * FROM table1 WHERE id = :id_value", values=[{id_value, SomeId}]},
```

Special cases include:

- providing `TTL` and `TIMESTAMP` option in statements, in which case the proplist key would be `[ttl]` and `[timestamp]` respectively. Note that, while values for a column of type `timestamp` are provided in **milliseconds**, a value for the `TIMESTAMP` option is expected in **microseconds**.
- `UPDATE keyspace SET set = set + ? WHERE id = 1;`. The name for this variable binding is `set`, the name of the column, and it's expected to be an erlang **list** of values.
- `UPDATE keyspace SET list = list + ? WHERE id = 1;`. The name for this variable binding is `list`, the name of the column, and it's expected to be an erlang **list** of values.
- `UPDATE keyspace SET map[?] = 1 WHERE id = 1;`. The name for this variable binding is `key(map)`, where `map` is the name of the column.
- `UPDATE keyspace SET map['key'] = ? WHERE id = 1;`. The name for this variable binding is `value(map)`, where `map` is the name of the column.
- `UPDATE keyspace SET list[?] = 1 WHERE id = 1;`. The name for this variable binding is `idx(list)`, where `list` is the name of the column.
- `SELECT * FROM keyspace LIMIT ?`. The name for the `LIMIT` variable is `[limit]`.

    Also, when providing the value for a `uuid`-type column, you can give the value `new`, `strong` or `weak`, in which case CQErl will generate a random UUID (v4), with either a *strong* or *weak* number random generator.

    Finally, when providing the value for a `timeuuid` or `timestamp` column, you can give the value `now`, in which case CQErl will generate a normal timestamp, or a UUID (v1) matching the current date and time.

##### Batched queries

To perform batched queries (which can include any non-`SELECT` [DML][5] statements), simply put one or more `#cql_query{}` records in a `#cql_query_batch{}` record, and run it in place of a normal `#cql_query{}`. `#cql_query_batch{}` include the following fields:

1. The `consistency` level to apply when executing the batch of queries.
2. The `mode` of the batch, which can be `logged`, `unlogged` or `counter`. Running a batch in *unlogged* mode removes the performance penalty of enforcing atomicity. The *counter* mode should be used to perform batched mutation of counter values.
3. Finally, you must specify the list of `queries`.

```erlang
InsertQ = #cql_query{statement = "INSERT INTO users(id, name, password) VALUES(?, ?, ?);"},
{ok, void} = cqerl:run_query(Client, #cql_query_batch{
  mode=unlogged,
  queries=[
    InsertQ#cql_query{values = [{id, new},{name, "sean"},{password, "12312"}]},
    InsertQ#cql_query{values = [{id, new},{name, "jenna"},{password, "11111"}]},
    InsertQ#cql_query{values = [{id, new},{name, "kate"},{password, "foobar"}]}
  ]
}).
```

##### Reusable queries

If any of the following is true:

* you set `#cql_query{}`'s `reusable` field to `true`
* the query contains positional variable bindings (`?`) and you did not explicitely `reusable` to false
* the query contains named variable bindings (`:name`) (ignores the value of `reusable`)

the query is considered *reusable*. This means that the first time this query will be performed, CQErl will ask the connected Cassandra node to prepare the query, after which, internally, a query ID will be used instead of the query statement when executing it. That particular cassandra node will hold on to the prepared query on its side and subsequent queries *that use exactly the same statement* [will be performed faster and with less network traffic][7].

CQErl can tell which query has been previously prepared on which node by keeping a local cache, so all of this happens correctly and transparently.

##### Data types

Here is a correspondance of cassandra column types with their equivalent Erlang types (bold denotes what will used in result sets, the rest is what is accepted).

Cassandra Column Type | Erlang types
----------------------|-----------------
ascii                 | **binary**, string (only US-ASCII)
bigint                | **integer** (signed 64-bit)
blob                  | **binary**
boolean               | `true`, `false`
counter               | **integer** (signed 64-bit)
decimal               | `{Unscaled :: integer(), Scale :: integer()}`
double                | **float** (signed 64-bit)
float                 | **float** (signed 32-bit)
int                   | **integer** (signed 32-bit)
timestamp             | **integer** (milliseconds, signed 64-bit), `now`, [binary or string][6]
uuid                  | **binary**, `new`
varchar               | **binary**, string
varint                | **integer** (arbitrary precision)
timeuuid              | **binary**, `now`
inet                  | `{X1, X2, X3, X4}` (IPv4), `{Y1, Y2, Y3, Y4, Y5, Y6, Y7, Y8}` (IPv6), string or binary


### Connecting to older Cassandra instances

By default, this client library assumes we're talking to a 2.2+ or 3+ instance of Cassandra. 2.1.x the latest native protocol (v4) which is required to use some of the newest datatypes and optimizations. To tell CQErl to use the older protocol version (v3), which is required to connect to a 2.1.x instance of Cassandra, you can set the `protocol_version` option to the integer `3`, in your configuration file, i.e.

```erlang
[
  {cqerl, [
            {cassandra_nodes, [ { "127.0.0.1", 9042 } ]},
            {protocol_version, 3}
          ]},
]
```

or in a `cqerl:get_client/2` or `cqerl:get_client/2` call

```erlang
{ok, Client} = cqerl:get_client("127.0.0.1:9042", [{protocol_version, 3}, {keyspace, oper}]).
```

### Installation

Just include this repository in your project's `rebar.config` file and run `./rebar get-deps`. See [rebar][3] for more details on how to use rebar for Erlang project management.

### Compatibility

As said earlier, this library uses Cassandra's newest native protocol versions (v4, or v3 [optionally](#connecting-to-older-cassandra-instances)), which is said to perform better than the older Thrift-based interface. It also speaks CQL version 3, and uses new features available in Cassandra 2.X, such as paging, parametrization, query preparation and so on.

All this means is that this library works with Cassandra 2.1.x (2.2+ or 3+ recommended), configured to enable the native protocol. [This documentation page][8] gives details about the how to configure this protocol. In the `cassandra.yaml` configuration file of your Cassandra installation, the `start_native_transport` need to be set to true and you need to take note of the value for `native_transport_port`, which is the port used by this library.

### Tests

CQErl includes a test suite that you can run yourself, especially if you plan to contribute to this project.

1. Clone this repo on your machine
2. Edit `test/test.config` and put your own cassandra's configurations
3. At the project's top directory, run `make test`

### Changelog

* v1.0.8 released (ESL fork starts here)  
* Fixed connection shutdown on TLS/SSL errors
* Fixed connection shutdown on unknown TCP errors
* Fixed `tcp_opts` handling
* Fixed supervisor restart intensity which prevents connection pool from shutting down without a reason
* Added `heartbeat` feature. This is enabled by default and can be disabled by adding `{heartbeat_interval, 0}` option. Value other then 0 for this option is an heartbeat interval in milliseconds.

### License

The MIT License (MIT)

Copyright (c) 2013 Mathieu D'Amours

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

[1]: https://github.com/seth/pooler
[2]: http://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer
[3]: https://github.com/rebar/rebar
[4]: http://www.datastax.com/documentation/cassandra/2.0/webhelp/index.html#cassandra/dml/dml_about_transactions_c.html
[5]: http://en.wikipedia.org/wiki/Data_manipulation_language
[6]: http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/cql_data_types_c.html#reference_ds_dsf_555_yj
[7]: http://www.datastax.com/dev/blog/client-side-improvements-in-cassandra-2-0
[8]: http://www.datastax.com/documentation/cassandra/2.0/cassandra/configuration/configCassandra_yaml_r.html
[9]: https://github.com/ferd/dispcount
[10]: http://www.datastax.com/dev/blog/client-side-improvements-in-cassandra-2-0
