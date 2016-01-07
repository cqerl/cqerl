# CQErl

Native Erlang client for CQL3 over Cassandra's binary protocol v4 (a.k.a. what you want as a client for Cassandra).

[**Usage**](#usage) &middot; [Connecting](#connecting) &middot; [Performing queries](#performing-queries) &middot; [Query options](#providing-options-along-queries) &middot; [Batched queries](#batched-queries) &middot; [Reusable queries](#reusable-queries) &middot; [Data types](#data-types)

[**Installation**](#installation) &middot; [**Compatibility**](#compatibility) &middot; [**Tests**](#tests) &middot; [**License**](#license)

---

*This project is still relatively new, so you are welcome to contribute to it with improvements, suggestions and any issues you encounter.*

### At a glance

CQErl offers a simple Erlang interface to Cassandra using the latest CQL version (v3). The main features include:

* Automatic (and configurable) connection pools using [pooler][1]
* Batched queries
* Variable bindings in CQL queries (named or not)
* Automatic query reuse when including variable bindings
* Collection types support
* Tunable consistency level
* Synchronous or asynchronous queries
* Automatic compression (using lz4 or snappy if available)
* SSL support
* Pluggable authentication (as long as it's [SASL][2]-based)

CQErl was designed to be as simple as possible on your side. You just provide the configuration you want as environment variables, and ask for a new client everytime you need to perform a transient piece of work (e.g. handle a web request). You do not need to (and should not) keep a client in state for a long time. Under the hood, CQErl maintains a pool of persistent connections with Cassandra and this pattern is the best way to ensure proper load balancing of requests across the pool.

### Usage

##### Connecting

If you installed cassandra and didn't change any configuration related to authentication or SSL, you should be able to connect like this

```erlang
{ok, Client} = cqerl:new_client({}).
```
    
And close the connection like this

```erlang
cqerl:close_client(Client).
```

1. The first argument to `cqerl:new_client/2` or `cqerl_new_client/1` is the node to which you wish to connect as `{Ip, Port}`. If empty, it defaults to `{"127.0.0.1", 9042}`, and `Ip` can be given as a string, or as a tuple of components, either IPv4 or IPv6.

2. The second possible argument (when using `cqerl:new_client/2`) is a list of options, that include `auth` (mentionned below), `ssl` (which is `false` by default, but can be set to a list of SSL options) and `keyspace` (string or binary). Other options include `pool_max_size`, `pool_min_size` and `pool_cull_interval`, which are used to configure [pooler][1] (see its documentation to understand those options).

If you've set simple username/password authentication scheme on Cassandra, you can provide those to CQErl

```erlang
{ok, Client} = cqerl:new_client({}, [{auth, {cqerl_auth_plain_handler, [{"test", "aaa"}]}}]).
```
    
Since Cassandra implements pluggable authentication mechanisms, CQErl also allows you to provide custom authentication modules (here `cqerl_auth_plain_handler`). The options you pass along with it are given to the module's `auth_init/3` as its first argument.

###### Using environment variables

All the options given above can be provided as environment variables, in which case they are used as default (and overridable) values to any `cqerl:new_client` calls. You can also provide a `cassandra_nodes` variable containing a list of the tuples used as the first argument to `cqerl:new_client`. So for example, in your `app.config` or `sys.config` file, you could have the following content:

```erlang
[
  {cqerl, [
            {cassandra_nodes, [ { "127.0.0.1", 9042 } ]},
            {ssl, [ {cacertfile, "cassandra.pem"} ]},
            {auth, {cqerl_auth_plain_handler, [ {"test", "aaa"} ]}}
          ]},
]
```

Doing so will fire up connection pools as soon as the CQErl application is started. So when later on you call `cqerl:new_client`, chances are you will hit a preallocated connection (unless they're so busy that CQErl needs to fire up new ones). In fact, if you provide the `cassandra_nodes` environment variable, you can call `cqerl:new_client/0`, which chooses an available client at random.

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
{ok, Client} = cqerl:new_client(),
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

`#cql_result{}` can also be used to fetch more result, synchronously or asynchronously

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

In the `#cql_query{}` record, you can provide `values` as a `proplists`, where the keys match the column names or binding variable names in the statement, in **lowercase**. 

Special cases include: 

- providing `TTL` and `TIMESTAMP` option in statements, in which case the proplist key would be `[ttl]` and `[timestamp]` respectively. Note that, while values for a column of type `timestamp` are provided in **milliseconds**, a value for the `TIMESTAMP` option is expected in **microseconds**.
- `UPDATE keyspace SET set = set + ? WHERE id = 1;`. The name for this variable binding is `set`, the name of the column, and it's expected to be an erlang **list** of values.
- `UPDATE keyspace SET list = list + ? WHERE id = 1;`. The name for this variable binding is `list`, the name of the column, and it's expected to be an erlang **list** of values.
- `UPDATE keyspace SET map[?] = 1 WHERE id = 1;`. The name for this variable binding is `key(map)`, where `map` is the name of the column.
- `UPDATE keyspace SET map['key'] = ? WHERE id = 1;`. The name for this variable binding is `value(map)`, where `map` is the name of the column.
- `UPDATE keyspace SET list[?] = 1 WHERE id = 1;`. The name for this variable binding is `idx(list)`, where `list` is the name of the column.

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

### Installation

Just include this repository in your project's `rebar.config` file and run `./rebar get-deps`. See [rebar][3] for more details on how to use rebar for Erlang project management.

### Compatibility

As said earlier, this library uses Cassandra's newest native protocol version (2), which is said to perform better than the older Thrift-based interface. It also speaks CQL version 3, and uses new features available in Cassandra 2.X, such as paging, parametrization, query preparation and so on.

All this means is that this library works with Cassandra 2.X, configured to enable the native protocol. [This documentation page][8] gives details about the how to configure this protocol. In the `cassandra.yaml` configuration file of your Cassandra installation, the `start_native_transport` need to be set to true and you need to take note of the value for `native_transport_port`, which is the port used by this library.

### Tests

CQErl includes a test suite that you can run yourself, especially if you plan to contribute to this project. 

1. Clone this repo on your machine
2. Edit `test/test.config` and put your own cassandra's configurations
3. At the project's top directory, run `make test`

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
