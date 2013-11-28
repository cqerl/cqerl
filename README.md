# CQErl

Native Erlang driver for CQL3 over Cassandra's binary protocol v2 (a.k.a. what you want as a driver for Cassandra).

### At a glance

CQErl offers a simple Erlang interface to Cassandra using the latest CQL version (v3). The main features include:

* Automatic (and configurable) connection pools using [pooler][1]
* Batched queries
* Variable bindings in CQL queries (named or not)
* Automatic query reuse when including variable bindings
* Collection types support
* Tunable consistency level
* Synchronous or asynchronous queries
* Automatic compression (using lz4 or snappy)
* SSL support
* Pluggable authentication (as long as it's [SASL][2]-based)

### Installation

Just include this repository in your project's `rebar.config` file and run `./rebar get-deps`. See [rebar][3] for more details on how to use rebar for Erlang project management.

### Testing

CQErl includes a test suite that you can run yourself, especially if you plan to contribute to this project. 

1. Clone this repo on your machine
2. Edit `test/test.config` and put your own cassandra's configurations