# CQErl

*Native Erlang CQL driver*

We needed a good erlang driver to talk CQL3 with a cassandra deployment. [One](https://github.com/lpgauth/cassanderl) only talks Thrift, 
and [the other](https://github.com/ostinelli/erlcassa) talks a dated CQL over Thrift, and has been stalled for 2 years, so we built one.
It talks modern CQL3 over the native binary protocol (v2), can be used raw, or with managed connection pools, using [pooler](https://github.com/seth/pooler).

*It currently is a work in progress though. Stay tuned*
