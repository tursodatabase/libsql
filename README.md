# Iku-Turso

This is a prototype of ChiselEdge.

Start a server with:

```console
cargo run -- serve
```

and one or more SQL shells with:

```console
cargo run -- shell
```

## Roadmap

* ChiselEdge proxy with SQLite-like interface
* Optimistic caching in ChiselEdge proxy
* Active replication from ChiselEdge server to write replica
* Passive replication from ChiselEdge server to read replicas
* Data policy enforcement at ChiselEdge server