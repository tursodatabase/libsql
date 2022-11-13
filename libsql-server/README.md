# Iku-Turso

This is a prototype of ChiselEdge, which aims to be:

* A distributed SQL database that speaks SQLite
* Provides low latency reads (read-only replicas over the world)
* Writes happen on a cloud-based primary server
* Enforce programmable policies on data (for example, dynamic data masking)

## Roadmap

* ChiselEdge proxy with SQLite-like interface
* Optimistic caching in ChiselEdge proxy
* Active replication from ChiselEdge server to write replica
* Passive replication from ChiselEdge server to read replicas
* Data policy enforcement at ChiselEdge server

## Getting Started

Start a server with:

```console
cargo run -- serve
```

and one or more SQL shells with:

```console
cargo run -- shell
```
