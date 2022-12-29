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

and connect to it with psql:

```console
psql -h 127.0.0.1 -p 5000
```

## Building from Sources

### Dependencies

**Linux:**

```console
./scripts/install-deps.sh
```

### Building

```console
cargo build
```

### Running tests

```console
make test
```
