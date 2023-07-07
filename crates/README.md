# LibSQL API

LibSQL is an embeddable SQL database engine based on SQLite.

The libSQL API a batteries-included wrapper around the SQLite C API to support transparent replication while retaining compatibility with the SQLite ecosystem, such as the SQL dialect and extensions.

## Getting Started

The libSQL API supports the following programming languages:

* ✅ [Rust](core) 
* ✅ [Python](bindings/python)
* 👷 [JavaScript](bindings/js)
* 👷 [Go](bindings/go)
* 👷 [C](bindings/c)

## Developing

Setting up the environment:

```sh
export LIBSQL_STATIC_LIB_DIR=$(pwd)/../.libs
```

Building the APIs:

```sh
cargo build
```

Running the tests:

```sh
cargo test
```

Running the benchmarks:

```sh
cargo test
```

Run benchmarks and generate flamegraphs:

```console
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
cargo bench --bench benchmark -- --profile-time=5
```
