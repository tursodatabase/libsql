# libSQL API for Rust

## Getting Started

#### Connecting to a database

```rust
use libsql_core::Database;

let db = Database::open("hello.db");

let conn = db.connect().unwrap();
```

#### Creating a table

```rust
conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ()).unwrap();
```

#### Inserting rows into a table

```rust
conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ()).unwrap();
```

#### Querying rows from a table

```rust
let rows = conn.execute("SELECT * FROM users WHERE email = ?", params!["alice@example.org"]).unwrap().unwrap();
let row = rows.next().unwrap().unwrap();
// prints "alice@example.org"
println!("{}", row.get::<&str>(0).unwrap());
```

## Developing

Setting up the environment:

```sh
export LIBSQL_STATIC_LIB_DIR=$(pwd)/../../.libs
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
cargo bench
```

Run benchmarks and generate flamegraphs:

```console
echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
cargo bench --bench benchmark -- --profile-time=5
```
