# libSQL API for Rust

This repository contains the libSQL API for Rust.

## Installation

The library is available on [crates.io](https://crates.io/crates/libsql). To use it in your application, add the following to the `Cargo.toml` of your project:

```toml
[dependencies]
libsql = "0.1.1"
```

## Getting Started

#### Connecting to a database

```rust
use libsql::Database;

fn main() {
    let db = Database::open("hello.db");
    let conn = db.connect().unwrap();
    let rows = conn.execute("SELECT 'hello, world!'", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    println!("{}", row.get::<&str>(0).unwrap());
}
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
println!("{}", row.get::<&str>(0).unwrap());
```

## Developing

See [DEVELOPING.md](DEVELOPING.md) for more information.

## License

This project is licensed under the [MIT license].

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in libSQL by you, shall be licensed as MIT, without any additional
terms or conditions.

[MIT license]: https://github.com/libsql/libsql/blob/main/LICENSE.md
