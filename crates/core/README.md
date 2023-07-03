# LibSQL API for Rust

LibSQL is an embeddable SQL database engine based on SQLite.
This Rust API is a batteries-included wrapper around the SQLite C API to support transparent replication while retaining compatibility with the SQLite ecosystem, such as the SQL dialect and extensions. If you are building an application in Rust, this is the crate you should use.
There are also libSQL language bindings of this Rust crate to other languages such as [JavaScript](../bindings/js), [Python](../bindings/python), [Go](../bindings/go), and [C](../bindings/c).

## Getting Started

To get started, you first need to create a [`Database`] object and then open a [`Connection`] to it, which you use to query:

```rust
use libsql_core::Database;

let db = Database::open(":memory:");
let conn = db.connect().unwrap();
conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)") .unwrap();
conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')").unwrap();
```
