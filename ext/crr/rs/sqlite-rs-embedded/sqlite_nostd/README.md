# sqlite_nostd

Existing Rust bindings for SQLite require the use of `std`. This isn't great when you need to target embedded environments (environments where SQLite often runs!) or WASM.

sqlite_nostd is a set of Rust bindings for SQLite that do not require the `std` crate.
