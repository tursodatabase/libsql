# Rust libSQL client library

libSQL Rust client library can be used to communicate with [sqld](https://github.com/libsql/sqld/) natively over HTTP protocol with native Rust interface.

At the moment the library works with the following backends:
 - local
 - reqwest
 - Cloudflare Workers environment (optional)

## Quickstart

In order to use the database in your project, just call `libsql_client::connect()`:
```rust
    let db = libsql_client::connect()?;
```

The only thing you need to provide is an env variable with the database URL, e.g.
```
export LIBSQL_CLIENT_URL="file:////tmp/example.db"
```
for a local database stored in a file, or
```
export LIBSQL_CLIENT_URL="https://user:pass@example.turso.io"
```
for a remote database connection.

You can also explicitly use a specific backend. Examples of that are covered in the next paragraphs.

### Local

In order to connect to the database, set up the URL to point to a local path:
```
export LIBSQL_CLIENT_URL = "/tmp/example.db"
```

`local_backend` feature is enabled by default, so add the dependency like this:
```
cargo add libsql-client
```

Example for how to connect to the database and perform a query:
```rust
    let db = libsql_client::local::Connection::connect_from_env()?;
    let response = db
        .execute("SELECT * FROM table WHERE key = 'key1'")
        .await?;
    (...)
```


### Reqwest
In order to connect to the database, set up the following env variables:
```
export LIBSQL_CLIENT_URL = "https://your-db-url.example.com"
export LIBSQL_CLIENT_USER = "me"
export LIBSQL_CLIENT_PASS = "my-password"
```
... or just the URL, if it contains the required auth information:
```
export LIBSQL_CLIENT_URL = "https://user@pass:your-db-url.example.com"
```

`reqwest_backend` feature is enabled by default, so add the dependency like this:
```
cargo add libsql-client
```

Example for how to connect to the database and perform a query from a GET handler:
```rust
    let db = libsql_client::reqwest::Connection::connect_from_env()?;
    let response = db
        .execute("SELECT * FROM table WHERE key = 'key1'")
        .await?;
    (...)
```

### Cloudflare Workers

In order to connect to the database, set up the following variables in `.dev.vars`, or register them as secrets:
```
LIBSQL_CLIENT_URL = "https://your-db-url.example.com"
LIBSQL_CLIENT_USER = "me"
LIBSQL_CLIENT_PASS = "my-password"
```

Add it as dependency with `workers_backend` backend enabled. Turn off default features, as they are not guaranteed to compile to `wasm32-unknown-unknown`,
which is required in this environment:
```
cargo add libsql-client --no-default-features -F workers_backend
```

Example for how to connect to the database and perform a query from a GET handler:
```rust
router.get_async("/", |_, ctx| async move {
    let db = libsql_client::workers::Connection::connect_from_ctx(&ctx)?;
    let response = db
        .execute("SELECT * FROM table WHERE key = 'key1'")
        .await?;
    (...)
```
