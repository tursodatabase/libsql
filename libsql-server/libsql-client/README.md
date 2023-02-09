# Rust libSQL client library

libSQL Rust client library can be used to communicate with [sqld](https://github.com/libsql/sqld/) over HTTP protocol with native Rust interface.

At the moment the library works with the following backends:
 - reqwest
 - Cloudflare Workers environment

## Reqwest
In order to connect to the database, set up the following env variables:
```
export LIBSQL_CLIENT_URL = "your-db-url.example.com"
export LIBSQL_CLIENT_USER = "me"
export LIBSQL_CLIENT_PASS = "my-password"
```

Add it as dependency with `reqwest_backend` backend enabled:
```
cargo add libsql-client -F reqwest_backend
```

Example for how to connect to the database and perform a query from a GET handler:
```rust
    let db = libsql_client::reqwest::Connection::connect_from_env()?;
    let response = db
        .execute("SELECT * FROM table WHERE key = 'key1'")
        .await?;
    (...)
```

## Cloudflare Workers

In order to connect to the database, set up the following variables in `.dev.vars`, or register them as secrets:
```
LIBSQL_CLIENT_URL = "your-db-url.example.com"
LIBSQL_CLIENT_USER = "me"
LIBSQL_CLIENT_PASS = "my-password"
```

Add it as dependency with `workers_backend` backend enabled:
```
cargo add libsql-client -F workers_backend
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
