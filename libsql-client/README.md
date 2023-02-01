# Rust libSQL client library

libSQL Rust client library can be used to communicate with [sqld](https://github.com/libsql/sqld/) over HTTP protocol with nativeRust interface.

At the moment the library works exclusively in Cloudflare Workers environment, but it is expected to be a general purpose library, with Workers being just one of its backends.

In order to connect to the database, set up the following variables in `.dev.vars`, or register them as secrets:
```
LIBSQL_CLIENT_URL = "your-db-url.example.com"
LIBSQL_CLIENT_USER = "me"
LIBSQL_CLIENT_PASS = "my-password"
```

Example for how to connect to the database and perform a query from a GET handler:
```rust
router.get_async("/", |_, ctx| async move {
    let db = libsql_client::Session::connect_from_ctx(&ctx)?;
    let response = db
        .execute("SELECT * FROM table WHERE key = 'key1'")
        .await?;
    (...)
```
