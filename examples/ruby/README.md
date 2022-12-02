# Ruby example app

To run with SQLite, type:

```console
DB_URI=example.db ruby ruby-sqlite.rb
```

To run with Postgres database, type:

```console
LD_PRELOAD=../../target/debug/libedgeproxy.so DB_URI=postgres://$USER@127.0.0.1:5432 ruby ruby-sqlite.rb
```

To enable tracing, set the `RUST_LOG` environment variable to `trace`:

```
RUST_LOG=trace LD_PRELOAD=../../target/debug/libedgeproxy.so DB_URI=postgres://$USER@127.0.0.1:5432 ruby ruby-sqlite.rb
```
