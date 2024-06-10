# `sqld` - a server mode for libSQL

The `sqld` ("SQL daemon") project is a server mode for
[libSQL](https://github.com/libsql/libsql/).

Embedded SQL databases such as libSQL and SQLite are great for a lot of use
cases, but sometimes you really do want to consume your database as a server.
For example, with apps running on serverless infrastructure, fitting a database
engine might be difficult given the limited size of the hardware. And even when
it's _possible_, it might be really inconvenient. We created `sqld` for this use
case.

## Features

* SQLite dialect layered on top of HTTP.
* SQLite-compatible API that you can drop-in with `LD_PRELOAD` in your
  application to switch from local database to a remote database.
* Read replica support.
* Integration with [mvSQLite](https://github.com/losfair/mvsqlite) for high
  availability and fault tolerance.

## Build and run

Follow the [instructions](../docs/BUILD-RUN.md) to build and run `sqld`
using Homebrew, Docker, or your own Rust toolchain.

## Tests

Run the command below to run all tests for `libsql` and `libsql-server`.

```
cargo xtask test
```

## Client libraries

The following client libraries enable your app to query `sqld` programmatically:

* [TypeScript and JavaScript](https://github.com/libsql/libsql-client-ts)
* [Rust](https://github.com/libsql/libsql-client-rs)
* [Go](https://github.com/libsql/libsql-client-go)
* [Python](https://github.com/libsql/libsql-client-py)

## SQLite extensions support

Extensions must be preloaded at startup. To do that, add all of your extensions
to a directory, and add a file called `trusted.lst` with the `sha256sum` of each
file to that directory. For example:

```console
$ cat trusted.lst
04cd193d2547ff99d672fbfc6dcd7e0b220869a1ab867a9bb325f7374d168533  vector0.so
74f9029cbf6e31b155c097a273e08517eb4e56f2300dede65c801407b01eb248  vss0.so
5bbbe0f80dd7721162157f852bd5f364348eb504f9799ae521f832d44c13a3a1  crypto.so
731a8cbe150351fed02944a00ca586fc60d8f3814e4f83efbe60fcef62d4332b  fuzzy.so
1dbe9e4e58c4b994a119f1b507d07eb7a4311a80b96482c979b3bc0defd485fb  math.so
511bf71b0621977bd9575d71e90adf6d02967008e460066a33aed8720957fecb  stats.so
ae7fff8412e4e66e7f22b9af620bd24074bc9c77da6746221a9aba9d2b38d6a6  text.so
9ed6e7f4738c2223e194c7a80525d87f323df269c04d155a769d733e0ab3b4d0  unicode.so
19106ded4fd3fd4986a5111433d062a73bcf9557e07fa6d9154e088523e02bb0  uuid.so
```

Extensions will be loaded in the order they appear on that file, so if there are
dependencies between extensions make sure they are listed in the proper order.

Then start the server with the `--extensions-path` option pointing at the
extension directory

## Integration with S3 bottomless replication

`sqld` is integrated with [bottomless replication subproject]. With bottomless
replication, the database state is continuously backed up to S3-compatible
storage. Each backup session is called a "generation" and consists of the main
database file snapshot and replicates [SQLite WAL] pages.

In order to enable automatic replication to S3 storage, compile `sqld` with `-F bottomless` flag
and run `sqld` with `--enable-bottomless-replication` parameter:

```bash
sqld --http-listen-addr=127.0.0.1:8000 --enable-bottomless-replication
```

[bottomless replication subproject]: ../bottomless
[SQLite WAL]: https://www.sqlite.org/wal.html

### Configuration

Replication needs to be able to access an S3-compatible bucket. The following
environment variables can be used to configure the replication:

```bash
LIBSQL_BOTTOMLESS_BUCKET=my-bucket                 # Default bucket name: bottomless
LIBSQL_BOTTOMLESS_ENDPOINT='http://localhost:9000' # address can be overridden for local testing, e.g. with Minio
LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY=           # regular AWS variables are used
LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID=               # ... to set up auth, regions, etc.
LIBSQL_BOTTOMLESS_AWS_REGION=                      # .
```

### bottomless-cli

Replicated snapshots can be inspected and managed with the official command-line
interface.

The tool can be installed via `cargo`:

```bash
RUSTFLAGS='--cfg uuid_unstable' cargo install bottomless-cli
```

For usage examples and description, refer to the [bottomless-cli
documentation].

[bottomless-cli documentation]: ../bottomless#cli

## License

This project is licensed under the MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `sqld` by you, shall be licensed as MIT, without any additional terms or conditions.
