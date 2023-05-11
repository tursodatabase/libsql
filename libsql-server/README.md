# `sqld` - a server mode for libSQL

The `sqld` ("SQL daemon") project is a server mode for [libSQL](https://libsql.org).

Embedded SQL databases such as libSQL and SQLite are great for a lot of use cases, but sometimes you really do want to consume your database as a server.
For example, with serverless apps, fitting a database engine, as slim as it may be, might be hard, and even when it's _possible_, it might be really inconvenient, which is why we created `sqld`.

With `sqld` you can use SQLite-like interface in your app, but have a transparent proxy translates the C API calls to PostgreSQL wire protocol to talk to a database server, which is internally libSQL.

_Disclaimer: although you can connect to `sqld` with a PostgreSQL client and many things just work because PostgreSQL and SQLite are so similar, the goal of this project is to provide SQLite compatibility at the client.
That is, whenever there's a conflict in behaviour between SQLite and PostgreSQL, we always go with SQLite behavior.
However, if libSQL starts to provide more [PostgreSQL compatibility](https://github.com/libsql/libsql/issues/80), `sqld` will also support that._

## Clients

* [TypeScript and JavaScript](https://github.com/libsql/libsql-client-ts)
* [Rust](https://github.com/libsql/libsql-client-rs)
* [Go](https://github.com/libsql/libsql-client-go)

## Features

* SQLite dialect layered on top of HTTP or the PostgreSQL wire protocol.
* TypeScript/JavaScript, Rust, Go and Python clients
* SQLite-compatible API that you can drop-in with `LD_PRELOAD` in your application to switch from local database to a remote database.
* Read replica support.
* Integration with [mvSQLite](https://github.com/losfair/mvsqlite) for high availability and fault tolerance.
# SQLite extensions support
 
## Roadmap

* Client authentication and TLS
* Integration with libSQL's [bottomless storage](https://github.com/libsql/sqld/tree/main/bottomless)

## Getting Started

Start a server with a postgres and http listeners, writing to the local SQLite-compatible file `foo.db`:

```console
sqld -d foo.db -p 127.0.0.1:5432 --http-listen-addr=127.0.0.1:8000
```

connect to it with psql:

```console
psql -h 127.0.0.1
```

or HTTP:

```console
curl -s -d '{\"statements\": [\"SELECT * from sqlite_master;\"] }' http://127.0.0.1:8000
```

You can also inspect the local foo.db file with the `sqlite3` shell

### Loading extensions

Extensions need to be preloaded at startup. To do that, add all of your extensions to a directory,
and add a file called `trusted.lst` with the `sha256sum` of each file to that directory. For example:

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

Then start the server with the `--extensions-path` option pointing at the extension directory


### Integration with S3 bottomless replication

`sqld` is integrated with [bottomless replication subproject](https://github.com/libsql/sqld/tree/main/bottomless). With bottomless replication, the database state is continuously backed up to S3-compatible storage. Each backup session is called a "generation" and consists of the main database file snapshot and replicates [WAL](https://www.sqlite.org/wal.html) pages.

In order to enable automatic replication to S3 storage, run `sqld` with `--enable-bottomless-replication` parameter:
```console
sqld --http-listen-addr=127.0.0.1:8000 --enable-bottomless-replication
```

#### Configuration
Replication needs to be able to access an S3-compatible bucket. The following environment variables can be used to configure the replication:
```sh
LIBSQL_BOTTOMLESS_BUCKET=my-bucket                 # Default bucket name: bottomless
LIBSQL_BOTTOMLESS_ENDPOINT='http://localhost:9000' # address can be overridden for local testing, e.g. with Minio
AWS_SECRET_ACCESS_KEY=                             # regular AWS variables are used
AWS_ACCESS_KEY_ID=                                 # ... to set up auth, regions, etc.
AWS_REGION=                                        # .
```

#### bottomless-cli
Replicated snapshots can be inspected and managed with the official command-line interface.

The tool can be installed via `cargo`:
```console
RUSTFLAGS='--cfg uuid_unstable' cargo install bottomless-cli
```
For usage examples and description, refer to the official bottomless-cli documentation: https://github.com/libsql/sqld/tree/main/bottomless#cli

## Homebrew

You can install `sqld` through homebrew by doing:

```
brew tap libsql/sqld
brew install sqld-beta
```

Note that until a stable version is released, it is provided as a separate tap, with a `beta` suffix.

## Clients

`sqld` ships with a native Javascript driver for TypeScript and Javascript. You can find more information [here](https://www.npmjs.com/package/@libsql/client)

## Building from Sources

### Using Docker/Podman
The easiest way to build `sqld` is using Docker; Build dependencies will be installed in the build container and compilation will be done in a clean, isolated environment. 

```console
docker build -t libsql/sqld:latest .
```

After building the docker image, you can run `sqld` as follows:

```console
docker volume create sqld-data
docker container run -d -v sqld-data:/var/lib/sqld --name sqld -P libsql/sqld:latest
docker container port sqld # View the mapped port for sqld container
```

The following environment variables can be used to configure the `sqld` container:
- `SQLD_DB_PATH` - Database file, defaults to `iku.db`. Absolute path can be used if you want the file in a different directory than `/var/lib/sqld` - note that the folder needs to be writable for `sqld` user (uid 666).
- `SQLD_NODE` - Node type, defaults to `primary`. Valid values are `primary`, `replica` or `standalone`.

All other standard `sqld` environment variables work as well. Try `docker container run --rm -it sqld /bin/sqld --help` to view them.

### Dependencies

**Linux:**

```console
./scripts/install-deps.sh
```

### Submodules

```run
git submodule update --init --force --recursive --depth 1
```

### Building

```console
cargo build
```

### Running tests

```console
make test
```

## License

This project is licensed under the MIT license.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in `sqld` by you, shall be licensed as MIT, without any additional terms or conditions.
