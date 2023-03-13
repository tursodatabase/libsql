# Bottomless WAL

Bottomless is a libSQL extension which transparently replicates the write-ahead log (WAL) to S3-compatible storage.
It also supports restoring the database state and has a command-line interface to manage the backups.

Documentation:
https://github.com/libsql/sqld/tree/main/bottomless
https://github.com/libsql/sqld/tree/main/bottomless#cli

## Integration

Bottomless is distributed as:
1. A built-in, statically linked extension to libSQL
2. A standalone libSQL extension, dynamically loadable
3. A Rust crate, linkable directly to your Rust projects

In order to build libSQL from source with bottomless statically linked in, run the following:
```sh
# Optionally: git submodule update --init
./configure --enable-bottomless-wal
make
```

## Try it out

The following paragraph explains how to set up bottomless.

### Storage setup

#### AWS S3 bucket
Bottomless is built on top of the official AWS SDK, so it works with regular AWS S3 buckets. Your AWS profile will be taken either from the standard AWS S3 environment variables (`AWS_SECRET_ACCESS_KEY`, `AWS_ACCESS_KEY_ID`, `AWS_REGION` and friends), or the standard `~/.aws` config files. If you have a bucket prepared already, set up the following environment variable to let `bottomless` know where to find it:
```
LIBSQL_BOTTOMLESS_BUCKET=<your-bucket-name>
```
The default bucket name, if not explicitly specified, is expected to be "`bottomless`".

#### Local S3-compatible server

For prototyping, it's convenient to use a local S3-compatible storage server, like [minio](https://min.io/). Once you follow their [quickstart guide](https://charts.min.io/), set up the following environment variables to enable Rust logging and point `bottomless` to the right server:
```
export RUST_LOG=info \
  LIBSQL_BOTTOMLESS_ENDPOINT=http://localhost:9000 \
  AWS_SECRET_ACCESS_KEY=minioadmin \
  AWS_ACCESS_KEY_ID=minioadmin
```

### Open with bottomless WAL enabled

In order for bottomless replication to work, `bottomless` virtual WAL methods should be enabled for your database connection. Here's how you can do that via libSQL shell:
```
libsql -cmd '.open file:/tmp/my-db?wal=bottomless
```

> NOTE: bottomless is implemented as a virtual WAL interface, so it only works in journaling WAL mode.

That's it! `bottomless` will now replicate your WAL to S3-compatible storage.

### Example
```
$ ./libsql -cmd '.open file:/tmp/my-db?wal=bottomless'
2023-03-13T09:18:33.840890Z  INFO bottomless::replicator: Bucket bottomless exists and is accessible
2023-03-13T09:18:33.842732Z  INFO bottomless::replicator: Restoring from generation e4e9a004-983e-775f-bebb-d6255f49aef8
2023-03-13T09:18:33.845875Z  INFO bottomless::replicator: Remote generation is up-to-date, reusing it in this session
2023-03-13T09:18:33.847309Z  INFO bottomless::replicator: Bucket bottomless exists and is accessible
2023-03-13T09:18:33.848741Z  INFO bottomless::replicator: Restoring from generation e4e9a004-983e-775f-bebb-d6255f49aef8
2023-03-13T09:18:33.851274Z  INFO bottomless::replicator: Remote generation is up-to-date, reusing it in this session
libSQL version 0.2.0 (based on SQLite version 3.41.0) 2023-02-05 20:29:10
Enter ".help" for usage hints.
libsql>
```

### Restoring and managing backups
The database is automatically checked if a restore is needed on startup.

On top of that, the [bottomless command-line interface](https://github.com/libsql/sqld/tree/main/bottomless#cli) can be used to inspect the backups, delete old ones, restore any of them manually, etc.
`bottomless-cli` can be installed via `cargo`. Note that a custom unstable flag needs to be specified (as of now), because `bottomless` is implemented with novel uuid v7:
```
RUSTFLAGS='--cfg uuid_unstable' cargo install bottomless-cli
```
Documentation and examples can be found here: https://github.com/libsql/sqld/tree/main/bottomless#cli

