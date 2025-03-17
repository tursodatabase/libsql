# Bottomless S3-compatible virtual WAL for libSQL

> [!WARNING]
> This project is in heavy development!

This project implements a virtual write-ahead log (WAL) which continuously backs up the data to S3-compatible storage and is able to restore it later.

## How to build
```
LIBSQL_DIR=/path/to/your/libsql/directory make
```
will produce a loadable `.so` libSQL extension with bottomless WAL implementation.
```
LIBSQL_DIR=/path/to/your/libsql/directory make release
```
will do the same, but for release mode.

## Configuration
By default, the S3 storage is expected to be available at `http://localhost:9000` (e.g. a local development [minio](https://min.io) server), and the auth information is extracted via regular S3 SDK mechanisms, i.e. environment variables and `~/.aws/credentials` file, if present. Ref: https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_environment.html

Default endpoint can be overridden by an environment variable too, and in the future it will be available directly from libSQL as an URI parameter:
```
export LIBSQL_BOTTOMLESS_ENDPOINT='http://localhost:9042'
```

Bucket used for replication can be configured with:
```
export LIBSQL_BOTTOMLESS_BUCKET='custom-bucket'
```

On top of that, bottomless is implemented on top of the official [Rust SDK for S3](https://crates.io/crates/aws-sdk-s3), so all AWS-specific environment variables like `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` also work, as well as the `~/.aws/credentials` file.

## How to use
From libSQL shell, load the extension and open a database file with `bottomless` WAL, e.g.:
```sql
.load ../target/debug/bottomless
.open file:test.db?wal=bottomless
PRAGMA journal_mode=wal;
```
Remember to set the journaling mode to `WAL`, which needs to be done at least once, before writing any content, otherwise the custom WAL implementation will not be used.

In order to customize logging, use `RUST_LOG` env variable, e.g. `RUST_LOG=info ./libsql`.

A short demo script is in `test/smoke_test.sh`, and can be executed with:

```sh
LIBSQL_DIR=/path/to/your/libsql/directory make test
```

## Details
All page writes committed to the database end up being asynchronously replicated to S3-compatible storage.
On boot, if the main database file is empty, it will be restored with data coming from the remote storage.
If the database file is newer, it will be uploaded to the remote location with a new generation number.
If a local WAL file is present and detected to be newer than remote data, it will be uploaded as well.

## Tests
A fully local test can be performed by using a local S3-compatible server, e.g. [Minio](https://min.io/). Assuming the server is available at HTTP port 9000,
you can use the following scripts:
```sh
cd test/
export LIBSQL_BOTTOMLESS_ENDPOINT=http://localhost:9000
./smoke_test.sh
./restore_test.sh
```

The `smoke_test` script sets up a new database in WAL mode and 64KiB page size - test.db - and then inserts a few records into the database.
The `restore_test` script syncs with the replication server and fetches the newest database if necessary. Once `smoke_test` ran at least once, `restore_test` should always be able to fetch the database data, even if the local `test.db` file is removed.

The same set of tests also work with remote servers. In case of AWS S3, just make sure that the AWS SDK credentials are valid and the user has permissions for managing the chosen bucket.

## CLI
[See dedicated CLI's documentation]: ../bottomless-cli/README.md
