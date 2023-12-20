# Bottomless S3-compatible virtual WAL for libSQL
##### Work in heavy progress!

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

## CLI
The command-line interface supports browsing, restoring and removing snapshot generations.
It can be installed as a standalone executable with:
```sh
RUSTFLAGS="--cfg uuid_unstable" cargo install bottomless-cli
```
Alternatively, bottomless-cli is available from the repository by running `cargo run`.
Available commands:
```
$ bottomless-cli --help
Bottomless CLI

Usage: bottomless-cli [OPTIONS] <COMMAND>

Commands:
  ls       List available generations
  restore  Restore the database
  rm       Remove given generation from remote storage
  help     Print this message or the help of the given subcommand(s)

Options:
  -e, --endpoint <ENDPOINT>  
  -b, --bucket <BUCKET>      
  -d, --database <DATABASE>  
  -h, --help                 Print help information
```

### Examples

#### Listing generations
```
[sarna@sarna-pc test]$ bottomless-cli -e http://localhost:9000 ls -v -l3
e4eb3c21-ff53-7b2e-a6ea-ca396f4df9b1
	created at (UTC):     2022-12-23 08:24:52.500
	change counter:       [0, 0, 0, 51]
	consistent WAL frame: 0
	WAL frame checksum:   0
	main database snapshot:
		object size:   408
		last modified: 2022-12-23T08:24:53Z

e4eb3c22-0359-7af6-9acb-285ed7b6ed59
	created at (UTC):     2022-12-23 08:24:51.470
	change counter:       [0, 0, 0, 51]
	consistent WAL frame: 1
	WAL frame checksum:   5335f2a044d2f455
	main database snapshot:
		object size:   399
		last modified: 2022-12-23T08:24:52Z

e4eb3c22-0941-73eb-85df-4e8552a0e88c
	created at (UTC):     2022-12-23 08:24:49.958
	change counter:       [0, 0, 0, 50]
	consistent WAL frame: 10
	WAL frame checksum:   6ac65882f9a2dba7
	main database snapshot:
		object size:   401
		last modified: 2022-12-23T08:24:51Z
```

#### Restoring the database
```
$ RUST_LOG=info bottomless-cli -e http://localhost:9000 restore
2022-12-23T10:16:10.703557Z  INFO bottomless::replicator: Bucket bottomless exists and is accessible
2022-12-23T10:16:10.709526Z  INFO bottomless_cli: Database: test.db
2022-12-23T10:16:10.713070Z  INFO bottomless::replicator: Restoring from generation e4eb3c29-fe84-7347-a0c0-b9a3a71d0fc2
2022-12-23T10:16:10.727646Z  INFO bottomless::replicator: Restored the main database file
```

#### Removing old snapshots
```
$ bottomless-cli -e http://localhost:9000 rm -v --older-than 2022-12-15
Removed 4 generations
```

## Details
All page writes committed to the database end up being asynchronously replicated to S3-compatible storage.
On boot, if the main database file is empty, it will be restored with data coming from the remote storage.
If the database file is newer, it will be uploaded to the remote location with a new generation number.
If a local WAL file is present and detected to be newer than remote data, it will be uploaded as well.

### Tests
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
