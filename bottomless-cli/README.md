# Bottomless CLI

The command-line interface helps inspect and manage replicated snapshots stored in S3-compatible storage.

It can be installed as a standalone executable with:

```sh
RUSTFLAGS="--cfg uuid_unstable" cargo install bottomless-cli
```

> [!WARNING]
> The [CLI crate](https://crates.io/crates/bottomless-cli) hasn't been updated in over a year, and while it's being heavily worked on, we recommend building against the source for now.

In order to build from the repository:

```sh
git clone git@github.com:tursodatabase/libsql.git
cargo build -p bottomless-cli --release
cargo install --path .
```

You will now be able to use the `bottomless-cli` command.

Available commands:
```
$ bottomless-cli
Bottomless CLI

Usage: bottomless-cli [OPTIONS] <COMMAND>

Commands:
  copy      Copy bottomless generation locally
  create    Create new generation from database
  ls        List available generations
  restore   Restore the database
  verify    Verify integrity of the database
  rm        Remove given generation from remote storage
  snapshot  Generate and upload a snapshot for a given generation or timestamp
  help      Print this message or the help of the given subcommand(s)

Options:
  -e, --endpoint <ENDPOINT>
  -b, --bucket <BUCKET>
  -d, --database <DATABASE>
  -n, --namespace <NAMESPACE>
      --encryption-key <ENCRYPTION_KEY>
      --db-name <DB_NAME>
  -h, --help  Print help
```

## Examples

### Listing generations

```sh
$ bottomless-cli -e http://localhost:9000 ls -v -l3
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

### Restoring the database

```sh
$ RUST_LOG=info bottomless-cli -e http://localhost:9000 restore
2022-12-23T10:16:10.703557Z  INFO bottomless::replicator: Bucket bottomless exists and is accessible
2022-12-23T10:16:10.709526Z  INFO bottomless_cli: Database: test.db
2022-12-23T10:16:10.713070Z  INFO bottomless::replicator: Restoring from generation e4eb3c29-fe84-7347-a0c0-b9a3a71d0fc2
2022-12-23T10:16:10.727646Z  INFO bottomless::replicator: Restored the main database file
```

### Removing old snapshots
```sh
$ bottomless-cli -e http://localhost:9000 rm -v --older-than 2022-12-15
Removed 4 generations
```
