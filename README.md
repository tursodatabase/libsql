
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libsql/libsql/blob/master/LICENSE.md)
[![Discord](https://img.shields.io/discord/1026540227218640906?color=5865F2&label=discord&logo=discord&logoColor=8a9095)](https://discord.gg/VzbXemj6Rg)

<p align="center">
<img src="https://user-images.githubusercontent.com/331197/205099307-3f20b4e5-96cf-466c-be62-73907e9f2325.png">
</p>

# What is libSQL?

[libSQL](https://turso.tech/libsql) is an open source, open contribution fork of SQLite, created and maintained by [Turso](https://turso.tech). We aim to evolve it to suit many more use cases than SQLite was originally designed for, and plan to use third-party OSS code wherever it makes sense.

libSQL is licensed under an [Open Source License](LICENSE.md), and we adhere to a clear [Code of Conduct](CODE_OF_CONDUCT.md)

## Features

* Embedded replicas that allow you to have replicated database inside your app.
* [libSQL server](libsql-server) for remote SQLite access, similar to PostgreSQL or MySQL
* Supports Rust, JavaScript, Python, Go, and more.

There are also various improvements and extensions to the core SQLite:

* [`ALTER TABLE` extension for modifying column types and constraints](https://github.com/libsql/libsql/blob/main/libsql-sqlite3/doc/libsql_extensions.md#altering-columns)
* [Randomized ROWID](https://github.com/libsql/libsql/issues/12)
* [WebAssembly User Defined Functions](https://blog.turso.tech/webassembly-functions-for-your-sqlite-compatible-database-7e1ad95a2aa7)
* [Pass down SQL string to virtual table implementation](https://github.com/libsql/libsql/pull/87)
* [Virtual write-ahead log interface](https://github.com/libsql/libsql/pull/53)

The comprehensive description can be found [here](libsql-sqlite3/doc/libsql_extensions.md)

## Getting Started

The project provides two interfaces: the libSQL API, which supports all the features, and the SQLite C API for compatibility.

To get started with the libSQL API:

* [JavaScript](https://github.com/libsql/libsql-experimental-node)
* [Rust](libsql) 
* [Python](https://github.com/libsql/libsql-experimental-python) (experimental)
* [Go](bindings/go) (experimental)
* [C](bindings/c) (experimantal)

To build the SQLite-compatible C library and tools, run:

```sh
cargo xtask build
```

To run the SQL shell, launch the `libsql` program:

```console
$ cd libsql-sqlite3 && ./libsql
libSQL version 0.2.1 (based on SQLite version 3.43.0) 2023-05-23 11:47:56
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
libsql>
```

## Why a fork?

SQLite has solidified its place in modern technology stacks, embedded in nearly any computing device you can think of. Its open source nature and public domain availability make it a popular choice for modification to meet specific use cases.

But despite having its code available, SQLite famously doesn't accept external contributors and doesn't adhere to a code of conduct. So community improvements cannot be widely enjoyed.

There have been other forks in the past, but they all focus on a specific technical difference. We aim to be a community where people can contribute from many different angles and motivations.

We want to see a world where everyone can benefit from all of the great ideas and hard work that the SQLite community contributes back to the codebase. Community contributions work well, because we’ve done it before. If this was possible, what do you think SQLite could become?

You can read more about our goals an motivation in our [product vision](https://turso.tech/libsql-manifesto) and our [announcement article](https://glaubercosta-11125.medium.com/sqlite-qemu-all-over-again-aedad19c9a1c)

## Compatibility with SQLite

Compatibility with SQLite is of great importance for us. But it can mean many things. So here's our stance:

* **The file format**: libSQL will always be able to ingest and write the SQLite file format. We would love to add extensions like encryption, and CRC that require the file to be changed. But we commit to always doing so in a way that generates standard sqlite files if those features are not used.
* **The API**: libSQL will keep 100% compatibility with the SQLite API, but we may add additional APIs.
* **Embedded**: SQLite is an embedded database that can be consumed as a single .c file with its accompanying header. libSQL will always be embeddable, meaning it runs inside your process without needing a network connection. But we may change the distribution, so that object files are generated, instead of a single .c file.
