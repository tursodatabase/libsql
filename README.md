<p align="center">
  <a href="https://turso.tech/libsql">
    <img alt="libSQL by Turso" src="https://github.com/tursodatabase/libsql/assets/950181/6c8679e7-65a9-4777-b08a-2ddf4321160f" width="1000">
    <h1 align="center">libSQL</h1>
  </a>
</p>

<p align="center">
  <a href="https://turso.tech/libsql">libSQL</a> is an open source, open contribution fork of SQLite, created and maintained by <a href="https://turso.tech">Turso</a>.
</p>

<p align="center">
  <a href="/docs"><strong>libSQL Docs</strong></a> ·
  <a href="https://turso.tech/libsql-manifesto"><strong>libSQL Manifesto</strong></a> ·
  <a href="https://turso.tech"><strong>Turso</strong></a> ·
  <a href="https://docs.turso.tech"><strong>Turso Docs</strong></a> ·
  <a href="https://discord.gg/turso"><strong>Discord</strong></a> ·
  <a href="https://turso.tech/blog"><strong>Blog &amp; Tutorials</strong></a>
</p>

<p align="center">
  <a href="https://github.com/tursodatabase/libsql/blob/main/LICENSE.md">
    <img src="https://img.shields.io/badge/license-MIT-blue" alt="MIT" title="MIT License" />
  </a>
  <a href="https://discord.gg/turso">
    <img src="https://dcbadge.vercel.app/api/server/4B5D7hYwub?style=flat" alt="discord activity" title="join us on discord" />
  </a>
   <a href="https://www.phorm.ai/query?projectId=3c9a471f-4a47-469f-81f6-4ea1ff9ab418"><img src="https://img.shields.io/badge/Phorm-Ask_AI-%23F2777A.svg?&logo=data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNSIgaGVpZ2h0PSI0IiBmaWxsPSJub25lIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogIDxwYXRoIGQ9Ik00LjQzIDEuODgyYTEuNDQgMS40NCAwIDAgMS0uMDk4LjQyNmMtLjA1LjEyMy0uMTE1LjIzLS4xOTIuMzIyLS4wNzUuMDktLjE2LjE2NS0uMjU1LjIyNmExLjM1MyAxLjM1MyAwIDAgMS0uNTk1LjIxMmMtLjA5OS4wMTItLjE5Mi4wMTQtLjI3OS4wMDZsLTEuNTkzLS4xNHYtLjQwNmgxLjY1OGMuMDkuMDAxLjE3LS4xNjkuMjQ2LS4xOTFhLjYwMy42MDMgMCAwIDAgLjItLjEwNi41MjkuNTI5IDAgMCAwIC4xMzgtLjE3LjY1NC42NTQgMCAwIDAgLjA2NS0uMjRsLjAyOC0uMzJhLjkzLjkzIDAgMCAwLS4wMzYtLjI0OS41NjcuNTY3IDAgMCAwLS4xMDMtLjIuNTAyLjUwMiAwIDAgMC0uMTY4LS4xMzguNjA4LjYwOCAwIDAgMC0uMjQtLjA2N0wyLjQzNy43MjkgMS42MjUuNjcxYS4zMjIuMzIyIDAgMCAwLS4yMzIuMDU4LjM3NS4zNzUgMCAwIDAtLjExNi4yMzJsLS4xMTYgMS40NS0uMDU4LjY5Ny0uMDU4Ljc1NEwuNzA1IDRsLS4zNTctLjA3OUwuNjAyLjkwNkMuNjE3LjcyNi42NjMuNTc0LjczOS40NTRhLjk1OC45NTggMCAwIDEgLjI3NC0uMjg1Ljk3MS45NzEgMCAwIDEgLjMzNy0uMTRjLjExOS0uMDI2LjIyNy0uMDM0LjMyNS0uMDI2TDMuMjMyLjE2Yy4xNTkuMDE0LjMzNi4wMy40NTkuMDgyYTEuMTczIDEuMTczIDAgMCAxIC41NDUuNDQ3Yy4wNi4wOTQuMTA5LjE5Mi4xNDQuMjkzYTEuMzkyIDEuMzkyIDAgMCAxIC4wNzguNThsLS4wMjkuMzJaIiBmaWxsPSIjRjI3NzdBIi8+CiAgPHBhdGggZD0iTTQuMDgyIDIuMDA3YTEuNDU1IDEuNDU1IDAgMCAxLS4wOTguNDI3Yy0uMDUuMTI0LS4xMTQuMjMyLS4xOTIuMzI0YTEuMTMgMS4xMyAwIDAgMS0uMjU0LjIyNyAxLjM1MyAxLjM1MyAwIDAgMS0uNTk1LjIxNGMtLjEuMDEyLS4xOTMuMDE0LS4yOC4wMDZsLTEuNTYtLjEwOC4wMzQtLjQwNi4wMy0uMzQ4IDEuNTU5LjE1NGMuMDkgMCAuMTczLS4wMS4yNDgtLjAzM2EuNjAzLjYwMyAwIDAgMCAuMi0uMTA2LjUzMi41MzIgMCAwIDAgLjEzOS0uMTcyLjY2LjY2IDAgMCAwIC4wNjQtLjI0MWwuMDI5LS4zMjFhLjk0Ljk0IDAgMCAwLS4wMzYtLjI1LjU3LjU3IDAgMCAwLS4xMDMtLjIwMi41MDIuNTAyIDAgMCAwLS4xNjgtLjEzOC42MDUuNjA1IDAgMCAwLS4yNC0uMDY3TDEuMjczLjgyN2MtLjA5NC0uMDA4LS4xNjguMDEtLjIyMS4wNTUtLjA1My4wNDUtLjA4NC4xMTQtLjA5Mi4yMDZMLjcwNSA0IDAgMy45MzhsLjI1NS0yLjkxMUExLjAxIDEuMDEgMCAwIDEgLjM5My41NzIuOTYyLjk2MiAwIDAgMSAuNjY2LjI4NmEuOTcuOTcgMCAwIDEgLjMzOC0uMTRDMS4xMjIuMTIgMS4yMy4xMSAxLjMyOC4xMTlsMS41OTMuMTRjLjE2LjAxNC4zLjA0Ny40MjMuMWExLjE3IDEuMTcgMCAwIDEgLjU0NS40NDhjLjA2MS4wOTUuMTA5LjE5My4xNDQuMjk1YTEuNDA2IDEuNDA2IDAgMCAxIC4wNzcuNTgzbC0uMDI4LjMyMloiIGZpbGw9IndoaXRlIi8+CiAgPHBhdGggZD0iTTQuMDgyIDIuMDA3YTEuNDU1IDEuNDU1IDAgMCAxLS4wOTguNDI3Yy0uMDUuMTI0LS4xMTQuMjMyLS4xOTIuMzI0YTEuMTMgMS4xMyAwIDAgMS0uMjU0LjIyNyAxLjM1MyAxLjM1MyAwIDAgMS0uNTk1LjIxNGMtLjEuMDEyLS4xOTMuMDE0LS4yOC4wMDZsLTEuNTYtLjEwOC4wMzQtLjQwNi4wMy0uMzQ4IDEuNTU5LjE1NGMuMDkgMCAuMTczLS4wMS4yNDgtLjAzM2EuNjAzLjYwMyAwIDAgMCAuMi0uMTA2LjUzMi41MzIgMCAwIDAgLjEzOS0uMTcyLjY2LjY2IDAgMCAwIC4wNjQtLjI0MWwuMDI5LS4zMjFhLjk0Ljk0IDAgMCAwLS4wMzYtLjI1LjU3LjU3IDAgMCAwLS4xMDMtLjIwMi41MDIuNTAyIDAgMCAwLS4xNjgtLjEzOC42MDUuNjA1IDAgMCAwLS4yNC0uMDY3TDEuMjczLjgyN2MtLjA5NC0uMDA4LS4xNjguMDEtLjIyMS4wNTUtLjA1My4wNDUtLjA4NC4xMTQtLjA5Mi4yMDZMLjcwNSA0IDAgMy45MzhsLjI1NS0yLjkxMUExLjAxIDEuMDEgMCAwIDEgLjM5My41NzIuOTYyLjk2MiAwIDAgMSAuNjY2LjI4NmEuOTcuOTcgMCAwIDEgLjMzOC0uMTRDMS4xMjIuMTIgMS4yMy4xMSAxLjMyOC4xMTlsMS41OTMuMTRjLjE2LjAxNC4zLjA0Ny40MjMuMWExLjE3IDEuMTcgMCAwIDEgLjU0NS40NDhjLjA2MS4wOTUuMTA5LjE5My4xNDQuMjk1YTEuNDA2IDEuNDA2IDAgMCAxIC4wNzcuNTgzbC0uMDI4LjMyMloiIGZpbGw9IndoaXRlIi8+Cjwvc3ZnPgo=" alt="phorm.ai">
  </a>
</p>

---

## Documentation

We aim to evolve it to suit many more use cases than SQLite was originally designed for, and plan to use third-party OSS code wherever it makes sense. 

libSQL has many great features, including:

* Embedded replicas that allow you to have replicated database inside your app.
* [libSQL server](libsql-server) for remote SQLite access, similar to PostgreSQL or MySQL
* Supports Rust, JavaScript, Python, Go, and more.

There are also various improvements and extensions to the core SQLite:

* [`ALTER TABLE` extension for modifying column types and constraints](https://github.com/tursodatabase/libsql/blob/main/libsql-sqlite3/doc/libsql_extensions.md#altering-columns)
* [Randomized ROWID](https://github.com/tursodatabase/libsql/issues/12)
* [WebAssembly User Defined Functions](https://turso.tech/blog/webassembly-functions-for-your-sqlite-compatible-database-7e1ad95a2aa7)
* [Pass down SQL string to virtual table implementation](https://github.com/tursodatabase/libsql/pull/87)
* [Virtual write-ahead log interface](https://github.com/tursodatabase/libsql/pull/53)

The comprehensive description can be found [here](libsql-sqlite3/doc/libsql_extensions.md)

### Official Drivers

* [TypeScript / JS](https://github.com/tursodatabase/libsql-client-ts)
* [Rust](libsql) 
* [Go](https://github.com/tursodatabase/go-libsql)
* [Go (no CGO)](https://github.com/tursodatabase/libsql-client-go)

### Experimental Drivers
* [Python](https://github.com/tursodatabase/libsql-experimental-python) (experimental)
* [C](bindings/c) (experimental)

### Community Drivers
* [PHP](https://github.com/tursodatabase/turso-client-php)
* [D](https://github.com/pdenapo/libsql-d) (experimental, based on the C driver)

### GUI Support
* [Beekeeper Studio](https://www.beekeeperstudio.io/db/libsql-client/) &mdash; macOS, Windows, and Linux
* [Outerbase](https://www.outerbase.com) &mdash; Runs in the browser
* [TablePlus](https://tableplus.com) &mdash; macOS, Windows, and Linux
* [Dataflare](https://dataflare.app) &mdash; Paid (with limited free version) macOS, Windows, and Linux
* [libSQL Studio](https://github.com/invisal/libsql-studio) - Runs in the browser

## Getting Started

The project provides two interfaces: the libSQL API, which supports all the features, and the SQLite C API for compatibility.

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

### Docker
To run libSQL using docker, refer to the [Docker Docs](docs/DOCKER.md)

## Why a fork?

SQLite has solidified its place in modern technology stacks, embedded in nearly any computing device you can think of. Its open source nature and public domain availability make it a popular choice for modification to meet specific use cases.

But despite having its code available, SQLite famously doesn't accept external contributors and doesn't adhere to a code of conduct. So community improvements cannot be widely enjoyed.

There have been other forks in the past, but they all focus on a specific technical difference. We aim to be a community where people can contribute from many different angles and motivations.

We want to see a world where everyone can benefit from all of the great ideas and hard work that the SQLite community contributes back to the codebase. Community contributions work well, because we’ve done it before. If this was possible, what do you think SQLite could become?

You can read more about our goals and motivation in our [product vision](https://turso.tech/libsql-manifesto).

## Compatibility with SQLite

Compatibility with SQLite is of great importance for us. But it can mean many things. So here's our stance:

* **The file format**: libSQL will always be able to ingest and write the SQLite file format. We would love to add extensions like encryption, and CRC that require the file to be changed. But we commit to always doing so in a way that generates standard sqlite files if those features are not used.
* **The API**: libSQL will keep 100% compatibility with the SQLite API, but we may add additional APIs.
* **Embedded**: SQLite is an embedded database that can be consumed as a single .c file with its accompanying header. libSQL will always be embeddable, meaning it runs inside your process without needing a network connection. But we may change the distribution, so that object files are generated, instead of a single .c file.

## License

libSQL is licensed under an [Open Source License](LICENSE.md), and we adhere to a clear [Code of Conduct](CODE_OF_CONDUCT.md).
