# Rusqlite

[![Travis Build Status](https://api.travis-ci.org/jgallagher/rusqlite.svg?branch=master)](https://travis-ci.org/jgallagher/rusqlite)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/jgallagher/rusqlite?branch=master&svg=true)](https://ci.appveyor.com/project/jgallagher/rusqlite)
[![dependency status](https://deps.rs/repo/github/jgallagher/rusqlite/status.svg)](https://deps.rs/repo/github/jgallagher/rusqlite)
[![Latest Version](https://img.shields.io/crates/v/rusqlite.svg)](https://crates.io/crates/rusqlite)
[![Docs](https://docs.rs/rusqlite/badge.svg)](https://docs.rs/rusqlite)

Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to expose
an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres).

```rust
extern crate rusqlite;
extern crate time;

use time::Timespec;
use rusqlite::Connection;

#[derive(Debug)]
struct Person {
    id: i32,
    name: String,
    time_created: Timespec,
    data: Option<Vec<u8>>
}

fn main() {
    let conn = Connection::open_in_memory().unwrap();

    conn.execute("CREATE TABLE person (
                  id              INTEGER PRIMARY KEY,
                  name            TEXT NOT NULL,
                  time_created    TEXT NOT NULL,
                  data            BLOB
                  )", &[]).unwrap();
    let me = Person {
        id: 0,
        name: "Steven".to_string(),
        time_created: time::get_time(),
        data: None
    };
    conn.execute("INSERT INTO person (name, time_created, data)
                  VALUES (?1, ?2, ?3)",
                 &[&me.name, &me.time_created, &me.data]).unwrap();

    let mut stmt = conn.prepare("SELECT id, name, time_created, data FROM person").unwrap();
    let person_iter = stmt.query_map(&[], |row| {
        Person {
            id: row.get(0),
            name: row.get(1),
            time_created: row.get(2),
            data: row.get(3)
        }
    }).unwrap();

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
}
```

### Supported SQLite Versions

The base `rusqlite` package supports SQLite version 3.6.8 or newer. If you need
support for older versions, please file an issue. Some cargo features require a
newer SQLite version; see details below.

### Optional Features

Rusqlite provides several features that are behind [Cargo
features](https://doc.rust-lang.org/cargo/reference/manifest.html#the-features-section). They are:

* [`load_extension`](https://docs.rs/rusqlite/0.13.0/rusqlite/struct.LoadExtensionGuard.html)
  allows loading dynamic library-based SQLite extensions.
* [`backup`](https://docs.rs/rusqlite/0.13.0/rusqlite/backup/index.html)
  allows use of SQLite's online backup API. Note: This feature requires SQLite 3.6.11 or later.
* [`functions`](https://docs.rs/rusqlite/0.13.0/rusqlite/functions/index.html)
  allows you to load Rust closures into SQLite connections for use in queries.
  Note: This feature requires SQLite 3.7.3 or later.
* [`trace`](https://docs.rs/rusqlite/0.13.0/rusqlite/trace/index.html)
  allows hooks into SQLite's tracing and profiling APIs. Note: This feature
  requires SQLite 3.6.23 or later.
* [`blob`](https://docs.rs/rusqlite/0.13.0/rusqlite/blob/index.html)
  gives `std::io::{Read, Write, Seek}` access to SQL BLOBs. Note: This feature
  requires SQLite 3.7.4 or later.
* [`limits`](https://docs.rs/rusqlite/0.13.0/rusqlite/struct.Connection.html#method.limit)
  allows you to set and retrieve SQLite's per connection limits.
* `chrono` implements [`FromSql`](https://docs.rs/rusqlite/0.13.0/rusqlite/types/trait.FromSql.html)
  and [`ToSql`](https://docs.rs/rusqlite/0.13.0/rusqlite/types/trait.ToSql.html) for various
  types from the [`chrono` crate](https://crates.io/crates/chrono).
* `serde_json` implements [`FromSql`](https://docs.rs/rusqlite/0.13.0/rusqlite/types/trait.FromSql.html)
  and [`ToSql`](https://docs.rs/rusqlite/0.13.0/rusqlite/types/trait.ToSql.html) for the
  `Value` type from the [`serde_json` crate](https://crates.io/crates/serde_json).
* `bundled` uses a bundled version of sqlite3.  This is a good option for cases where linking to sqlite3 is complicated, such as Windows.
* `sqlcipher` looks for the SQLCipher library to link against instead of SQLite. This feature is mutually exclusive with `bundled`.
* `hooks` for [Commit, Rollback](http://sqlite.org/c3ref/commit_hook.html) and [Data Change](http://sqlite.org/c3ref/update_hook.html) notification callbacks.
* `unlock_notify` for [Unlock](https://sqlite.org/unlock_notify.html) notification.
* `vtab` for [virtual table](https://sqlite.org/vtab.html) support (allows you to write virtual table implemntations in Rust). Currently, only read-only virtual tables are supported.
* [`csvtab`](https://sqlite.org/csv.html), CSV virtual table written in Rust.
* [`array`](https://sqlite.org/carray.html), The `rarray()` Table-Valued Function.

## Notes on building rusqlite and libsqlite3-sys

`libsqlite3-sys` is a separate crate from `rusqlite` that provides the Rust
declarations for SQLite's C API. By default, `libsqlite3-sys` attempts to find a SQLite library that already exists on your system using pkg-config, or a
[Vcpkg](https://github.com/Microsoft/vcpkg) installation for MSVC ABI builds. 

You can adjust this behavior in a number of ways:

* If you use the `bundled` feature, `libsqlite3-sys` will use the
  [gcc](https://crates.io/crates/gcc) crate to compile SQLite from source and
  link against that. This source is embedded in the `libsqlite3-sys` crate and
  is currently SQLite 3.24.0 (as of `rusqlite` 0.14.0 / `libsqlite3-sys`
  0.9.3).  This is probably the simplest solution to any build problems. You can enable this by adding the following in your `Cargo.toml` file:
  ```
  [dependencies.rusqlite]
  version = "0.14.0"
  features = ["bundled"]
  ```
* You can set the `SQLITE3_LIB_DIR` to point to directory containing the SQLite
  library.
* Installing the sqlite3 development packages will usually be all that is required, but
  the build helpers for [pkg-config](https://github.com/alexcrichton/pkg-config-rs)
  and [vcpkg](https://github.com/mcgoo/vcpkg-rs) have some additional configuration
  options. The default when using vcpkg is to dynamically link. `vcpkg install sqlite3:x64-windows` will install the required library.

### Binding generation

We use [bindgen](https://crates.io/crates/bindgen) to generate the Rust
declarations from SQLite's C header file. `bindgen`
[recommends](https://github.com/servo/rust-bindgen#library-usage-with-buildrs)
running this as part of the build process of libraries that used this. We tried
this briefly (`rusqlite` 0.10.0, specifically), but it had some annoyances:

* The build time for `libsqlite3-sys` (and therefore `rusqlite`) increased
  dramatically.
* Running `bindgen` requires a relatively-recent version of Clang, which many
  systems do not have installed by default.
* Running `bindgen` also requires the SQLite header file to be present.

As of `rusqlite` 0.10.1, we avoid running `bindgen` at build-time by shipping
pregenerated bindings for several versions of SQLite. When compiling
`rusqlite`, we use your selected Cargo features to pick the bindings for the
minimum SQLite version that supports your chosen features. If you are using
`libsqlite3-sys` directly, you can use the same features to choose which
pregenerated bindings are chosen:

* `min_sqlite_version_3_6_8` - SQLite 3.6.8 bindings (this is the default)
* `min_sqlite_version_3_6_11` - SQLite 3.6.11 bindings
* `min_sqlite_version_3_6_23` - SQLite 3.6.23 bindings
* `min_sqlite_version_3_7_3` - SQLite 3.7.3 bindings
* `min_sqlite_version_3_7_4` - SQLite 3.7.4 bindings

If you use the `bundled` feature, you will get pregenerated bindings for the
bundled version of SQLite. If you need other specific pregenerated binding
versions, please file an issue. If you want to run `bindgen` at buildtime to
produce your own bindings, use the `buildtime_bindgen` Cargo feature.

## Author

John Gallagher, johnkgallagher@gmail.com

## License

Rusqlite is available under the MIT license. See the LICENSE file for more info.
