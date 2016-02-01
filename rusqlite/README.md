# Rusqlite

[![Travis Build Status](https://api.travis-ci.org/jgallagher/rusqlite.svg?branch=master)](https://travis-ci.org/jgallagher/rusqlite)
[![AppVeyor Build Status](https://ci.appveyor.com/api/projects/status/github/jgallagher/rusqlite?branch=master&svg=true)](https://ci.appveyor.com/project/jgallagher/rusqlite)

Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to expose
an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres). View the full
[API documentation](http://jgallagher.github.io/rusqlite/rusqlite/index.html).

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
                  VALUES ($1, $2, $3)",
                 &[&me.name, &me.time_created, &me.data]).unwrap();

    let mut stmt = conn.prepare("SELECT id, name, time_created, data FROM person").unwrap();
    let mut person_iter = stmt.query_map(&[], |row| {
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

### Optional Features

Rusqlite provides several features that are behind [Cargo
features](http://doc.crates.io/manifest.html#the-features-section). They are:

* [`load_extension`](http://jgallagher.github.io/rusqlite/rusqlite/struct.LoadExtensionGuard.html)
  allows loading dynamic library-based SQLite extensions.
* [`backup`](http://jgallagher.github.io/rusqlite/rusqlite/backup/index.html)
  allows use of SQLite's online backup API.
* [`functions`](http://jgallagher.github.io/rusqlite/rusqlite/functions/index.html)
  allows you to load Rust closures into SQLite connections for use in queries.
* [`trace`](http://jgallagher.github.io/rusqlite/rusqlite/trace/index.html)
  allows hooks into SQLite's tracing and profiling APIs.
* [`blob`](http://jgallagher.github.io/rusqlite/rusqlite/blob/index.html)
  gives `std::io::{Read, Write, Seek}` access to SQL BLOBs.

### Design of Rows and Row

To retrieve the result rows from a query, SQLite requires you to call
[sqlite3_step()](https://www.sqlite.org/c3ref/step.html) on a prepared statement. You can only
retrieve the values of the "current" row. From the Rust point of view, this means that each row
is only valid until the next row is fetched.  [rust-sqlite3](https://github.com/dckc/rust-sqlite3)
solves this the correct way with lifetimes.  However, this means that the result rows do not
satisfy the [Iterator](http://doc.rust-lang.org/std/iter/trait.Iterator.html) trait, which means
you cannot (as easily) loop over the rows, or use many of the helpful Iterator methods like `map`
and `filter`.

Instead, Rusqlite's `Rows` handle does conform to `Iterator`. It ensures safety by
performing checks at runtime to ensure you do not try to retrieve the values of a "stale" row, and
will panic if you do so. A specific example that will panic:

```rust
fn bad_function_will_panic(conn: &Connection) -> Result<i64> {
    let mut stmt = try!(conn.prepare("SELECT id FROM my_table"));
    let mut rows = try!(stmt.query(&[]));

    let row0 = try!(rows.next().unwrap());
    // row 0 is valid now...

    let row1 = try!(rows.next().unwrap());
    // row 0 is now STALE, and row 1 is valid

    let my_id = row0.get(0); // WILL PANIC because row 0 is stale
    Ok(my_id)
}
```

There are other, less obvious things that may result in a panic as well, such as calling
`collect()` on a `Rows` and then trying to use the collected rows.

Strongly consider using the method `query_map()` instead, if you can.
`query_map()` returns an iterator over rows-mapped-to-some-type. This
iterator does not have any of the above issues with panics due to attempting to
access stale rows.

## Author

John Gallagher, johnkgallagher@gmail.com

## License

Rusqlite is available under the MIT license. See the LICENSE file for more info.
