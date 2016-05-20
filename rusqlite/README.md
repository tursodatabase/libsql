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
* `chrono` implements [`FromSql`](http://jgallagher.github.io/rusqlite/rusqlite/types/trait.FromSql.html)
  and [`ToSql`](http://jgallagher.github.io/rusqlite/rusqlite/types/trait.ToSql.html) for various
  types from the [`chrono` crate](https://crates.io/crates/chrono).
* `serde_json` implements [`FromSql`](http://jgallagher.github.io/rusqlite/rusqlite/types/trait.FromSql.html)
  and [`ToSql`](http://jgallagher.github.io/rusqlite/rusqlite/types/trait.ToSql.html) for the
  `Value` type from the [`serde_json` crate](https://crates.io/crates/serde_json).

## Author

John Gallagher, johnkgallagher@gmail.com

## License

Rusqlite is available under the MIT license. See the LICENSE file for more info.
