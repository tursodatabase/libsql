# Rusqlite

[![Build Status](https://api.travis-ci.org/jgallagher/rusqlite.svg?branch=master)](https://travis-ci.org/jgallagher/rusqlite)

Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to expose
an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres). View the full
[API documentation](http://www.rust-ci.org/jgallagher/rusqlite/doc/rusqlite/).

```rust
extern crate rusqlite;
extern crate time;

use time::Timespec;
use rusqlite::SqliteConnection;

#[deriving(Show)]
struct Person {
    id: i32,
    name: String,
    time_created: Timespec,
    data: Option<Vec<u8>>
}

fn main() {
    let conn = SqliteConnection::open(":memory:").unwrap();

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
    for row in stmt.query(&[]).unwrap().map(|row| row.unwrap()) {
        let person = Person {
            id: row.get(0),
            name: row.get(1),
            time_created: row.get(2),
            data: row.get(3)
        };
        println!("Found person {}", person);
    }
}
```

### Design of SqliteRows and SqliteRow

To retrieve the result rows from a query, SQLite requires you to call
[sqlite3_step()](https://www.sqlite.org/c3ref/step.html) on a prepared statement. You can only
retrieve the values of the "current" row. From the Rust point of view, this means that each row
is only valid until the next row is fetched.  [rust-sqlite3](https://github.com/dckc/rust-sqlite3)
solves this the correct way with lifetimes.  However, this means that the result rows do not
satisfy the [Iterator](http://doc.rust-lang.org/std/iter/trait.Iterator.html) trait, which means
you cannot (as easily) loop over the rows, or use many of the helpful Iterator methods like `map`
and `filter`.

Instead, Rusqlite's `SqliteRows` handle does conform to `Iterator`. It ensures safety by
performing checks at runtime to ensure you do not try to retrieve the values of a "stale" row, and
will panic if you do so. A specific example that will panic:

```rust
fn bad_function_will_panic(conn: &SqliteConnection) -> SqliteResult<i64> {
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
`collect()` on a `SqliteRows` and then trying to use the collected rows.

## Author

John Gallagher, johnkgallagher@gmail.com

## License

Rusqlite is available under the MIT license. See the LICENSE file for more info.
