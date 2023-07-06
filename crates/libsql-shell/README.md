# libSQL shell

This project contains [libSQL](https://libsql.org)'s new shell,
implemented in Rust on top of a few industry standard crates: `rusqlite`, `rustyline`, `clap`, `tracing`, etc.

The long-term goal of this project is to:
 - Match all features of the original libSQL shell (inherited from SQLite and implemented in C),
 - Add new features on top, for instance:
   - importing and exporting additional formats (Parquet and friends);
   - accessing network resources.
 - Make contributions to libSQL as easy as possible.

## Status
This project is still in early development phase, so expect missing items!

## Example
```
$ ./libsql
libSQL version 0.2.0
Connected to a transient in-memory database.

libsql> create table test(id, v);
libsql> insert into test values(42, zeroblob(12));
libsql> insert into test values(3.14, 'hello');
libsql> insert into test values(null, null);
libsql> select id, v, length(v), hex(v) from test;
 id   | v                  | length(v) | hex(v)                   
------+--------------------+-----------+--------------------------
 42   | 0xAAAAAAAAAAAAAAAA | 12        | 000000000000000000000000 
 3.14 | hello              | 5         | 68656C6C6F               
 null | null               | null      |                          
libsql> 
```
