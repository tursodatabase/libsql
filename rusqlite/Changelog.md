# Version 0.6.0 (2015-12-17)

* BREAKING CHANGE: `SqliteError` is now an enum instead of a struct. Previously, we were (ab)using
  the error code and message to send back both underlying SQLite errors and errors that occurred
  at the Rust level. Now those have been separated out; SQLite errors are returned as 
  `SqliteFailure` cases (which still include the error code but also include a Rust-friendlier
  enum as well), and rusqlite-level errors are captured in other cases. Because of this change,
  `SqliteError` no longer implements `PartialEq`.
* BREAKING CHANGE: When opening a new detection, rusqlite now detects if SQLite was compiled or
  configured for single-threaded use only; if it was, connection attempts will fail. If this
  affects you, please open an issue.
* BREAKING CHANGE: `SqliteTransactionDeferred`, `SqliteTransactionImmediate`, and
  `SqliteTransactionExclusive` are no longer exported. Instead, use
  `TransactionBehavior::Deferred`, `TransactionBehavior::Immediate`, and
  `TransactionBehavior::Exclusive`.
* Removed `Sqlite` prefix on many types:
    * `SqliteConnection` is now `Connection`
    * `SqliteError` is now `Error`
    * `SqliteResult` is now `Result`
    * `SqliteStatement` is now `Statement`
    * `SqliteRows` is now `Rows`
    * `SqliteRow` is now `Row`
    * `SqliteOpenFlags` is now `OpenFlags`
    * `SqliteTransaction` is now `Transaction`.
    * `SqliteTransactionBehavior` is now `TransactionBehavior`.
    * `SqliteLoadExtensionGuard` is now `LoadExtensionGuard`.
  The old, prefixed names are still exported but are deprecated.
* Adds a variety of `..._named` methods for executing queries using named placeholder parameters.
* Adds `backup` feature that exposes SQLite's online backup API.
* Adds `blob` feature that exposes SQLite's Incremental I/O for BLOB API.
* Adds `functions` feature that allows user-defined scalar functions to be added to
  open `SqliteConnection`s.

# Version 0.5.0 (2015-12-08)

* Adds `trace` feature that allows the use of SQLite's logging, tracing, and profiling hooks.
* Slight change to the closure types passed to `query_map` and `query_and_then`:
    * Remove the `'static` requirement on the closure's output type.
    * Give the closure a `&SqliteRow` instead of a `SqliteRow`.
* When building, the environment variable `SQLITE3_LIB_DIR` now takes precedence over pkg-config.
* If `pkg-config` is not available, we will try to find `libsqlite3` in `/usr/lib`.
* Add more documentation for failure modes of functions that return `SqliteResult`s.
* Updates `libc` dependency to 0.2, fixing builds on ARM for Rust 1.6 or newer.

# Version 0.4.0 (2015-11-03)

* Adds `Sized` bound to `FromSql` trait as required by RFC 1214.

# Version 0.3.1 (2015-09-22)

* Reset underlying SQLite statements as soon as possible after executing, as recommended by
  http://www.sqlite.org/cvstrac/wiki?p=ScrollingCursor.

# Version 0.3.0 (2015-09-21)

* Removes `get_opt`. Use `get_checked` instead.
* Add `query_row_and_then` and `query_and_then` convenience functions. These are analogous to
  `query_row` and `query_map` but allow functions that can fail by returning `Result`s.
* Relax uses of `P: AsRef<...>` from `&P` to `P`.
* Add additional error check for calling `execute` when `query` was intended.
* Improve debug formatting of `SqliteStatement` and `SqliteConnection`.
* Changes documentation of `get_checked` to correctly indicate that it returns errors (not panics)
  when given invalid types or column indices.

# Version 0.2.0 (2015-07-26)

* Add `column_names()` to `SqliteStatement`.
* By default, include `SQLITE_OPEN_NO_MUTEX` and `SQLITE_OPEN_URI` flags when opening a
  new conneciton.
* Fix generated bindings (e.g., `sqlite3_exec` was wrong).
* Use now-generated `sqlite3_destructor_type` to define `SQLITE_STATIC` and `SQLITE_TRANSIENT`.

# Version 0.1.0 (2015-05-11)

* [breaking-change] Modify `query_row` to return a `Result` instead of unwrapping.
* Deprecate `query_row_safe` (use `query_row` instead).
* Add `query_map`.
* Add `get_checked`, which asks SQLite to do some basic type-checking of columns.

# Version 0.0.17 (2015-04-03)

* Publish version that builds on stable rust (beta). This version lives on the
  `stable` branch. Development continues on `master` and still requires a nightly
  version of Rust.

# Version 0.0.16

* Updates to track rustc nightly.

# Version 0.0.15

* Make SqliteConnection `Send`.

# Version 0.0.14

* Remove unneeded features (also involves switching to `libc` crate).

# Version 0.0.13 (2015-03-26)

* Updates to track rustc nightly.

# Version 0.0.12 (2015-03-24)

* Updates to track rustc stabilization.

# Version 0.0.11 (2015-03-12)

* Reexport `sqlite3_stmt` from `libsqlite3-sys` for easier `impl`-ing of `ToSql` and `FromSql`.
* Updates to track latest rustc changes.
* Update dependency versions.

# Version 0.0.10 (2015-02-23)

* BREAKING CHANGE: `open` now expects a `Path` rather than a `str`. There is a separate
  `open_in_memory` constructor for opening in-memory databases.
* Added the ability to load SQLite extensions. This is behind the `load_extension` Cargo feature,
  because not all builds of sqlite3 include this ability. Notably the default libsqlite3 that
	ships with OS X 10.10 does not support extensions.

# Version 0.0.9 (2015-02-13)

* Updates to track latest rustc changes.
* Implement standard `Error` trait for `SqliteError`.

# Version 0.0.8 (2015-02-04)

* Updates to track latest rustc changes.

# Version 0.0.7 (2015-01-20)

* Use external bitflags from crates.io.

# Version 0.0.6 (2015-01-10)

* Updates to track latest rustc changes (1.0.0-alpha).
* Add `query_row_safe`, a `SqliteResult`-returning variant of `query_row`.

# Version 0.0.5 (2015-01-07)

* Updates to track latest rustc changes (closure syntax).
* Updates to track latest rust stdlib changes (`std::c_str` -> `std::ffi`).

# Version 0.0.4 (2015-01-05)

* Updates to track latest rustc changes.

# Version 0.0.3 (2014-12-23)

* Updates to track latest rustc changes.
* Add call to `sqlite3_busy_timeout`.

# Version 0.0.2 (2014-12-04)

* Remove use of now-deprecated `std::vec::raw::from_buf`.
* Update to latest version of `time` crate.

# Version 0.0.1 (2014-11-21)

* Initial release
