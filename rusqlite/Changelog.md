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
