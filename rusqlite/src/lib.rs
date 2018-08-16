//! Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to
//! expose an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres).
//!
//! ```rust
//! extern crate rusqlite;
//! extern crate time;
//!
//! use rusqlite::Connection;
//! use time::Timespec;
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     time_created: Timespec,
//!     data: Option<Vec<u8>>,
//! }
//!
//! fn main() {
//!     let conn = Connection::open_in_memory().unwrap();
//!
//!     conn.execute(
//!         "CREATE TABLE person (
//!                   id              INTEGER PRIMARY KEY,
//!                   name            TEXT NOT NULL,
//!                   time_created    TEXT NOT NULL,
//!                   data            BLOB
//!                   )",
//!         &[],
//!     ).unwrap();
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         time_created: time::get_time(),
//!         data: None,
//!     };
//!     conn.execute(
//!         "INSERT INTO person (name, time_created, data)
//!                   VALUES (?1, ?2, ?3)",
//!         &[&me.name, &me.time_created, &me.data],
//!     ).unwrap();
//!
//!     let mut stmt = conn
//!         .prepare("SELECT id, name, time_created, data FROM person")
//!         .unwrap();
//!     let person_iter = stmt
//!         .query_map(&[], |row| Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             time_created: row.get(2),
//!             data: row.get(3),
//!         }).unwrap();
//!
//!     for person in person_iter {
//!         println!("Found person {:?}", person.unwrap());
//!     }
//! }
//! ```
#![allow(unknown_lints)]

extern crate libsqlite3_sys as ffi;
extern crate lru_cache;
#[macro_use]
extern crate bitflags;
#[cfg(any(test, feature = "vtab"))]
#[macro_use]
extern crate lazy_static;

use std::cell::RefCell;
use std::convert;
use std::default::Default;
use std::ffi::{CStr, CString};
use std::fmt;
use std::mem;
use std::os::raw::{c_char, c_int};

use std::path::{Path, PathBuf};
use std::ptr;
use std::result;
use std::str;
use std::sync::atomic::{AtomicBool, Ordering, ATOMIC_BOOL_INIT};
use std::sync::{Once, ONCE_INIT};

use cache::StatementCache;
use error::{error_from_handle, error_from_sqlite_code};
use raw_statement::RawStatement;
use types::{ToSql, ValueRef};

pub use statement::Statement;

pub use row::{AndThenRows, MappedRows, Row, RowIndex, Rows};

pub use transaction::{DropBehavior, Savepoint, Transaction, TransactionBehavior};
#[allow(deprecated)]
pub use transaction::{SqliteTransaction, SqliteTransactionBehavior};

pub use error::Error;
#[allow(deprecated)]
pub use error::SqliteError;
pub use ffi::ErrorCode;

pub use cache::CachedStatement;
pub use version::*;

#[cfg(feature = "hooks")]
pub use hooks::*;
#[cfg(feature = "load_extension")]
#[allow(deprecated)]
pub use load_extension_guard::{LoadExtensionGuard, SqliteLoadExtensionGuard};

#[cfg(feature = "backup")]
pub mod backup;
#[cfg(feature = "blob")]
pub mod blob;
mod busy;
mod cache;
#[cfg(any(feature = "functions", feature = "vtab"))]
mod context;
mod error;
#[cfg(feature = "functions")]
pub mod functions;
#[cfg(feature = "hooks")]
mod hooks;
#[cfg(feature = "limits")]
pub mod limits;
#[cfg(feature = "load_extension")]
mod load_extension_guard;
mod raw_statement;
mod row;
mod statement;
#[cfg(feature = "trace")]
pub mod trace;
mod transaction;
pub mod types;
mod unlock_notify;
mod version;
#[cfg(feature = "vtab")]
pub mod vtab;

// Number of cached prepared statements we'll hold on to.
const STATEMENT_CACHE_DEFAULT_CAPACITY: usize = 16;

/// Old name for `Result`. `SqliteResult` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Result instead")]
pub type SqliteResult<T> = Result<T>;

/// A typedef of the result returned by many methods.
pub type Result<T> = result::Result<T, Error>;

unsafe fn errmsg_to_string(errmsg: *const c_char) -> String {
    let c_slice = CStr::from_ptr(errmsg).to_bytes();
    String::from_utf8_lossy(c_slice).into_owned()
}

fn str_to_cstring(s: &str) -> Result<CString> {
    Ok(try!(CString::new(s)))
}

fn path_to_cstring(p: &Path) -> Result<CString> {
    let s = try!(p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned())));
    str_to_cstring(s)
}

/// Name for a database within a SQLite connection.
#[derive(Copy, Clone)]
pub enum DatabaseName<'a> {
    /// The main database.
    Main,

    /// The temporary database (e.g., any "CREATE TEMPORARY TABLE" tables).
    Temp,

    /// A database that has been attached via "ATTACH DATABASE ...".
    Attached(&'a str),
}

// Currently DatabaseName is only used by the backup and blob mods, so hide
// this (private) impl to avoid dead code warnings.
#[cfg(any(feature = "backup", feature = "blob"))]
impl<'a> DatabaseName<'a> {
    fn to_cstring(&self) -> Result<CString> {
        use self::DatabaseName::{Attached, Main, Temp};
        match *self {
            Main => str_to_cstring("main"),
            Temp => str_to_cstring("temp"),
            Attached(s) => str_to_cstring(s),
        }
    }
}

/// Old name for `Connection`. `SqliteConnection` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Connection instead")]
pub type SqliteConnection = Connection;

/// A connection to a SQLite database.
pub struct Connection {
    db: RefCell<InnerConnection>,
    cache: StatementCache,
    path: Option<PathBuf>,
}

unsafe impl Send for Connection {}

impl Drop for Connection {
    fn drop(&mut self) {
        self.flush_prepared_statement_cache();
    }
}

impl Connection {
    /// Open a new connection to a SQLite database.
    ///
    /// `Connection::open(path)` is equivalent to
    /// `Connection::open_with_flags(path,
    /// OpenFlags::SQLITE_OPEN_READ_WRITE |
    /// OpenFlags::SQLITE_OPEN_CREATE)`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        let flags = OpenFlags::default();
        Connection::open_with_flags(path, flags)
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub fn open_in_memory() -> Result<Connection> {
        let flags = OpenFlags::default();
        Connection::open_in_memory_with_flags(flags)
    }

    /// Open a new connection to a SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Connection> {
        let c_path = try!(path_to_cstring(path.as_ref()));
        InnerConnection::open_with_flags(&c_path, flags).map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: Some(path.as_ref().to_path_buf()),
        })
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub fn open_in_memory_with_flags(flags: OpenFlags) -> Result<Connection> {
        let c_memory = try!(str_to_cstring(":memory:"));
        InnerConnection::open_with_flags(&c_memory, flags).map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: None,
        })
    }

    /// Convenience method to run multiple SQL statements (that cannot take any
    /// parameters).
    ///
    /// Uses [sqlite3_exec](http://www.sqlite.org/c3ref/exec.html) under the hood.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn create_tables(conn: &Connection) -> Result<()> {
    ///     conn.execute_batch(
    ///         "BEGIN;
    ///                         CREATE TABLE foo(x INTEGER);
    ///                         CREATE TABLE bar(y TEXT);
    ///                         COMMIT;",
    ///     )
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.db.borrow_mut().execute_batch(sql)
    }

    /// Convenience method to prepare and execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?", &[&1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute(&self, sql: &str, params: &[&ToSql]) -> Result<usize> {
        self.prepare(sql).and_then(|mut stmt| stmt.execute(params))
    }

    /// Convenience method to prepare and execute a single SQL statement with
    /// named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert(conn: &Connection) -> Result<usize> {
    ///     conn.execute_named(
    ///         "INSERT INTO test (name) VALUES (:name)",
    ///         &[(":name", &"one")],
    ///     )
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute_named(&self, sql: &str, params: &[(&str, &ToSql)]) -> Result<usize> {
        self.prepare(sql)
            .and_then(|mut stmt| stmt.execute_named(params))
    }

    /// Get the SQLite rowid of the most recent successful INSERT.
    ///
    /// Uses [sqlite3_last_insert_rowid](https://www.sqlite.org/c3ref/last_insert_rowid.html) under
    /// the hood.
    pub fn last_insert_rowid(&self) -> i64 {
        self.db.borrow_mut().last_insert_rowid()
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         &[],
    ///         |row| row.get(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn query_row<T, F>(&self, sql: &str, params: &[&ToSql], f: F) -> Result<T>
    where
        F: FnOnce(&Row) -> T,
    {
        let mut stmt = try!(self.prepare(sql));
        stmt.query_row(params, f)
    }

    /// Convenience method to execute a query with named parameter(s) that is
    /// expected to return a single row.
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn query_row_named<T, F>(&self, sql: &str, params: &[(&str, &ToSql)], f: F) -> Result<T>
    where
        F: FnOnce(&Row) -> T,
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query_named(params));

        rows.get_expected_row().map(|r| f(&r))
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row, and execute a mapping via `f` on that returned row with
    /// the possibility of failure. The `Result` type of `f` must implement
    /// `std::convert::From<Error>`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_and_then(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         &[],
    ///         |row| row.get_checked(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn query_row_and_then<T, E, F>(
        &self,
        sql: &str,
        params: &[&ToSql],
        f: F,
    ) -> result::Result<T, E>
    where
        F: FnOnce(&Row) -> result::Result<T, E>,
        E: convert::From<Error>,
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query(params));

        rows.get_expected_row().map_err(E::from).and_then(|r| f(&r))
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_safe(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         &[],
    ///         |row| row.get(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// ## Deprecated
    ///
    /// This method should be considered deprecated. Use `query_row` instead,
    /// which now does exactly the same thing.
    #[deprecated(since = "0.1.0", note = "Use query_row instead")]
    pub fn query_row_safe<T, F>(&self, sql: &str, params: &[&ToSql], f: F) -> Result<T>
    where
        F: FnOnce(&Row) -> T,
    {
        self.query_row(sql, params, f)
    }

    /// Prepare a SQL statement for execution.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     let mut stmt = try!(conn.prepare("INSERT INTO People (name) VALUES (?)"));
    ///     try!(stmt.execute(&[&"Joe Smith"]));
    ///     try!(stmt.execute(&[&"Bob Jones"]));
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn prepare<'a>(&'a self, sql: &str) -> Result<Statement<'a>> {
        self.db.borrow_mut().prepare(self, sql)
    }

    /// Close the SQLite connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for
    /// `Connection` except that on failure, it returns an error and the
    /// connection itself (presumably so closing can be attempted again).
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn close(self) -> std::result::Result<(), (Connection, Error)> {
        self.flush_prepared_statement_cache();
        let r = self.db.borrow_mut().close();
        r.map_err(move |err| (self, err))
    }

    /// Enable loading of SQLite extensions. Strongly consider using
    /// `LoadExtensionGuard` instead of this function.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # use std::path::{Path};
    /// fn load_my_extension(conn: &Connection) -> Result<()> {
    ///     try!(conn.load_extension_enable());
    ///     try!(conn.load_extension(Path::new("my_sqlite_extension"), None));
    ///     conn.load_extension_disable()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    pub fn load_extension_enable(&self) -> Result<()> {
        self.db.borrow_mut().enable_load_extension(1)
    }

    /// Disable loading of SQLite extensions.
    ///
    /// See `load_extension_enable` for an example.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    pub fn load_extension_disable(&self) -> Result<()> {
        self.db.borrow_mut().enable_load_extension(0)
    }

    /// Load the SQLite extension at `dylib_path`. `dylib_path` is passed
    /// through to `sqlite3_load_extension`, which may attempt OS-specific
    /// modifications if the file cannot be loaded directly.
    ///
    /// If `entry_point` is `None`, SQLite will attempt to find the entry
    /// point. If it is not `None`, the entry point will be passed through
    /// to `sqlite3_load_extension`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, LoadExtensionGuard};
    /// # use std::path::{Path};
    /// fn load_my_extension(conn: &Connection) -> Result<()> {
    ///     let _guard = try!(LoadExtensionGuard::new(conn));
    ///
    ///     conn.load_extension("my_sqlite_extension", None)
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    pub fn load_extension<P: AsRef<Path>>(
        &self,
        dylib_path: P,
        entry_point: Option<&str>,
    ) -> Result<()> {
        self.db
            .borrow_mut()
            .load_extension(dylib_path.as_ref(), entry_point)
    }

    /// Get access to the underlying SQLite database connection handle.
    ///
    /// # Warning
    ///
    /// You should not need to use this function. If you do need to, please
    /// [open an issue on the rusqlite repository](https://github.com/jgallagher/rusqlite/issues) and describe
    /// your use case. This function is unsafe because it gives you raw access
    /// to the SQLite connection, and what you do with it could impact the
    /// safety of this `Connection`.
    pub unsafe fn handle(&self) -> *mut ffi::sqlite3 {
        self.db.borrow().db()
    }

    fn decode_result(&self, code: c_int) -> Result<()> {
        self.db.borrow_mut().decode_result(code)
    }

    fn changes(&self) -> usize {
        self.db.borrow_mut().changes()
    }

    /// Test for auto-commit mode.
    /// Autocommit mode is on by default.
    pub fn is_autocommit(&self) -> bool {
        self.db.borrow().is_autocommit()
    }

    /// Determine if all associated prepared statements have been reset.
    #[cfg(feature = "bundled")]
    pub fn is_busy(&self) -> bool {
        self.db.borrow().is_busy()
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path)
            .finish()
    }
}

struct InnerConnection {
    db: *mut ffi::sqlite3,
    #[cfg(feature = "hooks")]
    free_commit_hook: Option<fn(*mut ::std::os::raw::c_void)>,
    #[cfg(feature = "hooks")]
    free_rollback_hook: Option<fn(*mut ::std::os::raw::c_void)>,
    #[cfg(feature = "hooks")]
    free_update_hook: Option<fn(*mut ::std::os::raw::c_void)>,
}

/// Old name for `OpenFlags`. `SqliteOpenFlags` is deprecated.
#[deprecated(since = "0.6.0", note = "Use OpenFlags instead")]
pub type SqliteOpenFlags = OpenFlags;

bitflags! {
    #[doc = "Flags for opening SQLite database connections."]
    #[doc = "See [sqlite3_open_v2](http://www.sqlite.org/c3ref/open.html) for details."]
    #[repr(C)]
    pub struct OpenFlags: ::std::os::raw::c_int {
        const SQLITE_OPEN_READ_ONLY     = ffi::SQLITE_OPEN_READONLY;
        const SQLITE_OPEN_READ_WRITE    = ffi::SQLITE_OPEN_READWRITE;
        const SQLITE_OPEN_CREATE        = ffi::SQLITE_OPEN_CREATE;
        const SQLITE_OPEN_URI           = 0x0000_0040;
        const SQLITE_OPEN_MEMORY        = 0x0000_0080;
        const SQLITE_OPEN_NO_MUTEX      = ffi::SQLITE_OPEN_NOMUTEX;
        const SQLITE_OPEN_FULL_MUTEX    = ffi::SQLITE_OPEN_FULLMUTEX;
        const SQLITE_OPEN_SHARED_CACHE  = 0x0002_0000;
        const SQLITE_OPEN_PRIVATE_CACHE = 0x0004_0000;
    }
}

impl Default for OpenFlags {
    fn default() -> OpenFlags {
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI
    }
}

static SQLITE_INIT: Once = ONCE_INIT;
static SQLITE_VERSION_CHECK: Once = ONCE_INIT;
static BYPASS_SQLITE_INIT: AtomicBool = ATOMIC_BOOL_INIT;
static BYPASS_VERSION_CHECK: AtomicBool = ATOMIC_BOOL_INIT;

/// rusqlite's check for a safe SQLite threading mode requires SQLite 3.7.0 or
/// later. If you are running against a SQLite older than that, rusqlite
/// attempts to ensure safety by performing configuration and initialization of
/// SQLite itself the first time you
/// attempt to open a connection. By default, rusqlite panics if that
/// initialization fails, since that could mean SQLite has been initialized in
/// single-thread mode.
///
/// If you are encountering that panic _and_ can ensure that SQLite has been
/// initialized in either multi-thread or serialized mode, call this function
/// prior to attempting to open a connection and rusqlite's initialization
/// process will by skipped. This
/// function is unsafe because if you call it and SQLite has actually been
/// configured to run in single-thread mode,
/// you may enounter memory errors or data corruption or any number of terrible
/// things that should not be possible when you're using Rust.
pub unsafe fn bypass_sqlite_initialization() {
    BYPASS_SQLITE_INIT.store(true, Ordering::Relaxed);
}

/// rusqlite performs a one-time check that the runtime SQLite version is at
/// least as new as the version of SQLite found when rusqlite was built.
/// Bypassing this check may be dangerous; e.g., if you use features of SQLite
/// that are not present in the runtime
/// version. If you are sure the runtime version is compatible with the
/// build-time version for your usage, you can bypass the version check by
/// calling this function before
/// your first connection attempt.
pub unsafe fn bypass_sqlite_version_check() {
    BYPASS_VERSION_CHECK.store(true, Ordering::Relaxed);
}

fn ensure_valid_sqlite_version() {
    SQLITE_VERSION_CHECK.call_once(|| {
        let version_number = version_number();

        // Check our hard floor.
        if version_number < 3_006_008 {
            panic!("rusqlite requires SQLite 3.6.8 or newer");
        }

        // Check that the major version number for runtime and buildtime match.
        let buildtime_major = ffi::SQLITE_VERSION_NUMBER / 1_000_000;
        let runtime_major = version_number / 1_000_000;
        if buildtime_major != runtime_major {
            panic!(
                "rusqlite was built against SQLite {} but is running with SQLite {}",
                str::from_utf8(ffi::SQLITE_VERSION).unwrap(),
                version()
            );
        }

        if BYPASS_VERSION_CHECK.load(Ordering::Relaxed) {
            return;
        }

        // Check that the runtime version number is compatible with the version number
        // we found at build-time.
        if version_number < ffi::SQLITE_VERSION_NUMBER {
            panic!(
                "\
rusqlite was built against SQLite {} but the runtime SQLite version is {}. To fix this, either:
* Recompile rusqlite and link against the SQLite version you are using at runtime, or
* Call rusqlite::bypass_sqlite_version_check() prior to your first connection attempt. Doing this
  means you're sure everything will work correctly even though the runtime version is older than
  the version we found at build time.",
                str::from_utf8(ffi::SQLITE_VERSION).unwrap(),
                version()
            );
        }
    });
}

fn ensure_safe_sqlite_threading_mode() -> Result<()> {
    // Ensure SQLite was compiled in thredsafe mode.
    if unsafe { ffi::sqlite3_threadsafe() == 0 } {
        return Err(Error::SqliteSingleThreadedMode);
    }

    // Now we know SQLite is _capable_ of being in Multi-thread of Serialized mode,
    // but it's possible someone configured it to be in Single-thread mode
    // before calling into us. That would mean we're exposing an unsafe API via
    // a safe one (in Rust terminology), which is no good. We have two options
    // to protect against this, depending on the version of SQLite we're linked
    // with:
    //
    // 1. If we're on 3.7.0 or later, we can ask SQLite for a mutex and check for
    //    the magic value 8. This isn't documented, but it's what SQLite
    //    returns for its mutex allocation function in Single-thread mode.
    // 2. If we're prior to SQLite 3.7.0, AFAIK there's no way to check the
    //    threading mode. The check we perform for >= 3.7.0 will segfault.
    //    Instead, we insist on being able to call sqlite3_config and
    //    sqlite3_initialize ourself, ensuring we know the threading
    //    mode. This will fail if someone else has already initialized SQLite
    //    even if they initialized it safely. That's not ideal either, which is
    //    why we expose bypass_sqlite_initialization    above.
    if version_number() >= 3_007_000 {
        const SQLITE_SINGLETHREADED_MUTEX_MAGIC: usize = 8;
        let is_singlethreaded = unsafe {
            let mutex_ptr = ffi::sqlite3_mutex_alloc(0);
            let is_singlethreaded = mutex_ptr as usize == SQLITE_SINGLETHREADED_MUTEX_MAGIC;
            ffi::sqlite3_mutex_free(mutex_ptr);
            is_singlethreaded
        };
        if is_singlethreaded {
            Err(Error::SqliteSingleThreadedMode)
        } else {
            Ok(())
        }
    } else {
        SQLITE_INIT.call_once(|| {
            if BYPASS_SQLITE_INIT.load(Ordering::Relaxed) {
                return;
            }

            unsafe {
                let msg = "\
Could not ensure safe initialization of SQLite.
To fix this, either:
* Upgrade SQLite to at least version 3.7.0
* Ensure that SQLite has been initialized in Multi-thread or Serialized mode and call
  rusqlite::bypass_sqlite_initialization() prior to your first connection attempt.";

                if ffi::sqlite3_config(ffi::SQLITE_CONFIG_MULTITHREAD) != ffi::SQLITE_OK {
                    panic!(msg);
                }
                if ffi::sqlite3_initialize() != ffi::SQLITE_OK {
                    panic!(msg);
                }
            }
        });
        Ok(())
    }
}

impl InnerConnection {
    #[cfg(not(feature = "hooks"))]
    fn new(db: *mut ffi::sqlite3) -> InnerConnection {
        InnerConnection { db }
    }

    #[cfg(feature = "hooks")]
    fn new(db: *mut ffi::sqlite3) -> InnerConnection {
        InnerConnection {
            db,
            free_commit_hook: None,
            free_rollback_hook: None,
            free_update_hook: None,
        }
    }

    fn open_with_flags(c_path: &CString, flags: OpenFlags) -> Result<InnerConnection> {
        ensure_valid_sqlite_version();
        ensure_safe_sqlite_threading_mode()?;

        // Replicate the check for sane open flags from SQLite, because the check in
        // SQLite itself wasn't added until version 3.7.3.
        debug_assert_eq!(1 << OpenFlags::SQLITE_OPEN_READ_ONLY.bits, 0x02);
        debug_assert_eq!(1 << OpenFlags::SQLITE_OPEN_READ_WRITE.bits, 0x04);
        debug_assert_eq!(
            1 << (OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE).bits,
            0x40
        );
        if (1 << (flags.bits & 0x7)) & 0x46 == 0 {
            return Err(Error::SqliteFailure(
                ffi::Error::new(ffi::SQLITE_MISUSE),
                None,
            ));
        }

        unsafe {
            let mut db: *mut ffi::sqlite3 = mem::uninitialized();
            let r = ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags.bits(), ptr::null());
            if r != ffi::SQLITE_OK {
                let e = if db.is_null() {
                    error_from_sqlite_code(r, None)
                } else {
                    let e = error_from_handle(db, r);
                    ffi::sqlite3_close(db);
                    e
                };

                return Err(e);
            }
            let r = ffi::sqlite3_busy_timeout(db, 5000);
            if r != ffi::SQLITE_OK {
                let e = error_from_handle(db, r);
                ffi::sqlite3_close(db);
                return Err(e);
            }

            // attempt to turn on extended results code; don't fail if we can't.
            ffi::sqlite3_extended_result_codes(db, 1);

            Ok(InnerConnection::new(db))
        }
    }

    fn db(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    fn decode_result(&mut self, code: c_int) -> Result<()> {
        if code == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(error_from_handle(self.db(), code))
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.db.is_null() {
            return Ok(());
        }
        self.remove_hooks();
        unsafe {
            let r = ffi::sqlite3_close(self.db());
            let r = self.decode_result(r);
            if r.is_ok() {
                self.db = ptr::null_mut();
            }
            r
        }
    }

    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let c_sql = try!(str_to_cstring(sql));
        unsafe {
            let r = ffi::sqlite3_exec(
                self.db(),
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            self.decode_result(r)
        }
    }

    #[cfg(feature = "load_extension")]
    fn enable_load_extension(&mut self, onoff: c_int) -> Result<()> {
        let r = unsafe { ffi::sqlite3_enable_load_extension(self.db, onoff) };
        self.decode_result(r)
    }

    #[cfg(feature = "load_extension")]
    fn load_extension(&self, dylib_path: &Path, entry_point: Option<&str>) -> Result<()> {
        let dylib_str = try!(path_to_cstring(dylib_path));
        unsafe {
            let mut errmsg: *mut c_char = mem::uninitialized();
            let r = if let Some(entry_point) = entry_point {
                let c_entry = try!(str_to_cstring(entry_point));
                ffi::sqlite3_load_extension(
                    self.db,
                    dylib_str.as_ptr(),
                    c_entry.as_ptr(),
                    &mut errmsg,
                )
            } else {
                ffi::sqlite3_load_extension(self.db, dylib_str.as_ptr(), ptr::null(), &mut errmsg)
            };
            if r == ffi::SQLITE_OK {
                Ok(())
            } else {
                let message = errmsg_to_string(&*errmsg);
                ffi::sqlite3_free(errmsg as *mut ::std::os::raw::c_void);
                Err(error_from_sqlite_code(r, Some(message)))
            }
        }
    }

    fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db()) }
    }

    fn prepare<'a>(&mut self, conn: &'a Connection, sql: &str) -> Result<Statement<'a>> {
        if sql.len() >= ::std::i32::MAX as usize {
            return Err(error_from_sqlite_code(ffi::SQLITE_TOOBIG, None));
        }
        let mut c_stmt: *mut ffi::sqlite3_stmt = unsafe { mem::uninitialized() };
        let c_sql = try!(str_to_cstring(sql));
        let len_with_nul = (sql.len() + 1) as c_int;
        let r = unsafe {
            if cfg!(feature = "unlock_notify") {
                let mut rc;
                loop {
                    rc = ffi::sqlite3_prepare_v2(
                        self.db(),
                        c_sql.as_ptr(),
                        len_with_nul,
                        &mut c_stmt,
                        ptr::null_mut(),
                    );
                    if !unlock_notify::is_locked(self.db, rc) {
                        break;
                    }
                    rc = unlock_notify::wait_for_unlock_notify(self.db);
                    if rc != ffi::SQLITE_OK {
                        break;
                    }
                }
                rc
            } else {
                ffi::sqlite3_prepare_v2(
                    self.db(),
                    c_sql.as_ptr(),
                    len_with_nul,
                    &mut c_stmt,
                    ptr::null_mut(),
                )
            }
        };
        self.decode_result(r)
            .map(|_| Statement::new(conn, RawStatement::new(c_stmt)))
    }

    fn changes(&mut self) -> usize {
        unsafe { ffi::sqlite3_changes(self.db()) as usize }
    }

    fn is_autocommit(&self) -> bool {
        unsafe { ffi::sqlite3_get_autocommit(self.db()) != 0 }
    }

    #[cfg(feature = "bundled")] // 3.8.6
    fn is_busy(&self) -> bool {
        let db = self.db();
        unsafe {
            let mut stmt = ffi::sqlite3_next_stmt(db, ptr::null_mut());
            while !stmt.is_null() {
                if ffi::sqlite3_stmt_busy(stmt) != 0 {
                    return true;
                }
                stmt = ffi::sqlite3_next_stmt(db, stmt);
            }
        }
        false
    }

    #[cfg(not(feature = "hooks"))]
    fn remove_hooks(&mut self) {}
}

impl Drop for InnerConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        use std::thread::panicking;

        if let Err(e) = self.close() {
            if panicking() {
                eprintln!("Error while closing SQLite connection: {:?}", e);
            } else {
                panic!("Error while closing SQLite connection: {:?}", e);
            }
        }
    }
}

/// Old name for `Statement`. `SqliteStatement` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Statement instead")]
pub type SqliteStatement<'conn> = Statement<'conn>;

/// Old name for `Rows`. `SqliteRows` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Rows instead")]
pub type SqliteRows<'stmt> = Rows<'stmt>;

/// Old name for `Row`. `SqliteRow` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Row instead")]
pub type SqliteRow<'a, 'stmt> = Row<'a, 'stmt>;

#[cfg(test)]
mod test {
    extern crate tempdir;
    use self::tempdir::TempDir;
    pub use super::*;
    use ffi;
    pub use std::error::Error as StdError;
    pub use std::fmt;

    // this function is never called, but is still type checked; in
    // particular, calls with specific instantiations will require
    // that those types are `Send`.
    #[allow(dead_code, unconditional_recursion)]
    fn ensure_send<T: Send>() {
        ensure_send::<Connection>();
    }

    pub fn checked_memory_handle() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn test_concurrent_transactions_busy_commit() {
        use std::time::Duration;
        let tmp = TempDir::new("locked").unwrap();
        let path = tmp.path().join("transactions.db3");

        Connection::open(&path)
            .expect("create temp db")
            .execute_batch(
                "
            BEGIN; CREATE TABLE foo(x INTEGER);
            INSERT INTO foo VALUES(42); END;",
            ).expect("create temp db");

        let mut db1 = Connection::open(&path).unwrap();
        let mut db2 = Connection::open(&path).unwrap();

        db1.busy_timeout(Duration::from_millis(0)).unwrap();
        db2.busy_timeout(Duration::from_millis(0)).unwrap();

        {
            let tx1 = db1.transaction().unwrap();
            let tx2 = db2.transaction().unwrap();

            // SELECT first makes sqlite lock with a shared lock
            let _ = tx1
                .query_row("SELECT x FROM foo LIMIT 1", &[], |_| ())
                .unwrap();
            let _ = tx2
                .query_row("SELECT x FROM foo LIMIT 1", &[], |_| ())
                .unwrap();

            tx1.execute("INSERT INTO foo VALUES(?1)", &[&1]).unwrap();
            let _ = tx2.execute("INSERT INTO foo VALUES(?1)", &[&2]);

            let _ = tx1.commit();
            let _ = tx2.commit();
        }

        let _ = db1
            .transaction()
            .expect("commit should have closed transaction");
        let _ = db2
            .transaction()
            .expect("commit should have closed transaction");
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new("test_open_file").unwrap();
        let path = temp_dir.path().join("test.db3");

        {
            let db = Connection::open(&path).unwrap();
            let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
            db.execute_batch(sql).unwrap();
        }

        let path_string = path.to_str().unwrap();
        let db = Connection::open(&path_string).unwrap();
        let the_answer: Result<i64> = db.query_row("SELECT x FROM foo", &[], |r| r.get(0));

        assert_eq!(42i64, the_answer.unwrap());
    }

    #[test]
    fn test_open() {
        assert!(Connection::open_in_memory().is_ok());

        let db = checked_memory_handle();
        assert!(db.close().is_ok());
    }

    #[test]
    fn test_close_retry() {
        let db = checked_memory_handle();

        // force the DB to be busy by preparing a statement; this must be done at the
        // FFI level to allow us to call .close() without dropping the prepared
        // statement first.
        let raw_stmt = {
            use super::str_to_cstring;
            use std::mem;
            use std::os::raw::c_int;
            use std::ptr;

            let raw_db = db.db.borrow_mut().db;
            let sql = "SELECT 1";
            let mut raw_stmt: *mut ffi::sqlite3_stmt = unsafe { mem::uninitialized() };
            let rc = unsafe {
                ffi::sqlite3_prepare_v2(
                    raw_db,
                    str_to_cstring(sql).unwrap().as_ptr(),
                    (sql.len() + 1) as c_int,
                    &mut raw_stmt,
                    ptr::null_mut(),
                )
            };
            assert_eq!(rc, ffi::SQLITE_OK);
            raw_stmt
        };

        // now that we have an open statement, trying (and retrying) to close should
        // fail.
        let (db, _) = db.close().unwrap_err();
        let (db, _) = db.close().unwrap_err();
        let (db, _) = db.close().unwrap_err();

        // finalize the open statement so a final close will succeed
        assert_eq!(ffi::SQLITE_OK, unsafe { ffi::sqlite3_finalize(raw_stmt) });

        db.close().unwrap();
    }

    #[test]
    fn test_open_with_flags() {
        for bad_flags in &[
            OpenFlags::empty(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_READ_WRITE,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_CREATE,
        ] {
            assert!(Connection::open_in_memory_with_flags(*bad_flags).is_err());
        }
    }

    #[test]
    fn test_execute_batch() {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql).unwrap();

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3")
            .unwrap();

        assert!(db.execute_batch("INVALID SQL").is_err());
    }

    #[test]
    fn test_execute() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(
            1,
            db.execute("INSERT INTO foo(x) VALUES (?)", &[&1i32])
                .unwrap()
        );
        assert_eq!(
            1,
            db.execute("INSERT INTO foo(x) VALUES (?)", &[&2i32])
                .unwrap()
        );

        assert_eq!(
            3i32,
            db.query_row::<i32, _>("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
                .unwrap()
        );
    }

    #[test]
    fn test_execute_select() {
        let db = checked_memory_handle();
        let err = db.execute("SELECT 1 WHERE 1 < ?", &[&1i32]).unwrap_err();
        match err {
            Error::ExecuteReturnedResults => (),
            _ => panic!("Unexpected error: {}", err),
        }
    }

    #[test]
    fn test_prepare_column_names() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);").unwrap();

        let stmt = db.prepare("SELECT * FROM foo").unwrap();
        assert_eq!(stmt.column_count(), 1);
        assert_eq!(stmt.column_names(), vec!["x"]);

        let stmt = db.prepare("SELECT x AS a, x AS b FROM foo").unwrap();
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_names(), vec!["a", "b"]);
    }

    #[test]
    fn test_prepare_execute() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);").unwrap();

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)").unwrap();
        assert_eq!(insert_stmt.execute(&[&1i32]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&2i32]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&3i32]).unwrap(), 1);

        assert_eq!(insert_stmt.execute(&[&"hello".to_string()]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&"goodbye".to_string()]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&types::Null]).unwrap(), 1);

        let mut update_stmt = db.prepare("UPDATE foo SET x=? WHERE x<?").unwrap();
        assert_eq!(update_stmt.execute(&[&3i32, &3i32]).unwrap(), 2);
        assert_eq!(update_stmt.execute(&[&3i32, &3i32]).unwrap(), 0);
        assert_eq!(update_stmt.execute(&[&8i32, &8i32]).unwrap(), 3);
    }

    #[test]
    fn test_prepare_query() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);").unwrap();

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)").unwrap();
        assert_eq!(insert_stmt.execute(&[&1i32]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&2i32]).unwrap(), 1);
        assert_eq!(insert_stmt.execute(&[&3i32]).unwrap(), 1);

        let mut query = db
            .prepare("SELECT x FROM foo WHERE x < ? ORDER BY x DESC")
            .unwrap();
        {
            let mut rows = query.query(&[&4i32]).unwrap();
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next() {
                v.push(row.unwrap().get(0));
            }

            assert_eq!(v, [3i32, 2, 1]);
        }

        {
            let mut rows = query.query(&[&3i32]).unwrap();
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next() {
                v.push(row.unwrap().get(0));
            }

            assert_eq!(v, [2i32, 1]);
        }
    }

    #[test]
    fn test_query_map() {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y TEXT);
                   INSERT INTO foo VALUES(4, \"hello\");
                   INSERT INTO foo VALUES(3, \", \");
                   INSERT INTO foo VALUES(2, \"world\");
                   INSERT INTO foo VALUES(1, \"!\");
                   END;";
        db.execute_batch(sql).unwrap();

        let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC").unwrap();
        let results: Result<Vec<String>> =
            query.query_map(&[], |row| row.get(1)).unwrap().collect();

        assert_eq!(results.unwrap().concat(), "hello, world!");
    }

    #[test]
    fn test_query_row() {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql).unwrap();

        assert_eq!(
            10i64,
            db.query_row::<i64, _>("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
                .unwrap()
        );

        let result: Result<i64> = db.query_row("SELECT x FROM foo WHERE x > 5", &[], |r| r.get(0));
        match result.unwrap_err() {
            Error::QueryReturnedNoRows => (),
            err => panic!("Unexpected error {}", err),
        }

        let bad_query_result = db.query_row("NOT A PROPER QUERY; test123", &[], |_| ());

        assert!(bad_query_result.is_err());
    }

    #[test]
    fn test_prepare_failures() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);").unwrap();

        let err = db.prepare("SELECT * FROM does_not_exist").unwrap_err();
        assert!(format!("{}", err).contains("does_not_exist"));
    }

    #[test]
    fn test_last_insert_rowid() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER PRIMARY KEY)")
            .unwrap();
        db.execute_batch("INSERT INTO foo DEFAULT VALUES").unwrap();

        assert_eq!(db.last_insert_rowid(), 1);

        let mut stmt = db.prepare("INSERT INTO foo DEFAULT VALUES").unwrap();
        for _ in 0i32..9 {
            stmt.execute(&[]).unwrap();
        }
        assert_eq!(db.last_insert_rowid(), 10);
    }

    #[test]
    fn test_is_autocommit() {
        let db = checked_memory_handle();
        assert!(
            db.is_autocommit(),
            "autocommit expected to be active by default"
        );
    }

    #[test]
    #[cfg(feature = "bundled")]
    fn test_is_busy() {
        let db = checked_memory_handle();
        assert!(!db.is_busy());
        let mut stmt = db.prepare("PRAGMA schema_version").unwrap();
        assert!(!db.is_busy());
        {
            let mut rows = stmt.query(&[]).unwrap();
            assert!(!db.is_busy());
            let row = rows.next();
            assert!(db.is_busy());
            assert!(row.is_some());
        }
        assert!(!db.is_busy());
    }

    #[test]
    fn test_statement_debugging() {
        let db = checked_memory_handle();
        let query = "SELECT 12345";
        let stmt = db.prepare(query).unwrap();

        assert!(format!("{:?}", stmt).contains(query));
    }

    #[test]
    fn test_notnull_constraint_error() {
        // extended error codes for constraints were added in SQLite 3.7.16; if we're
        // running on our bundled version, we know the extended error code exists.
        #[cfg(feature = "bundled")]
        fn check_extended_code(extended_code: c_int) {
            assert_eq!(extended_code, ffi::SQLITE_CONSTRAINT_NOTNULL);
        }
        #[cfg(not(feature = "bundled"))]
        fn check_extended_code(_extended_code: c_int) {}

        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x NOT NULL)").unwrap();

        let result = db.execute("INSERT INTO foo (x) VALUES (NULL)", &[]);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::SqliteFailure(err, _) => {
                assert_eq!(err.code, ErrorCode::ConstraintViolation);
                check_extended_code(err.extended_code);
            }
            err => panic!("Unexpected error {}", err),
        }
    }

    #[test]
    fn test_version_string() {
        let n = version_number();
        let major = n / 1_000_000;
        let minor = (n % 1_000_000) / 1_000;
        let patch = n % 1_000;

        assert!(version().contains(&format!("{}.{}.{}", major, minor, patch)));
    }

    mod query_and_then_tests {
        extern crate libsqlite3_sys as ffi;
        use super::*;

        #[derive(Debug)]
        enum CustomError {
            SomeError,
            Sqlite(Error),
        }

        impl fmt::Display for CustomError {
            fn fmt(&self, f: &mut fmt::Formatter) -> ::std::result::Result<(), fmt::Error> {
                match *self {
                    CustomError::SomeError => write!(f, "{}", self.description()),
                    CustomError::Sqlite(ref se) => write!(f, "{}: {}", self.description(), se),
                }
            }
        }

        impl StdError for CustomError {
            fn description(&self) -> &str {
                "my custom error"
            }

            fn cause(&self) -> Option<&StdError> {
                match *self {
                    CustomError::SomeError => None,
                    CustomError::Sqlite(ref se) => Some(se),
                }
            }
        }

        impl From<Error> for CustomError {
            fn from(se: Error) -> CustomError {
                CustomError::Sqlite(se)
            }
        }

        type CustomResult<T> = ::std::result::Result<T, CustomError>;

        #[test]
        fn test_query_and_then() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
                       END;";
            db.execute_batch(sql).unwrap();

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC").unwrap();
            let results: Result<Vec<String>> = query
                .query_and_then(&[], |row| row.get_checked(1))
                .unwrap()
                .collect();

            assert_eq!(results.unwrap().concat(), "hello, world!");
        }

        #[test]
        fn test_query_and_then_fails() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
                       END;";
            db.execute_batch(sql).unwrap();

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC").unwrap();
            let bad_type: Result<Vec<f64>> = query
                .query_and_then(&[], |row| row.get_checked(1))
                .unwrap()
                .collect();

            match bad_type.unwrap_err() {
                Error::InvalidColumnType(_, _) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: Result<Vec<String>> = query
                .query_and_then(&[], |row| row.get_checked(3))
                .unwrap()
                .collect();

            match bad_idx.unwrap_err() {
                Error::InvalidColumnIndex(_) => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        fn test_query_and_then_custom_error() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
                       END;";
            db.execute_batch(sql).unwrap();

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC").unwrap();
            let results: CustomResult<Vec<String>> = query
                .query_and_then(&[], |row| row.get_checked(1).map_err(CustomError::Sqlite))
                .unwrap()
                .collect();

            assert_eq!(results.unwrap().concat(), "hello, world!");
        }

        #[test]
        fn test_query_and_then_custom_error_fails() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
                       END;";
            db.execute_batch(sql).unwrap();

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC").unwrap();
            let bad_type: CustomResult<Vec<f64>> = query
                .query_and_then(&[], |row| row.get_checked(1).map_err(CustomError::Sqlite))
                .unwrap()
                .collect();

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(_, _)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<Vec<String>> = query
                .query_and_then(&[], |row| row.get_checked(3).map_err(CustomError::Sqlite))
                .unwrap()
                .collect();

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<Vec<String>> = query
                .query_and_then(&[], |_| Err(CustomError::SomeError))
                .unwrap()
                .collect();

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        fn test_query_row_and_then_custom_error() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       END;";
            db.execute_batch(sql).unwrap();

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let results: CustomResult<String> = db.query_row_and_then(query, &[], |row| {
                row.get_checked(1).map_err(CustomError::Sqlite)
            });

            assert_eq!(results.unwrap(), "hello");
        }

        #[test]
        fn test_query_row_and_then_custom_error_fails() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       END;";
            db.execute_batch(sql).unwrap();

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let bad_type: CustomResult<f64> = db.query_row_and_then(query, &[], |row| {
                row.get_checked(1).map_err(CustomError::Sqlite)
            });

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(_, _)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<String> = db.query_row_and_then(query, &[], |row| {
                row.get_checked(3).map_err(CustomError::Sqlite)
            });

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<String> =
                db.query_row_and_then(query, &[], |_| Err(CustomError::SomeError));

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        fn test_dynamic() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       END;";
            db.execute_batch(sql).unwrap();

            db.query_row("SELECT * FROM foo", &[], |r| {
                assert_eq!(2, r.column_count())
            }).unwrap();
        }
    }
}
