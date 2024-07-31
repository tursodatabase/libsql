#![allow(clippy::all)]
//! Rusqlite is an ergonomic wrapper for using SQLite from Rust.
//!
//! Historically, the API was based on the one from
//! [`rust-postgres`](https://github.com/sfackler/rust-postgres). However, the
//! two have diverged in many ways, and no compatibility between the two is
//! intended.
//!
//! ```rust
//! use rusqlite::{params, Connection, Result};
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>,
//! }
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::open_in_memory()?;
//!
//!     conn.execute(
//!         "CREATE TABLE person (
//!             id   INTEGER PRIMARY KEY,
//!             name TEXT NOT NULL,
//!             data BLOB
//!         )",
//!         (), // empty list of parameters.
//!     )?;
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         data: None,
//!     };
//!     conn.execute(
//!         "INSERT INTO person (name, data) VALUES (?1, ?2)",
//!         (&me.name, &me.data),
//!     )?;
//!
//!     let mut stmt = conn.prepare("SELECT id, name, data FROM person")?;
//!     let person_iter = stmt.query_map([], |row| {
//!         Ok(Person {
//!             id: row.get(0)?,
//!             name: row.get(1)?,
//!             data: row.get(2)?,
//!         })
//!     })?;
//!
//!     for person in person_iter {
//!         println!("Found person {:?}", person.unwrap());
//!     }
//!     Ok(())
//! }
//! ```
#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use libsql_ffi as ffi;

use std::cell::RefCell;
use std::default::Default;
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::{c_char, c_int};

use std::path::Path;
use std::result;
use std::str;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::cache::StatementCache;
use crate::inner_connection::{InnerConnection, BYPASS_SQLITE_INIT};
use crate::raw_statement::RawStatement;
use crate::types::ValueRef;

pub use crate::cache::CachedStatement;
pub use crate::column::Column;
pub use crate::error::Error;
pub use crate::ffi::ErrorCode;
#[cfg(feature = "load_extension")]
pub use crate::load_extension_guard::LoadExtensionGuard;
pub use crate::params::{params_from_iter, Params, ParamsFromIter};
pub use crate::row::{AndThenRows, Map, MappedRows, Row, RowIndex, Rows};
pub use crate::statement::{Statement, StatementStatus};
#[cfg(feature = "modern_sqlite")]
pub use crate::transaction::TransactionState;
pub use crate::transaction::{DropBehavior, Savepoint, Transaction, TransactionBehavior};
pub use crate::types::ToSql;
pub use crate::version::*;

mod error;

#[cfg(feature = "backup")]
#[cfg_attr(docsrs, doc(cfg(feature = "backup")))]
pub mod backup;
#[cfg(feature = "blob")]
#[cfg_attr(docsrs, doc(cfg(feature = "blob")))]
pub mod blob;
mod busy;
mod cache;
#[cfg(feature = "collation")]
#[cfg_attr(docsrs, doc(cfg(feature = "collation")))]
mod collation;
mod column;
pub mod config;
#[cfg(any(feature = "functions", feature = "vtab"))]
mod context;
#[cfg(feature = "functions")]
#[cfg_attr(docsrs, doc(cfg(feature = "functions")))]
pub mod functions;
#[cfg(feature = "hooks")]
#[cfg_attr(docsrs, doc(cfg(feature = "hooks")))]
pub mod hooks;
mod inner_connection;
#[cfg(feature = "limits")]
#[cfg_attr(docsrs, doc(cfg(feature = "limits")))]
pub mod limits;
#[cfg(feature = "load_extension")]
mod load_extension_guard;
mod params;
mod pragma;
mod raw_statement;
mod row;
#[cfg(feature = "session")]
#[cfg_attr(docsrs, doc(cfg(feature = "session")))]
pub mod session;
mod statement;
#[cfg(feature = "trace")]
#[cfg_attr(docsrs, doc(cfg(feature = "trace")))]
pub mod trace;
mod transaction;
pub mod types;
#[cfg(feature = "unlock_notify")]
mod unlock_notify;
mod version;
#[cfg(feature = "vtab")]
#[cfg_attr(docsrs, doc(cfg(feature = "vtab")))]
pub mod vtab;

pub(crate) mod util;
pub(crate) use util::SmallCString;

// Number of cached prepared statements we'll hold on to.
const STATEMENT_CACHE_DEFAULT_CAPACITY: usize = 16;

/// A macro making it more convenient to longer lists of
/// parameters as a `&[&dyn ToSql]`.
///
/// # Example
///
/// ```rust,no_run
/// # use rusqlite::{Result, Connection, params};
///
/// struct Person {
///     name: String,
///     age_in_years: u8,
///     data: Option<Vec<u8>>,
/// }
///
/// fn add_person(conn: &Connection, person: &Person) -> Result<()> {
///     conn.execute(
///         "INSERT INTO person(name, age_in_years, data) VALUES (?1, ?2, ?3)",
///         params![person.name, person.age_in_years, person.data],
///     )?;
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! params {
    () => {
        &[] as &[&dyn $crate::ToSql]
    };
    ($($param:expr),+ $(,)?) => {
        &[$(&$param as &dyn $crate::ToSql),+] as &[&dyn $crate::ToSql]
    };
}

/// A macro making it more convenient to pass lists of named parameters
/// as a `&[(&str, &dyn ToSql)]`.
///
/// # Example
///
/// ```rust,no_run
/// # use rusqlite::{Result, Connection, named_params};
///
/// struct Person {
///     name: String,
///     age_in_years: u8,
///     data: Option<Vec<u8>>,
/// }
///
/// fn add_person(conn: &Connection, person: &Person) -> Result<()> {
///     conn.execute(
///         "INSERT INTO person (name, age_in_years, data)
///          VALUES (:name, :age, :data)",
///         named_params! {
///             ":name": person.name,
///             ":age": person.age_in_years,
///             ":data": person.data,
///         },
///     )?;
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! named_params {
    () => {
        &[] as &[(&str, &dyn $crate::ToSql)]
    };
    // Note: It's a lot more work to support this as part of the same macro as
    // `params!`, unfortunately.
    ($($param_name:literal: $param_val:expr),+ $(,)?) => {
        &[$(($param_name, &$param_val as &dyn $crate::ToSql)),+] as &[(&str, &dyn $crate::ToSql)]
    };
}

/// A typedef of the result returned by many methods.
pub type Result<T, E = Error> = result::Result<T, E>;

/// See the [method documentation](#tymethod.optional).
pub trait OptionalExtension<T> {
    /// Converts a `Result<T>` into a `Result<Option<T>>`.
    ///
    /// By default, Rusqlite treats 0 rows being returned from a query that is
    /// expected to return 1 row as an error. This method will
    /// handle that error, and give you back an `Option<T>` instead.
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalExtension<T> for Result<T> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

unsafe fn errmsg_to_string(errmsg: *const c_char) -> String {
    let c_slice = CStr::from_ptr(errmsg).to_bytes();
    String::from_utf8_lossy(c_slice).into_owned()
}

fn str_to_cstring(s: &str) -> Result<SmallCString> {
    Ok(SmallCString::new(s)?)
}

/// Returns `Ok((string ptr, len as c_int, SQLITE_STATIC | SQLITE_TRANSIENT))`
/// normally.
/// Returns error if the string is too large for sqlite.
/// The `sqlite3_destructor_type` item is always `SQLITE_TRANSIENT` unless
/// the string was empty (in which case it's `SQLITE_STATIC`, and the ptr is
/// static).
fn str_for_sqlite(s: &[u8]) -> Result<(*const c_char, c_int, ffi::sqlite3_destructor_type)> {
    let len = len_as_c_int(s.len())?;
    let (ptr, dtor_info) = if len != 0 {
        (s.as_ptr().cast::<c_char>(), ffi::SQLITE_TRANSIENT())
    } else {
        // Return a pointer guaranteed to live forever
        ("".as_ptr().cast::<c_char>(), ffi::SQLITE_STATIC())
    };
    Ok((ptr, len, dtor_info))
}

// Helper to cast to c_int safely, returning the correct error type if the cast
// failed.
fn len_as_c_int(len: usize) -> Result<c_int> {
    if len >= (c_int::MAX as usize) {
        Err(Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_TOOBIG),
            None,
        ))
    } else {
        Ok(len as c_int)
    }
}

#[cfg(unix)]
fn path_to_cstring(p: &Path) -> Result<CString> {
    use std::os::unix::ffi::OsStrExt;
    Ok(CString::new(p.as_os_str().as_bytes())?)
}

#[cfg(not(unix))]
fn path_to_cstring(p: &Path) -> Result<CString> {
    let s = p.to_str().ok_or_else(|| Error::InvalidPath(p.to_owned()))?;
    Ok(CString::new(s)?)
}

/// Name for a database within a SQLite connection.
#[derive(Copy, Clone, Debug)]
pub enum DatabaseName<'a> {
    /// The main database.
    Main,

    /// The temporary database (e.g., any "CREATE TEMPORARY TABLE" tables).
    Temp,

    /// A database that has been attached via "ATTACH DATABASE ...".
    Attached(&'a str),
}

/// Shorthand for [`DatabaseName::Main`].
pub const MAIN_DB: DatabaseName<'static> = DatabaseName::Main;

/// Shorthand for [`DatabaseName::Temp`].
pub const TEMP_DB: DatabaseName<'static> = DatabaseName::Temp;

// Currently DatabaseName is only used by the backup and blob mods, so hide
// this (private) impl to avoid dead code warnings.
impl DatabaseName<'_> {
    #[inline]
    fn as_cstring(&self) -> Result<SmallCString> {
        use self::DatabaseName::{Attached, Main, Temp};
        match *self {
            Main => str_to_cstring("main"),
            Temp => str_to_cstring("temp"),
            Attached(s) => str_to_cstring(s),
        }
    }
}

/// A connection to a SQLite database.
pub struct Connection {
    db: RefCell<InnerConnection>,
    cache: StatementCache,
}

unsafe impl Send for Connection {}

impl Drop for Connection {
    #[inline]
    fn drop(&mut self) {
        self.flush_prepared_statement_cache();
    }
}

impl Connection {
    /// Open a new connection to a SQLite database. If a database does not exist
    /// at the path, one is created.
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn open_my_db() -> Result<()> {
    ///     let path = "./my_db.db3";
    ///     let db = Connection::open(path)?;
    ///     // Use the database somehow...
    ///     println!("{}", db.is_autocommit());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Flags
    ///
    /// `Connection::open(path)` is equivalent to using
    /// [`Connection::open_with_flags`] with the default [`OpenFlags`]. That is,
    /// it's equivalent to:
    ///
    /// ```ignore
    /// Connection::open_with_flags(
    ///     path,
    ///     OpenFlags::SQLITE_OPEN_READ_WRITE
    ///         | OpenFlags::SQLITE_OPEN_CREATE
    ///         | OpenFlags::SQLITE_OPEN_URI
    ///         | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    /// )
    /// ```
    ///
    /// These flags have the following effects:
    ///
    /// - Open the database for both reading or writing.
    /// - Create the database if one does not exist at the path.
    /// - Allow the filename to be interpreted as a URI (see <https://www.sqlite.org/uri.html#uri_filenames_in_sqlite>
    ///   for details).
    /// - Disables the use of a per-connection mutex.
    ///
    ///     Rusqlite enforces thread-safety at compile time, so additional
    ///     locking is not needed and provides no benefit. (See the
    ///     documentation on [`OpenFlags::SQLITE_OPEN_FULL_MUTEX`] for some
    ///     additional discussion about this).
    ///
    /// Most of these are also the default settings for the C API, although
    /// technically the default locking behavior is controlled by the flags used
    /// when compiling SQLite -- rather than let it vary, we choose `NO_MUTEX`
    /// because it's a fairly clearly the best choice for users of this library.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible string
    /// or if the underlying SQLite open call fails.
    #[inline]
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        let flags = OpenFlags::default();
        Connection::open_with_flags(path, flags)
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    #[inline]
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
    #[inline]
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        InnerConnection::open_with_flags(
            &c_path,
            flags,
            None,
            #[cfg(feature = "libsql-experimental")]
            None,
        )
        .map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
        })
    }

    /// Open a new connection to a SQLite database using the specific flags and
    /// vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if either `path` or `vfs` cannot be converted to a
    /// C-compatible string or if the underlying SQLite open call fails.
    #[inline]
    pub fn open_with_flags_and_vfs<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
        vfs: &str,
    ) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        let c_vfs = str_to_cstring(vfs)?;
        InnerConnection::open_with_flags(
            &c_path,
            flags,
            Some(&c_vfs),
            #[cfg(feature = "libsql-experimental")]
            None,
        )
        .map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
        })
    }

    /// Open a new connection to a SQLite database using the specific flags and
    /// WAL methods name.
    ///
    ///
    /// # Failure
    ///
    /// Will return `Err` if either `path` or `wal` cannot be converted to a
    /// C-compatible string or if the underlying SQLite open call fails.
    #[inline]
    #[cfg(feature = "libsql-experimental")]
    pub fn open_with_flags_and_wal<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
        wal_manager: libsql_ffi::libsql_wal_manager,
    ) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        InnerConnection::open_with_flags(&c_path, flags, None, Some(wal_manager)).map(|db| {
            Connection {
                db: RefCell::new(db),
                cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            }
        })
    }

    /// Open a new connection to a SQLite database using the specific flags and
    /// WAL methods name.
    ///
    ///
    /// # Failure
    ///
    /// Will return `Err` if either `path` or `wal` cannot be converted to a
    /// C-compatible string or if the underlying SQLite open call fails.
    #[inline]
    #[cfg(feature = "libsql-experimental")]
    pub fn open_with_flags_vfs_and_wal<P: AsRef<Path>>(
        path: P,
        flags: OpenFlags,
        vfs: &str,
        wal_manager: ffi::libsql_wal_manager,
    ) -> Result<Connection> {
        let c_path = path_to_cstring(path.as_ref())?;
        let c_vfs = str_to_cstring(vfs)?;
        InnerConnection::open_with_flags(&c_path, flags, Some(&c_vfs), Some(wal_manager)).map(
            |db| Connection {
                db: RefCell::new(db),
                cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            },
        )
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    #[inline]
    pub fn open_in_memory_with_flags(flags: OpenFlags) -> Result<Connection> {
        Connection::open_with_flags(":memory:", flags)
    }

    /// Open a new connection to an in-memory SQLite database using the specific
    /// flags and vfs name.
    ///
    /// [Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `vfs` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
    #[inline]
    pub fn open_in_memory_with_flags_and_vfs(flags: OpenFlags, vfs: &str) -> Result<Connection> {
        Connection::open_with_flags_and_vfs(":memory:", flags, vfs)
    }

    /// Convenience method to run multiple SQL statements (that cannot take any
    /// parameters).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn create_tables(conn: &Connection) -> Result<()> {
    ///     conn.execute_batch(
    ///         "BEGIN;
    ///          CREATE TABLE foo(x INTEGER);
    ///          CREATE TABLE bar(y TEXT);
    ///          COMMIT;",
    ///     )
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        let mut sql = sql;
        while !sql.is_empty() {
            let stmt = self.prepare(sql)?;
            if !stmt.stmt.is_null() && stmt.step()? && cfg!(feature = "extra_check") {
                // Some PRAGMA may return rows
                return Err(Error::ExecuteReturnedResults);
            }
            let tail = stmt.stmt.tail();
            if tail == 0 || tail >= sql.len() {
                break;
            }
            sql = &sql[tail..];
        }
        Ok(())
    }

    /// Convenience method to prepare and execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ### With positional params
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?1", [1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// ### With positional params of varying types
    ///
    /// ```rust,no_run
    /// # use rusqlite::{params, Connection};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute(
    ///         "UPDATE foo SET bar = 'baz' WHERE qux = ?1 AND quux = ?2",
    ///         params![1i32, 1.5f64],
    ///     ) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// ### With named params
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert(conn: &Connection) -> Result<usize> {
    ///     conn.execute(
    ///         "INSERT INTO test (name) VALUES (:name)",
    ///         &[(":name", "one")],
    ///     )
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[inline]
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> Result<usize> {
        self.prepare(sql)
            .and_then(|mut stmt| stmt.check_no_tail().and_then(|_| stmt.execute(params)))
    }

    /// Returns the path to the database file, if one exists and is known.
    ///
    /// Returns `Some("")` for a temporary or in-memory database.
    ///
    /// Note that in some cases [PRAGMA
    /// database_list](https://sqlite.org/pragma.html#pragma_database_list) is
    /// likely to be more robust.
    #[inline]
    pub fn path(&self) -> Option<&str> {
        unsafe {
            let db = self.handle();
            let db_name = DatabaseName::Main.as_cstring().unwrap();
            let db_filename = ffi::sqlite3_db_filename(db, db_name.as_ptr());
            if db_filename.is_null() {
                None
            } else {
                CStr::from_ptr(db_filename).to_str().ok()
            }
        }
    }

    /// Attempts to free as much heap memory as possible from the database
    /// connection.
    ///
    /// This calls [`sqlite3_db_release_memory`](https://www.sqlite.org/c3ref/db_release_memory.html).
    #[inline]
    #[cfg(feature = "release_memory")]
    pub fn release_memory(&self) -> Result<()> {
        self.db.borrow_mut().release_memory()
    }

    /// Get the SQLite rowid of the most recent successful INSERT.
    ///
    /// Uses [sqlite3_last_insert_rowid](https://www.sqlite.org/c3ref/last_insert_rowid.html) under
    /// the hood.
    #[inline]
    pub fn last_insert_rowid(&self) -> i64 {
        self.db.borrow_mut().last_insert_rowid()
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result, Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         [],
    ///         |row| row.get(0),
    ///     )
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are
    /// ignored.
    ///
    /// Returns `Err(QueryReturnedNoRows)` if no results are returned. If the
    /// query truly is optional, you can call `.optional()` on the result of
    /// this to get a `Result<Option<T>>`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[inline]
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.check_no_tail()?;
        stmt.query_row(params, f)
    }

    // https://sqlite.org/tclsqlite.html#onecolumn
    #[cfg(test)]
    pub(crate) fn one_column<T: crate::types::FromSql>(&self, sql: &str) -> Result<T> {
        self.query_row(sql, [], |r| r.get(0))
    }

    /// Convenience method to execute a query that is expected to return a
    /// single row, and execute a mapping via `f` on that returned row with
    /// the possibility of failure. The `Result` type of `f` must implement
    /// `std::convert::From<Error>`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result, Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_and_then(
    ///         "SELECT value FROM preferences WHERE name='locale'",
    ///         [],
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
    #[inline]
    pub fn query_row_and_then<T, E, P, F>(&self, sql: &str, params: P, f: F) -> Result<T, E>
    where
        P: Params,
        F: FnOnce(&Row<'_>) -> Result<T, E>,
        E: From<Error>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.check_no_tail()?;
        let mut rows = stmt.query(params)?;

        rows.get_expected_row().map_err(E::from).and_then(f)
    }

    /// Prepare a SQL statement for execution.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("INSERT INTO People (name) VALUES (?1)")?;
    ///     stmt.execute(["Joe Smith"])?;
    ///     stmt.execute(["Bob Jones"])?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[inline]
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_>> {
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
    #[inline]
    pub fn close(self) -> Result<(), (Connection, Error)> {
        self.flush_prepared_statement_cache();
        let r = self.db.borrow_mut().close();
        r.map_err(move |err| (self, err))
    }

    /// Enable loading of SQLite extensions from both SQL queries and Rust.
    ///
    /// You must call [`Connection::load_extension_disable`] when you're
    /// finished loading extensions (failure to call it can lead to bad things,
    /// see "Safety"), so you should strongly consider using
    /// [`LoadExtensionGuard`] instead of this function, automatically disables
    /// extension loading when it goes out of scope.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn load_my_extension(conn: &Connection) -> Result<()> {
    ///     // Safety: We fully trust the loaded extension and execute no untrusted SQL
    ///     // while extension loading is enabled.
    ///     unsafe {
    ///         conn.load_extension_enable()?;
    ///         let r = conn.load_extension("my/trusted/extension", None);
    ///         conn.load_extension_disable()?;
    ///         r
    ///     }
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    ///
    /// # Safety
    ///
    /// TLDR: Don't execute any untrusted queries between this call and
    /// [`Connection::load_extension_disable`].
    ///
    /// Perhaps surprisingly, this function does not only allow the use of
    /// [`Connection::load_extension`] from Rust, but it also allows SQL queries
    /// to perform [the same operation][loadext]. For example, in the period
    /// between `load_extension_enable` and `load_extension_disable`, the
    /// following operation will load and call some function in some dynamic
    /// library:
    ///
    /// ```sql
    /// SELECT load_extension('why_is_this_possible.dll', 'dubious_func');
    /// ```
    ///
    /// This means that while this is enabled a carefully crafted SQL query can
    /// be used to escalate a SQL injection attack into code execution.
    ///
    /// Safely using this function requires that you trust all SQL queries run
    /// between when it is called, and when loading is disabled (by
    /// [`Connection::load_extension_disable`]).
    ///
    /// [loadext]: https://www.sqlite.org/lang_corefunc.html#load_extension
    #[cfg(feature = "load_extension")]
    #[cfg_attr(docsrs, doc(cfg(feature = "load_extension")))]
    #[inline]
    pub unsafe fn load_extension_enable(&self) -> Result<()> {
        self.db.borrow_mut().enable_load_extension(1)
    }

    /// Disable loading of SQLite extensions.
    ///
    /// See [`Connection::load_extension_enable`] for an example.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    #[cfg_attr(docsrs, doc(cfg(feature = "load_extension")))]
    #[inline]
    pub fn load_extension_disable(&self) -> Result<()> {
        // It's always safe to turn off extension loading.
        unsafe { self.db.borrow_mut().enable_load_extension(0) }
    }

    /// Load the SQLite extension at `dylib_path`. `dylib_path` is passed
    /// through to `sqlite3_load_extension`, which may attempt OS-specific
    /// modifications if the file cannot be loaded directly (for example
    /// converting `"some/ext"` to `"some/ext.so"`, `"some\\ext.dll"`, ...).
    ///
    /// If `entry_point` is `None`, SQLite will attempt to find the entry point.
    /// If it is not `None`, the entry point will be passed through to
    /// `sqlite3_load_extension`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result, LoadExtensionGuard};
    /// fn load_my_extension(conn: &Connection) -> Result<()> {
    ///     // Safety: we don't execute any SQL statements while
    ///     // extension loading is enabled.
    ///     let _guard = unsafe { LoadExtensionGuard::new(conn)? };
    ///     // Safety: `my_sqlite_extension` is highly trustworthy.
    ///     unsafe { conn.load_extension("my_sqlite_extension", None) }
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    ///
    /// # Safety
    ///
    /// This is equivalent to performing a `dlopen`/`LoadLibrary` on a shared
    /// library, and calling a function inside, and thus requires that you trust
    /// the library that you're loading.
    ///
    /// That is to say: to safely use this, the code in the extension must be
    /// sound, trusted, correctly use the SQLite APIs, and not contain any
    /// memory or thread safety errors.
    #[cfg(feature = "load_extension")]
    #[cfg_attr(docsrs, doc(cfg(feature = "load_extension")))]
    #[inline]
    pub unsafe fn load_extension<P: AsRef<Path>>(
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
    /// [open an issue on the rusqlite repository](https://github.com/rusqlite/rusqlite/issues) and describe
    /// your use case.
    ///
    /// # Safety
    ///
    /// This function is unsafe because it gives you raw access
    /// to the SQLite connection, and what you do with it could impact the
    /// safety of this `Connection`.
    #[inline]
    pub unsafe fn handle(&self) -> *mut ffi::sqlite3 {
        self.db.borrow().db()
    }

    /// Create a `Connection` from a raw handle.
    ///
    /// The underlying SQLite database connection handle will not be closed when
    /// the returned connection is dropped/closed.
    ///
    /// # Safety
    ///
    /// This function is unsafe because improper use may impact the Connection.
    #[inline]
    pub unsafe fn from_handle(db: *mut ffi::sqlite3) -> Result<Connection> {
        let db = InnerConnection::new(db, false);
        Ok(Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
        })
    }

    /// Create a `Connection` from a raw owned handle.
    ///
    /// The returned connection will attempt to close the inner connection
    /// when dropped/closed. This function should only be called on connections
    /// owned by the caller.
    ///
    /// # Safety
    ///
    /// This function is unsafe because improper use may impact the Connection.
    /// In particular, it should only be called on connections created
    /// and owned by the caller, e.g. as a result of calling ffi::sqlite3_open().
    #[inline]
    pub unsafe fn from_handle_owned(db: *mut ffi::sqlite3) -> Result<Connection> {
        let db = InnerConnection::new(db, true);
        Ok(Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
        })
    }

    /// Get access to a handle that can be used to interrupt long running
    /// queries from another thread.
    #[inline]
    pub fn get_interrupt_handle(&self) -> InterruptHandle {
        self.db.borrow().get_interrupt_handle()
    }

    #[inline]
    fn decode_result(&self, code: c_int) -> Result<()> {
        self.db.borrow().decode_result(code)
    }

    /// Return the number of rows modified, inserted or deleted by the most
    /// recently completed INSERT, UPDATE or DELETE statement on the database
    /// connection.
    ///
    /// See <https://www.sqlite.org/c3ref/changes.html>
    #[inline]
    pub fn changes(&self) -> u64 {
        self.db.borrow().changes()
    }

    /// Test for auto-commit mode.
    /// Autocommit mode is on by default.
    #[inline]
    pub fn is_autocommit(&self) -> bool {
        self.db.borrow().is_autocommit()
    }

    /// Determine if all associated prepared statements have been reset.
    #[inline]
    pub fn is_busy(&self) -> bool {
        self.db.borrow().is_busy()
    }

    /// Flush caches to disk mid-transaction
    pub fn cache_flush(&self) -> Result<()> {
        self.db.borrow_mut().cache_flush()
    }

    /// Determine if a database is read-only
    pub fn is_readonly(&self, db_name: DatabaseName<'_>) -> Result<bool> {
        self.db.borrow().db_readonly(db_name)
    }

    /// Try initializing the WebAssembly functions table (idempotent)
    #[cfg(feature = "libsql-wasm-experimental")]
    pub fn try_initialize_wasm_func_table(&self) -> Result<()> {
        self.db.borrow().try_initialize_wasm_func_table()
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path())
            .finish()
    }
}

/// Batch iterator
/// ```rust
/// use rusqlite::{Batch, Connection, Result};
///
/// fn main() -> Result<()> {
///     let conn = Connection::open_in_memory()?;
///     let sql = r"
///     CREATE TABLE tbl1 (col);
///     CREATE TABLE tbl2 (col);
///     ";
///     let mut batch = Batch::new(&conn, sql);
///     while let Some(mut stmt) = batch.next()? {
///         stmt.execute([])?;
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct Batch<'conn, 'sql> {
    conn: &'conn Connection,
    sql: &'sql str,
    tail: usize,
}

impl<'conn, 'sql> Batch<'conn, 'sql> {
    /// Constructor
    pub fn new(conn: &'conn Connection, sql: &'sql str) -> Batch<'conn, 'sql> {
        Batch { conn, sql, tail: 0 }
    }

    /// Iterates on each batch statements.
    ///
    /// Returns `Ok(None)` when batch is completed.
    #[allow(clippy::should_implement_trait)] // fallible iterator
    pub fn next(&mut self) -> Result<Option<Statement<'conn>>> {
        while self.tail < self.sql.len() {
            let sql = &self.sql[self.tail..];
            let next = self.conn.prepare(sql)?;
            let tail = next.stmt.tail();
            if tail == 0 {
                self.tail = self.sql.len();
            } else {
                self.tail += tail;
            }
            if next.stmt.is_null() {
                continue;
            }
            return Ok(Some(next));
        }
        Ok(None)
    }
}

impl<'conn> Iterator for Batch<'conn, '_> {
    type Item = Result<Statement<'conn>>;

    fn next(&mut self) -> Option<Result<Statement<'conn>>> {
        self.next().transpose()
    }
}

bitflags::bitflags! {
    /// Flags for opening SQLite database connections. See
    /// [sqlite3_open_v2](http://www.sqlite.org/c3ref/open.html) for details.
    ///
    /// The default open flags are `SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE
    /// | SQLITE_OPEN_URI | SQLITE_OPEN_NO_MUTEX`. See [`Connection::open`] for
    /// some discussion about these flags.
    #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
    #[repr(C)]
    pub struct OpenFlags: ::std::os::raw::c_int {
        /// The database is opened in read-only mode.
        /// If the database does not already exist, an error is returned.
        const SQLITE_OPEN_READ_ONLY = ffi::SQLITE_OPEN_READONLY;
        /// The database is opened for reading and writing if possible,
        /// or reading only if the file is write protected by the operating system.
        /// In either case the database must already exist, otherwise an error is returned.
        const SQLITE_OPEN_READ_WRITE = ffi::SQLITE_OPEN_READWRITE;
        /// The database is created if it does not already exist
        const SQLITE_OPEN_CREATE = ffi::SQLITE_OPEN_CREATE;
        /// The filename can be interpreted as a URI if this flag is set.
        const SQLITE_OPEN_URI = ffi::SQLITE_OPEN_URI;
        /// The database will be opened as an in-memory database.
        const SQLITE_OPEN_MEMORY = ffi::SQLITE_OPEN_MEMORY;
        /// The new database connection will not use a per-connection mutex (the
        /// connection will use the "multi-thread" threading mode, in SQLite
        /// parlance).
        ///
        /// This is used by default, as proper `Send`/`Sync` usage (in
        /// particular, the fact that [`Connection`] does not implement `Sync`)
        /// ensures thread-safety without the need to perform locking around all
        /// calls.
        const SQLITE_OPEN_NO_MUTEX = ffi::SQLITE_OPEN_NOMUTEX;
        /// The new database connection will use a per-connection mutex -- the
        /// "serialized" threading mode, in SQLite parlance.
        ///
        /// # Caveats
        ///
        /// This flag should probably never be used with `rusqlite`, as we
        /// ensure thread-safety statically (we implement [`Send`] and not
        /// [`Sync`]). That said
        ///
        /// Critically, even if this flag is used, the [`Connection`] is not
        /// safe to use across multiple threads simultaneously. To access a
        /// database from multiple threads, you should either create multiple
        /// connections, one for each thread (if you have very many threads,
        /// wrapping the `rusqlite::Connection` in a mutex is also reasonable).
        ///
        /// This is both because of the additional per-connection state stored
        /// by `rusqlite` (for example, the prepared statement cache), and
        /// because not all of SQLites functions are fully thread safe, even in
        /// serialized/`SQLITE_OPEN_FULLMUTEX` mode.
        ///
        /// All that said, it's fairly harmless to enable this flag with
        /// `rusqlite`, it will just slow things down while providing no
        /// benefit.
        const SQLITE_OPEN_FULL_MUTEX = ffi::SQLITE_OPEN_FULLMUTEX;
        /// The database is opened with shared cache enabled.
        ///
        /// This is frequently useful for in-memory connections, but note that
        /// broadly speaking it's discouraged by SQLite itself, which states
        /// "Any use of shared cache is discouraged" in the official
        /// [documentation](https://www.sqlite.org/c3ref/enable_shared_cache.html).
        const SQLITE_OPEN_SHARED_CACHE = 0x0002_0000;
        /// The database is opened shared cache disabled.
        const SQLITE_OPEN_PRIVATE_CACHE = 0x0004_0000;
        /// The database filename is not allowed to be a symbolic link. (3.31.0)
        const SQLITE_OPEN_NOFOLLOW = 0x0100_0000;
        /// Extended result codes. (3.37.0)
        const SQLITE_OPEN_EXRESCODE = 0x0200_0000;
    }
}

impl Default for OpenFlags {
    #[inline]
    fn default() -> OpenFlags {
        // Note: update the `Connection::open` and top-level `OpenFlags` docs if
        // you change these.
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI
    }
}

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
/// process will by skipped.
///
/// # Safety
///
/// This function is unsafe because if you call it and SQLite has actually been
/// configured to run in single-thread mode,
/// you may encounter memory errors or data corruption or any number of terrible
/// things that should not be possible when you're using Rust.
pub unsafe fn bypass_sqlite_initialization() {
    BYPASS_SQLITE_INIT.store(true, Ordering::Relaxed);
}

/// Allows interrupting a long-running computation.
pub struct InterruptHandle {
    db_lock: Arc<Mutex<*mut ffi::sqlite3>>,
}

unsafe impl Send for InterruptHandle {}
unsafe impl Sync for InterruptHandle {}

impl InterruptHandle {
    /// Interrupt the query currently executing on another thread. This will
    /// cause that query to fail with a `SQLITE3_INTERRUPT` error.
    pub fn interrupt(&self) {
        let db_handle = self.db_lock.lock().unwrap();
        if !db_handle.is_null() {
            unsafe { ffi::sqlite3_interrupt(*db_handle) }
        }
    }
}

#[cfg(doctest)]
doc_comment::doctest!("../README.md");

#[cfg(test)]
mod test {
    use super::*;
    use crate::ffi;
    use fallible_iterator::FallibleIterator;
    use std::error::Error as StdError;
    use std::fmt;

    // this function is never called, but is still type checked; in
    // particular, calls with specific instantiations will require
    // that those types are `Send`.
    #[allow(
        dead_code,
        unconditional_recursion,
        clippy::extra_unused_type_parameters
    )]
    fn ensure_send<T: Send>() {
        ensure_send::<Connection>();
        ensure_send::<InterruptHandle>();
    }

    #[allow(
        dead_code,
        unconditional_recursion,
        clippy::extra_unused_type_parameters
    )]
    fn ensure_sync<T: Sync>() {
        ensure_sync::<InterruptHandle>();
    }

    fn checked_memory_handle() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn test_concurrent_transactions_busy_commit() -> Result<()> {
        use std::time::Duration;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("transactions.db3");

        Connection::open(&path)?.execute_batch(
            "
            BEGIN; CREATE TABLE foo(x INTEGER);
            INSERT INTO foo VALUES(42); END;",
        )?;

        let mut db1 = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        let mut db2 = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

        db1.busy_timeout(Duration::from_millis(0))?;
        db2.busy_timeout(Duration::from_millis(0))?;

        {
            let tx1 = db1.transaction()?;
            let tx2 = db2.transaction()?;

            // SELECT first makes sqlite lock with a shared lock
            tx1.query_row("SELECT x FROM foo LIMIT 1", [], |_| Ok(()))?;
            tx2.query_row("SELECT x FROM foo LIMIT 1", [], |_| Ok(()))?;

            tx1.execute("INSERT INTO foo VALUES(?1)", [1])?;
            let _ = tx2.execute("INSERT INTO foo VALUES(?1)", [2]);

            let _ = tx1.commit();
            let _ = tx2.commit();
        }

        let _ = db1
            .transaction()
            .expect("commit should have closed transaction");
        let _ = db2
            .transaction()
            .expect("commit should have closed transaction");
        Ok(())
    }

    #[test]
    fn test_persistence() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db3");

        {
            let db = Connection::open(&path)?;
            let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
            db.execute_batch(sql)?;
        }

        let path_string = path.to_str().unwrap();
        let db = Connection::open(path_string)?;
        let the_answer: i64 = db.one_column("SELECT x FROM foo")?;

        assert_eq!(42i64, the_answer);
        Ok(())
    }

    #[test]
    fn test_open() {
        Connection::open_in_memory().unwrap();

        let db = checked_memory_handle();
        db.close().unwrap();
    }

    #[test]
    fn test_path() -> Result<()> {
        let tmp = tempfile::tempdir().unwrap();
        let db = Connection::open("")?;
        assert_eq!(Some(""), db.path());
        let db = Connection::open_in_memory()?;
        assert_eq!(Some(""), db.path());
        let db = Connection::open("file:dummy.db?mode=memory&cache=shared")?;
        assert_eq!(Some(""), db.path());
        let path = tmp.path().join("file.db");
        let db = Connection::open(path)?;
        assert!(db.path().map(|p| p.ends_with("file.db")).unwrap_or(false));
        Ok(())
    }

    #[test]
    fn test_open_failure() {
        let filename = "no_such_file.db";
        let result = Connection::open_with_flags(filename, OpenFlags::SQLITE_OPEN_READ_ONLY);
        let err = result.unwrap_err();
        if let Error::SqliteFailure(e, Some(msg)) = err {
            assert_eq!(ErrorCode::CannotOpen, e.code);
            assert_eq!(ffi::SQLITE_CANTOPEN, e.extended_code);
            assert!(
                msg.contains(filename),
                "error message '{}' does not contain '{}'",
                msg,
                filename
            );
        } else {
            panic!("SqliteFailure expected");
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_invalid_unicode_file_names() -> Result<()> {
        use std::ffi::OsStr;
        use std::fs::File;
        use std::os::unix::ffi::OsStrExt;
        let temp_dir = tempfile::tempdir().unwrap();

        let path = temp_dir.path();
        if File::create(path.join(OsStr::from_bytes(&[0xFE]))).is_err() {
            // Skip test, filesystem doesn't support invalid Unicode
            return Ok(());
        }
        let db_path = path.join(OsStr::from_bytes(&[0xFF]));
        {
            let db = Connection::open(&db_path)?;
            let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(42);
                   END;";
            db.execute_batch(sql)?;
        }

        let db = Connection::open(&db_path)?;
        let the_answer: i64 = db.one_column("SELECT x FROM foo")?;

        assert_eq!(42i64, the_answer);
        Ok(())
    }

    #[test]
    fn test_close_retry() -> Result<()> {
        let db = Connection::open_in_memory()?;

        // force the DB to be busy by preparing a statement; this must be done at the
        // FFI level to allow us to call .close() without dropping the prepared
        // statement first.
        let raw_stmt = {
            use super::str_to_cstring;
            use std::os::raw::c_int;
            use std::ptr;

            let raw_db = db.db.borrow_mut().db;
            let sql = "SELECT 1";
            let mut raw_stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
            let cstring = str_to_cstring(sql)?;
            let rc = unsafe {
                ffi::sqlite3_prepare_v2(
                    raw_db,
                    cstring.as_ptr(),
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
        Ok(())
    }

    #[test]
    fn test_open_with_flags() {
        for bad_flags in &[
            OpenFlags::empty(),
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_READ_WRITE,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_CREATE,
        ] {
            Connection::open_in_memory_with_flags(*bad_flags).unwrap_err();
        }
    }

    #[test]
    fn test_execute_batch() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3")?;

        db.execute_batch("INVALID SQL").unwrap_err();
        Ok(())
    }

    #[test]
    fn test_execute() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        assert_eq!(1, db.execute("INSERT INTO foo(x) VALUES (?1)", [1i32])?);
        assert_eq!(1, db.execute("INSERT INTO foo(x) VALUES (?1)", [2i32])?);

        assert_eq!(3i32, db.one_column::<i32>("SELECT SUM(x) FROM foo")?);
        Ok(())
    }

    #[test]
    #[cfg(feature = "extra_check")]
    fn test_execute_select() {
        let db = checked_memory_handle();
        let err = db.execute("SELECT 1 WHERE 1 < ?1", [1i32]).unwrap_err();
        assert_eq!(
            err,
            Error::ExecuteReturnedResults,
            "Unexpected error: {}",
            err
        );
    }

    #[test]
    #[cfg(feature = "extra_check")]
    fn test_execute_multiple() {
        let db = checked_memory_handle();
        let err = db
            .execute(
                "CREATE TABLE foo(x INTEGER); CREATE TABLE foo(x INTEGER)",
                [],
            )
            .unwrap_err();
        match err {
            Error::MultipleStatement => (),
            _ => panic!("Unexpected error: {}", err),
        }
    }

    #[test]
    fn test_prepare_column_names() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let stmt = db.prepare("SELECT * FROM foo")?;
        assert_eq!(stmt.column_count(), 1);
        assert_eq!(stmt.column_names(), vec!["x"]);

        let stmt = db.prepare("SELECT x AS a, x AS b FROM foo")?;
        assert_eq!(stmt.column_count(), 2);
        assert_eq!(stmt.column_names(), vec!["a", "b"]);
        Ok(())
    }

    #[test]
    fn test_prepare_execute() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?1)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        assert_eq!(insert_stmt.execute(["hello"])?, 1);
        assert_eq!(insert_stmt.execute(["goodbye"])?, 1);
        assert_eq!(insert_stmt.execute([types::Null])?, 1);

        let mut update_stmt = db.prepare("UPDATE foo SET x=?1 WHERE x<?2")?;
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 2);
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 0);
        assert_eq!(update_stmt.execute([8i32, 8i32])?, 3);
        Ok(())
    }

    #[test]
    fn test_prepare_query() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?1)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        let mut query = db.prepare("SELECT x FROM foo WHERE x < ?1 ORDER BY x DESC")?;
        {
            let mut rows = query.query([4i32])?;
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next()? {
                v.push(row.get(0)?);
            }

            assert_eq!(v, [3i32, 2, 1]);
        }

        {
            let mut rows = query.query([3i32])?;
            let mut v = Vec::<i32>::new();

            while let Some(row) = rows.next()? {
                v.push(row.get(0)?);
            }

            assert_eq!(v, [2i32, 1]);
        }
        Ok(())
    }

    #[test]
    fn test_query_map() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y TEXT);
                   INSERT INTO foo VALUES(4, 'hello');
                   INSERT INTO foo VALUES(3, ', ');
                   INSERT INTO foo VALUES(2, 'world');
                   INSERT INTO foo VALUES(1, '!');
                   END;";
        db.execute_batch(sql)?;

        let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
        let results: Result<Vec<String>> = query.query([])?.map(|row| row.get(1)).collect();

        assert_eq!(results?.concat(), "hello, world!");
        Ok(())
    }

    #[test]
    fn test_query_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        assert_eq!(10i64, db.one_column::<i64>("SELECT SUM(x) FROM foo")?);

        let result: Result<i64> = db.one_column("SELECT x FROM foo WHERE x > 5");
        match result.unwrap_err() {
            Error::QueryReturnedNoRows => (),
            err => panic!("Unexpected error {}", err),
        }

        let bad_query_result = db.query_row("NOT A PROPER QUERY; test123", [], |_| Ok(()));

        bad_query_result.unwrap_err();
        Ok(())
    }

    #[test]
    fn test_optional() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let result: Result<i64> = db.one_column("SELECT 1 WHERE 0 <> 0");
        let result = result.optional();
        match result? {
            None => (),
            _ => panic!("Unexpected result"),
        }

        let result: Result<i64> = db.one_column("SELECT 1 WHERE 0 == 0");
        let result = result.optional();
        match result? {
            Some(1) => (),
            _ => panic!("Unexpected result"),
        }

        let bad_query_result: Result<i64> = db.one_column("NOT A PROPER QUERY");
        let bad_query_result = bad_query_result.optional();
        bad_query_result.unwrap_err();
        Ok(())
    }

    #[test]
    fn test_pragma_query_row() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert_eq!("memory", db.one_column::<String>("PRAGMA journal_mode")?);
        let mode = db.one_column::<String>("PRAGMA journal_mode=off")?;
        if cfg!(feature = "bundled") {
            assert_eq!(mode, "off");
        } else {
            // Note: system SQLite on macOS defaults to "off" rather than
            // "memory" for the journal mode (which cannot be changed for
            // in-memory connections). This seems like it's *probably* legal
            // according to the docs below, so we relax this test when not
            // bundling:
            //
            // From https://www.sqlite.org/pragma.html#pragma_journal_mode
            // > Note that the journal_mode for an in-memory database is either
            // > MEMORY or OFF and can not be changed to a different value. An
            // > attempt to change the journal_mode of an in-memory database to
            // > any setting other than MEMORY or OFF is ignored.
            assert!(mode == "memory" || mode == "off", "Got mode {:?}", mode);
        }

        Ok(())
    }

    #[test]
    fn test_prepare_failures() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let err = db.prepare("SELECT * FROM does_not_exist").unwrap_err();
        assert!(format!("{err}").contains("does_not_exist"));
        Ok(())
    }

    #[test]
    fn test_last_insert_rowid() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER PRIMARY KEY)")?;
        db.execute_batch("INSERT INTO foo DEFAULT VALUES")?;

        assert_eq!(db.last_insert_rowid(), 1);

        let mut stmt = db.prepare("INSERT INTO foo DEFAULT VALUES")?;
        for _ in 0i32..9 {
            stmt.execute([])?;
        }
        assert_eq!(db.last_insert_rowid(), 10);
        Ok(())
    }

    #[test]
    fn test_is_autocommit() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert!(
            db.is_autocommit(),
            "autocommit expected to be active by default"
        );
        Ok(())
    }

    #[test]
    fn test_is_busy() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert!(!db.is_busy());
        let mut stmt = db.prepare("PRAGMA schema_version")?;
        assert!(!db.is_busy());
        {
            let mut rows = stmt.query([])?;
            assert!(!db.is_busy());
            let row = rows.next()?;
            assert!(db.is_busy());
            assert!(row.is_some());
        }
        assert!(!db.is_busy());
        Ok(())
    }

    #[test]
    fn test_statement_debugging() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let query = "SELECT 12345";
        let stmt = db.prepare(query)?;

        assert!(format!("{stmt:?}").contains(query));
        Ok(())
    }

    #[test]
    fn test_notnull_constraint_error() -> Result<()> {
        // extended error codes for constraints were added in SQLite 3.7.16; if we're
        // running on our bundled version, we know the extended error code exists.
        fn check_extended_code(extended_code: c_int) {
            assert_eq!(extended_code, ffi::SQLITE_CONSTRAINT_NOTNULL);
        }

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x NOT NULL)")?;

        let result = db.execute("INSERT INTO foo (x) VALUES (NULL)", []);

        match result.unwrap_err() {
            Error::SqliteFailure(err, _) => {
                assert_eq!(err.code, ErrorCode::ConstraintViolation);
                check_extended_code(err.extended_code);
            }
            err => panic!("Unexpected error {}", err),
        }
        Ok(())
    }

    #[test]
    fn test_version_string() {
        let n = version_number();
        let major = n / 1_000_000;
        let minor = (n % 1_000_000) / 1_000;
        let patch = n % 1_000;

        assert!(version().contains(&format!("{major}.{minor}.{patch}")));
    }

    #[test]
    #[cfg(feature = "functions")]
    fn test_interrupt() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let interrupt_handle = db.get_interrupt_handle();

        db.create_scalar_function(
            "interrupt",
            0,
            functions::FunctionFlags::default(),
            move |_| {
                interrupt_handle.interrupt();
                Ok(0)
            },
        )?;

        let mut stmt =
            db.prepare("SELECT interrupt() FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3)")?;

        let result: Result<Vec<i32>> = stmt.query([])?.map(|r| r.get(0)).collect();

        assert_eq!(
            result.unwrap_err().sqlite_error_code(),
            Some(ErrorCode::OperationInterrupted)
        );
        Ok(())
    }

    #[test]
    fn test_interrupt_close() {
        let db = checked_memory_handle();
        let handle = db.get_interrupt_handle();
        handle.interrupt();
        db.close().unwrap();
        handle.interrupt();

        // Look at it's internals to see if we cleared it out properly.
        let db_guard = handle.db_lock.lock().unwrap();
        assert!(db_guard.is_null());
        // It would be nice to test that we properly handle close/interrupt
        // running at the same time, but it seems impossible to do with any
        // degree of reliability.
    }

    #[test]
    fn test_get_raw() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(i, x);")?;
        let vals = ["foobar", "1234", "qwerty"];
        let mut insert_stmt = db.prepare("INSERT INTO foo(i, x) VALUES(?1, ?2)")?;
        for (i, v) in vals.iter().enumerate() {
            let i_to_insert = i as i64;
            assert_eq!(insert_stmt.execute(params![i_to_insert, v])?, 1);
        }

        let mut query = db.prepare("SELECT i, x FROM foo")?;
        let mut rows = query.query([])?;

        while let Some(row) = rows.next()? {
            let i = row.get_ref(0)?.as_i64()?;
            let expect = vals[i as usize];
            let x = row.get_ref("x")?.as_str()?;
            assert_eq!(x, expect);
        }

        let mut query = db.prepare("SELECT x FROM foo")?;
        let rows = query.query_map([], |row| {
            let x = row.get_ref(0)?.as_str()?; // check From<FromSqlError> for Error
            Ok(x[..].to_owned())
        })?;

        for (i, row) in rows.enumerate() {
            assert_eq!(row?, vals[i]);
        }
        Ok(())
    }

    #[test]
    fn test_from_handle() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let handle = unsafe { db.handle() };
        {
            let db = unsafe { Connection::from_handle(handle) }?;
            db.execute_batch("PRAGMA VACUUM")?;
        }
        db.close().unwrap();
        Ok(())
    }

    #[test]
    fn test_from_handle_owned() -> Result<()> {
        let mut handle: *mut ffi::sqlite3 = std::ptr::null_mut();
        let r = unsafe { ffi::sqlite3_open(":memory:\0".as_ptr() as *const i8, &mut handle) };
        assert_eq!(r, ffi::SQLITE_OK);
        let db = unsafe { Connection::from_handle_owned(handle) }?;
        db.execute_batch("PRAGMA VACUUM")?;
        Ok(())
    }

    mod query_and_then_tests {

        use super::*;

        #[derive(Debug)]
        enum CustomError {
            SomeError,
            Sqlite(Error),
        }

        impl fmt::Display for CustomError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
                match *self {
                    CustomError::SomeError => write!(f, "my custom error"),
                    CustomError::Sqlite(ref se) => write!(f, "my custom error: {se}"),
                }
            }
        }

        impl StdError for CustomError {
            fn description(&self) -> &str {
                "my custom error"
            }

            fn cause(&self) -> Option<&dyn StdError> {
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

        type CustomResult<T> = Result<T, CustomError>;

        #[test]
        fn test_query_and_then() -> Result<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let results: Result<Vec<String>> =
                query.query_and_then([], |row| row.get(1))?.collect();

            assert_eq!(results?.concat(), "hello, world!");
            Ok(())
        }

        #[test]
        fn test_query_and_then_fails() -> Result<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let bad_type: Result<Vec<f64>> = query.query_and_then([], |row| row.get(1))?.collect();

            match bad_type.unwrap_err() {
                Error::InvalidColumnType(..) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: Result<Vec<String>> =
                query.query_and_then([], |row| row.get(3))?.collect();

            match bad_idx.unwrap_err() {
                Error::InvalidColumnIndex(_) => (),
                err => panic!("Unexpected error {}", err),
            }
            Ok(())
        }

        #[test]
        fn test_query_and_then_custom_error() -> CustomResult<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let results: CustomResult<Vec<String>> = query
                .query_and_then([], |row| row.get(1).map_err(CustomError::Sqlite))?
                .collect();

            assert_eq!(results?.concat(), "hello, world!");
            Ok(())
        }

        #[test]
        fn test_query_and_then_custom_error_fails() -> Result<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       INSERT INTO foo VALUES(3, ', ');
                       INSERT INTO foo VALUES(2, 'world');
                       INSERT INTO foo VALUES(1, '!');
                       END;";
            db.execute_batch(sql)?;

            let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
            let bad_type: CustomResult<Vec<f64>> = query
                .query_and_then([], |row| row.get(1).map_err(CustomError::Sqlite))?
                .collect();

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(..)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<Vec<String>> = query
                .query_and_then([], |row| row.get(3).map_err(CustomError::Sqlite))?
                .collect();

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<Vec<String>> = query
                .query_and_then([], |_| Err(CustomError::SomeError))?
                .collect();

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
            Ok(())
        }

        #[test]
        fn test_query_row_and_then_custom_error() -> CustomResult<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
            db.execute_batch(sql)?;

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let results: CustomResult<String> =
                db.query_row_and_then(query, [], |row| row.get(1).map_err(CustomError::Sqlite));

            assert_eq!(results?, "hello");
            Ok(())
        }

        #[test]
        fn test_query_row_and_then_custom_error_fails() -> Result<()> {
            let db = Connection::open_in_memory()?;
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
            db.execute_batch(sql)?;

            let query = "SELECT x, y FROM foo ORDER BY x DESC";
            let bad_type: CustomResult<f64> =
                db.query_row_and_then(query, [], |row| row.get(1).map_err(CustomError::Sqlite));

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType(..)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<String> =
                db.query_row_and_then(query, [], |row| row.get(3).map_err(CustomError::Sqlite));

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<String> =
                db.query_row_and_then(query, [], |_| Err(CustomError::SomeError));

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
            Ok(())
        }
    }

    #[test]
    fn test_dynamic() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, 'hello');
                       END;";
        db.execute_batch(sql)?;

        db.query_row("SELECT * FROM foo", [], |r| {
            assert_eq!(2, r.as_ref().column_count());
            Ok(())
        })
    }
    #[test]
    fn test_dyn_box() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;
        let b: Box<dyn ToSql> = Box::new(5);
        db.execute("INSERT INTO foo VALUES(?1)", [b])?;
        db.query_row("SELECT x FROM foo", [], |r| {
            assert_eq!(5, r.get_unwrap::<_, i32>(0));
            Ok(())
        })
    }

    #[test]
    fn test_params() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.query_row(
            "SELECT
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20,
            ?21, ?22, ?23, ?24, ?25, ?26, ?27, ?28, ?29, ?30,
            ?31, ?32, ?33, ?34;",
            params![
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1,
            ],
            |r| {
                assert_eq!(1, r.get_unwrap::<_, i32>(0));
                Ok(())
            },
        )
    }

    #[test]
    #[cfg(not(feature = "extra_check"))]
    fn test_alter_table() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE x(t);")?;
        // `execute_batch` should be used but `execute` should also work
        db.execute("ALTER TABLE x RENAME TO y;", [])?;
        Ok(())
    }

    #[test]
    fn test_batch() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let sql = r"
             CREATE TABLE tbl1 (col);
             CREATE TABLE tbl2 (col);
             ";
        let batch = Batch::new(&db, sql);
        for stmt in batch {
            let mut stmt = stmt?;
            stmt.execute([])?;
        }
        Ok(())
    }

    #[test]
    #[cfg(feature = "modern_sqlite")]
    fn test_returning() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo(x INTEGER PRIMARY KEY)")?;
        let row_id = db.one_column::<i64>("INSERT INTO foo DEFAULT VALUES RETURNING ROWID")?;
        assert_eq!(row_id, 1);
        Ok(())
    }

    #[test]
    fn test_cache_flush() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.cache_flush()
    }

    #[test]
    pub fn db_readonly() -> Result<()> {
        let db = Connection::open_in_memory()?;
        assert!(!db.is_readonly(MAIN_DB)?);
        Ok(())
    }
}
