//! Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to
//! expose an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres).
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
//!                   id              INTEGER PRIMARY KEY,
//!                   name            TEXT NOT NULL,
//!                   data            BLOB
//!                   )",
//!         [],
//!     )?;
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         data: None,
//!     };
//!     conn.execute(
//!         "INSERT INTO person (name, data) VALUES (?1, ?2)",
//!         params![me.name, me.data],
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

pub use libsqlite3_sys as ffi;

use std::cell::RefCell;
use std::convert;
use std::default::Default;
use std::ffi::{CStr, CString};
use std::fmt;
use std::os::raw::{c_char, c_int};

use std::path::{Path, PathBuf};
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
#[cfg(feature = "hooks")]
pub use crate::hooks::Action;
#[cfg(feature = "load_extension")]
pub use crate::load_extension_guard::LoadExtensionGuard;
pub use crate::params::{params_from_iter, Params, ParamsFromIter};
pub use crate::row::{AndThenRows, Map, MappedRows, Row, RowIndex, Rows};
pub use crate::statement::{Statement, StatementStatus};
pub use crate::transaction::{DropBehavior, Savepoint, Transaction, TransactionBehavior};
pub use crate::types::ToSql;
pub use crate::version::*;

#[macro_use]
mod error;

#[cfg(feature = "backup")]
pub mod backup;
#[cfg(feature = "blob")]
pub mod blob;
mod busy;
mod cache;
#[cfg(feature = "collation")]
mod collation;
mod column;
pub mod config;
#[cfg(any(feature = "functions", feature = "vtab"))]
mod context;
#[cfg(feature = "functions")]
pub mod functions;
#[cfg(feature = "hooks")]
mod hooks;
mod inner_connection;
#[cfg(feature = "limits")]
pub mod limits;
#[cfg(feature = "load_extension")]
mod load_extension_guard;
mod params;
mod pragma;
mod raw_statement;
mod row;
#[cfg(feature = "session")]
pub mod session;
mod statement;
#[cfg(feature = "trace")]
pub mod trace;
mod transaction;
pub mod types;
mod unlock_notify;
mod version;
#[cfg(feature = "vtab")]
pub mod vtab;

pub(crate) mod util;
pub(crate) use util::SmallCString;

// Number of cached prepared statements we'll hold on to.
const STATEMENT_CACHE_DEFAULT_CAPACITY: usize = 16;
/// To be used when your statement has no [parameter][sqlite-varparam].
///
/// [sqlite-varparam]: https://sqlite.org/lang_expr.html#varparam
///
/// This is deprecated in favor of using an empty array literal.
#[deprecated = "Use an empty array instead; `stmt.execute(NO_PARAMS)` => `stmt.execute([])`"]
pub const NO_PARAMS: &[&dyn ToSql] = &[];

/// A macro making it more convenient to pass heterogeneous or long lists of
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
///     conn.execute("INSERT INTO person (name, age_in_years, data)
///                   VALUES (?1, ?2, ?3)",
///                  params![person.name, person.age_in_years, person.data])?;
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
///         named_params!{
///             ":name": person.name,
///             ":age": person.age_in_years,
///             ":data": person.data,
///         }
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
        (s.as_ptr() as *const c_char, ffi::SQLITE_TRANSIENT())
    } else {
        // Return a pointer guaranteed to live forever
        ("".as_ptr() as *const c_char, ffi::SQLITE_STATIC())
    };
    Ok((ptr, len, dtor_info))
}

// Helper to cast to c_int safely, returning the correct error type if the cast
// failed.
fn len_as_c_int(len: usize) -> Result<c_int> {
    if len >= (c_int::max_value() as usize) {
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
#[cfg(any(
    feature = "backup",
    feature = "blob",
    feature = "session",
    feature = "modern_sqlite"
))]
impl DatabaseName<'_> {
    #[inline]
    fn to_cstring(&self) -> Result<util::SmallCString> {
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
    path: Option<PathBuf>,
}

unsafe impl Send for Connection {}

impl Drop for Connection {
    #[inline]
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
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn open_my_db() -> Result<()> {
    ///     let path = "./my_db.db3";
    ///     let db = Connection::open(&path)?;
    ///     println!("{}", db.is_autocommit());
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible
    /// string or if the underlying SQLite open call fails.
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
        InnerConnection::open_with_flags(&c_path, flags, None).map(|db| Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: Some(path.as_ref().to_path_buf()),
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
        InnerConnection::open_with_flags(&c_path, flags, Some(&c_vfs)).map(|db| Connection {
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
    /// Will return `Err` if vfs` cannot be converted to a C-compatible
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
    ///     conn.execute_batch("BEGIN;
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
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?", [1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    ///
    /// ### With positional params of varying types
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection};
    /// fn update_rows(conn: &Connection) {
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?", [1i32]) {
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
    ///         rusqlite::named_params!{ ":name": "one" },
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

    /// Convenience method to prepare and execute a single SQL statement with
    /// named parameter(s).
    ///
    /// On success, returns the number of rows that were changed or inserted or
    /// deleted (via `sqlite3_changes`).
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string
    /// or if the underlying SQLite call fails.
    #[deprecated = "You can use `execute` with named params now."]
    pub fn execute_named(&self, sql: &str, params: &[(&str, &dyn ToSql)]) -> Result<usize> {
        // This function itself is deprecated, so it's fine
        #![allow(deprecated)]
        self.prepare(sql).and_then(|mut stmt| {
            stmt.check_no_tail()
                .and_then(|_| stmt.execute_named(params))
        })
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

    /// Convenience method to execute a query with named parameter(s) that is
    /// expected to return a single row.
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
    #[deprecated = "You can use `query_row` with named params now."]
    pub fn query_row_named<T, F>(&self, sql: &str, params: &[(&str, &dyn ToSql)], f: F) -> Result<T>
    where
        F: FnOnce(&Row<'_>) -> Result<T>,
    {
        self.query_row(sql, params, f)
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
        E: convert::From<Error>,
    {
        let mut stmt = self.prepare(sql)?;
        stmt.check_no_tail()?;
        let mut rows = stmt.query(params)?;

        rows.get_expected_row().map_err(E::from).and_then(|r| f(&r))
    }

    /// Prepare a SQL statement for execution.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn insert_new_people(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("INSERT INTO People (name) VALUES (?)")?;
    ///     stmt.execute(&["Joe Smith"])?;
    ///     stmt.execute(&["Bob Jones"])?;
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

    /// `feature = "load_extension"` Enable loading of SQLite extensions.
    /// Strongly consider using `LoadExtensionGuard` instead of this function.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// # use std::path::{Path};
    /// fn load_my_extension(conn: &Connection) -> Result<()> {
    ///     conn.load_extension_enable()?;
    ///     conn.load_extension(Path::new("my_sqlite_extension"), None)?;
    ///     conn.load_extension_disable()
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    #[inline]
    pub fn load_extension_enable(&self) -> Result<()> {
        self.db.borrow_mut().enable_load_extension(1)
    }

    /// `feature = "load_extension"` Disable loading of SQLite extensions.
    ///
    /// See `load_extension_enable` for an example.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    #[inline]
    pub fn load_extension_disable(&self) -> Result<()> {
        self.db.borrow_mut().enable_load_extension(0)
    }

    /// `feature = "load_extension"` Load the SQLite extension at `dylib_path`.
    /// `dylib_path` is passed through to `sqlite3_load_extension`, which may
    /// attempt OS-specific modifications if the file cannot be loaded directly.
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
    ///     let _guard = LoadExtensionGuard::new(conn)?;
    ///
    ///     conn.load_extension("my_sqlite_extension", None)
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    #[cfg(feature = "load_extension")]
    #[inline]
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
        let db_path = db_filename(db);
        let db = InnerConnection::new(db, false);
        Ok(Connection {
            db: RefCell::new(db),
            cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
            path: db_path,
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
        self.db.borrow_mut().decode_result(code)
    }

    /// Return the number of rows modified, inserted or deleted by the most
    /// recently completed INSERT, UPDATE or DELETE statement on the database
    /// connection.
    #[inline]
    fn changes(&self) -> usize {
        self.db.borrow_mut().changes()
    }

    /// Test for auto-commit mode.
    /// Autocommit mode is on by default.
    #[inline]
    pub fn is_autocommit(&self) -> bool {
        self.db.borrow().is_autocommit()
    }

    /// Determine if all associated prepared statements have been reset.
    #[inline]
    #[cfg(feature = "modern_sqlite")] // 3.8.6
    pub fn is_busy(&self) -> bool {
        self.db.borrow().is_busy()
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("path", &self.path)
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
    /// Flags for opening SQLite database connections.
    /// See [sqlite3_open_v2](http://www.sqlite.org/c3ref/open.html) for details.
    #[repr(C)]
    pub struct OpenFlags: ::std::os::raw::c_int {
        /// The database is opened in read-only mode.
        /// If the database does not already exist, an error is returned.
        const SQLITE_OPEN_READ_ONLY     = ffi::SQLITE_OPEN_READONLY;
        /// The database is opened for reading and writing if possible,
        /// or reading only if the file is write protected by the operating system.
        /// In either case the database must already exist, otherwise an error is returned.
        const SQLITE_OPEN_READ_WRITE    = ffi::SQLITE_OPEN_READWRITE;
        /// The database is created if it does not already exist
        const SQLITE_OPEN_CREATE        = ffi::SQLITE_OPEN_CREATE;
        /// The filename can be interpreted as a URI if this flag is set.
        const SQLITE_OPEN_URI           = 0x0000_0040;
        /// The database will be opened as an in-memory database.
        const SQLITE_OPEN_MEMORY        = 0x0000_0080;
        /// The new database connection will use the "multi-thread" threading mode.
        const SQLITE_OPEN_NO_MUTEX      = ffi::SQLITE_OPEN_NOMUTEX;
        /// The new database connection will use the "serialized" threading mode.
        const SQLITE_OPEN_FULL_MUTEX    = ffi::SQLITE_OPEN_FULLMUTEX;
        /// The database is opened shared cache enabled.
        const SQLITE_OPEN_SHARED_CACHE  = 0x0002_0000;
        /// The database is opened shared cache disabled.
        const SQLITE_OPEN_PRIVATE_CACHE = 0x0004_0000;
        /// The database filename is not allowed to be a symbolic link.
        const SQLITE_OPEN_NOFOLLOW = 0x0100_0000;
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
/// you may enounter memory errors or data corruption or any number of terrible
/// things that should not be possible when you're using Rust.
pub unsafe fn bypass_sqlite_initialization() {
    BYPASS_SQLITE_INIT.store(true, Ordering::Relaxed);
}

/// rusqlite performs a one-time check that the runtime SQLite version is at
/// least as new as the version of SQLite found when rusqlite was built.
/// Bypassing this check may be dangerous; e.g., if you use features of SQLite
/// that are not present in the runtime version.
///
/// # Safety
///
/// If you are sure the runtime version is compatible with the
/// build-time version for your usage, you can bypass the version check by
/// calling this function before your first connection attempt.
pub unsafe fn bypass_sqlite_version_check() {
    #[cfg(not(feature = "bundled"))]
    inner_connection::BYPASS_VERSION_CHECK.store(true, Ordering::Relaxed);
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

#[cfg(feature = "modern_sqlite")] // 3.7.10
unsafe fn db_filename(db: *mut ffi::sqlite3) -> Option<PathBuf> {
    let db_name = DatabaseName::Main.to_cstring().unwrap();
    let db_filename = ffi::sqlite3_db_filename(db, db_name.as_ptr());
    if db_filename.is_null() {
        None
    } else {
        CStr::from_ptr(db_filename).to_str().ok().map(PathBuf::from)
    }
}
#[cfg(not(feature = "modern_sqlite"))]
unsafe fn db_filename(_: *mut ffi::sqlite3) -> Option<PathBuf> {
    None
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
    #[allow(dead_code, unconditional_recursion)]
    fn ensure_send<T: Send>() {
        ensure_send::<Connection>();
        ensure_send::<InterruptHandle>();
    }

    #[allow(dead_code, unconditional_recursion)]
    fn ensure_sync<T: Sync>() {
        ensure_sync::<InterruptHandle>();
    }

    pub fn checked_memory_handle() -> Connection {
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

            tx1.execute("INSERT INTO foo VALUES(?1)", &[&1])?;
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
        let db = Connection::open(&path_string)?;
        let the_answer: Result<i64> = db.query_row("SELECT x FROM foo", [], |r| r.get(0));

        assert_eq!(42i64, the_answer?);
        Ok(())
    }

    #[test]
    fn test_open() {
        assert!(Connection::open_in_memory().is_ok());

        let db = checked_memory_handle();
        assert!(db.close().is_ok());
    }

    #[test]
    fn test_open_failure() {
        let filename = "no_such_file.db";
        let result = Connection::open_with_flags(filename, OpenFlags::SQLITE_OPEN_READ_ONLY);
        assert!(!result.is_ok());
        let err = result.err().unwrap();
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
        let the_answer: Result<i64> = db.query_row("SELECT x FROM foo", [], |r| r.get(0));

        assert_eq!(42i64, the_answer?);
        Ok(())
    }

    #[test]
    fn test_close_retry() -> Result<()> {
        let db = checked_memory_handle();

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
            assert!(Connection::open_in_memory_with_flags(*bad_flags).is_err());
        }
    }

    #[test]
    fn test_execute_batch() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3")?;

        assert!(db.execute_batch("INVALID SQL").is_err());
        Ok(())
    }

    #[test]
    fn test_execute() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)")?;

        assert_eq!(1, db.execute("INSERT INTO foo(x) VALUES (?)", [1i32])?);
        assert_eq!(1, db.execute("INSERT INTO foo(x) VALUES (?)", [2i32])?);

        assert_eq!(
            3i32,
            db.query_row::<i32, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    #[cfg(feature = "extra_check")]
    fn test_execute_select() {
        let db = checked_memory_handle();
        let err = db.execute("SELECT 1 WHERE 1 < ?", [1i32]).unwrap_err();
        if err != Error::ExecuteReturnedResults {
            panic!("Unexpected error: {}", err);
        }
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
        let db = checked_memory_handle();
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
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        assert_eq!(insert_stmt.execute(["hello".to_string()])?, 1);
        assert_eq!(insert_stmt.execute(["goodbye".to_string()])?, 1);
        assert_eq!(insert_stmt.execute([types::Null])?, 1);

        let mut update_stmt = db.prepare("UPDATE foo SET x=? WHERE x<?")?;
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 2);
        assert_eq!(update_stmt.execute([3i32, 3i32])?, 0);
        assert_eq!(update_stmt.execute([8i32, 8i32])?, 3);
        Ok(())
    }

    #[test]
    fn test_prepare_query() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let mut insert_stmt = db.prepare("INSERT INTO foo(x) VALUES(?)")?;
        assert_eq!(insert_stmt.execute([1i32])?, 1);
        assert_eq!(insert_stmt.execute([2i32])?, 1);
        assert_eq!(insert_stmt.execute([3i32])?, 1);

        let mut query = db.prepare("SELECT x FROM foo WHERE x < ? ORDER BY x DESC")?;
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
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER, y TEXT);
                   INSERT INTO foo VALUES(4, \"hello\");
                   INSERT INTO foo VALUES(3, \", \");
                   INSERT INTO foo VALUES(2, \"world\");
                   INSERT INTO foo VALUES(1, \"!\");
                   END;";
        db.execute_batch(sql)?;

        let mut query = db.prepare("SELECT x, y FROM foo ORDER BY x DESC")?;
        let results: Result<Vec<String>> = query.query([])?.map(|row| row.get(1)).collect();

        assert_eq!(results?.concat(), "hello, world!");
        Ok(())
    }

    #[test]
    fn test_query_row() -> Result<()> {
        let db = checked_memory_handle();
        let sql = "BEGIN;
                   CREATE TABLE foo(x INTEGER);
                   INSERT INTO foo VALUES(1);
                   INSERT INTO foo VALUES(2);
                   INSERT INTO foo VALUES(3);
                   INSERT INTO foo VALUES(4);
                   END;";
        db.execute_batch(sql)?;

        assert_eq!(
            10i64,
            db.query_row::<i64, _, _>("SELECT SUM(x) FROM foo", [], |r| r.get(0))?
        );

        let result: Result<i64> = db.query_row("SELECT x FROM foo WHERE x > 5", [], |r| r.get(0));
        match result.unwrap_err() {
            Error::QueryReturnedNoRows => (),
            err => panic!("Unexpected error {}", err),
        }

        let bad_query_result = db.query_row("NOT A PROPER QUERY; test123", [], |_| Ok(()));

        assert!(bad_query_result.is_err());
        Ok(())
    }

    #[test]
    fn test_optional() -> Result<()> {
        let db = checked_memory_handle();

        let result: Result<i64> = db.query_row("SELECT 1 WHERE 0 <> 0", [], |r| r.get(0));
        let result = result.optional();
        match result? {
            None => (),
            _ => panic!("Unexpected result"),
        }

        let result: Result<i64> = db.query_row("SELECT 1 WHERE 0 == 0", [], |r| r.get(0));
        let result = result.optional();
        match result? {
            Some(1) => (),
            _ => panic!("Unexpected result"),
        }

        let bad_query_result: Result<i64> = db.query_row("NOT A PROPER QUERY", [], |r| r.get(0));
        let bad_query_result = bad_query_result.optional();
        assert!(bad_query_result.is_err());
        Ok(())
    }

    #[test]
    fn test_pragma_query_row() -> Result<()> {
        let db = checked_memory_handle();

        assert_eq!(
            "memory",
            db.query_row::<String, _, _>("PRAGMA journal_mode", [], |r| r.get(0))?
        );
        assert_eq!(
            "off",
            db.query_row::<String, _, _>("PRAGMA journal_mode=off", [], |r| r.get(0))?
        );
        Ok(())
    }

    #[test]
    fn test_prepare_failures() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;

        let err = db.prepare("SELECT * FROM does_not_exist").unwrap_err();
        assert!(format!("{}", err).contains("does_not_exist"));
        Ok(())
    }

    #[test]
    fn test_last_insert_rowid() -> Result<()> {
        let db = checked_memory_handle();
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
    fn test_is_autocommit() {
        let db = checked_memory_handle();
        assert!(
            db.is_autocommit(),
            "autocommit expected to be active by default"
        );
    }

    #[test]
    #[cfg(feature = "modern_sqlite")]
    fn test_is_busy() -> Result<()> {
        let db = checked_memory_handle();
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
        let db = checked_memory_handle();
        let query = "SELECT 12345";
        let stmt = db.prepare(query)?;

        assert!(format!("{:?}", stmt).contains(query));
        Ok(())
    }

    #[test]
    fn test_notnull_constraint_error() -> Result<()> {
        // extended error codes for constraints were added in SQLite 3.7.16; if we're
        // running on our bundled version, we know the extended error code exists.
        #[cfg(feature = "modern_sqlite")]
        fn check_extended_code(extended_code: c_int) {
            assert_eq!(extended_code, ffi::SQLITE_CONSTRAINT_NOTNULL);
        }
        #[cfg(not(feature = "modern_sqlite"))]
        fn check_extended_code(_extended_code: c_int) {}

        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x NOT NULL)")?;

        let result = db.execute("INSERT INTO foo (x) VALUES (NULL)", []);
        assert!(result.is_err());

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

        assert!(version().contains(&format!("{}.{}.{}", major, minor, patch)));
    }

    #[test]
    #[cfg(feature = "functions")]
    fn test_interrupt() -> Result<()> {
        let db = checked_memory_handle();

        let interrupt_handle = db.get_interrupt_handle();

        db.create_scalar_function(
            "interrupt",
            0,
            crate::functions::FunctionFlags::default(),
            move |_| {
                interrupt_handle.interrupt();
                Ok(0)
            },
        )?;

        let mut stmt =
            db.prepare("SELECT interrupt() FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3)")?;

        let result: Result<Vec<i32>> = stmt.query([])?.map(|r| r.get(0)).collect();

        match result.unwrap_err() {
            Error::SqliteFailure(err, _) => {
                assert_eq!(err.code, ErrorCode::OperationInterrupted);
            }
            err => {
                panic!("Unexpected error {}", err);
            }
        }
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
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(i, x);")?;
        let vals = ["foobar", "1234", "qwerty"];
        let mut insert_stmt = db.prepare("INSERT INTO foo(i, x) VALUES(?, ?)")?;
        for (i, v) in vals.iter().enumerate() {
            let i_to_insert = i as i64;
            assert_eq!(insert_stmt.execute(params![i_to_insert, v])?, 1);
        }

        let mut query = db.prepare("SELECT i, x FROM foo")?;
        let mut rows = query.query([])?;

        while let Some(row) = rows.next()? {
            let i = row.get_raw(0).as_i64()?;
            let expect = vals[i as usize];
            let x = row.get_raw("x").as_str()?;
            assert_eq!(x, expect);
        }
        Ok(())
    }

    #[test]
    fn test_from_handle() -> Result<()> {
        let db = checked_memory_handle();
        let handle = unsafe { db.handle() };
        {
            let db = unsafe { Connection::from_handle(handle) }?;
            db.execute_batch("PRAGMA VACUUM")?;
        }
        db.close().unwrap();
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
                    CustomError::Sqlite(ref se) => write!(f, "my custom error: {}", se),
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       INSERT INTO foo VALUES(3, \", \");
                       INSERT INTO foo VALUES(2, \"world\");
                       INSERT INTO foo VALUES(1, \"!\");
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
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
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
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
        let db = checked_memory_handle();
        let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       END;";
        db.execute_batch(sql)?;

        db.query_row("SELECT * FROM foo", [], |r| {
            assert_eq!(2, r.column_count());
            Ok(())
        })
    }
    #[test]
    fn test_dyn_box() -> Result<()> {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);")?;
        let b: Box<dyn ToSql> = Box::new(5);
        db.execute("INSERT INTO foo VALUES(?)", [b])?;
        db.query_row("SELECT x FROM foo", [], |r| {
            assert_eq!(5, r.get_unwrap::<_, i32>(0));
            Ok(())
        })
    }

    #[test]
    fn test_params() -> Result<()> {
        let db = checked_memory_handle();
        db.query_row(
            "SELECT
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?;",
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
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE x(t);")?;
        // `execute_batch` should be used but `execute` should also work
        db.execute("ALTER TABLE x RENAME TO y;", [])?;
        Ok(())
    }

    #[test]
    fn test_batch() -> Result<()> {
        let db = checked_memory_handle();
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
}
