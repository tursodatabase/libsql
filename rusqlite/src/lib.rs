//! Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to expose
//! an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres).
//!
//! ```rust
//! extern crate rusqlite;
//! extern crate time;
//!
//! use time::Timespec;
//! use rusqlite::Connection;
//!
//! #[derive(Debug)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     time_created: Timespec,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let conn = Connection::open_in_memory().unwrap();
//!
//!     conn.execute("CREATE TABLE person (
//!                   id              INTEGER PRIMARY KEY,
//!                   name            TEXT NOT NULL,
//!                   time_created    TEXT NOT NULL,
//!                   data            BLOB
//!                   )", &[]).unwrap();
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_string(),
//!         time_created: time::get_time(),
//!         data: None
//!     };
//!     conn.execute("INSERT INTO person (name, time_created, data)
//!                   VALUES (?1, ?2, ?3)",
//!                  &[&me.name, &me.time_created, &me.data]).unwrap();
//!
//!     let mut stmt = conn.prepare("SELECT id, name, time_created, data FROM person").unwrap();
//!     let mut person_iter = stmt.query_map(&[], |row| {
//!         Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             time_created: row.get(2),
//!             data: row.get(3)
//!         }
//!     }).unwrap();
//!
//!     for person in person_iter {
//!         println!("Found person {:?}", person.unwrap());
//!     }
//! }
//! ```

extern crate libc;
extern crate libsqlite3_sys as ffi;
extern crate lru_cache;
#[macro_use]
extern crate bitflags;
#[cfg(test)]
#[macro_use]
extern crate lazy_static;

use std::default::Default;
use std::convert;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::fmt;
use std::path::{Path, PathBuf};
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::result;
use std::str;
use libc::{c_int, c_char, c_void};

use types::{ToSql, ToSqlOutput, FromSql, ValueRef};
use error::{error_from_sqlite_code, error_from_handle};
use raw_statement::RawStatement;
use cache::StatementCache;

pub use transaction::{SqliteTransaction, SqliteTransactionBehavior, DropBehavior, Savepoint,
                      Transaction, TransactionBehavior};
pub use error::{SqliteError, Error};
pub use cache::CachedStatement;

#[cfg(feature = "load_extension")]
pub use load_extension_guard::{SqliteLoadExtensionGuard, LoadExtensionGuard};

pub mod types;
mod transaction;
mod cache;
mod named_params;
mod error;
mod convenient;
mod raw_statement;
#[cfg(feature = "load_extension")]mod load_extension_guard;
#[cfg(feature = "trace")]pub mod trace;
#[cfg(feature = "backup")]pub mod backup;
#[cfg(feature = "functions")]pub mod functions;
#[cfg(feature = "blob")]pub mod blob;

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
    let s = try!(p.to_str().ok_or(Error::InvalidPath(p.to_owned())));
    str_to_cstring(s)
}

/// Name for a database within a SQLite connection.
pub enum DatabaseName<'a> {
    /// The main database.
    Main,

    /// The temporary database (e.g., any "CREATE TEMPORARY TABLE" tables).
    Temp,

    /// A database that has been attached via "ATTACH DATABASE ...".
    Attached(&'a str),
}

// Currently DatabaseName is only used by the backup and blob mods, so hide this (private)
// impl to avoid dead code warnings.
#[cfg(any(feature = "backup", feature = "blob"))]
impl<'a> DatabaseName<'a> {
    fn to_cstring(&self) -> Result<CString> {
        use self::DatabaseName::{Main, Temp, Attached};
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

impl Connection {
    /// Open a new connection to a SQLite database.
    ///
    /// `Connection::open(path)` is equivalent to `Connection::open_with_flags(path,
    /// SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE)`.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible string or if the
    /// underlying SQLite open call fails.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Connection> {
        let flags = Default::default();
        Connection::open_with_flags(path, flags)
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub fn open_in_memory() -> Result<Connection> {
        let flags = Default::default();
        Connection::open_in_memory_with_flags(flags)
    }

    /// Open a new connection to a SQLite database.
    ///
    /// Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `path` cannot be converted to a C-compatible string or if the
    /// underlying SQLite open call fails.
    pub fn open_with_flags<P: AsRef<Path>>(path: P, flags: OpenFlags) -> Result<Connection> {
        let c_path = try!(path_to_cstring(path.as_ref()));
        InnerConnection::open_with_flags(&c_path, flags).map(|db| {
            Connection {
                db: RefCell::new(db),
                cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
                path: Some(path.as_ref().to_path_buf()),
            }
        })
    }

    /// Open a new connection to an in-memory SQLite database.
    ///
    /// Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite open call fails.
    pub fn open_in_memory_with_flags(flags: OpenFlags) -> Result<Connection> {
        let c_memory = try!(str_to_cstring(":memory:"));
        InnerConnection::open_with_flags(&c_memory, flags).map(|db| {
            Connection {
                db: RefCell::new(db),
                cache: StatementCache::with_capacity(STATEMENT_CACHE_DEFAULT_CAPACITY),
                path: None,
            }
        })
    }

    /// Convenience method to run multiple SQL statements (that cannot take any parameters).
    ///
    /// Uses [sqlite3_exec](http://www.sqlite.org/c3ref/exec.html) under the hood.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn create_tables(conn: &Connection) -> Result<()> {
    ///     conn.execute_batch("BEGIN;
    ///                         CREATE TABLE foo(x INTEGER);
    ///                         CREATE TABLE bar(y TEXT);
    ///                         COMMIT;")
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn execute_batch(&self, sql: &str) -> Result<()> {
        self.db.borrow_mut().execute_batch(sql)
    }

    /// Convenience method to prepare and execute a single SQL statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
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
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn execute(&self, sql: &str, params: &[&ToSql]) -> Result<c_int> {
        self.prepare(sql).and_then(|mut stmt| stmt.execute(params))
    }

    /// Get the SQLite rowid of the most recent successful INSERT.
    ///
    /// Uses [sqlite3_last_insert_rowid](https://www.sqlite.org/c3ref/last_insert_rowid.html) under
    /// the hood.
    pub fn last_insert_rowid(&self) -> i64 {
        self.db.borrow_mut().last_insert_rowid()
    }

    /// Convenience method to execute a query that is expected to return a single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row("SELECT value FROM preferences WHERE name='locale'", &[], |row| {
    ///         row.get(0)
    ///     })
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn query_row<T, F>(&self, sql: &str, params: &[&ToSql], f: F) -> Result<T>
        where F: FnOnce(&Row) -> T
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query(params));

        rows.get_expected_row().map(|r| f(&r))
    }

    /// Convenience method to execute a query that is expected to return a single row,
    /// and execute a mapping via `f` on that returned row with the possibility of failure.
    /// The `Result` type of `f` must implement `std::convert::From<Error>`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_and_then("SELECT value FROM preferences WHERE name='locale'",
    ///                             &[],
    ///                             |row| {
    ///         row.get_checked(0)
    ///     })
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are ignored.
    ///
    /// # Failure
    ///
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn query_row_and_then<T, E, F>(&self,
                                       sql: &str,
                                       params: &[&ToSql],
                                       f: F)
                                       -> result::Result<T, E>
        where F: FnOnce(&Row) -> result::Result<T, E>,
              E: convert::From<Error>
    {
        let mut stmt = try!(self.prepare(sql));
        let mut rows = try!(stmt.query(params));

        rows.get_expected_row().map_err(E::from).and_then(|r| f(&r))
    }

    /// Convenience method to execute a query that is expected to return a single row.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Result,Connection};
    /// fn preferred_locale(conn: &Connection) -> Result<String> {
    ///     conn.query_row_safe("SELECT value FROM preferences WHERE name='locale'", &[], |row| {
    ///         row.get(0)
    ///     })
    /// }
    /// ```
    ///
    /// If the query returns more than one row, all rows except the first are ignored.
    ///
    /// ## Deprecated
    ///
    /// This method should be considered deprecated. Use `query_row` instead, which now
    /// does exactly the same thing.
    #[deprecated(since = "0.1.0", note = "Use query_row instead")]
    pub fn query_row_safe<T, F>(&self, sql: &str, params: &[&ToSql], f: F) -> Result<T>
        where F: FnOnce(&Row) -> T
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
    /// Will return `Err` if `sql` cannot be converted to a C-compatible string or if the
    /// underlying SQLite call fails.
    pub fn prepare<'a>(&'a self, sql: &str) -> Result<Statement<'a>> {
        self.db.borrow_mut().prepare(self, sql)
    }

    /// Close the SQLite connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for `Connection` except
    /// that it returns any error encountered to the caller.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn close(self) -> Result<()> {
        self.flush_prepared_statement_cache();
        let mut db = self.db.borrow_mut();
        db.close()
    }

    /// Enable loading of SQLite extensions. Strongly consider using `LoadExtensionGuard`
    /// instead of this function.
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

    /// Load the SQLite extension at `dylib_path`. `dylib_path` is passed through to
    /// `sqlite3_load_extension`, which may attempt OS-specific modifications if the file
    /// cannot be loaded directly.
    ///
    /// If `entry_point` is `None`, SQLite will attempt to find the entry point. If it is not
    /// `None`, the entry point will be passed through to `sqlite3_load_extension`.
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
    pub fn load_extension<P: AsRef<Path>>(&self,
                                          dylib_path: P,
                                          entry_point: Option<&str>)
                                          -> Result<()> {
        self.db.borrow_mut().load_extension(dylib_path.as_ref(), entry_point)
    }

    /// Get access to the underlying SQLite database connection handle.
    ///
    /// # Warning
    ///
    /// You should not need to use this function. If you do need to, please [open an issue
    /// on the rusqlite repository](https://github.com/jgallagher/rusqlite/issues) and describe
    /// your use case. This function is unsafe because it gives you raw access to the SQLite
    /// connection, and what you do with it could impact the safety of this `Connection`.
    pub unsafe fn handle(&self) -> *mut ffi::Struct_sqlite3 {
        self.db.borrow().db()
    }

    fn decode_result(&self, code: c_int) -> Result<()> {
        self.db.borrow_mut().decode_result(code)
    }

    fn changes(&self) -> c_int {
        self.db.borrow_mut().changes()
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
    db: *mut ffi::Struct_sqlite3,
}

/// Old name for `OpenFlags`. `SqliteOpenFlags` is deprecated.
#[deprecated(since = "0.6.0", note = "Use OpenFlags instead")]
pub type SqliteOpenFlags = OpenFlags;

bitflags! {
    #[doc = "Flags for opening SQLite database connections."]
    #[doc = "See [sqlite3_open_v2](http://www.sqlite.org/c3ref/open.html) for details."]
    #[repr(C)]
    pub flags OpenFlags: ::libc::c_int {
        const SQLITE_OPEN_READ_ONLY     = 0x00000001,
        const SQLITE_OPEN_READ_WRITE    = 0x00000002,
        const SQLITE_OPEN_CREATE        = 0x00000004,
        const SQLITE_OPEN_URI           = 0x00000040,
        const SQLITE_OPEN_MEMORY        = 0x00000080,
        const SQLITE_OPEN_NO_MUTEX      = 0x00008000,
        const SQLITE_OPEN_FULL_MUTEX    = 0x00010000,
        const SQLITE_OPEN_SHARED_CACHE  = 0x00020000,
        const SQLITE_OPEN_PRIVATE_CACHE = 0x00040000,
    }
}

impl Default for OpenFlags {
    fn default() -> OpenFlags {
        SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NO_MUTEX | SQLITE_OPEN_URI
    }
}

impl InnerConnection {
    fn open_with_flags(c_path: &CString, flags: OpenFlags) -> Result<InnerConnection> {
        unsafe {
            // Before opening the database, we need to check that SQLite hasn't been
            // compiled or configured to be in single-threaded mode. If it has, we're
            // exposing a very unsafe API to Rust, so refuse to open connections at all.
            // Unfortunately, the check for this is quite gross. sqlite3_threadsafe() only
            // returns how SQLite was _compiled_; there is no public API to check whether
            // someone called sqlite3_config() to set single-threaded mode. We can cheat
            // by trying to allocate a mutex, though; in single-threaded mode due to
            // compilation settings, the magic value 8 is returned (see the definition of
            // sqlite3_mutex_alloc at https://github.com/mackyle/sqlite/blob/master/src/mutex.h);
            // in single-threaded mode due to sqlite3_config(), the magic value 8 is also
            // returned (see the definition of noopMutexAlloc at
            // https://github.com/mackyle/sqlite/blob/master/src/mutex_noop.c).
            const SQLITE_SINGLETHREADED_MUTEX_MAGIC: usize = 8;
            let mutex_ptr = ffi::sqlite3_mutex_alloc(0);
            let is_singlethreaded = mutex_ptr as usize == SQLITE_SINGLETHREADED_MUTEX_MAGIC;
            ffi::sqlite3_mutex_free(mutex_ptr);
            if is_singlethreaded {
                return Err(Error::SqliteSingleThreadedMode);
            }

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

            Ok(InnerConnection { db: db })
        }
    }

    fn db(&self) -> *mut ffi::Struct_sqlite3 {
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
        unsafe {
            let r = ffi::sqlite3_close(self.db());
            self.db = ptr::null_mut();
            self.decode_result(r)
        }
    }

    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let c_sql = try!(str_to_cstring(sql));
        unsafe {
            let r = ffi::sqlite3_exec(self.db(),
                                      c_sql.as_ptr(),
                                      None,
                                      ptr::null_mut(),
                                      ptr::null_mut());
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
                ffi::sqlite3_load_extension(self.db,
                                            dylib_str.as_ptr(),
                                            c_entry.as_ptr(),
                                            &mut errmsg)
            } else {
                ffi::sqlite3_load_extension(self.db, dylib_str.as_ptr(), ptr::null(), &mut errmsg)
            };
            if r == ffi::SQLITE_OK {
                Ok(())
            } else {
                let message = errmsg_to_string(&*errmsg);
                ffi::sqlite3_free(errmsg as *mut libc::c_void);
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
        let r = unsafe {
            let len_with_nul = (sql.len() + 1) as c_int;
            ffi::sqlite3_prepare_v2(self.db(),
                                    c_sql.as_ptr(),
                                    len_with_nul,
                                    &mut c_stmt,
                                    ptr::null_mut())
        };
        self.decode_result(r).map(|_| Statement::new(conn, RawStatement::new(c_stmt)))
    }

    fn changes(&mut self) -> c_int {
        unsafe { ffi::sqlite3_changes(self.db()) }
    }
}

impl Drop for InnerConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.close();
    }
}

/// Old name for `Statement`. `SqliteStatement` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Statement instead")]
pub type SqliteStatement<'conn> = Statement<'conn>;

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    stmt: RawStatement,
}

impl<'conn> Statement<'conn> {
    fn new(conn: &Connection, stmt: RawStatement) -> Statement {
        Statement {
            conn: conn,
            stmt: stmt,
        }
    }

    /// Get all the column names in the result set of the prepared statement.
    pub fn column_names(&self) -> Vec<&str> {
        let n = self.column_count();
        let mut cols = Vec::with_capacity(n as usize);
        for i in 0..n {
            let slice = self.stmt.column_name(i);
            let s = str::from_utf8(slice.to_bytes()).unwrap();
            cols.push(s);
        }
        cols
    }

    /// Return the number of columns in the result set returned by the prepared statement.
    pub fn column_count(&self) -> i32 {
        self.stmt.column_count()
    }

    /// Returns the column index in the result set for a given column name.
    ///
    /// If there is no AS clause then the name of the column is unspecified and may change from one
    /// release of SQLite to the next.
    ///
    /// # Failure
    ///
    /// Will return an `Error::InvalidColumnName` when there is no column with the specified `name`.
    pub fn column_index(&self, name: &str) -> Result<i32> {
        let bytes = name.as_bytes();
        let n = self.column_count();
        for i in 0..n {
            if bytes == self.stmt.column_name(i).to_bytes() {
                return Ok(i);
            }
        }
        Err(Error::InvalidColumnName(String::from(name)))
    }

    /// Execute the prepared statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn update_rows(conn: &Connection) -> Result<()> {
    ///     let mut stmt = try!(conn.prepare("UPDATE foo SET bar = 'baz' WHERE qux = ?"));
    ///
    ///     try!(stmt.execute(&[&1i32]));
    ///     try!(stmt.execute(&[&2i32]));
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails, the executed statement returns rows (in
    /// which case `query` should be used instead), or the underling SQLite call fails.
    pub fn execute(&mut self, params: &[&ToSql]) -> Result<c_int> {
        try!(self.bind_parameters(params));
        self.execute_()
    }

    fn execute_(&mut self) -> Result<c_int> {
        let r = self.stmt.step();
        self.stmt.reset();
        match r {
            ffi::SQLITE_DONE => {
                if self.column_count() == 0 {
                    Ok(self.conn.changes())
                } else {
                    Err(Error::ExecuteReturnedResults)
                }
            }
            ffi::SQLITE_ROW => Err(Error::ExecuteReturnedResults),
            _ => Err(self.conn.decode_result(r).unwrap_err()),
        }
    }

    /// Execute the prepared statement, returning a handle to the resulting rows.
    ///
    /// Due to lifetime restricts, the rows handle returned by `query` does not
    /// implement the `Iterator` trait. Consider using `query_map` or `query_and_then`
    /// instead, which do.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people"));
    ///     let mut rows = try!(stmt.query(&[]));
    ///
    ///     let mut names = Vec::new();
    ///     while let Some(result_row) = rows.next() {
    ///         let row = try!(result_row);
    ///         names.push(row.get(0));
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query<'a>(&'a mut self, params: &[&ToSql]) -> Result<Rows<'a>> {
        try!(self.bind_parameters(params));
        Ok(Rows::new(self))
    }

    /// Executes the prepared statement and maps a function over the resulting rows, returning
    /// an iterator over the mapped function results.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{Connection, Result};
    /// fn get_names(conn: &Connection) -> Result<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people"));
    ///     let rows = try!(stmt.query_map(&[], |row| row.get(0)));
    ///
    ///     let mut names = Vec::new();
    ///     for name_result in rows {
    ///         names.push(try!(name_result));
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_map<'a, T, F>(&'a mut self, params: &[&ToSql], f: F) -> Result<MappedRows<'a, F>>
        where F: FnMut(&Row) -> T
    {
        let row_iter = try!(self.query(params));

        Ok(MappedRows {
            rows: row_iter,
            map: f,
        })
    }

    /// Executes the prepared statement and maps a function over the resulting
    /// rows, where the function returns a `Result` with `Error` type implementing
    /// `std::convert::From<Error>` (so errors can be unified).
    ///
    /// # Failure
    ///
    /// Will return `Err` if binding parameters fails.
    pub fn query_and_then<'a, T, E, F>(&'a mut self,
                                       params: &[&ToSql],
                                       f: F)
                                       -> Result<AndThenRows<'a, F>>
        where E: convert::From<Error>,
              F: FnMut(&Row) -> result::Result<T, E>
    {
        let row_iter = try!(self.query(params));

        Ok(AndThenRows {
            rows: row_iter,
            map: f,
        })
    }

    /// Consumes the statement.
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows callers to see any errors
    /// that occur.
    ///
    /// # Failure
    ///
    /// Will return `Err` if the underlying SQLite call fails.
    pub fn finalize(mut self) -> Result<()> {
        self.finalize_()
    }

    fn bind_parameter(&self, param: &ToSql, col: c_int) -> Result<()> {
        let value = try!(param.to_sql());

        let ptr = unsafe { self.stmt.ptr() };
        let value = match value {
            ToSqlOutput::Borrowed(v) => v,
            ToSqlOutput::Owned(ref v) => ValueRef::from(v),

            #[cfg(feature = "blob")]
            ToSqlOutput::ZeroBlob(len) => {
                return self.conn
                    .decode_result(unsafe { ffi::sqlite3_bind_zeroblob(ptr, col, len) });
            }
        };
        self.conn.decode_result(match value {
            ValueRef::Null => unsafe { ffi::sqlite3_bind_null(ptr, col) },
            ValueRef::Integer(i) => unsafe { ffi::sqlite3_bind_int64(ptr, col, i) },
            ValueRef::Real(r) => unsafe { ffi::sqlite3_bind_double(ptr, col, r) },
            ValueRef::Text(ref s) => unsafe {
                let length = s.len();
                if length > ::std::i32::MAX as usize {
                    ffi::SQLITE_TOOBIG
                } else {
                    let c_str = try!(str_to_cstring(s));
                    let destructor = if length > 0 {
                        ffi::SQLITE_TRANSIENT()
                    } else {
                        ffi::SQLITE_STATIC()
                    };
                    ffi::sqlite3_bind_text(ptr, col, c_str.as_ptr(), length as c_int, destructor)
                }
            },
            ValueRef::Blob(ref b) => unsafe {
                let length = b.len();
                if length > ::std::i32::MAX as usize {
                    ffi::SQLITE_TOOBIG
                } else if length == 0 {
                    ffi::sqlite3_bind_zeroblob(ptr, col, 0)
                } else {
                    ffi::sqlite3_bind_blob(ptr,
                                           col,
                                           b.as_ptr() as *const c_void,
                                           length as c_int,
                                           ffi::SQLITE_TRANSIENT())
                }
            },
        })
    }

    fn bind_parameters(&mut self, params: &[&ToSql]) -> Result<()> {
        assert!(params.len() as c_int == self.stmt.bind_parameter_count(),
                "incorrect number of parameters to query(): expected {}, got {}",
                self.stmt.bind_parameter_count(),
                params.len());

        for (i, p) in params.iter().enumerate() {
            try!(self.bind_parameter(*p, (i + 1) as c_int));
        }

        Ok(())
    }

    fn finalize_(&mut self) -> Result<()> {
        let mut stmt = RawStatement::new(ptr::null_mut());
        mem::swap(&mut stmt, &mut self.stmt);
        self.conn.decode_result(stmt.finalize())
    }
}

impl<'conn> Into<RawStatement> for Statement<'conn> {
    fn into(mut self) -> RawStatement {
        let mut stmt = RawStatement::new(ptr::null_mut());
        mem::swap(&mut stmt, &mut self.stmt);
        stmt
    }
}

impl<'conn> fmt::Debug for Statement<'conn> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let sql = str::from_utf8(self.stmt.sql().to_bytes());
        f.debug_struct("Statement")
            .field("conn", self.conn)
            .field("stmt", &self.stmt)
            .field("sql", &sql)
            .finish()
    }
}

impl<'conn> Drop for Statement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.finalize_();
    }
}

/// An iterator over the mapped resulting rows of a query.
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<'stmt, T, F> Iterator for MappedRows<'stmt, F>
    where F: FnMut(&Row) -> T
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows.next().map(|row_result| row_result.map(|row| (map)(&row)))
    }
}

/// An iterator over the mapped resulting rows of a query, with an Error type
/// unifying with Error.
pub struct AndThenRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<'stmt, T, E, F> Iterator for AndThenRows<'stmt, F>
    where E: convert::From<Error>,
          F: FnMut(&Row) -> result::Result<T, E>
{
    type Item = result::Result<T, E>;

    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows.next().map(|row_result| {
            row_result.map_err(E::from)
                .and_then(|row| (map)(&row))
        })
    }
}

/// Old name for `Rows`. `SqliteRows` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Rows instead")]
pub type SqliteRows<'stmt> = Rows<'stmt>;

/// An handle for the resulting rows of a query.
pub struct Rows<'stmt> {
    stmt: Option<&'stmt Statement<'stmt>>,
}

impl<'stmt> Rows<'stmt> {
    fn new(stmt: &'stmt Statement<'stmt>) -> Rows<'stmt> {
        Rows { stmt: Some(stmt) }
    }

    fn get_expected_row<'a>(&'a mut self) -> Result<Row<'a, 'stmt>> {
        match self.next() {
            Some(row) => row,
            None => Err(Error::QueryReturnedNoRows),
        }
    }

    fn reset(&mut self) {
        if let Some(stmt) = self.stmt.take() {
            stmt.stmt.reset();
        }
    }

    /// Attempt to get the next row from the query. Returns `Some(Ok(Row))` if there
    /// is another row, `Some(Err(...))` if there was an error getting the next
    /// row, and `None` if all rows have been retrieved.
    ///
    /// ## Note
    ///
    /// This interface is not compatible with Rust's `Iterator` trait, because the
    /// lifetime of the returned row is tied to the lifetime of `self`. This is a
    /// "streaming iterator". For a more natural interface, consider using `query_map`
    /// or `query_and_then` instead, which return types that implement `Iterator`.
    pub fn next<'a>(&'a mut self) -> Option<Result<Row<'a, 'stmt>>> {
        self.stmt.and_then(|stmt| {
            match stmt.stmt.step() {
                ffi::SQLITE_ROW => {
                    Some(Ok(Row {
                        stmt: stmt,
                        phantom: PhantomData,
                    }))
                }
                ffi::SQLITE_DONE => {
                    self.reset();
                    None
                }
                code => {
                    self.reset();
                    Some(Err(stmt.conn.decode_result(code).unwrap_err()))
                }
            }
        })
    }
}

impl<'stmt> Drop for Rows<'stmt> {
    fn drop(&mut self) {
        self.reset();
    }
}

/// Old name for `Row`. `SqliteRow` is deprecated.
#[deprecated(since = "0.6.0", note = "Use Row instead")]
pub type SqliteRow<'a, 'stmt> = Row<'a, 'stmt>;

/// A single result row of a query.
pub struct Row<'a, 'stmt> {
    stmt: &'stmt Statement<'stmt>,
    phantom: PhantomData<&'a ()>,
}

impl<'a, 'stmt> Row<'a, 'stmt> {
    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Panics if the underlying SQLite column type is not a valid type as a source for `T`.
    ///
    /// Panics if `idx` is outside the range of columns in the returned query.
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> T {
        self.get_checked(idx).unwrap()
    }

    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnType` if the underlying SQLite column
    /// type is not a valid type as a source for `T`.
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid column range
    /// for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column name
    /// for this row.
    pub fn get_checked<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        let idx = try!(idx.idx(self.stmt));
        let value = unsafe { ValueRef::new(&self.stmt.stmt, idx) };
        FromSql::column_result(value)
    }

    /// Return the number of columns in the current row.
    pub fn column_count(&self) -> i32 {
        self.stmt.column_count()
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement) -> Result<i32>;
}

impl RowIndex for i32 {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Result<i32> {
        if *self < 0 || *self >= stmt.column_count() {
            Err(Error::InvalidColumnIndex(*self))
        } else {
            Ok(*self)
        }
    }
}

impl<'a> RowIndex for &'a str {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Result<i32> {
        stmt.column_index(*self)
    }
}

impl<'a> ValueRef<'a> {
    unsafe fn new(stmt: &RawStatement, col: c_int) -> ValueRef {
        use std::slice::from_raw_parts;

        let raw = stmt.ptr();

        match stmt.column_type(col) {
            ffi::SQLITE_NULL => ValueRef::Null,
            ffi::SQLITE_INTEGER => ValueRef::Integer(ffi::sqlite3_column_int64(raw, col)),
            ffi::SQLITE_FLOAT => ValueRef::Real(ffi::sqlite3_column_double(raw, col)),
            ffi::SQLITE_TEXT => {
                let text = ffi::sqlite3_column_text(raw, col);
                assert!(!text.is_null(),
                        "unexpected SQLITE_TEXT column type with NULL data");
                let s = CStr::from_ptr(text as *const c_char);

                // sqlite3_column_text returns UTF8 data, so our unwrap here should be fine.
                let s = s.to_str().expect("sqlite3_column_text returned invalid UTF-8");
                ValueRef::Text(s)
            }
            ffi::SQLITE_BLOB => {
                let blob = ffi::sqlite3_column_blob(raw, col);

                let len = ffi::sqlite3_column_bytes(raw, col);
                assert!(len >= 0, "unexpected negative return from sqlite3_column_bytes");
                if len > 0 {
                    assert!(!blob.is_null(), "unexpected SQLITE_BLOB column type with NULL data");
                    ValueRef::Blob(from_raw_parts(blob as *const u8, len as usize))
                } else {
                    // The return value from sqlite3_column_blob() for a zero-length BLOB is a NULL pointer.
                    ValueRef::Blob(&[])
                }

            }
            _ => unreachable!("sqlite3_column_type returned invalid value"),
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    pub use super::*;
    use ffi;
    use self::tempdir::TempDir;
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
    #[cfg_attr(rustfmt, rustfmt_skip)]
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
    fn test_open_with_flags() {
        for bad_flags in &[OpenFlags::empty(),
                           SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_READ_WRITE,
                           SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_CREATE] {
            assert!(Connection::open_in_memory_with_flags(*bad_flags).is_err());
        }
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
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

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3").unwrap();

        assert!(db.execute_batch("INVALID SQL").is_err());
    }

    #[test]
    fn test_execute() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(1,
                   db.execute("INSERT INTO foo(x) VALUES (?)", &[&1i32]).unwrap());
        assert_eq!(1,
                   db.execute("INSERT INTO foo(x) VALUES (?)", &[&2i32]).unwrap());

        assert_eq!(3i32,
                   db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0)).unwrap());
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

        let mut query = db.prepare("SELECT x FROM foo WHERE x < ? ORDER BY x DESC").unwrap();
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
    #[cfg_attr(rustfmt, rustfmt_skip)]
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
        let results: Result<Vec<String>> = query.query_map(&[], |row| row.get(1))
            .unwrap()
            .collect();

        assert_eq!(results.unwrap().concat(), "hello, world!");
    }

    #[test]
    #[cfg_attr(rustfmt, rustfmt_skip)]
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

        assert_eq!(10i64,
                   db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0))
                   .unwrap());

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
        db.execute_batch("CREATE TABLE foo(x INTEGER PRIMARY KEY)").unwrap();
        db.execute_batch("INSERT INTO foo DEFAULT VALUES").unwrap();

        assert_eq!(db.last_insert_rowid(), 1);

        let mut stmt = db.prepare("INSERT INTO foo DEFAULT VALUES").unwrap();
        for _ in 0i32..9 {
            stmt.execute(&[]).unwrap();
        }
        assert_eq!(db.last_insert_rowid(), 10);
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
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x NOT NULL)").unwrap();

        let result = db.execute("INSERT INTO foo (x) VALUES (NULL)", &[]);
        assert!(result.is_err());

        match result.unwrap_err() {
            Error::SqliteFailure(err, _) => {
                assert_eq!(err.code, ffi::ErrorCode::ConstraintViolation);

                // extended error codes for constraints were added in SQLite 3.7.16; if we're
                // running on a version at least that new, check for the extended code
                let version = unsafe { ffi::sqlite3_libversion_number() };
                if version >= 3007016 {
                    assert_eq!(err.extended_code, ffi::SQLITE_CONSTRAINT_NOTNULL)
                }
            }
            err => panic!("Unexpected error {}", err),
        }
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
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
            let results: Result<Vec<String>> = query.query_and_then(&[],
                                                                          |row| row.get_checked(1))
                .unwrap()
                .collect();

            assert_eq!(results.unwrap().concat(), "hello, world!");
        }

        #[test]
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
            let bad_type: Result<Vec<f64>> = query.query_and_then(&[], |row| row.get_checked(1))
                .unwrap()
                .collect();

            match bad_type.unwrap_err() {
                Error::InvalidColumnType => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: Result<Vec<String>> = query.query_and_then(&[], |row| row.get_checked(3))
                .unwrap()
                .collect();

            match bad_idx.unwrap_err() {
                Error::InvalidColumnIndex(_) => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
            let results: CustomResult<Vec<String>> = query.query_and_then(&[], |row| {
                row.get_checked(1)
                .map_err(CustomError::Sqlite)
            })
            .unwrap()
                .collect();

            assert_eq!(results.unwrap().concat(), "hello, world!");
        }

        #[test]
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
            let bad_type: CustomResult<Vec<f64>> = query.query_and_then(&[], |row| {
                row.get_checked(1)
                .map_err(CustomError::Sqlite)
            })
            .unwrap()
                .collect();

            match bad_type.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnType) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<Vec<String>> = query.query_and_then(&[], |row| {
                row.get_checked(3)
                .map_err(CustomError::Sqlite)
            })
            .unwrap()
                .collect();

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<Vec<String>> = query.query_and_then(&[], |_| {
                Err(CustomError::SomeError)
            })
            .unwrap()
                .collect();

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
        #[cfg_attr(rustfmt, rustfmt_skip)]
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
                CustomError::Sqlite(Error::InvalidColumnType) => (),
                err => panic!("Unexpected error {}", err),
            }

            let bad_idx: CustomResult<String> = db.query_row_and_then(query, &[], |row| {
                row.get_checked(3).map_err(CustomError::Sqlite)
            });

            match bad_idx.unwrap_err() {
                CustomError::Sqlite(Error::InvalidColumnIndex(_)) => (),
                err => panic!("Unexpected error {}", err),
            }

            let non_sqlite_err: CustomResult<String> = db.query_row_and_then(query, &[], |_| {
                Err(CustomError::SomeError)
            });

            match non_sqlite_err.unwrap_err() {
                CustomError::SomeError => (),
                err => panic!("Unexpected error {}", err),
            }
        }

        #[test]
        #[cfg_attr(rustfmt, rustfmt_skip)]
        fn test_dynamic() {
            let db = checked_memory_handle();
            let sql = "BEGIN;
                       CREATE TABLE foo(x INTEGER, y TEXT);
                       INSERT INTO foo VALUES(4, \"hello\");
                       END;";
            db.execute_batch(sql).unwrap();

            db.query_row("SELECT * FROM foo", &[], |r| assert_eq!(2, r.column_count())).unwrap();
        }
    }
}
