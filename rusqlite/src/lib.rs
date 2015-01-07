//! Rusqlite is an ergonomic wrapper for using SQLite from Rust. It attempts to expose
//! an interface similar to [rust-postgres](https://github.com/sfackler/rust-postgres).
//!
//! ```rust
//! extern crate rusqlite;
//! extern crate time;
//!
//! use time::Timespec;
//! use rusqlite::SqliteConnection;
//!
//! #[derive(Show)]
//! struct Person {
//!     id: i32,
//!     name: String,
//!     time_created: Timespec,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let conn = SqliteConnection::open(":memory:").unwrap();
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
//!                   VALUES ($1, $2, $3)",
//!                  &[&me.name, &me.time_created, &me.data]).unwrap();
//!
//!     let mut stmt = conn.prepare("SELECT id, name, time_created, data FROM person").unwrap();
//!     for row in stmt.query(&[]).unwrap().map(|row| row.unwrap()) {
//!         let person = Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             time_created: row.get(2),
//!             data: row.get(3)
//!         };
//!         println!("Found person {}", person);
//!     }
//! }
//! ```
#![feature(unsafe_destructor)]

extern crate libc;

use std::mem;
use std::ptr;
use std::fmt;
use std::rc::{Rc};
use std::cell::{RefCell, Cell};
use std::c_str::{CString, ToCStr};
use libc::{c_int, c_void, c_char};

use types::{ToSql, FromSql};

pub use transaction::{SqliteTransaction};
pub use transaction::{SqliteTransactionBehavior,
                      SqliteTransactionDeferred,
                      SqliteTransactionImmediate,
                      SqliteTransactionExclusive};

pub mod types;
mod transaction;

/// Automatically generated FFI bindings (via [bindgen](https://github.com/crabtw/rust-bindgen)).
#[allow(dead_code,non_snake_case,non_camel_case_types)] pub mod ffi;

/// A typedef of the result returned by many methods.
pub type SqliteResult<T> = Result<T, SqliteError>;

unsafe fn errmsg_to_string(errmsg: *const c_char) -> String {
    let c_str = CString::new(errmsg, false);
    c_str.as_str().unwrap_or("Invalid error message encoding").to_string()
}

/// Encompasses an error result from a call to the SQLite C API.
#[derive(Show)]
pub struct SqliteError {
    /// The error code returned by a SQLite C API call. See [SQLite Result
    /// Codes](http://www.sqlite.org/rescode.html) for details.
    pub code: c_int,

    /// The error message provided by [sqlite3_errmsg](http://www.sqlite.org/c3ref/errcode.html),
    /// if possible, or a generic error message based on `code` otherwise.
    pub message: String,
}

impl SqliteError {
    fn from_handle(db: *mut ffi::Struct_sqlite3, code: c_int) -> SqliteError {
        let message = if db.is_null() {
            ffi::code_to_str(code).to_string()
        } else {
            unsafe { errmsg_to_string(ffi::sqlite3_errmsg(db)) }
        };
        SqliteError{ code: code, message: message }
    }
}

/// A connection to a SQLite database.
///
/// ## Warning
///
/// Note that despite the fact that most `SqliteConnection` methods take an immutable reference to
/// `self`, `SqliteConnection` is NOT threadsafe, and using it from multiple threads may result in
/// runtime panics or data races. The SQLite connection handle has at least two pieces of internal
/// state (the last insertion ID and the last error message) that rusqlite uses, but wrapping these
/// APIs in a safe way from Rust would be too restrictive (for example, you would not be able to
/// prepare multiple statements at the same time).
pub struct SqliteConnection {
    db: RefCell<InnerSqliteConnection>,
}

impl SqliteConnection {
    /// Open a new connection to a SQLite database.
    ///
    /// Use the special path `:memory:` to create an in-memory database.
    /// `SqliteConnection::open(path)` is equivalent to `SqliteConnection::open_with_flags(path,
    /// SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE)`.
    pub fn open(path: &str) -> SqliteResult<SqliteConnection> {
        let flags = SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE;
        SqliteConnection::open_with_flags(path, flags)
    }

    /// Open a new connection to a SQLite database.
    ///
    /// Use the special path `:memory:` to create an in-memory database. See [Opening A New
    /// Database Connection](http://www.sqlite.org/c3ref/open.html) for a description of valid
    /// flag combinations.
    pub fn open_with_flags(path: &str, flags: SqliteOpenFlags) -> SqliteResult<SqliteConnection> {
        InnerSqliteConnection::open_with_flags(path, flags).map(|db| {
            SqliteConnection{ db: RefCell::new(db) }
        })
    }

    /// Begin a new transaction with the default behavior (DEFERRED).
    ///
    /// The transaction defaults to rolling back when it is dropped. If you want the transaction to
    /// commit, you must call `commit` or `set_commit`.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// # fn do_queries_part_1(conn: &SqliteConnection) -> SqliteResult<()> { Ok(()) }
    /// # fn do_queries_part_2(conn: &SqliteConnection) -> SqliteResult<()> { Ok(()) }
    /// fn perform_queries(conn: &SqliteConnection) -> SqliteResult<()> {
    ///     let tx = try!(conn.transaction());
    ///
    ///     try!(do_queries_part_1(conn)); // tx causes rollback if this fails
    ///     try!(do_queries_part_2(conn)); // tx causes rollback if this fails
    ///
    ///     tx.commit()
    /// }
    /// ```
    pub fn transaction<'a>(&'a self) -> SqliteResult<SqliteTransaction<'a>> {
        SqliteTransaction::new(self, SqliteTransactionDeferred)
    }

    /// Begin a new transaction with a specified behavior.
    ///
    /// See `transaction`.
    pub fn transaction_with_behavior<'a>(&'a self, behavior: SqliteTransactionBehavior)
            -> SqliteResult<SqliteTransaction<'a>> {
        SqliteTransaction::new(self, behavior)
    }

    /// Convenience method to run multiple SQL statements (that cannot take any parameters).
    ///
    /// Uses [sqlite3_exec](http://www.sqlite.org/c3ref/exec.html) under the hood.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn create_tables(conn: &SqliteConnection) -> SqliteResult<()> {
    ///     conn.execute_batch("BEGIN;
    ///                         CREATE TABLE foo(x INTEGER);
    ///                         CREATE TABLE bar(y TEXT);
    ///                         COMMIT;")
    /// }
    /// ```
    pub fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
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
    /// # use rusqlite::{SqliteConnection};
    /// fn update_rows(conn: &SqliteConnection) {
    ///     match conn.execute("UPDATE foo SET bar = 'baz' WHERE qux = ?", &[&1i32]) {
    ///         Ok(updated) => println!("{} rows were updated", updated),
    ///         Err(err) => println!("update failed: {}", err),
    ///     }
    /// }
    /// ```
    pub fn execute(&self, sql: &str, params: &[&ToSql]) -> SqliteResult<uint> {
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
    /// # use rusqlite::{SqliteConnection};
    /// fn preferred_locale(conn: &SqliteConnection) -> String {
    ///     conn.query_row("SELECT value FROM preferences WHERE name='locale'", &[], |row| {
    ///         row.get(0)
    ///     })
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Panics if:
    ///
    ///   * Preparing the query fails.
    ///   * Running the query fails (i.e., calling `query` on the prepared statement).
    ///   * The query does not successfully return at least one row.
    ///
    /// If the query returns more than one row, all rows except the first are ignored.
    pub fn query_row<T, F>(&self, sql: &str, params: &[&ToSql], f: F) -> T
                           where F: FnOnce(SqliteRow) -> T {
        let mut stmt = self.prepare(sql).unwrap();
        let mut rows = stmt.query(params).unwrap();
        f(rows.next().expect("Query did not return a row").unwrap())
    }

    /// Prepare a SQL statement for execution.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn insert_new_people(conn: &SqliteConnection) -> SqliteResult<()> {
    ///     let mut stmt = try!(conn.prepare("INSERT INTO People (name) VALUES (?)"));
    ///     try!(stmt.execute(&[&"Joe Smith"]));
    ///     try!(stmt.execute(&[&"Bob Jones"]));
    ///     Ok(())
    /// }
    /// ```
    pub fn prepare<'a>(&'a self, sql: &str) -> SqliteResult<SqliteStatement<'a>> {
        self.db.borrow_mut().prepare(self, sql)
    }

    /// Close the SQLite connection.
    ///
    /// This is functionally equivalent to the `Drop` implementation for `SqliteConnection` except
    /// that it returns any error encountered to the caller.
    pub fn close(self) -> SqliteResult<()> {
        self.db.borrow_mut().close()
    }

    fn decode_result(&self, code: c_int) -> SqliteResult<()> {
        self.db.borrow_mut().decode_result(code)
    }

    fn changes(&self) -> uint {
        self.db.borrow_mut().changes()
    }
}

impl fmt::Show for SqliteConnection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqliteConnection()")
    }
}

struct InnerSqliteConnection {
    db: *mut ffi::Struct_sqlite3,
}

bitflags! {
    #[doc = "Flags for opening SQLite database connections."]
    #[doc = "See [sqlite3_open_v2](http://www.sqlite.org/c3ref/open.html) for details."]
    #[repr(C)]
    flags SqliteOpenFlags: c_int {
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

impl InnerSqliteConnection {
    fn open_with_flags(path: &str, flags: SqliteOpenFlags) -> SqliteResult<InnerSqliteConnection> {
        path.with_c_str(|c_path| unsafe {
            let mut db: *mut ffi::sqlite3 = mem::uninitialized();
            let r = ffi::sqlite3_open_v2(c_path, &mut db, flags.bits(), ptr::null());
            if r != ffi::SQLITE_OK {
                let e = if db.is_null() {
                    SqliteError{ code: r,
                                 message: ffi::code_to_str(r).to_string() }
                } else {
                    let e = SqliteError::from_handle(db, r);
                    ffi::sqlite3_close(db);
                    e
                };

                return Err(e);
            }
            let r = ffi::sqlite3_busy_timeout(db, 5000);
            if r != ffi::SQLITE_OK {
                let e = SqliteError::from_handle(db, r);
                ffi::sqlite3_close(db);
                return Err(e);
            }
            Ok(InnerSqliteConnection{ db: db })
        })
    }

    fn decode_result(&mut self, code: c_int) -> SqliteResult<()> {
        if code == ffi::SQLITE_OK {
            Ok(())
        } else {
            Err(SqliteError::from_handle(self.db, code))
        }
    }

    fn close(&mut self) -> SqliteResult<()> {
        let r = unsafe { ffi::sqlite3_close(self.db) };
        self.db = ptr::null_mut();
        self.decode_result(r)
    }

    fn execute_batch(&mut self, sql: &str) -> SqliteResult<()> {
        sql.with_c_str(|c_sql| unsafe {
            let mut errmsg: *mut c_char = mem::uninitialized();
            let r = ffi::sqlite3_exec(self.db, c_sql, None, ptr::null_mut(), &mut errmsg);
            if r == ffi::SQLITE_OK {
                Ok(())
            } else {
                let message = errmsg_to_string(&*errmsg);
                ffi::sqlite3_free(errmsg as *mut c_void);
                Err(SqliteError{ code: r, message: message })
            }
        })
    }

    fn last_insert_rowid(&self) -> i64 {
        unsafe {
            ffi::sqlite3_last_insert_rowid(self.db)
        }
    }

    fn prepare<'a>(&mut self,
                   conn: &'a SqliteConnection,
                   sql: &str) -> SqliteResult<SqliteStatement<'a>> {
        let mut c_stmt: *mut ffi::sqlite3_stmt = unsafe { mem::uninitialized() };
        let r = sql.with_c_str(|c_sql| unsafe {
            let len_with_nul = (sql.len() + 1) as c_int;
            ffi::sqlite3_prepare_v2(self.db, c_sql, len_with_nul, &mut c_stmt, ptr::null_mut())
        });
        self.decode_result(r).map(|_| {
            SqliteStatement::new(conn, c_stmt)
        })
    }

    fn changes(&mut self) -> uint {
        unsafe{ ffi::sqlite3_changes(self.db) as uint }
    }
}

impl Drop for InnerSqliteConnection {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.close();
    }
}

/// A prepared statement.
pub struct SqliteStatement<'conn> {
    conn: &'conn SqliteConnection,
    stmt: *mut ffi::sqlite3_stmt,
    needs_reset: bool,
}

impl<'conn> SqliteStatement<'conn> {
    fn new(conn: &SqliteConnection, stmt: *mut ffi::sqlite3_stmt) -> SqliteStatement {
        SqliteStatement{ conn: conn, stmt: stmt, needs_reset: false }
    }

    /// Execute the prepared statement.
    ///
    /// On success, returns the number of rows that were changed or inserted or deleted (via
    /// `sqlite3_changes`).
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn update_rows(conn: &SqliteConnection) -> SqliteResult<()> {
    ///     let mut stmt = try!(conn.prepare("UPDATE foo SET bar = 'baz' WHERE qux = ?"));
    ///
    ///     try!(stmt.execute(&[&1i32]));
    ///     try!(stmt.execute(&[&2i32]));
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn execute(&mut self, params: &[&ToSql]) -> SqliteResult<uint> {
        self.reset_if_needed();

        unsafe {
            assert_eq!(params.len() as c_int, ffi::sqlite3_bind_parameter_count(self.stmt));

            for (i, p) in params.iter().enumerate() {
                try!(self.conn.decode_result(p.bind_parameter(self.stmt, (i + 1) as c_int)));
            }

            self.needs_reset = true;
            let r = ffi::sqlite3_step(self.stmt);
            match r {
                ffi::SQLITE_DONE => Ok(self.conn.changes()),
                ffi::SQLITE_ROW => Err(SqliteError{ code: r,
                    message: "Unexpected row result - did you mean to call query?".to_string() }),
                _ => Err(self.conn.decode_result(r).unwrap_err()),
            }
        }
    }

    /// Execute the prepared statement, returning an iterator over the resulting rows.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn get_names(conn: &SqliteConnection) -> SqliteResult<Vec<String>> {
    ///     let mut stmt = try!(conn.prepare("SELECT name FROM people"));
    ///     let mut rows = try!(stmt.query(&[]));
    ///
    ///     let mut names = Vec::new();
    ///     for result_row in rows {
    ///         let row = try!(result_row);
    ///         names.push(row.get(0));
    ///     }
    ///
    ///     Ok(names)
    /// }
    /// ```
    pub fn query<'a>(&'a mut self, params: &[&ToSql]) -> SqliteResult<SqliteRows<'a>> {
        self.reset_if_needed();

        unsafe {
            assert_eq!(params.len() as c_int, ffi::sqlite3_bind_parameter_count(self.stmt));

            for (i, p) in params.iter().enumerate() {
                try!(self.conn.decode_result(p.bind_parameter(self.stmt, (i + 1) as c_int)));
            }

            self.needs_reset = true;
            Ok(SqliteRows::new(self))
        }
    }

    /// Consumes the statement.
    ///
    /// Functionally equivalent to the `Drop` implementation, but allows callers to see any errors
    /// that occur.
    pub fn finalize(mut self) -> SqliteResult<()> {
        self.finalize_()
    }

    fn reset_if_needed(&mut self) {
        if self.needs_reset {
            unsafe { ffi::sqlite3_reset(self.stmt); };
            self.needs_reset = false;
        }
    }

    fn finalize_(&mut self) -> SqliteResult<()> {
        let r = unsafe { ffi::sqlite3_finalize(self.stmt) };
        self.stmt = ptr::null_mut();
        self.conn.decode_result(r)
    }
}

impl<'conn> fmt::Show for SqliteStatement<'conn> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Statement( conn: {}, stmt: {} )", self.conn, self.stmt)
    }
}

#[unsafe_destructor]
impl<'conn> Drop for SqliteStatement<'conn> {
    #[allow(unused_must_use)]
    fn drop(&mut self) {
        self.finalize_();
    }
}

/// An iterator over the resulting rows of a query.
///
/// ## Warning
///
/// Due to the way SQLite returns result rows of a query, it is not safe to attempt to get values
/// from a row after it has become stale (i.e., `next()` has been called again on the `SqliteRows`
/// iterator). For example:
///
/// ```rust,no_run
/// # use rusqlite::{SqliteConnection, SqliteResult};
/// fn bad_function_will_panic(conn: &SqliteConnection) -> SqliteResult<i64> {
///     let mut stmt = try!(conn.prepare("SELECT id FROM my_table"));
///     let mut rows = try!(stmt.query(&[]));
///
///     let row0 = try!(rows.next().unwrap());
///     // row 0 is value now...
///
///     let row1 = try!(rows.next().unwrap());
///     // row 0 is now STALE, and row 1 is valid
///
///     let my_id = row0.get(0); // WILL PANIC because row 0 is stale
///     Ok(my_id)
/// }
/// ```
///
/// Please note that this means some of the methods on `Iterator` are not useful, such as `collect`
/// (which would result in a collection of rows, only the last of which can safely be used) and
/// `min`/`max` (which could return a stale row unless the last row happened to be the min or max,
/// respectively).
///
/// This problem could be solved by changing the signature of `next` to tie the lifetime of the
/// returned row to the lifetime of (a mutable reference to) the result rows handle, but this would
/// no longer implement `Iterator`, and therefore you would lose access to the majority of
/// functions which are useful (such as support for `for ... in ...` looping, `map`, `filter`,
/// etc.).
pub struct SqliteRows<'stmt> {
    stmt: &'stmt SqliteStatement<'stmt>,
    current_row: Rc<Cell<c_int>>,
    failed: bool,
}

impl<'stmt> SqliteRows<'stmt> {
    fn new(stmt: &'stmt SqliteStatement<'stmt>) -> SqliteRows<'stmt> {
        SqliteRows{ stmt: stmt, current_row: Rc::new(Cell::new(0)), failed: false }
    }
}

impl<'stmt> Iterator for SqliteRows<'stmt> {
    type Item = SqliteResult<SqliteRow<'stmt>>;

    fn next(&mut self) -> Option<SqliteResult<SqliteRow<'stmt>>> {
        if self.failed {
            return None;
        }
        match unsafe { ffi::sqlite3_step(self.stmt.stmt) } {
            ffi::SQLITE_ROW => {
                let current_row = self.current_row.get() + 1;
                self.current_row.set(current_row);
                Some(Ok(SqliteRow{
                    stmt: self.stmt,
                    current_row: self.current_row.clone(),
                    row_idx: current_row,
                }))
            },
            ffi::SQLITE_DONE => None,
            code => {
                self.failed = true;
                Some(Err(self.stmt.conn.decode_result(code).unwrap_err()))
            }
        }
    }
}

/// A single result row of a query.
pub struct SqliteRow<'stmt> {
    stmt: &'stmt SqliteStatement<'stmt>,
    current_row: Rc<Cell<c_int>>,
    row_idx: c_int,
}

impl<'stmt> SqliteRow<'stmt> {
    /// Get the value of a particular column of the result row.
    ///
    /// Note that `SqliteRow` can panic at runtime if you use it incorrectly. When you are
    /// retrieving the rows of a query, a row becomes stale once you have requested the next row,
    /// and the values can no longer be retrieved. In general (when using looping over the rows,
    /// for example) this isn't an issue, but it means you cannot do something like this:
    ///
    /// ```rust,no_run
    /// # use rusqlite::{SqliteConnection, SqliteResult};
    /// fn bad_function_will_panic(conn: &SqliteConnection) -> SqliteResult<i64> {
    ///     let mut stmt = try!(conn.prepare("SELECT id FROM my_table"));
    ///     let mut rows = try!(stmt.query(&[]));
    ///
    ///     let row0 = try!(rows.next().unwrap());
    ///     // row 0 is value now...
    ///
    ///     let row1 = try!(rows.next().unwrap());
    ///     // row 0 is now STALE, and row 1 is valid
    ///
    ///     let my_id = row0.get(0); // WILL PANIC because row 0 is stale
    ///     Ok(my_id)
    /// }
    /// ```
    ///
    /// ## Failure
    ///
    /// Panics if `idx` is outside the range of columns in the returned query or if this row
    /// is stale.
    pub fn get<T: FromSql>(&self, idx: c_int) -> T {
        self.get_opt(idx).unwrap()
    }

    /// Attempt to get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Returns a `SQLITE_MISUSE`-coded `SqliteError` if `idx` is outside the valid column range
    /// for this row or if this row is stale.
    pub fn get_opt<T: FromSql>(&self, idx: c_int) -> SqliteResult<T> {
        if self.row_idx != self.current_row.get() {
            return Err(SqliteError{ code: ffi::SQLITE_MISUSE,
                message: "Cannot get values from a row after advancing to next row".to_string() });
        }
        unsafe {
            if idx < 0 || idx >= ffi::sqlite3_column_count(self.stmt.stmt) {
                return Err(SqliteError{ code: ffi::SQLITE_MISUSE,
                    message: "Invalid column index".to_string() });
            }
            FromSql::column_result(self.stmt.stmt, idx)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn checked_memory_handle() -> SqliteConnection {
        SqliteConnection::open(":memory:").unwrap()
    }

    #[test]
    fn test_open() {
        assert!(SqliteConnection::open(":memory:").is_ok());

        let db = checked_memory_handle();
        assert!(db.close().is_ok());
    }

    #[test]
    fn test_open_with_flags() {
        for bad_flags in [
            SqliteOpenFlags::empty(),
            SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_READ_WRITE,
            SQLITE_OPEN_READ_ONLY | SQLITE_OPEN_CREATE,
        ].iter() {
            assert!(SqliteConnection::open_with_flags(":memory:", *bad_flags).is_err());
        }

        assert!(SqliteConnection::open_with_flags(
                "file::memory:", SQLITE_OPEN_READ_ONLY|SQLITE_OPEN_URI).is_ok());
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

        db.execute_batch("UPDATE foo SET x = 3 WHERE x < 3").unwrap();

        assert!(db.execute_batch("INVALID SQL").is_err());
    }

    #[test]
    fn test_execute() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();

        assert_eq!(db.execute("INSERT INTO foo(x) VALUES (?)", &[&1i32]).unwrap(), 1);
        assert_eq!(db.execute("INSERT INTO foo(x) VALUES (?)", &[&2i32]).unwrap(), 1);

        assert_eq!(3i32, db.query_row("SELECT SUM(x) FROM foo", &[], |r| r.get(0)));
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
            let rows = query.query(&[&4i32]).unwrap();
            let v: Vec<i32> = rows.map(|r| r.unwrap().get(0)).collect();

            assert_eq!(v.as_slice(), [3i32, 2, 1].as_slice());
        }

        {
            let rows = query.query(&[&3i32]).unwrap();
            let v: Vec<i32> = rows.map(|r| r.unwrap().get(0)).collect();
            assert_eq!(v.as_slice(), [2i32, 1].as_slice());
        }
    }

    #[test]
    fn test_prepare_failures() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER);").unwrap();

        let err = db.prepare("SELECT * FROM does_not_exist").unwrap_err();
        assert!(err.message.as_slice().contains("does_not_exist"));
    }

    #[test]
    fn test_row_expiration() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER)").unwrap();
        db.execute_batch("INSERT INTO foo(x) VALUES(1)").unwrap();
        db.execute_batch("INSERT INTO foo(x) VALUES(2)").unwrap();

        let mut stmt = db.prepare("SELECT x FROM foo ORDER BY x").unwrap();
        let mut rows = stmt.query(&[]).unwrap();
        let first = rows.next().unwrap().unwrap();
        let second = rows.next().unwrap().unwrap();

        assert_eq!(2i32, second.get(0));

        let result = first.get_opt::<i32>(0);
        assert!(result.unwrap_err().message.as_slice().contains("advancing to next row"));
    }

    #[test]
    fn test_last_insert_rowid() {
        let db = checked_memory_handle();
        db.execute_batch("CREATE TABLE foo(x INTEGER PRIMARY KEY)").unwrap();
        db.execute_batch("INSERT INTO foo DEFAULT VALUES").unwrap();

        assert_eq!(db.last_insert_rowid(), 1);

        let mut stmt = db.prepare("INSERT INTO foo DEFAULT VALUES").unwrap();
        for _ in range(0i, 9) {
            stmt.execute(&[]).unwrap();
        }
        assert_eq!(db.last_insert_rowid(), 10);
    }
}
