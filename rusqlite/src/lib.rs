#![feature(globs)]
#![feature(unsafe_destructor)]
#![feature(macro_rules)]

extern crate libc;

use std::mem;
use std::ptr;
use std::fmt;
use std::rc::{Rc};
use std::cell::{RefCell, Cell};
use std::c_str::{CString};
use libc::{c_int, c_void, c_char};

use types::{ToSql, FromSql};

pub use transaction::{SqliteTransaction};
pub use transaction::{SqliteTransactionMode,
                      SqliteTransactionDeferred,
                      SqliteTransactionImmediate,
                      SqliteTransactionExclusive};

pub mod types;
mod transaction;
#[allow(dead_code,non_snake_case,non_camel_case_types)] pub mod ffi;

pub type SqliteResult<T> = Result<T, SqliteError>;

unsafe fn errmsg_to_string(errmsg: *const c_char) -> String {
    let c_str = CString::new(errmsg, false);
    c_str.as_str().unwrap_or("Invalid error message encoding").to_string()
}

#[deriving(Show)]
pub struct SqliteError {
    pub code: c_int,
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

pub struct SqliteConnection {
    db: RefCell<InnerSqliteConnection>,
}

impl SqliteConnection {
    pub fn open(path: &str) -> SqliteResult<SqliteConnection> {
        let flags = SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_CREATE;
        SqliteConnection::open_with_flags(path, flags)
    }

    pub fn open_with_flags(path: &str, flags: SqliteOpenFlags) -> SqliteResult<SqliteConnection> {
        InnerSqliteConnection::open_with_flags(path, flags).map(|db| {
            SqliteConnection{ db: RefCell::new(db) }
        })
    }

    pub fn transaction<'a>(&'a self) -> SqliteResult<SqliteTransaction<'a>> {
        SqliteTransaction::new(self, SqliteTransactionDeferred)
    }

    pub fn transaction_with_mode<'a>(&'a self, mode: SqliteTransactionMode)
            -> SqliteResult<SqliteTransaction<'a>> {
        SqliteTransaction::new(self, mode)
    }

    pub fn execute_batch(&self, sql: &str) -> SqliteResult<()> {
        self.db.borrow_mut().execute_batch(sql)
    }

    pub fn execute(&self, sql: &str, params: &[&ToSql]) -> SqliteResult<uint> {
        self.prepare(sql).and_then(|mut stmt| stmt.execute(params))
    }

    pub fn last_insert_rowid(&self) -> i64 {
        self.db.borrow_mut().last_insert_rowid()
    }

    pub fn query_row<T>(&self, sql: &str, params: &[&ToSql],
                        f: |SqliteResult<SqliteRow>| -> T) -> T {
        f(self.prepare(sql).unwrap().query(params).unwrap().next().unwrap())
    }

    pub fn prepare<'a>(&'a self, sql: &str) -> SqliteResult<SqliteStatement<'a>> {
        self.db.borrow_mut().prepare(self, sql)
    }

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
    #[repr(C)] flags SqliteOpenFlags: c_int {
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
                    ffi::sqlite3_close(db);
                    SqliteError::from_handle(db, r)
                };

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

pub struct SqliteStatement<'conn> {
    conn: &'conn SqliteConnection,
    stmt: *mut ffi::sqlite3_stmt,
    needs_reset: bool,
}

impl<'conn> SqliteStatement<'conn> {
    fn new(conn: &SqliteConnection, stmt: *mut ffi::sqlite3_stmt) -> SqliteStatement {
        SqliteStatement{ conn: conn, stmt: stmt, needs_reset: false }
    }

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

impl<'stmt> Iterator<SqliteResult<SqliteRow<'stmt>>> for SqliteRows<'stmt> {
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

pub struct SqliteRow<'stmt> {
    stmt: &'stmt SqliteStatement<'stmt>,
    current_row: Rc<Cell<c_int>>,
    row_idx: c_int,
}

impl<'stmt> SqliteRow<'stmt> {
    pub fn get<T: FromSql>(&self, idx: c_int) -> T {
        self.get_opt(idx).unwrap()
    }

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
            Ok(FromSql::column_result(self.stmt.stmt, idx))
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

        assert!(SqliteConnection::open_with_flags(
                "/invalid", SQLITE_OPEN_READ_ONLY|SQLITE_OPEN_MEMORY).is_ok());
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

        assert_eq!(3i32, db.query_row("SELECT SUM(x) FROM foo", [], |r| r.unwrap().get(0)));
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
        let mut rows = stmt.query([]).unwrap();
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
            stmt.execute([]).unwrap();
        }
        assert_eq!(db.last_insert_rowid(), 10);
    }
}
