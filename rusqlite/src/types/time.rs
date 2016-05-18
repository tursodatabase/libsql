extern crate time;

use libc::c_int;
use {Error, Result};
use types::{FromSql, ToSql};

use ffi::sqlite3_stmt;

const SQLITE_DATETIME_FMT: &'static str = "%Y-%m-%d %H:%M:%S";

impl ToSql for time::Timespec {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let time_str = time::at_utc(*self).strftime(SQLITE_DATETIME_FMT).unwrap().to_string();
        time_str.bind_parameter(stmt, col)
    }
}

impl FromSql for time::Timespec {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<time::Timespec> {
        let s = try!(String::column_result(stmt, col));
        match time::strptime(&s, SQLITE_DATETIME_FMT) {
            Ok(tm) => Ok(tm.to_timespec()),
            Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
        }
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> Result<()> {
        String::column_has_valid_sqlite_type(stmt, col)
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use super::time;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT)").unwrap();
        db
    }

    #[test]
    fn test_timespec() {
        let db = checked_memory_handle();

        let ts = time::Timespec {
            sec: 10_000,
            nsec: 0,
        };
        db.execute("INSERT INTO foo(t) VALUES (?)", &[&ts]).unwrap();

        let from: time::Timespec = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(from, ts);
    }
}
