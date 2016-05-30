extern crate time;

use libc::c_int;
use {Error, Result};
use types::{FromSql, ToSql, ValueRef};

use ffi::sqlite3_stmt;

const SQLITE_DATETIME_FMT: &'static str = "%Y-%m-%d %H:%M:%S";

impl ToSql for time::Timespec {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let time_str = time::at_utc(*self).strftime(SQLITE_DATETIME_FMT).unwrap().to_string();
        time_str.bind_parameter(stmt, col)
    }
}

impl FromSql for time::Timespec {
    fn column_result(value: ValueRef) -> Result<Self> {
        value.as_str().and_then(|s| match time::strptime(s, SQLITE_DATETIME_FMT) {
            Ok(tm) => Ok(tm.to_timespec()),
            Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
        })
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
