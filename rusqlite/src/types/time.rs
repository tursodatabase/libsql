extern crate time;

use libc::c_int;
use {Error, Result};
use types::{FromSql, ToSql};

use ffi;
use ffi::sqlite3_stmt;
use ffi::sqlite3_column_type;

const SQLITE_DATETIME_FMT: &'static str = "%Y-%m-%d %H:%M:%S";
const JULIAN_DAY: f64 = 2440587.5; // 1970-01-01 00:00:00 is JD 2440587.5
const DAY_IN_SECONDS: f64 = 86400.0;

impl ToSql for time::Timespec {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let time_str = time::at_utc(*self).strftime(SQLITE_DATETIME_FMT).unwrap().to_string();
        time_str.bind_parameter(stmt, col)
    }
}

impl FromSql for time::Timespec {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<time::Timespec> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let col_str = FromSql::column_result(stmt, col);
                col_str.and_then(|txt: String| {
                    match time::strptime(&txt, SQLITE_DATETIME_FMT) {
                        Ok(tm) => Ok(tm.to_timespec()),
                        Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
                    }
                })
            }
            ffi::SQLITE_INTEGER => Ok(time::Timespec::new(ffi::sqlite3_column_int64(stmt, col), 0)),
            ffi::SQLITE_FLOAT => {
                let mut jd = ffi::sqlite3_column_double(stmt, col);
                jd -= JULIAN_DAY;
                jd *= DAY_IN_SECONDS;
                let ns = jd.fract() * 10f64.powi(9);
                Ok(time::Timespec::new(jd as i64, ns as i32))
            }
            _ => Err(Error::InvalidColumnType),
        }
    }

    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true // to avoid double check
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
        db.execute("INSERT INTO foo(t, i) VALUES (?, ?)", &[&ts, &ts.sec]).unwrap();
        db.execute("UPDATE foo SET f = julianday(t)", &[]).unwrap();

        let from: time::Timespec = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(from, ts);
        let from: time::Timespec = db.query_row("SELECT i FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(from, ts);
        // `Timespec { sec: 9999, nsec: 999994039 }` vs `Timespec{ sec: 10000, nsec: 0 }`
        let from: time::Timespec = db.query_row("SELECT f FROM foo", &[], |r| r.get(0)).unwrap();
        assert!((from.sec - ts.sec).abs() <= 1);
    }
}
