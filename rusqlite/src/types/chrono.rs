//! Convert most of the [Time Strings](http://sqlite.org/lang_datefunc.html) to chrono types.
extern crate chrono;

use std::borrow::Cow;

use self::chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, TimeZone, UTC, Local};
use libc::c_int;

use {Error, Result};
use types::{FromSql, ToSql, ValueRef};

use ffi::sqlite3_stmt;

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
impl ToSql for NaiveDate {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%d").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD" => ISO 8601 calendar date without timezone.
impl FromSql for NaiveDate {
    fn column_result(value: ValueRef) -> Result<Self> {
        value.as_str().and_then(|s| match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            Ok(dt) => Ok(dt),
            Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
        })
    }
}

/// ISO 8601 time without timezone => "HH:MM:SS.SSS"
impl ToSql for NaiveTime {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%H:%M:%S%.f").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS" => ISO 8601 time without timezone.
impl FromSql for NaiveTime {
    fn column_result(value: ValueRef) -> Result<Self> {
        value.as_str().and_then(|s| {
            let fmt = match s.len() {
                5 => "%H:%M",
                8 => "%H:%M:%S",
                _ => "%H:%M:%S%.f",
            };
            match NaiveTime::parse_from_str(s, fmt) {
                Ok(dt) => Ok(dt),
                Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
            }
        })
    }
}

/// ISO 8601 combined date and time without timezone => "YYYY-MM-DD HH:MM:SS.SSS"
impl ToSql for NaiveDateTime {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%dT%H:%M:%S%.f").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date and time
/// without timezone. ("YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS" also supported)
impl FromSql for NaiveDateTime {
    fn column_result(value: ValueRef) -> Result<Self> {
        value.as_str().and_then(|s| {
            let fmt = if s.len() >= 11 && s.as_bytes()[10] == b'T' {
                "%Y-%m-%dT%H:%M:%S%.f"
            } else {
                "%Y-%m-%d %H:%M:%S%.f"
            };

            match NaiveDateTime::parse_from_str(s, fmt) {
                Ok(dt) => Ok(dt),
                Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
            }
        })
    }
}

/// Date and time with time zone => UTC RFC3339 timestamp ("YYYY-MM-DDTHH:MM:SS.SSS+00:00").
impl<Tz: TimeZone> ToSql for DateTime<Tz> {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let utc_dt = self.with_timezone(&UTC);
        utc_dt.to_rfc3339().bind_parameter(stmt, col)
    }
}

/// RFC3339 ("YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM") into DateTime<UTC>.
impl FromSql for DateTime<UTC> {
    fn column_result(value: ValueRef) -> Result<Self> {
        {
            // Try to parse value as rfc3339 first.
            let s = try!(value.as_str());

            // If timestamp looks space-separated, make a copy and replace it with 'T'.
            let s = if s.len() >= 11 && s.as_bytes()[10] == b' ' {
                let mut s = s.to_string();
                unsafe {
                    let sbytes = s.as_mut_vec();
                    sbytes[10] = b'T';
                }
                Cow::Owned(s)
            } else {
                Cow::Borrowed(s)
            };

            match DateTime::parse_from_rfc3339(&s) {
                Ok(dt) => return Ok(dt.with_timezone(&UTC)),
                Err(_) => (),
            }
        }

        // Couldn't parse as rfc3339 - fall back to NaiveDateTime.
        NaiveDateTime::column_result(value).map(|dt| UTC.from_utc_datetime(&dt))
    }
}

/// RFC3339 ("YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM") into DateTime<Local>.
impl FromSql for DateTime<Local> {
    fn column_result(value: ValueRef) -> Result<Self> {
        let utc_dt = try!(DateTime::<UTC>::column_result(value));
        Ok(utc_dt.with_timezone(&Local))
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use super::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, UTC,
                        Duration};

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT, b BLOB)").unwrap();
        db
    }

    #[test]
    fn test_naive_date() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        db.execute("INSERT INTO foo (t) VALUES (?)", &[&date]).unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23", s);
        let t: NaiveDate = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(date, t);
    }

    #[test]
    fn test_naive_time() {
        let db = checked_memory_handle();
        let time = NaiveTime::from_hms(23, 56, 4);
        db.execute("INSERT INTO foo (t) VALUES (?)", &[&time]).unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("23:56:04", s);
        let v: NaiveTime = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(time, v);
    }

    #[test]
    fn test_naive_date_time() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        let time = NaiveTime::from_hms(23, 56, 4);
        let dt = NaiveDateTime::new(date, time);

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&dt]).unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23T23:56:04", s);
        let v: NaiveDateTime = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(dt, v);

        db.execute("UPDATE foo set b = datetime(t)", &[]).unwrap(); // "YYYY-MM-DD HH:MM:SS"
        let hms: NaiveDateTime = db.query_row("SELECT b FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(dt, hms);
    }

    #[test]
    fn test_date_time_utc() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        let time = NaiveTime::from_hms_milli(23, 56, 4, 789);
        let dt = NaiveDateTime::new(date, time);
        let utc = UTC.from_utc_datetime(&dt);

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&utc]).unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23T23:56:04.789+00:00", s);

        let v1: DateTime<UTC> = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(utc, v1);

        let v2: DateTime<UTC> = db.query_row("SELECT '2016-02-23 23:56:04.789'", &[], |r| r.get(0))
            .unwrap();
        assert_eq!(utc, v2);

        let v3: DateTime<UTC> = db.query_row("SELECT '2016-02-23 23:56:04'", &[], |r| r.get(0))
            .unwrap();
        assert_eq!(utc - Duration::milliseconds(789), v3);

        let v4: DateTime<UTC> =
            db.query_row("SELECT '2016-02-23 23:56:04.789+00:00'", &[], |r| r.get(0)).unwrap();
        assert_eq!(utc, v4);
    }

    #[test]
    fn test_date_time_local() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        let time = NaiveTime::from_hms_milli(23, 56, 4, 789);
        let dt = NaiveDateTime::new(date, time);
        let local = Local.from_local_datetime(&dt).single().unwrap();

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&local]).unwrap();

        // Stored string should be in UTC
        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert!(s.ends_with("+00:00"));

        let v: DateTime<Local> = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(local, v);
    }
}
