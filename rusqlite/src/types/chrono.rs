//! Convert most of the [Time Strings](http://sqlite.org/lang_datefunc.html) to chrono types.
extern crate chrono;

use std::error;
use self::chrono::{NaiveDate, NaiveTime, NaiveDateTime, DateTime, TimeZone, UTC, Local};
use libc::c_int;

use {Error, Result};
use types::{FromSql, ToSql};

use ffi;
use ffi::sqlite3_stmt;
use ffi::sqlite3_column_type;

const JULIAN_DAY: f64 = 2440587.5; // 1970-01-01 00:00:00 is JD 2440587.5
const DAY_IN_SECONDS: f64 = 86400.0;
const JULIAN_DAY_GREGORIAN: f64 = 1721424.5; // Jan 1, 1 proleptic Gregorian calendar

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
impl ToSql for NaiveDate {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%d").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD" or Julian Day => ISO 8601 calendar date without timezone.
impl FromSql for NaiveDate {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<NaiveDate> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let s = try!(String::column_result(stmt, col));
                match NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
                    Ok(dt) => Ok(dt),
                    Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
                }
            }
            ffi::SQLITE_FLOAT => {
                // if column affinity is REAL and an integer/unix timestamp is inserted => unexpected result
                let mut jd = ffi::sqlite3_column_double(stmt, col);
                jd -= JULIAN_DAY_GREGORIAN;
                if jd < i32::min_value() as f64 || jd > i32::max_value() as f64 {
                    let err: Box<error::Error + Sync + Send> = "out-of-range date".into();
                    return Err(Error::FromSqlConversionFailure(err));
                }
                match NaiveDate::from_num_days_from_ce_opt(jd as i32) {
                    Some(dt) => Ok(dt),
                    None => {
                        let err: Box<error::Error + Sync + Send> = "out-of-range date".into();
                        Err(Error::FromSqlConversionFailure(err))
                    }
                }
            }
            _ => Err(Error::InvalidColumnType),
        }
    }

    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true // to avoid double check
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
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<NaiveTime> {
        let s = try!(String::column_result(stmt, col));
        let fmt = match s.len() {
            5 => "%H:%M",
            8 => "%H:%M:%S",
            _ => "%H:%M:%S%.f",
        };
        match NaiveTime::parse_from_str(&s, fmt) {
            Ok(dt) => Ok(dt),
            Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
        }
    }

    unsafe fn column_has_valid_sqlite_type(stmt: *mut sqlite3_stmt, col: c_int) -> bool {
        sqlite3_column_type(stmt, col) == ffi::SQLITE_TEXT
    }
}

/// ISO 8601 combined date and time without timezone => "YYYY-MM-DD HH:MM:SS.SSS"
impl ToSql for NaiveDateTime {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%d %H:%M:%S%.f").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD HH:MM"/"YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS"/ Julian Day / Unix Time => ISO 8601 combined date and time without timezone.
/// ("YYYY-MM-DDTHH:MM"/"YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS" also supported)
impl FromSql for NaiveDateTime {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<NaiveDateTime> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let s = try!(String::column_result(stmt, col));
                let fmt = match s.len() {
                    16 => {
                        match s.as_bytes()[10] {
                            b'T' => "%Y-%m-%dT%H:%M",
                            _ => "%Y-%m-%d %H:%M",
                        }
                    }
                    19 => {
                        match s.as_bytes()[10] {
                            b'T' => "%Y-%m-%dT%H:%M:%S",
                            _ => "%Y-%m-%d %H:%M:%S",
                        }
                    }
                    _ => {
                        match s.as_bytes()[10] {
                            b'T' => "%Y-%m-%dT%H:%M:%S%.f",
                            _ => "%Y-%m-%d %H:%M:%S%.f",
                        }
                    }
                };
                match NaiveDateTime::parse_from_str(&s, fmt) {
                    Ok(dt) => Ok(dt),
                    Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
                }
            }
            ffi::SQLITE_INTEGER => {
                match NaiveDateTime::from_timestamp_opt(ffi::sqlite3_column_int64(stmt, col), 0) {
                    Some(dt) => Ok(dt),
                    None => {
                        let err: Box<error::Error + Sync + Send> = "out-of-range number of seconds"
                                                                       .into();
                        Err(Error::FromSqlConversionFailure(err))
                    }
                }
            }
            ffi::SQLITE_FLOAT => {
                // if column affinity is REAL and an integer/unix timestamp is inserted => unexpected result
                let mut jd = ffi::sqlite3_column_double(stmt, col);
                jd -= JULIAN_DAY;
                jd *= DAY_IN_SECONDS;
                let ns = jd.fract() * 10f64.powi(9);
                match NaiveDateTime::from_timestamp_opt(jd as i64, ns as u32) {
                    Some(dt) => Ok(dt),
                    None => {
                        let err: Box<error::Error + Sync + Send> = "out-of-range number of \
                                                                    seconds and/or invalid \
                                                                    nanosecond"
                                                                       .into();
                        Err(Error::FromSqlConversionFailure(err))
                    }
                }
            }
            _ => Err(Error::InvalidColumnType),
        }
    }

    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true // to avoid double check
    }
}

/// ISO 8601 date and time with time zone => "YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM"
impl ToSql for DateTime<UTC> {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM"/"YYYY-MM-DD HH:MM"/"YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS"/ Julian Day / Unix Time => ISO 8601 date and time with time zone.
/// ("YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM"/"YYYY-MM-DDTHH:MM"/"YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS" also supported)
/// When the timezone is not specified, UTC is used.
impl FromSql for DateTime<UTC> {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<DateTime<UTC>> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let s = try!(String::column_result(stmt, col));
                if s.len() > 23 {
                    let fmt = if s.as_bytes()[10] == b'T' {
                        "%Y-%m-%dT%H:%M:%S%.f%:z"
                    } else {
                        "%Y-%m-%d %H:%M:%S%.f%:z"
                    };
                    match UTC.datetime_from_str(&s, fmt) {
                        Ok(dt) => Ok(dt),
                        Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
                    }
                } else {
                    NaiveDateTime::column_result(stmt, col).map(|dt| UTC.from_utc_datetime(&dt))
                }
            }
            ffi::SQLITE_INTEGER => {
                NaiveDateTime::column_result(stmt, col).map(|dt| UTC.from_utc_datetime(&dt))
            }
            ffi::SQLITE_FLOAT => {
                NaiveDateTime::column_result(stmt, col).map(|dt| UTC.from_utc_datetime(&dt))
            }
            _ => Err(Error::InvalidColumnType),
        }
    }

    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true // to avoid double check
    }
}


/// ISO 8601 date and time with time zone => "YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM"
impl ToSql for DateTime<Local> {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let date_str = self.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
        date_str.bind_parameter(stmt, col)
    }
}

/// "YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM"/"YYYY-MM-DD HH:MM"/"YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS"/ Julian Day / Unix Time => ISO 8601 date and time with time zone.
/// ("YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM"/"YYYY-MM-DDTHH:MM"/"YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS" also supported)
/// When the timezone is not specified, Local is used.
impl FromSql for DateTime<Local> {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<DateTime<Local>> {
        match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let s = try!(String::column_result(stmt, col));
                if s.len() > 23 {
                    let fmt = if s.as_bytes()[10] == b'T' {
                        "%Y-%m-%dT%H:%M:%S%.f%:z"
                    } else {
                        "%Y-%m-%d %H:%M:%S%.f%:z"
                    };
                    match Local.datetime_from_str(&s, fmt) {
                        Ok(dt) => Ok(dt),
                        Err(err) => Err(Error::FromSqlConversionFailure(Box::new(err))),
                    }
                } else {
                    NaiveDateTime::column_result(stmt, col).map(|dt| Local.from_utc_datetime(&dt))
                }
            }
            ffi::SQLITE_INTEGER => {
                NaiveDateTime::column_result(stmt, col).map(|dt| Local.from_utc_datetime(&dt))
            }
            ffi::SQLITE_FLOAT => {
                NaiveDateTime::column_result(stmt, col).map(|dt| Local.from_utc_datetime(&dt))
            }
            _ => Err(Error::InvalidColumnType),
        }
    }

    unsafe fn column_has_valid_sqlite_type(_: *mut sqlite3_stmt, _: c_int) -> bool {
        true // to avoid double check
    }
}

// struct UnixTime(NaiveDateTime);
// struct JulianTime(NaiveDateTime)

#[cfg(test)]
mod test {
    use Connection;
    use super::chrono::{DateTime, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, UTC};

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
        db.execute("UPDATE foo SET f = julianday(t)", &[]).unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23", s);
        let t: NaiveDate = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(date, t);
        let f: NaiveDate = db.query_row("SELECT f FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(date, f);
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

        let di = NaiveDateTime::new(date, NaiveTime::from_hms(23, 56, 3));
        let ds = NaiveDateTime::new(date, NaiveTime::from_hms(23, 56, 5));

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&dt]).unwrap();
        db.execute("UPDATE foo SET f = julianday(t), i = strftime('%s', t)",
                   &[])
          .unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23 23:56:04", s);
        let v: NaiveDateTime = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(dt, v);
        let f: NaiveDateTime = db.query_row("SELECT f FROM foo", &[], |r| r.get(0)).unwrap();
        // `2016-02-23T23:56:04` vs `2016-02-23T23:56:03.999992609`
        assert!(f.ge(&di) && f.le(&ds));
        let i: NaiveDateTime = db.query_row("SELECT i FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(dt, i);

        db.execute("UPDATE foo set b = datetime(t)", &[]).unwrap(); // "YYYY-MM-DD HH:MM:SS"
        let hms: NaiveDateTime = db.query_row("SELECT b FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(dt, hms);

        db.execute("UPDATE foo set b = strftime('%Y-%m-%dT%H:%M', t)", &[]).unwrap();
        let hm: NaiveDateTime = db.query_row("SELECT b FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(NaiveDateTime::new(date, NaiveTime::from_hms(23, 56, 0)), hm);
    }

    #[test]
    fn test_date_time_utc() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        let time = NaiveTime::from_hms(23, 56, 4);
        let dt = NaiveDateTime::new(date, time);
        let utc = UTC.from_utc_datetime(&dt);

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&utc]).unwrap();
        db.execute("UPDATE foo SET f = julianday(t), i = strftime('%s', t)",
                   &[])
          .unwrap();

        let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!("2016-02-23 23:56:04+00:00", s);
        let v: DateTime<UTC> = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(utc, v);
        let i: DateTime<UTC> = db.query_row("SELECT i FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(utc, i);
    }

    #[test]
    fn test_date_time_local() {
        let db = checked_memory_handle();
        let date = NaiveDate::from_ymd(2016, 2, 23);
        let time = NaiveTime::from_hms(23, 56, 4);
        let dt = NaiveDateTime::new(date, time);
        let local = Local.from_local_datetime(&dt).single().unwrap();

        db.execute("INSERT INTO foo (t) VALUES (?)", &[&local]).unwrap();
        db.execute("UPDATE foo SET f = julianday(t), i = strftime('%s', t)",
                   &[])
          .unwrap();

        // let s: String = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        // assert_eq!("2016-02-23 23:56:04.000+01:00", s);
        let v: DateTime<Local> = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(local, v);
        let i: DateTime<Local> = db.query_row("SELECT i FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(local, i);
    }
}
