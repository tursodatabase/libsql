//! Convert most of the [Time Strings](http://sqlite.org/lang_datefunc.html) to chrono types.

use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;

/// ISO 8601 calendar date without timezone => "YYYY-MM-DD"
impl ToSql for NaiveDate {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "YYYY-MM-DD" => ISO 8601 calendar date without timezone.
impl FromSql for NaiveDate {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value
            .as_str()
            .and_then(|s| match NaiveDate::parse_from_str(s, "%F") {
                Ok(dt) => Ok(dt),
                Err(err) => Err(FromSqlError::Other(Box::new(err))),
            })
    }
}

/// ISO 8601 time without timezone => "HH:MM:SS.SSS"
impl ToSql for NaiveTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%T%.f").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "HH:MM"/"HH:MM:SS"/"HH:MM:SS.SSS" => ISO 8601 time without timezone.
impl FromSql for NaiveTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            let fmt = match s.len() {
                5 => "%H:%M",
                8 => "%T",
                _ => "%T%.f",
            };
            match NaiveTime::parse_from_str(s, fmt) {
                Ok(dt) => Ok(dt),
                Err(err) => Err(FromSqlError::Other(Box::new(err))),
            }
        })
    }
}

/// ISO 8601 combined date and time without timezone =>
/// "YYYY-MM-DD HH:MM:SS.SSS"
impl ToSql for NaiveDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F %T%.f").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// "YYYY-MM-DD HH:MM:SS"/"YYYY-MM-DD HH:MM:SS.SSS" => ISO 8601 combined date
/// and time without timezone. ("YYYY-MM-DDTHH:MM:SS"/"YYYY-MM-DDTHH:MM:SS.SSS"
/// also supported)
impl FromSql for NaiveDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            let fmt = if s.len() >= 11 && s.as_bytes()[10] == b'T' {
                "%FT%T%.f"
            } else {
                "%F %T%.f"
            };

            match NaiveDateTime::parse_from_str(s, fmt) {
                Ok(dt) => Ok(dt),
                Err(err) => Err(FromSqlError::Other(Box::new(err))),
            }
        })
    }
}

/// UTC time => UTC RFC3339 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS+00:00").
impl ToSql for DateTime<Utc> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// Local time => UTC RFC3339 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS+00:00").
impl ToSql for DateTime<Local> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.with_timezone(&Utc).format("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// Date and time with time zone => RFC3339 timestamp
/// ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM").
impl ToSql for DateTime<FixedOffset> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let date_str = self.format("%F %T%.f%:z").to_string();
        Ok(ToSqlOutput::from(date_str))
    }
}

/// RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Utc>`.
impl FromSql for DateTime<Utc> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        {
            // Try to parse value as rfc3339 first.
            let s = value.as_str()?;

            let fmt = if s.len() >= 11 && s.as_bytes()[10] == b'T' {
                "%FT%T%.f%#z"
            } else {
                "%F %T%.f%#z"
            };

            if let Ok(dt) = DateTime::parse_from_str(s, fmt) {
                return Ok(dt.with_timezone(&Utc));
            }
        }

        // Couldn't parse as rfc3339 - fall back to NaiveDateTime.
        NaiveDateTime::column_result(value).map(|dt| Utc.from_utc_datetime(&dt))
    }
}

/// RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<Local>`.
impl FromSql for DateTime<Local> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let utc_dt = DateTime::<Utc>::column_result(value)?;
        Ok(utc_dt.with_timezone(&Local))
    }
}

/// RFC3339 ("YYYY-MM-DD HH:MM:SS.SSS[+-]HH:MM") into `DateTime<FixedOffset>`.
impl FromSql for DateTime<FixedOffset> {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let s = String::column_result(value)?;
        Self::parse_from_rfc3339(s.as_str())
            .or_else(|_| Self::parse_from_str(s.as_str(), "%F %T%.f%:z"))
            .map_err(|e| FromSqlError::Other(Box::new(e)))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        types::{FromSql, ValueRef},
        Connection, Result,
    };
    use chrono::{
        DateTime, Duration, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc,
    };

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT, b BLOB)")?;
        Ok(db)
    }

    #[test]
    fn test_naive_date() -> Result<()> {
        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        db.execute("INSERT INTO foo (t) VALUES (?1)", [date])?;

        let s: String = db.one_column("SELECT t FROM foo")?;
        assert_eq!("2016-02-23", s);
        let t: NaiveDate = db.one_column("SELECT t FROM foo")?;
        assert_eq!(date, t);
        Ok(())
    }

    #[test]
    fn test_naive_time() -> Result<()> {
        let db = checked_memory_handle()?;
        let time = NaiveTime::from_hms_opt(23, 56, 4).unwrap();
        db.execute("INSERT INTO foo (t) VALUES (?1)", [time])?;

        let s: String = db.one_column("SELECT t FROM foo")?;
        assert_eq!("23:56:04", s);
        let v: NaiveTime = db.one_column("SELECT t FROM foo")?;
        assert_eq!(time, v);
        Ok(())
    }

    #[test]
    fn test_naive_date_time() -> Result<()> {
        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_opt(23, 56, 4).unwrap();
        let dt = NaiveDateTime::new(date, time);

        db.execute("INSERT INTO foo (t) VALUES (?1)", [dt])?;

        let s: String = db.one_column("SELECT t FROM foo")?;
        assert_eq!("2016-02-23 23:56:04", s);
        let v: NaiveDateTime = db.one_column("SELECT t FROM foo")?;
        assert_eq!(dt, v);

        db.execute("UPDATE foo set b = datetime(t)", [])?; // "YYYY-MM-DD HH:MM:SS"
        let hms: NaiveDateTime = db.one_column("SELECT b FROM foo")?;
        assert_eq!(dt, hms);
        Ok(())
    }

    #[test]
    fn test_date_time_utc() -> Result<()> {
        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_milli_opt(23, 56, 4, 789).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let utc = Utc.from_utc_datetime(&dt);

        db.execute("INSERT INTO foo (t) VALUES (?1)", [utc])?;

        let s: String = db.one_column("SELECT t FROM foo")?;
        assert_eq!("2016-02-23 23:56:04.789+00:00", s);

        let v1: DateTime<Utc> = db.one_column("SELECT t FROM foo")?;
        assert_eq!(utc, v1);

        let v2: DateTime<Utc> = db.one_column("SELECT '2016-02-23 23:56:04.789'")?;
        assert_eq!(utc, v2);

        let v3: DateTime<Utc> = db.one_column("SELECT '2016-02-23 23:56:04'")?;
        assert_eq!(utc - Duration::milliseconds(789), v3);

        let v4: DateTime<Utc> = db.one_column("SELECT '2016-02-23 23:56:04.789+00:00'")?;
        assert_eq!(utc, v4);
        Ok(())
    }

    #[test]
    fn test_date_time_local() -> Result<()> {
        let db = checked_memory_handle()?;
        let date = NaiveDate::from_ymd_opt(2016, 2, 23).unwrap();
        let time = NaiveTime::from_hms_milli_opt(23, 56, 4, 789).unwrap();
        let dt = NaiveDateTime::new(date, time);
        let local = Local.from_local_datetime(&dt).single().unwrap();

        db.execute("INSERT INTO foo (t) VALUES (?1)", [local])?;

        // Stored string should be in UTC
        let s: String = db.one_column("SELECT t FROM foo")?;
        assert!(s.ends_with("+00:00"));

        let v: DateTime<Local> = db.one_column("SELECT t FROM foo")?;
        assert_eq!(local, v);
        Ok(())
    }

    #[test]
    fn test_date_time_fixed() -> Result<()> {
        let db = checked_memory_handle()?;
        let time = DateTime::parse_from_rfc3339("2020-04-07T11:23:45+04:00").unwrap();

        db.execute("INSERT INTO foo (t) VALUES (?1)", [time])?;

        // Stored string should preserve timezone offset
        let s: String = db.one_column("SELECT t FROM foo")?;
        assert!(s.ends_with("+04:00"));

        let v: DateTime<FixedOffset> = db.one_column("SELECT t FROM foo")?;
        assert_eq!(time.offset(), v.offset());
        assert_eq!(time, v);
        Ok(())
    }

    #[test]
    fn test_sqlite_functions() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<NaiveTime> = db.one_column("SELECT CURRENT_TIME");
        result.unwrap();
        let result: Result<NaiveDate> = db.one_column("SELECT CURRENT_DATE");
        result.unwrap();
        let result: Result<NaiveDateTime> = db.one_column("SELECT CURRENT_TIMESTAMP");
        result.unwrap();
        let result: Result<DateTime<Utc>> = db.one_column("SELECT CURRENT_TIMESTAMP");
        result.unwrap();
        Ok(())
    }

    #[test]
    fn test_naive_date_time_param() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<bool> = db.query_row("SELECT 1 WHERE ?1 BETWEEN datetime('now', '-1 minute') AND datetime('now', '+1 minute')", [Utc::now().naive_utc()], |r| r.get(0));
        result.unwrap();
        Ok(())
    }

    #[test]
    fn test_date_time_param() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<bool> = db.query_row("SELECT 1 WHERE ?1 BETWEEN datetime('now', '-1 minute') AND datetime('now', '+1 minute')", [Utc::now()], |r| r.get(0));
        result.unwrap();
        Ok(())
    }

    #[test]
    fn test_lenient_parse_timezone() {
        DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00Z")).unwrap();
        DateTime::<Utc>::column_result(ValueRef::Text(b"1970-01-01T00:00:00+00")).unwrap();
    }
}
