//! [`ToSql`] and [`FromSql`] implementation for [`time::OffsetDateTime`].
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;
use time::{Format, OffsetDateTime, PrimitiveDateTime, UtcOffset};

impl ToSql for OffsetDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        // FIXME keep original offset
        let time_string = self
            .to_offset(UtcOffset::UTC)
            .format("%Y-%m-%d %H:%M:%S.%NZ");
        Ok(ToSqlOutput::from(time_string))
    }
}

impl FromSql for OffsetDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            match s.len() {
                len if len <= 10 => PrimitiveDateTime::parse(s, "%Y-%m-%d").map(|d| d.assume_utc()),
                len if len <= 19 => {
                    // TODO YYYY-MM-DDTHH:MM:SS
                    PrimitiveDateTime::parse(s, "%Y-%m-%d %H:%M:%S").map(|d| d.assume_utc())
                }
                _ if s.ends_with('Z') => {
                    // TODO YYYY-MM-DDTHH:MM:SS.SSS
                    // FIXME time bug: %N specifier doesn't parse millis correctly (https://github.com/time-rs/time/issues/329)
                    PrimitiveDateTime::parse(s, "%Y-%m-%d %H:%M:%S.%NZ").map(|d| d.assume_utc())
                }
                _ if s.as_bytes()[10] == b'T' => {
                    // YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM
                    OffsetDateTime::parse(s, Format::Rfc3339)
                }
                _ if s.as_bytes()[19] == b':' => {
                    // legacy
                    // FIXME time bug: %N specifier doesn't parse millis correctly (https://github.com/time-rs/time/issues/329)
                    OffsetDateTime::parse(s, "%Y-%m-%d %H:%M:%S:%N %z")
                }
                _ => {
                    // FIXME time bug: %N specifier doesn't parse millis correctly (https://github.com/time-rs/time/issues/329)
                    // FIXME time bug: %z does not support ':' (https://github.com/time-rs/time/issues/241)
                    OffsetDateTime::parse(s, "%Y-%m-%d %H:%M:%S.%N%z").or_else(|err| {
                        PrimitiveDateTime::parse(s, "%Y-%m-%d %H:%M:%S.%N")
                            .map(|d| d.assume_utc())
                            .map_err(|_| err)
                    })
                }
            }
            .map_err(|err| FromSqlError::Other(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use std::time::Duration;
    use time::{date, offset, OffsetDateTime, Time};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT)")?;
        Ok(db)
    }

    #[test]
    fn test_offset_date_time() -> Result<()> {
        let db = checked_memory_handle()?;

        let mut ts_vec = vec![];

        let make_datetime =
            |secs, nanos| OffsetDateTime::from_unix_timestamp(secs) + Duration::from_nanos(nanos);

        ts_vec.push(make_datetime(10_000, 0)); //January 1, 1970 2:46:40 AM
        ts_vec.push(make_datetime(10_000, 1000)); //January 1, 1970 2:46:40 AM (and one microsecond)
        ts_vec.push(make_datetime(1_500_391_124, 1_000_000)); //July 18, 2017
        ts_vec.push(make_datetime(2_000_000_000, 2_000_000)); //May 18, 2033
        ts_vec.push(make_datetime(3_000_000_000, 999_999_999)); //January 24, 2065
        ts_vec.push(make_datetime(10_000_000_000, 0)); //November 20, 2286

        for ts in ts_vec {
            db.execute("INSERT INTO foo(t) VALUES (?)", [ts])?;

            let from: OffsetDateTime = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;

            db.execute("DELETE FROM foo", [])?;

            assert_eq!(from, ts);
        }
        Ok(())
    }

    #[test]
    fn test_string_values() -> Result<()> {
        let db = checked_memory_handle()?;
        for (s, t) in vec![
            (
                "2013-10-07 08:23:19.120",
                Ok(date!(2013 - 10 - 07)
                    .with_time(
                        Time::/*FIXME time bug try_from_hms_milli*/try_from_hms_nano(
                            8, 23, 19, 120,
                        )
                        .unwrap(),
                    )
                    .assume_utc()),
            ),
            (
                "2013-10-07 08:23:19.120Z",
                Ok(date!(2013 - 10 - 07)
                    .with_time(
                        Time::/*FIXME time bug try_from_hms_milli*/try_from_hms_nano(
                            8, 23, 19, 120,
                        )
                        .unwrap(),
                    )
                    .assume_utc()),
            ),
            //"2013-10-07T08:23:19.120Z", // TODO
            (
                "2013-10-07 04:23:19.120-04:00",
                Ok(date!(2013 - 10 - 07)
                    .with_time(
                        Time::/*FIXME time bug try_from_hms_milli*/try_from_hms_nano(
                            4, 23, 19, 120,
                        )
                        .unwrap(),
                    )
                    .assume_offset(offset!(-4))),
            ),
        ] {
            let result: Result<OffsetDateTime> = db.query_row("SELECT ?", [s], |r| r.get(0));
            assert_eq!(result, t);
        }
        Ok(())
    }

    #[test]
    fn test_sqlite_functions() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<OffsetDateTime> =
            db.query_row("SELECT CURRENT_TIMESTAMP", [], |r| r.get(0));
        assert!(result.is_ok());
        Ok(())
    }

    #[test]
    fn test_param() -> Result<()> {
        let db = checked_memory_handle()?;
        let result: Result<bool> = db.query_row("SELECT 1 WHERE ? BETWEEN datetime('now', '-1 minute') AND datetime('now', '+1 minute')", [OffsetDateTime::now_utc()], |r| r.get(0));
        assert!(result.is_ok());
        Ok(())
    }
}
