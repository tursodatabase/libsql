//! [`ToSql`] and [`FromSql`] implementation for [`time::OffsetDateTime`].
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

const CURRENT_TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S";
const SQLITE_DATETIME_FMT: &str = "%Y-%m-%dT%H:%M:%S.%NZ";
const SQLITE_DATETIME_FMT_LEGACY: &str = "%Y-%m-%d %H:%M:%S:%N %z";

impl ToSql for OffsetDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        let time_string = self.to_offset(UtcOffset::UTC).format(SQLITE_DATETIME_FMT);
        Ok(ToSqlOutput::from(time_string))
    }
}

impl FromSql for OffsetDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            match s.len() {
                19 => PrimitiveDateTime::parse(s, CURRENT_TIMESTAMP_FMT).map(|d| d.assume_utc()),
                _ => PrimitiveDateTime::parse(s, SQLITE_DATETIME_FMT)
                    .map(|d| d.assume_utc())
                    .or_else(|err| {
                        OffsetDateTime::parse(s, SQLITE_DATETIME_FMT_LEGACY).map_err(|_| err)
                    }),
            }
            .map_err(|err| FromSqlError::Other(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use std::time::Duration;
    use time::OffsetDateTime;

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
            db.execute("INSERT INTO foo(t) VALUES (?)", &[&ts])?;

            let from: OffsetDateTime = db.query_row("SELECT t FROM foo", [], |r| r.get(0))?;

            db.execute("DELETE FROM foo", [])?;

            assert_eq!(from, ts);
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
}
