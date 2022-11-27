//! [`ToSql`] and [`FromSql`] implementation for [`time::OffsetDateTime`].
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::{Error, Result};
use time::format_description::well_known::Rfc3339;
use time::format_description::FormatItem;
use time::macros::format_description;
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

const PRIMITIVE_SHORT_DATE_TIME_FORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second]");
const PRIMITIVE_DATE_TIME_FORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]");
const PRIMITIVE_DATE_TIME_Z_FORMAT: &[FormatItem<'_>] =
    format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]Z");
const OFFSET_SHORT_DATE_TIME_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second][offset_hour sign:mandatory]:[offset_minute]"
);
const OFFSET_DATE_TIME_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond][offset_hour sign:mandatory]:[offset_minute]"
);
const LEGACY_DATE_TIME_FORMAT: &[FormatItem<'_>] = format_description!(
    "[year]-[month]-[day] [hour]:[minute]:[second]:[subsecond] [offset_hour sign:mandatory]:[offset_minute]"
);

impl ToSql for OffsetDateTime {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        // FIXME keep original offset
        let time_string = self
            .to_offset(UtcOffset::UTC)
            .format(&PRIMITIVE_DATE_TIME_Z_FORMAT)
            .map_err(|err| Error::ToSqlConversionFailure(err.into()))?;
        Ok(ToSqlOutput::from(time_string))
    }
}

impl FromSql for OffsetDateTime {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        value.as_str().and_then(|s| {
            if s.len() > 10 && s.as_bytes()[10] == b'T' {
                // YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM
                return OffsetDateTime::parse(s, &Rfc3339)
                    .map_err(|err| FromSqlError::Other(Box::new(err)));
            }
            let s = s.strip_suffix('Z').unwrap_or(s);
            match s.len() {
                len if len <= 19 => {
                    // TODO YYYY-MM-DDTHH:MM:SS
                    PrimitiveDateTime::parse(s, &PRIMITIVE_SHORT_DATE_TIME_FORMAT)
                        .map(PrimitiveDateTime::assume_utc)
                }
                _ if s.as_bytes()[19] == b':' => {
                    // legacy
                    OffsetDateTime::parse(s, &LEGACY_DATE_TIME_FORMAT)
                }
                _ if s.as_bytes()[19] == b'.' => OffsetDateTime::parse(s, &OFFSET_DATE_TIME_FORMAT)
                    .or_else(|err| {
                        PrimitiveDateTime::parse(s, &PRIMITIVE_DATE_TIME_FORMAT)
                            .map(PrimitiveDateTime::assume_utc)
                            .map_err(|_| err)
                    }),
                _ => OffsetDateTime::parse(s, &OFFSET_SHORT_DATE_TIME_FORMAT),
            }
            .map_err(|err| FromSqlError::Other(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::{Connection, Result};
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    #[test]
    fn test_offset_date_time() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT)")?;

        let mut ts_vec = vec![];

        let make_datetime = |secs: i128, nanos: i128| {
            OffsetDateTime::from_unix_timestamp_nanos(1_000_000_000 * secs + nanos).unwrap()
        };

        ts_vec.push(make_datetime(10_000, 0)); //January 1, 1970 2:46:40 AM
        ts_vec.push(make_datetime(10_000, 1000)); //January 1, 1970 2:46:40 AM (and one microsecond)
        ts_vec.push(make_datetime(1_500_391_124, 1_000_000)); //July 18, 2017
        ts_vec.push(make_datetime(2_000_000_000, 2_000_000)); //May 18, 2033
        ts_vec.push(make_datetime(3_000_000_000, 999_999_999)); //January 24, 2065
        ts_vec.push(make_datetime(10_000_000_000, 0)); //November 20, 2286

        for ts in ts_vec {
            db.execute("INSERT INTO foo(t) VALUES (?1)", [ts])?;

            let from: OffsetDateTime = db.one_column("SELECT t FROM foo")?;

            db.execute("DELETE FROM foo", [])?;

            assert_eq!(from, ts);
        }
        Ok(())
    }

    #[test]
    fn test_string_values() -> Result<()> {
        let db = Connection::open_in_memory()?;
        for (s, t) in vec![
            (
                "2013-10-07 08:23:19",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07 08:23:19Z",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07T08:23:19Z",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07 08:23:19.120",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19.120Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07 08:23:19.120Z",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19.120Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07T08:23:19.120Z",
                Ok(OffsetDateTime::parse("2013-10-07T08:23:19.120Z", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07 04:23:19-04:00",
                Ok(OffsetDateTime::parse("2013-10-07T04:23:19-04:00", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07 04:23:19.120-04:00",
                Ok(OffsetDateTime::parse("2013-10-07T04:23:19.120-04:00", &Rfc3339).unwrap()),
            ),
            (
                "2013-10-07T04:23:19.120-04:00",
                Ok(OffsetDateTime::parse("2013-10-07T04:23:19.120-04:00", &Rfc3339).unwrap()),
            ),
        ] {
            let result: Result<OffsetDateTime> = db.query_row("SELECT ?1", [s], |r| r.get(0));
            assert_eq!(result, t);
        }
        Ok(())
    }

    #[test]
    fn test_sqlite_functions() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let result: Result<OffsetDateTime> = db.one_column("SELECT CURRENT_TIMESTAMP");
        result.unwrap();
        Ok(())
    }

    #[test]
    fn test_param() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let result: Result<bool> = db.query_row("SELECT 1 WHERE ?1 BETWEEN datetime('now', '-1 minute') AND datetime('now', '+1 minute')", [OffsetDateTime::now_utc()], |r| r.get(0));
        result.unwrap();
        Ok(())
    }
}
