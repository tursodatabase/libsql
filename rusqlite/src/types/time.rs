//! [`ToSql`] and [`FromSql`] implementation for [`time::OffsetDateTime`].
use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::{Error, Result};
use time::format_description::well_known::Rfc3339;
use time::format_description::{modifier, Component, FormatItem};
use time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

const DATE_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Component(Component::Year(modifier::Year {
        repr: modifier::YearRepr::Full,
        iso_week_based: false,
        sign_is_mandatory: false,
        padding: modifier::Padding::Zero,
    })),
    FormatItem::Literal(b"-"),
    FormatItem::Component(Component::Month(modifier::Month {
        repr: modifier::MonthRepr::Numerical,
        padding: modifier::Padding::Zero,
    })),
    FormatItem::Literal(b"-"),
    FormatItem::Component(Component::Day(modifier::Day {
        padding: modifier::Padding::Zero,
    })),
];

const SHORT_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Component(Component::Hour(modifier::Hour {
        padding: modifier::Padding::Zero,
        is_12_hour_clock: false,
    })),
    FormatItem::Literal(b":"),
    FormatItem::Component(Component::Minute(modifier::Minute {
        padding: modifier::Padding::Zero,
    })),
    FormatItem::Literal(b":"),
    FormatItem::Component(Component::Second(modifier::Second {
        padding: modifier::Padding::Zero,
    })),
];
const TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(SHORT_TIME_FORMAT),
    FormatItem::Literal(b"."),
    FormatItem::Component(Component::Subsecond(modifier::Subsecond {
        digits: modifier::SubsecondDigits::OneOrMore, // TODO SQLite supports ZeroOrMore
    })),
];
const LEGACY_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(SHORT_TIME_FORMAT),
    FormatItem::Literal(b":"), // legacy
    FormatItem::Component(Component::Subsecond(modifier::Subsecond {
        digits: modifier::SubsecondDigits::OneOrMore,
    })),
];

const OFFSET_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Component(Component::OffsetHour(modifier::OffsetHour {
        sign_is_mandatory: true,
        padding: modifier::Padding::Zero,
    })),
    FormatItem::Literal(b":"),
    FormatItem::Component(Component::OffsetMinute(modifier::OffsetMinute {
        padding: modifier::Padding::Zero,
    })),
];

const PRIMITIVE_SHORT_DATE_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(SHORT_TIME_FORMAT),
];

const PRIMITIVE_DATE_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(TIME_FORMAT),
];
const PRIMITIVE_DATE_TIME_Z_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(TIME_FORMAT),
    FormatItem::Literal(b"Z"), // TODO "T"
];

const OFFSET_SHORT_DATE_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(SHORT_TIME_FORMAT),
    //FormatItem::Literal(b" "), optional
    FormatItem::Compound(OFFSET_FORMAT),
];

const OFFSET_DATE_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(TIME_FORMAT),
    // FormatItem::Literal(b" "), optional
    FormatItem::Compound(OFFSET_FORMAT),
];

const LEGACY_DATE_TIME_FORMAT: &[FormatItem<'_>] = &[
    FormatItem::Compound(DATE_FORMAT),
    FormatItem::Literal(b" "), // TODO "T"
    FormatItem::Compound(LEGACY_TIME_FORMAT),
    FormatItem::Literal(b" "),
    FormatItem::Compound(OFFSET_FORMAT),
];

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
            let s = s.strip_suffix('Z').unwrap_or(s);
            match s.len() {
                len if len <= 19 => {
                    // TODO YYYY-MM-DDTHH:MM:SS
                    PrimitiveDateTime::parse(s, &PRIMITIVE_SHORT_DATE_TIME_FORMAT)
                        .map(|d| d.assume_utc())
                }
                _ if s.as_bytes()[10] == b'T' => {
                    // YYYY-MM-DDTHH:MM:SS.SSS[+-]HH:MM
                    OffsetDateTime::parse(s, &Rfc3339)
                }
                _ if s.as_bytes()[19] == b':' => {
                    // legacy
                    OffsetDateTime::parse(s, &LEGACY_DATE_TIME_FORMAT)
                }
                _ if s.as_bytes()[19] == b'.' => OffsetDateTime::parse(s, &OFFSET_DATE_TIME_FORMAT)
                    .or_else(|err| {
                        PrimitiveDateTime::parse(s, &PRIMITIVE_DATE_TIME_FORMAT)
                            .map(|d| d.assume_utc())
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
    use time::{Date, Month, OffsetDateTime, Time, UtcOffset};

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT)")?;
        Ok(db)
    }

    #[test]
    fn test_offset_date_time() -> Result<()> {
        let db = checked_memory_handle()?;

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
                Ok(Date::from_calendar_date(2013, Month::October, 7)
                    .unwrap()
                    .with_time(Time::from_hms_milli(8, 23, 19, 120).unwrap())
                    .assume_utc()),
            ),
            (
                "2013-10-07 08:23:19.120Z",
                Ok(Date::from_calendar_date(2013, Month::October, 7)
                    .unwrap()
                    .with_time(Time::from_hms_milli(8, 23, 19, 120).unwrap())
                    .assume_utc()),
            ),
            //"2013-10-07T08:23:19.120Z", // TODO
            (
                "2013-10-07 04:23:19.120-04:00",
                Ok(Date::from_calendar_date(2013, Month::October, 7)
                    .unwrap()
                    .with_time(Time::from_hms_milli(4, 23, 19, 120).unwrap())
                    .assume_offset(UtcOffset::from_hms(-4, 0, 0).unwrap())),
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
