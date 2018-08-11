extern crate time;

use types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use Result;

const SQLITE_DATETIME_FMT: &str = "%Y-%m-%dT%H:%M:%S.%fZ";
const SQLITE_DATETIME_FMT_LEGACY: &str = "%Y-%m-%d %H:%M:%S:%f %Z";

impl ToSql for time::Timespec {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        let time_string = time::at_utc(*self)
            .strftime(SQLITE_DATETIME_FMT)
            .unwrap()
            .to_string();
        Ok(ToSqlOutput::from(time_string))
    }
}

impl FromSql for time::Timespec {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        value
            .as_str()
            .and_then(|s| {
                time::strptime(s, SQLITE_DATETIME_FMT).or_else(|err| {
                    time::strptime(s, SQLITE_DATETIME_FMT_LEGACY)
                        .or_else(|_| Err(FromSqlError::Other(Box::new(err))))
                })
            }).map(|tm| tm.to_timespec())
    }
}

#[cfg(test)]
mod test {
    use super::time;
    use Connection;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (t TEXT, i INTEGER, f FLOAT)")
            .unwrap();
        db
    }

    #[test]
    fn test_timespec() {
        let db = checked_memory_handle();

        let mut ts_vec = vec![];

        ts_vec.push(time::Timespec::new(10_000, 0)); //January 1, 1970 2:46:40 AM
        ts_vec.push(time::Timespec::new(10_000, 1000)); //January 1, 1970 2:46:40 AM (and one microsecond)
        ts_vec.push(time::Timespec::new(1500391124, 1_000_000)); //July 18, 2017
        ts_vec.push(time::Timespec::new(2000000000, 2_000_000)); //May 18, 2033
        ts_vec.push(time::Timespec::new(3000000000, 999_999_999)); //January 24, 2065
        ts_vec.push(time::Timespec::new(10000000000, 0)); //November 20, 2286

        for ts in ts_vec {
            db.execute("INSERT INTO foo(t) VALUES (?)", &[&ts]).unwrap();

            let from: time::Timespec = db
                .query_row("SELECT t FROM foo", &[], |r| r.get(0))
                .unwrap();

            db.execute("DELETE FROM foo", &[]).unwrap();

            assert_eq!(from, ts);
        }
    }
}
