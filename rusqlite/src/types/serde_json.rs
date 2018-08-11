//! `ToSql` and `FromSql` implementation for JSON `Value`.
extern crate serde_json;

use self::serde_json::Value;

use types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use Result;

/// Serialize JSON `Value` to text.
impl ToSql for Value {
    fn to_sql(&self) -> Result<ToSqlOutput> {
        Ok(ToSqlOutput::from(serde_json::to_string(self).unwrap()))
    }
}

/// Deserialize text/blob to JSON `Value`.
impl FromSql for Value {
    fn column_result(value: ValueRef) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => serde_json::from_str(s),
            ValueRef::Blob(b) => serde_json::from_slice(b),
            _ => return Err(FromSqlError::InvalidType),
        }.map_err(|err| FromSqlError::Other(Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use super::serde_json;
    use Connection;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (t TEXT, b BLOB)")
            .unwrap();
        db
    }

    #[test]
    fn test_json_value() {
        let db = checked_memory_handle();

        let json = r#"{"foo": 13, "bar": "baz"}"#;
        let data: serde_json::Value = serde_json::from_str(json).unwrap();
        db.execute(
            "INSERT INTO foo (t, b) VALUES (?, ?)",
            &[&data, &json.as_bytes()],
        ).unwrap();

        let t: serde_json::Value = db
            .query_row("SELECT t FROM foo", &[], |r| r.get(0))
            .unwrap();
        assert_eq!(data, t);
        let b: serde_json::Value = db
            .query_row("SELECT b FROM foo", &[], |r| r.get(0))
            .unwrap();
        assert_eq!(data, b);
    }
}
