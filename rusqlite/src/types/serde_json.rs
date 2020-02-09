//! `ToSql` and `FromSql` implementation for JSON `Value`.

use serde_json::Value;

use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;

/// Serialize JSON `Value` to text.
impl ToSql for Value {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(serde_json::to_string(self).unwrap()))
    }
}

/// Deserialize text/blob to JSON `Value`.
impl FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => serde_json::from_slice(s),
            ValueRef::Blob(b) => serde_json::from_slice(b),
            _ => return Err(FromSqlError::InvalidType),
        }
        .map_err(|err| FromSqlError::Other(Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::types::ToSql;
    use crate::{Connection, NO_PARAMS};

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
            &[&data as &dyn ToSql, &json.as_bytes()],
        )
        .unwrap();

        let t: serde_json::Value = db
            .query_row("SELECT t FROM foo", NO_PARAMS, |r| r.get(0))
            .unwrap();
        assert_eq!(data, t);
        let b: serde_json::Value = db
            .query_row("SELECT b FROM foo", NO_PARAMS, |r| r.get(0))
            .unwrap();
        assert_eq!(data, b);
    }
}
