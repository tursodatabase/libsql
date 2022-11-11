//! [`ToSql`] and [`FromSql`] implementation for JSON `Value`.

use serde_json::Value;

use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::Result;

/// Serialize JSON `Value` to text.
impl ToSql for Value {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(serde_json::to_string(self).unwrap()))
    }
}

/// Deserialize text/blob to JSON `Value`.
impl FromSql for Value {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        let bytes = value.as_bytes()?;
        serde_json::from_slice(bytes).map_err(|err| FromSqlError::Other(Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::types::ToSql;
    use crate::{Connection, Result};
    use serde_json::Value;

    fn checked_memory_handle() -> Result<Connection> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (t TEXT, b BLOB)")?;
        Ok(db)
    }

    #[test]
    fn test_json_value() -> Result<()> {
        let db = checked_memory_handle()?;

        let json = r#"{"foo": 13, "bar": "baz"}"#;
        let data: Value = serde_json::from_str(json).unwrap();
        db.execute(
            "INSERT INTO foo (t, b) VALUES (?, ?)",
            [&data as &dyn ToSql, &json.as_bytes()],
        )?;

        let t: Value = db.one_column("SELECT t FROM foo")?;
        assert_eq!(data, t);
        let b: Value = db.one_column("SELECT b FROM foo")?;
        assert_eq!(data, b);
        Ok(())
    }
}
