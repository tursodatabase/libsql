//! [`ToSql`] and [`FromSql`] implementation for JSON `Value`.

use serde_json::{Number, Value};

use crate::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use crate::{Error, Result};

/// Serialize JSON `Value` to text:
///
///
/// | JSON   | SQLite    |
/// |----------|---------|
/// | Null     | NULL    |
/// | Bool     | 'true' / 'false' |
/// | Number   | INT or REAL except u64 |
/// | _ | TEXT |
impl ToSql for Value {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match self {
            Value::Null => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
            Value::Number(n) if n.is_i64() => Ok(ToSqlOutput::from(n.as_i64().unwrap())),
            Value::Number(n) if n.is_f64() => Ok(ToSqlOutput::from(n.as_f64().unwrap())),
            _ => serde_json::to_string(self)
                .map(ToSqlOutput::from)
                .map_err(|err| Error::ToSqlConversionFailure(err.into())),
        }
    }
}

/// Deserialize SQLite value to JSON `Value`:
///
/// | SQLite   | JSON    |
/// |----------|---------|
/// | NULL     | Null    |
/// | 'null'   | Null    |
/// | 'true'   | Bool    |
/// | 1        | Number  |
/// | 0.1      | Number  |
/// | '"text"' | String  |
/// | 'text'   | _Error_ |
/// | '[0, 1]' | Array   |
/// | '{"x": 1}' | Object  |
impl FromSql for Value {
    #[inline]
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Text(s) => serde_json::from_slice(s), // KO for b"text"
            ValueRef::Blob(b) => serde_json::from_slice(b),
            ValueRef::Integer(i) => Ok(Value::Number(Number::from(i))),
            ValueRef::Real(f) => {
                match Number::from_f64(f) {
                    Some(n) => Ok(Value::Number(n)),
                    _ => return Err(FromSqlError::InvalidType), // FIXME
                }
            }
            ValueRef::Null => Ok(Value::Null),
        }
        .map_err(|err| FromSqlError::Other(Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use crate::types::ToSql;
    use crate::{Connection, Result};
    use serde_json::{Number, Value};

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
            "INSERT INTO foo (t, b) VALUES (?1, ?2)",
            [&data as &dyn ToSql, &json.as_bytes()],
        )?;

        let t: Value = db.one_column("SELECT t FROM foo")?;
        assert_eq!(data, t);
        let b: Value = db.one_column("SELECT b FROM foo")?;
        assert_eq!(data, b);
        Ok(())
    }

    #[test]
    fn test_to_sql() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let v: Option<String> = db.query_row("SELECT ?", [Value::Null], |r| r.get(0))?;
        assert_eq!(None, v);
        let v: String = db.query_row("SELECT ?", [Value::Bool(true)], |r| r.get(0))?;
        assert_eq!("true", v);
        let v: i64 = db.query_row("SELECT ?", [Value::Number(Number::from(1))], |r| r.get(0))?;
        assert_eq!(1, v);
        let v: f64 = db.query_row(
            "SELECT ?",
            [Value::Number(Number::from_f64(0.1).unwrap())],
            |r| r.get(0),
        )?;
        assert_eq!(0.1, v);
        let v: String =
            db.query_row("SELECT ?", [Value::String("text".to_owned())], |r| r.get(0))?;
        assert_eq!("\"text\"", v);
        Ok(())
    }

    #[test]
    fn test_from_sql() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let v: Value = db.one_column("SELECT NULL")?;
        assert_eq!(Value::Null, v);
        let v: Value = db.one_column("SELECT 'null'")?;
        assert_eq!(Value::Null, v);
        let v: Value = db.one_column("SELECT 'true'")?;
        assert_eq!(Value::Bool(true), v);
        let v: Value = db.one_column("SELECT 1")?;
        assert_eq!(Value::Number(Number::from(1)), v);
        let v: Value = db.one_column("SELECT 0.1")?;
        assert_eq!(Value::Number(Number::from_f64(0.1).unwrap()), v);
        let v: Value = db.one_column("SELECT '\"text\"'")?;
        assert_eq!(Value::String("text".to_owned()), v);
        let v: Result<Value> = db.one_column("SELECT 'text'");
        assert!(v.is_err());
        Ok(())
    }
}
