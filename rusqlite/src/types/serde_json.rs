//! `ToSql` and `FromSql` implementation for JSON `Value`.
extern crate serde_json;

use libc::c_int;
use self::serde_json::Value;

use {Error, Result};
use types::{FromSql, ToSql};

use ffi;
use ffi::sqlite3_stmt;
use ffi::sqlite3_column_type;

/// Serialize JSON `Value` to text.
impl ToSql for Value {
    unsafe fn bind_parameter(&self, stmt: *mut sqlite3_stmt, col: c_int) -> c_int {
        let s = serde_json::to_string(self).unwrap();
        s.bind_parameter(stmt, col)
    }
}

/// Deserialize text/blob to JSON `Value`.
impl FromSql for Value {
    unsafe fn column_result(stmt: *mut sqlite3_stmt, col: c_int) -> Result<Value> {
        let value_result = match sqlite3_column_type(stmt, col) {
            ffi::SQLITE_TEXT => {
                let s = try!(String::column_result(stmt, col));
                serde_json::from_str(&s)
            }
            ffi::SQLITE_BLOB => {
                let blob = try!(Vec::<u8>::column_result(stmt, col));
                serde_json::from_slice(&blob)
            }
            _ => return Err(Error::InvalidColumnType),
        };
        value_result.map_err(|err| Error::FromSqlConversionFailure(Box::new(err)))
    }
}

#[cfg(test)]
mod test {
    use Connection;
    use super::serde_json;

    fn checked_memory_handle() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE foo (t TEXT, b BLOB)").unwrap();
        db
    }

    #[test]
    fn test_json_value() {
        let db = checked_memory_handle();

        let json = r#"{"foo": 13, "bar": "baz"}"#;
        let data: serde_json::Value = serde_json::from_str(json).unwrap();
        db.execute("INSERT INTO foo (t, b) VALUES (?, ?)",
                     &[&data, &json.as_bytes()])
            .unwrap();

        let t: serde_json::Value = db.query_row("SELECT t FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(data, t);
        let b: serde_json::Value = db.query_row("SELECT b FROM foo", &[], |r| r.get(0)).unwrap();
        assert_eq!(data, b);
    }
}
