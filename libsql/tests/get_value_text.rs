#![allow(deprecated)]

// Regression tests for tursodatabase/libsql#2285:
// local Row::get_value must preserve embedded NULs in TEXT and must return
// a decoding error (not panic) for malformed UTF-8.
use libsql::{Database, Error, Value};

async fn setup() -> libsql::Connection {
    let db = Database::open(":memory:").unwrap();
    db.connect().unwrap()
}

#[tokio::test]
async fn get_value_preserves_embedded_nul_in_text() {
    let conn = setup().await;

    let mut rows = conn
        .query("SELECT ?1", [Value::Text("a\0b".to_string())])
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("a\0b".to_string()));
    drop(row);
    drop(rows);

    // distinct keys with NULs stay distinct through the owned conversion
    let mut rows = conn
        .query("SELECT ?1", [Value::Text("a".to_string())])
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("a".to_string()));
}

#[tokio::test]
async fn get_value_rejects_malformed_utf8_without_panicking() {
    let conn = setup().await;

    let mut rows = conn
        .query("SELECT CAST(x'80' AS TEXT)", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    let err = row.get_value(0).unwrap_err();
    assert!(
        matches!(err, Error::InvalidColumnType),
        "unexpected error: {err:?}"
    );
    drop(row);
    drop(rows);

    // connection is still usable afterwards
    let mut rows = conn.query("SELECT 'ok'", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("ok".into()));
}

#[tokio::test]
async fn get_value_handles_empty_text_unicode_and_other_types() {
    let conn = setup().await;

    let mut rows = conn
        .query("SELECT '', 'héllo 🌍', 42, 1.5, x'deadbeef', NULL", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get_value(0).unwrap(), Value::Text("".into()));
    assert_eq!(
        row.get_value(1).unwrap(),
        Value::Text("héllo 🌍".to_string())
    );
    assert_eq!(row.get_value(2).unwrap(), Value::Integer(42));
    assert_eq!(row.get_value(3).unwrap(), Value::Real(1.5));
    assert_eq!(
        row.get_value(4).unwrap(),
        Value::Blob(vec![0xde, 0xad, 0xbe, 0xef])
    );
    assert_eq!(row.get_value(5).unwrap(), Value::Null);
}
