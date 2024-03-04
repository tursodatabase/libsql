#![allow(deprecated)]

use futures::{StreamExt, TryStreamExt};
use libsql::{
    named_params, params,
    params::{IntoParams, IntoValue},
    Connection, Database, Value,
};

async fn setup() -> Connection {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _ = conn
        .execute("CREATE TABLE users (id INTEGER, name TEXT)", ())
        .await;
    conn
}

#[tokio::test]
async fn connection_drops_before_statements() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _stmt = conn.prepare("SELECT 1").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn file_prefix_open() {
    let tempdir = std::env::temp_dir();

    let path = tempdir.join("prefixeddata.db");

    let path = format!("file:{}", path.display());

    let db = Database::open(path).unwrap();
    let conn = db.connect().unwrap();
    let _stmt = conn.prepare("SELECT 1").await.unwrap();
    drop(conn);
}

#[tokio::test]
async fn connection_query() {
    let conn = setup().await;
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .await
        .unwrap();
    let mut rows = conn.query("SELECT * FROM users", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
}

#[tokio::test]
async fn connection_execute_batch() {
    let conn = setup().await;

    conn.execute_batch(
        "BEGIN;
         CREATE TABLE foo(x INTEGER);
         CREATE TABLE bar(y TEXT);
         COMMIT;",
    )
    .await
    .unwrap();

    let mut rows = conn
        .query(
            "SELECT 
                name
            FROM 
                sqlite_schema
            WHERE 
                type ='table' AND 
                name NOT LIKE 'sqlite_%';",
            (),
        )
        .await
        .unwrap();

    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "users");

    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "foo");

    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "bar");
}

#[tokio::test]
async fn connection_execute_batch_inserts() {
    let conn = setup().await;

    conn.execute("CREATE TABLE foo(x INTEGER)", ())
        .await
        .unwrap();

    conn.execute_batch(
        "BEGIN;
        INSERT INTO foo VALUES (1);
        INSERT INTO foo VALUES (2);
        INSERT INTO foo VALUES (3);
        COMMIT;
        ",
    )
    .await
    .unwrap();

    let mut rows = conn.query("SELECT count(*) FROM foo", ()).await.unwrap();

    let count = rows.next().await.unwrap().unwrap().get::<u64>(0).unwrap();
    assert_eq!(count, 3);
}

#[tokio::test]
async fn connection_execute_batch_newline() {
    // This test checks that we handle a null raw
    // stament in execute_batch. What happens when there
    // are no more queries but the sql string is not empty?
    // Well sqlite returns a null statment from prepare_v2
    // so this test checks that we check if the statement is
    // null before we try to step it!
    let conn = setup().await;

    conn.execute_batch(
        "
        create table foo (x INT);
        ",
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn prepare_invalid_sql() {
    let conn = setup().await;
    let result = conn.prepare("SYNTAX ERROR").await;
    assert!(result.is_err());
    let actual = result.err().unwrap();
    match actual {
        libsql::Error::SqliteFailure(code, msg) => {
            assert_eq!(code, 1);
            assert_eq!(msg, "near \"SYNTAX\": syntax error".to_string());
        }
        _ => panic!("Expected SqliteFailure"),
    }
}

#[tokio::test]
async fn statement_query() {
    let conn = setup().await;
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .await
        .unwrap();

    let params = [2];

    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE id = ?1")
        .await
        .unwrap();

    let mut rows = stmt.query(&params).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");

    stmt.reset();

    let row = stmt.query_row(&params).await.unwrap();

    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");

    stmt.reset();

    let rows = stmt.query(&params).await.unwrap();
    let mut names = rows
        .into_stream()
        .boxed()
        .map_ok(|r| r.get::<String>(1).unwrap());

    let name = names.next().await.unwrap().unwrap();

    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn prepare_and_query() {
    let conn = setup().await;
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (2, 'Alice')",
        (),
    )
    .await;
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        params![2u64, "Alice".to_string()],
    )
    .await;
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        params![2, "Alice"],
    )
    .await;
}

#[tokio::test]
async fn prepare_and_query_named_params() {
    let conn = setup().await;

    conn.query("SELECT 1", named_params![]).await.unwrap();
    conn.query("SELECT 1", params![]).await.unwrap();
    conn.query("SELECT 1", ()).await.unwrap();

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (:a, :b)",
        ((":a", 2), (":b", "Alice")),
    )
    .await;

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (:a, :b)",
        named_params! {
            ":a": 2,
            ":b": "Alice"
        },
    )
    .await;

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (:a, :b)",
        named_params! {
            ":a": 2,
            ":b": "Alice",
        },
    )
    .await;

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (@a, @b)",
        named_params! {
            "@a": 2,
            "@b": "Alice",
        },
    )
    .await;

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES ($a, $b)",
        named_params! {
            "$a": 2,
            "$b": "Alice",
        },
    )
    .await;

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES ($a, $b)",
        named_params! {
            "$a": 2,
            "$b": "Alice",
        },
    )
    .await;
}

#[tokio::test]
async fn prepare_and_dont_query() {
    // TODO: how can we check that we've cleaned up the statement?

    let conn = setup().await;

    conn.prepare("INSERT INTO users (id, name) VALUES (?1, ?2)")
        .await
        .unwrap();

    // Drop the connection explicitly here to show that we want to drop
    // it while the above statment has not been queryd.
    drop(conn);
}

async fn check_insert(conn: &Connection, sql: &str, params: impl IntoParams) {
    let _ = conn.execute(sql, params).await.unwrap();
    let mut rows = conn.query("SELECT * FROM users", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    // Use two since if you forget to insert an id it will automatically
    // be set to 1 which defeats the purpose of checking it here.
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
}

#[tokio::test]
async fn nulls() {
    let conn = setup().await;
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (NULL, NULL)", ())
        .await
        .unwrap();
    let mut rows = conn.query("SELECT * FROM users", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert!(row.get::<i32>(0).is_err());
    assert!(row.get::<String>(1).is_err());
}

#[tokio::test]
async fn blob() {
    let conn = setup().await;
    let _ = conn
        .execute("CREATE TABLE bbb (id INTEGER PRIMARY KEY, data BLOB)", ())
        .await
        .unwrap();

    let bytes = vec![2u8; 64];
    let _ = conn
        .execute("INSERT INTO bbb (data) VALUES (?1)", [bytes.clone()])
        .await
        .unwrap();

    let mut rows = conn.query("SELECT * FROM bbb", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();

    let out = row.get::<Vec<u8>>(1).unwrap();
    assert_eq!(&out, &bytes);
}

#[tokio::test]
async fn transaction() {
    let conn = setup().await;
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .await
        .unwrap();
    let tx = conn.transaction().await.unwrap();
    tx.execute("INSERT INTO users (id, name) VALUES (3, 'Bob')", ())
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut rows = conn.query("SELECT * FROM users", ()).await.unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<String>(1).unwrap(), "Alice");
    assert!(rows.next().await.unwrap().is_none());
}

#[tokio::test]
async fn custom_params() {
    let conn = setup().await;

    enum MyValue {
        Text(String),
        Int(i64),
    }

    impl IntoValue for MyValue {
        fn into_value(self) -> libsql::Result<Value> {
            match self {
                MyValue::Text(s) => Ok(Value::Text(s)),
                MyValue::Int(i) => Ok(Value::Integer(i)),
            }
        }
    }

    let params = vec![MyValue::Int(2), MyValue::Text("Alice".into())];

    conn.execute(
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        libsql::params_from_iter(params),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn debug_print_row() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _ = conn
        .execute(
            "CREATE TABLE users (id INTEGER, name TEXT, score REAL, data BLOB, age INTEGER)",
            (),
        )
        .await;
    conn.execute("INSERT INTO users (id, name, score, data, age) VALUES (123, 'potato', 3.14, X'deadbeef', NULL)", ())
    .await
    .unwrap();

    let mut stmt = conn.prepare("SELECT * FROM users").await.unwrap();
    let mut rows = stmt.query(()).await.unwrap();
    assert_eq!(
        format!("{:?}", rows.next().await.unwrap().unwrap()),
        "{Some(\"id\"): (Integer, 123), Some(\"name\"): (Text, \"potato\"), Some(\"score\"): (Real, 3.14), Some(\"data\"): (Blob, 4), Some(\"age\"): (Null, ())}"
    );
}

#[cfg(feature = "serde")]
#[tokio::test]
async fn deserialize_row() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _ = conn
        .execute(
            "CREATE TABLE users (id INTEGER, name TEXT, score REAL, data BLOB, age INTEGER)",
            (),
        )
        .await;
    conn.execute("INSERT INTO users (id, name, score, data, age) VALUES (123, 'potato', 42.0, X'deadbeef', NULL)", ())
    .await
    .unwrap();

    use serde::Deserialize;

    #[derive(Deserialize, Debug)]
    struct Data {
        id: i64,
        name: String,
        score: f64,
        data: Vec<u8>,
        age: Option<i64>,
        none: Option<()>,
    }

    let row = conn
        .query("SELECT * FROM users", ())
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let data: Data = libsql::de::from_row(&row).unwrap();
    assert_eq!(data.id, 123);
    assert_eq!(data.name, "potato".to_string());
    assert_eq!(data.score, 42.0);
    assert_eq!(data.data, vec![0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(data.age, None);
    assert_eq!(data.none, None)
}
