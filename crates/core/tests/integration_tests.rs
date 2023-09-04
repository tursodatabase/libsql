use libsql::{named_params, params, Connection, Database, Params, Value};

fn setup() -> Connection {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _ = conn.execute("CREATE TABLE users (id INTEGER, name TEXT)", ());
    conn
}

#[test]
fn connection_drops_before_statements() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    let _stmt = conn.prepare("SELECT 1").unwrap();
    drop(conn);
}

#[test]
fn connection_query() {
    let conn = setup();
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .unwrap();
    let rows = conn.query("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");
}

#[test]
fn connection_execute_batch() {
    let conn = setup();

    conn.execute_batch(
        "BEGIN;
         CREATE TABLE foo(x INTEGER);
         CREATE TABLE bar(y TEXT);
         COMMIT;",
    )
    .unwrap();

    let rows = conn
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
        .unwrap()
        .unwrap();

    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<&str>(0).unwrap(), "users");

    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<&str>(0).unwrap(), "foo");

    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<&str>(0).unwrap(), "bar");
}

#[test]
fn connection_execute_batch_newline() {
    // This test checks that we handle a null raw
    // stament in execute_batch. What happens when there
    // are no more queries but the sql string is not empty?
    // Well sqlite returns a null statment from prepare_v2
    // so this test checks that we check if the statement is
    // null before we try to step it!
    let conn = setup();

    conn.execute_batch(
        "
        create table foo (x INT);
        ",
    )
    .unwrap()
}

#[test]
fn statement_query() {
    let conn = setup();
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .unwrap();

    let params = Params::from(vec![libsql::Value::from(2)]);

    let stmt = conn.prepare("SELECT * FROM users WHERE id = ?1").unwrap();

    let rows = stmt.query(&params).unwrap();
    let row = rows.next().unwrap().unwrap();

    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");

    stmt.reset();

    let row = stmt.query_row(&params).unwrap();

    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");

    stmt.reset();

    let mut names = stmt
        .query_map(&params, |r| r.get::<&str>(1).map(str::to_owned))
        .unwrap();

    let name = names.next().unwrap().unwrap();

    assert_eq!(name, "Alice");
}

#[test]
fn prepare_and_query() {
    let conn = setup();
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (2, 'Alice')",
        ().into(),
    );
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        params![2, "Alice"],
    );
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        (vec![2.into(), "Alice".into()] as Vec<params::Value>).into(),
    );
}

#[test]
fn prepare_and_query_named_params() {
    let conn = setup();

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (:a, :b)",
        vec![
            (":a".to_string(), 2.into()),
            (":b".to_string(), "Alice".into()),
        ]
        .into(),
    );

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (:a, :b)",
        named_params! {
            ":a": 2,
            ":b": "Alice",
        },
    );

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (@a, @b)",
        vec![
            ("@a".to_string(), 2.into()),
            ("@b".to_string(), "Alice".into()),
        ]
        .into(),
    );
    
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (@a, @b)",
        named_params! {
            "@a": 2,
            "@b": "Alice",
        },
    );

    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES ($a, $b)",
        vec![
            ("$a".to_string(), 2.into()),
            ("$b".to_string(), "Alice".into()),
        ]
        .into(),
    );
    
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES ($a, $b)",
        named_params! {
            "$a": 2,
            "$b": "Alice",
        },
    );
}

#[test]
fn prepare_and_dont_query() {
    // TODO: how can we check that we've cleaned up the statement?

    let conn = setup();

    conn.prepare("INSERT INTO users (id, name) VALUES (?1, ?2)")
        .unwrap();

    // Drop the connection explicitly here to show that we want to drop
    // it while the above statment has not been queryd.
    drop(conn);
}

fn check_insert(conn: &Connection, sql: &str, params: Params) {
    let _ = conn.execute(sql, params).unwrap();
    let rows = conn.query("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    // Use two since if you forget to insert an id it will automatically
    // be set to 1 which defeats the purpose of checking it here.
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");
}

#[test]
fn nulls() {
    let conn = setup();
    let _ = conn
        .execute("INSERT INTO users (id, name) VALUES (NULL, NULL)", ())
        .unwrap();
    let rows = conn.query("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 0);
    assert!(row.get::<&str>(1).is_err());
}

#[test]
fn blob() {
    let conn = setup();
    let _ = conn
        .execute("CREATE TABLE bbb (id INTEGER PRIMARY KEY, data BLOB)", ())
        .unwrap();

    let bytes = vec![2u8; 64];
    let value = Value::from(bytes.clone());
    let _ = conn
        .execute("INSERT INTO bbb (data) VALUES (?1)", vec![value])
        .unwrap();

    let rows = conn.query("SELECT * FROM bbb", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();

    let out = row.get::<Vec<u8>>(1).unwrap();
    assert_eq!(&out, &bytes);
}

#[test]
fn transaction() {
    let conn = setup();
    conn.execute("INSERT INTO users (id, name) VALUES (2, 'Alice')", ())
        .unwrap();
    let tx = conn.transaction().unwrap();
    tx.execute("INSERT INTO users (id, name) VALUES (3, 'Bob')", ())
        .unwrap();
    tx.rollback().unwrap();
    let rows = conn.query("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 2);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");
    assert!(rows.next().unwrap().is_none());
}

#[test]
fn custom_params() {
    let conn = setup();

    enum MyValue {
        Text(String),
        Int(i64),
    }

    impl TryInto<libsql::Value> for MyValue {
        type Error = std::io::Error;

        fn try_into(self) -> Result<libsql::Value, Self::Error> {
            match self {
                MyValue::Text(s) => Ok(Value::Text(s)),
                MyValue::Int(i) => Ok(Value::Integer(i)),
            }
        }
    }

    let params = vec![MyValue::Int(2), MyValue::Text("Alice".into())];

    conn.execute(
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        libsql::params_from_iter(params).unwrap(),
    )
    .unwrap();
}
