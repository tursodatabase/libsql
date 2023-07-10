use libsql_core::{params, Connection, Database, Params};

fn setup() -> Connection {
    let db = Database::open(":memory:");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", ())
        .unwrap();
    conn
}

#[test]
fn execute() {
    let conn = setup();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')", ())
        .unwrap();
    let rows = conn.execute("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 1);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");
}

#[test]
fn prepare_and_execute() {
    let conn = setup();
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        ().into(),
    );
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        params![1, "Alice"],
    );
    check_insert(
        &conn,
        "INSERT INTO users (id, name) VALUES (?1, ?2)",
        vec![1.into(), "Alice".into()].into(),
    );
}

fn check_insert(conn: &Connection, sql: &str, params: Params) {
    conn.execute(sql, params).unwrap();
    let rows = conn.execute("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 1);
    assert_eq!(row.get::<&str>(1).unwrap(), "Alice");
}

#[test]
fn nulls() {
    let conn = setup();
    conn.execute("INSERT INTO users (id, name) VALUES (NULL, NULL)", ())
        .unwrap();
    let rows = conn.execute("SELECT * FROM users", ()).unwrap().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 1);
    assert!(row.get::<&str>(1).is_err());
}
