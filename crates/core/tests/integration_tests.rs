#[test]
fn simple_usage() {
    let db = libsql_core::Database::open(":memory:");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        .unwrap();
    let stmt = conn.prepare("SELECT * FROM users").unwrap();
    let rows = stmt.execute().unwrap();
    let row = rows.next().unwrap().unwrap();
    assert_eq!(row.get::<i32>(0).unwrap(), 1);
}
