#[test]
fn simple_usage() {
    let db = libsql_core::Database::open(":memory:");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE foo (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
    conn.execute("INSERT INTO foo (name) VALUES ('Alice')").unwrap();
}