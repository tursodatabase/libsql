#[test]
fn simple_usage() {
    let db = libsql_core::Database::open(":memory:");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (name TEXT)").unwrap();
    //conn.execute("INSERT INTO foo (name) VALUES ('Alice')").unwrap();
    //let stmt = conn.prepare("SELECT * FROM foo").unwrap();
    //let rows = stmt.execute().unwrap();
    //let row = rows.next().unwrap().unwrap();
    //assert_eq!(row.get::<i32>(0).unwrap(), 1);
}
