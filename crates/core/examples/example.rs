use libsql::Database;

fn main() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .unwrap();
    conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ())
        .unwrap();
}
