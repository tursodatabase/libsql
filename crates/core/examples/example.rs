use libsql_core::Database;

fn main() {
    let db = Database::open(":memory:");
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)").wait().unwrap();
    conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')").wait().unwrap();
}
