use libsql::Database;

#[tokio::main]
async fn main() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().await.unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ())
        .await
        .unwrap();
}
