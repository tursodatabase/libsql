use libsql::v2::Database;

#[tokio::main]
async fn main() {
    let db = if let Ok(url) = std::env::var("LIBSQL_HRANA_URL") {
        Database::open_remote(url).unwrap()
    } else {
        Database::open_in_memory().unwrap()
    };

    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();

    let stmt = conn
        .prepare("INSERT INTO users (email) VALUES (?1)")
        .await
        .unwrap();

    stmt.execute(&libsql::params!["foo@example.com"])
        .await
        .unwrap();

    let stmt = conn
        .prepare("SELECT * FROM users WHERE email = ?1")
        .await
        .unwrap();

    let mut rows = stmt
        .query(&libsql::params!["foo@example.com"])
        .await
        .unwrap();

    let row = rows.next().unwrap().unwrap();

    let value = row.get_value(0).unwrap();

    println!("Row: {:?}", value);
}
