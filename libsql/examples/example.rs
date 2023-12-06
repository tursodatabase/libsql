use libsql::Database;

#[tokio::main]
async fn main() {
    let db = if let Ok(url) = std::env::var("LIBSQL_URL") {
        let token = std::env::var("LIBSQL_AUTH_TOKEN").unwrap_or_else(|_| {
            println!("LIBSQL_TOKEN not set, using empty token...");
            "".to_string()
        });

        Database::open_remote(url, token).unwrap()
    } else {
        Database::open_in_memory().unwrap()
    };

    let conn = db.connect().unwrap();

    conn.query("select 1; select 1;", ()).await.unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();

    let mut stmt = conn
        .prepare("INSERT INTO users (email) VALUES (?1)")
        .await
        .unwrap();
    conn.execute(
        "CREATE TABLE test1 (t TEXT, i INTEGER, f FLOAT, b BLOB)",
        (),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test1 (t, i, f, b) VALUES (?, ?, ?, ?)",
        ("a", 1, 1.0, vec![1, 2, 3]),
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO test1 (t, i, f, b) VALUES (?, ?, ?, ?)",
        ("b", 2_u64, 2.0, vec![4, 5, 6]),
    )
    .await
    .unwrap();
    let mut rows = conn.query("SELECT * FROM test1", ()).await.unwrap();
    while let Ok(Some(row)) = rows.next().await {
        println!(
            "{:?} {:?} {:?} {:?}",
            row.get_value(0),
            row.get_value(1),
            row.get_value(2),
            row.get_value(3)
        );
    }

    let mut rows = conn.query("SELECT * FROM test1", ()).await.unwrap();

    let row = rows.next().await.unwrap().unwrap();

    let mut stmt = conn
        .prepare("SELECT * FROM users WHERE email = ?1")
        .await
        .unwrap();

    let mut rows = stmt.query(["foo@example.com"]).await.unwrap();

    let row = rows.next().unwrap().unwrap();

    let value = row.get_value(0).unwrap();

    println!("Row: {:?}", value);
}
