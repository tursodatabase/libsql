use libsql::Database;

#[tokio::main]
async fn main() {
    let db = Database::open(":memory:").unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE IF NOT EXISTS users (email TEXT)", ())
        .await
        .unwrap();
    conn.execute("INSERT INTO users (email) VALUES ('alice@example.org')", ())
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
    while let Ok(Some(row)) = rows.next() {
        println!(
            "{:?} {:?} {:?} {:?}",
            row.get_value(0),
            row.get_value(1),
            row.get_value(2),
            row.get_value(3)
        );
    }
}
