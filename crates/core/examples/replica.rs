use libsql::{Database, Value};
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db_file = tempfile::NamedTempFile::new().unwrap();
    println!("Database {}", db_file.path().display());

    let auth_token = std::env::var("TURSO_AUTH_TOKEN").expect("Expected a TURSO_AUTH_TOKEN");

    let db = Database::open_with_remote_sync(
        db_file.path().to_str().unwrap(),
        "http://localhost:8080",
        auth_token,
    )
    .await
    .unwrap();
    let conn = db.connect().unwrap();

    let f = db.sync().await.unwrap();
    println!("inital sync complete, frame no: {}", f);

    conn.execute("CREATE TABLE IF NOT EXISTS foo (x TEXT)", ())
        .await
        .unwrap();

    db.sync().await.unwrap();

    let mut jh = tokio::spawn(async move {
        conn.execute(
            "INSERT INTO foo (x) VALUES (?1)",
            vec![Value::from(
                "this value was written by an embedded replica!",
            )],
        )
        .await
        .unwrap();

        let mut rows = conn.query("SELECT * FROM foo", ()).await.unwrap();

        while let Some(row) = rows.next().unwrap() {
            println!("Row: {}", row.get_str(0).unwrap());
        }
    });

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                let r = db.sync().await.unwrap();
                println!("{} frames have been applied", r);
            }

            r = &mut jh => {
                r.unwrap();
                return;
            }
        }
    }
}
