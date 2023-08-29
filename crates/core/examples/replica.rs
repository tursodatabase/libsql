use libsql::v2::Database;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db_file = tempfile::NamedTempFile::new().unwrap();
    println!("Database {}", db_file.path().display());

    let auth_token = std::env::var("TURSO_AUTH_TOKEN").expect("Expected a TURSO_AUTH_TOKEN");

    let db = Database::open_with_sync(
        db_file.path().to_str().unwrap(),
        "http://localhost:8080",
        auth_token,
    )
    .await
    .unwrap();
    let conn = db.connect().await.unwrap();

    loop {
        match db.sync().await {
            Ok(frames_applied) => {
                if frames_applied == 0 {
                    println!("No more frames at the moment! See you later");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    continue;
                }
                println!("Applied {frames_applied} frames");
            }
            Err(e) => {
                println!("Error: {e}");
                break;
            }
        }

        let mut rows = conn.query("SELECT * FROM sqlite_master", ()).await.unwrap();

        while let Ok(Some(row)) = rows.next() {
            println!(
                "| {:024} | {:024} | {:024} | {:024} |",
                row.get_str(0).unwrap(),
                row.get_str(1).unwrap(),
                row.get_str(2).unwrap(),
                row.get_str(3).unwrap(),
            );
        }
    }
}
