use libsql::{Database, Value};
use std::time::Duration;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db_file = tempfile::NamedTempFile::new().unwrap();
    println!("Database {}", db_file.path().display());

    let auth_token = std::env::var("TURSO_AUTH_TOKEN").unwrap_or_else(|_| {
        println!("Using empty token since TURSO_AUTH_TOKEN was not set");
        "".to_string()
    });

    let db = Database::open_with_remote_sync(
        db_file.path().to_str().unwrap(),
        "http://localhost:8080",
        auth_token,
    )
    .await
    .unwrap();
    let conn = db.connect().unwrap();

    let f = db.sync().await.unwrap();
    println!("inital sync complete, frame no: {f:?}");

    conn.execute("CREATE TABLE IF NOT EXISTS foo (x TEXT)", ())
        .await
        .unwrap();

    db.sync().await.unwrap();

    let mut jh = tokio::spawn(async move {
        let mut rows = conn
            .query(
                "INSERT INTO foo (x) VALUES (?1) RETURNING *",
                vec![Value::from(
                    "this value was written by an embedded replica!",
                )],
            )
            .await
            .unwrap();

        println!("Rows insert call");
        while let Some(row) = rows.next().unwrap() {
            println!("Row: {}", row.get_str(0).unwrap());
        }

        println!("--------");

        let mut rows = conn.query("SELECT * FROM foo", ()).await.unwrap();

        println!("Rows coming from a read after write call");
        while let Some(row) = rows.next().unwrap() {
            println!("Row: {}", row.get_str(0).unwrap());
        }

        println!("--------");
    });

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                let r = db.sync().await.unwrap();
                println!("replicated until index {r:?}");
            }

            r = &mut jh => {
                r.unwrap();
                return;
            }
        }
    }
}
