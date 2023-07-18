use libsql::Database;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    std::fs::create_dir("data.libsql").ok();
    std::fs::copy("tests/template.db", "data.libsql/data").unwrap();

    let opts = libsql::Opts::with_rpc_sync("http://localhost:5001".to_owned());
    let db = Database::open_with_opts("test.db", opts).await.unwrap();
    let conn = db.connect().unwrap();

    let db = std::sync::Arc::new(parking_lot::Mutex::new(db));
    loop {
        if let Err(e) = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.lock().sync()
        })
        .await
        {
            println!("Error: {e}");
            break;
        };
        let response = conn.execute("SELECT * FROM sqlite_master", ()).unwrap();
        let rows = match response {
            Some(rows) => rows,
            None => {
                println!("No rows");
                continue;
            }
        };
        while let Ok(Some(row)) = rows.next() {
            println!(
                "| {:024} | {:024} | {:024} | {:024} |",
                row.get::<&str>(0).unwrap(),
                row.get::<&str>(1).unwrap(),
                row.get::<&str>(2).unwrap(),
                row.get::<&str>(3).unwrap(),
            );
        }
    }
}
