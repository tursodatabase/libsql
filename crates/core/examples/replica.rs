use libsql_core::Database;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let db = Database::open("libsql://localhost:8888").unwrap();
    let conn = db.connect().unwrap();
    tokio::spawn(db.replicator.unwrap().run());
    loop {
        println!("rows: {:?}", conn.execute("SELECT * FROM t", ()).ok());
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
