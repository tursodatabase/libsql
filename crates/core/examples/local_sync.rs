use libsql::Database;
use libsql_replication::{Frames, TempSnapshot};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db = Database::open_with_local_sync("test.db").await.unwrap();
    let conn = db.connect().unwrap();

    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        println!("Usage: {} <snapshot path>", args[0]);
        return;
    }
    let snapshot_path = args.get(1).unwrap();
    let snapshot = TempSnapshot::from_snapshot_file(snapshot_path.as_ref()).unwrap();

    tokio::task::spawn_blocking(move || {
        db.sync_frames(Frames::Snapshot(snapshot)).unwrap();
    })
    .await
    .unwrap();

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
