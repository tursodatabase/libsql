use libsql::replication::{Frames, TempSnapshot};
use libsql::Database;
use std::sync::{Arc, Mutex};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let db = Arc::new(Mutex::new(
        Database::open_with_local_sync("test.db").await.unwrap(),
    ));
    let conn = db.lock().unwrap().connect().unwrap();

    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        println!("Usage: {} <snapshots path>", args[0]);
        return;
    }
    let snapshots_path = args.get(1).unwrap();

    loop {
        let paths = std::fs::read_dir(snapshots_path).unwrap();
        for snapshot_path in paths {
            let db = db.clone();
            let snapshot_path = snapshot_path.unwrap().path();
            println!(
                "Applying snapshot to local database: {}\n",
                snapshot_path.display()
            );
            let snapshot = TempSnapshot::from_snapshot_file(snapshot_path.as_ref()).unwrap();
            tokio::task::spawn_blocking(move || {
                match db.lock().unwrap().sync_frames(Frames::Snapshot(snapshot)) {
                    Ok(n) => println!("{n} frames from {} applied", snapshot_path.display()),
                    Err(e) => println!(
                        "Syncing frames from {} failed: {e}",
                        snapshot_path.display()
                    ),
                }
            })
            .await
            .unwrap();
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
        println!("Sleeping for 5 seconds ...");
        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}
