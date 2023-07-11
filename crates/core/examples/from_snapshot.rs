use libsql_core::Database;
use libsql_replication::{Frames, TempSnapshot};

fn main() {
    tracing_subscriber::fmt::init();

    let mut db = Database::with_replicator("test.db");
    let conn = db.connect().unwrap();

    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 2 {
        println!("Usage: {} <snapshot path>", args[0]);
        return;
    }
    let snapshot_path = args.get(1).unwrap();
    let snapshot = TempSnapshot::from_snapshot_file(snapshot_path.as_ref()).unwrap();

    db.sync(Frames::Snapshot(snapshot)).unwrap();

    let rows = conn
        .execute("SELECT * FROM sqlite_master", ())
        .unwrap()
        .unwrap();
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
