use libsql_core::Database;
use libsql_replication::{Context, Frame, FrameHeader, Frames, Replicator};

fn frame_data_offset(frame_no: u64) -> u64 {
    tracing::debug!(
        "WAL offset: {frame_no}->{}",
        32 + (frame_no - 1) * (24 + 4096) + 24
    );
    32 + (frame_no - 1) * (24 + 4096) + 24
}

fn test_frame(frame_no: u64) -> Frame {
    let header = FrameHeader {
        frame_no,
        checksum: 0xdeadc0de,
        page_no: frame_no as u32,
        size_after: frame_no as u32,
    };

    let loaded = {
        use std::io::{Read, Seek};
        let mut f = std::fs::File::open("tests/template.db-wal").unwrap();
        f.seek(std::io::SeekFrom::Start(frame_data_offset(frame_no)))
            .unwrap();
        let mut buf = vec![0; 4096];
        f.read_exact(&mut buf).unwrap();
        buf
    };

    Frame::from_parts(&header, &loaded)
}

fn main() {
    tracing_subscriber::fmt::init();

    std::fs::create_dir("data.libsql").ok();
    std::fs::copy("tests/template.db", "data.libsql/data").unwrap();

    let db = Database::open("data.libsql/data");
    let conn = db.connect().unwrap();

    let Context {
        mut hook_ctx,
        frames_sender,
        current_frame_no_notifier,
        meta: _,
    } = Replicator::create_context("data.libsql").unwrap();

    // Initialize the replicator
    let mut replicator = libsql_replication::Replicator::new(
        "data.libsql",
        &mut hook_ctx,
        frames_sender,
        current_frame_no_notifier,
    )
    .unwrap();

    let sync_result = replicator.sync(Frames::Vec(vec![test_frame(1), test_frame(2)]));
    println!("sync result: {:?}", sync_result);
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
