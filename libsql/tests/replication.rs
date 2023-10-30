use libsql::{Database, Frames};
use libsql_replication::{
    frame::{FrameBorrowed, FrameHeader, FrameMut},
    LIBSQL_PAGE_SIZE,
};

const DB: &[u8] = include_bytes!("test.db");

#[tokio::test]
async fn inject_frames() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open_with_local_sync(tmp.path().join("data").to_str().unwrap())
        .await
        .unwrap();

    let mut frames: Vec<FrameMut> = DB
        .chunks(LIBSQL_PAGE_SIZE)
        .enumerate()
        .map(|(i, data)| {
            let header = FrameHeader {
                frame_no: i as _,
                checksum: 0,
                page_no: i as u32 + 1,
                size_after: 0,
            };
            FrameBorrowed::from_parts(&header, data).into()
        })
        .collect();

    frames.last_mut().unwrap().header_mut().size_after = frames.len() as _;

    let frames = frames.into_iter().map(Into::into).collect();

    assert_eq!(
        db.sync_frames(libsql::Frames::Vec(frames))
            .await
            .unwrap()
            .unwrap(),
        2
    );

    let conn = db.connect().unwrap();
    let mut rows = conn.query("select count(*) from test", ()).await.unwrap();
    assert_eq!(
        *rows
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap(),
        10
    );

    // inject the same frames again, this should be idempotent
    let mut frames: Vec<FrameMut> = DB
        .chunks(LIBSQL_PAGE_SIZE)
        .enumerate()
        .map(|(i, data)| {
            let header = FrameHeader {
                frame_no: i as u64 + 3,
                checksum: 0,
                page_no: i as u32 + 1,
                size_after: 0,
            };
            FrameBorrowed::from_parts(&header, data).into()
        })
        .collect();

    frames.last_mut().unwrap().header_mut().size_after = frames.len() as _;

    let frames = frames.into_iter().map(Into::into).collect();

    assert_eq!(
        db.sync_frames(libsql::Frames::Vec(frames))
            .await
            .unwrap()
            .unwrap(),
        5
    );

    let conn = db.connect().unwrap();
    let mut rows = conn.query("select count(*) from test", ()).await.unwrap();
    assert_eq!(
        *rows
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap(),
        10
    );
}

#[tokio::test]
async fn inject_frames_split_txn() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open_with_local_sync(tmp.path().join("data").to_str().unwrap())
        .await
        .unwrap();

    let mut frames = DB.chunks(LIBSQL_PAGE_SIZE).enumerate().map(|(i, data)| {
        let header = FrameHeader {
            frame_no: i as _,
            checksum: 0,
            page_no: i as u32 + 1,
            size_after: 0,
        };
        FrameBorrowed::from_parts(&header, data)
    });

    let conn = db.connect().unwrap();
    assert!(conn.query("select count(*) from test", ()).await.is_err());

    assert!(db
        .sync_frames(Frames::Vec(vec![frames.next().unwrap().into()]))
        .await
        .unwrap()
        .is_none());
    assert!(db.flush_replicator().await.unwrap().is_none());
    assert!(conn.query("select count(*) from test", ()).await.is_err());

    assert!(db
        .sync_frames(Frames::Vec(vec![frames.next().unwrap().into()]))
        .await
        .unwrap()
        .is_none());
    assert!(db.flush_replicator().await.unwrap().is_none());
    assert!(conn.query("select count(*) from test", ()).await.is_err());

    // commit frame
    assert_eq!(
        db.sync_frames(Frames::Vec(vec![frames
            .next()
            .map(|mut f| {
                f.header_mut().size_after = 3;
                f
            })
            .unwrap()
            .into()]))
            .await
            .unwrap()
            .unwrap(),
        2
    );
    assert_eq!(db.flush_replicator().await.unwrap().unwrap(), 2);
    let mut rows = conn.query("select count(*) from test", ()).await.unwrap();
    assert_eq!(
        *rows
            .next()
            .unwrap()
            .unwrap()
            .get_value(0)
            .unwrap()
            .as_integer()
            .unwrap(),
        10
    );
}
