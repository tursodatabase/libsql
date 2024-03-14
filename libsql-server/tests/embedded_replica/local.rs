use std::time::Duration;

use libsql::{replication::Frames, Database};
use libsql_replication::snapshot::SnapshotFile;
use serde_json::json;
use tempfile::tempdir;
use turmoil::Builder;

use crate::common::{http::Client, net::TurmoilConnector};

use super::make_primary;

#[test]
fn local_sync_with_writes() {
    let mut sim = Builder::new()
        .simulation_duration(Duration::from_secs(120))
        .build();

    let tmp_embedded = tempdir().unwrap();
    let tmp_host = tempdir().unwrap();
    let tmp_embedded_path = tmp_embedded.path().to_owned();
    let tmp_host_path = tmp_host.path().to_owned();

    make_primary(&mut sim, tmp_host_path.clone());

    sim.client("client", async move {
        let client = Client::new();
        client
            .post("http://primary:9090/v1/namespaces/foo/create", json!({}))
            .await?;

        println!("{:?}", tmp_host_path);

        let _path = tmp_embedded_path.join("embedded");

        let primary =
            Database::open_remote_with_connector("http://foo.primary:8080", "", TurmoilConnector)?;
        let conn = primary.connect()?;

        // Do enough writes to ensure that we can force the server to write some snapshots
        conn.execute("create table test (x)", ()).await.unwrap();
        for _ in 0..233 {
            conn.execute("insert into test values (randomblob(4092))", ())
                .await
                .unwrap();
        }

        let snapshots_path = tmp_host_path.join("dbs").join("foo").join("snapshots");

        let mut dir = tokio::fs::read_dir(snapshots_path).await.unwrap();

        let mut snapshots = Vec::new();

        while let Some(snapshot) = dir.next_entry().await.unwrap() {
            let snap = SnapshotFile::open(snapshot.path(), None).await.unwrap();

            snapshots.push(snap);
        }

        snapshots.sort_by(|a, b| {
            a.header()
                .start_frame_no
                .get()
                .cmp(&b.header().start_frame_no.get())
        });

        let db = Database::open_with_local_sync_remote_writes_connector(
            tmp_host_path.join("embedded").to_str().unwrap(),
            "http://foo.primary:8080".to_string(),
            "".to_string(),
            TurmoilConnector,
            None,
        )
        .await?;

        for snapshot in snapshots {
            println!("snapshots: {:?}", snapshot.header().end_frame_no.get());
            db.sync_frames(Frames::Snapshot(snapshot)).await.unwrap();
        }

        let conn = db.connect()?;

        let row = conn
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let count = row.get::<u64>(0).unwrap();

        assert_eq!(count, 233);

        tracing::info!("executing write delegated inserts");

        // Attempt to write and ensure it writes only to the primary
        for _ in 0..300 {
            conn.execute("insert into test values (randomblob(4092))", ())
                .await
                .unwrap();
        }

        // Verify no new writes were done locally
        let row = conn
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let count = row.get::<u64>(0).unwrap();
        assert_eq!(count, 233);

        let snapshots_path = tmp_host_path.join("dbs").join("foo").join("snapshots");

        let mut dir = tokio::fs::read_dir(snapshots_path).await.unwrap();

        let mut snapshots = Vec::new();

        while let Some(snapshot) = dir.next_entry().await.unwrap() {
            let snap = SnapshotFile::open(snapshot.path(), None).await.unwrap();

            snapshots.push(snap);
        }

        snapshots.sort_by(|a, b| {
            a.header()
                .start_frame_no
                .get()
                .cmp(&b.header().start_frame_no.get())
        });

        for snapshot in snapshots.into_iter() {
            println!("snapshots: {:?}", snapshot.header().end_frame_no.get());
            db.sync_frames(Frames::Snapshot(snapshot)).await.unwrap();
        }

        let conn = db.connect()?;

        let row = conn
            .query("select count(*) from test", ())
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap();
        let count = row.get::<u64>(0).unwrap();

        assert_eq!(count, 467);

        Ok(())
    });

    sim.run().unwrap();
}
