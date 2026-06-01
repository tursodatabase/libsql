use super::fixtures::{MinioFixture, SqldFixture, TestDatabase};
use std::time::Duration;

#[tokio::test]
async fn test_restore_completes_after_network_partition() {
    let _ = tracing_subscriber::fmt::try_init();

    let minio = MinioFixture::start().await.expect("Failed to start minio");

    let data_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let mut sqld = SqldFixture::new(&minio);

    // Phase 1: Create database and replicate to minio
    sqld.start(data_dir.path())
        .await
        .expect("Failed to start sqld");
    sqld.wait_for_ready(Duration::from_secs(30))
        .await
        .expect("sqld did not become ready");

    let endpoint = sqld.http_endpoint();
    let db = TestDatabase::new(endpoint.clone());
    db.create_schema().await.expect("Failed to create schema");
    db.insert_test_data(1000)
        .await
        .expect("Failed to insert data");
    db.wait_for_replication()
        .await
        .expect("Failed to wait for replication");

    sqld.stop().await.expect("Failed to stop sqld");

    // Phase 2: Delete local database files to force restore
    sqld.cleanup_data_dir(data_dir.path())
        .await
        .expect("Failed to cleanup dbs dir");

    sqld.start(data_dir.path())
        .await
        .expect("Failed to start sqld for restore");

    // Wait for restore to begin
    sqld.wait_for_restore_start()
        .await
        .expect("sqld did not start restoring");

    // Give restore a moment to progress
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Phase 3: Simulate network partition by disconnecting sqld from the Docker network
    let disconnect_output = tokio::process::Command::new("docker")
        .args([
            "network",
            "disconnect",
            &minio.network_name,
            &sqld.container_name,
        ])
        .output()
        .await
        .expect("Failed to disconnect sqld from network");
    if !disconnect_output.status.success() {
        panic!(
            "Failed to disconnect sqld from network: {}",
            String::from_utf8_lossy(&disconnect_output.stderr)
        );
    }

    // Wait a bit while sqld is partitioned
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Phase 4: Reconnect sqld to the network (partition heals)
    let connect_output = tokio::process::Command::new("docker")
        .args([
            "network",
            "connect",
            &minio.network_name,
            &sqld.container_name,
        ])
        .output()
        .await
        .expect("Failed to reconnect sqld to network");
    if !connect_output.status.success() {
        panic!(
            "Failed to reconnect sqld to network: {}",
            String::from_utf8_lossy(&connect_output.stderr)
        );
    }

    // Phase 5: sqld should recover and complete restore without restart
    sqld.wait_for_ready(Duration::from_secs(120))
        .await
        .expect("sqld did not become ready after network partition healed");

    // Phase 6: Verify database is intact
    let endpoint2 = sqld.http_endpoint();
    let db2 = TestDatabase::new(endpoint2);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let restored_data = db2.query_all().await.expect("Failed to query data");
    assert_eq!(
        restored_data.len(),
        1000,
        "Expected 1000 rows after network partition"
    );

    db2.verify_integrity()
        .await
        .expect("Data integrity check failed after network partition");

    sqld.cleanup().await.ok();
    minio.cleanup().await.ok();
}
