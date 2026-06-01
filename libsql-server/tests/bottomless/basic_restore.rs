use super::fixtures::{MinioFixture, SqldFixture, TestDatabase};
use std::time::Duration;

#[tokio::test]
async fn test_basic_restore() {
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
    db.insert_test_data(100)
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

    // Phase 3: Start sqld - should restore from minio
    sqld.start(data_dir.path())
        .await
        .expect("Failed to restart sqld");
    let endpoint2 = sqld.http_endpoint();
    sqld.wait_for_ready(Duration::from_secs(60))
        .await
        .expect("sqld did not become ready after restore");

    // Phase 4: Verify database is intact
    let db2 = TestDatabase::new(endpoint2);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let restored_data = db2.query_all().await.expect("Failed to query data");
    assert_eq!(restored_data.len(), 100, "Expected 100 rows after restore");

    db2.verify_integrity()
        .await
        .expect("Data integrity check failed");

    sqld.cleanup().await.ok();
    minio.cleanup().await.ok();
}
