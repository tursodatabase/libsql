use crate::{run_server, Config};
use anyhow::Result;
use libsql_client::{Connection, QueryResult, Value};
use reqwest::StatusCode;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;
use tracing_test::traced_test;
use url::Url;

#[tokio::test]
#[traced_test]
async fn backup_restore() {
    const BUCKET: &str = "testbackuprestore";
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;

    // assert that MinIO (S3 mockup) is up and doesn't keep data from previous test run
    assert_minio_ready().await;
    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let connection_addr = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let db_config = Config {
        bottomless_replication: Some(bottomless::replicator::Options {
            create_bucket_if_not_exists: true,
            verify_crc: false,
            use_compression: false,
            bucket_name: BUCKET.to_string(),
            ..Default::default()
        }),
        db_path: PATH.into(),
        http_addr: Some(listener_addr),
        ..Config::default()
    };

    {
        // 1: create a local database, fill it with data, wait for WAL backup
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config.clone()));

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE t(id)", "INSERT INTO t VALUES (42)"],
        )
        .await
        .unwrap();

        sleep(Duration::from_millis(100)).await;

        db_job.abort();
        drop(cleaner); // drop database files
    }

    // make sure that db file doesn't exist, and that the bucket contains backup
    assert!(!std::path::Path::new(PATH).exists());
    assert_bucket_occupancy(BUCKET, false).await;

    {
        // 2: recreate the database, wait for restore from backup
        let _ = DbFileCleaner::new(PATH);
        let db_job = tokio::spawn(run_server(db_config));

        sleep(Duration::from_secs(2)).await;

        let result = sql(&connection_addr, ["SELECT id FROM t"]).await.unwrap();
        let rs = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0].cells["id"], Value::Integer(42));

        db_job.abort();
    }
}

async fn sql<I: IntoIterator<Item = &'static str>>(
    url: &Url,
    stmts: I,
) -> Result<Vec<QueryResult>> {
    let db = libsql_client::reqwest::Connection::connect_from_url(url)?;
    db.batch(stmts).await
}

/// Verify that MinIO (S3 local stub) is up.
async fn assert_minio_ready() {
    const MINIO_URL: &str = "http://127.0.0.1:9090/";

    let status = reqwest::get(format!("{}/minio/health/ready", MINIO_URL))
        .await
        .expect("couldn't reach minio health check")
        .status();
    assert_eq!(status, StatusCode::OK);
}

/// Checks if the corresponding bucket is empty (has any elements) or not.
/// If bucket was not found, it's equivalent of an empty one.
async fn assert_bucket_occupancy(bucket: &str, expect_empty: bool) {
    use aws_sdk_s3::Client;

    let loader = aws_config::from_env().endpoint_url("http://127.0.0.1:9000/");
    let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .build();
    let client = Client::from_conf(conf);
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let contents = out.contents().unwrap_or_default();
        assert_eq!(contents.is_empty(), expect_empty);
    } else if !expect_empty {
        panic!("bucket '{}' doesn't exist", bucket);
    }
}

/// Guardian struct used for cleaning up the test data from
/// database file dir at the beginning and end of a test.
struct DbFileCleaner(PathBuf);

impl DbFileCleaner {
    fn new<P: Into<PathBuf>>(path: P) -> Self {
        let path = path.into();
        Self::cleanup(&path);
        DbFileCleaner(path)
    }

    fn cleanup(path: &PathBuf) {
        let _ = std::fs::remove_dir_all(path);
    }
}

impl Drop for DbFileCleaner {
    fn drop(&mut self) {
        Self::cleanup(&self.0)
    }
}

/// Guardian struct used for cleaning up the test data from
/// S3 bucket dir at the beginning and end of a test.
struct S3BucketCleaner(&'static str);

impl S3BucketCleaner {
    async fn new(bucket: &'static str) -> Self {
        let _ = Self::cleanup(bucket).await; // cleanup the bucket before test
        S3BucketCleaner(bucket)
    }

    /// Delete all objects from S3 bucket with provided name (doesn't delete bucket itself).
    async fn cleanup(bucket: &str) -> Result<()> {
        use aws_sdk_s3::types::{Delete, ObjectIdentifier};
        use aws_sdk_s3::Client;

        let loader = aws_config::from_env().endpoint_url("http://127.0.0.1:9000/");
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(conf);
        let objects = client.list_objects().bucket(bucket).send().await?;
        let mut delete_keys = Vec::new();
        for o in objects.contents().unwrap_or_default() {
            let id = ObjectIdentifier::builder()
                .set_key(o.key().map(String::from))
                .build();
            delete_keys.push(id);
        }

        let _ = client
            .delete_objects()
            .bucket(bucket)
            .delete(Delete::builder().set_objects(Some(delete_keys)).build())
            .send()
            .await?;

        Ok(())
    }
}

impl Drop for S3BucketCleaner {
    fn drop(&mut self) {
        //FIXME: running line below on tokio::test runtime will hang.
        //let _ = block_on(Self::cleanup(self.0));
    }
}
