use crate::{run_server, Config};
use anyhow::Result;
use aws_sdk_s3::config::{Credentials, Region};
use libsql_client::{Connection, QueryResult, Statement, Value};
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use url::Url;

const S3_URL: &str = "http://localhost:9000/";

fn start_db(step: u32, config: &Config) -> JoinHandle<()> {
    let db_config = config.clone();
    tokio::spawn(async move {
        if let Err(e) = run_server(db_config).await {
            panic!("Failed step {}: {}", step, e);
        }
    })
}

#[tokio::test]
async fn backup_restore() {
    let _ = env_logger::builder().is_test(true).try_init();
    const BUCKET: &str = "testbackuprestore";
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;
    const OPS: usize = 2000;
    const ROWS: usize = 10;

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
            verify_crc: true,
            use_compression: bottomless::replicator::CompressionKind::Gzip,
            bucket_name: BUCKET.to_string(),
            max_batch_interval: Duration::from_millis(250),
            restore_transaction_page_swap_after: 1, // in this test swap should happen at least once
            ..bottomless::replicator::Options::from_env().unwrap()
        }),
        db_path: PATH.into(),
        http_addr: Some(listener_addr),
        ..Config::default()
    };

    {
        tracing::info!(
            "---STEP 1: create a local database, fill it with data, wait for WAL backup---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, &db_config);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        let stmts: Vec<_> = (0..OPS)
            .map(|i| {
                format!(
                    "INSERT INTO t(id, name) VALUES({}, '{}') ON CONFLICT (id) DO UPDATE SET name = '{}';",
                    i % ROWS,
                    i,
                    i
                )
            })
            .collect();
        let _ = sql(&connection_addr, stmts).await.unwrap();

        sleep(Duration::from_secs(2)).await;

        db_job.abort();
        drop(cleaner);
    }

    // make sure that db file doesn't exist, and that the bucket contains backup
    assert!(!std::path::Path::new(PATH).exists());
    assert_bucket_occupancy(BUCKET, false).await;

    {
        tracing::info!(
            "---STEP 2: recreate the database from WAL - create a snapshot at the end---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, &db_config);

        sleep(Duration::from_secs(2)).await;

        let result = sql(&connection_addr, ["SELECT id, name FROM t ORDER BY id;"])
            .await
            .unwrap();
        let rs = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()
            .unwrap();
        assert_eq!(rs.rows.len(), ROWS, "unexpected number of rows");
        let base = if OPS < 10 { 0 } else { OPS - 10 } as i64;
        for (i, row) in rs.rows.iter().enumerate() {
            let i = i as i64;
            let id = row.cells["id"].clone();
            let name = row.cells["name"].clone();
            assert_eq!(
                (id, name),
                (Value::Integer(i), Value::Text((base + i).to_string())),
                "unexpected values for row {}",
                i
            );
        }

        db_job.abort();
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 3: recreate database from snapshot alone---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(3, &db_config);

        sleep(Duration::from_secs(2)).await;

        // override existing entries, this will generate WAL
        let stmts: Vec<_> = (0..OPS)
            .map(|i| {
                format!(
                    "INSERT INTO t(id, name) VALUES({}, '{}-x') ON CONFLICT (id) DO UPDATE SET name = '{}-x';",
                    i % ROWS,
                    i,
                    i
                )
            })
            .collect();
        let _ = sql(&connection_addr, stmts).await.unwrap();

        // wait for WAL to backup
        sleep(Duration::from_secs(2)).await;
        db_job.abort();
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 4: recreate the database from snapshot + WAL---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, &db_config);

        sleep(Duration::from_secs(2)).await;

        let result = sql(&connection_addr, ["SELECT id, name FROM t ORDER BY id;"])
            .await
            .unwrap();
        let rs = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()
            .unwrap();
        assert_eq!(rs.rows.len(), ROWS, "unexpected number of rows");
        let base = if OPS < 10 { 0 } else { OPS - 10 } as i64;
        for (i, row) in rs.rows.iter().enumerate() {
            let i = i as i64;
            let id = row.cells["id"].clone();
            let name = row.cells["name"].clone();
            assert_eq!(
                (id, name),
                (Value::Integer(i), Value::Text(format!("{}-x", base + i))),
                "unexpected values for row {}",
                i
            );
        }

        db_job.abort();
        drop(cleaner);
    }
}

#[tokio::test]
async fn rollback_restore() {
    let _ = env_logger::builder().is_test(true).try_init();
    const BUCKET: &str = "testrollbackrestore";
    const PATH: &str = "rollback_restore.sqld";
    const PORT: u16 = 15002;

    async fn get_data(conn: &Url) -> Result<Vec<(Value, Value)>> {
        let result = sql(conn, ["SELECT * FROM t"]).await?;
        let rows = result
            .into_iter()
            .next()
            .unwrap()
            .into_result_set()?
            .rows
            .into_iter()
            .map(|row| (row.cells["id"].clone(), row.cells["name"].clone()))
            .collect();
        Ok(rows)
    }

    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let conn = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let db_config = Config {
        bottomless_replication: Some(bottomless::replicator::Options {
            create_bucket_if_not_exists: true,
            verify_crc: true,
            use_compression: bottomless::replicator::CompressionKind::Gzip,
            bucket_name: BUCKET.to_string(),
            max_batch_interval: Duration::from_millis(250),
            restore_transaction_page_swap_after: 1, // in this test swap should happen at least once
            ..bottomless::replicator::Options::from_env().unwrap()
        }),
        db_path: PATH.into(),
        http_addr: Some(listener_addr),
        ..Config::default()
    };

    {
        tracing::info!("---STEP 1: create db, write row, rollback---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, &db_config);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &conn,
            [
                "CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);",
                "INSERT INTO t(id, name) VALUES(1, 'A')",
            ],
        )
        .await
        .unwrap();

        let _ = sql(
            &conn,
            [
                "BEGIN",
                "UPDATE t SET name = 'B' WHERE id = 1",
                "ROLLBACK",
                "INSERT INTO t(id, name) VALUES(2, 'B')",
            ],
        )
        .await
        .unwrap();

        // wait for backup
        sleep(Duration::from_secs(2)).await;
        assert_bucket_occupancy(BUCKET, false).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "rollback value should not be updated"
        );

        db_job.abort();
        drop(cleaner);
    }

    {
        tracing::info!("---STEP 2: recreate database, read modify, read again ---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, &db_config);
        sleep(Duration::from_secs(2)).await;

        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("A".into())),
                (Value::Integer(2), Value::Text("B".into()))
            ],
            "restored value should not contain rollbacked update"
        );
        let _ = sql(&conn, ["UPDATE t SET name = 'C'"]).await.unwrap();
        let rs = get_data(&conn).await.unwrap();
        assert_eq!(
            rs,
            vec![
                (Value::Integer(1), Value::Text("C".into())),
                (Value::Integer(2), Value::Text("C".into()))
            ]
        );

        db_job.abort();
        drop(cleaner);
    }
}

async fn sql<I, S>(url: &Url, stmts: I) -> Result<Vec<QueryResult>>
where
    I: IntoIterator<Item = S>,
    S: Into<Statement>,
{
    let db = libsql_client::reqwest::Connection::connect_from_url(url)?;
    db.batch(stmts).await
}

async fn s3_config() -> aws_sdk_s3::config::Config {
    let loader = aws_config::from_env().endpoint_url(S3_URL);
    aws_sdk_s3::config::Builder::from(&loader.load().await)
        .force_path_style(true)
        .region(Region::new(
            std::env::var("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION").unwrap(),
        ))
        .credentials_provider(Credentials::new(
            std::env::var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID").unwrap(),
            std::env::var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY").unwrap(),
            None,
            None,
            "Static",
        ))
        .build()
}

/// Checks if the corresponding bucket is empty (has any elements) or not.
/// If bucket was not found, it's equivalent of an empty one.
async fn assert_bucket_occupancy(bucket: &str, expect_empty: bool) {
    use aws_sdk_s3::Client;

    let client = Client::from_conf(s3_config().await);
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let contents = out.contents().unwrap_or_default();
        if expect_empty {
            assert!(
                contents.is_empty(),
                "expected S3 bucket to be empty but {} were found",
                contents.len()
            );
        } else {
            assert!(
                !contents.is_empty(),
                "expected S3 bucket to be filled with backup data but it was empty"
            );
        }
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

        let client = Client::from_conf(s3_config().await);
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
