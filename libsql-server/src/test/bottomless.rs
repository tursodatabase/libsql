use anyhow::Result;
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use futures_core::Future;
use itertools::Itertools;
use libsql_client::{Connection, QueryResult, Statement, Value};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Once;
use tokio::time::sleep;
use tokio::time::Duration;
use url::Url;
use uuid::Uuid;

use crate::auth::user_auth_strategies::Disabled;
use crate::auth::Auth;
use crate::config::{DbConfig, UserApiConfig};
use crate::net::AddrIncoming;
use crate::Server;

const S3_URL: &str = "http://localhost:9000/";

static S3_SERVER: Once = Once::new();

async fn start_s3_server() {
    std::env::set_var("LIBSQL_BOTTOMLESS_ENDPOINT", "http://localhost:9000");
    std::env::set_var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY", "foo");
    std::env::set_var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID", "bar");
    std::env::set_var("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION", "us-east-1");
    std::env::set_var("LIBSQL_BOTTOMLESS_BUCKET", "my-bucket");

    S3_SERVER.call_once(|| {
        let tmp = std::env::temp_dir().join(format!("s3s-{}", Uuid::new_v4().as_simple()));

        std::fs::create_dir_all(&tmp).unwrap();

        tracing::info!("starting mock s3 server with path: {}", tmp.display());

        let s3_impl = s3s_fs::FileSystem::new(tmp).unwrap();

        let key = std::env::var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID").unwrap();
        let secret = std::env::var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY").unwrap();

        let auth = SimpleAuth::from_single(key, secret);

        let mut s3 = S3ServiceBuilder::new(s3_impl);
        s3.set_auth(auth);
        let s3 = s3.build().into_shared().into_make_service();

        tokio::spawn(async move {
            let addr = ([127, 0, 0, 1], 9000).into();

            hyper::Server::bind(&addr).serve(s3).await.unwrap();
        });
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// returns a future that once polled will shutdown the server and wait for cleanup
fn start_db(step: u32, server: Server) -> impl Future<Output = ()> {
    let notify = server.shutdown.clone();
    let handle = tokio::spawn(async move {
        if let Err(e) = server.start().await {
            panic!("Failed step {}: {}", step, e);
        }
    });

    async move {
        notify.notify_waiters();
        handle.await.unwrap();
    }
}

async fn configure_server(
    options: &bottomless::replicator::Options,
    addr: SocketAddr,
    path: impl Into<PathBuf>,
) -> Server {
    let http_acceptor = AddrIncoming::new(tokio::net::TcpListener::bind(addr).await.unwrap());
    Server {
        db_config: DbConfig {
            extensions_path: None,
            bottomless_replication: Some(options.clone()),
            max_log_size: 200 * 4046,
            max_log_duration: None,
            soft_heap_limit_mb: None,
            hard_heap_limit_mb: None,
            max_response_size: 10000000 * 4096,
            max_total_response_size: 10000000 * 4096,
            snapshot_exec: None,
            checkpoint_interval: Some(Duration::from_secs(3)),
            snapshot_at_shutdown: false,
            encryption_config: None,
            max_concurrent_requests: 128,
            connection_creation_timeout: None,
            disable_intelligent_throttling: false,
        },
        admin_api_config: None,
        disable_namespaces: true,
        user_api_config: UserApiConfig {
            hrana_ws_acceptor: None,
            http_acceptor: Some(http_acceptor),
            enable_http_console: false,
            self_url: None,
            primary_url: None,
            auth_strategy: Auth::new(Disabled::new()),
        },
        path: path.into().into(),
        disable_default_namespace: false,
        max_active_namespaces: 100,
        heartbeat_config: None,
        idle_shutdown_timeout: None,
        initial_idle_shutdown_timeout: None,
        rpc_server_config: None,
        rpc_client_config: None,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore]
async fn backup_restore() {
    let _ = tracing_subscriber::fmt::try_init();

    start_s3_server().await;

    const DB_ID: &str = "testbackuprestore";
    const BUCKET: &str = "testbackuprestore";
    const PATH: &str = "backup_restore.sqld";
    const PORT: u16 = 15001;
    const OPS: usize = 2000;
    const ROWS: usize = 10;

    let _ = S3BucketCleaner::new(BUCKET).await;
    assert_bucket_occupancy(BUCKET, true).await;

    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        bucket_name: BUCKET.to_string(),
        max_batch_interval: Duration::from_millis(250),
        ..bottomless::replicator::Options::from_env().unwrap()
    };
    let connection_addr = Url::parse(&format!("http://localhost:{}", PORT)).unwrap();
    let listener_addr = format!("0.0.0.0:{}", PORT)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();

    let make_server = || async { configure_server(&options, listener_addr, PATH).await };

    {
        tracing::info!(
            "---STEP 1: create a local database, fill it with data, wait for WAL backup---"
        );
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

        sleep(Duration::from_secs(2)).await;

        let _ = sql(
            &connection_addr,
            ["CREATE TABLE IF NOT EXISTS t(id INT PRIMARY KEY, name TEXT);"],
        )
        .await
        .unwrap();

        perform_updates(&connection_addr, ROWS, OPS, "A").await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        sleep(Duration::from_secs(2)).await;

        db_job.await;
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
        let db_job = start_db(2, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "A").await;

        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 3: recreate database from snapshot alone---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(3, make_server().await);

        sleep(Duration::from_secs(2)).await;

        // override existing entries, this will generate WAL
        perform_updates(&connection_addr, ROWS, OPS, "B").await;

        // wait for WAL to backup
        sleep(Duration::from_secs(2)).await;
        db_job.await;
        drop(cleaner);
    }

    assert!(!std::path::Path::new(PATH).exists());

    {
        tracing::info!("---STEP 4: recreate the database from snapshot + WAL---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }

    {
        // make sure that we can follow back until the generation from which snapshot could be possible
        tracing::info!("---STEP 5: recreate database from generation missing snapshot ---");

        // manually remove snapshots from all generations, this will force restore across generations
        // from the very beginning
        remove_snapshots(BUCKET).await;

        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(4, make_server().await);

        sleep(Duration::from_secs(2)).await;

        assert_updates(&connection_addr, ROWS, OPS, "B").await;

        db_job.await;
        drop(cleaner);
    }
}

#[tokio::test]
async fn rollback_restore() {
    let _ = tracing_subscriber::fmt::try_init();

    start_s3_server().await;

    const DB_ID: &str = "testrollbackrestore";
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
    let options = bottomless::replicator::Options {
        db_id: Some(DB_ID.to_string()),
        create_bucket_if_not_exists: true,
        verify_crc: true,
        use_compression: bottomless::replicator::CompressionKind::Gzip,
        bucket_name: BUCKET.to_string(),
        max_batch_interval: Duration::from_millis(250),
        ..bottomless::replicator::Options::from_env().unwrap()
    };
    let make_server = || async { configure_server(&options, listener_addr, PATH).await };

    {
        tracing::info!("---STEP 1: create db, write row, rollback---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(1, make_server().await);

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

        db_job.await;
        drop(cleaner);
    }

    {
        tracing::info!("---STEP 2: recreate database, read modify, read again ---");
        let cleaner = DbFileCleaner::new(PATH);
        let db_job = start_db(2, make_server().await);
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

        db_job.await;
        drop(cleaner);
    }
}

async fn perform_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let stmts: Vec<_> = (0..ops_count)
        .map(|i| {
            format!(
                "INSERT INTO t(id, name) VALUES({}, '{}-{}') ON CONFLICT (id) DO UPDATE SET name = '{}-{}';",
                i % row_count,
                i,
                update,
                i,
                update
            )
        })
        .collect();
    let _ = sql(connection_addr, stmts).await.unwrap();
}

async fn assert_updates(connection_addr: &Url, row_count: usize, ops_count: usize, update: &str) {
    let result = sql(connection_addr, ["SELECT id, name FROM t ORDER BY id;"])
        .await
        .unwrap();
    let rs = result
        .into_iter()
        .next()
        .unwrap()
        .into_result_set()
        .unwrap();
    assert_eq!(rs.rows.len(), row_count, "unexpected number of rows");
    let base = if ops_count < 10 { 0 } else { ops_count - 10 } as i64;
    for (i, row) in rs.rows.iter().enumerate() {
        let i = i as i64;
        let id = row.cells["id"].clone();
        let name = row.cells["name"].clone();
        assert_eq!(
            (&id, &name),
            (
                &Value::Integer(i),
                &Value::Text(format!("{}-{}", base + i, update))
            ),
            "unexpected values for row {}: ({})",
            i,
            name
        );
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

async fn s3_client() -> Result<Client> {
    let conf = s3_config().await;
    let client = Client::from_conf(conf);
    Ok(client)
}

/// Remove a snapshot objects from all generation. This may trigger bottomless to do rollup restore
/// across all generations.
async fn remove_snapshots(bucket: &str) {
    let client = s3_client().await.unwrap();
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let keys = out
            .contents()
            .iter()
            .map(|o| {
                let key = o.key().unwrap();
                let prefix = key.split('/').next().unwrap();
                format!("{}/db.gz", prefix)
            })
            .unique()
            .map(|key| ObjectIdentifier::builder().key(key).build().unwrap())
            .collect();

        client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(keys))
                    .quiet(true)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
}

/// Checks if the corresponding bucket is empty (has any elements) or not.
/// If bucket was not found, it's equivalent of an empty one.
async fn assert_bucket_occupancy(bucket: &str, expect_empty: bool) {
    let client = s3_client().await.unwrap();
    if let Ok(out) = client.list_objects().bucket(bucket).send().await {
        let contents = out.contents();
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
#[allow(dead_code)]
struct S3BucketCleaner(&'static str);

impl S3BucketCleaner {
    async fn new(bucket: &'static str) -> Self {
        let _ = Self::cleanup(bucket).await; // cleanup the bucket before test
        S3BucketCleaner(bucket)
    }

    /// Delete all objects from S3 bucket with provided name (doesn't delete bucket itself).
    async fn cleanup(bucket: &str) -> Result<()> {
        let client = s3_client().await?;
        let objects = client.list_objects().bucket(bucket).send().await?;
        let mut delete_keys = Vec::new();
        for o in objects.contents() {
            let id = ObjectIdentifier::builder()
                .set_key(o.key().map(String::from))
                .build()
                .unwrap();
            delete_keys.push(id);
        }

        let _ = client
            .delete_objects()
            .bucket(bucket)
            .delete(
                Delete::builder()
                    .set_objects(Some(delete_keys))
                    .build()
                    .unwrap(),
            )
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

/// Regression tests for the snapshot upload pipeline: a failed or interrupted
/// snapshot upload must not wedge checkpoints until a process restart, must
/// never be laundered into a success marker, and a process restart must link
/// the new generation to its predecessor with a `.dep` object.
///
/// These tests drive the bottomless WAL wrapper through a raw libsql
/// connection (the same wiring the meta store uses) against a per-test mock
/// S3 server that can inject failures into snapshot uploads.
mod snapshot_pipeline {
    use super::*;

    use std::path::Path;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    use bottomless::bottomless_wal::BottomlessWalWrapper;
    use bottomless::replicator::Replicator;
    use libsql_sys::wal::wrapper::{WalWrapper, WrappedWal};
    use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager};
    use s3s::dto::{
        CreateBucketInput, CreateBucketOutput, DeleteObjectInput, DeleteObjectOutput,
        DeleteObjectsInput, DeleteObjectsOutput, GetObjectInput, GetObjectOutput, HeadBucketInput,
        HeadBucketOutput, HeadObjectInput, HeadObjectOutput, ListObjectsInput, ListObjectsOutput,
        ListObjectsV2Input, ListObjectsV2Output, PutObjectInput, PutObjectOutput,
    };
    use s3s::{S3Request, S3Response, S3Result, S3};

    use crate::connection::legacy::open_conn_active_checkpoint;

    const FLAKY_S3_KEY: &str = "flaky-key";
    const FLAKY_S3_SECRET: &str = "flaky-secret";

    type BottomlessConn =
        libsql_sys::Connection<WrappedWal<Option<BottomlessWalWrapper>, Sqlite3Wal>>;

    #[derive(Clone, Default)]
    struct S3FailureInjector {
        /// Number of snapshot PUTs to fail: a positive value counts down with
        /// every failed request, a negative value fails all snapshot PUTs and
        /// zero disables the injection.
        fail_snapshot_puts: Arc<AtomicI64>,
        /// Delay applied to snapshot PUTs, in milliseconds.
        delay_snapshot_puts_ms: Arc<AtomicI64>,
    }

    /// An S3 implementation that delegates to the `s3s-fs` filesystem backend,
    /// but can inject failures into and delay snapshot uploads (PUT requests
    /// of `db.*` objects).
    struct FlakyS3 {
        inner: s3s_fs::FileSystem,
        injector: S3FailureInjector,
    }

    fn is_snapshot_key(key: &str) -> bool {
        key.rsplit('/')
            .next()
            .map(|name| name.starts_with("db."))
            .unwrap_or(false)
    }

    #[async_trait::async_trait]
    impl S3 for FlakyS3 {
        async fn create_bucket(
            &self,
            req: S3Request<CreateBucketInput>,
        ) -> S3Result<S3Response<CreateBucketOutput>> {
            self.inner.create_bucket(req).await
        }

        async fn head_bucket(
            &self,
            req: S3Request<HeadBucketInput>,
        ) -> S3Result<S3Response<HeadBucketOutput>> {
            self.inner.head_bucket(req).await
        }

        async fn put_object(
            &self,
            req: S3Request<PutObjectInput>,
        ) -> S3Result<S3Response<PutObjectOutput>> {
            if is_snapshot_key(&req.input.key) {
                let delay = self.injector.delay_snapshot_puts_ms.load(Ordering::SeqCst);
                if delay > 0 {
                    sleep(Duration::from_millis(delay as u64)).await;
                }
                let remaining = self.injector.fail_snapshot_puts.load(Ordering::SeqCst);
                if remaining != 0 {
                    if remaining > 0 {
                        self.injector
                            .fail_snapshot_puts
                            .fetch_sub(1, Ordering::SeqCst);
                    }
                    return Err(s3s::S3Error::with_message(
                        s3s::S3ErrorCode::InternalError,
                        "injected snapshot upload failure",
                    ));
                }
            }
            self.inner.put_object(req).await
        }

        async fn get_object(
            &self,
            req: S3Request<GetObjectInput>,
        ) -> S3Result<S3Response<GetObjectOutput>> {
            self.inner.get_object(req).await
        }

        async fn head_object(
            &self,
            req: S3Request<HeadObjectInput>,
        ) -> S3Result<S3Response<HeadObjectOutput>> {
            self.inner.head_object(req).await
        }

        async fn list_objects(
            &self,
            req: S3Request<ListObjectsInput>,
        ) -> S3Result<S3Response<ListObjectsOutput>> {
            self.inner.list_objects(req).await
        }

        async fn list_objects_v2(
            &self,
            req: S3Request<ListObjectsV2Input>,
        ) -> S3Result<S3Response<ListObjectsV2Output>> {
            self.inner.list_objects_v2(req).await
        }

        async fn delete_object(
            &self,
            req: S3Request<DeleteObjectInput>,
        ) -> S3Result<S3Response<DeleteObjectOutput>> {
            self.inner.delete_object(req).await
        }

        async fn delete_objects(
            &self,
            req: S3Request<DeleteObjectsInput>,
        ) -> S3Result<S3Response<DeleteObjectsOutput>> {
            self.inner.delete_objects(req).await
        }
    }

    async fn start_flaky_s3_server(port: u16, injector: S3FailureInjector) {
        let tmp = std::env::temp_dir().join(format!("s3s-flaky-{}", Uuid::new_v4().as_simple()));
        std::fs::create_dir_all(&tmp).unwrap();
        let s3_impl = FlakyS3 {
            inner: s3s_fs::FileSystem::new(tmp).unwrap(),
            injector,
        };
        let auth = SimpleAuth::from_single(FLAKY_S3_KEY, FLAKY_S3_SECRET);
        let mut s3 = S3ServiceBuilder::new(s3_impl);
        s3.set_auth(auth);
        let s3 = s3.build().into_shared().into_make_service();
        tokio::spawn(async move {
            let addr = ([127, 0, 0, 1], port).into();
            hyper::Server::bind(&addr).serve(s3).await.unwrap();
        });
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                return;
            }
            sleep(Duration::from_millis(50)).await;
        }
        panic!("mock s3 server did not start on port {port}");
    }

    fn flaky_s3_options(db_id: &str, bucket: &str, port: u16) -> bottomless::replicator::Options {
        bottomless::replicator::Options {
            db_id: Some(db_id.to_string()),
            create_bucket_if_not_exists: true,
            verify_crc: true,
            use_compression: bottomless::replicator::CompressionKind::Gzip,
            encryption_config: None,
            aws_endpoint: Some(format!("http://localhost:{port}")),
            access_key_id: Some(FLAKY_S3_KEY.to_string()),
            secret_access_key: Some(FLAKY_S3_SECRET.to_string()),
            session_token: None,
            region: Some("us-east-1".to_string()),
            bucket_name: bucket.to_string(),
            max_frames_per_batch: 10_000,
            max_batch_interval: Duration::from_millis(250),
            s3_max_parallelism: 32,
            // injected failures must surface to bottomless instead of being
            // absorbed by the SDK's internal retries
            s3_max_retries: 1,
            skip_snapshot: false,
            skip_shutdown_upload: false,
        }
    }

    async fn assertion_client(port: u16) -> Client {
        let loader = aws_config::from_env().endpoint_url(format!("http://localhost:{port}"));
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                FLAKY_S3_KEY,
                FLAKY_S3_SECRET,
                None,
                None,
                "Static",
            ))
            .build();
        Client::from_conf(conf)
    }

    async fn object_exists(client: &Client, bucket: &str, key: &str) -> bool {
        client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .is_ok()
    }

    /// Mirrors the replicator initialization performed by
    /// `init_bottomless_replicator` in `namespace/configurator/helpers.rs`.
    async fn init_replicator(
        db_dir: &Path,
        options: &bottomless::replicator::Options,
    ) -> Replicator {
        tokio::fs::create_dir_all(db_dir).await.unwrap();
        let db_file = db_dir.join("data");
        let mut replicator = Replicator::with_options(db_file.to_str().unwrap(), options.clone())
            .await
            .unwrap();
        let (action, _did_recover) = replicator.restore(None, None).await.unwrap();
        match action {
            bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
                replicator.new_generation().await;
                replicator.snapshot_main_db_file(true).await.unwrap();
                replicator.maybe_replicate_wal().await.unwrap();
            }
            bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
                replicator.set_generation(gen);
            }
        }
        replicator
    }

    /// Opens a connection whose WAL is wrapped by the bottomless wrapper, the
    /// same way the meta store wires it up. Autocheckpoint is disabled, like
    /// on a primary configured with a checkpoint interval.
    async fn open_bottomless_conn(
        db_dir: &Path,
        replicator: Arc<tokio::sync::Mutex<Option<Replicator>>>,
    ) -> BottomlessConn {
        let wal_manager = WalWrapper::new(
            Some(BottomlessWalWrapper::new(replicator)),
            Sqlite3WalManager::default(),
        );
        let db_dir = db_dir.to_path_buf();
        tokio::task::spawn_blocking(move || {
            open_conn_active_checkpoint(&db_dir, wal_manager, None, 0, None).unwrap()
        })
        .await
        .unwrap()
    }

    /// Connection methods must run on a blocking thread: the bottomless WAL
    /// hooks use `blocking_lock`/`block_on` internally.
    async fn exec(conn: BottomlessConn, sql: &'static str) -> BottomlessConn {
        tokio::task::spawn_blocking(move || {
            conn.execute_batch(sql).unwrap();
            conn
        })
        .await
        .unwrap()
    }

    /// Attempts a TRUNCATE checkpoint. Returns true when the checkpoint was
    /// performed and false when it was refused (busy).
    async fn try_checkpoint(conn: BottomlessConn) -> (BottomlessConn, bool) {
        tokio::task::spawn_blocking(move || {
            let busy: i64 = conn
                .query_row("PRAGMA wal_checkpoint(TRUNCATE)", (), |row| row.get(0))
                .unwrap();
            (conn, busy == 0)
        })
        .await
        .unwrap()
    }

    async fn close_conn(conn: BottomlessConn) {
        tokio::task::spawn_blocking(move || drop(conn))
            .await
            .unwrap()
    }

    /// A snapshot upload that fails is retried within the snapshot task, so a
    /// transient error does not leave the generation without its snapshot.
    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_upload_is_retried() {
        let _ = tracing_subscriber::fmt::try_init();

        const DB_ID: &str = "testsnapshotretry";
        const BUCKET: &str = "testsnapshotretry";
        const PATH: &str = "snapshot_retry.sqld";
        const PORT: u16 = 9081;

        let injector = S3FailureInjector::default();
        start_flaky_s3_server(PORT, injector.clone()).await;
        let options = flaky_s3_options(DB_ID, BUCKET, PORT);

        let _cleaner = DbFileCleaner::new(PATH);
        let replicator = init_replicator(Path::new(PATH), &options).await;
        let replicator = Arc::new(tokio::sync::Mutex::new(Some(replicator)));
        let conn = open_bottomless_conn(Path::new(PATH), replicator.clone()).await;

        let conn = exec(
            conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('a');",
        )
        .await;

        // fail exactly one snapshot PUT: the upload must recover on its own
        injector.fail_snapshot_puts.store(1, Ordering::SeqCst);

        let (conn, checkpointed) = try_checkpoint(conn).await;
        assert!(checkpointed, "checkpoint of a fresh database must succeed");

        let generation = {
            let mut guard = replicator.lock().await;
            let replicator = guard.as_mut().unwrap();
            let generation = replicator.generation().unwrap();
            let snapshotted = replicator.wait_until_snapshotted().await.unwrap();
            assert!(
                snapshotted,
                "snapshot upload must succeed despite a failed attempt"
            );
            generation
        };
        assert_eq!(
            injector.fail_snapshot_puts.load(Ordering::SeqCst),
            0,
            "the injected failure must have been consumed"
        );

        let client = assertion_client(PORT).await;
        let snapshot_key = format!("{DB_ID}-{generation}/db.gz");
        assert!(
            object_exists(&client, BUCKET, &snapshot_key).await,
            "snapshot object {snapshot_key} must exist after the retried upload"
        );

        // the per-generation compression artifact must have been cleaned up
        for entry in std::fs::read_dir(PATH).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_str().unwrap().to_string();
            assert!(
                !name.starts_with("db."),
                "leftover snapshot artifact: {name}"
            );
        }

        close_conn(conn).await;
    }

    /// A snapshot upload that keeps failing must neither wedge checkpoints
    /// until a process restart (the next checkpoint attempt re-triggers the
    /// upload), nor be laundered into a success marker by a checkpoint
    /// attempt that finds an empty WAL.
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_snapshot_recovers_without_restart() {
        let _ = tracing_subscriber::fmt::try_init();

        const DB_ID: &str = "testsnapshotwedge";
        const BUCKET: &str = "testsnapshotwedge";
        const PATH: &str = "snapshot_wedge.sqld";
        const PORT: u16 = 9082;

        let injector = S3FailureInjector::default();
        start_flaky_s3_server(PORT, injector.clone()).await;
        let options = flaky_s3_options(DB_ID, BUCKET, PORT);

        let _cleaner = DbFileCleaner::new(PATH);
        let replicator = init_replicator(Path::new(PATH), &options).await;
        let replicator = Arc::new(tokio::sync::Mutex::new(Some(replicator)));
        let conn = open_bottomless_conn(Path::new(PATH), replicator.clone()).await;

        let conn = exec(
            conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('a');",
        )
        .await;

        // fail every snapshot PUT from now on
        injector.fail_snapshot_puts.store(-1, Ordering::SeqCst);

        // the checkpoint itself succeeds (the previous generation was
        // snapshotted), but the snapshot of the new generation will fail
        let (conn, checkpointed) = try_checkpoint(conn).await;
        assert!(checkpointed, "checkpoint of a fresh database must succeed");

        let wedged_generation = {
            let mut guard = replicator.lock().await;
            let replicator = guard.as_mut().unwrap();
            let generation = replicator.generation().unwrap();
            let err = replicator.wait_until_snapshotted().await;
            assert!(err.is_err(), "snapshot upload must fail: {err:?}");
            generation
        };

        // a checkpoint attempt with an empty WAL is refused and must not mark
        // the failed snapshot as complete
        let (conn, checkpointed) = try_checkpoint(conn).await;
        assert!(
            !checkpointed,
            "empty-WAL checkpoint attempt must be refused"
        );

        let conn = exec(conn, "INSERT INTO t(v) VALUES ('b');").await;

        // the failure must not have been laundered by the empty-WAL attempt:
        // the next checkpoint has to be refused because the current generation
        // still has no snapshot
        let (conn, checkpointed) = try_checkpoint(conn).await;
        assert!(
            !checkpointed,
            "checkpoint must be refused while the current generation has no snapshot"
        );

        // let snapshot uploads succeed again: checkpoint attempts re-trigger
        // the missing snapshot and eventually proceed, without a restart
        injector.fail_snapshot_puts.store(0, Ordering::SeqCst);

        let mut conn = conn;
        let mut recovered = false;
        for _ in 0..40 {
            sleep(Duration::from_millis(500)).await;
            let (c, checkpointed) = try_checkpoint(conn).await;
            conn = c;
            if checkpointed {
                recovered = true;
                break;
            }
        }
        assert!(
            recovered,
            "checkpoints must resume without a process restart once snapshot uploads succeed"
        );

        let client = assertion_client(PORT).await;
        let snapshot_key = format!("{DB_ID}-{wedged_generation}/db.gz");
        assert!(
            object_exists(&client, BUCKET, &snapshot_key).await,
            "the re-triggered snapshot upload must have stored {snapshot_key}"
        );

        close_conn(conn).await;
    }

    /// While a snapshot upload is in flight, another snapshot request is
    /// skipped instead of spawning a second task that would race with the
    /// first one.
    #[tokio::test(flavor = "multi_thread")]
    async fn overlapping_snapshot_is_skipped() {
        let _ = tracing_subscriber::fmt::try_init();

        const DB_ID: &str = "testsnapshotoverlap";
        const BUCKET: &str = "testsnapshotoverlap";
        const PATH: &str = "snapshot_overlap.sqld";
        const PORT: u16 = 9083;

        let injector = S3FailureInjector::default();
        start_flaky_s3_server(PORT, injector.clone()).await;
        let options = flaky_s3_options(DB_ID, BUCKET, PORT);

        let _cleaner = DbFileCleaner::new(PATH);
        let replicator = init_replicator(Path::new(PATH), &options).await;
        let replicator = Arc::new(tokio::sync::Mutex::new(Some(replicator)));
        let conn = open_bottomless_conn(Path::new(PATH), replicator.clone()).await;

        let conn = exec(
            conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('a');",
        )
        .await;

        // slow down the snapshot upload so that it is still in flight when the
        // second snapshot request comes in
        injector
            .delay_snapshot_puts_ms
            .store(2_000, Ordering::SeqCst);

        let (conn, checkpointed) = try_checkpoint(conn).await;
        assert!(checkpointed, "checkpoint of a fresh database must succeed");

        {
            let mut guard = replicator.lock().await;
            let replicator = guard.as_mut().unwrap();
            assert!(
                replicator.snapshot_in_flight(),
                "the delayed snapshot upload must still be in flight"
            );
            let handle = replicator.snapshot_main_db_file(true).await.unwrap();
            assert!(
                handle.is_none(),
                "a snapshot request while an upload is in flight must be skipped"
            );
            let snapshotted = replicator.wait_until_snapshotted().await.unwrap();
            assert!(snapshotted, "the in-flight snapshot upload must complete");
        }

        close_conn(conn).await;
    }

    /// Restarting a process on top of a non-empty database file creates a new
    /// generation that carries a `.dep` link to the latest remote generation,
    /// so the restore chain stays intact even if the new generation never
    /// receives its snapshot.
    #[tokio::test(flavor = "multi_thread")]
    async fn restart_links_new_generation_to_predecessor() {
        let _ = tracing_subscriber::fmt::try_init();

        const DB_ID: &str = "testsnapshotdep";
        const BUCKET: &str = "testsnapshotdep";
        const PATH: &str = "snapshot_dep.sqld";
        const PORT: u16 = 9084;

        let injector = S3FailureInjector::default();
        start_flaky_s3_server(PORT, injector.clone()).await;
        let options = flaky_s3_options(DB_ID, BUCKET, PORT);

        let _cleaner = DbFileCleaner::new(PATH);
        let replicator = init_replicator(Path::new(PATH), &options).await;
        let replicator = Arc::new(tokio::sync::Mutex::new(Some(replicator)));
        let conn = open_bottomless_conn(Path::new(PATH), replicator.clone()).await;

        // two checkpointed write rounds pump the database change counter past
        // 1, which is what makes a restart take the "local file is newer"
        // shortcut that used to skip the `.dep` link
        let mut conn = exec(
            conn,
            "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT);
             INSERT INTO t(v) VALUES ('a');",
        )
        .await;
        for _ in 0..2 {
            let (c, checkpointed) = try_checkpoint(conn).await;
            conn = c;
            assert!(checkpointed, "checkpoint must succeed");
            let mut guard = replicator.lock().await;
            let snapshotted = guard
                .as_mut()
                .unwrap()
                .wait_until_snapshotted()
                .await
                .unwrap();
            assert!(snapshotted, "snapshot upload must succeed");
            drop(guard);
            conn = exec(conn, "INSERT INTO t(v) VALUES ('b');").await;
        }

        close_conn(conn).await;
        let pre_restart_generation = {
            let mut replicator = replicator.lock().await.take().unwrap();
            let generation = replicator.generation().unwrap();
            replicator.shutdown_gracefully().await.unwrap();
            generation
        };

        let header = std::fs::read(Path::new(PATH).join("data")).unwrap();
        let change_counter = u32::from_be_bytes(header[24..28].try_into().unwrap());
        assert!(
            change_counter >= 2,
            "test setup must leave a change counter >= 2, got {change_counter}"
        );

        // "restart": initialize a fresh replicator on top of the existing
        // database file; it must create a new generation with a `.dep` link
        // to the pre-restart generation
        let mut restarted = init_replicator(Path::new(PATH), &options).await;
        let post_restart_generation = restarted.generation().unwrap();
        assert_ne!(post_restart_generation, pre_restart_generation);

        let client = assertion_client(PORT).await;
        let dep_key = format!("{DB_ID}-{post_restart_generation}/.dep");
        let mut dep = None;
        // the `.dep` object is stored asynchronously on a best-effort basis
        for _ in 0..100 {
            if let Ok(out) = client
                .get_object()
                .bucket(BUCKET)
                .key(&dep_key)
                .send()
                .await
            {
                let bytes = out.body.collect().await.unwrap().into_bytes();
                dep = Some(Uuid::from_bytes(bytes.as_ref().try_into().unwrap()));
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(
            dep,
            Some(pre_restart_generation),
            "the post-restart generation must carry a .dep link to its predecessor"
        );

        restarted.shutdown_gracefully().await.unwrap();
    }
}
