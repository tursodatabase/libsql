use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use async_lock::{RwLock, RwLockUpgradableReadGuard};
use bytes::Bytes;
use futures_core::Stream;
use hyper::Uri;
use rusqlite::ErrorCode;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;
use tokio::task::{block_in_place, JoinSet};
use tokio_util::io::StreamReader;
use tonic::transport::Channel;

use crate::connection::config::DatabaseConfigStore;
use crate::connection::libsql::{open_db, LibSqlDbFactory};
use crate::connection::write_proxy::MakeWriteProxyConnection;
use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase, ReplicaDatabase};
use crate::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use crate::replication::replica::Replicator;
use crate::replication::{NamespacedSnapshotCallback, ReplicationLogger};
use crate::stats::Stats;
use crate::{
    check_fresh_db, init_bottomless_replicator, run_periodic_compactions, DB_CREATE_TIMEOUT,
    DEFAULT_AUTO_CHECKPOINT, MAX_CONCURRENT_DBS,
};

/// Creates a new `Namespace` for database of the `Self::Database` type.
#[async_trait::async_trait]
pub trait MakeNamespace: Sync + Send + 'static {
    type Database: Database;

    async fn create(
        &self,
        name: Bytes,
        dump: Option<DumpStream>,
    ) -> anyhow::Result<Namespace<Self::Database>>;
}
/// Creates new primary `Namespace`
pub struct PrimaryNamespaceMaker {
    /// base config to create primary namespaces
    config: PrimaryNamespaceConfig,
}

impl PrimaryNamespaceMaker {
    pub fn new(config: PrimaryNamespaceConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl MakeNamespace for PrimaryNamespaceMaker {
    type Database = PrimaryDatabase;

    async fn create(
        &self,
        name: Bytes,
        dump: Option<DumpStream>,
    ) -> anyhow::Result<Namespace<Self::Database>> {
        Namespace::new_primary(&self.config, name, dump).await
    }
}

/// Creates new replica `Namespace`
pub struct ReplicaNamespaceMaker {
    /// base config to create replica namespaces
    config: ReplicaNamespaceConfig,
}

impl ReplicaNamespaceMaker {
    pub fn new(config: ReplicaNamespaceConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl MakeNamespace for ReplicaNamespaceMaker {
    type Database = ReplicaDatabase;

    async fn create(
        &self,
        name: Bytes,
        dump: Option<DumpStream>,
    ) -> anyhow::Result<Namespace<Self::Database>> {
        if dump.is_some() {
            bail!("cannot load dump on replica");
        }

        Namespace::new_replica(&self.config, name).await
    }
}

/// Stores and manage a set of namespaces.
pub struct NamespaceStore<F: MakeNamespace> {
    inner: RwLock<HashMap<Bytes, Namespace<F::Database>>>,
    /// The namespace factory, to create new namespaces.
    factory: F,
}

impl NamespaceStore<ReplicaNamespaceMaker> {
    pub async fn reset(&self, namespace: Bytes) -> anyhow::Result<()> {
        let mut lock = self.inner.write().await;
        if let Some(ns) = lock.remove(&namespace) {
            // FIXME: when destroying, we are waiting for all the tasks associated with the
            // allocation to finnish, which create a lot of contention on the lock. Need to use a
            // conccurent hashmap to deal with this issue.
            ns.destroy().await?;
            // re-create the namespace
            let ns = self.factory.create(namespace.clone(), None).await?;
            lock.insert(namespace, ns);
        }

        Ok(())
    }
}

impl<F: MakeNamespace> NamespaceStore<F> {
    pub fn new(factory: F) -> Self {
        Self {
            inner: Default::default(),
            factory,
        }
    }

    pub async fn with<Fun, R>(&self, namespace: Bytes, f: Fun) -> anyhow::Result<R>
    where
        Fun: FnOnce(&Namespace<F::Database>) -> R,
    {
        let lock = self.inner.upgradable_read().await;
        if let Some(ns) = lock.get(&namespace) {
            Ok(f(ns))
        } else {
            let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
            let ns = self.factory.create(namespace.clone(), None).await?;
            let ret = f(&ns);
            lock.insert(namespace, ns);
            Ok(ret)
        }
    }

    pub async fn create_with_dump(&self, namespace: Bytes, dump: DumpStream) -> anyhow::Result<()> {
        let lock = self.inner.upgradable_read().await;
        if lock.contains_key(&namespace) {
            bail!("cannot create from dump: the namespace already exists");
        }

        let ns = self.factory.create(namespace.clone(), Some(dump)).await?;

        let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
        lock.insert(namespace, ns);

        Ok(())
    }
}

/// A namspace isolates the resources pertaining to a database of type T
#[derive(Debug)]
pub struct Namespace<T: Database> {
    pub db: T,
    /// The set of tasks associated with this namespace
    tasks: JoinSet<anyhow::Result<()>>,
    /// Path to the namespace data
    path: PathBuf,
}

pub struct ReplicaNamespaceConfig {
    /// root path of the sqld directory
    pub base_path: PathBuf,
    /// grpc channel
    pub channel: Channel,
    /// grpc uri
    pub uri: Uri,
    /// Extensions to load for the database connection
    pub extensions: Vec<PathBuf>,
    /// Stats monitor
    pub stats: Stats,
    /// Reference to the config store
    pub config_store: Arc<DatabaseConfigStore>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    /// hard reset sender.
    /// When a replica need to be wiped and recovered from scratch, its namespace
    /// is sent to this channel
    pub hard_reset: mpsc::Sender<Bytes>,
}

impl Namespace<ReplicaDatabase> {
    async fn new_replica(config: &ReplicaNamespaceConfig, name: Bytes) -> anyhow::Result<Self> {
        let name_str = std::str::from_utf8(&name)?;
        let db_path = config.base_path.join("dbs").join(name_str);
        tokio::fs::create_dir_all(&db_path).await?;
        let mut join_set = JoinSet::new();
        let replicator = Replicator::new(
            db_path.clone(),
            config.channel.clone(),
            config.uri.clone(),
            name.clone(),
            &mut join_set,
            config.hard_reset.clone(),
        )
        .await?;

        let applied_frame_no_receiver = replicator.current_frame_no_notifier.clone();

        join_set.spawn(replicator.run());

        let connection_maker = MakeWriteProxyConnection::new(
            db_path.clone(),
            config.extensions.clone(),
            config.channel.clone(),
            config.uri.clone(),
            config.stats.clone(),
            config.config_store.clone(),
            applied_frame_no_receiver,
            config.max_response_size,
            config.max_total_response_size,
            name.clone(),
        )
        .throttled(
            MAX_CONCURRENT_DBS,
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
        );

        Ok(Self {
            tasks: join_set,
            db: ReplicaDatabase {
                connection_maker: Arc::new(connection_maker),
            },
            path: db_path,
        })
    }

    async fn destroy(mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        tokio::fs::remove_dir_all(&self.path).await?;
        Ok(())
    }
}

pub struct PrimaryNamespaceConfig {
    pub base_path: PathBuf,
    pub max_log_size: u64,
    pub db_is_dirty: bool,
    pub max_log_duration: Option<Duration>,
    pub snapshot_callback: NamespacedSnapshotCallback,
    pub bottomless_replication: Option<bottomless::replicator::Options>,
    pub extensions: Vec<PathBuf>,
    pub stats: Stats,
    pub config_store: Arc<DatabaseConfigStore>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    pub checkpoint_interval: Option<Duration>,
}

type DumpStream = Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static + Unpin>;

impl Namespace<PrimaryDatabase> {
    async fn new_primary(
        config: &PrimaryNamespaceConfig,
        name: Bytes,
        dump: Option<DumpStream>,
    ) -> anyhow::Result<Self> {
        let mut join_set = JoinSet::new();
        let name_str = std::str::from_utf8(&name)?;
        let db_path = config.base_path.join("dbs").join(name_str);
        tokio::fs::create_dir_all(&db_path).await?;
        let mut is_dirty = config.db_is_dirty;

        let bottomless_replicator = if let Some(options) = &config.bottomless_replication {
            let mut options = options.clone();
            let db_id = format!("ns-{}", std::str::from_utf8(&name).unwrap());
            options.db_id = Some(db_id);
            let (replicator, did_recover) =
                init_bottomless_replicator(db_path.join("data"), options.clone()).await?;
            is_dirty |= did_recover;
            Some(Arc::new(std::sync::Mutex::new(replicator)))
        } else {
            None
        };

        tokio::fs::create_dir_all(&db_path).await?;
        let is_fresh_db = check_fresh_db(&db_path);
        // switch frame-count checkpoint to time-based one
        let auto_checkpoint =
            if config.checkpoint_interval.is_some() && config.bottomless_replication.is_some() {
                0
            } else {
                DEFAULT_AUTO_CHECKPOINT
            };
        let logger = Arc::new(ReplicationLogger::open(
            &db_path,
            config.max_log_size,
            config.max_log_duration,
            is_dirty,
            auto_checkpoint,
            Box::new({
                let name = name.clone();
                let cb = config.snapshot_callback.clone();
                move |path: &Path| cb(path, &name)
            }),
        )?);

        let ctx_builder = {
            let logger = logger.clone();
            let bottomless_replicator = bottomless_replicator.clone();
            move || ReplicationLoggerHookCtx::new(logger.clone(), bottomless_replicator.clone())
        };

        let connection_maker: Arc<_> = LibSqlDbFactory::new(
            db_path.clone(),
            &REPLICATION_METHODS,
            ctx_builder.clone(),
            config.stats.clone(),
            config.config_store.clone(),
            config.extensions.clone(),
            config.max_response_size,
            config.max_total_response_size,
            auto_checkpoint,
        )
        .await?
        .throttled(
            MAX_CONCURRENT_DBS,
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
        )
        .into();

        if let Some(dump) = dump {
            if !is_fresh_db {
                anyhow::bail!("cannot load from a dump if a database already exists.\nIf you're sure you want to load from a dump, delete your database folder at `{}`", db_path.display());
            }
            let mut ctx = ctx_builder();
            load_dump(&db_path, dump, &mut ctx).await?;
        }

        join_set.spawn(run_periodic_compactions(logger.clone()));

        Ok(Self {
            tasks: join_set,
            db: PrimaryDatabase {
                logger,
                connection_maker,
            },
            path: db_path,
        })
    }
}

const WASM_TABLE_CREATE: &str =
    "CREATE TABLE libsql_wasm_func_table (name text PRIMARY KEY, body text) WITHOUT ROWID;";

async fn load_dump<S>(
    db_path: &Path,
    dump: S,
    ctx: &mut ReplicationLoggerHookCtx,
) -> anyhow::Result<()>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    let mut retries = 0;
    let auto_checkpoint = ctx.logger().auto_checkpoint;
    // there is a small chance we fail to acquire the lock right away, so we perform a few retries
    let conn = loop {
        match block_in_place(|| open_db(db_path, &REPLICATION_METHODS, ctx, None, auto_checkpoint))
        {
            Ok(conn) => {
                break conn;
            }
            // Creating the loader database can, in rare occurences, return sqlite busy,
            // because of a race condition opening the monitor thread db. This is there to
            // retry a bunch of times if that happens.
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: ErrorCode::DatabaseBusy,
                    ..
                },
                _,
            )) if retries < 10 => {
                retries += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => {
                bail!(e);
            }
        }
    };

    let mut reader = tokio::io::BufReader::new(StreamReader::new(dump));
    let mut curr = String::new();
    let mut line = String::new();
    let mut skipped_wasm_table = false;

    while let Ok(n) = reader.read_line(&mut curr).await {
        if n == 0 {
            break;
        }
        let frag = curr.trim();

        if frag.is_empty() || frag.starts_with("--") {
            curr.clear();
            continue;
        }

        line.push_str(frag);
        curr.clear();

        // This is a hack to ignore the libsql_wasm_func_table table because it is already created
        // by the system.
        if !skipped_wasm_table && line == WASM_TABLE_CREATE {
            skipped_wasm_table = true;
            line.clear();
            continue;
        }

        if line.ends_with(';') {
            block_in_place(|| conn.execute(&line, ()))?;
            line.clear();
        } else {
            line.push(' ');
        }
    }

    Ok(())
}
