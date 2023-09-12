use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context as _};
use async_lock::{RwLock, RwLockUpgradableReadGuard};
use bottomless::replicator::Options;
use bytes::Bytes;
use chrono::NaiveDateTime;
use enclose::enclose;
use futures_core::future::BoxFuture;
use futures_core::Stream;
use hyper::Uri;
use rusqlite::ErrorCode;
use tokio::io::AsyncBufReadExt;
use tokio::task::{block_in_place, JoinSet};
use tokio_util::io::StreamReader;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::connection::config::DatabaseConfigStore;
use crate::connection::libsql::{open_db, LibSqlDbFactory};
use crate::connection::write_proxy::MakeWriteProxyConnection;
use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase, ReplicaDatabase};
use crate::error::{Error, LoadDumpError};
use crate::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use crate::replication::replica::Replicator;
use crate::replication::{NamespacedSnapshotCallback, ReplicationLogger};
use crate::stats::Stats;
use crate::{
    DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT, DEFAULT_NAMESPACE_NAME, MAX_CONCURRENT_DBS,
};

pub use fork::ForkError;

use self::fork::ForkTask;

mod fork;
pub type ResetCb = Box<dyn Fn(ResetOp) -> BoxFuture<'static, crate::Result<()>> + Send + Sync>;

pub enum ResetOp {
    Reset(Bytes),
    Destroy(Bytes),
}

/// Creates a new `Namespace` for database of the `Self::Database` type.
#[async_trait::async_trait]
pub trait MakeNamespace: Sync + Send + 'static {
    type Database: Database;

    /// Create a new Namespace instance
    async fn create(
        &self,
        name: Bytes,
        restore_option: RestoreOption,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>>;

    /// Destroy all resources associated with `namespace`.
    /// When `prune_all` is false, remove only files from local disk.
    /// When `prune_all` is true remove local database files as well as remote backup.
    async fn destroy(&self, namespace: &Bytes, prune_all: bool) -> crate::Result<()>;
    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: Bytes,
        reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>>;
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
        restore_option: RestoreOption,
        allow_creation: bool,
        _reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        Namespace::new_primary(&self.config, name, restore_option, allow_creation).await
    }

    async fn destroy(&self, namespace: &Bytes, prune_all: bool) -> crate::Result<()> {
        let ns_path = self
            .config
            .base_path
            .join("dbs")
            .join(std::str::from_utf8(namespace).unwrap());

        if prune_all {
            if let Some(ref options) = self.config.bottomless_replication {
                let options = make_bottomless_options(options, namespace);
                let replicator = bottomless::replicator::Replicator::with_options(
                    ns_path.join("data").to_str().unwrap(),
                    options,
                )
                .await?;
                let delete_all = replicator.delete_all(None).await?;

                // perform hard deletion in the background
                tokio::spawn(delete_all.commit());
            }
        }

        tokio::fs::remove_dir_all(ns_path).await?;

        Ok(())
    }

    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: Bytes,
        reset_cb: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        let fork_task = ForkTask {
            base_path: self.config.base_path.clone(),
            dest_namespace: to,
            logger: from.db.logger.clone(),
            make_namespace: self,
            reset_cb,
        };
        let ns = fork_task.fork().await?;
        Ok(ns)
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
        restore_option: RestoreOption,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        match restore_option {
            RestoreOption::Latest => { /* move on*/ }
            _ => Err(LoadDumpError::ReplicaLoadDump)?,
        }

        Namespace::new_replica(&self.config, name, allow_creation, reset).await
    }

    async fn destroy(&self, namespace: &Bytes, _prune_all: bool) -> crate::Result<()> {
        let ns_path = self
            .config
            .base_path
            .join("dbs")
            .join(std::str::from_utf8(namespace).unwrap());
        tokio::fs::remove_dir_all(ns_path).await?;
        Ok(())
    }

    async fn fork(
        &self,
        _from: &Namespace<Self::Database>,
        _to: Bytes,
        _reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        return Err(ForkError::ForkReplica.into());
    }
}

/// Stores and manage a set of namespaces.
pub struct NamespaceStore<M: MakeNamespace> {
    inner: Arc<NamespaceStoreInner<M>>,
}

impl<M: MakeNamespace> Clone for NamespaceStore<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct NamespaceStoreInner<M: MakeNamespace> {
    store: RwLock<HashMap<Bytes, Namespace<M::Database>>>,
    /// The namespace factory, to create new namespaces.
    make_namespace: M,
    allow_lazy_creation: bool,
}

impl<M: MakeNamespace> NamespaceStore<M> {
    pub fn new(make_namespace: M, allow_lazy_creation: bool) -> Self {
        Self {
            inner: Arc::new(NamespaceStoreInner {
                store: Default::default(),
                make_namespace,
                allow_lazy_creation,
            }),
        }
    }

    pub async fn destroy(&self, namespace: Bytes) -> crate::Result<()> {
        let mut lock = self.inner.store.write().await;
        if let Some(ns) = lock.remove(&namespace) {
            // FIXME: when destroying, we are waiting for all the tasks associated with the
            // allocation to finnish, which create a lot of contention on the lock. Need to use a
            // conccurent hashmap to deal with this issue.

            // deallocate in-memory resources
            ns.destroy().await?;
        }

        // destroy on-disk database and backups
        self.inner.make_namespace.destroy(&namespace, true).await?;

        tracing::info!(
            "destroyed namespace: {}",
            std::str::from_utf8(&namespace).unwrap_or_default()
        );

        Ok(())
    }

    pub async fn reset(
        &self,
        namespace: Bytes,
        restore_option: RestoreOption,
    ) -> anyhow::Result<()> {
        let mut lock = self.inner.store.write().await;
        if let Some(ns) = lock.remove(&namespace) {
            // FIXME: when destroying, we are waiting for all the tasks associated with the
            // allocation to finnish, which create a lot of contention on the lock. Need to use a
            // conccurent hashmap to deal with this issue.

            // deallocate in-memory resources
            ns.destroy().await?;
        }

        // destroy on-disk database
        self.inner.make_namespace.destroy(&namespace, false).await?;
        let ns = self
            .inner
            .make_namespace
            .create(
                namespace.clone(),
                restore_option,
                true,
                self.make_reset_cb(),
            )
            .await?;
        lock.insert(namespace, ns);

        Ok(())
    }

    fn make_reset_cb(&self) -> ResetCb {
        let this = self.clone();
        Box::new(move |op| {
            let this = this.clone();
            Box::pin(async move {
                match op {
                    ResetOp::Reset(ns) => {
                        tracing::warn!(
                            "received reset signal for: {:?}",
                            std::str::from_utf8(&ns).ok()
                        );
                        this.reset(ns, RestoreOption::Latest).await?;
                    }
                    ResetOp::Destroy(ns) => {
                        this.destroy(ns).await?;
                    }
                }
                Ok(())
            })
        })
    }

    pub async fn fork(&self, from: Bytes, to: Bytes) -> crate::Result<()> {
        let mut lock = self.inner.store.write().await;
        if lock.contains_key(&to) {
            return Err(crate::error::Error::NamespaceAlreadyExist(
                String::from_utf8(to.to_vec()).unwrap_or_default(),
            ));
        }

        // check that the source namespace exists
        let from_ns = match lock.entry(from.clone()) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                // we just want to load the namespace into memory, so we refuse creation.
                let ns = self
                    .inner
                    .make_namespace
                    .create(
                        from.clone(),
                        RestoreOption::Latest,
                        false,
                        self.make_reset_cb(),
                    )
                    .await?;
                e.insert(ns)
            }
        };

        let forked = self
            .inner
            .make_namespace
            .fork(from_ns, to.clone(), self.make_reset_cb())
            .await?;
        lock.insert(to.clone(), forked);

        Ok(())
    }

    pub async fn with<Fun, R>(&self, namespace: Bytes, f: Fun) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace<M::Database>) -> R,
    {
        let lock = self.inner.store.upgradable_read().await;
        if let Some(ns) = lock.get(&namespace) {
            Ok(f(ns))
        } else {
            let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
            let ns = self
                .inner
                .make_namespace
                .create(
                    namespace.clone(),
                    RestoreOption::Latest,
                    self.inner.allow_lazy_creation,
                    self.make_reset_cb(),
                )
                .await?;
            let ret = f(&ns);
            tracing::info!(
                "loaded namespace: `{}`",
                std::str::from_utf8(&namespace).unwrap_or_default()
            );
            lock.insert(namespace, ns);
            Ok(ret)
        }
    }

    pub async fn create(
        &self,
        namespace: Bytes,
        restore_option: RestoreOption,
    ) -> crate::Result<()> {
        let lock = self.inner.store.upgradable_read().await;
        if lock.contains_key(&namespace) {
            return Err(crate::error::Error::NamespaceAlreadyExist(
                String::from_utf8(namespace.to_vec()).unwrap_or_default(),
            ));
        }

        let ns = self
            .inner
            .make_namespace
            .create(
                namespace.clone(),
                restore_option,
                true,
                self.make_reset_cb(),
            )
            .await?;

        let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
        tracing::info!(
            "loaded namespace: `{}`",
            std::str::from_utf8(&namespace).unwrap_or_default()
        );
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
}

impl<T: Database> Namespace<T> {
    async fn destroy(mut self) -> anyhow::Result<()> {
        self.db.shutdown();
        self.tasks.shutdown().await;

        Ok(())
    }
}

pub struct ReplicaNamespaceConfig {
    pub base_path: Arc<Path>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    /// grpc channel
    pub channel: Channel,
    /// grpc uri
    pub uri: Uri,
    /// Extensions to load for the database connection
    pub extensions: Arc<[PathBuf]>,
    /// Stats monitor
    pub stats: Stats,
    /// Reference to the config store
    pub config_store: Arc<DatabaseConfigStore>,
}

impl Namespace<ReplicaDatabase> {
    async fn new_replica(
        config: &ReplicaNamespaceConfig,
        name: Bytes,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Self> {
        let name_str = std::str::from_utf8(&name).map_err(|_| Error::InvalidNamespace)?;
        let db_path = config.base_path.join("dbs").join(name_str);

        // there isn't a database folder for this database, and we're not allowed to create it.
        if !allow_creation && !db_path.exists() {
            return Err(crate::error::Error::NamespaceDoesntExist(
                String::from_utf8(name.to_vec()).unwrap_or_default(),
            ));
        }

        let mut join_set = JoinSet::new();
        let replicator = Replicator::new(
            db_path.clone(),
            config.channel.clone(),
            config.uri.clone(),
            name.clone(),
            &mut join_set,
            reset,
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
        })
    }
}

pub struct PrimaryNamespaceConfig {
    pub base_path: Arc<Path>,
    pub max_log_size: u64,
    pub db_is_dirty: bool,
    pub max_log_duration: Option<Duration>,
    pub snapshot_callback: NamespacedSnapshotCallback,
    pub bottomless_replication: Option<bottomless::replicator::Options>,
    pub extensions: Arc<[PathBuf]>,
    pub stats: Stats,
    pub config_store: Arc<DatabaseConfigStore>,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    pub checkpoint_interval: Option<Duration>,
    pub disable_namespace: bool,
}

pub type DumpStream =
    Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static + Unpin>;

fn make_bottomless_options(options: &Options, name: &Bytes) -> Options {
    let mut options = options.clone();
    let namespace = std::str::from_utf8(name).unwrap();
    let db_id = options.db_id.unwrap_or_default();
    let db_id = format!("ns-{db_id}:{namespace}");
    options.db_id = Some(db_id);
    options
}

impl Namespace<PrimaryDatabase> {
    async fn new_primary(
        config: &PrimaryNamespaceConfig,
        name: Bytes,
        restore_option: RestoreOption,
        allow_creation: bool,
    ) -> crate::Result<Self> {
        // if namespaces are disabled, then we allow creation for the default namespace.
        let allow_creation =
            allow_creation || (config.disable_namespace && name == DEFAULT_NAMESPACE_NAME);

        let mut join_set = JoinSet::new();
        let name_str = std::str::from_utf8(&name).map_err(|_| Error::InvalidNamespace)?;
        let db_path = config.base_path.join("dbs").join(name_str);

        // The database folder doesn't exist, bottomless replication is disabled (no db to recover)
        // and we're not allowed to create a new database, return an error.
        if !allow_creation && config.bottomless_replication.is_none() && !db_path.try_exists()? {
            return Err(crate::error::Error::NamespaceDoesntExist(
                String::from_utf8(name.to_vec()).unwrap_or_default(),
            ));
        }
        let mut is_dirty = config.db_is_dirty;

        tokio::fs::create_dir_all(&db_path).await?;

        let bottomless_replicator = if let Some(options) = &config.bottomless_replication {
            let options = make_bottomless_options(options, &name);
            let (replicator, did_recover) =
                init_bottomless_replicator(db_path.join("data"), options, &restore_option).await?;

            // There wasn't any database to recover from bottomless, and we are not allowed to
            // create a new database
            if !did_recover && !allow_creation && !db_path.try_exists()? {
                // clean stale directory
                // FIXME: this is not atomic, we could be left with a stale directory. Maybe do
                // setup in a temp directory and then atomically rename it?
                let _ = tokio::fs::remove_dir_all(&db_path).await;
                return Err(crate::error::Error::NamespaceDoesntExist(
                    String::from_utf8(name.to_vec()).unwrap_or_default(),
                ));
            }

            is_dirty |= did_recover;
            Some(Arc::new(std::sync::Mutex::new(replicator)))
        } else {
            None
        };

        let is_fresh_db = check_fresh_db(&db_path)?;
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

        let mut ctx = ctx_builder();
        match restore_option {
            RestoreOption::Dump(_) if !is_fresh_db => {
                Err(LoadDumpError::LoadDumpExistingDb)?;
            }
            RestoreOption::Dump(dump) => {
                load_dump(&db_path, dump, &mut ctx).await?;
            }
            _ => { /* other cases were already handled when creating bottomless */ }
        }

        join_set.spawn(run_periodic_compactions(logger.clone()));

        Ok(Self {
            tasks: join_set,
            db: PrimaryDatabase {
                logger,
                connection_maker,
            },
        })
    }
}

#[derive(Default)]
pub enum RestoreOption {
    /// Restore database state from the most recent version found in a backup.
    #[default]
    Latest,
    /// Restore database from SQLite dump.
    Dump(DumpStream),
    /// Restore database state to a backup version equal to specific generation.
    Generation(Uuid),
    /// Restore database state to a backup version present at a specific point in time.
    /// Granularity depends of how frequently WAL log pages are being snapshotted.
    PointInTime(NaiveDateTime),
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

pub async fn init_bottomless_replicator(
    path: impl AsRef<std::path::Path>,
    options: bottomless::replicator::Options,
    restore_option: &RestoreOption,
) -> anyhow::Result<(bottomless::replicator::Replicator, bool)> {
    tracing::debug!("Initializing bottomless replication");
    let path = path
        .as_ref()
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("Invalid db path"))?
        .to_owned();
    let mut replicator = bottomless::replicator::Replicator::with_options(path, options).await?;

    let (generation, timestamp) = match restore_option {
        RestoreOption::Latest | RestoreOption::Dump(_) => (None, None),
        RestoreOption::Generation(generation) => (Some(*generation), None),
        RestoreOption::PointInTime(timestamp) => (None, Some(*timestamp)),
    };

    let (action, did_recover) = replicator.restore(generation, timestamp).await?;
    match action {
        bottomless::replicator::RestoreAction::SnapshotMainDbFile => {
            replicator.new_generation();
            replicator.snapshot_main_db_file(None).await?;
            // Restoration process only leaves the local WAL file if it was
            // detected to be newer than its remote counterpart.
            replicator.maybe_replicate_wal().await?
        }
        bottomless::replicator::RestoreAction::ReuseGeneration(gen) => {
            replicator.set_generation(gen);
        }
    }

    Ok((replicator, did_recover))
}

async fn run_periodic_compactions(logger: Arc<ReplicationLogger>) -> anyhow::Result<()> {
    // calling `ReplicationLogger::maybe_compact()` is cheap if the compaction does not actually
    // take place, so we can affort to poll it very often for simplicity
    let mut interval = tokio::time::interval(Duration::from_millis(1000));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let handle = tokio::task::spawn_blocking(enclose! {(logger) move || {
            logger.maybe_compact()
        }});
        handle
            .await
            .expect("Compaction task crashed")
            .context("Compaction failed")?;
    }
}

fn check_fresh_db(path: &Path) -> crate::Result<bool> {
    let is_fresh = !path.join("wallog").try_exists()?;
    Ok(is_fresh)
}
