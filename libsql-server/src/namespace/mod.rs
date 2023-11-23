use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Context as _;
use async_lock::{RwLock, RwLockUpgradableReadGuard};
use bottomless::replicator::Options;
use bytes::Bytes;
use chrono::NaiveDateTime;
use enclose::enclose;
use futures_core::Stream;
use hyper::Uri;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use parking_lot::Mutex;
use rusqlite::ErrorCode;
use sqld_libsql_bindings::wal_hook::TRANSPARENT_METHODS;
use tokio::io::AsyncBufReadExt;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};
use tokio_util::io::StreamReader;
use tonic::transport::Channel;
use tracing::trace;
use uuid::Uuid;

use crate::auth::Authenticated;
use crate::connection::config::{DatabaseConfig, DatabaseConfigStore};
use crate::connection::libsql::{open_conn, MakeLibSqlConn};
use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::Connection;
use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase, ReplicaDatabase};
use crate::error::{Error, LoadDumpError};
use crate::metrics::NAMESPACE_LOAD_LATENCY;
use crate::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use crate::replication::{FrameNo, NamespacedSnapshotCallback, ReplicationLogger};
use crate::stats::Stats;
use crate::{
    run_periodic_checkpoint, StatsSender, BLOCKING_RT, DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT,
    MAX_CONCURRENT_DBS,
};

use crate::namespace::fork::PointInTimeRestore;
pub use fork::ForkError;

use self::fork::ForkTask;

mod fork;
pub type ResetCb = Box<dyn Fn(ResetOp) + Send + Sync + 'static>;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NamespaceName(Bytes);

impl fmt::Debug for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Default for NamespaceName {
    fn default() -> Self {
        Self(Bytes::from_static(b"default"))
    }
}

impl NamespaceName {
    pub fn from_string(s: String) -> crate::Result<Self> {
        Self::validate(&s)?;
        Ok(Self(Bytes::from(s)))
    }

    fn validate(s: &str) -> crate::Result<()> {
        if s.is_empty() {
            return Err(crate::error::Error::InvalidNamespace);
        }

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        // Safety: the namespace is always valid UTF8
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    pub fn from_bytes(bytes: Bytes) -> crate::Result<Self> {
        let s = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidNamespace)?;
        Self::validate(s)?;
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

pub enum ResetOp {
    Reset(NamespaceName),
    Destroy(NamespaceName),
}

#[derive(Clone, Debug)]
pub enum NamespaceBottomlessDbId {
    Namespace(String),
    NotProvided,
}

impl NamespaceBottomlessDbId {
    fn from_config(config: &DatabaseConfig) -> NamespaceBottomlessDbId {
        match config.bottomless_db_id.clone() {
            Some(db_id) => NamespaceBottomlessDbId::Namespace(db_id),
            None => NamespaceBottomlessDbId::NotProvided,
        }
    }
}

#[derive(Clone, Debug)]
pub enum NamespaceBottomlessDbIdInit {
    Provided(NamespaceBottomlessDbId),
    FetchFromConfig,
}

/// Creates a new `Namespace` for database of the `Self::Database` type.
#[async_trait::async_trait]
pub trait MakeNamespace: Sync + Send + 'static {
    type Database: Database;

    /// Create a new Namespace instance
    async fn create(
        &self,
        name: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>>;

    /// Destroy all resources associated with `namespace`.
    /// When `prune_all` is false, remove only files from local disk.
    /// When `prune_all` is true remove local database files as well as remote backup.
    async fn destroy(
        &self,
        namespace: NamespaceName,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        prune_all: bool,
    ) -> crate::Result<()>;
    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: NamespaceName,
        timestamp: Option<NaiveDateTime>,
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
        name: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
        allow_creation: bool,
        _reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        Namespace::new_primary(
            &self.config,
            name,
            restore_option,
            bottomless_db_id,
            allow_creation,
        )
        .await
    }

    async fn destroy(
        &self,
        namespace: NamespaceName,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        prune_all: bool,
    ) -> crate::Result<()> {
        let ns_path = self.config.base_path.join("dbs").join(namespace.as_str());

        if prune_all {
            if let Some(ref options) = self.config.bottomless_replication {
                let bottomless_db_id = match bottomless_db_id_init {
                    NamespaceBottomlessDbIdInit::Provided(db_id) => db_id,
                    NamespaceBottomlessDbIdInit::FetchFromConfig => {
                        if !ns_path.try_exists()? {
                            NamespaceBottomlessDbId::NotProvided
                        } else {
                            let db_config_store_result = DatabaseConfigStore::load(&ns_path);
                            let db_config_store = match db_config_store_result {
                                Ok(store) => store,
                                Err(err) => {
                                    tracing::error!("could not load database: {}", err);
                                    return Err(err);
                                }
                            };
                            let config = db_config_store.get();
                            NamespaceBottomlessDbId::from_config(&config)
                        }
                    }
                };
                let options = make_bottomless_options(options, bottomless_db_id, namespace);
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

        if ns_path.try_exists()? {
            tokio::fs::remove_dir_all(ns_path).await?;
        }

        Ok(())
    }

    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: NamespaceName,
        timestamp: Option<NaiveDateTime>,
    ) -> crate::Result<Namespace<Self::Database>> {
        let bottomless_db_id = NamespaceBottomlessDbId::from_config(&from.db_config_store.get());
        let restore_to = if let Some(timestamp) = timestamp {
            if let Some(ref options) = self.config.bottomless_replication {
                Some(PointInTimeRestore {
                    timestamp,
                    replicator_options: make_bottomless_options(
                        options,
                        bottomless_db_id.clone(),
                        from.name().clone(),
                    ),
                })
            } else {
                return Err(Error::Fork(ForkError::BackupServiceNotConfigured));
            }
        } else {
            None
        };
        let fork_task = ForkTask {
            base_path: self.config.base_path.clone(),
            dest_namespace: to,
            logger: from.db.logger.clone(),
            make_namespace: self,
            restore_to,
            bottomless_db_id,
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
        name: NamespaceName,
        restore_option: RestoreOption,
        _bottomless_db_id: NamespaceBottomlessDbId,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Namespace<Self::Database>> {
        match restore_option {
            RestoreOption::Latest => { /* move on*/ }
            _ => Err(LoadDumpError::ReplicaLoadDump)?,
        }

        Namespace::new_replica(&self.config, name, allow_creation, reset).await
    }

    async fn destroy(
        &self,
        namespace: NamespaceName,
        _bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        _prune_all: bool,
    ) -> crate::Result<()> {
        let ns_path = self.config.base_path.join("dbs").join(namespace.as_str());
        tokio::fs::remove_dir_all(ns_path).await?;
        Ok(())
    }

    async fn fork(
        &self,
        _from: &Namespace<Self::Database>,
        _to: NamespaceName,
        _timestamp: Option<NaiveDateTime>,
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
    store: RwLock<HashMap<NamespaceName, Namespace<M::Database>>>,
    /// The namespace factory, to create new namespaces.
    make_namespace: M,
    allow_lazy_creation: bool,
    has_shutdown: AtomicBool,
}

impl<M: MakeNamespace> NamespaceStore<M> {
    pub fn new(make_namespace: M, allow_lazy_creation: bool) -> Self {
        Self {
            inner: Arc::new(NamespaceStoreInner {
                store: Default::default(),
                make_namespace,
                allow_lazy_creation,
                has_shutdown: AtomicBool::new(false),
            }),
        }
    }

    pub async fn destroy(&self, namespace: NamespaceName) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let mut lock = self.inner.store.write().await;
        let mut bottomless_db_id_init = NamespaceBottomlessDbIdInit::FetchFromConfig;
        if let Some(ns) = lock.remove(&namespace) {
            bottomless_db_id_init = NamespaceBottomlessDbIdInit::Provided(
                NamespaceBottomlessDbId::from_config(&ns.db_config_store.get()),
            );
            // FIXME: when destroying, we are waiting for all the tasks associated with the
            // allocation to finnish, which create a lot of contention on the lock. Need to use a
            // conccurent hashmap to deal with this issue.

            // deallocate in-memory resources
            ns.destroy().await?;
        }

        // destroy on-disk database and backups
        self.inner
            .make_namespace
            .destroy(namespace.clone(), bottomless_db_id_init, true)
            .await?;

        tracing::info!("destroyed namespace: {namespace}");

        Ok(())
    }

    async fn reset(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
    ) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let mut lock = self.inner.store.write().await;
        if let Some(ns) = lock.remove(&namespace) {
            // FIXME: when destroying, we are waiting for all the tasks associated with the
            // allocation to finnish, which create a lot of contention on the lock. Need to use a
            // conccurent hashmap to deal with this issue.

            // deallocate in-memory resources
            ns.destroy().await?;
        }

        // destroy on-disk database
        self.inner
            .make_namespace
            .destroy(
                namespace.clone(),
                NamespaceBottomlessDbIdInit::FetchFromConfig,
                false,
            )
            .await?;
        let ns = self
            .inner
            .make_namespace
            .create(
                namespace.clone(),
                restore_option,
                NamespaceBottomlessDbId::NotProvided,
                true,
                self.make_reset_cb(),
            )
            .await?;
        lock.insert(namespace, ns);

        Ok(())
    }

    // This is only called on replica
    fn make_reset_cb(&self) -> ResetCb {
        let this = self.clone();
        Box::new(move |op| {
            let this = this.clone();
            tokio::spawn(async move {
                match op {
                    ResetOp::Reset(ns) => {
                        tracing::info!("received reset signal for: {ns}");
                        if let Err(e) = this.reset(ns.clone(), RestoreOption::Latest).await {
                            tracing::error!("error resetting namespace `{ns}`: {e}");
                        }
                    }
                    ResetOp::Destroy(ns) => {
                        if let Err(e) = this.destroy(ns.clone()).await {
                            tracing::error!("error destroying namesace `{ns}`: {e}",);
                        }
                    }
                }
            });
        })
    }

    pub async fn fork(
        &self,
        from: NamespaceName,
        to: NamespaceName,
        timestamp: Option<NaiveDateTime>,
    ) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let mut lock = self.inner.store.write().await;
        if lock.contains_key(&to) {
            return Err(crate::error::Error::NamespaceAlreadyExist(
                to.as_str().to_string(),
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
                        NamespaceBottomlessDbId::NotProvided,
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
            .fork(from_ns, to.clone(), timestamp)
            .await?;
        lock.insert(to.clone(), forked);

        Ok(())
    }

    pub async fn with_authenticated<Fun, R>(
        &self,
        namespace: NamespaceName,
        auth: Authenticated,
        f: Fun,
    ) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace<M::Database>) -> R,
    {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        if !auth.is_namespace_authorized(&namespace) {
            return Err(Error::NamespaceDoesntExist(namespace.to_string()));
        }

        self.with(namespace, f).await
    }

    pub async fn with<Fun, R>(&self, namespace: NamespaceName, f: Fun) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace<M::Database>) -> R,
    {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let before_load = Instant::now();
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
                    NamespaceBottomlessDbId::NotProvided,
                    self.inner.allow_lazy_creation,
                    self.make_reset_cb(),
                )
                .await?;
            let ret = f(&ns);
            tracing::info!("loaded namespace: `{namespace}`");
            lock.insert(namespace, ns);

            NAMESPACE_LOAD_LATENCY.record(before_load.elapsed());

            Ok(ret)
        }
    }

    pub async fn create(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
    ) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let lock = self.inner.store.upgradable_read().await;
        if lock.contains_key(&namespace) {
            return Err(crate::error::Error::NamespaceAlreadyExist(
                namespace.as_str().to_owned(),
            ));
        }

        let ns = self
            .inner
            .make_namespace
            .create(
                namespace.clone(),
                restore_option,
                bottomless_db_id,
                true,
                self.make_reset_cb(),
            )
            .await?;

        let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
        tracing::info!("loaded namespace: `{namespace}`");
        lock.insert(namespace, ns);

        Ok(())
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        self.inner.has_shutdown.store(true, Ordering::Relaxed);
        let mut lock = self.inner.store.write().await;
        for (name, ns) in lock.drain() {
            ns.shutdown().await?;
            trace!("shutdown namespace: `{}`", name);
        }
        Ok(())
    }

    pub(crate) async fn stats(&self, namespace: NamespaceName) -> crate::Result<Arc<Stats>> {
        self.with(namespace, |ns| ns.stats.clone()).await
    }

    pub(crate) async fn config_store(
        &self,
        namespace: NamespaceName,
    ) -> crate::Result<Arc<DatabaseConfigStore>> {
        self.with(namespace, |ns| ns.db_config_store.clone()).await
    }
}

/// A namespace isolates the resources pertaining to a database of type T
#[derive(Debug)]
pub struct Namespace<T: Database> {
    pub db: T,
    name: NamespaceName,
    /// The set of tasks associated with this namespace
    tasks: JoinSet<anyhow::Result<()>>,
    stats: Arc<Stats>,
    db_config_store: Arc<DatabaseConfigStore>,
}

impl<T: Database> Namespace<T> {
    pub(crate) fn name(&self) -> &NamespaceName {
        &self.name
    }

    async fn destroy(mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        self.db.destroy();
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        let conn = self.db.connection_maker().create().await?;
        conn.vacuum_if_needed().await?;
        conn.checkpoint().await?;
        Ok(())
    }

    async fn shutdown(mut self) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        self.checkpoint().await?;
        self.db.shutdown().await?;
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
    pub stats_sender: StatsSender,
}

impl Namespace<ReplicaDatabase> {
    async fn new_replica(
        config: &ReplicaNamespaceConfig,
        name: NamespaceName,
        allow_creation: bool,
        reset: ResetCb,
    ) -> crate::Result<Self> {
        let db_path = config.base_path.join("dbs").join(name.as_str());

        // there isn't a database folder for this database, and we're not allowed to create it.
        if !allow_creation && !db_path.exists() {
            return Err(crate::error::Error::NamespaceDoesntExist(
                name.as_str().to_owned(),
            ));
        }

        let db_config_store = Arc::new(
            DatabaseConfigStore::load(&db_path).context("Could not load database config")?,
        );

        let rpc_client =
            ReplicationLogClient::with_origin(config.channel.clone(), config.uri.clone());
        let client =
            crate::replication::replicator_client::Client::new(name.clone(), rpc_client, &db_path)
                .await?;
        let applied_frame_no_receiver = client.current_frame_no_notifier.subscribe();
        let mut replicator = libsql_replication::replicator::Replicator::new(
            client,
            db_path.join("data"),
            DEFAULT_AUTO_CHECKPOINT,
        )
        .await?;

        // force a handshake now, to retrieve the primary's current replication index
        match replicator.try_perform_handshake().await {
            Err(libsql_replication::replicator::Error::Meta(
                libsql_replication::meta::Error::LogIncompatible,
            )) => {
                tracing::error!("trying to replicate incompatible logs, reseting replica");
                (reset)(ResetOp::Reset(name.clone()));
            }
            Err(e) => Err(e)?,
            Ok(_) => (),
        }
        let primary_current_replicatio_index = replicator.client_mut().primary_replication_index;

        let mut join_set = JoinSet::new();
        let namespace = name.clone();
        join_set.spawn(async move {
            use libsql_replication::replicator::Error;
            loop {
                match replicator.run().await {
                    err @ Error::Fatal(_) => Err(err)?,
                    err @ Error::NamespaceDoesntExist => {
                        tracing::error!("namespace {namespace} doesn't exist, destroying...");
                        (reset)(ResetOp::Destroy(namespace.clone()));
                        Err(err)?;
                    }
                    Error::Meta(err) => {
                        use libsql_replication::meta::Error;
                        match err {
                            Error::LogIncompatible => {
                                tracing::error!("trying to replicate incompatible logs, reseting replica");
                                (reset)(ResetOp::Reset(namespace.clone()));
                                Err(err)?;
                            }
                            Error::InvalidMetaFile
                            | Error::Io(_)
                            | Error::InvalidLogId
                            | Error::FailedToCommit(_) => {
                                // We retry from last frame index?
                                tracing::warn!("non-fatal replication error, retrying from last commit index: {err}");
                            },
                        }
                    }
                    e @ (Error::Internal(_)
                    | Error::Injector(_)
                    | Error::Client(_)
                    | Error::PrimaryHandshakeTimeout
                    | Error::NeedSnapshot) => {
                        tracing::warn!("non-fatal replication error, retrying from last commit index: {e}");
                    },
                    Error::NoHandshake => {
                        // not strictly necessary, but in case the handshake error goes uncaught,
                        // we reset the client state.
                        replicator.client_mut().reset_token();
                    }
                }
            }
        });

        let stats = make_stats(
            &db_path,
            &mut join_set,
            config.stats_sender.clone(),
            name.clone(),
            applied_frame_no_receiver.clone(),
        )
        .await?;

        let connection_maker = MakeWriteProxyConn::new(
            db_path.clone(),
            config.extensions.clone(),
            config.channel.clone(),
            config.uri.clone(),
            stats.clone(),
            db_config_store.clone(),
            applied_frame_no_receiver,
            config.max_response_size,
            config.max_total_response_size,
            name.clone(),
            primary_current_replicatio_index,
        )
        .await?
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
            name,
            stats,
            db_config_store,
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
    pub stats_sender: StatsSender,
    pub max_response_size: u64,
    pub max_total_response_size: u64,
    pub checkpoint_interval: Option<Duration>,
    pub disable_namespace: bool,
}

pub type DumpStream =
    Box<dyn Stream<Item = std::io::Result<Bytes>> + Send + Sync + 'static + Unpin>;

fn make_bottomless_options(
    options: &Options,
    namespace_db_id: NamespaceBottomlessDbId,
    name: NamespaceName,
) -> Options {
    let mut options = options.clone();
    let mut db_id = match namespace_db_id {
        NamespaceBottomlessDbId::Namespace(id) => id,
        NamespaceBottomlessDbId::NotProvided => options.db_id.unwrap_or_default(),
    };

    db_id = format!("ns-{db_id}:{name}");
    options.db_id = Some(db_id);
    options
}

impl Namespace<PrimaryDatabase> {
    async fn new_primary(
        config: &PrimaryNamespaceConfig,
        name: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
        allow_creation: bool,
    ) -> crate::Result<Self> {
        // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
        match Self::try_new_primary(
            config,
            name.clone(),
            restore_option,
            bottomless_db_id,
            allow_creation,
        )
        .await
        {
            Ok(ns) => Ok(ns),
            Err(e) => {
                let path = config.base_path.join("dbs").join(name.as_str());
                if let Err(e) = tokio::fs::remove_dir_all(path).await {
                    tracing::error!("failed to clean dirty namespace: {e}");
                }
                Err(e)
            }
        }
    }

    async fn try_new_primary(
        config: &PrimaryNamespaceConfig,
        name: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
        allow_creation: bool,
    ) -> crate::Result<Self> {
        // if namespaces are disabled, then we allow creation for the default namespace.
        let allow_creation =
            allow_creation || (config.disable_namespace && name == NamespaceName::default());

        let mut join_set = JoinSet::new();
        let db_path = config.base_path.join("dbs").join(name.as_str());

        // The database folder doesn't exist, bottomless replication is disabled (no db to recover)
        // and we're not allowed to create a new database, return an error.
        if !allow_creation && config.bottomless_replication.is_none() && !db_path.try_exists()? {
            return Err(crate::error::Error::NamespaceDoesntExist(name.to_string()));
        }
        let mut is_dirty = config.db_is_dirty;

        tokio::fs::create_dir_all(&db_path).await?;
        let db_config_store = Arc::new(
            DatabaseConfigStore::load(&db_path).context("Could not load database config")?,
        );
        let bottomless_db_id = match bottomless_db_id {
            NamespaceBottomlessDbId::Namespace(ref db_id) => {
                let mut config = (*db_config_store.get()).clone();
                config.bottomless_db_id = Some(db_id.clone());
                db_config_store.store(config)?;
                bottomless_db_id
            }
            NamespaceBottomlessDbId::NotProvided => {
                NamespaceBottomlessDbId::from_config(&db_config_store.get())
            }
        };

        // FIXME: due to a bug in logger::checkpoint_db we call regular checkpointing code
        // instead of our virtual WAL one. It's a bit tangled to fix right now, because
        // we need WAL context for checkpointing, and WAL context needs the ReplicationLogger...
        // So instead we checkpoint early, *before* bottomless gets initialized. That way
        // we're sure bottomless won't try to back up any existing WAL frames and will instead
        // treat the existing db file as the source of truth.
        if config.bottomless_replication.is_some() {
            tracing::debug!("Checkpointing before initializing bottomless");
            crate::replication::primary::logger::checkpoint_db(&db_path.join("data"))?;
            tracing::debug!("Checkpointed before initializing bottomless");
        }

        let bottomless_replicator = if let Some(options) = &config.bottomless_replication {
            let options = make_bottomless_options(options, bottomless_db_id, name.clone());
            let (replicator, did_recover) =
                init_bottomless_replicator(db_path.join("data"), options, &restore_option).await?;

            // There wasn't any database to recover from bottomless, and we are not allowed to
            // create a new database
            if !did_recover && !allow_creation && !db_path.try_exists()? {
                // clean stale directory
                // FIXME: this is not atomic, we could be left with a stale directory. Maybe do
                // setup in a temp directory and then atomically rename it?
                let _ = tokio::fs::remove_dir_all(&db_path).await;
                return Err(crate::error::Error::NamespaceDoesntExist(name.to_string()));
            }

            is_dirty |= did_recover;
            Some(Arc::new(std::sync::Mutex::new(Some(replicator))))
        } else {
            None
        };

        let is_fresh_db = check_fresh_db(&db_path)?;
        // switch frame-count checkpoint to time-based one
        let auto_checkpoint = if config.checkpoint_interval.is_some() {
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
            bottomless_replicator.clone(),
        )?);

        let ctx_builder = {
            let logger = logger.clone();
            let bottomless_replicator = bottomless_replicator.clone();
            move || ReplicationLoggerHookCtx::new(logger.clone(), bottomless_replicator.clone())
        };

        let stats = make_stats(
            &db_path,
            &mut join_set,
            config.stats_sender.clone(),
            name.clone(),
            logger.new_frame_notifier.subscribe(),
        )
        .await?;

        let connection_maker: Arc<_> = MakeLibSqlConn::new(
            db_path.clone(),
            &REPLICATION_METHODS,
            ctx_builder.clone(),
            stats.clone(),
            db_config_store.clone(),
            config.extensions.clone(),
            config.max_response_size,
            config.max_total_response_size,
            auto_checkpoint,
            logger.new_frame_notifier.subscribe(),
        )
        .await?
        .throttled(
            MAX_CONCURRENT_DBS,
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
        )
        .into();

        match restore_option {
            RestoreOption::Dump(_) if !is_fresh_db => {
                Err(LoadDumpError::LoadDumpExistingDb)?;
            }
            RestoreOption::Dump(dump) => {
                load_dump(&db_path, dump, ctx_builder, logger.auto_checkpoint).await?;
            }
            _ => { /* other cases were already handled when creating bottomless */ }
        }

        join_set.spawn(run_periodic_compactions(logger.clone()));

        if let Some(checkpoint_interval) = config.checkpoint_interval {
            join_set.spawn(run_periodic_checkpoint(
                connection_maker.clone(),
                checkpoint_interval,
            ));
        }

        Ok(Self {
            tasks: join_set,
            db: PrimaryDatabase {
                logger,
                connection_maker,
            },
            name,
            stats,
            db_config_store,
        })
    }
}

async fn make_stats(
    db_path: &Path,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    stats_sender: StatsSender,
    name: NamespaceName,
    mut current_frame_no: watch::Receiver<Option<FrameNo>>,
) -> anyhow::Result<Arc<Stats>> {
    let stats = Stats::new(name.clone(), db_path, join_set).await?;

    // the storage monitor is optional, so we ignore the error here.
    let _ = stats_sender
        .send((name.clone(), Arc::downgrade(&stats)))
        .await;

    join_set.spawn({
        let stats = stats.clone();
        // initialize the current_frame_no value
        current_frame_no
            .borrow_and_update()
            .map(|fno| stats.set_current_frame_no(fno));
        async move {
            while current_frame_no.changed().await.is_ok() {
                current_frame_no
                    .borrow_and_update()
                    .map(|fno| stats.set_current_frame_no(fno));
            }
            Ok(())
        }
    });

    join_set.spawn(run_storage_monitor(db_path.into(), Arc::downgrade(&stats)));

    Ok(stats)
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
    mk_ctx: impl Fn() -> ReplicationLoggerHookCtx,
    auto_checkpoint: u32,
) -> crate::Result<(), LoadDumpError>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
{
    let mut retries = 0;
    // there is a small chance we fail to acquire the lock right away, so we perform a few retries
    let conn = loop {
        let ctx = mk_ctx();
        let db_path = db_path.to_path_buf();
        match tokio::task::spawn_blocking(move || {
            open_conn(&db_path, &REPLICATION_METHODS, ctx, None, auto_checkpoint)
        })
        .await?
        {
            Ok(conn) => {
                break conn;
            }
            // Creating the loader database can, in rare occurrences, return sqlite busy,
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
            Err(e) => Err(e)?,
        }
    };

    let conn = Arc::new(Mutex::new(conn));

    let mut reader = tokio::io::BufReader::new(StreamReader::new(dump));
    let mut curr = String::new();
    let mut line = String::new();
    let mut skipped_wasm_table = false;
    let mut n_stmt = 0;

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
            n_stmt += 1;
            // dump must be performd within a txn
            if n_stmt > 2 && conn.lock().is_autocommit() {
                return Err(LoadDumpError::NoTxn);
            }

            line = tokio::task::spawn_blocking({
                let conn = conn.clone();
                move || -> crate::Result<String, LoadDumpError> {
                    conn.lock().execute(&line, ())?;
                    Ok(line)
                }
            })
            .await??;
            line.clear();
        } else {
            line.push(' ');
        }
    }

    if !conn.lock().is_autocommit() {
        let _ = conn.lock().execute("rollback", ());
        return Err(LoadDumpError::NoCommit);
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
            if let Some(_handle) = replicator.snapshot_main_db_file().await? {
                tracing::trace!("got snapshot handle after restore with generation upgrade");
            }
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
    // take place, so we can afford to poll it very often for simplicity
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1000));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;
        let handle = BLOCKING_RT.spawn_blocking(enclose! {(logger) move || {
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

// Periodically check the storage used by the database and save it in the Stats structure.
// TODO: Once we have a separate fiber that does WAL checkpoints, running this routine
// right after checkpointing is exactly where it should be done.
async fn run_storage_monitor(db_path: PathBuf, stats: Weak<Stats>) -> anyhow::Result<()> {
    // on initialization, the database file doesn't exist yet, so we wait a bit for it to be
    // created
    tokio::time::sleep(Duration::from_secs(1)).await;

    let duration = tokio::time::Duration::from_secs(60);
    let db_path: Arc<Path> = db_path.into();
    loop {
        let db_path = db_path.clone();
        let Some(stats) = stats.upgrade() else {
            return Ok(());
        };
        let _ = tokio::task::spawn_blocking(move || {
            // because closing the last connection interferes with opening a new one, we lazily
            // initialize a connection here, and keep it alive for the entirety of the program. If we
            // fail to open it, we wait for `duration` and try again later.
            // We can safely open db with DEFAULT_AUTO_CHECKPOINT, since monitor is read-only: it 
            // won't produce new updates, frames or generate checkpoints.
            match open_conn(&db_path, &TRANSPARENT_METHODS, (), Some(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY), DEFAULT_AUTO_CHECKPOINT) {
                Ok(conn) => {
                    if let Ok(storage_bytes_used) =
                        conn.query_row("select sum(pgsize) from dbstat;", [], |row| {
                            row.get::<usize, u64>(0)
                        })
                    {
                        stats.set_storage_bytes_used(storage_bytes_used);
                    }

                },
                Err(e) => {
                    tracing::warn!("failed to open connection for storager monitor: {e}, trying again in {duration:?}");
                },
            }
        }).await;

        tokio::time::sleep(duration).await;
    }
}
