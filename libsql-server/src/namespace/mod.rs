mod fork;
pub mod meta_store;
pub mod replication_wal;

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Context as _;
use async_lock::RwLock;
use bottomless::replicator::Options;
use bytes::Bytes;
use chrono::NaiveDateTime;
use enclose::enclose;
use futures::TryFutureExt;
use futures_core::{Future, Stream};
use hyper::Uri;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_sys::wal::{Sqlite3WalManager, WalManager};
use moka::future::Cache;
use parking_lot::Mutex;
use rusqlite::ErrorCode;
use serde::de::Visitor;
use serde::Deserialize;
use tokio::io::AsyncBufReadExt;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};
use tokio_util::io::StreamReader;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::auth::parse_jwt_key;
use crate::auth::Authenticated;
use crate::config::MetaStoreConfig;
use crate::connection::config::DatabaseConfig;
use crate::connection::libsql::{open_conn, MakeLibSqlConn};
use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::Connection;
use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase, ReplicaDatabase};
use crate::error::{Error, LoadDumpError};
use crate::metrics::NAMESPACE_LOAD_LATENCY;
use crate::replication::{FrameNo, NamespacedSnapshotCallback, ReplicationLogger};
use crate::stats::Stats;
use crate::{
    run_periodic_checkpoint, StatsSender, BLOCKING_RT, DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT,
    MAX_CONCURRENT_DBS,
};

use crate::namespace::fork::PointInTimeRestore;
pub use fork::ForkError;

use self::fork::ForkTask;
use self::meta_store::{MetaStore, MetaStoreHandle};
use self::replication_wal::make_replication_wal;

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

impl AsRef<str> for NamespaceName {
    fn as_ref(&self) -> &str {
        self.as_str()
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

impl<'de> Deserialize<'de> for NamespaceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = NamespaceName;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a valid namespace name")
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NamespaceName::from_string(v).map_err(|e| E::custom(e))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NamespaceName::from_string(v.to_string()).map_err(|e| E::custom(e))
            }
        }

        deserializer.deserialize_string(V)
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
        reset: ResetCb,
        meta_store: &MetaStore,
    ) -> crate::Result<Namespace<Self::Database>>;

    /// Destroy all resources associated with `namespace`.
    /// When `prune_all` is false, remove only files from local disk.
    /// When `prune_all` is true remove local database files as well as remote backup.
    async fn destroy(
        &self,
        namespace: NamespaceName,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        prune_all: bool,
        meta_store: &MetaStore,
    ) -> crate::Result<()>;

    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: NamespaceName,
        timestamp: Option<NaiveDateTime>,
        meta_store: &MetaStore,
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
        _reset: ResetCb,
        meta_store: &MetaStore,
    ) -> crate::Result<Namespace<Self::Database>> {
        Namespace::new_primary(
            &self.config,
            name.clone(),
            restore_option,
            bottomless_db_id,
            meta_store.handle(name),
        )
        .await
    }

    async fn destroy(
        &self,
        namespace: NamespaceName,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        prune_all: bool,
        meta_store: &MetaStore,
    ) -> crate::Result<()> {
        let ns_path = self.config.base_path.join("dbs").join(namespace.as_str());
        let db_config = meta_store.remove(namespace.clone())?;

        if prune_all {
            if let Some(ref options) = self.config.bottomless_replication {
                let bottomless_db_id = match bottomless_db_id_init {
                    NamespaceBottomlessDbIdInit::Provided(db_id) => db_id,
                    NamespaceBottomlessDbIdInit::FetchFromConfig => {
                        if !ns_path.try_exists()? {
                            NamespaceBottomlessDbId::NotProvided
                        } else if let Some(config) = db_config {
                            NamespaceBottomlessDbId::from_config(&config)
                        } else {
                            return Err(Error::NamespaceDoesntExist(namespace.to_string()));
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
            tracing::debug!("removing database directory: {}", ns_path.display());
            tokio::fs::remove_dir_all(ns_path).await?;
        }

        Ok(())
    }

    async fn fork(
        &self,
        from: &Namespace<Self::Database>,
        to: NamespaceName,
        timestamp: Option<NaiveDateTime>,
        meta_store: &MetaStore,
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
            logger: from.db.wal_manager.wrapped().logger(),
            make_namespace: self,
            restore_to,
            bottomless_db_id,
            meta_store,
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
        reset: ResetCb,
        meta_store: &MetaStore,
    ) -> crate::Result<Namespace<Self::Database>> {
        match restore_option {
            RestoreOption::Latest => { /* move on*/ }
            _ => Err(LoadDumpError::ReplicaLoadDump)?,
        }

        Namespace::new_replica(&self.config, name.clone(), reset, meta_store.handle(name)).await
    }

    async fn destroy(
        &self,
        namespace: NamespaceName,
        _bottomless_db_id_init: NamespaceBottomlessDbIdInit,
        _prune_all: bool,
        _meta_store: &MetaStore,
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
        _meta_store: &MetaStore,
    ) -> crate::Result<Namespace<Self::Database>> {
        return Err(ForkError::ForkReplica.into());
    }
}

type NamespaceEntry<T> = Arc<RwLock<Option<Namespace<T>>>>;

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
    store: Cache<NamespaceName, NamespaceEntry<M::Database>>,
    metadata: MetaStore,
    /// The namespace factory, to create new namespaces.
    make_namespace: M,
    allow_lazy_creation: bool,
    has_shutdown: AtomicBool,
    snapshot_at_shutdown: bool,
}

impl<M: MakeNamespace> NamespaceStore<M> {
    pub async fn new(
        make_namespace: M,
        allow_lazy_creation: bool,
        snapshot_at_shutdown: bool,
        max_active_namespaces: usize,
        base_path: &Path,
        meta_store_config: Option<MetaStoreConfig>,
    ) -> crate::Result<Self> {
        let metadata = MetaStore::new(meta_store_config, base_path).await?;
        tracing::trace!("Max active namespaces: {max_active_namespaces}");
        let store = Cache::<NamespaceName, NamespaceEntry<M::Database>>::builder()
            .async_eviction_listener(move |name, ns, cause| {
                tracing::debug!("evicting namespace `{name}` asynchronously: {cause:?}");
                // TODO(sarna): not clear if we should snapshot-on-evict...
                // On the one hand, better to do so, because we have no idea
                // for how long we're evicting a namespace.
                // On the other, if there's lots of cache pressure, snapshotting
                // very often will kill the machine's I/O.
                Box::pin(async move {
                    tracing::info!("namespace `{name}` deallocated");
                    // shutdown namespace
                    if let Some(ns) = ns.write().await.take() {
                        if let Err(e) = ns.shutdown(snapshot_at_shutdown).await {
                            tracing::error!("error deallocating `{name}`: {e}")
                        }
                    }
                })
            })
            .max_capacity(max_active_namespaces as u64)
            .time_to_idle(Duration::from_secs(86400))
            .build();

        Ok(Self {
            inner: Arc::new(NamespaceStoreInner {
                store,
                metadata,
                make_namespace,
                allow_lazy_creation,
                has_shutdown: AtomicBool::new(false),
                snapshot_at_shutdown,
            }),
        })
    }

    pub async fn destroy(&self, namespace: NamespaceName) -> crate::Result<()> {
        if self.inner.has_shutdown.load(Ordering::Relaxed) {
            return Err(Error::NamespaceStoreShutdown);
        }
        let mut bottomless_db_id_init = NamespaceBottomlessDbIdInit::FetchFromConfig;
        if let Some(ns) = self.inner.store.remove(&namespace).await {
            // deallocate in-memory resources
            if let Some(ns) = ns.write().await.take() {
                bottomless_db_id_init = NamespaceBottomlessDbIdInit::Provided(
                    NamespaceBottomlessDbId::from_config(&ns.db_config_store.get()),
                );
                ns.destroy().await?;
            }
        }

        // destroy on-disk database and backups
        self.inner
            .make_namespace
            .destroy(
                namespace.clone(),
                bottomless_db_id_init,
                true,
                &self.inner.metadata,
            )
            .await?;
        tracing::info!("destroyed namespace: {namespace}");

        Ok(())
    }

    pub async fn reset(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
    ) -> anyhow::Result<()> {
        // The process for reseting is as follow:
        // - get a lock on the namespace entry, if the entry exists, then it's a lock on the entry,
        // if it doesn't exist, insert an empty entry and take a lock on it
        // - destroy the old namespace
        // - create a new namespace and insert it in the held lock
        let entry = self
            .inner
            .store
            .get_with(namespace.clone(), async { Default::default() })
            .await;
        let mut lock = entry.write().await;
        if let Some(ns) = lock.take() {
            ns.destroy().await?;
        }
        // destroy on-disk database
        self.inner
            .make_namespace
            .destroy(
                namespace.clone(),
                NamespaceBottomlessDbIdInit::FetchFromConfig,
                false,
                &self.inner.metadata,
            )
            .await?;
        let ns = self
            .inner
            .make_namespace
            .create(
                namespace.clone(),
                restore_option,
                NamespaceBottomlessDbId::NotProvided,
                self.make_reset_cb(),
                &self.inner.metadata,
            )
            .await?;

        lock.replace(ns);

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

        // check that the source namespace exists
        if !self.inner.metadata.exists(&from) {
            return Err(crate::error::Error::NamespaceDoesntExist(from.to_string()));
        }

        let to_entry = self
            .inner
            .store
            .get_with(to.clone(), async { Default::default() })
            .await;
        let mut to_lock = to_entry.write().await;
        if to_lock.is_some() {
            return Err(crate::error::Error::NamespaceAlreadyExist(to.to_string()));
        }

        let from_entry = self
            .inner
            .store
            .try_get_with(from.clone(), async {
                let ns = self
                    .inner
                    .make_namespace
                    .create(
                        from.clone(),
                        RestoreOption::Latest,
                        NamespaceBottomlessDbId::NotProvided,
                        self.make_reset_cb(),
                        &self.inner.metadata,
                    )
                    .await?;
                tracing::info!("loaded namespace: `{to}`");
                Ok::<_, crate::error::Error>(Arc::new(RwLock::new(Some(ns))))
            })
            .await?;

        let from_lock = from_entry.read().await;
        let Some(from_ns) = &*from_lock else {
            return Err(crate::error::Error::NamespaceDoesntExist(to.to_string()));
        };

        let to_ns = self
            .inner
            .make_namespace
            .fork(from_ns, to.clone(), timestamp, &self.inner.metadata)
            .await?;

        to_lock.replace(to_ns);

        Ok(())
    }

    pub async fn with_authenticated<Fun, R>(
        &self,
        namespace: NamespaceName,
        auth: Authenticated,
        f: Fun,
    ) -> crate::Result<R>
    where
        Fun: FnOnce(&Namespace<M::Database>) -> R + 'static,
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
        Fun: FnOnce(&Namespace<M::Database>) -> R + 'static,
    {
        let init = {
            let namespace = namespace.clone();
            async move {
                if namespace != NamespaceName::default()
                    && !self.inner.metadata.exists(&namespace)
                    && !self.inner.allow_lazy_creation
                {
                    return Err(Error::NamespaceDoesntExist(namespace.to_string()));
                }
                let ns = self
                    .inner
                    .make_namespace
                    .create(
                        namespace.clone(),
                        RestoreOption::Latest,
                        NamespaceBottomlessDbId::NotProvided,
                        self.make_reset_cb(),
                        &self.inner.metadata,
                    )
                    .await?;
                tracing::info!("loaded namespace: `{namespace}`");

                Ok(Some(ns))
            }
        };

        let f = {
            let name = namespace.clone();
            move |ns: NamespaceEntry<M::Database>| async move {
                let lock = ns.read().await;
                match &*lock {
                    Some(ns) => Ok(f(ns)),
                    // the namespace was taken out of the entry
                    None => Err(Error::NamespaceDoesntExist(name.to_string())),
                }
            }
        };

        self.with_lock_or_init(namespace, f, init).await?
    }

    async fn with_lock_or_init<Fun, R, Init, Fut>(
        &self,
        namespace: NamespaceName,
        f: Fun,
        init: Init,
    ) -> crate::Result<R>
    where
        Fun: FnOnce(NamespaceEntry<M::Database>) -> Fut,
        Fut: Future<Output = R>,
        Init: Future<Output = crate::Result<Option<Namespace<M::Database>>>>,
    {
        let before_load = Instant::now();
        let ns = self
            .inner
            .store
            .try_get_with(
                namespace.clone(),
                init.map_ok(|ns| Arc::new(RwLock::new(ns))),
            )
            .await?;
        NAMESPACE_LOAD_LATENCY.record(before_load.elapsed());
        Ok(f(ns).await)
    }

    pub async fn create(
        &self,
        namespace: NamespaceName,
        restore_option: RestoreOption,
        bottomless_db_id: NamespaceBottomlessDbId,
    ) -> crate::Result<()> {
        // With namespaces disabled, the default namespace can be auto-created,
        // otherwise it's an error.
        if self.inner.allow_lazy_creation || namespace == NamespaceName::default() {
            tracing::trace!("auto-creating the namespace");
        } else if self.inner.metadata.exists(&namespace) {
            return Err(Error::NamespaceAlreadyExist(namespace.to_string()));
        }

        let name = namespace.clone();
        let bottomless_db_id_for_init = bottomless_db_id.clone();
        let init = async {
            let ns = self
                .inner
                .make_namespace
                .create(
                    name.clone(),
                    restore_option,
                    bottomless_db_id_for_init,
                    self.make_reset_cb(),
                    &self.inner.metadata,
                )
                .await;
            match ns {
                Ok(ns) => {
                    tracing::info!("loaded namespace: `{name}`");
                    Ok(Some(ns))
                }
                // return an empty slot to put the new namespace in
                Err(Error::NamespaceDoesntExist(_)) => Ok(None),
                Err(e) => Err(e),
            }
        };

        self.with_lock_or_init(namespace, |_| async { Ok(()) }, init)
            .await?
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        self.inner.has_shutdown.store(true, Ordering::Relaxed);
        for (_name, entry) in self.inner.store.iter() {
            let mut lock = entry.write().await;
            if let Some(ns) = lock.take() {
                ns.shutdown(self.inner.snapshot_at_shutdown).await?;
            }
        }
        self.inner.metadata.shutdown().await?;
        self.inner.store.invalidate_all();
        self.inner.store.run_pending_tasks().await;
        Ok(())
    }

    pub(crate) async fn stats(&self, namespace: NamespaceName) -> crate::Result<Arc<Stats>> {
        self.with(namespace, |ns| ns.stats.clone()).await
    }

    pub(crate) async fn config_store(
        &self,
        namespace: NamespaceName,
    ) -> crate::Result<MetaStoreHandle> {
        self.with(namespace, |ns| ns.db_config_store.clone()).await
    }
}

/// A namespace isolates the resources pertaining to a database of type T
#[derive(Debug)]
pub struct Namespace<T> {
    pub db: T,
    name: NamespaceName,
    /// The set of tasks associated with this namespace
    tasks: JoinSet<anyhow::Result<()>>,
    stats: Arc<Stats>,
    db_config_store: MetaStoreHandle,
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

    async fn shutdown(mut self, should_checkpoint: bool) -> anyhow::Result<()> {
        self.tasks.shutdown().await;
        if should_checkpoint {
            self.checkpoint().await?;
        }
        self.db.shutdown().await?;
        Ok(())
    }

    pub fn config(&self) -> Arc<DatabaseConfig> {
        self.db_config_store.get()
    }

    pub fn config_version(&self) -> usize {
        self.db_config_store.version()
    }

    pub fn jwt_key(&self) -> crate::Result<Option<jsonwebtoken::DecodingKey>> {
        let config = self.db_config_store.get();
        if let Some(jwt_key) = config.jwt_key.as_deref() {
            Ok(Some(
                parse_jwt_key(jwt_key).context("Could not parse JWT decoding key")?,
            ))
        } else {
            Ok(None)
        }
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
    pub encryption_key: Option<bytes::Bytes>,
}

impl Namespace<ReplicaDatabase> {
    #[tracing::instrument(skip(config, reset, meta_store_handle))]
    async fn new_replica(
        config: &ReplicaNamespaceConfig,
        name: NamespaceName,
        reset: ResetCb,
        meta_store_handle: MetaStoreHandle,
    ) -> crate::Result<Self> {
        tracing::debug!("creating replica namespace");
        let db_path = config.base_path.join("dbs").join(name.as_str());

        let rpc_client =
            ReplicationLogClient::with_origin(config.channel.clone(), config.uri.clone());
        let client = crate::replication::replicator_client::Client::new(
            name.clone(),
            rpc_client,
            &db_path,
            meta_store_handle.clone(),
        )
        .await?;
        let applied_frame_no_receiver = client.current_frame_no_notifier.subscribe();
        let mut replicator = libsql_replication::replicator::Replicator::new(
            client,
            db_path.join("data"),
            DEFAULT_AUTO_CHECKPOINT,
            config.encryption_key.clone(),
        )
        .await?;

        tracing::debug!("try perform handshake");
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

        tracing::debug!("done performing handshake");

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
            config.encryption_key.clone(),
        )
        .await?;

        let connection_maker = MakeWriteProxyConn::new(
            db_path.clone(),
            config.extensions.clone(),
            config.channel.clone(),
            config.uri.clone(),
            stats.clone(),
            meta_store_handle.clone(),
            applied_frame_no_receiver,
            config.max_response_size,
            config.max_total_response_size,
            name.clone(),
            primary_current_replicatio_index,
            config.encryption_key.clone(),
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
            db_config_store: meta_store_handle,
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
    pub encryption_key: Option<bytes::Bytes>,
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
        meta_store_handle: MetaStoreHandle,
    ) -> crate::Result<Self> {
        // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
        match Self::try_new_primary(
            config,
            name.clone(),
            restore_option,
            bottomless_db_id,
            meta_store_handle,
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
        meta_store_handle: MetaStoreHandle,
    ) -> crate::Result<Self> {
        let mut join_set = JoinSet::new();
        let db_path = config.base_path.join("dbs").join(name.as_str());

        let mut is_dirty = config.db_is_dirty;

        tokio::fs::create_dir_all(&db_path).await?;

        let bottomless_db_id = match bottomless_db_id {
            NamespaceBottomlessDbId::Namespace(ref db_id) => {
                let config = &*(meta_store_handle.get()).clone();
                let config = DatabaseConfig {
                    bottomless_db_id: Some(db_id.clone()),
                    ..config.clone()
                };
                meta_store_handle.store(config).await?;
                bottomless_db_id
            }
            NamespaceBottomlessDbId::NotProvided => {
                NamespaceBottomlessDbId::from_config(&meta_store_handle.get())
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
            is_dirty |= did_recover;
            Some(replicator)
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
        )?);

        let stats = make_stats(
            &db_path,
            &mut join_set,
            config.stats_sender.clone(),
            name.clone(),
            logger.new_frame_notifier.subscribe(),
            config.encryption_key.clone(),
        )
        .await?;

        let wal_manager = make_replication_wal(bottomless_replicator, logger.clone());
        let connection_maker: Arc<_> = MakeLibSqlConn::new(
            db_path.clone(),
            wal_manager.clone(),
            stats.clone(),
            meta_store_handle.clone(),
            config.extensions.clone(),
            config.max_response_size,
            config.max_total_response_size,
            auto_checkpoint,
            logger.new_frame_notifier.subscribe(),
            config.encryption_key.clone(),
        )
        .await?
        .throttled(
            MAX_CONCURRENT_DBS,
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
        )
        .into();

        // this must happen after we create the connection maker. The connection maker old on a
        // connection to ensure that no other connection is closing while we try to open the dump.
        // that would cause a SQLITE_LOCKED error.
        match restore_option {
            RestoreOption::Dump(_) if !is_fresh_db => {
                Err(LoadDumpError::LoadDumpExistingDb)?;
            }
            RestoreOption::Dump(dump) => {
                load_dump(
                    &db_path,
                    dump,
                    wal_manager.clone(),
                    config.encryption_key.clone(),
                )
                .await?;
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
                wal_manager,
                connection_maker,
            },
            name,
            stats,
            db_config_store: meta_store_handle,
        })
    }
}

async fn make_stats(
    db_path: &Path,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    stats_sender: StatsSender,
    name: NamespaceName,
    mut current_frame_no: watch::Receiver<Option<FrameNo>>,
    encryption_key: Option<bytes::Bytes>,
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

    join_set.spawn(run_storage_monitor(
        db_path.into(),
        Arc::downgrade(&stats),
        encryption_key,
    ));

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

async fn load_dump<S, C>(
    db_path: &Path,
    dump: S,
    wal_manager: C,
    encryption_key: Option<bytes::Bytes>,
) -> crate::Result<(), LoadDumpError>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
    C: WalManager + Clone + Send + 'static,
    C::Wal: Send + 'static,
{
    let mut retries = 0;
    // there is a small chance we fail to acquire the lock right away, so we perform a few retries
    let conn = loop {
        let db_path = db_path.to_path_buf();
        let wal_manager = wal_manager.clone();

        let encryption_key = encryption_key.clone();
        match tokio::task::spawn_blocking(move || {
            open_conn(&db_path, wal_manager, None, encryption_key)
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
async fn run_storage_monitor(
    db_path: PathBuf,
    stats: Weak<Stats>,
    encryption_key: Option<bytes::Bytes>,
) -> anyhow::Result<()> {
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

        let encryption_key = encryption_key.clone();
        let _ = tokio::task::spawn_blocking(move || {
            // because closing the last connection interferes with opening a new one, we lazily
            // initialize a connection here, and keep it alive for the entirety of the program. If we
            // fail to open it, we wait for `duration` and try again later.
            match open_conn(&db_path, Sqlite3WalManager::new(), Some(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY), encryption_key) {
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
