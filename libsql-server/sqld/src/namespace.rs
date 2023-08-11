use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use bytes::Bytes;
use hyper::Uri;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tonic::transport::Channel;

use crate::connection::config::DatabaseConfigStore;
use crate::connection::dump::loader::DumpLoader;
use crate::connection::libsql::LibSqlDbFactory;
use crate::connection::write_proxy::MakeWriteProxyConnection;
use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase, ReplicaDatabase};
use crate::replication::primary::logger::{ReplicationLoggerHookCtx, REPLICATION_METHODS};
use crate::replication::replica::Replicator;
use crate::replication::{NamespacedSnapshotCallback, ReplicationLogger};
use crate::stats::Stats;
use crate::{
    check_fresh_db, init_bottomless_replicator, run_periodic_compactions, DB_CREATE_TIMEOUT,
    MAX_CONCURRENT_DBS,
};

/// Creates a new `Namespace` for database of the `Self::Database` type.
#[async_trait::async_trait]
pub trait MakeNamespace: Sync + Send + 'static {
    type Database: Database;

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database>>;
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

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database>> {
        Namespace::new_primary(&self.config, name).await
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

    async fn create(&self, name: Bytes) -> anyhow::Result<Namespace<Self::Database>> {
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
            let ns = self.factory.create(namespace.clone()).await?;
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
            let ns = self.factory.create(namespace.clone()).await?;
            let ret = f(&ns);
            lock.insert(namespace, ns);
            Ok(ret)
        }
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
    pub load_from_dump: Option<PathBuf>,
    pub max_total_response_size: u64,
}

impl Namespace<PrimaryDatabase> {
    async fn new_primary(config: &PrimaryNamespaceConfig, name: Bytes) -> anyhow::Result<Self> {
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
        let logger = Arc::new(ReplicationLogger::open(
            &db_path,
            config.max_log_size,
            config.max_log_duration,
            is_dirty,
            Box::new({
                let name = name.clone();
                let cb = config.snapshot_callback.clone();
                move |path: &Path| cb(path, &name)
            }),
        )?);

        join_set.spawn(run_periodic_compactions(logger.clone()));

        // load dump is necessary
        let dump_loader = DumpLoader::new(
            db_path.clone(),
            logger.clone(),
            bottomless_replicator.clone(),
        )
        .await?;
        if let Some(ref path) = config.load_from_dump {
            if !is_fresh_db {
                anyhow::bail!("cannot load from a dump if a database already exists.\nIf you're sure you want to load from a dump, delete your database folder at `{}`", db_path.display());
            }
            dump_loader.load_dump(path.into()).await?;
        }

        let connection_maker: Arc<_> = LibSqlDbFactory::new(
            db_path.clone(),
            &REPLICATION_METHODS,
            {
                let logger = logger.clone();
                let bottomless_replicator = bottomless_replicator.clone();
                move || ReplicationLoggerHookCtx::new(logger.clone(), bottomless_replicator.clone())
            },
            config.stats.clone(),
            config.config_store.clone(),
            config.extensions.clone(),
            config.max_response_size,
            config.max_total_response_size,
        )
        .await?
        .throttled(
            MAX_CONCURRENT_DBS,
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
        )
        .into();

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
