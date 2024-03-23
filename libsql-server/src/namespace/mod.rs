mod fork;
pub mod meta_store;
mod name;
pub mod replication_wal;
mod schema_lock;
mod store;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context as _, Error};
use bottomless::replicator::Options;
use bytes::Bytes;
use chrono::NaiveDateTime;
use enclose::enclose;
use futures_core::{Future, Stream};
use hyper::Uri;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_sys::wal::wrapper::WrapWal;
use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager, WalManager};
use libsql_sys::EncryptionConfig;
use parking_lot::Mutex;
use rusqlite::ErrorCode;
use tokio::io::AsyncBufReadExt;
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio_util::io::StreamReader;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::auth::parse_jwt_key;
use crate::connection::config::DatabaseConfig;
use crate::connection::libsql::{open_conn, MakeLibSqlConn};
use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::Connection;
use crate::connection::MakeConnection;
use crate::database::{
    Database, DatabaseKind, PrimaryConnectionMaker, PrimaryDatabase, ReplicaDatabase,
    SchemaDatabase,
};
use crate::error::LoadDumpError;
use crate::replication::script_backup_manager::ScriptBackupManager;
use crate::replication::{FrameNo, ReplicationLogger};
use crate::schema::{has_pending_migration_task, setup_migration_table, SchedulerHandle};
use crate::stats::Stats;
use crate::{
    run_periodic_checkpoint, StatsSender, BLOCKING_RT, DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT,
};

pub use fork::ForkError;

use self::fork::{ForkTask, PointInTimeRestore};
use self::meta_store::MetaStoreHandle;
pub use self::name::NamespaceName;
use self::replication_wal::{make_replication_wal_wrapper, ReplicationWalWrapper};
pub use self::store::NamespaceStore;

pub type ResetCb = Box<dyn Fn(ResetOp) + Send + Sync + 'static>;
pub type ResolveNamespacePathFn =
    Arc<dyn Fn(&NamespaceName) -> crate::Result<Arc<Path>> + Sync + Send + 'static>;

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

/// A namespace isolates the resources pertaining to a database of type T
#[derive(Debug)]
pub struct Namespace {
    pub db: Database,
    name: NamespaceName,
    /// The set of tasks associated with this namespace
    tasks: JoinSet<anyhow::Result<()>>,
    stats: Arc<Stats>,
    db_config_store: MetaStoreHandle,
    path: Arc<Path>,
}

impl Namespace {
    async fn from_config(
        ns_config: &NamespaceConfig,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> crate::Result<Self> {
        match ns_config.db_kind {
            DatabaseKind::Primary if db_config.get().is_shared_schema => {
                Self::new_schema(
                    ns_config,
                    name.clone(),
                    db_config,
                    restore_option,
                    resolve_attach_path,
                )
                .await
            }
            DatabaseKind::Primary => {
                Self::new_primary(
                    ns_config,
                    name.clone(),
                    db_config,
                    restore_option,
                    resolve_attach_path,
                )
                .await
            }
            DatabaseKind::Replica => {
                Self::new_replica(
                    ns_config,
                    name.clone(),
                    db_config,
                    reset,
                    resolve_attach_path,
                )
                .await
            }
        }
    }

    pub(crate) fn name(&self) -> &NamespaceName {
        &self.name
    }

    /// completely remove resources associated with the namespace
    pub(crate) async fn cleanup(
        ns_config: &NamespaceConfig,
        name: &NamespaceName,
        db_config: &DatabaseConfig,
        prune_all: bool,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> crate::Result<()> {
        let ns_path = ns_config.base_path.join("dbs").join(name.as_str());
        match ns_config.db_kind {
            DatabaseKind::Primary => {
                if let Some(ref options) = ns_config.bottomless_replication {
                    let bottomless_db_id = match bottomless_db_id_init {
                        NamespaceBottomlessDbIdInit::Provided(db_id) => db_id,
                        NamespaceBottomlessDbIdInit::FetchFromConfig => {
                            NamespaceBottomlessDbId::from_config(&db_config)
                        }
                    };
                    let options = make_bottomless_options(options, bottomless_db_id, name.clone());
                    let replicator = bottomless::replicator::Replicator::with_options(
                        ns_path.join("data").to_str().unwrap(),
                        options,
                    )
                    .await?;
                    if prune_all {
                        let delete_all = replicator.delete_all(None).await?;
                        // perform hard deletion in the background
                        tokio::spawn(delete_all.commit());
                    } else {
                        // for soft delete make sure that local db is fully backed up
                        replicator.savepoint().confirmed().await?;
                    }
                }
            }
            DatabaseKind::Replica => (),
        }

        if ns_path.try_exists()? {
            tracing::debug!("removing database directory: {}", ns_path.display());
            tokio::fs::remove_dir_all(ns_path).await?;
        }

        Ok(())
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
        let _ = tokio::fs::remove_file(self.path.join(".sentinel")).await;
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

    pub fn stats(&self) -> Arc<Stats> {
        self.stats.clone()
    }

    pub fn config_changed(&self) -> impl Future<Output = ()> {
        self.db_config_store.changed()
    }

    async fn new_primary(
        config: &NamespaceConfig,
        name: NamespaceName,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> crate::Result<Self> {
        let db_path: Arc<Path> = config.base_path.join("dbs").join(name.as_str()).into();
        let fresh_namespace = !db_path.try_exists()?;
        // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
        match Self::try_new_primary(
            config,
            name.clone(),
            meta_store_handle,
            restore_option,
            resolve_attach_path,
            db_path.clone(),
        )
        .await
        {
            Ok(this) => Ok(this),
            Err(e) if fresh_namespace => {
                tracing::error!("an error occured while deleting creating namespace, cleaning...");
                if let Err(e) = tokio::fs::remove_dir_all(&db_path).await {
                    tracing::error!("failed to remove dirty namespace directory: {e}")
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn make_primary_connection_maker(
        ns_config: &NamespaceConfig,
        meta_store_handle: &MetaStoreHandle,
        db_path: &Path,
        name: &NamespaceName,
        restore_option: RestoreOption,
        block_writes: Arc<AtomicBool>,
        join_set: &mut JoinSet<anyhow::Result<()>>,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> crate::Result<(PrimaryConnectionMaker, ReplicationWalWrapper, Arc<Stats>)> {
        let db_config = meta_store_handle.get();
        let bottomless_db_id = NamespaceBottomlessDbId::from_config(&db_config);
        // FIXME: figure how to to it per-db
        let mut is_dirty = {
            let sentinel_path = db_path.join(".sentinel");
            if sentinel_path.try_exists()? {
                true
            } else {
                tokio::fs::File::create(&sentinel_path).await?;
                false
            }
        };

        // FIXME: due to a bug in logger::checkpoint_db we call regular checkpointing code
        // instead of our virtual WAL one. It's a bit tangled to fix right now, because
        // we need WAL context for checkpointing, and WAL context needs the ReplicationLogger...
        // So instead we checkpoint early, *before* bottomless gets initialized. That way
        // we're sure bottomless won't try to back up any existing WAL frames and will instead
        // treat the existing db file as the source of truth.

        let bottomless_replicator = match ns_config.bottomless_replication {
            Some(ref options) => {
                tracing::debug!("Checkpointing before initializing bottomless");
                crate::replication::primary::logger::checkpoint_db(&db_path.join("data"))?;
                tracing::debug!("Checkpointed before initializing bottomless");
                let options = make_bottomless_options(options, bottomless_db_id, name.clone());
                let (replicator, did_recover) =
                    init_bottomless_replicator(db_path.join("data"), options, &restore_option)
                        .await?;
                is_dirty |= did_recover;
                Some(replicator)
            }
            None => None,
        };

        let is_fresh_db = check_fresh_db(&db_path)?;
        // switch frame-count checkpoint to time-based one
        let auto_checkpoint = if ns_config.checkpoint_interval.is_some() {
            0
        } else {
            DEFAULT_AUTO_CHECKPOINT
        };

        let logger = Arc::new(ReplicationLogger::open(
            &db_path,
            ns_config.max_log_size,
            ns_config.max_log_duration,
            is_dirty,
            auto_checkpoint,
            ns_config.scripted_backup.clone(),
            name.clone(),
            ns_config.encryption_config.clone(),
        )?);

        let stats = make_stats(
            &db_path,
            join_set,
            ns_config.stats_sender.clone(),
            name.clone(),
            logger.new_frame_notifier.subscribe(),
            ns_config.encryption_config.clone(),
        )
        .await?;

        let wal_wrapper = make_replication_wal_wrapper(bottomless_replicator, logger.clone());
        let connection_maker = MakeLibSqlConn::new(
            db_path.to_path_buf(),
            wal_wrapper.clone(),
            stats.clone(),
            meta_store_handle.clone(),
            ns_config.extensions.clone(),
            ns_config.max_response_size,
            ns_config.max_total_response_size,
            auto_checkpoint,
            logger.new_frame_notifier.subscribe(),
            ns_config.encryption_config.clone(),
            block_writes,
            resolve_attach_path,
        )
        .await?
        .throttled(
            ns_config.max_concurrent_connections.clone(),
            Some(DB_CREATE_TIMEOUT),
            ns_config.max_total_response_size,
            ns_config.max_concurrent_requests,
        );

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
                    wal_wrapper.clone().map_wal(),
                    ns_config.encryption_config.clone(),
                )
                .await?;
            }
            _ => { /* other cases were already handled when creating bottomless */ }
        }

        join_set.spawn(run_periodic_compactions(logger.clone()));

        Ok((connection_maker, wal_wrapper, stats))
    }

    async fn try_new_primary(
        ns_config: &NamespaceConfig,
        name: NamespaceName,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        resolve_attach_path: ResolveNamespacePathFn,
        db_path: Arc<Path>,
    ) -> crate::Result<Self> {
        let mut join_set = JoinSet::new();

        tokio::fs::create_dir_all(&db_path).await?;

        let block_writes = Arc::new(AtomicBool::new(false));
        let (connection_maker, wal_wrapper, stats) = Self::make_primary_connection_maker(
            ns_config,
            &meta_store_handle,
            &db_path,
            &name,
            restore_option,
            block_writes.clone(),
            &mut join_set,
            resolve_attach_path,
        )
        .await?;
        let connection_maker = Arc::new(connection_maker);

        if meta_store_handle.get().shared_schema_name.is_some() {
            let block_writes = block_writes.clone();
            let conn = connection_maker.create().await?;
            tokio::task::spawn_blocking(move || {
                conn.with_raw(|conn| -> crate::Result<()> {
                    setup_migration_table(conn)?;
                    if has_pending_migration_task(conn)? {
                        block_writes.store(true, Ordering::SeqCst);
                    }
                    Ok(())
                })
            })
            .await
            .unwrap()?;
        }

        if let Some(checkpoint_interval) = ns_config.checkpoint_interval {
            join_set.spawn(run_periodic_checkpoint(
                connection_maker.clone(),
                checkpoint_interval,
            ));
        }

        Ok(Self {
            tasks: join_set,
            db: Database::Primary(PrimaryDatabase {
                wal_wrapper,
                connection_maker,
                block_writes,
            }),
            name,
            stats,
            db_config_store: meta_store_handle,
            path: db_path.into(),
        })
    }

    #[tracing::instrument(skip(config, reset, meta_store_handle, resolve_attach_path))]
    #[async_recursion::async_recursion]
    async fn new_replica(
        config: &NamespaceConfig,
        name: NamespaceName,
        meta_store_handle: MetaStoreHandle,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> crate::Result<Self> {
        tracing::debug!("creating replica namespace");
        let db_path = config.base_path.join("dbs").join(name.as_str());
        let channel = config.channel.clone().expect("bad replica config");
        let uri = config.uri.clone().expect("bad replica config");

        let rpc_client = ReplicationLogClient::with_origin(channel.clone(), uri.clone());
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
            config.encryption_config.clone(),
        )
        .await?;

        tracing::debug!("try perform handshake");
        // force a handshake now, to retrieve the primary's current replication index
        match replicator.try_perform_handshake().await {
            Err(libsql_replication::replicator::Error::Meta(
                libsql_replication::meta::Error::LogIncompatible,
            )) => {
                tracing::error!(
                    "trying to replicate incompatible logs, reseting replica and nuking db dir"
                );
                std::fs::remove_dir_all(&db_path).unwrap();
                return Self::new_replica(
                    config,
                    name,
                    meta_store_handle,
                    reset,
                    resolve_attach_path,
                )
                .await;
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
                    e @ Error::Injector(_) => {
                        tracing::error!("potential corruption detected while replicating, reseting  replica: {e}");
                        (reset)(ResetOp::Reset(namespace.clone()));
                        Err(e)?;
                    },
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
                            | Error::FailedToCommit(_)
                            | Error::InvalidReplicationPath
                            | Error::RequiresCleanDatabase => {
                                // We retry from last frame index?
                                tracing::warn!("non-fatal replication error, retrying from last commit index: {err}");
                            },
                        }
                    }
                    e @ (Error::Internal(_)
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
                    Error::SnapshotPending => unreachable!(),
                }
            }
        });

        let stats = make_stats(
            &db_path,
            &mut join_set,
            config.stats_sender.clone(),
            name.clone(),
            applied_frame_no_receiver.clone(),
            config.encryption_config.clone(),
        )
        .await?;

        let connection_maker = MakeWriteProxyConn::new(
            db_path.clone(),
            config.extensions.clone(),
            channel.clone(),
            uri.clone(),
            stats.clone(),
            meta_store_handle.clone(),
            applied_frame_no_receiver,
            config.max_response_size,
            config.max_total_response_size,
            primary_current_replicatio_index,
            config.encryption_config.clone(),
            resolve_attach_path,
        )
        .await?
        .throttled(
            config.max_concurrent_connections.clone(),
            Some(DB_CREATE_TIMEOUT),
            config.max_total_response_size,
            config.max_concurrent_requests,
        );

        Ok(Self {
            tasks: join_set,
            db: Database::Replica(ReplicaDatabase {
                connection_maker: Arc::new(connection_maker),
            }),
            name,
            stats,
            db_config_store: meta_store_handle,
            path: db_path.into(),
        })
    }

    async fn fork(
        ns_config: &NamespaceConfig,
        from_ns: &Namespace,
        from_config: MetaStoreHandle,
        to_ns: NamespaceName,
        to_config: MetaStoreHandle,
        timestamp: Option<NaiveDateTime>,
        resolve_attach: ResolveNamespacePathFn,
    ) -> crate::Result<Namespace> {
        let from_config = from_config.get();
        match ns_config.db_kind {
            DatabaseKind::Primary => {
                let bottomless_db_id = NamespaceBottomlessDbId::from_config(&from_config);
                let restore_to = if let Some(timestamp) = timestamp {
                    if let Some(ref options) = ns_config.bottomless_replication {
                        Some(PointInTimeRestore {
                            timestamp,
                            replicator_options: make_bottomless_options(
                                options,
                                bottomless_db_id.clone(),
                                from_ns.name().clone(),
                            ),
                        })
                    } else {
                        return Err(crate::Error::Fork(ForkError::BackupServiceNotConfigured));
                    }
                } else {
                    None
                };

                let logger = match &from_ns.db {
                    Database::Primary(db) => db.wal_wrapper.wrapper().logger(),
                    Database::Schema(db) => db.wal_wrapper.wrapper().logger(),
                    _ => {
                        return Err(crate::Error::Fork(ForkError::Internal(Error::msg(
                            "Invalid source database type for fork",
                        ))));
                    }
                };

                let fork_task = ForkTask {
                    base_path: ns_config.base_path.clone(),
                    to_namespace: to_ns,
                    logger,
                    restore_to,
                    bottomless_db_id,
                    to_config,
                    ns_config,
                    resolve_attach,
                };

                let ns = fork_task.fork().await?;
                Ok(ns)
            }
            DatabaseKind::Replica => Err(ForkError::ForkReplica.into()),
        }
    }

    async fn new_schema(
        ns_config: &NamespaceConfig,
        name: NamespaceName,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        resolve_attach_path: ResolveNamespacePathFn,
    ) -> crate::Result<Namespace> {
        let mut join_set = JoinSet::new();
        let db_path = ns_config.base_path.join("dbs").join(name.as_str());

        tokio::fs::create_dir_all(&db_path).await?;

        let (connection_maker, wal_manager, stats) = Self::make_primary_connection_maker(
            ns_config,
            &meta_store_handle,
            &db_path,
            &name,
            restore_option,
            Arc::new(AtomicBool::new(false)), // this is always false for schema
            &mut join_set,
            resolve_attach_path,
        )
        .await?;

        Ok(Namespace {
            db: Database::Schema(SchemaDatabase::new(
                ns_config.migration_scheduler.clone(),
                name.clone(),
                connection_maker,
                wal_manager,
                meta_store_handle.clone(),
            )),
            name,
            tasks: join_set,
            stats,
            db_config_store: meta_store_handle,
            path: db_path.into(),
        })
    }
}

pub struct NamespaceConfig {
    /// Default database kind the store should be Creating
    pub(crate) db_kind: DatabaseKind,
    // Common config
    pub(crate) base_path: Arc<Path>,
    pub(crate) max_log_size: u64,
    pub(crate) max_log_duration: Option<Duration>,
    pub(crate) extensions: Arc<[PathBuf]>,
    pub(crate) stats_sender: StatsSender,
    pub(crate) max_response_size: u64,
    pub(crate) max_total_response_size: u64,
    pub(crate) checkpoint_interval: Option<Duration>,
    pub(crate) max_concurrent_connections: Arc<Semaphore>,
    pub(crate) max_concurrent_requests: u64,
    pub(crate) encryption_config: Option<EncryptionConfig>,

    // Replica specific config
    /// grpc channel for replica
    pub channel: Option<Channel>,
    /// grpc uri
    pub uri: Option<Uri>,

    // primary only config
    pub(crate) bottomless_replication: Option<bottomless::replicator::Options>,
    pub(crate) scripted_backup: Option<ScriptBackupManager>,
    pub(crate) migration_scheduler: SchedulerHandle,
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
        // FIXME(marin): I don't like that, if bottomless is enabled, proper config must be passed.
        NamespaceBottomlessDbId::NotProvided => options.db_id.unwrap_or_default(),
    };

    db_id = format!("ns-{db_id}:{name}");
    options.db_id = Some(db_id);
    options
}

async fn make_stats(
    db_path: &Path,
    join_set: &mut JoinSet<anyhow::Result<()>>,
    stats_sender: StatsSender,
    name: NamespaceName,
    mut current_frame_no: watch::Receiver<Option<FrameNo>>,
    encryption_config: Option<EncryptionConfig>,
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
        encryption_config,
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

async fn load_dump<S, W>(
    db_path: &Path,
    dump: S,
    wal_wrapper: W,
    encryption_config: Option<EncryptionConfig>,
) -> crate::Result<(), LoadDumpError>
where
    S: Stream<Item = std::io::Result<Bytes>> + Unpin,
    W: WrapWal<Sqlite3Wal> + Clone + Send + 'static,
{
    let mut retries = 0;
    // there is a small chance we fail to acquire the lock right away, so we perform a few retries
    let conn = loop {
        let db_path = db_path.to_path_buf();
        let wal_manager = Sqlite3WalManager::default().wrap(wal_wrapper.clone());

        let encryption_config = encryption_config.clone();
        match tokio::task::spawn_blocking(move || {
            open_conn(&db_path, wal_manager, None, encryption_config)
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
            replicator.new_generation().await;
            if let Some(_handle) = replicator.snapshot_main_db_file(true).await? {
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
    encryption_config: Option<EncryptionConfig>,
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

        let encryption_config = encryption_config.clone();
        let _ = tokio::task::spawn_blocking(move || {
            // because closing the last connection interferes with opening a new one, we lazily
            // initialize a connection here, and keep it alive for the entirety of the program. If we
            // fail to open it, we wait for `duration` and try again later.
            match open_conn(&db_path, Sqlite3WalManager::new(), Some(rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY), encryption_config) {
                Ok(mut conn) => {
                    if let Ok(tx) = conn.transaction() {
                        let page_count = tx.query_row("pragma page_count;", [], |row| { row.get::<usize, u64>(0) });
                        let freelist_count = tx.query_row("pragma freelist_count;", [], |row| { row.get::<usize, u64>(0) });
                        if let (Ok(page_count), Ok(freelist_count)) = (page_count, freelist_count) {
                            let storage_bytes_used = (page_count - freelist_count) * 4096;
                            stats.set_storage_bytes_used(storage_bytes_used);
                        }
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
