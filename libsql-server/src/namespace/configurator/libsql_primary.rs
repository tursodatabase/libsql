use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::prelude::Future;
use libsql_sys::name::NamespaceResolver;
use libsql_sys::wal::either::Either;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::storage::backend::Backend;
use libsql_wal::wal::LibsqlWalManager;
use tokio::task::JoinSet;

use crate::connection::config::DatabaseConfig;
use crate::connection::libsql::{MakeLibsqlConnection, MakeLibsqlConnectionInner};
use crate::connection::{Connection as _, MakeConnection};
use crate::database::{Database, LibsqlPrimaryConnectionMaker, LibsqlPrimaryDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::configurator::helpers::{make_stats, run_storage_monitor};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceBottomlessDbIdInit, NamespaceName, NamespaceStore, ResetCb,
    ResolveNamespacePathFn, RestoreOption,
};
use crate::schema::{has_pending_migration_task, setup_migration_table};
use crate::stats::Stats;
use crate::{SqldStorage, DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT};

use super::helpers::cleanup_libsql;
use super::{BaseNamespaceConfig, ConfigureNamespace, PrimaryConfig};

pub struct LibsqlPrimaryConfigurator {
    base: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
    registry: Arc<WalRegistry<StdIO, SqldStorage>>,
    namespace_resolver: Arc<dyn NamespaceResolver>,
}

pub struct LibsqlPrimaryCommon {
    pub stats: Arc<Stats>,
    pub connection_maker: Arc<LibsqlPrimaryConnectionMaker>,
    pub join_set: JoinSet<anyhow::Result<()>>,
    pub block_writes: Arc<AtomicBool>,
    pub notifier: tokio::sync::watch::Receiver<u64>,
}

pub(super) async fn libsql_primary_common(
    db_path: Arc<Path>,
    db_config: MetaStoreHandle,
    base_config: &BaseNamespaceConfig,
    primary_config: &PrimaryConfig,
    namespace: NamespaceName,
    broadcaster: BroadcasterHandle,
    resolve_attach_path: ResolveNamespacePathFn,
    registry: Arc<WalRegistry<StdIO, SqldStorage>>,
    namespace_resolver: Arc<dyn NamespaceResolver>,
) -> crate::Result<LibsqlPrimaryCommon> {
    let mut join_set = JoinSet::new();

    tokio::fs::create_dir_all(&db_path).await?;

    tracing::debug!("Done making new primary");
    let (_snd, rcv) = tokio::sync::watch::channel(None);
    let stats = make_stats(
        &db_path,
        &mut join_set,
        db_config.clone(),
        base_config.stats_sender.clone(),
        namespace.clone(),
        rcv.clone(),
    )
    .await?;

    let auto_checkpoint = if primary_config.checkpoint_interval.is_some() {
        0
    } else {
        DEFAULT_AUTO_CHECKPOINT
    };
    let block_writes = Arc::new(AtomicBool::new(false));

    let connection_maker = MakeLibsqlConnection {
        inner: Arc::new(MakeLibsqlConnectionInner {
            db_path: db_path.into(),
            stats: stats.clone(),
            broadcaster,
            config_store: db_config.clone(),
            extensions: base_config.extensions.clone(),
            max_response_size: base_config.max_response_size,
            max_total_response_size: base_config.max_total_response_size,
            auto_checkpoint,
            current_frame_no_receiver: rcv.clone(),
            encryption_config: base_config.encryption_config.clone(),
            block_writes: block_writes.clone(),
            resolve_attach_path,
            wal_manager: LibsqlWalManager::new(registry.clone(), namespace_resolver.clone()),
        }),
    }
    .throttled(
        base_config.max_concurrent_connections.clone(),
        Some(DB_CREATE_TIMEOUT),
        base_config.max_total_response_size,
        base_config.max_concurrent_requests,
    );
    let connection_maker = Arc::new(connection_maker);

    // FIXME: dummy connection to load the wal
    let _ = connection_maker.create().await?;
    let shared = registry.get_async(&namespace.into()).await.unwrap();
    let new_frame_notifier = shared.new_frame_notifier();

    join_set.spawn(run_storage_monitor(
        Arc::downgrade(&stats),
        connection_maker.clone(),
    ));

    if db_config.get().shared_schema_name.is_some() {
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

    Ok(LibsqlPrimaryCommon {
        stats,
        connection_maker,
        join_set,
        block_writes,
        notifier: new_frame_notifier,
    })
}

impl LibsqlPrimaryConfigurator {
    pub fn new(
        base: BaseNamespaceConfig,
        primary_config: PrimaryConfig,
        registry: Arc<WalRegistry<StdIO, SqldStorage>>,
        namespace_resolver: Arc<dyn NamespaceResolver>,
    ) -> Self {
        Self {
            base,
            primary_config,
            registry,
            namespace_resolver,
        }
    }

    #[tracing::instrument(skip_all, fields(namespace))]
    async fn try_new_primary(
        &self,
        namespace: NamespaceName,
        db_config: MetaStoreHandle,
        _restore_option: RestoreOption,
        resolve_attach_path: ResolveNamespacePathFn,
        db_path: Arc<Path>,
        broadcaster: BroadcasterHandle,
    ) -> crate::Result<Namespace> {
        let LibsqlPrimaryCommon {
            stats,
            connection_maker,
            mut join_set,
            block_writes,
            mut notifier,
        } = libsql_primary_common(
            db_path.clone(),
            db_config.clone(),
            &self.base,
            &self.primary_config,
            namespace.clone(),
            broadcaster,
            resolve_attach_path,
            self.registry.clone(),
            self.namespace_resolver.clone(),
        )
        .await?;

        let (notifier_sender, new_frame_notifier) = tokio::sync::watch::channel(None);
        join_set.spawn(async move {
            while let Ok(()) = notifier.changed().await {
                let new = *notifier.borrow_and_update();
                notifier_sender.send_replace(Some(new));
            }

            Ok(())
        });

        Ok(Namespace {
            tasks: join_set,
            db: Database::LibsqlPrimary(LibsqlPrimaryDatabase {
                connection_maker,
                block_writes,
                new_frame_notifier,
            }),
            name: namespace,
            stats,
            db_config_store: db_config,
            path: db_path.into(),
        })
    }
}

impl ConfigureNamespace for LibsqlPrimaryConfigurator {
    fn setup<'a>(
        &'a self,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        _reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        _store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(async move {
            let db_path: Arc<Path> = self.base.base_path.join("dbs").join(name.as_str()).into();
            let fresh_namespace = !db_path.try_exists()?;
            // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
            match self
                .try_new_primary(
                    name.clone(),
                    meta_store_handle,
                    restore_option,
                    resolve_attach_path,
                    db_path.clone(),
                    broadcaster,
                )
                .await
            {
                Ok(this) => Ok(this),
                Err(e) if fresh_namespace => {
                    tracing::error!(
                        "an error occured while deleting creating namespace, cleaning..."
                    );
                    if let Err(e) = tokio::fs::remove_dir_all(&db_path).await {
                        tracing::error!("failed to remove dirty namespace directory: {e}")
                    }
                    Err(e)
                }
                Err(e) => Err(e),
            }
        })
    }

    fn cleanup<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        _db_config: &'a DatabaseConfig,
        _prune_all: bool,
        _bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
        Box::pin(cleanup_libsql(
            namespace,
            &self.registry,
            &self.base.base_path,
        ))
    }

    fn fork<'a>(
        &'a self,
        from_ns: &'a Namespace,
        _from_config: MetaStoreHandle,
        _to_ns: NamespaceName,
        _to_config: MetaStoreHandle,
        timestamp: Option<chrono::prelude::NaiveDateTime>,
        _store: NamespaceStore,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(async move {
            match self.registry.storage() {
                Either::A(s) => {
                    match timestamp {
                        Some(ts) => {
                            let ns: libsql_sys::name::NamespaceName = from_ns.name().clone().into();
                            let _key = s
                                .backend()
                                .find_segment(
                                    &s.backend().default_config(),
                                    &ns,
                                    libsql_wal::storage::backend::FindSegmentReq::Timestamp(
                                        ts.and_utc(),
                                    ),
                                )
                                .await
                                .unwrap();
                            todo!()
                        }
                        // find the most recent frame_no
                        None => todo!("fork from most recent"),
                    };
                }
                Either::B(_) => {
                    todo!("cannot fork without storage");
                }
            }
        })
    }
}
