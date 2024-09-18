use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::prelude::Future;
use libsql_sys::EncryptionConfig;
use tokio::task::JoinSet;

use crate::connection::config::DatabaseConfig;
use crate::connection::connection_manager::InnerWalManager;
use crate::connection::{Connection as _, MakeConnection};
use crate::database::{Database, PrimaryDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::configurator::helpers::make_primary_connection_maker;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceBottomlessDbIdInit, NamespaceName, NamespaceStore, ResetCb,
    ResolveNamespacePathFn, RestoreOption,
};
use crate::run_periodic_checkpoint;
use crate::schema::{has_pending_migration_task, setup_migration_table};

use super::helpers::cleanup_primary;
use super::{BaseNamespaceConfig, ConfigureNamespace, PrimaryConfig};

pub struct PrimaryConfigurator {
    base: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
}

impl PrimaryConfigurator {
    pub fn new(
        base: BaseNamespaceConfig,
        primary_config: PrimaryConfig,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    ) -> Self {
        Self {
            base,
            primary_config,
            make_wal_manager,
        }
    }

    #[tracing::instrument(skip_all, fields(namespace))]
    async fn try_new_primary(
        &self,
        namespace: NamespaceName,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        resolve_attach_path: ResolveNamespacePathFn,
        db_path: Arc<Path>,
        broadcaster: BroadcasterHandle,
        encryption_config: Option<EncryptionConfig>,
    ) -> crate::Result<Namespace> {
        let mut join_set = JoinSet::new();

        tokio::fs::create_dir_all(&db_path).await?;

        let block_writes = Arc::new(AtomicBool::new(false));
        let (connection_maker, wal_wrapper, stats) = make_primary_connection_maker(
            &self.primary_config,
            &self.base,
            &meta_store_handle,
            &db_path,
            &namespace,
            restore_option,
            block_writes.clone(),
            &mut join_set,
            resolve_attach_path,
            broadcaster,
            self.make_wal_manager.clone(),
            encryption_config,
        )
        .await?;

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

        if let Some(checkpoint_interval) = self.primary_config.checkpoint_interval {
            join_set.spawn(run_periodic_checkpoint(
                connection_maker.clone(),
                checkpoint_interval,
                namespace.clone(),
            ));
        }

        tracing::debug!("Done making new primary");

        Ok(Namespace {
            tasks: join_set,
            db: Database::Primary(PrimaryDatabase {
                wal_wrapper,
                connection_maker,
                block_writes,
            }),
            name: namespace,
            stats,
            db_config_store: meta_store_handle,
            path: db_path.into(),
        })
    }
}

impl ConfigureNamespace for PrimaryConfigurator {
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
                    self.base.encryption_config.clone(),
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
        db_config: &'a DatabaseConfig,
        prune_all: bool,
        bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            cleanup_primary(
                &self.base,
                &self.primary_config,
                namespace,
                db_config,
                prune_all,
                bottomless_db_id_init,
            )
            .await
        })
    }

    fn fork<'a>(
        &'a self,
        from_ns: &'a Namespace,
        from_config: MetaStoreHandle,
        to_ns: NamespaceName,
        to_config: MetaStoreHandle,
        timestamp: Option<chrono::prelude::NaiveDateTime>,
        store: NamespaceStore,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(super::fork::fork(
            from_ns,
            from_config,
            to_ns,
            to_config,
            timestamp,
            store,
            &self.primary_config,
            self.base.base_path.clone(),
        ))
    }
}
