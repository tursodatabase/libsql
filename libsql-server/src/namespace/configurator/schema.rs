use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::Future;
use tokio::task::JoinSet;

use crate::connection::config::DatabaseConfig;
use crate::connection::connection_manager::InnerWalManager;
use crate::database::{Database, SchemaDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceName, NamespaceStore, ResetCb, ResolveNamespacePathFn, RestoreOption,
};
use crate::schema::SchedulerHandle;

use super::helpers::{cleanup_primary, make_primary_connection_maker};
use super::{BaseNamespaceConfig, ConfigureNamespace, PrimaryConfig};

pub struct SchemaConfigurator {
    base: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    migration_scheduler: SchedulerHandle,
}

impl SchemaConfigurator {
    pub fn new(
        base: BaseNamespaceConfig,
        primary_config: PrimaryConfig,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
        migration_scheduler: SchedulerHandle,
    ) -> Self {
        Self {
            base,
            primary_config,
            make_wal_manager,
            migration_scheduler,
        }
    }
}

impl ConfigureNamespace for SchemaConfigurator {
    fn setup<'a>(
        &'a self,
        db_config: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        _reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        _store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> std::pin::Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(async move {
            let mut join_set = JoinSet::new();
            let db_path = self.base.base_path.join("dbs").join(name.as_str());

            tokio::fs::create_dir_all(&db_path).await?;

            let (connection_maker, wal_manager, stats) = make_primary_connection_maker(
                &self.primary_config,
                &self.base,
                &db_config,
                &db_path,
                &name,
                restore_option,
                Arc::new(AtomicBool::new(false)), // this is always false for schema
                &mut join_set,
                resolve_attach_path,
                broadcaster,
                self.make_wal_manager.clone(),
                self.base.encryption_config.clone(),
            )
            .await?;

            Ok(Namespace {
                db: Database::Schema(SchemaDatabase::new(
                    self.migration_scheduler.clone(),
                    name.clone(),
                    connection_maker,
                    Some(wal_manager.clone()),
                    db_config.clone(),
                    wal_manager
                        .wrapper()
                        .logger()
                        .new_frame_notifier
                        .subscribe(),
                )),
                name: name.clone(),
                tasks: join_set,
                stats,
                db_config_store: db_config.clone(),
                path: db_path.into(),
            })
        })
    }

    fn cleanup<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        db_config: &'a DatabaseConfig,
        prune_all: bool,
        bottomless_db_id_init: crate::namespace::NamespaceBottomlessDbIdInit,
    ) -> std::pin::Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
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
    ) -> std::pin::Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
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
