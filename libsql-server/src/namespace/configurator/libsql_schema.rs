use std::path::Path;
use std::sync::Arc;

use futures::prelude::Future;
use libsql_sys::name::NamespaceResolver;
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;

use crate::connection::config::DatabaseConfig;
use crate::database::{Database, SchemaDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceName, NamespaceStore, ResetCb, ResolveNamespacePathFn, RestoreOption,
};
use crate::schema::SchedulerHandle;
use crate::SqldStorage;

use super::helpers::cleanup_libsql;
use super::libsql_primary::{libsql_primary_common, LibsqlPrimaryCommon};
use super::{BaseNamespaceConfig, ConfigureNamespace, PrimaryConfig};

pub struct LibsqlSchemaConfigurator {
    base: BaseNamespaceConfig,
    primary_config: PrimaryConfig,
    migration_scheduler: SchedulerHandle,
    registry: Arc<WalRegistry<StdIO, SqldStorage>>,
    namespace_resolver: Arc<dyn NamespaceResolver>,
}

impl LibsqlSchemaConfigurator {
    pub fn new(
        base: BaseNamespaceConfig,
        primary_config: PrimaryConfig,
        migration_scheduler: SchedulerHandle,
        registry: Arc<WalRegistry<StdIO, SqldStorage>>,
        namespace_resolver: Arc<dyn NamespaceResolver>,
    ) -> Self {
        Self {
            base,
            primary_config,
            migration_scheduler,
            registry,
            namespace_resolver,
        }
    }

    #[tracing::instrument(skip_all, fields(namespace))]
    async fn try_new_schema(
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
            mut notifier,
            ..
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
            db: Database::LibsqlSchema(SchemaDatabase::new(
                self.migration_scheduler.clone(),
                namespace.clone(),
                connection_maker,
                None,
                db_config.clone(),
                new_frame_notifier,
            )),
            name: namespace,
            stats,
            db_config_store: db_config,
            path: db_path.into(),
        })
    }
}

impl ConfigureNamespace for LibsqlSchemaConfigurator {
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
            let db_path: Arc<Path> = self.base.base_path.join("dbs").join(name.as_str()).into();
            let fresh_namespace = !db_path.try_exists()?;
            // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
            match self
                .try_new_schema(
                    name.clone(),
                    db_config,
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
        _bottomless_db_id_init: crate::namespace::NamespaceBottomlessDbIdInit,
    ) -> std::pin::Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
        Box::pin(cleanup_libsql(
            namespace,
            &self.registry,
            &self.base.base_path,
        ))
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
