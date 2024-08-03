use std::sync::{atomic::AtomicBool, Arc};

use futures::prelude::Future;
use tokio::task::JoinSet;

use crate::database::{Database, SchemaDatabase};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{
    Namespace, NamespaceConfig, NamespaceName, NamespaceStore,
    ResetCb, ResolveNamespacePathFn, RestoreOption,
};
use crate::namespace::broadcasters::BroadcasterHandle;

use super::ConfigureNamespace;

pub struct SchemaConfigurator;

impl ConfigureNamespace for SchemaConfigurator {
    fn setup<'a>(
        &'a self,
        ns_config: &'a NamespaceConfig,
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
            let db_path = ns_config.base_path.join("dbs").join(name.as_str());

            tokio::fs::create_dir_all(&db_path).await?;

            let (connection_maker, wal_manager, stats) = Namespace::make_primary_connection_maker(
                ns_config,
                &db_config,
                &db_path,
                &name,
                restore_option,
                Arc::new(AtomicBool::new(false)), // this is always false for schema
                &mut join_set,
                resolve_attach_path,
                broadcaster,
            )
            .await?;

            Ok(Namespace {
                db: Database::Schema(SchemaDatabase::new(
                    ns_config.migration_scheduler.clone(),
                    name.clone(),
                    connection_maker,
                    wal_manager,
                    db_config.clone(),
                )),
                name: name.clone(),
                tasks: join_set,
                stats,
                db_config_store: db_config.clone(),
                path: db_path.into(),
            })
        })
    }
}
