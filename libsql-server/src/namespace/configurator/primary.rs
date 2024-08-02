use std::sync::atomic::{AtomicBool, Ordering};
use std::{path::Path, pin::Pin, sync::Arc};

use futures::prelude::Future;
use tokio::task::JoinSet;

use crate::connection::MakeConnection;
use crate::database::{Database, PrimaryDatabase};
use crate::namespace::{Namespace, NamespaceConfig, NamespaceName, NamespaceStore, ResetCb, ResolveNamespacePathFn, RestoreOption};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::run_periodic_checkpoint;
use crate::schema::{has_pending_migration_task, setup_migration_table};

use super::ConfigureNamespace;

pub struct PrimaryConfigurator;

impl ConfigureNamespace for PrimaryConfigurator {
    fn setup<'a>(
        &'a self,
        config: &'a NamespaceConfig,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        _reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        _store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>>
    {
        Box::pin(async move {
            let db_path: Arc<Path> = config.base_path.join("dbs").join(name.as_str()).into();
            let fresh_namespace = !db_path.try_exists()?;
            // FIXME: make that truly atomic. explore the idea of using temp directories, and it's implications
            match try_new_primary(
                config,
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
                        tracing::error!("an error occured while deleting creating namespace, cleaning...");
                        if let Err(e) = tokio::fs::remove_dir_all(&db_path).await {
                            tracing::error!("failed to remove dirty namespace directory: {e}")
                        }
                        Err(e)
                    }
                    Err(e) => Err(e),
                }
        })
    }
}

#[tracing::instrument(skip_all, fields(namespace))]
async fn try_new_primary(
    ns_config: &NamespaceConfig,
    namespace: NamespaceName,
    meta_store_handle: MetaStoreHandle,
    restore_option: RestoreOption,
    resolve_attach_path: ResolveNamespacePathFn,
    db_path: Arc<Path>,
    broadcaster: BroadcasterHandle,
) -> crate::Result<Namespace> {
    let mut join_set = JoinSet::new();

    tokio::fs::create_dir_all(&db_path).await?;

    let block_writes = Arc::new(AtomicBool::new(false));
    let (connection_maker, wal_wrapper, stats) = Namespace::make_primary_connection_maker(
        ns_config,
        &meta_store_handle,
        &db_path,
        &namespace,
        restore_option,
        block_writes.clone(),
        &mut join_set,
        resolve_attach_path,
        broadcaster,
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
