use std::pin::Pin;
use std::sync::Arc;

use futures::Future;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use tokio::task::JoinSet;

use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::MakeConnection;
use crate::database::{Database, ReplicaDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{Namespace, RestoreOption};
use crate::namespace::{
    make_stats, NamespaceConfig, NamespaceName, NamespaceStore, ResetCb, ResetOp,
    ResolveNamespacePathFn,
};
use crate::{DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT};

use super::ConfigureNamespace;

pub struct ReplicaConfigurator;

impl ConfigureNamespace for ReplicaConfigurator {
    fn setup<'a>(
        &'a self,
        config: &'a NamespaceConfig,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>>
    {
        Box::pin(async move {
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
                store.clone(),
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
                    return self.setup(
                        config,
                        meta_store_handle,
                        restore_option,
                        name,
                        reset,
                        resolve_attach_path,
                        store,
                        broadcaster,
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
                meta_store_handle.clone(),
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
                broadcaster,
                meta_store_handle.clone(),
                applied_frame_no_receiver,
                config.max_response_size,
                config.max_total_response_size,
                primary_current_replicatio_index,
                config.encryption_config.clone(),
                resolve_attach_path,
                config.make_wal_manager.clone(),
            )
                .await?
                .throttled(
                    config.max_concurrent_connections.clone(),
                    Some(DB_CREATE_TIMEOUT),
                    config.max_total_response_size,
                    config.max_concurrent_requests,
                );

            Ok(Namespace {
                tasks: join_set,
                db: Database::Replica(ReplicaDatabase {
                    connection_maker: Arc::new(connection_maker),
                }),
                name: name.clone(),
                stats,
                db_config_store: meta_store_handle,
                path: db_path.into(),
            })
        })
    }
}
