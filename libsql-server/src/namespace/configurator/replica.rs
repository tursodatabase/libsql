use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use futures::Future;
use hyper::Uri;
use libsql_replication::rpc::replication::log_offset::WalFlavor;
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_sys::wal::wrapper::PassthroughWalWrapper;
use tokio::task::JoinSet;
use tonic::transport::Channel;

use crate::connection::config::DatabaseConfig;
use crate::connection::connection_manager::InnerWalManager;
use crate::connection::legacy::MakeLegacyConnection;
use crate::connection::write_proxy::MakeWriteProxyConn;
use crate::connection::MakeConnection;
use crate::database::{Database, ReplicaDatabase};
use crate::namespace::broadcasters::BroadcasterHandle;
use crate::namespace::configurator::helpers::{make_stats, run_storage_monitor};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::{Namespace, NamespaceBottomlessDbIdInit, RestoreOption};
use crate::namespace::{NamespaceName, NamespaceStore, ResetCb, ResetOp, ResolveNamespacePathFn};
use crate::{DB_CREATE_TIMEOUT, DEFAULT_AUTO_CHECKPOINT};

use super::{BaseNamespaceConfig, ConfigureNamespace};

pub struct ReplicaConfigurator {
    base: BaseNamespaceConfig,
    channel: Channel,
    uri: Uri,
    make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
}

impl ReplicaConfigurator {
    pub fn new(
        base: BaseNamespaceConfig,
        channel: Channel,
        uri: Uri,
        make_wal_manager: Arc<dyn Fn() -> InnerWalManager + Sync + Send + 'static>,
    ) -> Self {
        Self {
            base,
            channel,
            uri,
            make_wal_manager,
        }
    }
}

impl ConfigureNamespace for ReplicaConfigurator {
    #[tracing::instrument(skip_all, fields(name))]
    fn setup<'a>(
        &'a self,
        meta_store_handle: MetaStoreHandle,
        restore_option: RestoreOption,
        name: &'a NamespaceName,
        reset: ResetCb,
        resolve_attach_path: ResolveNamespacePathFn,
        store: NamespaceStore,
        broadcaster: BroadcasterHandle,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(async move {
            tracing::debug!("creating replica namespace");
            let db_path = self.base.base_path.join("dbs").join(name.as_str());
            let channel = self.channel.clone();
            let uri = self.uri.clone();

            let rpc_client = ReplicationLogClient::with_origin(channel.clone(), uri.clone());
            let client = crate::replication::replicator_client::Client::new(
                name.clone(),
                rpc_client,
                &db_path,
                meta_store_handle.clone(),
                store.clone(),
                WalFlavor::Sqlite,
            )
            .await?;
            let applied_frame_no_receiver = client.current_frame_no_notifier.subscribe();
            let mut replicator = libsql_replication::replicator::Replicator::new_sqlite(
                client,
                db_path.join("data"),
                DEFAULT_AUTO_CHECKPOINT,
                None,
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
                    return self
                        .setup(
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

            let primary_current_replicatio_index =
                replicator.client_mut().primary_replication_index;

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
                self.base.stats_sender.clone(),
                name.clone(),
                applied_frame_no_receiver.clone(),
            )
            .await?;

            let connection_maker = MakeLegacyConnection::new(
                db_path.clone(),
                PassthroughWalWrapper,
                stats.clone(),
                broadcaster,
                meta_store_handle.clone(),
                self.base.extensions.clone(),
                self.base.max_response_size,
                self.base.max_total_response_size,
                DEFAULT_AUTO_CHECKPOINT,
                applied_frame_no_receiver.clone(),
                self.base.encryption_config.clone(),
                Arc::new(AtomicBool::new(false)), // this is always false for write proxy
                resolve_attach_path,
                self.make_wal_manager.clone(),
            )
            .await?;

            let connection_maker = Arc::new(
                MakeWriteProxyConn::new(
                    channel.clone(),
                    uri.clone(),
                    stats.clone(),
                    applied_frame_no_receiver,
                    self.base.max_response_size,
                    self.base.max_total_response_size,
                    primary_current_replicatio_index,
                    self.base.encryption_config.clone(),
                    connection_maker,
                )
                .throttled(
                    self.base.max_concurrent_connections.clone(),
                    Some(DB_CREATE_TIMEOUT),
                    self.base.max_total_response_size,
                    self.base.max_concurrent_requests,
                ),
            );

            join_set.spawn(run_storage_monitor(
                Arc::downgrade(&stats),
                connection_maker.clone(),
            ));

            Ok(Namespace {
                tasks: join_set,
                db: Database::Replica(ReplicaDatabase { connection_maker }),
                name: name.clone(),
                stats,
                db_config_store: meta_store_handle,
                path: db_path.into(),
            })
        })
    }

    fn cleanup<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        _db_config: &DatabaseConfig,
        _prune_all: bool,
        _bottomless_db_id_init: NamespaceBottomlessDbIdInit,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let ns_path = self.base.base_path.join("dbs").join(namespace.as_str());
            if ns_path.try_exists()? {
                tracing::debug!("removing database directory: {}", ns_path.display());
                tokio::fs::remove_dir_all(ns_path).await?;
            }
            Ok(())
        })
    }

    fn fork<'a>(
        &'a self,
        _from_ns: &'a Namespace,
        _from_config: MetaStoreHandle,
        _to_ns: NamespaceName,
        _to_config: MetaStoreHandle,
        _timestamp: Option<chrono::NaiveDateTime>,
        _store: NamespaceStore,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Namespace>> + Send + 'a>> {
        Box::pin(std::future::ready(Err(crate::Error::Fork(
            super::fork::ForkError::ForkReplica,
        ))))
    }
}
