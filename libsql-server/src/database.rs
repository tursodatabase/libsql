use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bottomless::bottomless_wal::BottomlessWalWrapper;
use bottomless::replicator::Replicator;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures_core::stream::BoxStream;
use futures_core::{Future, Stream};
use libsql_replication::frame::Frame;
use libsql_replication::snapshot::SnapshotFile;
use libsql_sys::wal::either::Either;
use libsql_sys::wal::wrapper::{WalWrapper, WrappedWal};
use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::connection::libsql::{InhibitCheckpointWalWrapper, LibSqlConnection};
use crate::connection::write_proxy::{RpcStream, WriteProxyConnection};
use crate::connection::{Connection, MakeConnection, TrackedConnection};
use crate::namespace::NamespaceName;
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::primary::replication_logger_wal::{
    ReplicationLoggerWal, ReplicationLoggerWalManager,
};
use crate::replication::snapshot_store::SnapshotStore;
use crate::replication::wal::compactor::CompactorWrapper;
use crate::replication::wal::frame_notifier::FrameNotifier;
use crate::replication::wal::record_commit::RecordCommitWrapper;
use crate::replication::wal::replication_index_injector::ReplicationIndexInjectorWrapper;
use crate::replication::wal::replicator::ReplicationBehavior;
use crate::replication::{FrameNo, LogReadError, ReplicationLogger};
use crate::stats::Stats;

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationWal>>;

pub type ReplicationWalManager = Either<ReplicationWalManagerV1, ReplicationWalManagerV2>;
type ReplicationWal = Either<ReplicationWalV1, ReplicationWalV2>;

// Those types look a bit ugly. To get the actual type name, build it using the .wrap() method, and
// then use LSP to print the type name.
type ReplicationWalManagerV1 =
    WalWrapper<Option<BottomlessWalWrapper>, ReplicationLoggerWalManager>;
type ReplicationWalManagerV2 = WalWrapper<
    InhibitCheckpointWalWrapper,
    WalWrapper<
        ReplicationIndexInjectorWrapper,
        WalWrapper<
            FrameNotifier,
            WalWrapper<CompactorWrapper, WalWrapper<RecordCommitWrapper, Sqlite3WalManager>>,
        >,
    >,
>;

pub type ReplicationWalV1 = WrappedWal<Option<BottomlessWalWrapper>, ReplicationLoggerWal>;
type ReplicationWalV2 = WrappedWal<
    InhibitCheckpointWalWrapper,
    WrappedWal<
        ReplicationIndexInjectorWrapper,
        WrappedWal<
            FrameNotifier,
            WrappedWal<CompactorWrapper, WrappedWal<RecordCommitWrapper, Sqlite3Wal>>,
        >,
    >,
>;

#[async_trait]
pub trait Database: Sync + Send + 'static {
    /// The connection type of the database
    type Connection: Connection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>>;

    fn destroy(self);

    async fn shutdown(self) -> crate::Result<()>;
}

pub struct ReplicaDatabase {
    pub connection_maker:
        Arc<dyn MakeConnection<Connection = TrackedConnection<WriteProxyConnection<RpcStream>>>>,
}

#[async_trait]
impl Database for ReplicaDatabase {
    type Connection = TrackedConnection<WriteProxyConnection<RpcStream>>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    fn destroy(self) {}

    async fn shutdown(self) -> crate::Result<()> {
        Ok(())
    }
}

pub enum PrimaryDatabase {
    V1 {
        logger: Arc<ReplicationLogger>,
        connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
        bottomless_replicator: Option<Arc<std::sync::Mutex<Option<Replicator>>>>,
        stats: Arc<Stats>,
    },
    V2 {
        notifier: FrameNotifier,
        connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
        db_id: Uuid,
        db_path: Arc<Path>,
        snapshot_store: SnapshotStore,
        commit_indexes: Arc<RwLock<HashMap<u32, u32>>>,
        encryption_key: Option<Bytes>,
    },
}

impl PrimaryDatabase {
    pub fn stream_replication_log(
        &self,
        namespace: &NamespaceName,
        next_frame_no: FrameNo,
        wait_for_more: bool,
    ) -> crate::Result<
        BoxStream<'static, crate::Result<(Frame, Option<DateTime<Utc>>), LogReadError>>,
    > {
        match &self {
            PrimaryDatabase::V1 { logger, stats, .. } => Ok(Box::pin(FrameStream::new(
                logger.clone(),
                next_frame_no,
                wait_for_more,
                None,
                Some(stats.clone())
            )?)),
            PrimaryDatabase::V2 {
                db_path,
                snapshot_store,
                notifier,
                commit_indexes,
                encryption_key,
                ..
            } => {
                let replication_behavior = if wait_for_more {
                    ReplicationBehavior::WaitForFrame {
                        notifier: notifier.clone(),
                    }
                } else {
                    ReplicationBehavior::Exit
                };
                let mut replicator = crate::replication::wal::replicator::Replicator::new(
                    db_path.as_ref(),
                    next_frame_no,
                    namespace.clone(),
                    snapshot_store.clone(),
                    replication_behavior,
                    commit_indexes.clone(),
                    encryption_key.clone(),
                )
                .unwrap();

                Ok(Box::pin(async_stream::stream! {
                    let stream = replicator.stream_frames();
                    tokio::pin!(stream);
                    while let Some(frame) = stream.next().await {
                        yield frame.map(|f| (f, None)).map_err(|e| LogReadError::Error(e.into()));
                    }
                }))
            }
        }
    }

    pub fn stream_snapshot(
        &self,
        next_frame_no: FrameNo,
    ) -> impl Future<
        Output = crate::Result<
            Option<impl Stream<Item = crate::Result<Frame, libsql_replication::snapshot::Error>>>,
        >,
    > + 'static {
        match &self {
            PrimaryDatabase::V1 { logger, stats, .. } => {
                let logger = logger.clone();
                let stats = stats.clone();
                async move {
                    let stream = logger
                        .get_snapshot_file(next_frame_no)
                        .await?
                        .map(move |s| make_snapshot_stream(s, next_frame_no, Some(stats)));

                    Ok(stream)
                }
            }
            PrimaryDatabase::V2 { .. } => unreachable!("V2 should handle snapshots"),
        }
    }

    pub fn current_replication_index(&self) -> FrameNo {
        match self {
            PrimaryDatabase::V1 { logger, .. } => *logger.new_frame_notifier.borrow(),
            PrimaryDatabase::V2 { notifier, .. } => notifier.current(),
        }
    }

    pub fn log_id(&self) -> Uuid {
        match self {
            PrimaryDatabase::V1 { logger, .. } => logger.log_id(),
            PrimaryDatabase::V2 { db_id, .. } => *db_id,
        }
    }
}

pub fn make_snapshot_stream(
    snapshot: SnapshotFile,
    offset: FrameNo,
    stats: Option<Arc<Stats>>,
) -> impl Stream<Item = crate::Result<Frame, libsql_replication::snapshot::Error>> {
    let size_after = snapshot.header().size_after;
    let frames = snapshot.into_stream_mut_from(offset).peekable();
    async_stream::stream! {
        tokio::pin!(frames);
        while let Some(frame) = frames.next().await {
            match frame {
                Ok(mut frame) => {
                    // this is the last frame we're sending for this snapshot, set the
                    // frame_no
                    if frames.as_mut().peek().await.is_none() {
                        frame.header_mut().size_after = size_after;
                    }

                    if let Some(stats) = &stats {
                        stats.inc_embedded_replica_frames_replicated();
                    }

                    yield Ok(Frame::from(frame));
                }
                Err(e) => {
                    yield Err(e);
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl Database for PrimaryDatabase {
    type Connection = PrimaryConnection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        match self {
            PrimaryDatabase::V1 {
                connection_maker, ..
            } => connection_maker.clone(),
            PrimaryDatabase::V2 {
                connection_maker, ..
            } => connection_maker.clone(),
        }
    }

    fn destroy(self) {
        match self {
            PrimaryDatabase::V1 { logger, .. } => {
                logger.closed_signal.send_replace(true);
            }
            PrimaryDatabase::V2 { .. } => (),
        }
    }

    async fn shutdown(self) -> crate::Result<()> {
        match self {
            PrimaryDatabase::V1 {
                logger,
                bottomless_replicator,
                ..
            } => {
                logger.closed_signal.send_replace(true);
                if let Some(bottomless) = bottomless_replicator {
                    if let Some(mut replicator) =
                        tokio::task::spawn_blocking(move || bottomless.lock().unwrap().take())
                            .await
                            .unwrap()
                    {
                        replicator.shutdown_gracefully().await?;
                    }
                }
            }
            PrimaryDatabase::V2 { .. } => (),
        }

        Ok(())
    }
}
