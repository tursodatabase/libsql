use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;
use bottomless::replicator::Replicator;
use chrono::NaiveDateTime;
use futures::TryStreamExt;
use libsql_replication::frame::FrameBorrowed;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::time::Duration;
use tokio_stream::StreamExt;

use crate::namespace::ResolveNamespacePathFn;
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::{LogReadError, ReplicationLogger};
use crate::{BLOCKING_RT, LIBSQL_PAGE_SIZE};

use super::meta_store::MetaStoreHandle;
use super::{Namespace, NamespaceBottomlessDbId, NamespaceConfig, NamespaceName, RestoreOption};

type Result<T> = crate::Result<T, ForkError>;

#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    #[error("internal error: {0}")]
    Internal(anyhow::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to read frame from replication log: {0}")]
    LogRead(anyhow::Error),
    #[error("an error occurred creating the namespace: {0}")]
    CreateNamespace(Box<crate::error::Error>),
    #[error("cannot fork a replica, try again with the primary.")]
    ForkReplica,
    #[error("backup service not configured")]
    BackupServiceNotConfigured,
}

impl From<tokio::task::JoinError> for ForkError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Internal(e.into())
    }
}

async fn write_frame(frame: &FrameBorrowed, temp_file: &mut tokio::fs::File) -> Result<()> {
    let page_no = frame.header().page_no.get();
    let page_pos = (page_no - 1) as usize * LIBSQL_PAGE_SIZE as usize;
    temp_file.seek(SeekFrom::Start(page_pos as u64)).await?;
    temp_file.write_all(frame.page()).await?;

    Ok(())
}

pub struct ForkTask<'a> {
    pub base_path: Arc<Path>,
    pub logger: Arc<ReplicationLogger>,
    pub to_namespace: NamespaceName,
    pub to_config: MetaStoreHandle,
    pub restore_to: Option<PointInTimeRestore>,
    pub bottomless_db_id: NamespaceBottomlessDbId,
    pub ns_config: &'a NamespaceConfig,
    pub resolve_attach: ResolveNamespacePathFn,
}

pub struct PointInTimeRestore {
    pub timestamp: NaiveDateTime,
    pub replicator_options: bottomless::replicator::Options,
}

impl<'a> ForkTask<'a> {
    pub async fn fork(self) -> Result<super::Namespace> {
        let base_path = self.base_path.clone();
        let dest_namespace = self.to_namespace.clone();
        match self.try_fork().await {
            Err(e) => {
                let _ =
                    tokio::fs::remove_dir_all(base_path.join("dbs").join(dest_namespace.as_str()))
                        .await;
                Err(e)
            }
            Ok(ns) => Ok(ns),
        }
    }

    async fn try_fork(self) -> Result<super::Namespace> {
        // until what index to replicate
        let base_path = self.base_path.clone();
        let temp_dir = BLOCKING_RT
            .spawn_blocking(move || tempfile::tempdir_in(base_path))
            .await??;
        let db_path = temp_dir.path().join("data");

        if let Some(restore) = self.restore_to {
            Self::restore_from_backup(restore, db_path)
                .await
                .map_err(ForkError::Internal)?;
        } else {
            Self::restore_from_log_file(&self.logger, db_path).await?;
        }

        let dest_path = self.base_path.join("dbs").join(self.to_namespace.as_str());
        tokio::fs::rename(temp_dir.path(), dest_path).await?;

        Namespace::from_config(
            self.ns_config,
            self.to_config.clone(),
            RestoreOption::Latest,
            &self.to_namespace,
            Box::new(|_op| {}),
            self.resolve_attach.clone(),
        )
        .await
        .map_err(|e| ForkError::CreateNamespace(Box::new(e)))
    }

    /// Restores the database state from a local log file.
    async fn restore_from_log_file(
        logger: &Arc<ReplicationLogger>,
        db_path: PathBuf,
    ) -> Result<()> {
        let mut data_file = File::create(db_path).await?;
        let end_frame_no = *logger.new_frame_notifier.borrow();
        if let Some(end_frame_no) = end_frame_no {
            let mut next_frame_no = 0;
            while next_frame_no < end_frame_no {
                let mut streamer =
                    FrameStream::new(logger.clone(), next_frame_no, false, None, None)
                        .map_err(|e| ForkError::LogRead(e.into()))?
                        .map_ok(|(f, _)| f);
                while let Some(res) = streamer.next().await {
                    match res {
                        Ok(frame) => {
                            next_frame_no = next_frame_no.max(frame.header().frame_no.get() + 1);
                            write_frame(&frame, &mut data_file).await?;
                        }
                        Err(LogReadError::SnapshotRequired) => {
                            let snapshot = loop {
                                if let Some(snap) = logger
                                    .get_snapshot_file(next_frame_no)
                                    .await
                                    .map_err(ForkError::Internal)?
                                {
                                    break snap;
                                }

                                // the snapshot must exist, it is just not yet available.
                                tokio::time::sleep(Duration::from_millis(100)).await;
                            };

                            let frames = snapshot.into_stream_mut_from(next_frame_no);
                            tokio::pin!(frames);
                            while let Some(frame) = frames.next().await {
                                let frame = frame.map_err(|e| ForkError::LogRead(anyhow!(e)))?;
                                next_frame_no =
                                    next_frame_no.max(frame.header().frame_no.get() + 1);
                                write_frame(&frame, &mut data_file).await?;
                            }
                        }
                        Err(LogReadError::Ahead) => {
                            unreachable!("trying to fork ahead of the forked database!")
                        }
                        Err(LogReadError::Error(e)) => return Err(ForkError::LogRead(e)),
                    }
                }
            }
        }
        data_file.shutdown().await?;
        Ok(())
    }

    async fn restore_from_backup(
        restore_to: PointInTimeRestore,
        db_path: PathBuf,
    ) -> anyhow::Result<()> {
        let mut replicator =
            Replicator::with_options(db_path.display().to_string(), restore_to.replicator_options)
                .await?;
        replicator.restore(None, Some(restore_to.timestamp)).await?;
        Ok(())
    }
}
