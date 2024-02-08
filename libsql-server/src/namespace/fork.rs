use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::anyhow;
use bottomless::replicator::Replicator;
use chrono::NaiveDateTime;
use futures::TryStreamExt;
use libsql_replication::frame::FrameBorrowed;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use zerocopy::FromZeroes;

use crate::database::PrimaryDatabase;
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::snapshot_store::SnapshotStore;
use crate::replication::{FrameNo, LogReadError, ReplicationLogger};
use crate::{BLOCKING_RT, LIBSQL_PAGE_SIZE};

use super::meta_store::MetaStore;
use super::{MakeNamespace, NamespaceBottomlessDbId, NamespaceName, RestoreOption};

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

pub enum ForkTask<'a> {
    V1(ForkTaskV1<'a>),
    V2(ForkTaskV2<'a>),
}

impl ForkTask<'_> {
    pub async fn fork(
        self,
        base_path: Arc<Path>,
        dest: NamespaceName,
    ) -> Result<super::Namespace<PrimaryDatabase>> {
        let ret = match self {
            ForkTask::V1(v1) => v1.try_fork(base_path.clone(), dest.clone()).await,
            ForkTask::V2(v2) => v2.try_fork(base_path.clone(), dest.clone()).await,
        };

        match ret {
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(base_path.join("dbs").join(dest.as_str())).await;
                Err(e)
            }
            Ok(ns) => Ok(ns),
        }
    }
}

pub struct ForkTaskV2<'a> {
    pub meta_store: &'a MetaStore,
    pub replicator: crate::replication::wal::replicator::Replicator,
    pub current_frame: FrameNo,
    pub make_namespace: &'a dyn MakeNamespace<Database = PrimaryDatabase>,
    pub snapshot_store: SnapshotStore,
}

impl ForkTaskV2<'_> {
    async fn try_fork(
        mut self,
        base_path: Arc<Path>,
        dest: NamespaceName,
    ) -> Result<super::Namespace<PrimaryDatabase>> {
        dbg!();
        // until what index to replicate
        let temp_dir = BLOCKING_RT
            .spawn_blocking({
                let base_path = base_path.clone();
                move || tempfile::tempdir_in(base_path)
            })
            .await??;
        let db_path = temp_dir.path().join("data");

        dbg!();
        self.restore_from_replicator(&db_path, &dest).await?;

        dbg!();
        let dest_path = base_path.join("dbs").join(dest.as_str());

        dbg!();
        tokio::fs::rename(temp_dir.path(), dest_path).await?;

        dbg!();
        self.make_namespace
            .create(
                dest.clone(),
                RestoreOption::Latest,
                NamespaceBottomlessDbId::NotProvided,
                // Forking works only on primary and
                // PrimaryNamespaceMaker::create ignores
                // reset_cb param
                Box::new(|_op| {}),
                self.meta_store,
            )
            .await
            .map_err(|e| ForkError::CreateNamespace(Box::new(e)))
    }

    /// Restores the database state from a local log file.
    async fn restore_from_replicator(
        &mut self,
        db_path: &Path,
        dest: &NamespaceName,
    ) -> Result<()> {
        dbg!();
        let mut data_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(db_path)
            .await?;
        // TODO: the restore process could be optimized by always streamign frames backward. We
        // would use size_after to know how many frames we need to see, and stop once we have see
        // all.
        dbg!();
        let stream = self.replicator.stream_frames();
        dbg!();
        tokio::pin!(stream);
        dbg!();
        while let Some(frame) = stream.next().await.transpose().unwrap() {
            if frame.header().frame_no.get() > self.current_frame {
                break;
            }
            write_frame(&frame, &mut data_file).await?;
        }

        dbg!();
        let db_size = dbg!(data_file.metadata().await?.len()) / LIBSQL_PAGE_SIZE;

        dbg!(db_size);
        // build snapshot
        let mut builder = self
            .snapshot_store
            .builder(dest.clone(), db_size as _)
            .unwrap();
        dbg!();
        data_file.seek(SeekFrom::Start(0)).await?;
        let mut frame: Box<FrameBorrowed> = FrameBorrowed::new_box_zeroed();
        for page in 1..=db_size {
            data_file.read_exact(frame.page_mut()).await?;
            frame.header_mut().page_no = (page as u32).into();
            frame.header_mut().frame_no = (db_size - page + 1).into();
            let (frame_back, builder_back) = tokio::task::spawn_blocking(
                move || -> crate::Result<_, crate::replication::snapshot_store::Error> {
                    builder.add_frame(&frame)?;
                    Ok((frame, builder))
                },
            )
            .await
            .unwrap()
            .unwrap();
            builder = builder_back;
            frame = frame_back;
        }
        data_file.shutdown().await?;
        Ok(())
    }
}

pub struct ForkTaskV1<'a> {
    pub logger: Arc<ReplicationLogger>,
    pub make_namespace: &'a dyn MakeNamespace<Database = PrimaryDatabase>,
    pub restore_to: Option<PointInTimeRestore>,
    pub bottomless_db_id: NamespaceBottomlessDbId,
    pub meta_store: &'a MetaStore,
}

pub struct PointInTimeRestore {
    pub timestamp: NaiveDateTime,
    pub replicator_options: bottomless::replicator::Options,
}

impl ForkTaskV1<'_> {
    async fn try_fork(
        self,
        base_path: Arc<Path>,
        dest: NamespaceName,
    ) -> Result<super::Namespace<PrimaryDatabase>> {
        // until what index to replicate
        let temp_dir = BLOCKING_RT
            .spawn_blocking({
                let base_path = base_path.clone();
                move || tempfile::tempdir_in(base_path)
            })
            .await??;
        let db_path = temp_dir.path().join("data");

        if let Some(restore) = self.restore_to {
            Self::restore_from_backup(restore, db_path)
                .await
                .map_err(ForkError::Internal)?;
        } else {
            Self::restore_from_log_file(&self.logger, db_path).await?;
        }

        let dest_path = base_path.join("dbs").join(dest.as_str());
        tokio::fs::rename(temp_dir.path(), dest_path).await?;

        self.make_namespace
            .create(
                dest.clone(),
                RestoreOption::Latest,
                self.bottomless_db_id,
                // Forking works only on primary and
                // PrimaryNamespaceMaker::create ignores
                // reset_cb param
                Box::new(|_op| {}),
                self.meta_store,
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
        let mut next_frame_no = 1;
        while next_frame_no < end_frame_no {
            let mut streamer = FrameStream::new(logger.clone(), next_frame_no, false, None, None)
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
                            next_frame_no = next_frame_no.max(frame.header().frame_no.get() + 1);
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
