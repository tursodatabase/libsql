use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio_stream::StreamExt;

use crate::database::PrimaryDatabase;
use crate::replication::frame::Frame;
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::{LogReadError, ReplicationLogger};

use super::{MakeNamespace, RestoreOption};

// FIXME: get this const from somewhere else (crate wide)
const PAGE_SIZE: usize = 4096;

type Result<T> = crate::Result<T, ForkError>;

#[derive(Debug, thiserror::Error)]
pub enum ForkError {
    #[error("internal error: {0}")]
    Internal(anyhow::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to read frame from replication log: {0}")]
    LogRead(anyhow::Error),
    #[error("an error occured creating the namespace: {0}")]
    CreateNamespace(Box<crate::error::Error>),
    #[error("cannot fork a replica, try again with the primary.")]
    ForkReplica,
}

impl From<tokio::task::JoinError> for ForkError {
    fn from(e: tokio::task::JoinError) -> Self {
        Self::Internal(e.into())
    }
}

async fn write_frame(frame: Frame, temp_file: &mut tokio::fs::File) -> Result<()> {
    let page_no = frame.header().page_no;
    let page_pos = (page_no - 1) as usize * PAGE_SIZE;
    temp_file.seek(SeekFrom::Start(page_pos as u64)).await?;
    temp_file.write_all(frame.page()).await?;

    Ok(())
}

pub struct ForkTask<'a> {
    pub base_path: PathBuf,
    pub logger: Arc<ReplicationLogger>,
    pub dest_namespace: Bytes,
    pub make_namespace: &'a dyn MakeNamespace<Database = PrimaryDatabase>,
}

impl ForkTask<'_> {
    pub async fn fork(self) -> Result<super::Namespace<PrimaryDatabase>> {
        match self.try_fork().await {
            Err(e) => {
                let _ = tokio::fs::remove_dir_all(
                    self.base_path
                        .join("dbs")
                        .join(std::str::from_utf8(&self.dest_namespace).unwrap()),
                )
                .await;
                Err(e)
            }
            Ok(ns) => Ok(ns),
        }
    }

    async fn try_fork(&self) -> Result<super::Namespace<PrimaryDatabase>> {
        // until what index to replicate
        let base_path = self.base_path.clone();
        let temp_dir =
            tokio::task::spawn_blocking(move || tempfile::tempdir_in(base_path)).await??;
        let mut data_file = tokio::fs::File::create(temp_dir.path().join("data")).await?;

        let logger = self.logger.clone();
        let end_frame_no = *logger.new_frame_notifier.borrow();
        let mut next_frame_no = 0;
        while next_frame_no < end_frame_no {
            let mut streamer = FrameStream::new(logger.clone(), next_frame_no, false, None)
                .map_err(|e| ForkError::LogRead(e.into()))?;
            while let Some(res) = streamer.next().await {
                match res {
                    Ok(frame) => {
                        next_frame_no = next_frame_no.max(frame.header().frame_no + 1);
                        write_frame(frame, &mut data_file).await?;
                    }
                    Err(LogReadError::SnapshotRequired) => {
                        let snapshot = loop {
                            if let Some(snap) = logger
                                .get_snapshot_file(next_frame_no)
                                .map_err(ForkError::Internal)?
                            {
                                break snap;
                            }

                            // the snapshot must exist, it is just not yet available.
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        };

                        let iter = snapshot.frames_iter_from(next_frame_no);
                        for frame in iter {
                            let frame = frame.map_err(ForkError::LogRead)?;
                            next_frame_no = next_frame_no.max(frame.header().frame_no + 1);
                            write_frame(frame, &mut data_file).await?;
                        }
                    }
                    Err(LogReadError::Ahead) => {
                        unreachable!("trying to fork ahead of the forked database!")
                    }
                    Err(LogReadError::Error(e)) => return Err(ForkError::LogRead(e)),
                }
            }
        }

        let dest_path = self
            .base_path
            .join("dbs")
            .join(std::str::from_utf8(&self.dest_namespace).unwrap());
        tokio::fs::rename(temp_dir.path(), dest_path).await?;

        tokio::io::stdin().read_i8().await.unwrap();
        self.make_namespace
            .create(self.dest_namespace.clone(), RestoreOption::Latest, true)
            .await
            .map_err(|e| ForkError::CreateNamespace(Box::new(e)))
    }
}
