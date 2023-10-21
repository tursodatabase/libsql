use std::io::{ErrorKind, SeekFrom};
use std::mem::size_of;
use std::path::Path;
use std::str::FromStr;

use bytemuck::{bytes_of, try_pod_read_unaligned, Pod, Zeroable};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::pin;
use uuid::Uuid;

use crate::frame::FrameNo;
use crate::rpc::replication::HelloResponse;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid meta file")]
    InvalidMetaFile,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid log id")]
    InvalidLogId,
    #[error("Replica log incompatible with log from handshake")]
    LogIncompatible,
    #[error("Failed to commit replication index")]
    FailedToCommit(std::io::Error),
}

#[repr(C)]
#[derive(Debug, Pod, Zeroable, Clone, Copy)]
pub struct WalIndexMetaData {
    /// id of the replicated log
    log_id: u128,
    /// committed frame index
    pub committed_frame_no: FrameNo,
    _padding: u64,
}

impl WalIndexMetaData {
    async fn read(file: impl AsyncRead) -> Result<Option<Self>, Error> {
        pin!(file);
        let mut buf = [0; size_of::<WalIndexMetaData>()];
        let meta = match file.read_exact(&mut buf).await {
            Ok(_) => {
                let meta: Self = try_pod_read_unaligned(&buf)
                    .map_err(|_| Error::InvalidMetaFile)?;
                Some(meta)
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => Err(e)?,
        };

        Ok(meta)
    }
}

pub struct WalIndexMeta {
    file: File,
    data: Option<WalIndexMetaData>,
}

impl WalIndexMeta {
    pub async fn open(db_path: &Path) -> Result<Self, Error> {
        let path = db_path.join("client_wal_index");

        tokio::fs::create_dir_all(db_path).await?;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        let data = WalIndexMetaData::read(&mut file).await?;

        Ok(Self { file, data })
    }

    /// attempts to merge two meta files.
    pub fn merge_hello(&mut self, hello: HelloResponse) -> Result<(), Error> {
        let hello_log_id = Uuid::from_str(&hello.log_id)
            .map_err(|_| Error::InvalidLogId)?
            .as_u128();

        match self.data {
            Some(meta) => {
                if meta.log_id != hello_log_id {
                    Err(Error::LogIncompatible)
                } else {
                    Ok(())
                }
            }
            None => {
                self.data = Some(WalIndexMetaData {
                    log_id: hello_log_id,
                    committed_frame_no: FrameNo::MAX,
                    _padding: 0,
                });
                Ok(())
            }
        }
    }

    pub async fn flush(&mut self) -> std::io::Result<()> {
        if let Some(data) = self.data {
            // FIXME: we can save a syscall by calling read_exact_at, but let's use tokio API for now
            self.file.seek(SeekFrom::Start(0)).await?;
            let s = self.file.write(bytes_of(&data)).await?;
            // WalIndexMeta is smaller than a page size, and aligned at the beginning of the file, if
            // should always be written in a single call
            assert_eq!(s, size_of::<WalIndexMetaData>());
            self.file.flush().await?;
        }

        Ok(())
    }

    /// Apply the last commit frame no to the meta file.
    /// This function must be called after each injection, because it's idempotent to re-apply the
    /// last transaction, but not idempotent if we lose track of more than one.
    pub async fn set_commit_frame_no(
        &mut self,
        commit_fno: FrameNo,
    ) -> Result<(), Error> {
        {
            let data = self
                .data
                .as_mut()
                .expect("call set_commit_frame_no before initializing meta");
            data.committed_frame_no = commit_fno;
        }

        if let Err(e) = self.flush().await {
            return Err(Error::FailedToCommit(e));
        }

        Ok(())
    }

    pub fn current_frame_no(&self) -> Option<FrameNo> {
        self.data.and_then(|d| {
            if d.committed_frame_no == FrameNo::MAX {
                None
            } else {
                Some(d.committed_frame_no)
            }
        })
    }
}
