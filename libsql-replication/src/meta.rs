use std::io::{ErrorKind, SeekFrom};
use std::mem::size_of;
use std::path::Path;
use std::str::FromStr;

use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::pin;
use uuid::Uuid;
use zerocopy::byteorder::little_endian::{U128 as lu128, U64 as lu64};
use zerocopy::{AsBytes, FromBytes};

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
    #[error("Invalid replication path")]
    InvalidReplicationPath,
    #[error(
        "Can not sync a database without a wal_index, please delete the database and attempt again"
    )]
    RequiresCleanDatabase,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes)]
pub struct WalIndexMetaData {
    /// id of the replicated log
    log_id: lu128,
    /// committed frame index
    pub committed_frame_no: lu64,
    _padding: [u8; 8],
}

impl WalIndexMetaData {
    async fn read(file: impl AsyncRead) -> Result<Option<Self>, Error> {
        pin!(file);
        let mut buf = [0; size_of::<WalIndexMetaData>()];
        let meta = match file.read_exact(&mut buf).await {
            Ok(_) => {
                let meta: Self = Self::read_from(&buf).ok_or(Error::InvalidMetaFile)?;
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
    pub data: Option<WalIndexMetaData>,
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

    pub async fn open_prefixed(db_path: &Path) -> Result<Self, Error> {
        let file_name = db_path.file_name().ok_or(Error::InvalidReplicationPath)?;

        let wal_index_file = format!("{}-client_wal_index", file_name.to_str().unwrap());

        let path = db_path.with_file_name(wal_index_file);

        // If there is no database or there exists a database AND a wal index file
        // then allow the embedded replica to be created. If Neither of those conditions are met
        // for example a database without a index file then we throw this error.
        if !(!db_path.exists() || path.exists()) {
            return Err(Error::RequiresCleanDatabase);
        }

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        let data = WalIndexMetaData::read(&mut file).await?;

        Ok(Self { file, data })
    }

    /// Inits metatdata from a handshake response.
    pub fn init_from_hello(&mut self, hello: HelloResponse) -> Result<(), Error> {
        let hello_log_id = Uuid::from_str(&hello.log_id)
            .map_err(|_| Error::InvalidLogId)?
            .as_u128();

        match self.data {
            Some(meta) => {
                if meta.log_id.get() != hello_log_id {
                    Err(Error::LogIncompatible)
                } else {
                    Ok(())
                }
            }
            None => {
                self.data = Some(WalIndexMetaData {
                    log_id: hello_log_id.into(),
                    committed_frame_no: FrameNo::MAX.into(),
                    _padding: Default::default(),
                });
                Ok(())
            }
        }
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        self.flush_inner().await?;

        Ok(())
    }

    async fn flush_inner(&mut self) -> std::io::Result<()> {
        if let Some(data) = self.data {
            // FIXME: we can save a syscall by calling read_exact_at, but let's use tokio API for now
            self.file.seek(SeekFrom::Start(0)).await?;
            self.file.write_all(data.as_bytes()).await?;
            self.file.flush().await?;
        }

        Ok(())
    }

    /// Apply the last commit frame no to the meta file.
    /// This function must be called after each injection, because it's idempotent to re-apply the
    /// last transaction, but not idempotent if we lose track of more than one.
    pub async fn set_commit_frame_no(&mut self, commit_fno: FrameNo) -> Result<(), Error> {
        {
            let data = self
                .data
                .as_mut()
                .expect("call set_commit_frame_no before initializing meta");
            data.committed_frame_no = commit_fno.into();
        }

        if let Err(e) = self.flush_inner().await {
            return Err(Error::FailedToCommit(e));
        }

        Ok(())
    }

    pub fn current_frame_no(&self) -> Option<FrameNo> {
        self.data.and_then(|d| {
            if d.committed_frame_no.get() == FrameNo::MAX {
                None
            } else {
                Some(d.committed_frame_no.get())
            }
        })
    }

    /// force default initialization, if the meta wasn't already initialized.
    /// The log_id is set to 0, and so is the replication index is set to None
    pub fn init_default(&mut self) {
        if self.data.is_none() {
            let meta = WalIndexMetaData {
                log_id: 0.into(),
                committed_frame_no: FrameNo::MAX.into(),
                _padding: Default::default(),
            };

            self.data.replace(meta);
        }
    }

    pub fn reset(&mut self) {
        self.data.take();
    }
}
