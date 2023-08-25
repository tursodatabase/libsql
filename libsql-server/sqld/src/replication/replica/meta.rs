use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::str::FromStr;

use anyhow::Context;
use bytemuck::{try_pod_read_unaligned, Pod, Zeroable};
use uuid::Uuid;

use crate::{replication::FrameNo, rpc::replication_log::rpc::HelloResponse};

use super::error::ReplicationError;

#[repr(C)]
#[derive(Debug, Pod, Zeroable, Clone, Copy)]
pub struct WalIndexMeta {
    /// This is the anticipated next frame_no to request
    pub pre_commit_frame_no: FrameNo,
    /// After we have written the frames back to the wal, we set this value to the same value as
    /// pre_commit_index
    /// On startup we check this value against the pre-commit value to check for consistency
    pub post_commit_frame_no: FrameNo,
    /// Generation Uuid
    /// This number is generated on each primary restart. This let's us know that the primary, and
    /// we need to make sure that we are not ahead of the primary.
    generation_id: u128,
    /// Uuid of the database this instance is a replica of
    database_id: u128,
}

impl WalIndexMeta {
    pub fn open(db_path: &Path) -> crate::Result<File> {
        let path = db_path.join("client_wal_index");
        std::fs::create_dir_all(db_path)?;

        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?)
    }

    pub fn read_from_path(db_path: &Path) -> anyhow::Result<Option<Self>> {
        let file = Self::open(db_path)?;
        Ok(Self::read(&file)?)
    }

    fn read(file: &File) -> crate::Result<Option<Self>> {
        let mut buf = [0; size_of::<WalIndexMeta>()];
        let meta = match file.read_exact_at(&mut buf, 0) {
            Ok(()) => {
                file.read_exact_at(&mut buf, 0)?;
                let meta: Self = try_pod_read_unaligned(&buf)
                    .map_err(|_| anyhow::anyhow!("invalid index meta file"))?;
                Some(meta)
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => Err(e)?,
        };

        Ok(meta)
    }

    /// attempts to merge two meta files.
    pub fn merge_from_hello(mut self, hello: HelloResponse) -> Result<Self, ReplicationError> {
        let hello_db_id = Uuid::from_str(&hello.database_id)
            .context("invalid database id from primary")?
            .as_u128();
        let hello_gen_id = Uuid::from_str(&hello.generation_id)
            .context("invalid generation id from primary")?
            .as_u128();

        if hello_db_id != self.database_id {
            return Err(ReplicationError::DbIncompatible);
        }

        if self.generation_id == hello_gen_id {
            Ok(self)
        } else if self.pre_commit_frame_no <= hello.generation_start_index {
            // Ok: generation changed, but we aren't ahead of primary
            self.generation_id = hello_gen_id;
            Ok(self)
        } else {
            Err(ReplicationError::Lagging)
        }
    }

    pub fn new_from_hello(hello: HelloResponse) -> anyhow::Result<WalIndexMeta> {
        let database_id = Uuid::from_str(&hello.database_id)
            .context("invalid database id from primary")?
            .as_u128();
        let generation_id = Uuid::from_str(&hello.generation_id)
            .context("invalid generation id from primary")?
            .as_u128();

        Ok(Self {
            pre_commit_frame_no: FrameNo::MAX,
            post_commit_frame_no: FrameNo::MAX,
            generation_id,
            database_id,
        })
    }
}
