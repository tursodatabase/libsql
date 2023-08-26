use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Seek};
use std::mem::size_of;
use std::io::Read;
use std::path::Path;

use bytemuck::{try_pod_read_unaligned, Pod, Zeroable};

#[repr(C)]
#[derive(Debug, Pod, Zeroable, Clone, Copy)]
pub struct WalIndexMeta {
    /// This is the anticipated next frame_no to request
    pub pre_commit_frame_no: crate::FrameNo,
    /// After we have written the frames back to the wal, we set this value to the same value as
    /// pre_commit_index
    /// On startup we check this value against the pre-commit value to check for consistency
    pub post_commit_frame_no: crate::FrameNo,
    /// Generation Uuid
    /// This number is generated on each primary restart. This let's us know that the primary, and
    /// we need to make sure that we are not ahead of the primary.
    pub generation_id: u128,
    /// Uuid of the database this instance is a replica of
    pub database_id: u128,
}

impl WalIndexMeta {
    pub fn read_from_path(db_path: &Path) -> anyhow::Result<(Option<Self>, File)> {
        let path = db_path.join("client_wal_index");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok((Self::read(&file)?, file))
    }

    fn read(mut file: &File) -> anyhow::Result<Option<Self>> {
        let mut buf = [0; size_of::<WalIndexMeta>()];
        file.seek(std::io::SeekFrom::Start(0))?;
        let meta = match file.read_exact(&mut buf) {
            Ok(()) => {
                let meta: Self = try_pod_read_unaligned(&buf)
                    .map_err(|_| anyhow::anyhow!("invalid index meta file"))?;
                Some(meta)
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => Err(e)?,
        };

        Ok(meta)
    }
}
