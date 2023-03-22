use std::fs::File;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;

use bytemuck::{pod_read_unaligned, Pod, Zeroable};
use bytes::{Bytes, BytesMut};

use super::logger::{FrameHeader, LogFile};

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct SnapshotFileHeader {
    /// id of the database
    pub db_id: u128,
    /// first frame in the snapshot
    pub start_frame_id: u64,
    /// end frame in the snapshot
    pub end_frame_index: u64,
    /// number of frames in the snapshot
    pub frame_count: u64,
    /// safe of the database after applying the snapshot
    pub size_after: u32,
    pub _pad: u32,
}

pub struct SnapshotFile {
    file: File,
    header: SnapshotFileHeader,
}

impl SnapshotFile {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut header_buf = [0; size_of::<SnapshotFileHeader>()];
        file.read_exact_at(&mut header_buf, 0)?;
        let header: SnapshotFileHeader = pod_read_unaligned(&header_buf);

        Ok(Self { file, header })
    }

    /// Iterator on the frames contained in the snapshot file, in reverse frame_id order.
    pub fn frames_iter_until(
        &self,
        offset: u64,
    ) -> impl Iterator<Item = anyhow::Result<Bytes>> + '_ {
        let mut current_offset = 0;
        std::iter::from_fn(move || {
            if current_offset >= self.header.frame_count {
                return None;
            }
            let read_offset = size_of::<SnapshotFileHeader>() as u64
                + current_offset * LogFile::FRAME_SIZE as u64;
            current_offset += 1;
            let mut buf = BytesMut::zeroed(LogFile::FRAME_SIZE);
            match self.file.read_exact_at(&mut buf, read_offset as _) {
                Ok(_) => {
                    let header: FrameHeader = pod_read_unaligned(&buf[..size_of::<FrameHeader>()]);
                    if header.frame_id <= offset {
                        None
                    } else {
                        Some(Ok(buf.freeze()))
                    }
                }
                Err(e) => Some(Err(e.into())),
            }
        })
    }
}
