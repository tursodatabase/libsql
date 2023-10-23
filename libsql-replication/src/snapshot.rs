use std::mem::size_of;
use std::mem::MaybeUninit;
use std::path::Path;

use bytemuck::{pod_read_unaligned, Pod, Zeroable};
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

use crate::frame::FrameBorrowed;
use crate::frame::FrameMut;
use crate::frame::FrameNo;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid snapshot file")]
    InvalidSnapshot,
}

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct SnapshotFileHeader {
    /// id of the database
    pub log_id: u128,
    /// first frame in the snapshot
    pub start_frame_no: u64,
    /// end frame in the snapshot
    pub end_frame_no: u64,
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
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut file = File::open(path).await?;
        let mut header_buf = [0; size_of::<SnapshotFileHeader>()];
        file.read_exact(&mut header_buf).await?;
        let header: SnapshotFileHeader = pod_read_unaligned(&header_buf);

        Ok(Self { file, header })
    }

    pub fn into_stream_mut(mut self) -> impl Stream<Item = Result<FrameMut, Error>> {
        async_stream::try_stream! {
            let mut previous_frame_no = None;
            for _ in 0..self.header.frame_count {
                let mut frame: MaybeUninit<FrameBorrowed> = MaybeUninit::uninit();
                let buf = unsafe { std::slice::from_raw_parts_mut(frame.as_mut_ptr() as *mut u8, size_of::<FrameBorrowed>()) };
                self.file.read_exact(buf).await?;
                let frame = unsafe { frame.assume_init() };

                if previous_frame_no.is_none() {
                    previous_frame_no = Some(frame.header().frame_no);
                } else if previous_frame_no.unwrap() <= frame.header().frame_no {
                    // frames in snapshot must be in reverse ordering
                    Err(Error::InvalidSnapshot)?;
                } else {
                    previous_frame_no = Some(frame.header().frame_no);
                }

                yield FrameMut::from(frame)
            }
        }
    }

    pub fn into_stream_mut_from(
        self,
        from: FrameNo,
    ) -> impl Stream<Item = Result<FrameMut, Error>> {
        self.into_stream_mut().take_while(move |f| match f {
            Ok(f) => f.header().frame_no >= from,
            Err(_) => true,
        })
    }

    #[inline(always)]
    pub fn header(&self) -> &SnapshotFileHeader {
        &self.header
    }
}
