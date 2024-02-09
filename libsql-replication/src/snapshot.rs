use std::mem::size_of;
use std::mem::MaybeUninit;
use std::path::Path;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use zerocopy::byteorder::little_endian::{U128 as lu128, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromZeroes};

use crate::frame::{FrameBorrowed, FrameMut, FrameNo};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid snapshot file")]
    InvalidSnapshot,
}

#[derive(
    Debug, Copy, Clone, PartialEq, Eq, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes,
)]
#[repr(C)]
pub struct SnapshotFileHeader {
    /// id of the database
    pub log_id: lu128,
    /// first frame in the snapshot
    pub start_frame_no: lu64,
    /// end frame in the snapshot
    pub end_frame_no: lu64,
    /// number of frames in the snapshot
    pub frame_count: lu64,
    /// safe of the database after applying the snapshot
    pub size_after: lu32,
    pub _pad: [u8; 4],
}

pub struct SnapshotFile {
    file: File,
    header: SnapshotFileHeader,
    encryptor: Option<crate::FrameEncryptor>,
}

impl SnapshotFile {
    pub async fn open(
        path: impl AsRef<Path>,
        encryptor: Option<crate::FrameEncryptor>,
    ) -> Result<Self, Error> {
        let mut file = File::open(path).await?;
        let mut header = SnapshotFileHeader::new_zeroed();
        file.read_exact(header.as_bytes_mut()).await?;

        Ok(Self {
            file,
            header,
            encryptor,
        })
    }

    pub fn into_stream_mut(mut self) -> impl Stream<Item = Result<FrameMut, Error>> {
        async_stream::try_stream! {
            let mut previous_frame_no = None;
            for _ in 0..self.header.frame_count.get() {
                let mut frame: MaybeUninit<FrameBorrowed> = MaybeUninit::uninit();
                let buf = unsafe { std::slice::from_raw_parts_mut(frame.as_mut_ptr() as *mut u8, size_of::<FrameBorrowed>()) };
                self.file.read_exact(buf).await?;
                let mut frame = unsafe { frame.assume_init() };
                if let Some(encryptor) = &self.encryptor {
                    encryptor.decrypt(frame.page_mut()).map_err(|_| Error::InvalidSnapshot)?;
                }

                if previous_frame_no.is_none() {
                    previous_frame_no = Some(frame.header().frame_no);
                } else if previous_frame_no.unwrap().get() <= frame.header().frame_no.get() {
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
            Ok(f) => f.header().frame_no.get() >= from,
            Err(_) => true,
        })
    }

    #[inline(always)]
    pub fn header(&self) -> &SnapshotFileHeader {
        &self.header
    }
}
