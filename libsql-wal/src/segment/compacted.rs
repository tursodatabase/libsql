use std::mem::size_of;
use std::io;

use zerocopy::little_endian::{U128 as lu128, U32 as lu32, U64 as lu64, U16 as lu16};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::io::buf::{ZeroCopyBoxIoBuf, ZeroCopyBuf};
use crate::io::FileExt;

use super::{Frame, Result};

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataHeader {
    pub(crate) magic: lu64,
    pub(crate) version: lu16,
    pub(crate) frame_count: lu32,
    pub(crate) segment_id: lu128,
    pub(crate) start_frame_no: lu64,
    pub(crate) end_frame_no: lu64,
    pub(crate) size_after: lu32,
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataFooter {
    pub(crate) checksum: lu32,
}

pub struct CompactedSegment<F> {
    header: CompactedSegmentDataHeader,
    file: F,
}

impl<F: FileExt> CompactedSegment<F> {
    pub(crate) async fn open(file: F) -> Result<Self> {
        let buf = ZeroCopyBuf::new_uninit();
        let (buf, ret) = file.read_exact_at_async(buf, 0).await;
        ret?;
        let header = buf.into_inner();
        Ok(Self { file, header })

    }

    pub(crate) async fn read_frame(&self, frame: Box<Frame>, offset: u32) -> (Box<Frame>, io::Result<()>) {
        let offset = size_of::<CompactedSegmentDataHeader>() + size_of::<Frame>() * offset as usize;
        let buf = ZeroCopyBoxIoBuf::new(frame);
        let (buf, ret) = self.file.read_exact_at_async(buf, offset as u64).await;
        (buf.into_inner(), ret)
    }
}
