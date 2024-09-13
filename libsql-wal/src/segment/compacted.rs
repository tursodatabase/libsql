use std::io;
use std::mem::size_of;

use zerocopy::little_endian::{U128 as lu128, U16 as lu16, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::io::buf::{IoBufMut, ZeroCopyBuf};
use crate::io::FileExt;
use crate::segment::FrameHeader;
use crate::{LIBSQL_MAGIC, LIBSQL_PAGE_SIZE, LIBSQL_WAL_VERSION};

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
    /// for now, always 4096
    pub(crate) page_size: lu16,
    pub(crate) timestamp: lu64,
}
impl CompactedSegmentDataHeader {
    fn check(&self) -> Result<()> {
        if self.magic.get() != LIBSQL_MAGIC {
            return Err(super::Error::InvalidHeaderMagic);
        }

        if self.page_size.get() != LIBSQL_PAGE_SIZE {
            return Err(super::Error::InvalidPageSize);
        }

        if self.version.get() != LIBSQL_WAL_VERSION {
            return Err(super::Error::InvalidPageSize);
        }

        Ok(())
    }

    pub fn size_after(&self) -> u32 {
        self.size_after.get()
    }
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

impl<F> CompactedSegment<F> {
    pub fn remap_file_type<FN, T>(self, f: FN) -> CompactedSegment<T>
    where
        FN: FnOnce(F) -> T,
    {
        CompactedSegment {
            header: self.header,
            file: f(self.file),
        }
    }

    pub fn header(&self) -> &CompactedSegmentDataHeader {
        &self.header
    }
}

impl<F: FileExt> CompactedSegment<F> {
    pub(crate) async fn open(file: F) -> Result<Self> {
        let buf = ZeroCopyBuf::new_uninit();
        let (buf, ret) = file.read_exact_at_async(buf, 0).await;
        ret?;
        let header: CompactedSegmentDataHeader = buf.into_inner();
        header.check()?;
        Ok(Self { file, header })
    }

    pub(crate) fn from_parts(file: F, header: CompactedSegmentDataHeader) -> Self {
        Self { header, file }
    }

    pub(crate) async fn read_frame<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u32,
    ) -> (B, io::Result<()>) {
        assert_eq!(buf.bytes_init(), 0);
        assert_eq!(buf.bytes_total(), size_of::<Frame>());
        let offset = size_of::<CompactedSegmentDataHeader>() + size_of::<Frame>() * offset as usize;
        let (buf, ret) = self.file.read_exact_at_async(buf, offset as u64).await;
        (buf, ret)
    }

    pub(crate) async fn read_page<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u32,
    ) -> (B, io::Result<()>) {
        assert_eq!(buf.bytes_init(), 0);
        assert_eq!(buf.bytes_total(), LIBSQL_PAGE_SIZE as usize);
        let offset = size_of::<CompactedSegmentDataHeader>()
            + size_of::<Frame>() * offset as usize
            + size_of::<FrameHeader>();
        let (buf, ret) = self.file.read_exact_at_async(buf, offset as u64).await;
        (buf, ret)
    }
}
