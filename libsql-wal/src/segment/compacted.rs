use std::io;
use std::mem::{offset_of, size_of};

use chrono::{DateTime, Utc};
use uuid::Uuid;
use zerocopy::little_endian::{U128 as lu128, U16 as lu16, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use crate::io::buf::{IoBufMut, ZeroCopyBuf};
use crate::io::FileExt;
use crate::{LIBSQL_MAGIC, LIBSQL_PAGE_SIZE, LIBSQL_WAL_VERSION};

use super::Result;

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentHeader {
    pub(crate) magic: lu64,
    pub(crate) version: lu16,
    pub(crate) log_id: lu128,
    pub(crate) start_frame_no: lu64,
    pub(crate) end_frame_no: lu64,
    pub(crate) size_after: lu32,
    /// for now, always 4096
    pub(crate) page_size: lu16,
    pub(crate) timestamp: lu64,
}

bitflags::bitflags! {
    pub struct CompactedFrameFlags: u32 {
        /// This flag is set for the last frame in the segment
        const LAST = 1 << 0;
    }
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedFrameHeader {
    pub flags: lu32,
    pub page_no: lu32,
    pub frame_no: lu64,
    /// running checksum from this frame
    /// this is the crc32 of the checksum of the previous frame and all the frame data, including
    /// all the fields before checksum in the header. THIS FIELD MUST ALWAYS BE THE last FIELD IN
    /// THE STRUCT
    pub checksum: lu32,
}

impl CompactedFrameHeader {
    pub fn flags(&self) -> CompactedFrameFlags {
        CompactedFrameFlags::from_bits(self.flags.get()).unwrap()
    }

    pub(crate) fn is_last(&self) -> bool {
        self.flags().contains(CompactedFrameFlags::LAST)
    }

    pub(crate) fn set_last(&mut self) {
        let mut flags = self.flags();
        flags.insert(CompactedFrameFlags::LAST);
        self.flags = flags.bits().into();
    }

    pub(crate) fn reset_flags(&mut self) {
        self.flags = 0.into();
    }

    pub(crate) fn compute_checksum(&self, previous: u32, data: &[u8]) -> u32 {
        assert_eq!(data.len(), LIBSQL_PAGE_SIZE as usize);
        let mut h = crc32fast::Hasher::new_with_initial(previous);
        h.update(&self.as_bytes()[..offset_of!(Self, checksum)]);
        h.update(data);
        h.finalize()
    }

    /// updates the checksum with the previous frame checksum and the frame data
    pub(crate) fn update_checksum(&mut self, previous: u32, data: &[u8]) -> u32 {
        let checksum = self.compute_checksum(previous, data);
        self.checksum = checksum.into();
        checksum
    }

    pub fn checksum(&self) -> u32 {
        self.checksum.get()
    }

    pub fn page_no(&self) -> u32 {
        self.page_no.get()
    }
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedFrame {
    pub header: CompactedFrameHeader,
    pub data: [u8; LIBSQL_PAGE_SIZE as usize],
}

impl CompactedFrame {
    pub fn header(&self) -> &CompactedFrameHeader {
        &self.header
    }

    pub(crate) fn header_mut(&mut self) -> &mut CompactedFrameHeader {
        &mut self.header
    }
}

impl CompactedSegmentHeader {
    pub fn new(
        start_frame_no: u64,
        end_frame_no: u64,
        size_after: u32,
        timestamp: DateTime<Utc>,
        log_id: Uuid,
    ) -> Self {
        Self {
            magic: LIBSQL_MAGIC.into(),
            version: LIBSQL_WAL_VERSION.into(),
            start_frame_no: start_frame_no.into(),
            end_frame_no: end_frame_no.into(),
            size_after: size_after.into(),
            page_size: LIBSQL_PAGE_SIZE.into(),
            timestamp: (timestamp.timestamp_millis() as u64).into(),
            log_id: log_id.as_u128().into(),
        }
    }

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

pub struct CompactedSegment<F> {
    header: CompactedSegmentHeader,
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

    pub fn header(&self) -> &CompactedSegmentHeader {
        &self.header
    }
}

impl<F: FileExt> CompactedSegment<F> {
    pub(crate) async fn open(file: F) -> Result<Self> {
        let buf = ZeroCopyBuf::new_uninit();
        let (buf, ret) = file.read_exact_at_async(buf, 0).await;
        ret?;
        let header: CompactedSegmentHeader = buf.into_inner();
        header.check()?;
        Ok(Self { file, header })
    }

    pub(crate) fn from_parts(file: F, header: CompactedSegmentHeader) -> Self {
        Self { header, file }
    }

    /// read a CompactedFrame from the segment
    pub(crate) async fn read_frame<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u32,
    ) -> (B, io::Result<()>) {
        assert_eq!(buf.bytes_init(), 0);
        assert_eq!(buf.bytes_total(), size_of::<CompactedFrame>());
        let offset =
            size_of::<CompactedSegmentHeader>() + size_of::<CompactedFrame>() * offset as usize;
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
        let offset = size_of::<CompactedSegmentHeader>()
            + size_of::<CompactedFrame>() * offset as usize
            + size_of::<CompactedFrameHeader>();
        let (buf, ret) = self.file.read_exact_at_async(buf, offset as u64).await;
        (buf, ret)
    }
}
