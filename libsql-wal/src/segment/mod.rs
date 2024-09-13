//! Libsql-wal is organized as a linked list of segments. Frames are appended to the head segments,
//! and eventually, the head segment is swapped for a new empty one. The previous head segment is
//! sealed and becomes immutable. The head segment is represented by the `CurrentSegment` type, and
//! the sealed segments by the `SealedSegment` type.
//!
//! When a reader starts a transaction, it record the head segment current frame_no. This is the
//! maximum frame_no that this reader is allowed to read. The reader also keeps a reference to the
//! head segment at the moment it was created.
#![allow(dead_code)]
use std::future::Future;
use std::hash::Hasher as _;
use std::io;
use std::mem::offset_of;
use std::mem::size_of;
use std::num::NonZeroU64;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use zerocopy::byteorder::little_endian::{U128, U16, U32, U64};
use zerocopy::AsBytes;

use crate::error::{Error, Result};
use crate::io::buf::IoBufMut;
use crate::io::FileExt;
use crate::io::Io;
use crate::LIBSQL_MAGIC;
use crate::LIBSQL_PAGE_SIZE;

pub(crate) mod compacted;
pub mod current;
pub mod list;
pub mod sealed;

bitflags::bitflags! {
    pub struct SegmentFlags: u32 {
        /// Frames in the segment are ordered in ascending frame_no.
        /// This is true for a segment created by a primary, but a replica may insert frames in any
        /// order, as long as commit boundaries are preserved.
        const FRAME_UNORDERED = 1 << 0;
        /// The segment is sealed. If this flag is set, then
        const SEALED          = 1 << 1;
    }
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, Clone, Copy)]
pub struct SegmentHeader {
    /// Set to LIBSQL_MAGIC
    pub magic: U64,
    /// header version
    pub version: U16,
    pub start_frame_no: U64,
    pub last_commited_frame_no: U64,
    /// number of frames in the segment
    pub frame_count: U64,
    /// size of the database in pages, after applying the segment.
    pub size_after: U32,
    /// byte offset of the index. If 0, then the index wasn't written, and must be recovered.
    /// If non-0, the segment is sealed, and must not be written to anymore
    /// the index is followed by its checksum
    pub index_offset: U64,
    pub index_size: U64,
    pub flags: U32,
    /// salt for the segment checksum
    pub salt: U32,
    /// right now we only support 4096, but if se decided to support other sizes,
    /// we could do it without changing the header
    pub page_size: U16,
    pub log_id: U128,
    /// ms, from unix epoch
    pub sealed_at_timestamp: U64,

    /// checksum of the header fields, excluding the checksum itself. This field must be the last
    pub header_cheksum: U32,
}

impl SegmentHeader {
    fn checksum(&self) -> u32 {
        let field_bytes: &[u8] = &self.as_bytes()[..offset_of!(SegmentHeader, header_cheksum)];
        let checksum = crc32fast::hash(field_bytes);
        checksum
    }

    fn check(&self) -> Result<()> {
        if self.page_size.get() != LIBSQL_PAGE_SIZE {
            return Err(Error::InvalidPageSize);
        }

        if self.magic.get() != LIBSQL_MAGIC {
            return Err(Error::InvalidHeaderChecksum);
        }

        if self.version.get() != 1 {
            return Err(Error::InvalidHeaderVersion);
        }

        let computed = self.checksum();
        if computed == self.header_cheksum.get() {
            return Ok(());
        } else {
            return Err(Error::InvalidHeaderChecksum);
        }
    }

    pub fn flags(&self) -> SegmentFlags {
        SegmentFlags::from_bits(self.flags.get()).unwrap()
    }

    fn set_flags(&mut self, flags: SegmentFlags) {
        self.flags = flags.bits().into();
    }

    fn recompute_checksum(&mut self) {
        let checksum = self.checksum();
        self.header_cheksum = checksum.into();
    }

    pub fn last_commited_frame_no(&self) -> u64 {
        self.last_commited_frame_no.get()
    }

    /// size fo the db after applying this segment
    pub fn size_after(&self) -> u32 {
        self.size_after.get()
    }

    fn is_empty(&self) -> bool {
        self.frame_count() == 0
    }

    pub fn frame_count(&self) -> usize {
        self.frame_count.get() as usize
    }

    pub fn last_committed(&self) -> u64 {
        // either the current segment is empty, and the start frame_no is the last frame_no commited on
        // the previous segment (start_frame_no - 1), or it's the last committed frame_no from this
        // segment.
        if self.is_empty() {
            self.start_frame_no.get() - 1
        } else {
            self.last_commited_frame_no.get()
        }
    }

    pub(crate) fn next_frame_no(&self) -> NonZeroU64 {
        if self.is_empty() {
            assert!(self.start_frame_no.get() > 0);
            NonZeroU64::new(self.start_frame_no.get()).unwrap()
        } else {
            NonZeroU64::new(self.last_commited_frame_no.get() + 1).unwrap()
        }
    }
}

pub trait Segment: Send + Sync + 'static {
    fn compact(
        &self,
        out_file: &impl FileExt,
        id: uuid::Uuid,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send;
    fn start_frame_no(&self) -> u64;
    fn last_committed(&self) -> u64;
    fn index(&self) -> &fst::Map<Arc<[u8]>>;
    fn is_storable(&self) -> bool;
    fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> io::Result<bool>;
    /// returns the number of readers currently holding a reference to this log.
    /// The read count must monotonically decrease.
    fn is_checkpointable(&self) -> bool;
    /// The size of the database after applying this segment.
    fn size_after(&self) -> u32;
    async fn read_frame_offset_async<B>(&self, offset: u32, buf: B) -> (B, Result<()>)
    where
        B: IoBufMut + Send + 'static;
    fn timestamp(&self) -> DateTime<Utc>;

    fn destroy<IO: Io>(&self, io: &IO) -> impl Future<Output = ()>;
}

impl<T: Segment> Segment for Arc<T> {
    fn compact(
        &self,
        out_file: &impl FileExt,
        id: uuid::Uuid,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send {
        self.as_ref().compact(out_file, id)
    }

    fn start_frame_no(&self) -> u64 {
        self.as_ref().start_frame_no()
    }

    fn last_committed(&self) -> u64 {
        self.as_ref().last_committed()
    }

    fn index(&self) -> &fst::Map<Arc<[u8]>> {
        self.as_ref().index()
    }

    fn is_storable(&self) -> bool {
        self.as_ref().is_storable()
    }

    fn read_page(&self, page_no: u32, max_frame_no: u64, buf: &mut [u8]) -> io::Result<bool> {
        self.as_ref().read_page(page_no, max_frame_no, buf)
    }

    fn is_checkpointable(&self) -> bool {
        self.as_ref().is_checkpointable()
    }

    fn size_after(&self) -> u32 {
        self.as_ref().size_after()
    }

    async fn read_frame_offset_async<B>(&self, offset: u32, buf: B) -> (B, Result<()>)
    where
        B: IoBufMut + Send + 'static,
    {
        self.as_ref().read_frame_offset_async(offset, buf).await
    }

    fn destroy<IO: Io>(&self, io: &IO) -> impl Future<Output = ()> {
        self.as_ref().destroy(io)
    }

    fn timestamp(&self) -> DateTime<Utc> {
        self.as_ref().timestamp()
    }
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
pub struct FrameHeader {
    pub page_no: U32,
    pub size_after: U32,
    pub frame_no: U64,
}

impl FrameHeader {
    pub fn page_no(&self) -> u32 {
        self.page_no.get()
    }

    pub fn size_after(&self) -> u32 {
        self.size_after.get()
    }

    pub fn frame_no(&self) -> u64 {
        self.frame_no.get()
    }

    pub fn set_frame_no(&mut self, frame_no: u64) {
        self.frame_no = frame_no.into();
    }

    pub fn set_page_no(&mut self, page_no: u32) {
        self.page_no = page_no.into();
    }

    pub fn set_size_after(&mut self, size_after: u32) {
        self.size_after = size_after.into();
    }

    pub fn is_commit(&self) -> bool {
        self.size_after() != 0
    }
}

/// A page with a running runnign checksum prepended.
/// `checksum` is computed by taking the checksum of the previous frame and crc32'ing it with frame
/// data (header and page content). The first page is hashed with the segment header salt.
#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
pub struct CheckedFrame {
    checksum: U32,
    // frame should always be the last field
    frame: Frame,
}

impl CheckedFrame {
    pub(crate) const fn offset_of_frame() -> usize {
        offset_of!(Self, frame)
    }
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
pub struct Frame {
    header: FrameHeader,
    data: [u8; LIBSQL_PAGE_SIZE as usize],
}

impl Frame {
    pub(crate) fn checksum(&self, previous_checksum: u32) -> u32 {
        let mut digest = crc32fast::Hasher::new_with_initial(previous_checksum);
        digest.write(self.as_bytes());
        digest.finalize()
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut FrameHeader {
        &mut self.header
    }

    pub(crate) fn size_after(&self) -> Option<u32> {
        let size_after = self.header().size_after.get();
        (size_after != 0).then_some(size_after)
    }

    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// offset of the CheckedFrame in a current of sealed segment
#[inline]
fn checked_frame_offset(offset: u32) -> u64 {
    (size_of::<SegmentHeader>() + (offset as usize) * size_of::<CheckedFrame>()) as u64
}
/// offset of a Frame in a current or sealed segment.
#[inline]
fn frame_offset(offset: u32) -> u64 {
    checked_frame_offset(offset) + CheckedFrame::offset_of_frame() as u64
}

/// offset of a frame's page in a current or sealed segment.
#[inline]
fn page_offset(offset: u32) -> u64 {
    frame_offset(offset) + size_of::<FrameHeader>() as u64
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn offsets() {
        assert_eq!(checked_frame_offset(0) as usize, size_of::<SegmentHeader>());
        assert_eq!(
            frame_offset(0) as usize,
            size_of::<SegmentHeader>() + CheckedFrame::offset_of_frame()
        );
        assert_eq!(
            page_offset(0) as usize,
            size_of::<SegmentHeader>() + CheckedFrame::offset_of_frame() + size_of::<FrameHeader>()
        );
    }
}
