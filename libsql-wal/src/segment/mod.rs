#![allow(dead_code)]
use std::mem::size_of;
use std::num::NonZeroU64;

use memoffset::offset_of;
use zerocopy::byteorder::little_endian::{U32, U64};
use zerocopy::AsBytes;

use crate::error::{Error, Result};

pub mod list;
pub mod sealed;

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, Clone, Copy)]
pub struct SegmentHeader {
    start_frame_no: U64,
    last_commited_frame_no: U64,
    /// size of the database in pages
    db_size: U32,
    /// byte offset of the index. If 0, then the index wasn't written, and must be recovered.
    /// If non-0, the segment is sealed, and must not be written to anymore
    index_offset: U64,
    index_size: U64,
    /// checksum of the header fields, excluding the checksum itself. This field must be the last
    header_cheksum: U64,
}

impl SegmentHeader {
    fn checksum(&self) -> u64 {
        let field_bytes: &[u8] = &self.as_bytes()[..offset_of!(SegmentHeader, header_cheksum)];
        let checksum = field_bytes
            .iter()
            .map(|x| *x as u64)
            .reduce(|a, b| a ^ b)
            .unwrap_or(0);
        checksum
    }

    fn check(&self) -> Result<()> {
        let computed = self.checksum();
        if computed == self.header_cheksum.get() {
            return Ok(());
        } else {
            return Err(Error::InvalidHeaderChecksum);
        }
    }

    fn recompute_checksum(&mut self) {
        let checksum = self.checksum();
        self.header_cheksum = checksum.into();
    }

    pub fn last_commited_frame_no(&self) -> u64 {
        self.last_commited_frame_no.get()
    }

    pub fn db_size(&self) -> u32 {
        self.db_size.get()
    }

    fn is_empty(&self) -> bool {
        self.last_commited_frame_no.get() == 0
    }

    fn count_committed(&self) -> usize {
        self.last_commited_frame_no
            .get()
            .checked_sub(self.start_frame_no.get() - 1)
            .unwrap_or(0) as usize
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

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
pub struct FrameHeader {
    page_no: U32,
    size_after: U32,
    frame_no: U64,
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
}

#[repr(C)]
#[derive(Debug, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
pub struct Frame {
    header: FrameHeader,
    data: [u8; 4096],
}

impl Frame {
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }
}

fn frame_offset(offset: u32) -> u64 {
    (size_of::<SegmentHeader>() + (offset as usize) * size_of::<Frame>()) as u64
}

fn page_offset(offset: u32) -> u64 {
    frame_offset(offset) + size_of::<FrameHeader>() as u64
}
