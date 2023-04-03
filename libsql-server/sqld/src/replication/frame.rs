use std::borrow::Cow;
use std::fmt;
use std::mem::{align_of, align_of_val, size_of, transmute};
use std::ops::Deref;

use bytemuck::{bytes_of, from_bytes, pod_read_unaligned, Pod, Zeroable};
use bytes::{Bytes, BytesMut};

use crate::replication::WAL_PAGE_SIZE;

use super::FrameNo;

/// The file header for the WAL log. All fields are represented in little-endian ordering.
/// See `encode` and `decode` for actual layout.
// repr C for stable sizing
#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct FrameHeader {
    /// Incremental frame number
    pub frame_no: FrameNo,
    /// Rolling checksum of all the previous frames, including this one.
    pub checksum: u64,
    /// page number, if frame_type is FrameType::Page
    pub page_no: u32,
    pub size_after: u32,
}

#[derive(Clone)]
/// The owned version of a replication frame.
/// Cloning this is cheap.
pub struct Frame {
    data: Bytes,
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("header", &self.header())
            .field("data", &"[..]")
            .finish()
    }
}

impl Frame {
    /// size of a single frame
    pub const SIZE: usize = size_of::<FrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn from_parts(header: &FrameHeader, data: &[u8]) -> Self {
        assert_eq!(data.len(), WAL_PAGE_SIZE as usize);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        buf.extend_from_slice(bytes_of(header));
        buf.extend_from_slice(data);

        Self { data: buf.freeze() }
    }

    pub fn try_from_bytes(data: Bytes) -> anyhow::Result<Self> {
        anyhow::ensure!(data.len() == Self::SIZE, "invalid frame size");
        Ok(Self { data })
    }
}

/// The borrowed version of Frame
#[repr(transparent)]
pub struct FrameBorrowed {
    data: [u8],
}

impl FrameBorrowed {
    pub fn header(&self) -> Cow<FrameHeader> {
        if align_of_val(&self.data) == align_of::<FrameHeader>() {
            Cow::Borrowed(from_bytes(&self.data[..size_of::<FrameHeader>()]))
        } else {
            Cow::Owned(pod_read_unaligned(&self.data[..size_of::<FrameHeader>()]))
        }
    }

    /// Returns the bytes for this frame. Includes the header bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn from_bytes(data: &[u8]) -> &Self {
        assert_eq!(data.len(), Frame::SIZE);
        // SAFETY: &WalFrameBorrowed is equivalent to &[u8]
        unsafe { transmute(data) }
    }

    /// returns this frame's page data.
    pub fn page(&self) -> &[u8] {
        &self.data[size_of::<FrameHeader>()..]
    }
}

impl Deref for Frame {
    type Target = FrameBorrowed;

    fn deref(&self) -> &Self::Target {
        let data: &[u8] = &self.data;
        // SAFETY: &WalFrameBorrowed is equivalent to &[u8]
        unsafe { transmute(data) }
    }
}
