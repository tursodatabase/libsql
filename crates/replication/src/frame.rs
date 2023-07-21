use std::borrow::Cow;
use std::fmt;
use std::mem::{size_of, transmute};
use std::ops::Deref;

use bytemuck::{bytes_of, pod_read_unaligned, try_from_bytes, Pod, Zeroable};
use bytes::{Bytes, BytesMut};

use crate::WAL_PAGE_SIZE;

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
    /// Size of the database (in page) after commiting the transaction. This is passed from sqlite,
    /// and serves as commit transaction boundary
    pub size_after: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
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

    pub fn bytes(&self) -> Bytes {
        self.data.clone()
    }
}

/// The borrowed version of Frame
#[repr(transparent)]
pub struct FrameBorrowed {
    data: [u8],
}

impl FrameBorrowed {
    pub fn header(&self) -> Cow<FrameHeader> {
        let data = &self.data[..size_of::<FrameHeader>()];
        try_from_bytes(data)
            .map(Cow::Borrowed)
            .unwrap_or_else(|_| Cow::Owned(pod_read_unaligned(data)))
    }

    /// Returns the bytes for this frame. Includes the header bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn from_bytes(data: &[u8]) -> &Self {
        assert_eq!(data.len(), Frame::SIZE);
        // SAFETY: &FrameBorrowed is equivalent to &[u8]
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
        FrameBorrowed::from_bytes(&self.data)
    }
}
