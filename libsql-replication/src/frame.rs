use std::alloc::Layout;
use std::fmt;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};

use bytemuck::{bytes_of, from_bytes, Pod, Zeroable};
use bytes::Bytes;

use crate::error::Error;
use crate::LIBSQL_PAGE_SIZE;

pub type FrameNo = u64;

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
    /// Size of the database (in page) after committing the transaction. This is passed from sqlite,
    /// and serves as commit transaction boundary
    pub size_after: u32,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
/// The shared version of a replication frame.
/// Cloning this is cheap.
pub struct Frame {
    inner: Bytes,
}

impl TryFrom<&[u8]> for Frame {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        Ok(FrameMut::try_from(data)?.into())
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Frame")
            .field("header", &self.header())
            .field("data", &"[..]")
            .finish()
    }
}

/// Owned version of a frame, on the heap
pub struct FrameMut {
    inner: Box<FrameBorrowed>,
}

impl TryFrom<&[u8]> for FrameMut {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() != size_of::<FrameBorrowed>() {
            return Err(Error::InvalidFrameLen);
        }
        // frames are relatively large (~4ko), we want to avoid allocating them on the stack and
        // then copying them to the heap, and instead copy them to the heap directly.
        let inner = unsafe {
            let layout = Layout::new::<FrameBorrowed>();
            let ptr = std::alloc::alloc(layout);
            ptr.copy_from(data.as_ptr(), data.len());
            Box::from_raw(ptr as *mut FrameBorrowed)
        };

        Ok(Self { inner })
    }
}

impl From<FrameMut> for Frame {
    fn from(value: FrameMut) -> Self {
        // transmute the FrameBorrowed into a Box<[u8; _]>. This is safe because the alignment of
        // [u8] divides the alignment of FrameBorrowed
        let data = unsafe {
            Vec::from_raw_parts(
                Box::into_raw(value.inner) as *mut u8,
                size_of::<FrameBorrowed>(),
                size_of::<FrameBorrowed>(),
            )
        };

        Self {
            inner: Bytes::from(data),
        }
    }
}

impl From<FrameBorrowed> for FrameMut {
    fn from(inner: FrameBorrowed) -> Self {
        Self {
            inner: Box::new(inner),
        }
    }
}

impl Frame {
    pub fn from_parts(header: &FrameHeader, data: &[u8]) -> Self {
        FrameBorrowed::from_parts(header, data).into()
    }

    pub fn bytes(&self) -> Bytes {
        self.inner.clone()
    }
}

impl From<FrameBorrowed> for Frame {
    fn from(value: FrameBorrowed) -> Self {
        FrameMut::from(value).into()
    }
}

/// The borrowed version of Frame
#[repr(C)]
#[derive(Pod, Zeroable, Copy, Clone)]
pub struct FrameBorrowed {
    header: FrameHeader,
    page: [u8; LIBSQL_PAGE_SIZE],
}

impl FrameBorrowed {
    /// Returns the bytes for this frame. Includes the header bytes.
    pub fn as_slice(&self) -> &[u8] {
        bytes_of(self)
    }

    /// returns this frame's page data.
    pub fn page(&self) -> &[u8] {
        &self.page
    }

    pub fn header(&self) -> &FrameHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut FrameHeader {
        &mut self.header
    }

    pub fn from_parts(header: &FrameHeader, page: &[u8]) -> Self {
        assert_eq!(page.len(), LIBSQL_PAGE_SIZE);

        FrameBorrowed {
            header: *header,
            page: page.try_into().unwrap(),
        }
    }
}

impl Deref for Frame {
    type Target = FrameBorrowed;

    fn deref(&self) -> &Self::Target {
        from_bytes(&self.inner)
    }
}

impl Deref for FrameMut {
    type Target = FrameBorrowed;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl DerefMut for FrameMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut()
    }
}
