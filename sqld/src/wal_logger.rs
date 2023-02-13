use std::ffi::{c_int, c_void};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::ensure;
use bytemuck::{cast_ref, try_from_bytes, Pod, Zeroable};
use bytes::{BufMut, Bytes, BytesMut};
use crc::Crc;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::libsql::ffi::{
    types::{XWalFrameFn, XWalUndoFn},
    PgHdr, Wal,
};
use crate::libsql::{ffi::PageHdrIter, wal_hook::WalHook};

pub const WAL_PAGE_SIZE: i32 = 4096;
const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");
const CRC_64_GO_ISO: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_GO_ISO);

pub type FrameId = u64;

// Clone is necessary only because opening a database may fail, and we need to clone the empty
// struct.
#[derive(Clone)]
pub struct WalLoggerHook {
    /// Current frame index, updated on each commit.
    buffer: Vec<WalPage>,
    logger: Arc<WalLogger>,
}

unsafe impl WalHook for WalLoggerHook {
    fn on_frames(
        &mut self,
        wal: *mut Wal,
        page_size: c_int,
        page_headers: *mut PgHdr,
        ntruncate: u32,
        is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        assert_eq!(page_size, 4096);

        let rc = unsafe {
            orig(
                wal,
                page_size,
                page_headers,
                ntruncate,
                is_commit,
                sync_flags,
            )
        };
        if rc != crate::libsql::ffi::SQLITE_OK {
            return rc;
        }

        for (page_no, data) in PageHdrIter::new(page_headers, page_size as _) {
            self.write_frame(page_no, data)
        }

        if is_commit != 0 {
            self.commit(ntruncate);
        }

        rc
    }

    fn on_undo(
        &mut self,
        wal: *mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        self.rollback();
        unsafe { orig(wal, func, ctx) }
    }
}

#[derive(Clone)]
struct WalPage {
    page_no: u32,
    /// 0 for non-commit frames
    size_after: u32,
    data: Bytes,
}

#[derive(Clone)]
/// A buffered WalFrame.
/// Cloning this is cheap.
pub struct WalFrame {
    pub header: WalFrameHeader,
    pub data: Bytes,
}

impl WalFrame {
    fn encode<B: BufMut>(&self, mut buf: B) {
        self.header.encode(&mut buf);
        // FIXME: unnecessary data copy. (clone is ok, since it's Bytes)
        buf.put(&mut self.data.clone());
    }

    pub fn decode(mut data: Bytes) -> anyhow::Result<Self> {
        let header_bytes = data.split_to(size_of::<WalFrameHeader>());
        ensure!(
            data.len() == WAL_PAGE_SIZE as usize,
            "invalid frame size, expected: {}, found: {}",
            WAL_PAGE_SIZE,
            data.len()
        );
        let header = WalFrameHeader::decode(&header_bytes)?;
        Ok(Self { header, data })
    }
}

impl WalLoggerHook {
    pub fn new(logger: Arc<WalLogger>) -> Self {
        Self {
            buffer: Default::default(),
            logger,
        }
    }

    fn write_frame(&mut self, page_no: u32, data: &[u8]) {
        let entry = WalPage {
            page_no,
            size_after: 0,
            data: Bytes::copy_from_slice(data),
        };
        self.buffer.push(entry);
    }

    fn commit(&mut self, size_after: u32) {
        self.buffer.last_mut().unwrap().size_after = size_after;
        self.logger.push_page(&self.buffer);
        self.buffer.clear();
    }

    fn rollback(&mut self) {
        self.buffer.clear();
    }
}

#[derive(Debug, Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct WalLoggerFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    magic: u64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    start_checksum: u64,
    /// Uuid of the database associated with this log.
    db_id: u128,
    /// Frame index of the first frame in the log
    start_frame_id: u64,
    /// Wal file version number, currently: 1
    version: u32,
    /// page size: 4096
    page_size: i32,
}

impl WalLoggerFileHeader {
    fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let this: Self =
            *try_from_bytes(buf).map_err(|_e| anyhow::anyhow!("invalid WAL log header"))?;
        ensure!(this.magic == WAL_MAGIC, "invalid WAL log header");

        Ok(this)
    }

    fn encode<B: BufMut>(&self, mut buf: B) {
        buf.put(&cast_ref::<_, [u8; size_of::<Self>()]>(self)[..]);
    }
}

/// The file header for the WAL log. All fields are represented in little-endian ordering.
/// See `encode` and `decode` for actual layout.
// repr C for stable sizing
#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct WalFrameHeader {
    /// Incremental frame id
    pub frame_id: u64,
    /// Rolling checksum of all the previous frames, including this one.
    pub checksum: u64,
    /// page number, if frame_type is FrameType::Page
    pub page_no: u32,
    pub size_after: u32,
}

impl WalFrameHeader {
    fn encode<B: BufMut>(&self, mut buf: B) {
        buf.put(&cast_ref::<_, [u8; size_of::<Self>()]>(self)[..]);
    }

    fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let this = try_from_bytes(buf).map_err(|_e| anyhow::anyhow!("invalid frame header"))?;
        Ok(*this)
    }
}

pub struct Generation {
    pub id: Uuid,
    pub start_index: u64,
}

impl Generation {
    fn new(start_index: u64) -> Self {
        Self {
            id: Uuid::new_v4(),
            start_index,
        }
    }
}

pub struct WalLogger {
    /// offset id of the next Frame to write into the log
    next_frame_id: Mutex<FrameId>,
    /// first index present in the file
    start_frame_id: FrameId,
    log_file: File,
    current_checksum: AtomicU64,
    pub database_id: Uuid,
    pub generation: Generation,
}

impl WalLogger {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<WalFrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().join("wallog");
        let mut log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let file_end = log_file.metadata()?.len();
        let end_id;
        let current_checksum;

        let header = if file_end == 0 {
            let db_id = Uuid::new_v4();
            let header = WalLoggerFileHeader {
                version: 1,
                start_frame_id: 0,
                magic: WAL_MAGIC,
                page_size: WAL_PAGE_SIZE,
                start_checksum: 0,
                db_id: db_id.as_u128(),
            };

            let mut header_buf = BytesMut::new();
            header.encode(&mut header_buf);

            assert_eq!(header_buf.len(), std::mem::size_of::<WalLoggerFileHeader>());

            log_file.write_all(&header_buf)?;
            end_id = 0;
            current_checksum = AtomicU64::new(0);
            header
        } else {
            let mut header_buf = BytesMut::zeroed(size_of::<WalLoggerFileHeader>());
            log_file.read_exact(&mut header_buf)?;
            let header = WalLoggerFileHeader::decode(&header_buf)?;
            end_id = (file_end - size_of::<WalFrameHeader>() as u64) / Self::FRAME_SIZE as u64;
            current_checksum = AtomicU64::new(Self::compute_checksum(&header, &log_file)?);
            header
        };

        Ok(Self {
            next_frame_id: Mutex::new(end_id),
            start_frame_id: header.start_frame_id,
            log_file,
            current_checksum,
            database_id: Uuid::from_u128(header.db_id),
            generation: Generation::new(end_id),
        })
    }

    fn push_page(&self, pages: &[WalPage]) {
        let mut lock = self.next_frame_id.lock();
        let mut current_offset = *lock;
        let mut buffer = BytesMut::with_capacity(Self::FRAME_SIZE);
        let mut current_checksum = self.current_checksum.load(Ordering::Relaxed);
        for page in pages.iter() {
            debug_assert_eq!(page.data.len(), WAL_PAGE_SIZE as usize);
            let mut digest = CRC_64_GO_ISO.digest_with_initial(current_checksum);
            digest.update(&page.data);
            let checksum = digest.finalize();

            let header = WalFrameHeader {
                frame_id: current_offset,
                checksum,
                page_no: page.page_no,
                size_after: page.size_after,
            };

            let frame = WalFrame {
                header,
                data: page.data.clone(),
            };

            frame.encode(&mut buffer);

            self.log_file
                .write_all_at(
                    &buffer,
                    self.byte_offset(current_offset)
                        .expect("attempt to write entry before first entry in the log"),
                )
                .unwrap();

            current_offset += 1;
            current_checksum = checksum;

            buffer.clear();
        }

        self.current_checksum
            .store(current_checksum, Ordering::Relaxed);

        *lock = current_offset;
    }

    /// Returns bytes represening a WalFrame for frame `id`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame_bytes(&self, id: FrameId) -> anyhow::Result<Option<Bytes>> {
        if id < self.start_frame_id {
            return Ok(None);
        }

        if id >= *self.next_frame_id.lock() {
            return Ok(None);
        }

        let mut buffer = BytesMut::zeroed(Self::FRAME_SIZE);
        self.log_file
            .read_exact_at(&mut buffer, self.byte_offset(id).unwrap())?;

        Ok(Some(buffer.freeze()))
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<WalLoggerFileHeader>() as u64 + nth * WalLogger::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameId) -> Option<u64> {
        if id < self.start_frame_id {
            return None;
        }
        Self::absolute_byte_offset(id - self.start_frame_id).into()
    }

    /// Returns an iterator over the WAL frame headers
    fn frames_iter(
        file: &File,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<WalFrame>> + '_> {
        fn read_frame_offset(file: &File, offset: u64) -> anyhow::Result<Option<WalFrame>> {
            let mut buffer = BytesMut::zeroed(WalLogger::FRAME_SIZE);
            file.read_exact_at(&mut buffer, offset)?;
            let mut buffer = buffer.freeze();
            let header_bytes = buffer.split_to(size_of::<WalFrameHeader>());
            let header = WalFrameHeader::decode(&header_bytes)?;
            Ok(Some(WalFrame {
                header,
                data: buffer,
            }))
        }

        let file_len = file.metadata()?.len();
        let mut current_offset = 0;

        Ok(std::iter::from_fn(move || {
            let read_offset = Self::absolute_byte_offset(current_offset);
            if read_offset >= file_len {
                return None;
            }
            current_offset += 1;
            read_frame_offset(file, read_offset).transpose()
        }))
    }

    fn compute_checksum(wal_header: &WalLoggerFileHeader, log_file: &File) -> anyhow::Result<u64> {
        tracing::debug!("computing WAL log running checksum...");
        let mut iter = Self::frames_iter(log_file)?;
        iter.try_fold(wal_header.start_frame_id, |sum, frame| {
            let frame = frame?;
            let mut digest = CRC_64_GO_ISO.digest_with_initial(sum);
            digest.update(&frame.data);
            let cs = digest.finalize();
            ensure!(
                cs == frame.header.checksum,
                "invalid WAL file: invalid checksum"
            );
            Ok(cs)
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();

        assert_eq!(*logger.next_frame_id.lock(), 0);

        let frames = (0..10)
            .map(|i| WalPage {
                page_no: i,
                size_after: 0,
                data: Bytes::from(vec![i as _; 4096]),
            })
            .collect::<Vec<_>>();
        logger.push_page(&frames);

        for i in 0..10 {
            let frame = WalFrame::decode(logger.frame_bytes(i).unwrap().unwrap()).unwrap();
            assert_eq!(frame.header.page_no, i as u32);
            assert!(frame.data.iter().all(|x| i as u8 == *x));
        }

        assert_eq!(*logger.next_frame_id.lock(), 10);
    }

    #[test]
    fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();
        assert!(logger.frame_bytes(1).unwrap().is_none());
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = WalLogger::open(dir.path()).unwrap();
        let entry = WalPage {
            page_no: 0,
            size_after: 0,
            data: vec![0; 3].into(),
        };
        logger.push_page(&[entry]);
    }
}
