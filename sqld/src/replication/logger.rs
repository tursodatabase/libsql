use std::ffi::{c_int, c_void};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::ensure;
use bytemuck::{bytes_of, cast_ref, try_from_bytes, Pod, Zeroable};
use bytes::{BufMut, Bytes, BytesMut};
use crc::Crc;
use parking_lot::Mutex;
use rusqlite::ffi::SQLITE_ERROR;
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
pub struct ReplicationLoggerHook {
    /// Current frame index, updated on each commit.
    buffer: Vec<WalPage>,
    logger: Arc<ReplicationLogger>,
}

/// This implementation of WalHook intercepts calls to `on_frame`, and writes them to a
/// shadow wal. Writing to the shadow wal is done in three steps:
/// i. append the new pages at the offset pointed by header.start_frame_index + header.frame_count
/// ii. call the underlying implementation of on_frames
/// iii. if the call of the underlying method was successfull, update the log header to the new
/// frame count.
///
/// If either writing to the database of to the shadow wal fails, it must be noop.
unsafe impl WalHook for ReplicationLoggerHook {
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

        for (page_no, data) in PageHdrIter::new(page_headers, page_size as _) {
            self.write_frame(page_no, data)
        }

        let commit_info = if is_commit != 0 {
            match self.flush(ntruncate) {
                Err(e) => {
                    tracing::error!("error writing to replication log: {e}");
                    return SQLITE_ERROR;
                }
                Ok(commit_info) => commit_info,
            }
        } else {
            None
        };

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

        if is_commit != 0 && rc == 0 {
            if let Some((count, checksum)) = commit_info {
                self.commit(count, checksum);
            }
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
    pub header: FrameHeader,
    pub data: Bytes,
}

impl WalFrame {
    fn encode<B: BufMut>(&self, mut buf: B) {
        self.header.encode(&mut buf);
        // FIXME: unnecessary data copy. (clone is ok, since it's Bytes)
        buf.put(&mut self.data.clone());
    }

    pub fn decode(mut data: Bytes) -> anyhow::Result<Self> {
        let header_bytes = data.split_to(size_of::<FrameHeader>());
        ensure!(
            data.len() == WAL_PAGE_SIZE as usize,
            "invalid frame size, expected: {}, found: {}",
            WAL_PAGE_SIZE,
            data.len()
        );
        let header = FrameHeader::decode(&header_bytes)?;
        Ok(Self { header, data })
    }
}

impl ReplicationLoggerHook {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
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

    /// write buffered pages to the logger, without commiting.
    /// Returns the attempted count and checksum, that need to be passed to `commit`
    fn flush(&mut self, size_after: u32) -> anyhow::Result<Option<(u64, u64)>> {
        if !self.buffer.is_empty() {
            self.buffer.last_mut().unwrap().size_after = size_after;
            let ret = self.logger.write_pages(&self.buffer)?;
            self.buffer.clear();
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }

    fn commit(&self, new_count: u64, new_checksum: u64) {
        self.logger.commit(new_count, new_checksum)
    }

    fn rollback(&mut self) {
        self.buffer.clear();
    }
}

#[derive(Debug, Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct LogFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    magic: u64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    start_checksum: u64,
    /// Uuid of the database associated with this log.
    db_id: u128,
    /// Frame index of the first frame in the log
    start_frame_id: u64,
    /// entry count in file
    frame_count: u64,
    /// Wal file version number, currently: 1
    version: u32,
    /// page size: 4096
    page_size: i32,
    /// 0 padding for alignment
    _pad: u64,
}

impl LogFileHeader {
    fn decode(buf: &[u8]) -> anyhow::Result<Self> {
        let this: Self =
            *try_from_bytes(buf).map_err(|_e| anyhow::anyhow!("invalid WAL log header"))?;
        ensure!(this.magic == WAL_MAGIC, "invalid WAL log header");

        Ok(this)
    }

    fn encode<B: BufMut>(&self, mut buf: B) {
        buf.put(&cast_ref::<_, [u8; size_of::<Self>()]>(self)[..]);
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<Self>() as u64 + nth * ReplicationLogger::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameId) -> Option<u64> {
        if id < self.start_frame_id || id > self.start_frame_id + self.frame_count {
            return None;
        }
        Self::absolute_byte_offset(id - self.start_frame_id).into()
    }
}

/// The file header for the WAL log. All fields are represented in little-endian ordering.
/// See `encode` and `decode` for actual layout.
// repr C for stable sizing
#[repr(C)]
#[derive(Debug, Clone, Copy, Zeroable, Pod)]
pub struct FrameHeader {
    /// Incremental frame id
    pub frame_id: u64,
    /// Rolling checksum of all the previous frames, including this one.
    pub checksum: u64,
    /// page number, if frame_type is FrameType::Page
    pub page_no: u32,
    pub size_after: u32,
}

impl FrameHeader {
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

pub struct ReplicationLogger {
    log_header: Mutex<LogFileHeader>,
    current_checksum: AtomicU64,
    pub generation: Generation,
    log_file: File,
}

impl ReplicationLogger {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().join("wallog");
        let mut log_file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;
        let file_end = log_file.metadata()?.len();
        let current_checksum;

        let header = if file_end == 0 {
            let db_id = Uuid::new_v4();
            let header = LogFileHeader {
                version: 1,
                start_frame_id: 0,
                magic: WAL_MAGIC,
                page_size: WAL_PAGE_SIZE,
                start_checksum: 0,
                db_id: db_id.as_u128(),
                frame_count: 0,
                _pad: 0,
            };

            let mut header_buf = BytesMut::new();
            header.encode(&mut header_buf);
            current_checksum = AtomicU64::new(0);

            assert_eq!(header_buf.len(), std::mem::size_of::<LogFileHeader>());

            log_file.write_all(&header_buf)?;
            header
        } else {
            let mut header_buf = BytesMut::zeroed(size_of::<LogFileHeader>());
            log_file.read_exact(&mut header_buf)?;
            let header = LogFileHeader::decode(&header_buf)?;
            current_checksum = AtomicU64::new(Self::compute_checksum(&header, &log_file)?);
            header
        };

        Ok(Self {
            current_checksum,
            generation: Generation::new(header.start_frame_id),
            log_header: Mutex::new(header),
            log_file,
        })
    }

    pub fn database_id(&self) -> Uuid {
        Uuid::from_u128(self.log_header.lock().db_id)
    }

    /// Write pages to the log, without updating the file header.
    /// Returns the new frame count and checksum to commit
    fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<(u64, u64)> {
        let log_header = { *self.log_header.lock() };
        let mut current_frame = log_header.frame_count;
        let mut buffer = BytesMut::with_capacity(Self::FRAME_SIZE);
        let mut current_checksum = self.current_checksum.load(Ordering::Relaxed);
        for page in pages.iter() {
            debug_assert_eq!(page.data.len(), WAL_PAGE_SIZE as usize);
            let mut digest = CRC_64_GO_ISO.digest_with_initial(current_checksum);
            digest.update(&page.data);
            let checksum = digest.finalize();

            let header = FrameHeader {
                frame_id: log_header.start_frame_id + current_frame,
                checksum,
                page_no: page.page_no,
                size_after: page.size_after,
            };

            let frame = WalFrame {
                header,
                data: page.data.clone(),
            };

            frame.encode(&mut buffer);

            let byte_offset = LogFileHeader::absolute_byte_offset(current_frame);
            tracing::trace!("writing frame {current_frame} at offset {byte_offset}");
            self.log_file.write_all_at(&buffer, byte_offset)?;

            current_frame += 1;
            current_checksum = checksum;

            buffer.clear();
        }

        Ok((
            log_header.frame_count + pages.len() as u64,
            current_checksum,
        ))
    }

    /// Returns bytes represening a WalFrame for frame `id`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame_bytes(&self, id: FrameId) -> anyhow::Result<Option<Bytes>> {
        let header = { *self.log_header.lock() };
        if id < header.start_frame_id {
            return Ok(None);
        }

        if id >= header.start_frame_id + header.frame_count {
            return Ok(None);
        }

        let mut buffer = BytesMut::zeroed(Self::FRAME_SIZE);
        self.log_file
            .read_exact_at(&mut buffer, header.byte_offset(id).unwrap())?; // unwrap: we checked
                                                                           // that the frame index
                                                                           // in in the file before

        Ok(Some(buffer.freeze()))
    }

    /// Returns an iterator over the WAL frame headers
    fn frames_iter(
        file: &File,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<WalFrame>> + '_> {
        fn read_frame_offset(file: &File, offset: u64) -> anyhow::Result<Option<WalFrame>> {
            let mut buffer = BytesMut::zeroed(ReplicationLogger::FRAME_SIZE);
            file.read_exact_at(&mut buffer, offset)?;
            let mut buffer = buffer.freeze();
            let header_bytes = buffer.split_to(size_of::<FrameHeader>());
            let header = FrameHeader::decode(&header_bytes)?;
            Ok(Some(WalFrame {
                header,
                data: buffer,
            }))
        }

        let file_len = file.metadata()?.len();
        let mut current_offset = 0;

        Ok(std::iter::from_fn(move || {
            let read_offset = LogFileHeader::absolute_byte_offset(current_offset);
            if read_offset >= file_len {
                return None;
            }
            current_offset += 1;
            read_frame_offset(file, read_offset).transpose()
        }))
    }

    fn compute_checksum(wal_header: &LogFileHeader, log_file: &File) -> anyhow::Result<u64> {
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

    fn commit(&self, new_frame_count: u64, new_current_checksum: u64) {
        let mut header = { *self.log_header.lock() };
        header.frame_count = new_frame_count;

        self.log_file
            .write_all_at(bytes_of(&header), 0)
            .expect("fatal error, failed to commit to log");

        self.current_checksum
            .store(new_current_checksum, Ordering::Relaxed);
        *self.log_header.lock() = header;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path()).unwrap();

        let frames = (0..10)
            .map(|i| WalPage {
                page_no: i,
                size_after: 0,
                data: Bytes::from(vec![i as _; 4096]),
            })
            .collect::<Vec<_>>();
        let (count, chk) = logger.write_pages(&frames).unwrap();
        logger.commit(count, chk);

        for i in 0..10 {
            let frame = WalFrame::decode(logger.frame_bytes(i).unwrap().unwrap()).unwrap();
            assert_eq!(frame.header.page_no, i as u32);
            assert!(frame.data.iter().all(|x| i as u8 == *x));
        }

        let header = { *logger.log_header.lock() };
        assert_eq!(header.start_frame_id + header.frame_count, 10);
    }

    #[test]
    fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path()).unwrap();
        assert!(logger.frame_bytes(1).unwrap().is_none());
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path()).unwrap();
        let entry = WalPage {
            page_no: 0,
            size_after: 0,
            data: vec![0; 3].into(),
        };

        let (count, chk) = logger.write_pages(&[entry]).unwrap();
        logger.commit(count, chk);
    }
}
