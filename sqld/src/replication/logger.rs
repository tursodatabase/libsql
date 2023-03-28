use std::ffi::{c_int, c_void};
use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, ensure};
use bytemuck::{bytes_of, pod_read_unaligned, try_from_bytes, Pod, Zeroable};
use bytes::{Bytes, BytesMut};
use crc::Crc;
use parking_lot::RwLock;
use rusqlite::ffi::SQLITE_ERROR;
use uuid::Uuid;

use crate::libsql::ffi::{
    types::{XWalFrameFn, XWalUndoFn},
    PgHdr, Wal,
};
use crate::libsql::{ffi::PageHdrIter, wal_hook::WalHook};

use super::snapshot::{find_snapshot_file, LogCompactor, SnapshotFile};

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");
const CRC_64_GO_ISO: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_GO_ISO);

/// The frame uniquely identifing, monotonically increasing number
pub type FrameNo = u64;

// Clone is necessary only because opening a database may fail, and we need to clone the empty
// struct.
#[derive(Clone)]
pub struct ReplicationLoggerHook {
    buffer: Vec<WalPage>,
    logger: Arc<ReplicationLogger>,
}

/// This implementation of WalHook intercepts calls to `on_frame`, and writes them to a
/// shadow wal. Writing to the shadow wal is done in three steps:
/// i. append the new pages at the offset pointed by header.start_frame_no + header.frame_count
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

            self.logger
                .log_file
                .write()
                .maybe_compact(
                    self.logger.compactor.clone(),
                    ntruncate,
                    &self.logger.db_path,
                    self.logger.current_checksum.load(Ordering::Relaxed),
                )
                .unwrap();
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
    /// size of a single frame
    pub const SIZE: usize = size_of::<FrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn try_from_bytes(mut data: Bytes) -> anyhow::Result<Self> {
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

pub struct WalFrameBorrowed<'a> {
    pub header: FrameHeader,
    pub data: &'a [u8],
}

impl<'a> From<&'a WalFrame> for WalFrameBorrowed<'a> {
    fn from(other: &'a WalFrame) -> WalFrameBorrowed<'a> {
        WalFrameBorrowed {
            header: other.header,
            data: &other.data,
        }
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

/// Represent a LogFile, and operations that can be performed on it.
/// A log file must only ever be opened by a single instance of LogFile, since it caches the file
/// header.
pub struct LogFile {
    file: File,
    header: LogFileHeader,
    max_log_frame_count: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum LogReadError {
    #[error("could not fetch log entry, snapshot required")]
    SnapshotRequired,
    #[error("requested entry is ahead of log")]
    Ahead,
    #[error(transparent)]
    Error(#[from] anyhow::Error),
}

impl LogFile {
    /// size of a single frame
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + WAL_PAGE_SIZE as usize;

    pub fn new(file: File, max_log_frame_count: u64) -> anyhow::Result<Self> {
        // FIXME: we should probably take a lock on this file, to prevent anybody else to write to
        // it.
        let file_end = file.metadata()?.len();

        if file_end == 0 {
            let db_id = Uuid::new_v4();
            let header = LogFileHeader {
                version: 1,
                start_frame_no: 0,
                magic: WAL_MAGIC,
                page_size: WAL_PAGE_SIZE,
                start_checksum: 0,
                db_id: db_id.as_u128(),
                frame_count: 0,
                _pad: 0,
            };

            let mut this = Self {
                file,
                header,
                max_log_frame_count,
            };

            this.write_header(&header)?;

            Ok(this)
        } else {
            let header = Self::read_header(&file)?;
            Ok(Self {
                file,
                header,
                max_log_frame_count,
            })
        }
    }

    pub fn read_header(file: &File) -> anyhow::Result<LogFileHeader> {
        let mut buf = [0; size_of::<LogFileHeader>()];
        file.read_exact_at(&mut buf, 0)?;
        let header: LogFileHeader = pod_read_unaligned(&buf);
        if header.magic != WAL_MAGIC {
            bail!("invalid replication log header");
        }

        Ok(header)
    }

    pub fn header(&self) -> &LogFileHeader {
        &self.header
    }

    pub fn write_header(&mut self, header: &LogFileHeader) -> anyhow::Result<()> {
        self.file.write_all_at(bytes_of(header), 0)?;
        self.header = *header;
        Ok(())
    }

    /// Returns an iterator over the WAL frame headers
    fn frames_iter(&self) -> anyhow::Result<impl Iterator<Item = anyhow::Result<WalFrame>> + '_> {
        let mut current_offset = 0;
        Ok(std::iter::from_fn(move || {
            if current_offset >= self.header.frame_count {
                return None;
            }
            let read_offset = Self::absolute_byte_offset(current_offset);
            current_offset += 1;
            self.read_frame_offset(read_offset).transpose()
        }))
    }

    /// Returns an iterator over the WAL frame headers
    pub fn rev_frames_iter(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<WalFrame>> + '_> {
        let mut current_offset = self.header.frame_count;

        Ok(std::iter::from_fn(move || {
            if current_offset == 0 {
                return None;
            }
            current_offset -= 1;
            let read_offset = Self::absolute_byte_offset(current_offset);
            self.read_frame_offset(read_offset).transpose()
        }))
    }

    pub fn push_frame(&mut self, frame: WalFrame) -> anyhow::Result<()> {
        let offset = frame.header.frame_no;
        let byte_offset = Self::absolute_byte_offset(offset - self.header.start_frame_no);
        tracing::trace!("writing frame {offset} at offset {byte_offset}");
        self.file
            .write_all_at(bytes_of(&frame.header), byte_offset)?;
        self.file
            .write_all_at(&frame.data, byte_offset + size_of::<FrameHeader>() as u64)?;

        Ok(())
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<LogFileHeader>() as u64 + nth * Self::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameNo) -> anyhow::Result<Option<u64>> {
        if id < self.header.start_frame_no
            || id > self.header.start_frame_no + self.header.frame_count
        {
            return Ok(None);
        }
        Ok(Self::absolute_byte_offset(id - self.header.start_frame_no).into())
    }

    /// Returns bytes represening a WalFrame for frame `frame_no`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame_bytes(&self, frame_no: FrameNo) -> std::result::Result<Bytes, LogReadError> {
        if frame_no < self.header.start_frame_no {
            return Err(LogReadError::SnapshotRequired);
        }

        if frame_no >= self.header.start_frame_no + self.header.frame_count {
            return Err(LogReadError::Ahead);
        }

        let mut buffer = BytesMut::zeroed(Self::FRAME_SIZE);
        self.file
            .read_exact_at(&mut buffer, self.byte_offset(frame_no)?.unwrap())
            .map_err(anyhow::Error::from)?; // unwrap: we checked
                                            // that the frame_no
                                            // in in the file before
        Ok(buffer.freeze())
    }

    fn maybe_compact(
        &mut self,
        compactor: LogCompactor,
        size_after: u32,
        path: &Path,
        start_checksum: u64,
    ) -> anyhow::Result<()> {
        if self.header.frame_count > self.max_log_frame_count {
            return self.do_compaction(compactor, size_after, path, start_checksum);
        }

        Ok(())
    }

    fn do_compaction(
        &mut self,
        compactor: LogCompactor,
        size_after: u32,
        path: &Path,
        start_checksum: u64,
    ) -> anyhow::Result<()> {
        tracing::info!("performing log compaction");
        let temp_log_path = path.join("temp_log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&temp_log_path)?;
        let mut new_log_file = LogFile::new(file, self.max_log_frame_count)?;
        let new_header = LogFileHeader {
            start_frame_no: self.header.start_frame_no + self.header.frame_count,
            frame_count: 0,
            start_checksum,
            ..self.header
        };
        new_log_file.write_header(&new_header).unwrap();
        // swap old and new snapshot
        atomic_rename(&temp_log_path, path.join("wallog")).unwrap();
        let old_log_file = std::mem::replace(self, new_log_file);
        compactor.compact(old_log_file, temp_log_path, size_after)?;

        Ok(())
    }

    fn read_frame_offset(&self, offset: u64) -> anyhow::Result<Option<WalFrame>> {
        let mut buffer = BytesMut::zeroed(LogFile::FRAME_SIZE);
        self.file.read_exact_at(&mut buffer, offset)?;
        let mut buffer = buffer.freeze();
        let header_bytes = buffer.split_to(size_of::<FrameHeader>());
        let header = FrameHeader::decode(&header_bytes)?;
        Ok(Some(WalFrame {
            header,
            data: buffer,
        }))
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use std::os::unix::prelude::OsStrExt;

    use nix::libc::renamex_np;
    use nix::libc::RENAME_SWAP;

    unsafe {
        let ret = renamex_np(
            p1.as_ref().as_os_str().as_bytes().as_ptr() as _,
            p2.as_ref().as_os_str().as_bytes().as_ptr() as _,
            RENAME_SWAP,
        );

        if ret != 0 {
            bail!("failed to perform snapshot file swap");
        }
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use anyhow::Context;
    use nix::fcntl::{renameat2, RenameFlags};

    renameat2(
        None,
        p1.as_ref(),
        None,
        p2.as_ref(),
        RenameFlags::RENAME_EXCHANGE,
    )
    .context("failed to perform snapshot file swap")?;

    Ok(())
}

#[derive(Debug, Clone, Copy, Zeroable, Pod)]
#[repr(C)]
pub struct LogFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    pub magic: u64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    pub start_checksum: u64,
    /// Uuid of the database associated with this log.
    pub db_id: u128,
    /// Frame_no of the first frame in the log
    pub start_frame_no: FrameNo,
    /// entry count in file
    pub frame_count: u64,
    /// Wal file version number, currently: 1
    pub version: u32,
    /// page size: 4096
    pub page_size: i32,
    pub _pad: u64,
}

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

impl FrameHeader {
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
    current_checksum: AtomicU64,
    pub generation: Generation,
    pub log_file: RwLock<LogFile>,
    compactor: LogCompactor,
    db_path: PathBuf,
}

impl ReplicationLogger {
    pub fn open(db_path: &Path, max_log_size: u64) -> anyhow::Result<Self> {
        let log_path = db_path.join("wallog");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(log_path)?;
        let max_log_frame_count = max_log_size * 1_000_000 / LogFile::FRAME_SIZE as u64;
        let log_file = LogFile::new(file, max_log_frame_count)?;
        let current_checksum =
            if log_file.header().frame_count == 0 && log_file.header().start_frame_no == 0 {
                AtomicU64::new(0)
            } else {
                AtomicU64::new(Self::compute_checksum(log_file.header(), &log_file)?)
            };

        let header = log_file.header;
        let generation_start_frame_no = header.start_frame_no + header.frame_count;
        Ok(Self {
            current_checksum,
            generation: Generation::new(generation_start_frame_no),
            compactor: LogCompactor::new(db_path, log_file.header.db_id)?,
            log_file: RwLock::new(log_file),
            db_path: db_path.to_owned(),
        })
    }

    pub fn database_id(&self) -> anyhow::Result<Uuid> {
        Ok(Uuid::from_u128((self.log_file.read()).header().db_id))
    }

    /// Write pages to the log, without updating the file header.
    /// Returns the new frame count and checksum to commit
    fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<(u64, u64)> {
        let mut log_file = self.log_file.write();
        let log_header = *log_file.header();
        let mut current_frame = log_header.frame_count;
        let mut current_checksum = self.current_checksum.load(Ordering::Relaxed);
        for page in pages.iter() {
            assert_eq!(page.data.len(), WAL_PAGE_SIZE as usize);
            let mut digest = CRC_64_GO_ISO.digest_with_initial(current_checksum);
            digest.update(&page.data);
            let checksum = digest.finalize();

            let header = FrameHeader {
                frame_no: log_header.start_frame_no + current_frame,
                checksum,
                page_no: page.page_no,
                size_after: page.size_after,
            };

            let frame = WalFrame {
                header,
                data: page.data.clone(),
            };

            log_file.push_frame(frame)?;

            current_frame += 1;
            current_checksum = checksum;
        }

        Ok((
            log_header.frame_count + pages.len() as u64,
            current_checksum,
        ))
    }

    fn compute_checksum(wal_header: &LogFileHeader, log_file: &LogFile) -> anyhow::Result<u64> {
        tracing::debug!("computing WAL log running checksum...");
        let mut iter = log_file.frames_iter()?;
        iter.try_fold(wal_header.start_checksum, |sum, frame| {
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
        let mut log_file = self.log_file.write();
        let mut header = *log_file.header();
        header.frame_count = new_frame_count;
        log_file.write_header(&header).expect("dailed to commit");
        self.current_checksum
            .store(new_current_checksum, Ordering::Relaxed);
    }

    pub fn get_snapshot_file(&self, from: FrameNo) -> anyhow::Result<Option<SnapshotFile>> {
        find_snapshot_file(&self.db_path, from)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), 0).unwrap();

        let frames = (0..10)
            .map(|i| WalPage {
                page_no: i,
                size_after: 0,
                data: Bytes::from(vec![i as _; 4096]),
            })
            .collect::<Vec<_>>();
        let (count, chk) = logger.write_pages(&frames).unwrap();
        logger.commit(count, chk);

        let log_file = logger.log_file.write();
        for i in 0..10 {
            let frame = WalFrame::try_from_bytes(log_file.frame_bytes(i).unwrap()).unwrap();
            assert_eq!(frame.header.page_no, i as u32);
            assert!(frame.data.iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            log_file.header.start_frame_no + log_file.header.frame_count,
            10
        );
    }

    #[test]
    fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), 0).unwrap();
        let log_file = logger.log_file.write();
        assert!(matches!(log_file.frame_bytes(1), Err(LogReadError::Ahead)));
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(dir.path(), 0).unwrap();
        let entry = WalPage {
            page_no: 0,
            size_after: 0,
            data: vec![0; 3].into(),
        };

        let (count, chk) = logger.write_pages(&[entry]).unwrap();
        logger.commit(count, chk);
    }
}
