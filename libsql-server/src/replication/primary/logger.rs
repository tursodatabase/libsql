use std::ffi::{c_int, c_void, CStr};
use std::fs::{remove_dir_all, File, OpenOptions};
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure};
use bytemuck::{bytes_of, pod_read_unaligned, Pod, Zeroable};
use bytes::{Bytes, BytesMut};
use libsql_replication::frame::{Frame, FrameHeader, FrameMut};
use libsql_replication::snapshot::SnapshotFile;
use parking_lot::{Mutex, RwLock};
use rusqlite::ffi::SQLITE_BUSY;
use sqld_libsql_bindings::init_static_wal_method;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;
use uuid::Uuid;

use crate::libsql_bindings::ffi::SQLITE_IOERR_WRITE;
use crate::libsql_bindings::ffi::{
    sqlite3,
    types::{XWalCheckpointFn, XWalFrameFn, XWalSavePointUndoFn, XWalUndoFn},
    PageHdrIter, PgHdr, Wal, SQLITE_CHECKPOINT_TRUNCATE, SQLITE_IOERR, SQLITE_OK,
};
use crate::libsql_bindings::wal_hook::WalHook;
use crate::replication::snapshot::{find_snapshot_file, LogCompactor};
use crate::replication::{FrameNo, SnapshotCallback, CRC_64_GO_ISO, WAL_MAGIC};
use crate::LIBSQL_PAGE_SIZE;

init_static_wal_method!(REPLICATION_METHODS, ReplicationLoggerHook);

#[derive(PartialEq, Eq)]
struct Version([u16; 4]);

impl Version {
    fn current() -> Self {
        let major = env!("CARGO_PKG_VERSION_MAJOR").parse().unwrap();
        let minor = env!("CARGO_PKG_VERSION_MINOR").parse().unwrap();
        let patch = env!("CARGO_PKG_VERSION_PATCH").parse().unwrap();
        Self([0, major, minor, patch])
    }
}

#[derive(Debug)]
pub enum ReplicationLoggerHook {}

#[derive(Clone, Debug)]
pub struct ReplicationLoggerHookCtx {
    buffer: Vec<WalPage>,
    logger: Arc<ReplicationLogger>,
    bottomless_replicator: Option<Arc<std::sync::Mutex<bottomless::replicator::Replicator>>>,
}

/// This implementation of WalHook intercepts calls to `on_frame`, and writes them to a
/// shadow wal. Writing to the shadow wal is done in three steps:
/// i. append the new pages at the offset pointed by header.start_frame_no + header.frame_count
/// ii. call the underlying implementation of on_frames
/// iii. if the call of the underlying method was successful, update the log header to the new
/// frame count.
///
/// If either writing to the database of to the shadow wal fails, it must be noop.
unsafe impl WalHook for ReplicationLoggerHook {
    type Context = ReplicationLoggerHookCtx;

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"replication_logger_hook\0").unwrap()
    }

    fn on_frames(
        wal: &mut Wal,
        page_size: c_int,
        page_headers: *mut PgHdr,
        ntruncate: u32,
        is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        assert_eq!(page_size, 4096);
        let wal_ptr = wal as *mut _;
        let last_valid_frame = wal.hdr.mxFrame;
        tracing::trace!("Last valid frame before applying: {last_valid_frame}");
        let ctx = Self::wal_extract_ctx(wal);

        let mut frame_count = 0;
        for (page_no, data) in PageHdrIter::new(page_headers, page_size as _) {
            ctx.write_frame(page_no, data);
            frame_count += 1;
        }
        if let Err(e) = ctx.flush(ntruncate) {
            tracing::error!("error writing to replication log: {e}");
            // returning IO_ERR ensure that xUndo will be called by sqlite.
            return SQLITE_IOERR;
        }

        let rc = unsafe {
            orig(
                wal_ptr,
                page_size,
                page_headers,
                ntruncate,
                is_commit,
                sync_flags,
            )
        };

        if is_commit != 0 && rc == 0 {
            if let Err(e) = ctx.commit() {
                // If we reach this point, it means that we have committed a transaction to sqlite wal,
                // but failed to commit it to the shadow WAL, which leaves us in an inconsistent state.
                tracing::error!(
                    "fatal error: log failed to commit: inconsistent replication log: {e}"
                );
                std::process::abort();
            }

            // do backup after log replication as we don't want to replicate potentially
            // inconsistent frames
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let mut replicator = replicator.lock().unwrap();
                replicator.register_last_valid_frame(last_valid_frame);
                if let Err(e) = replicator.set_page_size(page_size as usize) {
                    tracing::error!("fatal error during backup: {e}, exiting");
                    std::process::abort()
                }
                replicator.submit_frames(frame_count as u32);
            }

            if let Err(e) = ctx.logger.log_file.write().maybe_compact(
                ctx.logger.compactor.clone(),
                ntruncate,
                &ctx.logger.db_path,
            ) {
                tracing::error!("fatal error: {e}, exiting");
                std::process::abort()
            }
        }

        rc
    }

    fn on_undo(
        wal: &mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        undo_ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        let ctx = Self::wal_extract_ctx(wal);
        ctx.rollback();
        unsafe { orig(wal, func, undo_ctx) }
    }

    fn on_savepoint_undo(wal: &mut Wal, wal_data: *mut u32, orig: XWalSavePointUndoFn) -> i32 {
        let rc = unsafe { orig(wal, wal_data) };
        if rc != SQLITE_OK {
            return rc;
        };

        {
            let ctx = Self::wal_extract_ctx(wal);
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let last_valid_frame = unsafe { *wal_data };
                let mut replicator = replicator.lock().unwrap();
                let prev_valid_frame = replicator.peek_last_valid_frame();
                tracing::trace!(
                    "Savepoint: rolling back from frame {prev_valid_frame} to {last_valid_frame}",
                );
                replicator.rollback_to_frame(last_valid_frame);
            }
        }

        rc
    }

    #[allow(clippy::too_many_arguments)]
    fn on_checkpoint(
        wal: &mut Wal,
        db: *mut sqlite3,
        emode: i32,
        busy_handler: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
        busy_arg: *mut c_void,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
        frames_in_wal: *mut i32,
        backfilled_frames: *mut i32,
        orig: XWalCheckpointFn,
    ) -> i32 {
        {
            tracing::trace!("bottomless checkpoint");

            /* In order to avoid partial checkpoints, passive checkpoint
             ** mode is not allowed. Only TRUNCATE checkpoints are accepted,
             ** because these are guaranteed to block writes, copy all WAL pages
             ** back into the main database file and reset the frame number.
             ** In order to avoid autocheckpoint on close (that's too often),
             ** checkpoint attempts weaker than TRUNCATE are ignored.
             */
            if emode < SQLITE_CHECKPOINT_TRUNCATE {
                tracing::trace!(
                    "Ignoring a checkpoint request weaker than TRUNCATE: {}",
                    emode
                );
                // Return an error to signal to sqlite that the WAL was not checkpointed, and it is
                // therefore not safe to delete it.
                return SQLITE_BUSY;
            }
        }

        #[allow(clippy::await_holding_lock)]
        // uncontended -> only gets called under a libSQL write lock
        {
            let ctx = Self::wal_extract_ctx(wal);
            let runtime = tokio::runtime::Handle::current();
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let mut replicator = replicator.lock().unwrap();
                let last_known_frame = replicator.last_known_frame();
                replicator.request_flush();
                if last_known_frame == 0 {
                    tracing::debug!("No committed changes in this generation, not snapshotting");
                    replicator.skip_snapshot_for_current_generation();
                    return SQLITE_OK;
                }
                if let Err(e) = runtime.block_on(replicator.wait_until_committed(last_known_frame))
                {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm {} frames backup: {}",
                        last_known_frame,
                        e
                    );
                    return SQLITE_IOERR_WRITE;
                }
                if let Err(e) = runtime.block_on(replicator.wait_until_snapshotted()) {
                    tracing::error!(
                        "Failed to wait for S3 replicator to confirm database snapshot backup: {}",
                        e
                    );
                    return SQLITE_IOERR_WRITE;
                }
            }
        }
        let rc = unsafe {
            orig(
                wal,
                db,
                emode,
                busy_handler,
                busy_arg,
                sync_flags,
                n_buf,
                z_buf,
                frames_in_wal,
                backfilled_frames,
            )
        };

        if rc != SQLITE_OK {
            return rc;
        }

        #[allow(clippy::await_holding_lock)]
        // uncontended -> only gets called under a libSQL write lock
        {
            let ctx = Self::wal_extract_ctx(wal);
            let runtime = tokio::runtime::Handle::current();
            if let Some(replicator) = ctx.bottomless_replicator.as_mut() {
                let mut replicator = replicator.lock().unwrap();
                let _prev = replicator.new_generation();
                if let Err(e) =
                    runtime.block_on(async move { replicator.snapshot_main_db_file().await })
                {
                    tracing::error!("Failed to snapshot the main db file during checkpoint: {e}");
                    return SQLITE_IOERR_WRITE;
                }
            }
        }
        SQLITE_OK
    }
}

#[derive(Clone, Debug)]
pub struct WalPage {
    pub page_no: u32,
    /// 0 for non-commit frames
    pub size_after: u32,
    pub data: Bytes,
}

impl ReplicationLoggerHookCtx {
    pub fn new(
        logger: Arc<ReplicationLogger>,
        bottomless_replicator: Option<Arc<std::sync::Mutex<bottomless::replicator::Replicator>>>,
    ) -> Self {
        if bottomless_replicator.is_some() {
            tracing::trace!("bottomless replication enabled");
        }
        Self {
            buffer: Default::default(),
            logger,
            bottomless_replicator,
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

    /// write buffered pages to the logger, without committing.
    fn flush(&mut self, size_after: u32) -> anyhow::Result<()> {
        if !self.buffer.is_empty() {
            self.buffer.last_mut().unwrap().size_after = size_after;
            self.logger.write_pages(&self.buffer)?;
            self.buffer.clear();
        }

        Ok(())
    }

    fn commit(&self) -> anyhow::Result<()> {
        let new_frame_no = self.logger.commit()?;
        tracing::trace!("new frame committed {new_frame_no:?}");
        self.logger.new_frame_notifier.send_replace(new_frame_no);
        Ok(())
    }

    fn rollback(&mut self) {
        self.logger.log_file.write().rollback();
        self.buffer.clear();
    }

    pub fn logger(&self) -> &ReplicationLogger {
        self.logger.as_ref()
    }
}

/// Represent a LogFile, and operations that can be performed on it.
/// A log file must only ever be opened by a single instance of LogFile, since it caches the file
/// header.
#[derive(Debug)]
pub struct LogFile {
    file: File,
    pub header: LogFileHeader,
    /// the maximum number of frames this log is allowed to contain before it should be compacted.
    max_log_frame_count: u64,
    /// the maximum duration before the log should be compacted.
    max_log_duration: Option<Duration>,
    /// the time of the last compaction
    last_compact_instant: Instant,

    /// number of frames in the log that have not been committed yet. On commit the header's frame
    /// count is incremented by that amount. New pages are written after the last
    /// header.frame_count + uncommit_frame_count.
    /// On rollback, this is reset to 0, so that everything that was written after the previous
    /// header.frame_count is ignored and can be overwritten
    uncommitted_frame_count: u64,
    uncommitted_checksum: u64,

    /// checksum of the last committed frame
    commited_checksum: u64,
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
    pub const FRAME_SIZE: usize = size_of::<FrameHeader>() + LIBSQL_PAGE_SIZE as usize;

    pub fn new(
        file: File,
        max_log_frame_count: u64,
        max_log_duration: Option<Duration>,
    ) -> anyhow::Result<Self> {
        // FIXME: we should probably take a lock on this file, to prevent anybody else to write to
        // it.
        let file_end = file.metadata()?.len();

        let header = if file_end == 0 {
            let log_id = Uuid::new_v4();
            LogFileHeader {
                version: 2,
                start_frame_no: 0,
                magic: WAL_MAGIC,
                page_size: LIBSQL_PAGE_SIZE as i32,
                start_checksum: 0,
                log_id: log_id.as_u128(),
                frame_count: 0,
                sqld_version: Version::current().0,
            }
        } else {
            Self::read_header(&file)?
        };

        let mut this = Self {
            file,
            header,
            max_log_frame_count,
            max_log_duration,
            last_compact_instant: Instant::now(),
            uncommitted_frame_count: 0,
            uncommitted_checksum: 0,
            commited_checksum: 0,
        };

        if file_end == 0 {
            this.write_header()?;
        } else if let Some(last_commited) = this.last_commited_frame_no() {
            // file is not empty, the starting checksum is the checksum from the last entry
            let last_frame = this.frame(last_commited)?;
            this.commited_checksum = last_frame.header().checksum;
            this.uncommitted_checksum = last_frame.header().checksum;
        } else {
            // file contains no entry, start with the initial checksum from the file header.
            this.commited_checksum = this.header.start_checksum;
            this.uncommitted_checksum = this.header.start_checksum;
        }

        Ok(this)
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

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.header.frame_count += self.uncommitted_frame_count;
        self.uncommitted_frame_count = 0;
        self.commited_checksum = self.uncommitted_checksum;
        self.write_header()?;

        Ok(())
    }

    fn rollback(&mut self) {
        self.uncommitted_frame_count = 0;
        self.uncommitted_checksum = self.commited_checksum;
    }

    pub fn write_header(&mut self) -> anyhow::Result<()> {
        self.file.write_all_at(bytes_of(&self.header), 0)?;
        self.file.flush()?;

        Ok(())
    }

    /// Returns an iterator over the WAL frame headers
    pub(crate) fn frames_iter(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Frame>> + '_> {
        let mut current_frame_offset = 0;
        Ok(std::iter::from_fn(move || {
            if current_frame_offset >= self.header.frame_count {
                return None;
            }
            let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
            current_frame_offset += 1;
            Some(
                self.read_frame_byte_offset_mut(read_byte_offset)
                    .map(|f| f.into()),
            )
        }))
    }

    /// Returns an iterator over the WAL frame headers
    pub fn rev_frames_iter_mut(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<FrameMut>> + '_> {
        let mut current_frame_offset = self.header.frame_count;

        Ok(std::iter::from_fn(move || {
            if current_frame_offset == 0 {
                return None;
            }
            current_frame_offset -= 1;
            let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
            let frame = self.read_frame_byte_offset_mut(read_byte_offset);
            Some(frame)
        }))
    }

    pub fn into_rev_stream_mut(self) -> impl Stream<Item = anyhow::Result<FrameMut>> {
        let mut current_frame_offset = self.header.frame_count;
        let file = Arc::new(Mutex::new(self));
        async_stream::try_stream! {
            loop {
                if current_frame_offset == 0 {
                    break;
                }
                current_frame_offset -= 1;
                let read_byte_offset = Self::absolute_byte_offset(current_frame_offset);
                let frame = tokio::task::spawn_blocking({
                    let file = file.clone();
                    move || file.lock().read_frame_byte_offset_mut(read_byte_offset)
                }).await??;
                yield frame
            }
        }
    }

    fn compute_checksum(&self, page: &WalPage) -> u64 {
        let mut digest = CRC_64_GO_ISO.digest_with_initial(self.uncommitted_checksum);
        digest.update(&page.data);
        digest.finalize()
    }

    pub fn push_page(&mut self, page: &WalPage) -> anyhow::Result<()> {
        let checksum = self.compute_checksum(page);
        let frame = Frame::from_parts(
            &FrameHeader {
                frame_no: self.next_frame_no(),
                checksum,
                page_no: page.page_no,
                size_after: page.size_after,
            },
            &page.data,
        );

        let byte_offset = self.next_byte_offset();
        tracing::trace!(
            "writing frame {} at offset {byte_offset}",
            frame.header().frame_no
        );
        self.file.write_all_at(frame.as_slice(), byte_offset)?;

        self.uncommitted_frame_count += 1;
        self.uncommitted_checksum = checksum;

        Ok(())
    }

    /// offset in bytes at which to write the next frame
    fn next_byte_offset(&self) -> u64 {
        Self::absolute_byte_offset(self.header().frame_count + self.uncommitted_frame_count)
    }

    fn next_frame_no(&self) -> FrameNo {
        self.header().start_frame_no + self.header().frame_count + self.uncommitted_frame_count
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

    /// Returns bytes representing a WalFrame for frame `frame_no`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame(&self, frame_no: FrameNo) -> std::result::Result<Frame, LogReadError> {
        if frame_no < self.header.start_frame_no {
            return Err(LogReadError::SnapshotRequired);
        }

        if frame_no >= self.header.start_frame_no + self.header.frame_count {
            return Err(LogReadError::Ahead);
        }

        let frame = self.read_frame_byte_offset_mut(self.byte_offset(frame_no)?.unwrap())?;

        Ok(frame.into())
    }

    fn should_compact(&self) -> bool {
        let mut compact = false;
        compact |= self.header.frame_count > self.max_log_frame_count;
        if let Some(max_log_duration) = self.max_log_duration {
            compact |= self.last_compact_instant.elapsed() > max_log_duration;
        }
        compact &= self.uncommitted_frame_count == 0;
        compact
    }

    fn maybe_compact(
        &mut self,
        compactor: LogCompactor,
        size_after: u32,
        path: &Path,
    ) -> anyhow::Result<()> {
        if self.should_compact() {
            self.do_compaction(compactor, size_after, path)
        } else {
            Ok(())
        }
    }

    /// perform the log compaction.
    fn do_compaction(
        &mut self,
        compactor: LogCompactor,
        size_after: u32,
        path: &Path,
    ) -> anyhow::Result<()> {
        assert_eq!(self.uncommitted_frame_count, 0);

        // nothing to compact
        if self.header().frame_count == 0 {
            return Ok(());
        }

        tracing::info!("performing log compaction");
        let temp_log_path = path.join("temp_log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&temp_log_path)?;
        let mut new_log_file = LogFile::new(file, self.max_log_frame_count, self.max_log_duration)?;
        let new_header = LogFileHeader {
            start_frame_no: self.header.last_frame_no().unwrap() + 1,
            frame_count: 0,
            start_checksum: self.commited_checksum,
            ..self.header
        };
        new_log_file.header = new_header;
        new_log_file.write_header().unwrap();
        // swap old and new snapshot
        atomic_rename(&temp_log_path, path.join("wallog")).unwrap();
        let old_log_file = std::mem::replace(self, new_log_file);
        compactor.compact(old_log_file, temp_log_path, size_after)?;

        Ok(())
    }

    fn read_frame_byte_offset_mut(&self, offset: u64) -> anyhow::Result<FrameMut> {
        let mut buffer = BytesMut::zeroed(LogFile::FRAME_SIZE);
        self.file.read_exact_at(&mut buffer, offset)?;

        Ok(FrameMut::try_from(&*buffer)?)
    }

    fn last_commited_frame_no(&self) -> Option<FrameNo> {
        if self.header.frame_count == 0 {
            None
        } else {
            Some(self.header.start_frame_no + self.header.frame_count - 1)
        }
    }

    fn reset(self) -> anyhow::Result<Self> {
        let max_log_frame_count = self.max_log_frame_count;
        let max_log_duration = self.max_log_duration;
        // truncate file
        self.file.set_len(0)?;
        Self::new(self.file, max_log_frame_count, max_log_duration)
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use std::ffi::CString;
    use std::os::unix::prelude::OsStrExt;

    use nix::libc::renamex_np;
    use nix::libc::RENAME_SWAP;

    let p1 = CString::new(p1.as_ref().as_os_str().as_bytes())?;
    let p2 = CString::new(p2.as_ref().as_os_str().as_bytes())?;
    unsafe {
        let ret = renamex_np(p1.as_ptr(), p2.as_ptr(), RENAME_SWAP);

        if ret != 0 {
            bail!(
                "failed to perform snapshot file swap: {ret}, errno: {}",
                std::io::Error::last_os_error()
            );
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
    /// Uuid of the this log.
    pub log_id: u128,
    /// Frame_no of the first frame in the log
    pub start_frame_no: FrameNo,
    /// entry count in file
    pub frame_count: u64,
    /// Wal file version number, currently: 2
    pub version: u32,
    /// page size: 4096
    pub page_size: i32,
    /// sqld version when creating this log
    pub sqld_version: [u16; 4],
}

impl LogFileHeader {
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        if self.start_frame_no == 0 && self.frame_count == 0 {
            // The log does not contain any frame yet
            None
        } else {
            Some(self.start_frame_no + self.frame_count - 1)
        }
    }

    fn sqld_version(&self) -> Version {
        Version(self.sqld_version)
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
pub struct ReplicationLogger {
    pub generation: Generation,
    pub log_file: RwLock<LogFile>,
    compactor: LogCompactor,
    db_path: PathBuf,
    /// a notifier channel other tasks can subscribe to, and get notified when new frames become
    /// available.
    pub new_frame_notifier: watch::Sender<Option<FrameNo>>,
    pub closed_signal: watch::Sender<bool>,
    pub auto_checkpoint: u32,
}

impl ReplicationLogger {
    pub fn open(
        db_path: &Path,
        max_log_size: u64,
        max_log_duration: Option<Duration>,
        dirty: bool,
        auto_checkpoint: u32,
        callback: SnapshotCallback,
    ) -> anyhow::Result<Self> {
        let log_path = db_path.join("wallog");
        let data_path = db_path.join("data");

        let fresh = !log_path.exists();

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(log_path)?;

        let max_log_frame_count = max_log_size * 1_000_000 / LogFile::FRAME_SIZE as u64;
        let log_file = LogFile::new(file, max_log_frame_count, max_log_duration)?;
        let header = log_file.header();

        let should_recover = if dirty {
            tracing::info!("Replication log is dirty, recovering from database file.");
            true
        } else if header.version < 2 || header.sqld_version() != Version::current() {
            tracing::info!("replication log version not compatible with current sqld version, recovering from database file.");
            true
        } else if fresh && data_path.exists() {
            tracing::info!("replication log not found, recovering from database file.");
            true
        } else {
            false
        };

        if should_recover {
            Self::recover(log_file, data_path, callback, auto_checkpoint)
        } else {
            Self::from_log_file(db_path.to_path_buf(), log_file, callback, auto_checkpoint)
        }
    }

    fn from_log_file(
        db_path: PathBuf,
        log_file: LogFile,
        callback: SnapshotCallback,
        auto_checkpoint: u32,
    ) -> anyhow::Result<Self> {
        let header = log_file.header();
        let generation_start_frame_no = header.last_frame_no();

        let (new_frame_notifier, _) = watch::channel(generation_start_frame_no);
        unsafe {
            let conn = rusqlite::Connection::open(db_path.join("data"))?;
            let rc = rusqlite::ffi::sqlite3_wal_autocheckpoint(conn.handle(), auto_checkpoint as _);
            if rc != 0 {
                bail!(
                    "Failed to set WAL autocheckpoint to {} - error code: {}",
                    auto_checkpoint,
                    rc
                )
            } else {
                tracing::info!("SQLite autocheckpoint: {}", auto_checkpoint);
            }
        }

        let (closed_signal, _) = watch::channel(false);

        Ok(Self {
            generation: Generation::new(generation_start_frame_no.unwrap_or(0)),
            compactor: LogCompactor::new(
                &db_path,
                Uuid::from_u128(log_file.header.log_id),
                callback,
            )?,
            log_file: RwLock::new(log_file),
            db_path,
            closed_signal,
            new_frame_notifier,
            auto_checkpoint,
        })
    }

    fn recover(
        log_file: LogFile,
        mut data_path: PathBuf,
        callback: SnapshotCallback,
        auto_checkpoint: u32,
    ) -> anyhow::Result<Self> {
        // It is necessary to checkpoint before we restore the replication log, since the WAL may
        // contain pages that are not in the database file.
        checkpoint_db(&data_path)?;
        let mut log_file = log_file.reset()?;
        let snapshot_path = data_path.parent().unwrap().join("snapshots");
        // best effort, there may be no snapshots
        let _ = remove_dir_all(snapshot_path);

        let data_file = File::open(&data_path)?;
        let size = data_path.metadata()?.len();
        assert!(
            size % LIBSQL_PAGE_SIZE == 0,
            "database file size is not a multiple of page size"
        );
        let num_page = size / LIBSQL_PAGE_SIZE;
        let mut buf = [0; LIBSQL_PAGE_SIZE as usize];
        let mut page_no = 1; // page numbering starts at 1
        for i in 0..num_page {
            data_file.read_exact_at(&mut buf, i * LIBSQL_PAGE_SIZE)?;
            log_file.push_page(&WalPage {
                page_no,
                size_after: if i == num_page - 1 { num_page as _ } else { 0 },
                data: Bytes::copy_from_slice(&buf),
            })?;
            log_file.commit()?;

            page_no += 1;
        }

        assert!(data_path.pop());

        Self::from_log_file(data_path, log_file, callback, auto_checkpoint)
    }

    pub fn log_id(&self) -> Uuid {
        Uuid::from_u128((self.log_file.read()).header().log_id)
    }

    /// Write pages to the log, without updating the file header.
    /// Returns the new frame count and checksum to commit
    fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<()> {
        let mut log_file = self.log_file.write();
        for page in pages.iter() {
            log_file.push_page(page)?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    fn compute_checksum(wal_header: &LogFileHeader, log_file: &LogFile) -> anyhow::Result<u64> {
        tracing::debug!("computing WAL log running checksum...");
        let mut iter = log_file.frames_iter()?;
        iter.try_fold(wal_header.start_checksum, |sum, frame| {
            let frame = frame?;
            let mut digest = CRC_64_GO_ISO.digest_with_initial(sum);
            digest.update(frame.page());
            let cs = digest.finalize();
            ensure!(
                cs == frame.header().checksum,
                "invalid WAL file: invalid checksum"
            );
            Ok(cs)
        })
    }

    /// commit the current transaction and returns the new top frame number
    fn commit(&self) -> anyhow::Result<Option<FrameNo>> {
        let mut log_file = self.log_file.write();
        log_file.commit()?;
        Ok(log_file.header().last_frame_no())
    }

    pub async fn get_snapshot_file(&self, from: FrameNo) -> anyhow::Result<Option<SnapshotFile>> {
        find_snapshot_file(&self.db_path, from).await
    }

    pub fn get_frame(&self, frame_no: FrameNo) -> Result<Frame, LogReadError> {
        self.log_file.read().frame(frame_no)
    }

    pub fn maybe_compact(&self) -> anyhow::Result<bool> {
        let mut log_file = self.log_file.write();
        if !log_file.should_compact() {
            // compaction is not necessary or impossible, so exit early
            return Ok(false);
        }

        let last_frame = {
            let mut frames_iter = log_file.rev_frames_iter_mut()?;
            let Some(last_frame_res) = frames_iter.next() else {
                // the log file is empty, nothing to compact
                return Ok(false);
            };
            last_frame_res?
        };

        let size_after = last_frame.header().size_after;
        assert!(size_after != 0);

        log_file.do_compaction(self.compactor.clone(), size_after, &self.db_path)?;
        Ok(true)
    }
}

// FIXME: calling rusqlite::Connection's checkpoint here is a bug,
// we need to always call our virtual WAL methods.
pub fn checkpoint_db(data_path: &Path) -> anyhow::Result<()> {
    let wal_path = match data_path.parent() {
        Some(path) => path.join("data-wal"),
        None => return Ok(()),
    };

    if wal_path.try_exists()? {
        if File::open(wal_path)?.metadata()?.len() == 0 {
            tracing::debug!("wal file is empty, checkpoint not necessary");
            return Ok(());
        }
    } else {
        tracing::debug!("wal file doesn't exist, checkpoint not necessary");
        return Ok(());
    }

    unsafe {
        let conn = rusqlite::Connection::open(data_path)?;
        conn.query_row("PRAGMA journal_mode=WAL", (), |_| Ok(()))?;
        tracing::info!("initialized journal_mode=WAL");
        conn.pragma_query(None, "page_size", |row| {
            let page_size = row.get::<_, i32>(0).unwrap();
            assert_eq!(
                page_size, LIBSQL_PAGE_SIZE as i32,
                "invalid database file, expected page size to be {}, but found {} instead",
                LIBSQL_PAGE_SIZE, page_size
            );
            Ok(())
        })?;
        let mut num_checkpointed: c_int = 0;
        let rc = rusqlite::ffi::sqlite3_wal_checkpoint_v2(
            conn.handle(),
            std::ptr::null(),
            SQLITE_CHECKPOINT_TRUNCATE,
            &mut num_checkpointed as *mut _,
            std::ptr::null_mut(),
        );
        if rc == 0 {
            if num_checkpointed == -1 {
                bail!("Checkpoint failed: database journal_mode is not WAL")
            } else {
                Ok(())
            }
        } else {
            bail!("Checkpoint failed: wal_checkpoint_v2 error code {}", rc)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::DEFAULT_AUTO_CHECKPOINT;

    #[tokio::test]
    async fn write_and_read_from_frame_log() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(
            dir.path(),
            0,
            None,
            false,
            DEFAULT_AUTO_CHECKPOINT,
            Box::new(|_| Ok(())),
        )
        .unwrap();

        let frames = (0..10)
            .map(|i| WalPage {
                page_no: i,
                size_after: 0,
                data: Bytes::from(vec![i as _; 4096]),
            })
            .collect::<Vec<_>>();
        logger.write_pages(&frames).unwrap();
        logger.commit().unwrap();

        let log_file = logger.log_file.write();
        for i in 0..10 {
            let frame = log_file.frame(i).unwrap();
            assert_eq!(frame.header().page_no, i as u32);
            assert!(frame.page().iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            log_file.header.start_frame_no + log_file.header.frame_count,
            10
        );
    }

    #[tokio::test]
    async fn index_out_of_bounds() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(
            dir.path(),
            0,
            None,
            false,
            DEFAULT_AUTO_CHECKPOINT,
            Box::new(|_| Ok(())),
        )
        .unwrap();
        let log_file = logger.log_file.write();
        assert!(matches!(log_file.frame(1), Err(LogReadError::Ahead)));
    }

    #[test]
    #[should_panic]
    fn incorrect_frame_size() {
        let dir = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(
            dir.path(),
            0,
            None,
            false,
            DEFAULT_AUTO_CHECKPOINT,
            Box::new(|_| Ok(())),
        )
        .unwrap();
        let entry = WalPage {
            page_no: 0,
            size_after: 0,
            data: vec![0; 3].into(),
        };

        logger.write_pages(&[entry]).unwrap();
        logger.commit().unwrap();
    }

    #[test]
    fn log_file_test_rollback() {
        let f = tempfile::tempfile().unwrap();
        let mut log_file = LogFile::new(f, 100, None).unwrap();
        (0..5)
            .map(|i| WalPage {
                page_no: i,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .for_each(|p| {
                log_file.push_page(&p).unwrap();
            });

        assert_eq!(log_file.frames_iter().unwrap().count(), 0);

        log_file.commit().unwrap();

        (0..5)
            .map(|i| WalPage {
                page_no: i,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .for_each(|p| {
                log_file.push_page(&p).unwrap();
            });

        log_file.rollback();
        assert_eq!(log_file.frames_iter().unwrap().count(), 5);

        log_file
            .push_page(&WalPage {
                page_no: 42,
                size_after: 5,
                data: Bytes::from_static(&[1; 4096]),
            })
            .unwrap();

        assert_eq!(log_file.frames_iter().unwrap().count(), 5);
        log_file.commit().unwrap();
        assert_eq!(log_file.frames_iter().unwrap().count(), 6);
    }
}
