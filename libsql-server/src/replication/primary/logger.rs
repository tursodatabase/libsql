use std::ffi::c_int;
use std::fs::{remove_dir_all, File, OpenOptions};
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, ensure};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use libsql_replication::frame::{Frame, FrameBorrowed, FrameHeader, FrameMut};
use libsql_replication::snapshot::SnapshotFile;
use libsql_sys::EncryptionConfig;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use rusqlite::ffi::SQLITE_CHECKPOINT_TRUNCATE;
use tokio::sync::watch;
use tokio::time::{Duration, Instant};
use tokio_stream::Stream;
use uuid::Uuid;
use zerocopy::byteorder::little_endian::{
    I32 as li32, U128 as lu128, U16 as lu16, U32 as lu32, U64 as lu64,
};
use zerocopy::{AsBytes, FromBytes};

use crate::namespace::NamespaceName;
use crate::replication::script_backup_manager::ScriptBackupManager;
use crate::replication::snapshot::{find_snapshot_file, LogCompactor};
use crate::replication::{FrameNo, CRC_64_GO_ISO, WAL_MAGIC};
use crate::LIBSQL_PAGE_SIZE;

pub use libsql_replication::FrameEncryptor;

static REPLICATION_LATENCY_CACHE_SIZE: Lazy<u64> = Lazy::new(|| {
    std::env::var("SQLD_REPLICATION_LATENCY_CACHE_SIZE").map_or(100, |s| s.parse().unwrap_or(100))
});

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

#[derive(Clone, Debug)]
pub struct WalPage {
    pub page_no: u32,
    /// 0 for non-commit frames
    pub size_after: u32,
    pub data: Bytes,
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

    /// Encryption layer
    encryption: Option<FrameEncryptor>,
    encryption_buf: BytesMut,
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
        encryption: Option<FrameEncryptor>,
    ) -> anyhow::Result<Self> {
        // FIXME: we should probably take a lock on this file, to prevent anybody else to write to
        // it.
        let file_end = file.metadata()?.len();

        let header = if file_end == 0 {
            let log_id = Uuid::new_v4();
            LogFileHeader {
                version: 2.into(),
                start_frame_no: 0.into(),
                magic: WAL_MAGIC.into(),
                page_size: (LIBSQL_PAGE_SIZE as i32).into(),
                start_checksum: 0.into(),
                log_id: log_id.as_u128().into(),
                frame_count: 0.into(),
                sqld_version: Version::current().0.map(Into::into),
            }
        } else {
            Self::read_header(&file)?
        };

        let encryption_buf = if encryption.is_some() {
            BytesMut::with_capacity(LIBSQL_PAGE_SIZE as usize)
        } else {
            BytesMut::new()
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
            encryption,
            encryption_buf,
        };

        if file_end == 0 {
            this.write_header()?;
        } else if let Some(last_commited) = this.last_commited_frame_no() {
            // file is not empty, the starting checksum is the checksum from the last entry
            let last_frame = this.frame(last_commited)?;
            this.commited_checksum = last_frame.header().checksum.get();
            this.uncommitted_checksum = last_frame.header().checksum.get();
        } else {
            // file contains no entry, start with the initial checksum from the file header.
            this.commited_checksum = this.header.start_checksum.get();
            this.uncommitted_checksum = this.header.start_checksum.get();
        }

        Ok(this)
    }

    pub fn read_header(file: &File) -> anyhow::Result<LogFileHeader> {
        let mut buf = [0; size_of::<LogFileHeader>()];
        file.read_exact_at(&mut buf, 0)?;
        let header = LogFileHeader::read_from(&buf)
            .ok_or_else(|| anyhow::anyhow!("invalid log file header"))?;
        if header.magic.get() != WAL_MAGIC {
            bail!("invalid replication log header");
        }

        Ok(header)
    }

    pub fn header(&self) -> &LogFileHeader {
        &self.header
    }

    pub fn commit(&mut self) -> anyhow::Result<()> {
        self.header.frame_count += self.uncommitted_frame_count.into();
        self.uncommitted_frame_count = 0;
        self.commited_checksum = self.uncommitted_checksum;
        self.write_header()?;

        Ok(())
    }

    pub(crate) fn rollback(&mut self) {
        self.uncommitted_frame_count = 0;
        self.uncommitted_checksum = self.commited_checksum;
    }

    pub fn write_header(&mut self) -> anyhow::Result<()> {
        self.file.write_all_at(self.header.as_bytes(), 0)?;
        self.file.flush()?;

        Ok(())
    }

    /// Returns an iterator over the WAL frame headers
    pub(crate) fn frames_iter(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = anyhow::Result<Frame>> + '_> {
        let mut current_frame_offset = 0;
        Ok(std::iter::from_fn(move || {
            if current_frame_offset >= self.header.frame_count.get() {
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
        let mut current_frame_offset = self.header.frame_count.get();

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

    // NOTICE: Frames are yielded as is, without decrypting their contents. Headers are not encrypted anyway.
    pub fn into_not_decrypted_rev_stream_mut(self) -> impl Stream<Item = anyhow::Result<FrameMut>> {
        let mut current_frame_offset = self.header.frame_count.get();
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
                    move || file.lock().read_not_decrypted_frame_byte_offset_mut(read_byte_offset)
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
        let data = if let Some(encryption) = &self.encryption {
            self.encryption_buf.clear();
            self.encryption_buf.extend_from_slice(&page.data);
            encryption.encrypt(self.encryption_buf.as_mut())?;
            self.encryption_buf.as_ref()
        } else {
            &page.data
        };
        let frame = Frame::from_parts(
            &FrameHeader {
                frame_no: self.next_frame_no().into(),
                checksum: checksum.into(),
                page_no: page.page_no.into(),
                size_after: page.size_after.into(),
            },
            &data,
        );

        let byte_offset = self.next_byte_offset();
        tracing::trace!(
            "writing frame {} at offset {byte_offset}",
            frame.header().frame_no
        );
        self.file.write_all_at(frame.as_bytes(), byte_offset)?;

        self.uncommitted_frame_count += 1;
        self.uncommitted_checksum = checksum;

        Ok(())
    }

    /// offset in bytes at which to write the next frame
    fn next_byte_offset(&self) -> u64 {
        Self::absolute_byte_offset(self.header().frame_count.get() + self.uncommitted_frame_count)
    }

    fn next_frame_no(&self) -> FrameNo {
        self.header().start_frame_no.get()
            + self.header().frame_count.get()
            + self.uncommitted_frame_count
    }

    /// Returns the bytes position of the `nth` entry in the log
    fn absolute_byte_offset(nth: u64) -> u64 {
        std::mem::size_of::<LogFileHeader>() as u64 + nth * Self::FRAME_SIZE as u64
    }

    fn byte_offset(&self, id: FrameNo) -> anyhow::Result<Option<u64>> {
        if id < self.header.start_frame_no.get()
            || id > self.header.start_frame_no.get() + self.header.frame_count.get()
        {
            return Ok(None);
        }
        Ok(Self::absolute_byte_offset(id - self.header.start_frame_no.get()).into())
    }

    /// Returns bytes representing a WalFrame for frame `frame_no`
    ///
    /// If the requested frame is before the first frame in the log, or after the last frame,
    /// Ok(None) is returned.
    pub fn frame(&self, frame_no: FrameNo) -> std::result::Result<Frame, LogReadError> {
        if frame_no < self.header.start_frame_no.get() {
            return Err(LogReadError::SnapshotRequired);
        }

        if frame_no >= self.header.start_frame_no.get() + self.header.frame_count.get() {
            return Err(LogReadError::Ahead);
        }

        let frame = self.read_frame_byte_offset_mut(self.byte_offset(frame_no)?.unwrap())?;

        Ok(frame.into())
    }

    fn should_compact(&self) -> bool {
        let mut compact = false;
        compact |= self.header.frame_count.get() > self.max_log_frame_count;
        if let Some(max_log_duration) = self.max_log_duration {
            compact |= self.last_compact_instant.elapsed() > max_log_duration;
        }
        compact &= self.uncommitted_frame_count == 0;
        compact
    }

    pub fn maybe_compact(&mut self, compactor: LogCompactor, path: &Path) -> anyhow::Result<()> {
        if self.should_compact() {
            self.do_compaction(compactor, path)
        } else {
            Ok(())
        }
    }

    /// perform the log compaction.
    fn do_compaction(&mut self, compactor: LogCompactor, path: &Path) -> anyhow::Result<()> {
        assert_eq!(self.uncommitted_frame_count, 0);

        // nothing to compact
        if self.header().frame_count.get() == 0 {
            return Ok(());
        }

        tracing::info!("performing log compaction");
        // To perform the compaction, we create a new, empty file in the `to_compact` directory.
        // We will then atomically swap that file with the current log file.
        // In case of a crash, when filling the compactor job queue, if we find that we find a log
        // file that doesn't contains only a header, we can safely assume that it was from a
        // previous crash that happenned in the middle of this operation.
        let to_compact_id = Uuid::new_v4();
        let to_compact_log_path = path.join("to_compact").join(to_compact_id.to_string());
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&to_compact_log_path)?;
        let mut new_log_file = LogFile::new(
            file,
            self.max_log_frame_count,
            self.max_log_duration,
            self.encryption.clone(),
        )?;
        let new_header = LogFileHeader {
            start_frame_no: (self.header.last_frame_no().unwrap() + 1).into(),
            frame_count: 0.into(),
            start_checksum: self.commited_checksum.into(),
            ..self.header
        };
        new_log_file.header = new_header;
        new_log_file.write_header().unwrap();
        // swap old and new snapshot
        // FIXME(marin): the dest path never changes, store it somewhere.
        atomic_rename(&to_compact_log_path, path.join("wallog")).unwrap();
        let old_log_file = std::mem::replace(self, new_log_file);
        compactor.compact(old_log_file, to_compact_log_path)?;

        Ok(())
    }

    fn read_frame_byte_offset_mut(&self, offset: u64) -> anyhow::Result<FrameMut> {
        use zerocopy::FromZeroes;
        let mut frame = FrameBorrowed::new_zeroed();
        self.file.read_exact_at(frame.as_bytes_mut(), offset)?;
        if let Some(encryption) = &self.encryption {
            encryption.decrypt(frame.page_mut())?;
        }
        Ok(frame.into())
    }

    fn read_not_decrypted_frame_byte_offset_mut(&self, offset: u64) -> anyhow::Result<FrameMut> {
        use zerocopy::FromZeroes;
        let mut frame = FrameBorrowed::new_zeroed();
        self.file.read_exact_at(frame.as_bytes_mut(), offset)?;

        Ok(frame.into())
    }

    fn last_commited_frame_no(&self) -> Option<FrameNo> {
        if self.header.frame_count.get() == 0 {
            None
        } else {
            Some(self.header.start_frame_no.get() + self.header.frame_count.get() - 1)
        }
    }

    fn reset(self) -> anyhow::Result<Self> {
        let max_log_frame_count = self.max_log_frame_count;
        let max_log_duration = self.max_log_duration;
        // truncate file
        self.file.set_len(0)?;
        let encryption = self.encryption;
        Self::new(self.file, max_log_frame_count, max_log_duration, encryption)
    }

    pub fn set_encryptor(&mut self, encryption: Option<FrameEncryptor>) -> Option<FrameEncryptor> {
        std::mem::replace(&mut self.encryption, encryption)
    }
}

#[cfg(target_os = "macos")]
fn atomic_rename(p1: impl AsRef<Path>, p2: impl AsRef<Path>) -> anyhow::Result<()> {
    use std::ffi::CString;
    use std::os::unix::prelude::OsStrExt;

    use nix::libc::renamex_np;
    use nix::libc::RENAME_SWAP;

    let cp1 = CString::new(p1.as_ref().as_os_str().as_bytes())?;
    let cp2 = CString::new(p2.as_ref().as_os_str().as_bytes())?;
    unsafe {
        let ret = renamex_np(cp1.as_ptr(), cp2.as_ptr(), RENAME_SWAP);

        if ret != 0 {
            bail!(
                "failed to perform snapshot file swap {} -> {}: {ret}, errno: {}",
                p1.as_ref().display(),
                p2.as_ref().display(),
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
    .with_context(|| {
        format!(
            "failed to perform snapshot file swap {} -> {}",
            p1.as_ref().display(),
            p2.as_ref().display()
        )
    })?;

    Ok(())
}

#[derive(Debug, Clone, Copy, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::AsBytes)]
#[repr(C)]
pub struct LogFileHeader {
    /// magic number: b"SQLDWAL\0" as u64
    pub magic: lu64,
    /// Initial checksum value for the rolling CRC checksum
    /// computed with the 64 bits CRC_64_GO_ISO
    pub start_checksum: lu64,
    /// Uuid of the this log.
    pub log_id: lu128,
    /// Frame_no of the first frame in the log
    pub start_frame_no: lu64,
    /// entry count in file
    pub frame_count: lu64,
    /// Wal file version number, currently: 2
    pub version: lu32,
    /// page size: 4096
    pub page_size: li32,
    /// sqld version when creating this log
    pub sqld_version: [lu16; 4],
}

impl LogFileHeader {
    pub fn last_frame_no(&self) -> Option<FrameNo> {
        if self.start_frame_no.get() == 0 && self.frame_count.get() == 0 {
            // The log does not contain any frame yet
            None
        } else {
            Some(self.start_frame_no.get() + self.frame_count.get() - 1)
        }
    }

    // fn sqld_version(&self) -> Version {
    //     Version(self.sqld_version.map(Into::into))
    // }
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
    pub commit_timestamp_cache: moka::sync::Cache<FrameNo, DateTime<Utc>>,
    compactor: LogCompactor,
    db_path: PathBuf,
    /// a notifier channel other tasks can subscribe to, and get notified when new frames become
    /// available.
    pub new_frame_notifier: watch::Sender<Option<FrameNo>>,
    pub closed_signal: watch::Sender<bool>,
    pub auto_checkpoint: u32,
    encryptor: Option<FrameEncryptor>,
}

impl ReplicationLogger {
    pub(crate) fn open(
        db_path: &Path,
        max_log_size: u64,
        max_log_duration: Option<Duration>,
        dirty: bool,
        auto_checkpoint: u32,
        scripted_backup: Option<ScriptBackupManager>,
        namespace: NamespaceName,
        encryption_config: Option<EncryptionConfig>,
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
        let encryption = encryption_config.clone().map(FrameEncryptor::new);
        let log_file = LogFile::new(file, max_log_frame_count, max_log_duration, encryption)?;
        let header = log_file.header();

        let should_recover = if dirty {
            if data_path.try_exists()? {
                tracing::info!("Replication log is dirty, recovering from database file.");
                true
            } else {
                // there is no database; nothing to recover
                false
            }
        } else if header.version.get() < 2
        /* || header.sqld_version() != Version::current() */
        {
            tracing::info!("replication log version not compatible with current sqld version, recovering from database file.");
            true
        } else if fresh && data_path.exists() {
            tracing::info!("replication log not found, recovering from database file.");
            true
        } else {
            false
        };

        if should_recover {
            Self::recover(
                log_file,
                data_path,
                auto_checkpoint,
                scripted_backup,
                namespace,
                encryption_config,
            )
        } else {
            Self::from_log_file(
                db_path.to_path_buf(),
                log_file,
                auto_checkpoint,
                scripted_backup,
                namespace,
                encryption_config,
            )
        }
    }

    fn from_log_file(
        db_path: PathBuf,
        log_file: LogFile,
        auto_checkpoint: u32,
        scripted_backup: Option<ScriptBackupManager>,
        namespace: NamespaceName,
        encryption_config: Option<EncryptionConfig>,
    ) -> anyhow::Result<Self> {
        let header = log_file.header();
        let generation_start_frame_no = header.last_frame_no();

        let (new_frame_notifier, _) = watch::channel(generation_start_frame_no);
        unsafe {
            let conn = if cfg!(feature = "unix-excl-vfs") {
                rusqlite::Connection::open_with_flags_and_vfs(
                    db_path.join("data"),
                    rusqlite::OpenFlags::default(),
                    "unix-excl",
                )
            } else {
                rusqlite::Connection::open(db_path.join("data"))
            }?;
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

        let encryptor = encryption_config.map(FrameEncryptor::new);
        Ok(Self {
            generation: Generation::new(generation_start_frame_no.unwrap_or(0)),
            compactor: LogCompactor::new(
                &db_path,
                Uuid::from_u128(log_file.header.log_id.get()),
                scripted_backup,
                namespace,
            )?,
            log_file: RwLock::new(log_file),
            db_path,
            closed_signal,
            new_frame_notifier,
            auto_checkpoint,
            // we keep the last 100 commit transaction timestamps
            commit_timestamp_cache: moka::sync::Cache::new(*REPLICATION_LATENCY_CACHE_SIZE),
            encryptor,
        })
    }

    fn recover(
        log_file: LogFile,
        mut data_path: PathBuf,
        auto_checkpoint: u32,
        scripted_backup: Option<ScriptBackupManager>,
        namespace: NamespaceName,
        encryption_config: Option<EncryptionConfig>,
    ) -> anyhow::Result<Self> {
        // It is necessary to checkpoint before we restore the replication log, since the WAL may
        // contain pages that are not in the database file.
        checkpoint_db(&data_path)?;
        let mut log_file = log_file.reset()?;
        let snapshot_path = data_path.parent().unwrap().join("snapshots");
        // best effort, there may be no snapshots
        let _ = remove_dir_all(snapshot_path);
        let to_compact_path = data_path.parent().unwrap().join("to_compact");
        // best effort, there may nothing to compact
        let _ = remove_dir_all(to_compact_path);

        let data_file = File::open(&data_path)?;
        let size = data_path.metadata()?.len();
        assert!(
            size % LIBSQL_PAGE_SIZE == 0,
            "database file size is not a multiple of page size"
        );
        let num_page = size / LIBSQL_PAGE_SIZE;
        let mut buf = [0; LIBSQL_PAGE_SIZE as usize];
        let mut page_no = 1; // page numbering starts at 1
                             // We take the encryption implementation out to restore undecrypted frames,
                             // and later set it back in to create the replicator.
        let encryptor = log_file.set_encryptor(None);
        for i in 0..num_page {
            data_file.read_exact_at(&mut buf, i * LIBSQL_PAGE_SIZE)?;
            log_file.push_page(&WalPage {
                page_no,
                size_after: if i == num_page - 1 { num_page as _ } else { 0 },
                data: Bytes::copy_from_slice(&buf),
            })?;

            page_no += 1;
        }

        log_file.commit()?;
        log_file.set_encryptor(encryptor);

        assert!(data_path.pop());

        Self::from_log_file(
            data_path,
            log_file,
            auto_checkpoint,
            scripted_backup,
            namespace,
            encryption_config,
        )
    }

    pub fn log_id(&self) -> Uuid {
        Uuid::from_u128((self.log_file.read()).header().log_id.get())
    }

    /// Write pages to the log, without updating the file header.
    /// Returns the new frame count and checksum to commit
    pub(crate) fn write_pages(&self, pages: &[WalPage]) -> anyhow::Result<()> {
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
        iter.try_fold(wal_header.start_checksum.get(), |sum, frame| {
            let frame = frame?;
            let mut digest = CRC_64_GO_ISO.digest_with_initial(sum);
            digest.update(frame.page());
            let cs = digest.finalize();
            ensure!(
                cs == frame.header().checksum.get(),
                "invalid WAL file: invalid checksum"
            );
            Ok(cs)
        })
    }

    /// commit the current transaction and returns the new top frame number
    pub(crate) fn commit(&self) -> anyhow::Result<Option<FrameNo>> {
        let mut log_file = self.log_file.write();
        log_file.commit()?;
        if let Some(frame_no) = log_file.header().last_frame_no() {
            self.commit_timestamp_cache.insert(frame_no, Utc::now());
        }
        Ok(log_file.header().last_frame_no())
    }

    pub async fn get_snapshot_file(&self, from: FrameNo) -> anyhow::Result<Option<SnapshotFile>> {
        find_snapshot_file(&self.db_path, from, self.encryptor.clone()).await
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

        let size_after = last_frame.header().size_after.get();
        assert!(size_after != 0);

        log_file.do_compaction(self.compactor.clone(), &self.db_path)?;
        Ok(true)
    }

    pub(crate) fn compactor(&self) -> &LogCompactor {
        &self.compactor
    }

    pub(crate) fn db_path(&self) -> &Path {
        &self.db_path
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
        let conn = if cfg!(feature = "unix-excl-vfs") {
            rusqlite::Connection::open_with_flags_and_vfs(
                data_path,
                rusqlite::OpenFlags::default(),
                "unix-excl",
            )
        } else {
            rusqlite::Connection::open(data_path)
        }?;
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
    use std::collections::HashSet;

    use libsql_sys::wal::{Sqlite3WalManager, WalManager};

    use super::*;
    use crate::connection::libsql::open_conn;
    use crate::replication::primary::replication_logger_wal::ReplicationLoggerWalWrapper;
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
            None,
            "test".into(),
            None,
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
            assert_eq!(frame.header().page_no.get(), i as u32);
            assert!(frame.page().iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            log_file.header.start_frame_no.get() + log_file.header.frame_count.get(),
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
            None,
            "test".into(),
            None,
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
            None,
            "test".into(),
            None,
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
        let mut log_file = LogFile::new(f, 100, None, None).unwrap();
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

    #[tokio::test]
    #[cfg(feature = "encryption")]
    async fn log_with_encryption() {
        let tmp = tempfile::tempdir().unwrap();
        let logger = ReplicationLogger::open(
            tmp.path(),
            100000000,
            None,
            false,
            100000,
            None,
            "test".into(),
            None,
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
            assert_eq!(frame.header().page_no.get(), i as u32);
            assert!(frame.page().iter().all(|x| i as u8 == *x));
        }

        assert_eq!(
            log_file.header.start_frame_no.get() + log_file.header.frame_count.get(),
            10
        );

        // The file contents do not contain raw data when read directly - it's encrypted
        let file = File::open(tmp.path().join("wallog")).unwrap();
        for i in 0..10 {
            let mut buf = [0; 4096];
            file.read_exact_at(&mut buf, i * 4096).unwrap();
            assert!(!buf.iter().all(|x| i as u8 == *x));
        }
        // When we read via the log file API though, we get the decrypted data
        for i in 0..10 {
            let frame = log_file.frame(i).unwrap();
            assert_eq!(frame.header().page_no.get(), i as u32);
            assert!(frame.page().iter().all(|x| i as u8 == *x));
        }
    }

    #[tokio::test]
    async fn savepoint_and_rollback() {
        let tmp = tempfile::tempdir().unwrap();
        let logger = Arc::new(
            ReplicationLogger::open(
                tmp.path(),
                100000000,
                None,
                false,
                100000,
                None,
                "test".into(),
                None,
            )
            .unwrap(),
        );
        let mut conn = open_conn(
            tmp.path(),
            Sqlite3WalManager::default().wrap(ReplicationLoggerWalWrapper::new(logger)),
            None,
            None,
        )
        .unwrap();
        conn.execute("BEGIN", ()).unwrap();

        conn.execute("CREATE TABLE test (x)", ()).unwrap();
        let mut savepoint = conn.savepoint().unwrap();
        // try to write a few pages
        for i in 0..10000 {
            savepoint
                .execute(&format!("INSERT INTO test values ('foobar{i}')"), ())
                .unwrap();
            // force a flush
            savepoint.cache_flush().unwrap();
        }

        // rollback savepoint and write a singular value
        savepoint.rollback().unwrap();
        drop(savepoint);

        conn.execute("INSERT INTO test VALUES (42)", ()).unwrap();
        conn.execute("COMMIT", ()).unwrap();

        // now we restore from the log and make sure the two db are consistent.
        let tmp2 = tempfile::tempdir().unwrap();
        let f = File::open(tmp.path().join("wallog")).unwrap();
        let logfile = LogFile::new(f, 1000000000, None, None).unwrap();
        let mut seen = HashSet::new();
        let mut new_db_file = File::create(tmp2.path().join("data")).unwrap();
        for frame in logfile.rev_frames_iter_mut().unwrap() {
            let frame = frame.unwrap();
            let page_no = frame.header().page_no;
            if !seen.contains(&page_no) {
                seen.insert(page_no);
                new_db_file
                    .write_all_at(frame.page(), (page_no.get() as u64 - 1) * LIBSQL_PAGE_SIZE)
                    .unwrap();
            }
        }

        new_db_file.flush().unwrap();

        let conn2 = open_conn(tmp2.path(), Sqlite3WalManager::new(), None, None).unwrap();

        conn2
            .query_row("SELECT count(*) FROM test", (), |row| {
                assert_eq!(row.get_ref(0).unwrap().as_i64().unwrap(), 1);
                Ok(())
            })
            .unwrap();

        conn2
            .pragma_query(None, "page_count", |r| {
                assert_eq!(r.get_ref(0).unwrap().as_i64().unwrap(), 2);
                Ok(())
            })
            .unwrap();

        conn.query_row("SELECT count(*) FROM test", (), |row| {
            assert_eq!(row.get_ref(0).unwrap().as_i64().unwrap(), 1);
            Ok(())
        })
        .unwrap();
    }
}
