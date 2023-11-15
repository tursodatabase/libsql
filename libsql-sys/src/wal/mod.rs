use std::ffi::{c_int, CStr};

pub use crate::ffi::Error;
use crate::ffi::*;

pub use sqlite3_wal::{CreateSqlite3Wal, Sqlite3Wal};

pub(crate) mod ffi;
mod sqlite3_wal;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait CreateWal {
    type Wal: Wal;

    fn use_shared_memory(&self) -> bool;

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> Result<Self::Wal>;
    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: &mut [u8],
    ) -> Result<()>;

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()>;
    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool>;

    fn destroy(self)
    where
        Self: Sized;
}

/// Wrapper type around `*mut sqlite3`, to seal the pointer from extern usage.
pub struct Sqlite3Db {
    inner: *mut sqlite3,
}

impl Sqlite3Db {
    pub(crate) fn as_ptr(&mut self) -> *mut sqlite3 {
        self.inner
    }
}

/// Wrapper type around `*mut sqlite3_file`, to seal the pointer from extern usage.
pub struct Sqlite3File {
    inner: *mut sqlite3_file,
}

impl Sqlite3File {
    pub(crate) fn as_ptr(&mut self) -> *mut sqlite3_file {
        self.inner
    }
}

/// Wrapper type around `*mut sqlite3_vfs`, to seal the pointer from extern usage.
pub struct Vfs {
    vfs: *mut sqlite3_vfs,
}

impl Vfs {
    pub(crate) fn as_ptr(&mut self) -> *mut sqlite3_vfs {
        self.vfs
    }
}

pub struct PageHeaders {
    inner: *mut libsql_pghdr,
}

impl PageHeaders {
    pub(crate) fn as_ptr(&mut self) -> *mut libsql_pghdr {
        self.inner
    }

    /// # Safety
    /// caller must ensure the headers list validity.
    pub unsafe fn from_raw(inner: *mut libsql_pghdr) -> Self {
        Self { inner }
    }

    /// # Safety
    /// The caller must not modify the list, unless they really know what they are doing.
    pub unsafe fn iter(&mut self) -> PageHdrIter {
        // TODO: move LIBSQL_PAGE_SIZE
        PageHdrIter::new(self.as_ptr(), 4096)
    }
}

pub trait BusyHandler {
    // Handle busy, and returns whether a retry should be performed
    fn handle_busy(&mut self) -> bool;
}

pub trait UndoHandler {
    fn handle_undo(&mut self, page_no: u32) -> Result<()>;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(i32)]
pub enum CheckpointMode {
    Passive = SQLITE_CHECKPOINT_PASSIVE,
    Full = SQLITE_CHECKPOINT_FULL,
    Restart = SQLITE_CHECKPOINT_RESTART,
    Truncate = SQLITE_CHECKPOINT_TRUNCATE,
}

pub trait Wal {
    /// Set the WAL limit in pages
    fn limit(&mut self, size: i64);
    /// start a read transaction. Returns whether the in-memory page cache should be invalidated.
    fn begin_read_txn(&mut self) -> Result<bool>;
    fn end_read_txn(&mut self);

    /// locate the frame containing page `page_no`
    fn find_frame(&mut self, page_no: u32) -> Result<u32>;
    /// reads frame `frame_no` into buffer.
    fn read_frame(&mut self, frame_no: u32, buffer: &mut [u8]) -> Result<()>;

    fn db_size(&self) -> u32;

    fn begin_write_txn(&mut self) -> Result<()>;
    fn end_write_txn(&mut self) -> Result<()>;

    fn undo<U: UndoHandler>(&mut self, handler: Option<&mut U>) -> Result<()>;

    fn savepoint(&mut self, rollback_data: &mut [u32]);
    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()>;

    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<()>;

    /// Returns the number of frames in the log and the number of checkpointed frames in the WAL.
    fn checkpoint<B: BusyHandler>(
        &mut self,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> Result<(u32, u32)>;

    fn exclusive_mode(&mut self, op: c_int) -> Result<()>;
    fn uses_heap_memory(&self) -> bool;
    fn set_db(&mut self, db: &mut Sqlite3Db);

    /// Return the value to pass to a sqlite3_wal_hook callback, the
    /// number of frames in the WAL at the point of the last commit since
    /// sqlite3WalCallback() was called.  If no commits have occurred since
    /// the last call, then return 0.
    fn callback(&self) -> i32;

    fn last_fame_index(&self) -> u32;
}
