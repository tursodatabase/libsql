use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;

pub use crate::ffi::Error;
use crate::ffi::*;

pub use sqlite3_wal::{Sqlite3Wal, Sqlite3WalManager};

pub mod either;
pub(crate) mod ffi;
mod sqlite3_wal;
pub mod wrapper;

pub type Result<T, E = Error> = std::result::Result<T, E>;
pub use ffi::make_wal_manager;

use self::wrapper::{WalWrapper, WrapWal};

pub trait WalManager {
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
        scratch: Option<&mut [u8]>,
    ) -> Result<()>;

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()>;
    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool>;

    fn destroy(self)
    where
        Self: Sized;

    fn wrap<U>(self, wrapper: U) -> WalWrapper<U, Self>
    where
        U: WrapWal<Self::Wal> + Clone,
        Self: Sized,
    {
        WalWrapper::new(wrapper, self)
    }
}

/// Wrapper type around `*mut sqlite3`, to seal the pointer from extern usage.
pub struct Sqlite3Db {
    inner: *mut sqlite3,
}

impl Sqlite3Db {
    pub fn as_ptr(&mut self) -> *mut sqlite3 {
        self.inner
    }
}

/// Wrapper type around `*mut sqlite3_file`, to seal the pointer from extern usage.
#[repr(transparent)]
pub struct Sqlite3File {
    inner: *mut sqlite3_file,
}

impl Sqlite3File {
    pub(crate) fn as_ptr(&mut self) -> *mut sqlite3_file {
        self.inner
    }

    pub fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        unsafe {
            assert!(!self.inner.is_null());
            let inner = &mut *self.inner;
            assert!(!inner.pMethods.is_null());
            let io_methods = &*inner.pMethods;

            let read = io_methods.xRead.unwrap();

            let rc = read(
                self.inner,
                buf.as_mut_ptr() as *mut _,
                buf.len() as _,
                offset as _,
            );

            if rc == 0 {
                Ok(())
            } else {
                Err(Error::new(rc))
            }
        }
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
    pub(crate) fn as_ptr(&self) -> *const libsql_pghdr {
        self.inner
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut libsql_pghdr {
        self.inner
    }

    /// # Safety
    /// caller must ensure the headers list validity.
    pub unsafe fn from_raw(inner: *mut libsql_pghdr) -> Self {
        Self { inner }
    }

    pub fn iter(&self) -> PageHdrIter {
        // TODO: move LIBSQL_PAGE_SIZE
        PageHdrIter::new(self.as_ptr(), 4096)
    }
}

pub trait BusyHandler {
    // Handle busy, and returns whether a retry should be performed
    fn handle_busy(&mut self) -> bool;
}

impl<F> BusyHandler for F
where
    F: FnMut() -> bool,
{
    fn handle_busy(&mut self) -> bool {
        (self)()
    }
}

pub trait UndoHandler {
    fn handle_undo(&mut self, page_no: u32) -> Result<()>;
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy)]
#[repr(i32)]
pub enum CheckpointMode {
    Passive = SQLITE_CHECKPOINT_PASSIVE,
    Full = SQLITE_CHECKPOINT_FULL,
    Restart = SQLITE_CHECKPOINT_RESTART,
    Truncate = SQLITE_CHECKPOINT_TRUNCATE,
}

pub trait CheckpointCallback {
    fn frame(
        &mut self,
        max_safe_frame_no: u32,
        frame: &[u8],
        page_no: NonZeroU32,
        frame_no: NonZeroU32,
    ) -> Result<()>;
    fn finish(&mut self) -> Result<()>;
}

pub trait Wal {
    /// Set the WAL limit in pages
    fn limit(&mut self, size: i64);
    /// start a read transaction. Returns whether the in-memory page cache should be invalidated.
    fn begin_read_txn(&mut self) -> Result<bool>;
    fn end_read_txn(&mut self);

    /// locate the frame containing page `page_no`
    fn find_frame(&mut self, page_no: NonZeroU32) -> Result<Option<NonZeroU32>>;
    /// reads frame `frame_no` into buffer.
    fn read_frame(&mut self, frame_no: NonZeroU32, buffer: &mut [u8]) -> Result<()>;

    fn db_size(&self) -> u32;

    fn begin_write_txn(&mut self) -> Result<()>;
    fn end_write_txn(&mut self) -> Result<()>;

    fn undo<U: UndoHandler>(&mut self, handler: Option<&mut U>) -> Result<()>;

    fn savepoint(&mut self, rollback_data: &mut [u32]);
    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()>;

    /// Insert frames in the wal. On commit, returns the number of inserted frames for that
    /// transaction, or 0 for non-commit calls.
    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<usize>;

    /// Returns the number of frames in the log and the number of checkpointed frames in the WAL.
    fn checkpoint(
        &mut self,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
        in_wal: Option<&mut i32>,
        backfilled: Option<&mut i32>,
    ) -> Result<()>;

    fn exclusive_mode(&mut self, op: c_int) -> Result<()>;
    fn uses_heap_memory(&self) -> bool;
    fn set_db(&mut self, db: &mut Sqlite3Db);

    /// Return the value to pass to a sqlite3_wal_hook callback, the
    /// number of frames in the WAL at the point of the last commit since
    /// sqlite3WalCallback() was called.  If no commits have occurred since
    /// the last call, then return 0.
    fn callback(&self) -> i32;

    fn frames_in_wal(&self) -> u32;
}
