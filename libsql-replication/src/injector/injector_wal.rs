use std::ffi::{c_int, CStr};
use std::num::NonZeroU32;

use libsql_sys::ffi::{Pager, PgHdr};
use libsql_sys::wal::{
    BusyHandler, CheckpointCallback, CheckpointMode, PageHeaders, Result, Sqlite3Db, Sqlite3File,
    Sqlite3Wal, Sqlite3WalManager, UndoHandler, Vfs, Wal, WalManager,
};

use crate::frame::FrameBorrowed;

use super::{headers::Headers, FrameBuffer};

// Those are custom error codes returned by the replicator hook.
pub const LIBSQL_INJECT_FATAL: c_int = 200;
/// Injection succeeded, left on a open txn state
pub const LIBSQL_INJECT_OK_TXN: c_int = 201;
/// Injection succeeded
pub const LIBSQL_INJECT_OK: c_int = 202;

pub struct InjectorWalManager {
    inner: Sqlite3WalManager,
    buffer: FrameBuffer,
}

impl InjectorWalManager {
    pub(crate) fn new(buffer: FrameBuffer) -> InjectorWalManager {
        Self {
            inner: Sqlite3WalManager::new(),
            buffer,
        }
    }
}

impl WalManager for InjectorWalManager {
    type Wal = InjectorWal;

    fn use_shared_memory(&self) -> bool {
        self.inner.use_shared_memory()
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> Result<Self::Wal> {
        let inner = self
            .inner
            .open(vfs, file, no_shm_mode, max_log_size, db_path)?;
        Ok(Self::Wal {
            inner,
            is_txn: false,
            buffer: self.buffer.clone(),
        })
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: Option<&mut [u8]>,
    ) -> Result<()> {
        self.inner.close(&mut wal.inner, db, sync_flags, scratch)
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        self.inner.destroy_log(vfs, db_path)
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        self.inner.log_exists(vfs, db_path)
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        self.inner.destroy()
    }
}

pub struct InjectorWal {
    inner: Sqlite3Wal,
    is_txn: bool,
    buffer: FrameBuffer,
}

impl Wal for InjectorWal {
    fn limit(&mut self, size: i64) {
        self.inner.limit(size)
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        self.inner.begin_read_txn()
    }

    fn end_read_txn(&mut self) {
        self.inner.end_read_txn()
    }

    fn find_frame(&mut self, page_no: NonZeroU32) -> Result<Option<NonZeroU32>> {
        self.inner.find_frame(page_no)
    }

    fn read_frame(&mut self, frame_no: NonZeroU32, buffer: &mut [u8]) -> Result<()> {
        self.inner.read_frame(frame_no, buffer)
    }

    fn db_size(&self) -> u32 {
        self.inner.db_size()
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        self.inner.begin_write_txn()
    }

    fn end_write_txn(&mut self) -> Result<()> {
        self.inner.end_write_txn()
    }

    fn undo<U: UndoHandler>(&mut self, undo_handler: Option<&mut U>) -> Result<()> {
        self.inner.undo(undo_handler)
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        self.inner.savepoint(rollback_data)
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        self.inner.savepoint_undo(rollback_data)
    }

    fn insert_frames(
        &mut self,
        page_size: c_int,
        orig_page_headers: &mut PageHeaders,
        _size_after: u32,
        _is_commit: bool,
        sync_flags: c_int,
    ) -> Result<usize> {
        self.is_txn = true;
        let mut buffer = self.buffer.lock();

        {
            // NOTICE: unwrap() is safe, because we never call insert_frames() without any frames
            let page_hdr = orig_page_headers.iter().current_ptr();
            let pager = unsafe { &*page_hdr }.pPager;
            let (mut headers, size_after) = make_page_header(pager, buffer.iter().map(|f| &**f));
            let mut page_headers = unsafe { PageHeaders::from_raw(headers.as_mut_ptr()) };
            if let Err(e) = self.inner.insert_frames(
                page_size,
                &mut page_headers,
                size_after,
                size_after != 0,
                sync_flags,
            ) {
                tracing::error!("fatal replication error: failed to apply pages: {e}");
                return Err(libsql_sys::wal::Error::new(LIBSQL_INJECT_FATAL));
            }

            drop(headers);
            if size_after != 0 {
                self.is_txn = false;
            }
        }
        tracing::trace!("applied frame batch");

        buffer.clear();

        if !self.is_txn {
            Err(libsql_sys::wal::Error::new(LIBSQL_INJECT_OK))
        } else {
            Err(libsql_sys::wal::Error::new(LIBSQL_INJECT_OK_TXN))
        }
    }

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
    ) -> Result<()> {
        self.inner.checkpoint(
            db,
            mode,
            busy_handler,
            sync_flags,
            buf,
            checkpoint_cb,
            in_wal,
            backfilled,
        )
    }

    fn exclusive_mode(&mut self, op: c_int) -> Result<()> {
        self.inner.exclusive_mode(op)
    }

    fn uses_heap_memory(&self) -> bool {
        self.inner.uses_heap_memory()
    }

    fn set_db(&mut self, db: &mut Sqlite3Db) {
        self.inner.set_db(db)
    }

    fn callback(&self) -> i32 {
        self.inner.callback()
    }

    fn frames_in_wal(&self) -> u32 {
        self.inner.frames_in_wal()
    }
}

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
/// return (headers, last_frame_no, size_after)
fn make_page_header<'a>(
    pager: *mut Pager,
    frames: impl Iterator<Item = &'a FrameBorrowed>,
) -> (Headers<'a>, u32) {
    let mut first_pg: *mut PgHdr = std::ptr::null_mut();
    let mut current_pg;
    let mut size_after = 0;

    let mut headers_count = 0;
    let mut prev_pg: *mut PgHdr = std::ptr::null_mut();
    let mut frames = frames.peekable();
    while let Some(frame) = frames.next() {
        // the last frame in a batch marks the end of the txn
        if frames.peek().is_none() {
            size_after = frame.header().size_after.get();
        }

        let page = PgHdr {
            pPage: std::ptr::null_mut(),
            pData: frame.page().as_ptr() as _,
            pExtra: std::ptr::null_mut(),
            pCache: std::ptr::null_mut(),
            pDirty: std::ptr::null_mut(),
            pPager: pager,
            pgno: frame.header().page_no.get(),
            pageHash: 0,
            flags: 0x02, // PGHDR_DIRTY - it works without the flag, but why risk it
            nRef: 0,
            pDirtyNext: std::ptr::null_mut(),
            pDirtyPrev: std::ptr::null_mut(),
        };
        headers_count += 1;
        current_pg = Box::into_raw(Box::new(page));
        if first_pg.is_null() {
            first_pg = current_pg;
        }
        if !prev_pg.is_null() {
            unsafe {
                (*prev_pg).pDirty = current_pg;
            }
        }
        prev_pg = current_pg;
    }

    tracing::trace!("built {headers_count} page headers");

    let headers = unsafe { Headers::new(first_pg) };
    (headers, size_after)
}
