use std::ffi::{c_int, c_void, CStr};
use std::mem::MaybeUninit;

use libsql_ffi::{
    libsql_create_wal, libsql_wal, sqlite3_create_wal, sqlite3_wal, Error, SQLITE_OK,
    WAL_SAVEPOINT_NDATA,
};

use super::{
    BusyHandler, CheckpointMode, CreateWal, PageHeaders, Result, Sqlite3Db, Sqlite3File,
    UndoHandler, Vfs, Wal,
};

/// SQLite3 default create_wal implementation.
#[derive(Clone, Copy)]
pub struct CreateSqlite3Wal {
    inner: libsql_create_wal,
}

/// Safety: the create pointer is an immutable global pointer
unsafe impl Send for CreateSqlite3Wal {}
unsafe impl Sync for CreateSqlite3Wal {}

impl CreateSqlite3Wal {
    pub fn new() -> Self {
        Self {
            inner: unsafe { sqlite3_create_wal },
        }
    }
}

impl Default for CreateSqlite3Wal {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateWal for CreateSqlite3Wal {
    type Wal = Sqlite3Wal;

    fn use_shared_memory(&self) -> bool {
        self.inner.bUsesShm != 0
    }

    fn open(
        &self,
        vfs: &mut Vfs,
        file: &mut Sqlite3File,
        no_shm_mode: c_int,
        max_log_size: i64,
        db_path: &CStr,
    ) -> Result<Self::Wal> {
        let mut wal: MaybeUninit<libsql_wal> = MaybeUninit::uninit();
        let rc = unsafe {
            (self.inner.xOpen.unwrap())(
                self.inner.pData,
                vfs.as_ptr(),
                file.as_ptr(),
                no_shm_mode,
                max_log_size,
                db_path.as_ptr(),
                wal.as_mut_ptr(),
            )
        };

        if rc != 0 {
            Err(Error::new(rc))?
        }

        let inner = unsafe { wal.assume_init() };

        Ok(Sqlite3Wal { inner })
    }

    fn close(
        &self,
        wal: &mut Self::Wal,
        db: &mut Sqlite3Db,
        sync_flags: c_int,
        scratch: &mut [u8],
    ) -> Result<()> {
        let rc = unsafe {
            (self.inner.xClose.unwrap())(
                self.inner.pData,
                wal.inner.pData,
                db.as_ptr(),
                sync_flags,
                scratch.len() as _,
                scratch.as_mut_ptr() as _,
            )
        };

        if rc != 0 {
            Err(Error::new(rc))?
        } else {
            Ok(())
        }
    }

    fn destroy_log(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<()> {
        let rc = unsafe {
            (self.inner.xLogDestroy.unwrap())(self.inner.pData, vfs.as_ptr(), db_path.as_ptr())
        };

        if rc != 0 {
            Err(Error::new(rc))?
        } else {
            Ok(())
        }
    }

    fn log_exists(&self, vfs: &mut Vfs, db_path: &CStr) -> Result<bool> {
        let mut out: c_int = 0;
        let rc = unsafe {
            (self.inner.xLogExists.unwrap())(
                self.inner.pData,
                vfs.as_ptr(),
                db_path.as_ptr(),
                &mut out,
            )
        };

        if rc != 0 {
            Err(Error::new(rc))?
        } else {
            Ok(out != 0)
        }
    }

    fn destroy(self)
    where
        Self: Sized,
    {
        unsafe { (self.inner.xDestroy.unwrap())(self.inner.pData) }
    }
}

unsafe impl Send for Sqlite3Wal {}

/// SQLite3 wal implementation
pub struct Sqlite3Wal {
    inner: libsql_wal,
}

impl Wal for Sqlite3Wal {
    fn limit(&mut self, size: i64) {
        unsafe {
            (self.inner.methods.xLimit.unwrap())(self.inner.pData, size);
        }
    }

    fn begin_read_txn(&mut self) -> Result<bool> {
        let mut out: c_int = 0;
        let rc = unsafe {
            (self.inner.methods.xBeginReadTransaction.unwrap())(
                self.inner.pData,
                &mut out as *mut _,
            )
        };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(out != 0)
        }
    }

    fn end_read_txn(&mut self) {
        unsafe {
            (self.inner.methods.xEndReadTransaction.unwrap())(self.inner.pData);
        }
    }

    fn find_frame(&mut self, page_no: u32) -> Result<u32> {
        let mut out: u32 = 0;
        let rc = unsafe {
            (self.inner.methods.xFindFrame.unwrap())(self.inner.pData, page_no, &mut out as *mut _)
        };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(out as _)
        }
    }

    fn read_frame(&mut self, frame_no: u32, buffer: &mut [u8]) -> Result<()> {
        let rc = unsafe {
            (self.inner.methods.xReadFrame.unwrap())(
                self.inner.pData,
                frame_no,
                buffer.len() as _,
                buffer.as_mut_ptr(),
            )
        };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn db_size(&self) -> u32 {
        unsafe { (self.inner.methods.xDbsize.unwrap())(self.inner.pData) }
    }

    fn begin_write_txn(&mut self) -> Result<()> {
        let rc = unsafe { (self.inner.methods.xBeginWriteTransaction.unwrap())(self.inner.pData) };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn end_write_txn(&mut self) -> Result<()> {
        let rc = unsafe { (self.inner.methods.xEndWriteTransaction.unwrap())(self.inner.pData) };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn undo<U: UndoHandler>(&mut self, undo_handler: Option<&mut U>) -> Result<()> {
        unsafe extern "C" fn call_handler<U: UndoHandler>(p: *mut c_void, page_no: u32) -> c_int {
            let this = &mut *(p as *mut U);
            match this.handle_undo(page_no) {
                Ok(_) => SQLITE_OK,
                Err(e) => e.extended_code,
            }
        }

        let handler = undo_handler
            .is_some()
            .then_some(call_handler::<U> as unsafe extern "C" fn(*mut c_void, u32) -> i32);
        let handler_data = undo_handler
            .map(|d| d as *mut _ as *mut _)
            .unwrap_or(std::ptr::null_mut());

        let rc =
            unsafe { (self.inner.methods.xUndo.unwrap())(self.inner.pData, handler, handler_data) };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn savepoint(&mut self, rollback_data: &mut [u32]) {
        assert_eq!(rollback_data.len(), WAL_SAVEPOINT_NDATA as usize);
        unsafe {
            (self.inner.methods.xSavepoint.unwrap())(self.inner.pData, rollback_data.as_mut_ptr());
        }
    }

    fn savepoint_undo(&mut self, rollback_data: &mut [u32]) -> Result<()> {
        assert_eq!(rollback_data.len(), WAL_SAVEPOINT_NDATA as usize);
        let rc = unsafe {
            (self.inner.methods.xSavepointUndo.unwrap())(
                self.inner.pData,
                rollback_data.as_mut_ptr(),
            )
        };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn insert_frames(
        &mut self,
        page_size: c_int,
        page_headers: &mut PageHeaders,
        size_after: u32,
        is_commit: bool,
        sync_flags: c_int,
    ) -> Result<()> {
        let rc = unsafe {
            (self.inner.methods.xFrames.unwrap())(
                self.inner.pData,
                page_size,
                page_headers.as_ptr(),
                size_after,
                is_commit as _,
                sync_flags,
            )
        };
        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn checkpoint<B: BusyHandler>(
        &mut self,
        db: &mut Sqlite3Db,
        mode: CheckpointMode,
        busy_handler: Option<&mut B>,
        sync_flags: u32,
        // temporary scratch buffer
        buf: &mut [u8],
    ) -> Result<(u32, u32)> {
        unsafe extern "C" fn call_handler<B: BusyHandler>(p: *mut c_void) -> c_int {
            let this = &mut *(p as *mut B);
            this.handle_busy() as _
        }

        let handler = busy_handler
            .is_some()
            .then_some(call_handler::<B> as unsafe extern "C" fn(*mut c_void) -> i32);
        let handler_data = busy_handler
            .map(|d| d as *mut _ as *mut _)
            .unwrap_or(std::ptr::null_mut());

        let mut out_log_num_frames: c_int = 0;
        let mut out_backfilled: c_int = 0;

        let rc = unsafe {
            (self.inner.methods.xCheckpoint.unwrap())(
                self.inner.pData,
                db.as_ptr(),
                mode as _,
                handler,
                handler_data,
                sync_flags as _,
                buf.len() as _,
                buf.as_mut_ptr(),
                &mut out_log_num_frames,
                &mut out_backfilled,
            )
        };

        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok((out_log_num_frames as _, out_backfilled as _))
        }
    }

    fn exclusive_mode(&mut self, op: c_int) -> Result<()> {
        let rc = unsafe { (self.inner.methods.xExclusiveMode.unwrap())(self.inner.pData, op) };

        if rc != 0 {
            Err(Error::new(rc))
        } else {
            Ok(())
        }
    }

    fn uses_heap_memory(&self) -> bool {
        unsafe { (self.inner.methods.xHeapMemory.unwrap())(self.inner.pData) != 0 }
    }

    fn set_db(&mut self, db: &mut Sqlite3Db) {
        unsafe {
            (self.inner.methods.xDb.unwrap())(self.inner.pData, db.as_ptr());
        }
    }

    fn callback(&self) -> i32 {
        unsafe { (self.inner.methods.xCallback.unwrap())(self.inner.pData) }
    }

    fn last_fame_index(&self) -> u32 {
        unsafe {
            let wal = &*(self.inner.pData as *const sqlite3_wal);
            wal.hdr.mxFrame
        }
    }
}
