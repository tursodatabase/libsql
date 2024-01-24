use std::ffi::{c_char, c_int, c_longlong, c_void, CStr};
use std::num::NonZeroU32;
use std::ptr::null;

use libsql_ffi::{
    libsql_wal, libsql_wal_manager, libsql_wal_methods, sqlite3, sqlite3_file, sqlite3_vfs,
    wal_impl, wal_manager_impl, Error, PgHdr, SQLITE_CHECKPOINT_FULL, SQLITE_CHECKPOINT_PASSIVE,
    SQLITE_CHECKPOINT_RESTART, SQLITE_CHECKPOINT_TRUNCATE, SQLITE_OK, WAL_SAVEPOINT_NDATA,
};

use crate::wal::{BusyHandler, CheckpointCallback, CheckpointMode, UndoHandler};

use super::{PageHeaders, Sqlite3Db, Sqlite3File, Vfs, Wal, WalManager};

// Construct a libsql_wal instance from a pointer to a Wal. This pointer must be valid until a call
// to WalManager::close
pub(crate) fn construct_libsql_wal<W: Wal>(wal: *mut W) -> libsql_wal {
    libsql_wal {
        methods: libsql_wal_methods {
            iVersion: 1,
            xLimit: Some(limit::<W>),
            xBeginReadTransaction: Some(begin_read_transaction::<W>),
            xEndReadTransaction: Some(end_read_transaction::<W>),
            xFindFrame: Some(find_frame::<W>),
            xReadFrame: Some(read_frame::<W>),
            xDbsize: Some(db_size::<W>),
            xBeginWriteTransaction: Some(begin_write_transaction::<W>),
            xEndWriteTransaction: Some(end_write_transaction::<W>),
            xUndo: Some(undo::<W>),
            xSavepoint: Some(savepoint::<W>),
            xSavepointUndo: Some(savepoint_undo::<W>),
            xFrames: Some(frames::<W>),
            xCheckpoint: Some(checkpoint::<W>),
            xCallback: Some(callback::<W>),
            xExclusiveMode: Some(exclusive_mode::<W>),
            xHeapMemory: Some(heap_memory::<W>),
            xSnapshotGet: None,
            xSnapshotOpen: None,
            xSnapshotRecover: None,
            xSnapshotCheck: None,
            xSnapshotUnlock: None,
            xFramesize: None,
            xFile: None, // TODO: not all wal are single file based
            xWriteLock: None,
            xDb: Some(db::<W>),
        },
        pData: wal as *mut _,
    }
}

/// Turn a `WalManager` into a `libsql_wal_manager`.
/// The caller is responsible for deallocating `libsql_wal_manager.pData`
pub fn make_wal_manager<T: WalManager>(wal_manager: T) -> libsql_wal_manager {
    libsql_wal_manager {
        bUsesShm: wal_manager.use_shared_memory() as _,
        xOpen: Some(open::<T>),
        xClose: Some(close::<T>),
        xLogDestroy: Some(log_destroy::<T>),
        xLogExists: Some(log_exists::<T>),
        xDestroy: Some(destroy_wal_manager::<T>),
        pData: Box::into_raw(Box::new(wal_manager)) as *mut _,
    }
}

// FFI functions mapping C traits to function pointers.

pub unsafe extern "C" fn open<T: WalManager>(
    wal_manager: *mut wal_manager_impl,
    vfs: *mut sqlite3_vfs,
    db_file: *mut sqlite3_file,
    no_shm_mode: c_int,
    max_size: c_longlong,
    db_path: *const c_char,
    out_wal: *mut libsql_wal,
) -> c_int {
    let this = &*(wal_manager as *mut T);
    let mut vfs = Vfs { vfs };
    let db_path = CStr::from_ptr(db_path);
    let mut file = Sqlite3File { inner: db_file };

    match this.open(
        &mut vfs,
        &mut file,
        no_shm_mode as _,
        max_size as _,
        db_path,
    ) {
        Ok(wal) => {
            let wal = Box::into_raw(Box::new(wal));
            *out_wal = construct_libsql_wal(wal);
            SQLITE_OK
        }
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn close<T: WalManager>(
    wal_manager: *mut wal_manager_impl,
    wal: *mut wal_impl,
    db: *mut sqlite3,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int {
    let this = &*(wal_manager as *mut T);
    let mut wal = Box::from_raw(wal as *mut T::Wal);
    let scratch = if z_buf.is_null() {
        None
    } else {
        Some(std::slice::from_raw_parts_mut(z_buf, n_buf as usize))
    };
    let mut db = Sqlite3Db { inner: db };

    match this.close(&mut wal, &mut db, sync_flags, scratch) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn log_destroy<T: WalManager>(
    wal_manager: *mut wal_manager_impl,
    vfs: *mut sqlite3_vfs,
    db_path: *const c_char,
) -> c_int {
    let this = &*(wal_manager as *mut T);
    let db_path = CStr::from_ptr(db_path);
    let mut vfs = Vfs { vfs };
    match this.destroy_log(&mut vfs, db_path) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn log_exists<T: WalManager>(
    wal_manager: *mut wal_manager_impl,
    vfs: *mut sqlite3_vfs,
    db_path: *const c_char,
    exists: *mut c_int,
) -> c_int {
    let this = &*(wal_manager as *mut T);
    let db_path = CStr::from_ptr(db_path);
    let mut vfs = Vfs { vfs };
    match this.log_exists(&mut vfs, db_path) {
        Ok(res) => {
            *exists = res as _;
            SQLITE_OK
        }
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn destroy_wal_manager<T: WalManager>(wal_manager: *mut wal_manager_impl) {
    let this = Box::from_raw(wal_manager as *mut T);
    this.destroy();
}

pub unsafe extern "C" fn limit<T: Wal>(wal: *mut wal_impl, limit: i64) {
    let this = &mut (*(wal as *mut T));
    this.limit(limit);
}

pub unsafe extern "C" fn begin_read_transaction<T: Wal>(
    wal: *mut wal_impl,
    changed: *mut i32,
) -> i32 {
    let this = &mut (*(wal as *mut T));
    match this.begin_read_txn() {
        Ok(res) => {
            *changed = res as i32;
            SQLITE_OK
        }
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn end_read_transaction<T: Wal>(wal: *mut wal_impl) {
    let this = &mut (*(wal as *mut T));
    this.end_read_txn();
}

pub unsafe extern "C" fn find_frame<T: Wal>(
    wal: *mut wal_impl,
    pgno: u32,
    frame: *mut u32,
) -> c_int {
    let this = &mut (*(wal as *mut T));
    match this.find_frame(NonZeroU32::new(pgno).expect("invalid page number")) {
        Ok(fno) => {
            *frame = fno.map(|x| x.get()).unwrap_or(0);
            SQLITE_OK
        }
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn read_frame<T: Wal>(
    wal: *mut wal_impl,
    frame: u32,
    n_out: c_int,
    p_out: *mut u8,
) -> i32 {
    let this = &mut (*(wal as *mut T));
    let buffer = std::slice::from_raw_parts_mut(p_out, n_out as usize);
    match this.read_frame(
        NonZeroU32::new(frame).expect("invalid frame number"),
        buffer,
    ) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn db_size<T: Wal>(wal: *mut wal_impl) -> u32 {
    let this = &mut (*(wal as *mut T));
    this.db_size()
}

pub unsafe extern "C" fn begin_write_transaction<T: Wal>(wal: *mut wal_impl) -> i32 {
    let this = &mut (*(wal as *mut T));
    match this.begin_write_txn() {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn end_write_transaction<T: Wal>(wal: *mut wal_impl) -> i32 {
    let this = &mut (*(wal as *mut T));
    match this.end_write_txn() {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn undo<T: Wal>(
    wal: *mut wal_impl,
    func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
    undo_ctx: *mut c_void,
) -> i32 {
    let this = &mut (*(wal as *mut T));
    struct SqliteUndoHandler {
        data: *mut c_void,
        f: unsafe extern "C" fn(busy_param: *mut c_void, page_no: u32) -> c_int,
    }

    impl UndoHandler for SqliteUndoHandler {
        fn handle_undo(&mut self, page_no: u32) -> Result<(), libsql_ffi::Error> {
            let rc = unsafe { (self.f)(self.data, page_no) };
            if rc != 0 {
                Err(libsql_ffi::Error::new(rc))
            } else {
                Ok(())
            }
        }
    }

    let mut undo_handler = func.map(|f| SqliteUndoHandler { data: undo_ctx, f });

    match this.undo(undo_handler.as_mut()) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn savepoint<T: Wal>(wal: *mut wal_impl, wal_data: *mut u32) {
    let this = &mut (*(wal as *mut T));
    let data = std::slice::from_raw_parts_mut(wal_data, WAL_SAVEPOINT_NDATA as usize);
    this.savepoint(data);
}

pub unsafe extern "C" fn savepoint_undo<T: Wal>(wal: *mut wal_impl, wal_data: *mut u32) -> i32 {
    let this = &mut (*(wal as *mut T));
    let data = std::slice::from_raw_parts_mut(wal_data, WAL_SAVEPOINT_NDATA as usize);
    match this.savepoint_undo(data) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn frames<T: Wal>(
    wal: *mut wal_impl,
    page_size: c_int,
    page_headers: *mut PgHdr,
    size_after: u32,
    is_commit: c_int,
    sync_flags: c_int,
    out_commited_frames: *mut c_int,
) -> c_int {
    let this = &mut (*(wal as *mut T));
    let mut headers = PageHeaders {
        inner: page_headers,
    };
    match this.insert_frames(
        page_size,
        &mut headers,
        size_after,
        is_commit != 0,
        sync_flags,
    ) {
        Ok(n) => {
            if !out_commited_frames.is_null() {
                unsafe {
                    *out_commited_frames = n as _;
                }
            }
            SQLITE_OK
        }
        Err(code) => code.extended_code,
    }
}

#[tracing::instrument(skip(wal, db))]
pub unsafe extern "C" fn checkpoint<T: Wal>(
    wal: *mut wal_impl,
    db: *mut libsql_ffi::sqlite3,
    emode: c_int,
    busy_handler: Option<unsafe extern "C" fn(busy_param: *mut c_void) -> c_int>,
    busy_arg: *mut c_void,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
    frames_in_wal_out: *mut c_int,
    checkpointed_frames_out: *mut c_int,
    checkpoint_cb: Option<
        unsafe extern "C" fn(
            data: *mut c_void,
            max_safe_frame_no: c_int,
            page: *const u8,
            page_size: c_int,
            page_no: c_int,
            frame_no: c_int,
        ) -> c_int,
    >,
    checkpoint_cb_data: *mut c_void,
) -> i32 {
    let this = &mut (*(wal as *mut T));
    struct SqliteBusyHandler {
        data: *mut c_void,
        f: unsafe extern "C" fn(busy_param: *mut c_void) -> c_int,
    }

    impl BusyHandler for SqliteBusyHandler {
        fn handle_busy(&mut self) -> bool {
            unsafe { (self.f)(self.data) != 0 }
        }
    }

    struct SqliteCheckpointCallback {
        data: *mut c_void,
        f: unsafe extern "C" fn(
            data: *mut c_void,
            max_safe_frame_no: c_int,
            page: *const u8,
            page_size: c_int,
            page_no: c_int,
            frame_no: c_int,
        ) -> c_int,
    }

    impl CheckpointCallback for SqliteCheckpointCallback {
        fn frame(
            &mut self,
            max_safe_frame_no: u32,
            page: &[u8],
            page_no: NonZeroU32,
            frame_no: NonZeroU32,
        ) -> crate::wal::Result<()> {
            unsafe {
                let rc = (self.f)(
                    self.data,
                    max_safe_frame_no as _,
                    page.as_ptr(),
                    page.len() as _,
                    page_no.get() as _,
                    frame_no.get() as _,
                );
                if rc == 0 {
                    Ok(())
                } else {
                    Err(Error::new(rc))
                }
            }
        }

        fn finish(&mut self) -> crate::wal::Result<()> {
            unsafe {
                let rc = (self.f)(self.data, 0, null(), 0, 0, 0);
                if rc == 0 {
                    Ok(())
                } else {
                    Err(Error::new(rc))
                }
            }
        }
    }

    let mut busy_handler = busy_handler.map(|f| SqliteBusyHandler { data: busy_arg, f });
    let mut checkpoint_cb = checkpoint_cb.map(|f| SqliteCheckpointCallback {
        f,
        data: checkpoint_cb_data,
    });
    let buf = std::slice::from_raw_parts_mut(z_buf, n_buf as usize);

    let mode = match emode {
        e if e == SQLITE_CHECKPOINT_TRUNCATE => CheckpointMode::Truncate,
        e if e == SQLITE_CHECKPOINT_FULL => CheckpointMode::Full,
        e if e == SQLITE_CHECKPOINT_PASSIVE => CheckpointMode::Passive,
        e if e == SQLITE_CHECKPOINT_RESTART => CheckpointMode::Restart,
        _ => panic!("invalid checkpoint mode"),
    };

    let in_wal = (!frames_in_wal_out.is_null()).then_some(&mut *frames_in_wal_out);
    let backfilled = (!checkpointed_frames_out.is_null()).then_some(&mut *checkpointed_frames_out);
    let mut db = Sqlite3Db { inner: db };
    match this.checkpoint(
        &mut db,
        mode,
        busy_handler.as_mut().map(|x| x as _),
        sync_flags as _,
        buf,
        checkpoint_cb.as_mut().map(|x| x as _),
        in_wal,
        backfilled,
    ) {
        Ok(()) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn callback<T: Wal>(wal: *mut wal_impl) -> c_int {
    let this = &mut (*(wal as *mut T));
    this.callback()
}

pub unsafe extern "C" fn exclusive_mode<T: Wal>(wal: *mut wal_impl, op: c_int) -> c_int {
    let this = &mut (*(wal as *mut T));
    match this.exclusive_mode(op) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn heap_memory<T: Wal>(wal: *mut wal_impl) -> c_int {
    let this = &mut (*(wal as *mut T));
    this.uses_heap_memory() as _
}

pub unsafe extern "C" fn db<T: Wal>(wal: *mut wal_impl, db: *mut libsql_ffi::sqlite3) {
    let this = &mut (*(wal as *mut T));
    let mut db = Sqlite3Db { inner: db };
    this.set_db(&mut db);
}
