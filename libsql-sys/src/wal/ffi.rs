use std::ffi::{c_char, c_int, c_longlong, c_void, CStr};

use libsql_ffi::{
    create_wal_impl, libsql_create_wal, libsql_wal, libsql_wal_methods, sqlite3, sqlite3_file,
    sqlite3_vfs, wal_impl, PgHdr, SQLITE_CHECKPOINT_FULL, SQLITE_CHECKPOINT_PASSIVE,
    SQLITE_CHECKPOINT_RESTART, SQLITE_CHECKPOINT_TRUNCATE, SQLITE_OK, WAL_SAVEPOINT_NDATA,
};

use crate::wal::{BusyHandler, CheckpointMode, UndoHandler};

use super::{CreateWal, PageHeaders, Sqlite3Db, Sqlite3File, Vfs, Wal};

// Construct a libsql_wal instance from a pointer to a Wal. This pointer must be valid until a call
// to CreateWal::close
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

pub(crate) fn make_create_wal<T: CreateWal>(create_wal: T) -> libsql_create_wal {
    libsql_create_wal {
        bUsesShm: create_wal.use_shared_memory() as _,
        xOpen: Some(open::<T>),
        xClose: Some(close::<T>),
        xLogDestroy: Some(log_destroy::<T>),
        xLogExists: Some(log_exists::<T>),
        xDestroy: Some(destroy_create_wal::<T>),
        pData: Box::into_raw(Box::new(create_wal)) as *mut _,
    }
}

// FFI functions mapping C traits to function pointers.

pub unsafe extern "C" fn open<T: CreateWal>(
    create_wal: *mut create_wal_impl,
    vfs: *mut sqlite3_vfs,
    db_file: *mut sqlite3_file,
    no_shm_mode: c_int,
    max_size: c_longlong,
    db_path: *const c_char,
    out_wal: *mut libsql_wal,
) -> c_int {
    let this = &*(create_wal as *mut T);
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

pub unsafe extern "C" fn close<T: CreateWal>(
    create_wal: *mut create_wal_impl,
    wal: *mut wal_impl,
    db: *mut sqlite3,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int {
    let this = &*(create_wal as *mut T);
    let mut wal = Box::from_raw(wal as *mut T::Wal);
    let scratch = std::slice::from_raw_parts_mut(z_buf, n_buf as usize);
    let mut db = Sqlite3Db { inner: db };

    match this.close(&mut wal, &mut db, sync_flags, scratch) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn log_destroy<T: CreateWal>(
    create_wal: *mut create_wal_impl,
    vfs: *mut sqlite3_vfs,
    db_path: *const c_char,
) -> c_int {
    let this = &*(create_wal as *mut T);
    let db_path = CStr::from_ptr(db_path);
    let mut vfs = Vfs { vfs };
    match this.destroy_log(&mut vfs, db_path) {
        Ok(_) => SQLITE_OK,
        Err(code) => code.extended_code,
    }
}

pub unsafe extern "C" fn log_exists<T: CreateWal>(
    create_wal: *mut create_wal_impl,
    vfs: *mut sqlite3_vfs,
    db_path: *const c_char,
    exists: *mut c_int,
) -> c_int {
    let this = &*(create_wal as *mut T);
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

pub unsafe extern "C" fn destroy_create_wal<T: CreateWal>(create_wal: *mut create_wal_impl) {
    let this = Box::from_raw(create_wal as *mut T);
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
    match this.find_frame(pgno) {
        Ok(fno) => {
            *frame = fno;
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
    match this.read_frame(frame, buffer) {
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
        Ok(_) => SQLITE_OK,
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

    let mut busy_handler = busy_handler.map(|f| SqliteBusyHandler { data: busy_arg, f });
    let buf = std::slice::from_raw_parts_mut(z_buf, n_buf as usize);

    let mode = match emode {
        e if e == SQLITE_CHECKPOINT_TRUNCATE => CheckpointMode::Truncate,
        e if e == SQLITE_CHECKPOINT_FULL => CheckpointMode::Full,
        e if e == SQLITE_CHECKPOINT_PASSIVE => CheckpointMode::Passive,
        e if e == SQLITE_CHECKPOINT_RESTART => CheckpointMode::Restart,
        _ => panic!("invalid checkpoint mode"),
    };

    let mut db = Sqlite3Db { inner: db };
    match this.checkpoint(&mut db, mode, busy_handler.as_mut(), sync_flags as _, buf) {
        Ok((frames_in_wal, backfilled_frames)) => {
            if !frames_in_wal_out.is_null() {
                *frames_in_wal_out = frames_in_wal as _;
            }
            if !checkpointed_frames_out.is_null() {
                *checkpointed_frames_out = backfilled_frames as _;
            }
            SQLITE_OK
        }
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
