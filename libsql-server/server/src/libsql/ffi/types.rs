///! Typedefs for virtual function signatures.
use std::ffi::{c_char, c_void};

use super::{libsql_wal_methods, sqlite3_file, sqlite3_vfs, PgHdr, Wal};

// WAL methods
pub type XWalLimitFn = extern "C" fn(wal: *mut Wal, limit: i64);
pub type XWalBeginReadTransactionFn = extern "C" fn(wal: *mut Wal, changed: *mut i32) -> i32;
pub type XWalEndReadTransaction = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalFindFrameFn = extern "C" fn(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32;
pub type XWalReadFrameFn =
    extern "C" fn(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32;
pub type XWalDbSizeFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalBeginWriteTransactionFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalEndWriteTransactionFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalSavepointFn = extern "C" fn(wal: *mut Wal, wal_data: *mut u32);
pub type XWalSavePointUndoFn = extern "C" fn(wal: *mut Wal, wal_data: *mut u32) -> i32;
pub type XWalCheckpointFn = extern "C" fn(
    wal: *mut Wal,
    db: *mut c_void,
    emode: i32,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    busy_arg: *const c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
    frames_in_wal: *mut i32,
    backfilled_frames: *mut i32,
) -> i32;
pub type XWalCallbackFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalExclusiveModeFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalHeapMemoryFn = extern "C" fn(wal: *mut Wal) -> i32;
pub type XWalFileFn = extern "C" fn(wal: *mut Wal) -> *const c_void;
pub type XWalDbFn = extern "C" fn(wal: *mut Wal, db: *const c_void);
pub type XWalPathNameLenFn = extern "C" fn(orig_len: i32) -> i32;
pub type XWalGetPathNameFn = extern "C" fn(buf: *mut u8, orig: *const u8, orig_len: i32);
pub type XWallPreMainDbOpen =
    extern "C" fn(methods: *mut libsql_wal_methods, path: *const i8) -> i32;
pub type XWalOpenFn = extern "C" fn(
    vfs: *const sqlite3_vfs,
    file: *mut sqlite3_file,
    wal_name: *const c_char,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32;
pub type XWalCloseFn = extern "C" fn(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: i32,
    z_buf: *mut u8,
) -> i32;
pub type XWalFrameFn = extern "C" fn(
    wal: *mut Wal,
    page_size: u32,
    page_headers: *mut PgHdr,
    size_after: i32,
    is_commit: i32,
    sync_flags: i32,
) -> i32;
pub type XWalUndoFn = extern "C" fn(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32;

// io methods
pub type XAccessFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, flags: i32, res: *mut i32) -> i32;
pub type XDeleteFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32;
pub type XFullPathNameFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32;
pub type XOpenFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    name: *const i8,
    file: *mut sqlite3_file,
    flags: i32,
    out_flags: *mut i32,
) -> i32;
pub type XDlOpenFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8) -> *const c_void;
pub type XDlErrorFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, msg: *mut u8);
pub type XDlSymFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    arg: *mut c_void,
    symbol: *const u8,
) -> unsafe extern "C" fn();
pub type XDlCloseFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void);
pub type XRandomnessFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut u8) -> i32;
pub type XSleepFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, ms: i32) -> i32;
pub type XCurrentTimeFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut f64) -> i32;
pub type XGetLastErrorFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, buf: *mut u8) -> i32;
pub type XCurrentTimeInt64 = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut i64) -> i32;
pub type XCloseFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32;
pub type XReadFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *mut u8, n: i32, off: i64) -> i32;
pub type XWriteFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *const u8, n: i32, off: i64) -> i32;
pub type XTruncateFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: i64) -> i32;
pub type XSyncFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, flags: i32) -> i32;
pub type XFileSizeFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: *mut i64) -> i32;
pub type XLockFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32;
pub type XUnlockFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32;
pub type XCheckReservedLockFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, res: *mut i32) -> i32;
pub type XFileControlFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, op: i32, arg: *mut c_void) -> i32;
pub type XSectorSizeFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32;
pub type XDeviceCharacteristicsFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32;
