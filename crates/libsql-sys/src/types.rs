//! Typedefs for virtual function signatures.
use std::ffi::{c_char, c_int, c_uint, c_void};

pub type Wal = crate::ffi::libsql_wal;
use crate::ffi::{libsql_wal_methods, sqlite3, sqlite3_file, sqlite3_vfs, PgHdr};

// WAL methods
pub type XWalLimitFn = extern "C" fn(wal: *mut Wal, limit: i64);
pub type XWalBeginReadTransactionFn = extern "C" fn(wal: *mut Wal, changed: *mut c_int) -> c_int;
pub type XWalEndReadTransaction = extern "C" fn(wal: *mut Wal);
pub type XWalFindFrameFn = extern "C" fn(wal: *mut Wal, pgno: u32, frame: *mut u32) -> c_int;
pub type XWalReadFrameFn =
    extern "C" fn(wal: *mut Wal, frame: u32, n_out: c_int, p_out: *mut u8) -> c_int;
pub type XWalDbsizeFn = extern "C" fn(wal: *mut Wal) -> u32;
pub type XWalBeginWriteTransactionFn = extern "C" fn(wal: *mut Wal) -> c_int;
pub type XWalEndWriteTransactionFn = extern "C" fn(wal: *mut Wal) -> c_int;
pub type XWalSavepointFn = extern "C" fn(wal: *mut Wal, wal_data: *mut u32);
pub type XWalSavePointUndoFn = unsafe extern "C" fn(wal: *mut Wal, wal_data: *mut u32) -> c_int;
pub type XWalCheckpointFn = unsafe extern "C" fn(
    wal: *mut Wal,
    db: *mut sqlite3,
    emode: c_int,
    busy_handler: Option<unsafe extern "C" fn(busy_param: *mut c_void) -> c_int>,
    busy_arg: *mut c_void,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
    frames_in_wal: *mut c_int,
    backfilled_frames: *mut c_int,
) -> c_int;
pub type XWalCallbackFn = extern "C" fn(wal: *mut Wal) -> c_int;
pub type XWalExclusiveModeFn = extern "C" fn(wal: *mut Wal, op: c_int) -> c_int;
pub type XWalHeapMemoryFn = extern "C" fn(wal: *mut Wal) -> c_int;
pub type XWalFileFn = extern "C" fn(wal: *mut Wal) -> *mut sqlite3_file;
pub type XWalDbFn = extern "C" fn(wal: *mut Wal, db: *mut sqlite3);
pub type XWalPathNameLenFn = extern "C" fn(orig_len: c_int) -> c_int;
pub type XWalGetPathNameFn = extern "C" fn(buf: *mut c_char, orig: *const c_char, orig_len: c_int);
pub type XWalPreMainDbOpen =
    extern "C" fn(methods: *mut libsql_wal_methods, path: *const c_char) -> c_int;
pub type XWalOpenFn = extern "C" fn(
    vfs: *mut sqlite3_vfs,
    file: *mut sqlite3_file,
    wal_name: *const c_char,
    no_shm_mode: c_int,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *mut Wal,
) -> c_int;
pub type XWalCloseFn = extern "C" fn(
    wal: *mut Wal,
    db: *mut sqlite3,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int;
pub type XWalFrameFn = unsafe extern "C" fn(
    wal: *mut Wal,
    page_size: c_int,
    page_headers: *mut PgHdr,
    size_after: u32,
    is_commit: c_int,
    sync_flags: c_int,
) -> c_int;
pub type XWalUndoFn = unsafe extern "C" fn(
    wal: *mut Wal,
    func: Option<unsafe extern "C" fn(*mut c_void, c_uint) -> c_int>,
    ctx: *mut c_void,
) -> c_int;

// io methods
pub type XAccessFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    res: *mut c_int,
) -> c_int;
pub type XDeleteFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const c_char, sync_dir: c_int) -> c_int;
pub type XFullPathNameFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    name: *const c_char,
    n: c_int,
    out: *mut c_char,
) -> c_int;
pub type XOpenFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    name: *const c_char,
    file: *mut sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int;
pub type XDlOpenFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const c_char) -> *const c_void;
pub type XDlErrorFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: c_int, msg: *mut c_char);
pub type XDlSymFn = unsafe extern "C" fn(
    vfs: *mut sqlite3_vfs,
    arg: *mut c_void,
    symbol: *const c_char,
) -> unsafe extern "C" fn();
pub type XDlCloseFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void);
pub type XRandomnessFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n_bytes: c_int, out: *mut c_char) -> c_int;
pub type XSleepFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, ms: c_int) -> c_int;
pub type XCurrentTimeFn = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut f64) -> c_int;
pub type XGetLastErrorFn =
    unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: c_int, buf: *mut c_char) -> c_int;
pub type XCurrentTimeInt64 = unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut i64) -> c_int;
pub type XCloseFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> c_int;
pub type XReadFn = unsafe extern "C" fn(
    file_ptr: *mut sqlite3_file,
    buf: *mut c_char,
    n: c_int,
    off: i64,
) -> c_int;
pub type XWriteFn = unsafe extern "C" fn(
    file_ptr: *mut sqlite3_file,
    buf: *const c_char,
    n: c_int,
    off: i64,
) -> c_int;
pub type XTruncateFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: i64) -> c_int;
pub type XSyncFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, flags: c_int) -> c_int;
pub type XFileSizeFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: *mut i64) -> c_int;
pub type XLockFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: c_int) -> c_int;
pub type XUnlockFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: c_int) -> c_int;
pub type XCheckReservedLockFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, res: *mut c_int) -> c_int;
pub type XFileControlFn =
    unsafe extern "C" fn(file_ptr: *mut sqlite3_file, op: c_int, arg: *mut c_void) -> c_int;
pub type XSectorSizeFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> c_int;
pub type XDeviceCharacteristicsFn = unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> c_int;

pub struct PageHdrIter {
    current_ptr: *const PgHdr,
    page_size: usize,
}

impl PageHdrIter {
    pub fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
        }
    }
}

impl std::iter::Iterator for PageHdrIter {
    type Item = (u32, &'static [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.pData as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data));
        self.current_ptr = current_hdr.pDirty;
        item
    }
}
