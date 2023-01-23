#![allow(dead_code)]

pub mod types;

use std::ffi::{c_char, c_int, c_void};

use types::*;

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_CANTOPEN: i32 = 14;
pub const SQLITE_IOERR_WRITE: i32 = 778;

pub const SQLITE_CHECKPOINT_FULL: i32 = 1;

#[repr(C)]
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct sqlite3_file {
    pub methods: *const sqlite3_io_methods,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct sqlite3_vfs {
    iVersion: c_int,
    szOsFile: c_int,
    mxPathname: c_int,
    pNext: *mut sqlite3_vfs,
    zname: *const c_char,
    pData: *const c_void,
    xOpen: XOpenFn,
    xDelete: XDeleteFn,
    xAccess: XAccessFn,
    xFullPathname: XFullPathNameFn,
    xDlOpen: XDlOpenFn,
    xDlError: XDlErrorFn,
    xDlSym: XDlSymFn,
    xDlClose: XDlCloseFn,
    xRandomness: XRandomnessFn,
    xSleep: XSleepFn,
    xCurrentTime: XCurrentTimeFn,
    xGetLastError: XGetLastErrorFn,
    xCurrentTimeInt64: XCurrentTimeInt64,
}

#[repr(C)]
#[derive(Debug)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct sqlite3_io_methods {
    iVersion: c_int,
    xClose: XCloseFn,
    xRead: XReadFn,
    xWrite: XWriteFn,
    xTruncate: XTruncateFn,
    xSync: XSyncFn,
    pub xFileSize: XFileSizeFn,
    xLock: XLockFn,
    xUnlock: XUnlockFn,
    xCheckReservedLock: XCheckReservedLockFn,
    xFileControl: XFileControlFn,
    xSectorSize: XSectorSizeFn,
    xDeviceCharacteristics: XDeviceCharacteristicsFn,
}

#[repr(C)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct Wal {
    pub vfs: *const sqlite3_vfs,
    pub db_fd: *mut sqlite3_file,
    pub wal_fd: *mut sqlite3_file,
    pub callback_value: u32,
    pub max_wal_size: i64,
    pub wi_data: i32,
    pub size_first_block: i32,
    pub ap_wi_data: *const *mut u32,
    pub page_size: u32,
    pub read_lock: i16,
    pub sync_flags: u8,
    pub exclusive_mode: u8,
    pub write_lock: u8,
    pub checkpoint_lock: u8,
    pub read_only: u8,
    pub truncate_on_commit: u8,
    pub sync_header: u8,
    pub pad_to_section_boundary: u8,
    pub b_shm_unreliable: u8,
    pub hdr: WalIndexHdr,
    pub min_frame: u32,
    pub recalculate_checksums: u32,
    pub wal_name: *const i8,
    pub n_checkpoints: u32,
    pub lock_error: u8,
    pub p_snapshot: *const c_void,
    pub p_db: *const c_void,
    pub wal_methods: *mut libsql_wal_methods,
}

#[repr(C)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct WalIndexHdr {
    pub version: u32,
    pub unused: u32,
    pub change: u32,
    pub is_init: u8,
    pub big_endian_checksum: u8,
    pub page_size: u16,
    pub last_valid_frame: u32,
    pub n_pages: u32,
    pub frame_checksum: [u32; 2],
    pub salt: [u32; 2],
    pub checksum: [u32; 2],
}

#[repr(C)]
#[allow(non_snake_case, non_camel_case_types)]
pub struct libsql_wal_methods {
    pub iVersion: i32,
    pub xOpen: XWalOpenFn,
    pub xClose: XWalCloseFn,
    pub xLimit: XWalLimitFn,
    pub xBeginReadTransaction: XWalBeginReadTransactionFn,
    pub xEndReadTransaction: XWalEndReadTransaction,
    pub xFindFrame: XWalFindFrameFn,
    pub xReadFrame: XWalReadFrameFn,
    pub xDbSize: XWalDbSizeFn,
    pub xBeginWriteTransaction: XWalBeginWriteTransactionFn,
    pub xEndWriteTransaction: XWalEndWriteTransactionFn,
    pub xUndo: XWalUndoFn,
    pub xSavepoint: XWalSavepointFn,
    pub xSavepointUndo: XWalSavePointUndoFn,
    pub xFrames: XWalFrameFn,
    pub xCheckpoint: XWalCheckpointFn,
    pub xCallback: XWalCallbackFn,
    pub xExclusiveMode: XWalExclusiveModeFn,
    pub xHeapMemory: XWalHeapMemoryFn,
    // snapshot stubs
    pub snapshot_get_stub: *const c_void,
    pub snapshot_open_stub: *const c_void,
    pub snapshot_recover_stub: *const c_void,
    pub snapshot_check_stub: *const c_void,
    pub snapshot_unlock_stub: *const c_void,
    pub framesize_stub: *const c_void, // enable_zipvfs stub
    pub xFile: XWalFileFn,
    pub write_lock_stub: *const c_void, // setlk stub
    pub xDb: XWalDbFn,
    pub xPathnameLen: XFullPathNameFn,
    pub xGetPathname: XWalGetPathNameFn,
    pub xPreMainDbOpen: XWalPreMainDbOpen,
    pub b_uses_shm: i32,
    pub name: *const u8,
    pub p_next: *const c_void,
}

#[repr(C)]
pub struct PgHdr {
    pub page: *const c_void,
    pub data: *const c_void,
    pub extra: *const c_void,
    pub pcache: *const c_void,
    pub dirty: *mut PgHdr,
    pub pager: *const c_void,
    pub pgno: u32,
    pub pagehash: u32,
    pub flags: u16,
}

extern "C" {
    pub fn libsql_wal_methods_register(wal_methods: *const libsql_wal_methods) -> i32;
    pub fn libsql_wal_methods_find(i: c_int) -> *mut libsql_wal_methods;
}

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
            unsafe { std::slice::from_raw_parts(current_hdr.data as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data));
        self.current_ptr = current_hdr.dirty;
        item
    }
}
