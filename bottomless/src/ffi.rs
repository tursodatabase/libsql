use std::ffi::c_void;

pub const SQLITE_OK: i32 = 0;
pub const SQLITE_CANTOPEN: i32 = 14;
pub const SQLITE_IOERR_WRITE: i32 = 778;

pub const SQLITE_CHECKPOINT_TRUNCATE: i32 = 3;

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_file {
    pub methods: *const sqlite3_io_methods,
}

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_vfs {
    iVersion: i32,
    szOsFile: i32,
    mxPathname: i32,
    pNext: *mut sqlite3_vfs,
    pub(crate) zName: *const i8,
    pData: *const c_void,
    xOpen: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        name: *const i8,
        file: *mut sqlite3_file,
        flags: i32,
        out_flags: *mut i32,
    ) -> i32,
    xDelete: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, sync_dir: i32) -> i32,
    xAccess: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        name: *const i8,
        flags: i32,
        res: *mut i32,
    ) -> i32,
    xFullPathname:
        unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8, n: i32, out: *mut i8) -> i32,
    xDlOpen: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, name: *const i8) -> *const c_void,
    xDlError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, msg: *mut u8),
    xDlSym: unsafe extern "C" fn(
        vfs: *mut sqlite3_vfs,
        arg: *mut c_void,
        symbol: *const u8,
    ) -> unsafe extern "C" fn(),
    xDlClose: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, arg: *mut c_void),
    xRandomness: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n_bytes: i32, out: *mut u8) -> i32,
    xSleep: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, ms: i32) -> i32,
    xCurrentTime: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut f64) -> i32,
    xGetLastError: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, n: i32, buf: *mut u8) -> i32,
    xCurrentTimeInt64: unsafe extern "C" fn(vfs: *mut sqlite3_vfs, time: *mut i64) -> i32,
}

#[repr(C)]
#[derive(Debug)]
pub struct sqlite3_io_methods {
    iVersion: i32,
    xClose: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    xRead: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *mut u8, n: i32, off: i64) -> i32,
    xWrite:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, buf: *const u8, n: i32, off: i64) -> i32,
    xTruncate: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: i64) -> i32,
    xSync: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, flags: i32) -> i32,
    pub xFileSize: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, size: *mut i64) -> i32,
    xLock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32,
    xUnlock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, lock: i32) -> i32,
    xCheckReservedLock: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, res: *mut i32) -> i32,
    xFileControl:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, op: i32, arg: *mut c_void) -> i32,
    xSectorSize: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    xDeviceCharacteristics: unsafe extern "C" fn(file_ptr: *mut sqlite3_file) -> i32,
    /* v2
    xShmMap: unsafe extern "C" fn(
        file_ptr: *mut sqlite3_file,
        pgno: i32,
        pgsize: i32,
        arg: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xShmLock:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, offset: i32, n: i32, flags: i32) -> i32,
    xShmBarrier: unsafe extern "C" fn(file_ptr: *mut sqlite3_file),
    xShmUnmap: unsafe extern "C" fn(file_ptr: *mut sqlite3_file, delete_flag: i32) -> i32,
    // v3
    xFetch: unsafe extern "C" fn(
        file_ptr: *mut sqlite3_file,
        off: i64,
        n: i32,
        addr: *mut *mut c_void,
    ) -> i32,
    xUnfetch:
        unsafe extern "C" fn(file_ptr: *mut sqlite3_file, off: i64, addr: *mut c_void) -> i32,
    */
}

#[repr(C)]
pub struct Wal {
    vfs: *const sqlite3_vfs,
    db_fd: *mut sqlite3_file,
    pub wal_fd: *mut sqlite3_file,
    callback_value: u32,
    max_wal_size: i64,
    wi_data: i32,
    size_first_block: i32,
    ap_wi_data: *const *mut u32,
    page_size: u32,
    read_lock: i16,
    sync_flags: u8,
    exclusive_mode: u8,
    write_lock: u8,
    checkpoint_lock: u8,
    read_only: u8,
    truncate_on_commit: u8,
    sync_header: u8,
    pad_to_section_boundary: u8,
    b_shm_unreliable: u8,
    pub(crate) hdr: WalIndexHdr,
    min_frame: u32,
    recalculate_checksums: u32,
    wal_name: *const i8,
    n_checkpoints: u32,
    lock_error: u8,
    p_snapshot: *const c_void,
    p_db: *const c_void,
    pub wal_methods: *mut libsql_wal_methods,
    pub replicator_context: *mut crate::replicator::Context,
}

#[repr(C)]
pub struct WalIndexHdr {
    version: u32,
    unused: u32,
    change: u32,
    is_init: u8,
    big_endian_checksum: u8,
    page_size: u16,
    pub(crate) last_valid_frame: u32,
    n_pages: u32,
    pub(crate) frame_checksum: [u32; 2],
    salt: [u32; 2],
    checksum: [u32; 2],
}

#[repr(C)]
pub struct libsql_wal_methods {
    pub iVersion: i32,
    pub xOpen: extern "C" fn(
        vfs: *const sqlite3_vfs,
        file: *mut sqlite3_file,
        wal_name: *const i8,
        no_shm_mode: i32,
        max_size: i64,
        methods: *mut libsql_wal_methods,
        wal: *mut *mut Wal,
    ) -> i32,
    pub xClose: extern "C" fn(
        wal: *mut Wal,
        db: *mut c_void,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
    ) -> i32,
    pub xLimit: extern "C" fn(wal: *mut Wal, limit: i64),
    pub xBeginReadTransaction: extern "C" fn(wal: *mut Wal, changed: *mut i32) -> i32,
    pub xEndReadTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    pub xFindFrame: extern "C" fn(wal: *mut Wal, pgno: i32, frame: *mut i32) -> i32,
    pub xReadFrame: extern "C" fn(wal: *mut Wal, frame: u32, n_out: i32, p_out: *mut u8) -> i32,
    pub xDbSize: extern "C" fn(wal: *mut Wal) -> i32,
    pub xBeginWriteTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    pub xEndWriteTransaction: extern "C" fn(wal: *mut Wal) -> i32,
    pub xUndo: extern "C" fn(
        wal: *mut Wal,
        func: extern "C" fn(*mut c_void, i32) -> i32,
        ctx: *mut c_void,
    ) -> i32,
    pub xSavepoint: extern "C" fn(wal: *mut Wal, wal_data: *mut u32),
    pub xSavepointUndo: extern "C" fn(wal: *mut Wal, wal_data: *mut u32) -> i32,
    pub xFrames: extern "C" fn(
        wal: *mut Wal,
        page_size: u32,
        page_headers: *const PgHdr,
        size_after: u32,
        is_commit: i32,
        sync_flags: i32,
    ) -> i32,
    pub xCheckpoint: extern "C" fn(
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
    ) -> i32,
    pub xCallback: extern "C" fn(wal: *mut Wal) -> i32,
    pub xExclusiveMode: extern "C" fn(wal: *mut Wal) -> i32,
    pub xHeapMemory: extern "C" fn(wal: *mut Wal) -> i32,
    // snapshot stubs
    pub snapshot_get_stub: *const c_void,
    pub snapshot_open_stub: *const c_void,
    pub snapshot_recover_stub: *const c_void,
    pub snapshot_check_stub: *const c_void,
    pub snapshot_unlock_stub: *const c_void,
    pub framesize_stub: *const c_void, // enable_zipvfs stub
    pub xFile: extern "C" fn(wal: *mut Wal) -> *const c_void,
    pub write_lock_stub: *const c_void, // setlk stub
    pub xDb: extern "C" fn(wal: *mut Wal, db: *const c_void),
    pub xPathnameLen: extern "C" fn(orig_len: i32) -> i32,
    pub xGetPathname: extern "C" fn(buf: *mut u8, orig: *const u8, orig_len: i32),
    pub xPreMainDbOpen: extern "C" fn(methods: *mut libsql_wal_methods, path: *const i8) -> i32,
    pub b_uses_shm: i32,
    pub name: *const u8,
    pub p_next: *const c_void,

    // User data
    pub underlying_methods: *const libsql_wal_methods,
}

#[repr(C)]
pub struct PgHdr {
    page: *const c_void,
    data: *const c_void,
    extra: *const c_void,
    pcache: *const c_void,
    dirty: *const PgHdr,
    pager: *const c_void,
    pgno: i32,
    flags: u16,
}

pub(crate) struct PageHdrIter {
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
    type Item = (i32, &'static [u8]);

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
