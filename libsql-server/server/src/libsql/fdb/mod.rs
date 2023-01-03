#![allow(non_snake_case)]

use std::cmp::Ordering;
use std::ffi::{c_char, c_int, c_void};
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Mutex};

use super::ffi::types::*;
use super::ffi::{
    libsql_wal_methods, libsql_wal_methods_register, sqlite3_file, sqlite3_vfs, PgHdr, Wal,
    WalIndexHdr,
};

use rusqlite::ffi;

pub mod replicator;

const WAL_NORMAL_MODE: u8 = 0;
const WAL_EXCLUSIVE_MODE: u8 = 1;
const WAL_HEAPMEMORY_MODE: u8 = 2;

#[repr(C)]
pub struct WalMethods {
    pub iVersion: c_int,
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
    pub xPathnameLen: XWalPathNameLenFn,
    pub xGetPathname: XWalGetPathNameFn,
    pub xPreMainDbOpen: XWallPreMainDbOpen,
    pub b_uses_shm: i32,
    pub name: *const u8,
    pub p_next: *const c_void,

    //user data
    replicator: replicator::Replicator,
}

// Only safe if we consider passing pointers from C safe to Send
unsafe impl Send for WalMethods {}

extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    _file: *mut sqlite3_file,
    wal_name: *const c_char,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32 {
    tracing::debug!("Opening {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name as *const i8)
            .to_str()
            .unwrap()
    });
    let exclusive_mode = if no_shm_mode != 0 {
        WAL_HEAPMEMORY_MODE
    } else {
        WAL_NORMAL_MODE
    };
    let new_wal = Box::into_raw(Box::new(Wal {
        vfs,
        db_fd: std::ptr::null_mut(),
        wal_fd: std::ptr::null_mut(),
        callback_value: 0,
        max_wal_size: max_size,
        wi_data: 0,
        size_first_block: 0,
        ap_wi_data: std::ptr::null(),
        page_size: 4096,
        read_lock: 0,
        sync_flags: 0,
        exclusive_mode,
        write_lock: 0,
        checkpoint_lock: 0,
        read_only: 0,
        truncate_on_commit: 0,
        sync_header: 0,
        pad_to_section_boundary: 0,
        b_shm_unreliable: 1,
        hdr: WalIndexHdr {
            version: 1,
            unused: 0,
            change: 0,
            is_init: 0,
            big_endian_checksum: 0,
            page_size: 4096,
            last_valid_frame: 1,
            n_pages: 1,
            frame_checksum: [0, 0],
            salt: [0, 0],
            checksum: [0, 0],
        },
        min_frame: 0,
        recalculate_checksums: 0,
        wal_name,
        n_checkpoints: 0,
        lock_error: 0,
        p_snapshot: std::ptr::null(),
        p_db: std::ptr::null(),

        wal_methods: methods as _,
    }));
    unsafe { *wal = new_wal }
    tracing::debug!("Opened WAL at {:?}", new_wal);
    ffi::SQLITE_OK
}

fn get_methods(wal: *mut Wal) -> &'static mut WalMethods {
    unsafe { &mut *((*wal).wal_methods as *mut WalMethods) }
}

extern "C" fn xClose(
    wal: *mut Wal,
    _db: *mut c_void,
    _sync_flags: c_int,
    _n_buf: c_int,
    _z_buf: *mut u8,
) -> c_int {
    let _ = unsafe { Box::from_raw(wal) };
    ffi::SQLITE_OK
}

extern "C" fn xLimit(_wal: *mut Wal, limit: i64) {
    tracing::debug!("Limit: {}", limit);
}

extern "C" fn xBeginReadTransaction(_wal: *mut Wal, changed: *mut i32) -> i32 {
    tracing::debug!("Read starts");
    unsafe { *changed = 1 }
    ffi::SQLITE_OK
}

extern "C" fn xEndReadTransaction(_wal: *mut Wal) -> i32 {
    tracing::debug!("Read ends");
    ffi::SQLITE_OK
}

extern "C" fn xFindFrame(wal: *mut Wal, pgno: u32, frame: *mut u32) -> c_int {
    let methods = get_methods(wal);
    let frameno = methods.replicator.find_frame(pgno);
    tracing::debug!("Page {} has frame {}", pgno, frameno);
    unsafe { *frame = frameno }
    ffi::SQLITE_OK
}

extern "C" fn xReadFrame(wal: *mut Wal, frameno: u32, n_out: i32, p_out: *mut u8) -> i32 {
    let methods = get_methods(wal);
    let frame = methods.replicator.get_frame(frameno);
    tracing::debug!(
        "Frame {}:{} retrieved (n_out={})",
        frameno,
        frame.len(),
        n_out
    );
    unsafe {
        std::ptr::copy(
            frame.as_ptr(),
            p_out,
            std::cmp::min(frame.len(), n_out as usize),
        )
    };
    ffi::SQLITE_OK
}

extern "C" fn xDbSize(wal: *mut Wal) -> u32 {
    let methods = get_methods(wal);
    methods.replicator.get_number_of_pages()
}

extern "C" fn xBeginWriteTransaction(_wal: *mut Wal) -> i32 {
    tracing::debug!("Write starts");
    ffi::SQLITE_OK
}

extern "C" fn xEndWriteTransaction(_wal: *mut Wal) -> i32 {
    tracing::debug!("Write ends");
    ffi::SQLITE_OK
}

extern "C" fn xUndo(
    wal: *mut Wal,
    _func: extern "C" fn(*mut c_void, i32) -> i32,
    _ctx: *mut c_void,
) -> i32 {
    let methods = get_methods(wal);
    methods.replicator.clear_pending();
    ffi::SQLITE_OK
}

extern "C" fn xSavepoint(_wal: *mut Wal, _wal_data: *mut u32) {
    tracing::debug!("Savepoint called!");
}

extern "C" fn xSavepointUndo(_wal: *mut Wal, _wal_data: *mut u32) -> i32 {
    tracing::debug!("Savepoint-undo called!");
    ffi::SQLITE_MISUSE
}

pub(crate) struct PageHdrIter {
    current_ptr: *const PgHdr,
    page_size: usize,
}

impl PageHdrIter {
    fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
        }
    }
}

impl std::iter::Iterator for PageHdrIter {
    type Item = (u32, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.data as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data.to_vec()));
        self.current_ptr = current_hdr.dirty;
        item
    }
}

extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: c_int,
    page_headers: *mut PgHdr,
    size_after: u32,
    is_commit: c_int,
    _sync_flags: c_int,
) -> c_int {
    let is_commit = is_commit != 0;
    let methods = get_methods(wal);
    unsafe { (*wal).page_size = page_size as _ };
    let pages_iter = PageHdrIter::new(page_headers, page_size as usize);
    methods.replicator.add_pending(pages_iter);
    tracing::debug!(
        "Commit? {:?} {}, size_after = {}",
        wal,
        is_commit,
        size_after
    );
    if is_commit && size_after > 0 {
        methods.replicator.flush_pending_pages();
        methods.replicator.set_number_of_pages(size_after);
    }
    ffi::SQLITE_OK
}

extern "C" fn xCheckpoint(
    _wal: *mut Wal,
    _db: *mut c_void,
    _emode: i32,
    _busy_handler: extern "C" fn(busy_param: *mut c_void) -> i32,
    _busy_arg: *const c_void,
    _sync_flags: i32,
    _n_buf: i32,
    _z_buf: *mut u8,
    _frames_in_wal: *mut i32,
    _backfilled_frames: *mut i32,
) -> i32 {
    tracing::debug!("Checkpoint called!");
    ffi::SQLITE_MISUSE
}

extern "C" fn xCallback(_wal: *mut Wal) -> i32 {
    tracing::debug!("Callback called!");
    ffi::SQLITE_MISUSE
}

fn get_mode(wal: *mut Wal) -> u8 {
    unsafe { (*wal).exclusive_mode }
}

fn set_mode(wal: *mut Wal, mode: u8) {
    unsafe { (*wal).exclusive_mode = mode }
}

extern "C" fn xExclusiveMode(wal: *mut Wal, op: c_int) -> c_int {
    match op.cmp(&0) {
        Ordering::Equal => {
            if get_mode(wal) != WAL_NORMAL_MODE {
                set_mode(wal, WAL_NORMAL_MODE);
                //FIXME: copy locking implementation from wal.c
                // and potentially base it on foundationdb input
            }
            (get_mode(wal) == WAL_NORMAL_MODE).into()
        }
        Ordering::Greater => {
            set_mode(wal, WAL_EXCLUSIVE_MODE);
            1
        }
        Ordering::Less => (get_mode(wal) == WAL_NORMAL_MODE).into(),
    }
}

extern "C" fn xHeapMemory(_wal: *mut Wal) -> c_int {
    42
}

extern "C" fn xFile(_wal: *mut Wal) -> *const c_void {
    tracing::debug!("file() called!");
    std::ptr::null()
}

extern "C" fn xDb(_wal: *mut Wal, _db: *const c_void) {
    tracing::debug!("db() called!");
}

extern "C" fn xPathnameLen(orig_len: c_int) -> c_int {
    orig_len
}

extern "C" fn xGetPathname(buf: *mut c_char, orig: *const c_char, orig_len: c_int) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
}

pub extern "C" fn xPreMainDbOpen(_methods: *mut libsql_wal_methods, _path: *const c_char) -> i32 {
    ffi::SQLITE_OK
}

#[cfg(feature = "fdb")]
impl WalMethods {
    pub(crate) fn new(fdb_config_path: Option<String>) -> anyhow::Result<Self> {
        let name: *const u8 = "edge_vwal\0".as_ptr();
        let vwal = WalMethods {
            iVersion: 1,
            xOpen,
            xClose,
            xLimit,
            xBeginReadTransaction,
            xEndReadTransaction,
            xFindFrame,
            xReadFrame,
            xDbSize,
            xBeginWriteTransaction,
            xEndWriteTransaction,
            xUndo,
            xSavepoint,
            xSavepointUndo,
            xFrames,
            xCheckpoint,
            xCallback,
            xExclusiveMode,
            xHeapMemory,
            snapshot_get_stub: std::ptr::null(),
            snapshot_open_stub: std::ptr::null(),
            snapshot_recover_stub: std::ptr::null(),
            snapshot_check_stub: std::ptr::null(),
            snapshot_unlock_stub: std::ptr::null(),
            framesize_stub: std::ptr::null(),
            xFile,
            write_lock_stub: std::ptr::null(),
            xDb,
            xPathnameLen,
            xGetPathname,
            xPreMainDbOpen,
            name,
            b_uses_shm: 0,
            p_next: std::ptr::null(),
            replicator: replicator::Replicator::new(fdb_config_path)?,
        };
        Ok(vwal)
    }
}

#[cfg(feature = "fdb")]
pub(crate) fn open_with_virtual_wal(
    path: impl AsRef<std::path::Path>,
    flags: rusqlite::OpenFlags,
    vwal_methods: Arc<Mutex<WalMethods>>,
) -> anyhow::Result<super::WalConnection> {
    let mut vwal_methods = vwal_methods.lock().map_err(|e| anyhow::anyhow!("{}", e))?;
    vwal_methods.replicator.load_top_frameno();
    unsafe {
        let mut pdb: *mut ffi::sqlite3 = std::ptr::null_mut();
        let ppdb: *mut *mut ffi::sqlite3 = &mut pdb;
        let register_err =
            libsql_wal_methods_register(&mut *vwal_methods as *const WalMethods as _);
        assert_eq!(register_err, 0);
        let open_err = super::libsql_open(
            path.as_ref().as_os_str().as_bytes().as_ptr(),
            ppdb,
            flags.bits(),
            std::ptr::null(),
            vwal_methods.name,
        );
        assert_eq!(open_err, 0);
        let conn = super::Connection::from_handle(pdb)?;
        conn.pragma_update(None, "journal_mode", "wal")?;
        tracing::trace!(
            "Opening a connection with virtual WAL at {}",
            path.as_ref().display()
        );
        Ok(super::WalConnection { inner: conn })
    }
}
