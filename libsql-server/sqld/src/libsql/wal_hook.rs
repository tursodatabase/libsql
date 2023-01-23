use std::ffi::{c_char, c_int, c_void};

use super::ffi::{self, libsql_wal_methods, sqlite3_file, sqlite3_vfs, types::*, PgHdr, Wal};

/// The `WalHook` trait allows to intercept WAL method call.
///
/// All the methods in this trait have the following format: - arguments to the WAL method -
/// function pointer to the wrapped WAL method
///
/// The default implementations for this trait methods is to transparently call the wrapped methods
/// with the passed arguments
///
/// # Safety
/// The implementer is responsible for calling the orig method with valid arguments.
pub unsafe trait WalHook {
    /// Intercept `xFrame` call. `orig` is the function pointer to the underlying wal method.
    /// The default implementation of this trait simply calls orig with the other passed arguments.
    #[allow(clippy::too_many_arguments)]
    fn on_frames(
        &mut self,
        wal: *mut Wal,
        page_size: c_int,
        page_headers: *mut PgHdr,
        size_after: u32,
        is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        (orig)(
            wal,
            page_size,
            page_headers,
            size_after,
            is_commit,
            sync_flags,
        )
    }

    /// Intercept `xUndo` call. `orig` is the function pointer to the underlying wal method.
    /// The default implementation of this trait simply calls orig with the other passed arguments.
    fn on_undo(
        &mut self,
        wal: *mut Wal,
        func: extern "C" fn(*mut c_void, i32) -> i32,
        ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        orig(wal, func, ctx)
    }
}

/// Wal implemementation that just proxies calls to the wrapped WAL methods implementation
unsafe impl WalHook for () {}

impl WalMethodsHook {
    pub const METHODS_NAME_STR: &'static str = "wal_hook";
    pub const METHODS_NAME: &'static [u8] = b"wal_hook\0";

    pub fn wrap(
        underlying_methods: *const libsql_wal_methods,
        hook: impl WalHook + 'static,
    ) -> *const libsql_wal_methods {
        let name = Self::METHODS_NAME.as_ptr();
        let wal_methods = WalMethodsHook {
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
            underlying_methods,
            hook: Box::new(hook),
        };

        Box::into_raw(Box::new(wal_methods)) as _
    }
}
#[allow(non_snake_case)]
pub extern "C" fn xOpen(
    vfs: *const sqlite3_vfs,
    db_file: *mut sqlite3_file,
    wal_name: *const c_char,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *const Wal,
) -> i32 {
    tracing::debug!("Opening WAL {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });
    let ref_methods = unsafe { &*(methods as *mut WalMethodsHook) };
    let origxOpen = unsafe { (*ref_methods.underlying_methods).xOpen };
    (origxOpen)(vfs, db_file, wal_name, no_shm_mode, max_size, methods, wal)
}

unsafe fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    let write_proxy = get_methods(wal);
    &*write_proxy.underlying_methods
}

unsafe fn get_methods(wal: *mut Wal) -> &'static mut WalMethodsHook {
    &mut *(&mut *(*wal).wal_methods as *mut _ as *mut WalMethodsHook)
}

#[allow(non_snake_case)]
pub extern "C" fn xClose(
    wal: *mut Wal,
    db: *mut c_void,
    sync_flags: i32,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xClose)(wal, db, sync_flags, n_buf, z_buf)
}

#[allow(non_snake_case)]
pub extern "C" fn xLimit(wal: *mut Wal, limit: i64) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xLimit)(wal, limit)
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xBeginReadTransaction)(wal, changed)
}

#[allow(non_snake_case)]
pub extern "C" fn xEndReadTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xEndReadTransaction)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: u32, frame: *mut u32) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xFindFrame)(wal, pgno, frame)
}

#[allow(non_snake_case)]
pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: c_int, p_out: *mut u8) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xReadFrame)(wal, frame, n_out, p_out)
}

#[allow(non_snake_case)]
pub extern "C" fn xDbSize(wal: *mut Wal) -> u32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xDbSize)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xBeginWriteTransaction)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xEndWriteTransaction)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xUndo(
    wal: *mut Wal,
    func: extern "C" fn(*mut c_void, i32) -> i32,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    let methods = unsafe { get_methods(wal) };
    methods.hook.on_undo(wal, func, ctx, orig_methods.xUndo)
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xSavepoint)(wal, wal_data)
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepointUndo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xSavepointUndo)(wal, wal_data)
}

#[allow(non_snake_case)]
pub extern "C" fn xFrames(
    wal: *mut Wal,
    page_size: c_int,
    page_headers: *mut PgHdr,
    size_after: u32,
    is_commit: c_int,
    sync_flags: c_int,
) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    let methods = unsafe { get_methods(wal) };
    let hook = &mut methods.hook;

    hook.on_frames(
        wal,
        page_size,
        page_headers,
        size_after,
        is_commit,
        sync_flags,
        orig_methods.xFrames,
    )
}

#[tracing::instrument(skip(wal, db))]
#[allow(non_snake_case)]
pub extern "C" fn xCheckpoint(
    wal: *mut Wal,
    db: *mut c_void,
    emode: c_int,
    busy_handler: extern "C" fn(busy_param: *mut c_void) -> c_int,
    busy_arg: *const c_void,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
    frames_in_wal: *mut c_int,
    backfilled_frames: *mut c_int,
) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xCheckpoint)(
        wal,
        db,
        emode,
        busy_handler,
        busy_arg,
        sync_flags,
        n_buf,
        z_buf,
        frames_in_wal,
        backfilled_frames,
    )
}

#[allow(non_snake_case)]
pub extern "C" fn xCallback(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xCallback)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xExclusiveMode(wal: *mut Wal, op: c_int) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xExclusiveMode)(wal, op)
}

#[allow(non_snake_case)]
pub extern "C" fn xHeapMemory(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xHeapMemory)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xFile(wal: *mut Wal) -> *const c_void {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xFile)(wal)
}

#[allow(non_snake_case)]
pub extern "C" fn xDb(wal: *mut Wal, db: *const c_void) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    (orig_methods.xDb)(wal, db)
}

#[allow(non_snake_case)]
pub extern "C" fn xPathnameLen(orig_len: i32) -> i32 {
    orig_len + 4
}

#[allow(non_snake_case)]
pub extern "C" fn xGetPathname(buf: *mut c_char, orig: *const c_char, orig_len: c_int) {
    unsafe { std::ptr::copy(orig, buf, orig_len as usize) }
    unsafe {
        std::ptr::copy(
            "-wal".as_ptr(),
            (buf as *mut u8).offset(orig_len as isize),
            4,
        )
    }
}

#[allow(non_snake_case)]
pub extern "C" fn xPreMainDbOpen(_methods: *mut libsql_wal_methods, _path: *const c_char) -> i32 {
    ffi::SQLITE_OK
}

#[repr(C)]
#[allow(non_snake_case)]
pub struct WalMethodsHook {
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
    pub xPathnameLen: XWalPathNameLenFn,
    pub xGetPathname: XWalGetPathNameFn,
    pub xPreMainDbOpen: XWalPreMainDbOpen,
    pub b_uses_shm: i32,
    pub name: *const u8,
    pub p_next: *const c_void,

    //user data
    underlying_methods: *const libsql_wal_methods,
    hook: Box<dyn WalHook>,
}
