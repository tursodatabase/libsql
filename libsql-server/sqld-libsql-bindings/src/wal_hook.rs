#![allow(clippy::not_unsafe_ptr_arg_deref)]
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
        unsafe {
            (orig)(
                wal,
                page_size,
                page_headers,
                size_after,
                is_commit,
                sync_flags,
            )
        }
    }

    /// Intercept `xUndo` call. `orig` is the function pointer to the underlying wal method.
    /// The default implementation of this trait simply calls orig with the other passed arguments.
    fn on_undo(
        &mut self,
        wal: *mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        unsafe { orig(wal, func, ctx) }
    }
}

/// Wal implemementation that just proxies calls to the wrapped WAL methods implementation
unsafe impl WalHook for () {}

impl WalMethodsHook {
    pub const METHODS_NAME_STR: &'static str = "wal_hook";
    pub const METHODS_NAME: &'static [u8] = b"wal_hook\0";

    pub fn wrap(
        underlying_methods: *mut libsql_wal_methods,
        hook: impl WalHook + 'static,
    ) -> *mut libsql_wal_methods {
        let name = Self::METHODS_NAME.as_ptr() as *const _;
        let wal_methods = WalMethodsHook {
            methods: libsql_wal_methods {
                iVersion: 1,
                xOpen: Some(xOpen),
                xClose: Some(xClose),
                xLimit: Some(xLimit),
                xBeginReadTransaction: Some(xBeginReadTransaction),
                xEndReadTransaction: Some(xEndReadTransaction),
                xFindFrame: Some(xFindFrame),
                xReadFrame: Some(xReadFrame),
                xDbsize: Some(xDbsize),
                xBeginWriteTransaction: Some(xBeginWriteTransaction),
                xEndWriteTransaction: Some(xEndWriteTransaction),
                xUndo: Some(xUndo),
                xSavepoint: Some(xSavepoint),
                xSavepointUndo: Some(xSavepointUndo),
                xFrames: Some(xFrames),
                xCheckpoint: Some(xCheckpoint),
                xCallback: Some(xCallback),
                xExclusiveMode: Some(xExclusiveMode),
                xHeapMemory: Some(xHeapMemory),
                xSnapshotGet: None,
                xSnapshotOpen: None,
                xSnapshotRecover: None,
                xSnapshotCheck: None,
                xSnapshotUnlock: None,
                xFramesize: None,
                xFile: Some(xFile),
                xWriteLock: None,
                xDb: Some(xDb),
                xPathnameLen: Some(xPathnameLen),
                xGetWalPathname: Some(xGetPathname),
                xPreMainDbOpen: Some(xPreMainDbOpen),
                zName: name,
                bUsesShm: 0,
                pNext: std::ptr::null_mut(),
            },
            underlying_methods,
            hook: Box::new(hook),
        };

        Box::into_raw(Box::new(wal_methods)) as _
    }
}
#[allow(non_snake_case)]
pub extern "C" fn xOpen(
    vfs: *mut sqlite3_vfs,
    db_file: *mut sqlite3_file,
    wal_name: *const c_char,
    no_shm_mode: i32,
    max_size: i64,
    methods: *mut libsql_wal_methods,
    wal: *mut *mut Wal,
) -> i32 {
    tracing::debug!("Opening WAL {}", unsafe {
        std::ffi::CStr::from_ptr(wal_name).to_str().unwrap()
    });
    let ref_methods = unsafe { &*(methods as *mut WalMethodsHook) };
    let origxOpen = unsafe { (*ref_methods.underlying_methods).xOpen.unwrap() };
    unsafe {
        (origxOpen)(
            vfs,
            db_file,
            wal_name,
            no_shm_mode,
            max_size,
            ref_methods.underlying_methods,
            wal,
        )
    }
}

unsafe fn get_orig_methods(wal: *mut Wal) -> &'static libsql_wal_methods {
    let write_proxy = get_methods(wal);
    &*write_proxy.underlying_methods
}

unsafe fn get_methods(wal: *mut Wal) -> &'static mut WalMethodsHook {
    &mut *(&mut *(*wal).pMethods as *mut _ as *mut WalMethodsHook)
}

#[allow(non_snake_case)]
pub extern "C" fn xClose(
    wal: *mut Wal,
    db: *mut rusqlite::ffi::sqlite3,
    sync_flags: i32,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xClose.unwrap())(wal, db, sync_flags, n_buf, z_buf) }
}

#[allow(non_snake_case)]
pub extern "C" fn xLimit(wal: *mut Wal, limit: i64) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xLimit.unwrap())(wal, limit) }
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginReadTransaction(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xBeginReadTransaction.unwrap())(wal, changed) }
}

#[allow(non_snake_case)]
pub extern "C" fn xEndReadTransaction(wal: *mut Wal) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xEndReadTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xFindFrame(wal: *mut Wal, pgno: u32, frame: *mut u32) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xFindFrame.unwrap())(wal, pgno, frame) }
}

#[allow(non_snake_case)]
pub extern "C" fn xReadFrame(wal: *mut Wal, frame: u32, n_out: c_int, p_out: *mut u8) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xReadFrame.unwrap())(wal, frame, n_out, p_out) }
}

#[allow(non_snake_case)]
pub extern "C" fn xDbsize(wal: *mut Wal) -> u32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xDbsize.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xBeginWriteTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xEndWriteTransaction(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xEndWriteTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xUndo(
    wal: *mut Wal,
    func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
    ctx: *mut c_void,
) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    let methods = unsafe { get_methods(wal) };
    methods
        .hook
        .on_undo(wal, func, ctx, orig_methods.xUndo.unwrap())
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepoint(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xSavepoint.unwrap())(wal, wal_data) }
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepointUndo(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xSavepointUndo.unwrap())(wal, wal_data) }
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
        orig_methods.xFrames.unwrap(),
    )
}

#[tracing::instrument(skip(wal, db))]
#[allow(non_snake_case)]
pub extern "C" fn xCheckpoint(
    wal: *mut Wal,
    db: *mut rusqlite::ffi::sqlite3,
    emode: c_int,
    busy_handler: Option<unsafe extern "C" fn(busy_param: *mut c_void) -> c_int>,
    busy_arg: *mut c_void,
    sync_flags: c_int,
    n_buf: c_int,
    z_buf: *mut u8,
    frames_in_wal: *mut c_int,
    backfilled_frames: *mut c_int,
) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe {
        (orig_methods.xCheckpoint.unwrap())(
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
}

#[allow(non_snake_case)]
pub extern "C" fn xCallback(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xCallback.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xExclusiveMode(wal: *mut Wal, op: c_int) -> c_int {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xExclusiveMode.unwrap())(wal, op) }
}

#[allow(non_snake_case)]
pub extern "C" fn xHeapMemory(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xHeapMemory.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xFile(wal: *mut Wal) -> *mut sqlite3_file {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xFile.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xDb(wal: *mut Wal, db: *mut rusqlite::ffi::sqlite3) {
    let orig_methods = unsafe { get_orig_methods(wal) };
    unsafe { (orig_methods.xDb.unwrap())(wal, db) }
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
    pub methods: libsql_wal_methods,

    //user data
    underlying_methods: *mut libsql_wal_methods,
    hook: Box<dyn WalHook>,
}
