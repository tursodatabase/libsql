#![allow(clippy::not_unsafe_ptr_arg_deref)]
use std::{
    ffi::{c_char, c_int, c_void, CStr},
    marker::PhantomData,
    panic::{catch_unwind, resume_unwind},
};

use crate::ffi::{libsql_wal_methods, sqlite3, sqlite3_file, sqlite3_vfs, types::*, PgHdr, Wal};
use crate::get_orig_wal_methods;

/// This macro handles the registering of a WalHook with the process's sqlite. It first instantiate a `WalMethodsHook`
/// to a stable location in memory, and then call `libsql_wal_methods_register` with the WAL methods.
///
/// The methods are never unregistered, since they're expected to live for the entirety of the program.
#[macro_export]
macro_rules! init_static_wal_method {
    ($name:ident, $ty:path) => {
        pub static $name: $crate::Lazy<&'static $crate::WalMethodsHook<$ty>> =
            $crate::Lazy::new(|| {
                // we need a 'static address before we can register the methods.
                static METHODS: $crate::Lazy<$crate::WalMethodsHook<$ty>> =
                    $crate::Lazy::new(|| $crate::WalMethodsHook::<$ty>::new());

                let ret = unsafe {
                    $crate::ffi::libsql_wal_methods_register(METHODS.as_wal_methods_ptr() as *mut _)
                };

                assert!(
                    ret == 0,
                    "failed to register wal methods for {}",
                    stringify!($ty)
                );

                &METHODS
            });
    };
}

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
    type Context;

    fn name() -> &'static CStr;
    /// Intercept `xFrame` call. `orig` is the function pointer to the underlying wal method.
    /// The default implementation of this trait simply calls orig with the other passed arguments.
    #[allow(clippy::too_many_arguments)]
    fn on_frames(
        wal: &mut Wal,
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
        wal: &mut Wal,
        func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
        undo_ctx: *mut c_void,
        orig: XWalUndoFn,
    ) -> i32 {
        unsafe { orig(wal, func, undo_ctx) }
    }

    fn wal_extract_ctx(wal: &mut Wal) -> &mut Self::Context {
        let ctx_ptr = wal.pMethodsData as *mut Self::Context;
        assert!(!ctx_ptr.is_null(), "missing wal context");
        unsafe { &mut *ctx_ptr }
    }

    fn on_savepoint_undo(wal: &mut Wal, wal_data: *mut u32, orig: XWalSavePointUndoFn) -> i32 {
        unsafe { orig(wal, wal_data) }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_checkpoint(
        wal: &mut Wal,
        db: *mut sqlite3,
        emode: i32,
        busy_handler: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
        busy_arg: *mut c_void,
        sync_flags: i32,
        n_buf: i32,
        z_buf: *mut u8,
        frames_in_wal: *mut i32,
        backfilled_frames: *mut i32,
        orig: XWalCheckpointFn,
    ) -> i32 {
        unsafe {
            orig(
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
}

init_static_wal_method!(TRANSPARENT_METHODS, TransparentMethods);

/// Wal implemementation that just proxies calls to the wrapped WAL methods implementation
#[derive(Debug)]
pub enum TransparentMethods {}

unsafe impl WalHook for TransparentMethods {
    type Context = ();

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"transparent\0").unwrap()
    }
}

impl<T: WalHook> Default for WalMethodsHook<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: WalHook> WalMethodsHook<T> {
    pub fn new() -> Self {
        let default_methods = get_orig_wal_methods().expect("failed to get original WAL methods");

        WalMethodsHook {
            methods: libsql_wal_methods {
                iVersion: 1,
                xOpen: Some(xOpen::<T>),
                xClose: Some(xClose::<T>),
                xLimit: Some(xLimit::<T>),
                xBeginReadTransaction: Some(xBeginReadTransaction::<T>),
                xEndReadTransaction: Some(xEndReadTransaction::<T>),
                xFindFrame: Some(xFindFrame::<T>),
                xReadFrame: Some(xReadFrame::<T>),
                xDbsize: Some(xDbsize::<T>),
                xBeginWriteTransaction: Some(xBeginWriteTransaction::<T>),
                xEndWriteTransaction: Some(xEndWriteTransaction::<T>),
                xUndo: Some(xUndo::<T>),
                xSavepoint: Some(xSavepoint::<T>),
                xSavepointUndo: Some(xSavepointUndo::<T>),
                xFrames: Some(xFrames::<T>),
                xCheckpoint: Some(xCheckpoint::<T>),
                xCallback: Some(xCallback::<T>),
                xExclusiveMode: Some(xExclusiveMode::<T>),
                xHeapMemory: Some(xHeapMemory::<T>),
                xSnapshotGet: None,
                xSnapshotOpen: None,
                xSnapshotRecover: None,
                xSnapshotCheck: None,
                xSnapshotUnlock: None,
                xFramesize: None,
                xFile: Some(xFile::<T>),
                xWriteLock: None,
                xDb: Some(xDb::<T>),
                xPathnameLen: Some(xPathnameLen::<T>),
                xGetWalPathname: Some(xGetPathname::<T>),
                xPreMainDbOpen: Some(xPreMainDbOpen::<T>),
                zName: T::name().as_ptr(),
                bUsesShm: 0,
                pNext: std::ptr::null_mut(),
            },
            underlying_methods: default_methods,
            _pth: PhantomData,
        }
    }

    pub fn as_wal_methods_ptr(&self) -> *const libsql_wal_methods {
        self as *const _ as *mut _
    }
}

macro_rules! catch_panic {
    ($name:literal, { $($body:tt)* }) => {
        {
            let ret = catch_unwind(move || {
                $($body)*
            });

            match ret {
                Ok(x) => x,
                Err(e) => {
                    let error = if let Some(s) = e.downcast_ref::<String>() {
                        s.as_str()
                    } else if let Some(s) = e.downcast_ref::<&str>() {
                        s
                    } else {
                        "unknown"
                    };
                    let bt = std::backtrace::Backtrace::force_capture();
                    tracing::error!("panic in call to {}: {error}:\n{bt}", $name);
                    resume_unwind(e)
                }
            }
        }
    };
}

#[allow(non_snake_case)]
pub extern "C" fn xOpen<T: WalHook>(
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
    let ref_methods = unsafe { &*(methods as *mut WalMethodsHook<T>) };
    let origxOpen = unsafe { (*ref_methods.underlying_methods).xOpen.unwrap() };
    unsafe { (origxOpen)(vfs, db_file, wal_name, no_shm_mode, max_size, methods, wal) }
}

fn get_orig_methods<T: WalHook>(wal: &mut Wal) -> &libsql_wal_methods {
    let methods = get_methods::<T>(wal);
    assert!(!methods.underlying_methods.is_null());
    unsafe { &*methods.underlying_methods }
}

fn get_methods<T>(wal: &mut Wal) -> &mut WalMethodsHook<T> {
    assert!(!wal.pMethods.is_null());
    unsafe { &mut *(wal.pMethods as *mut _ as *mut WalMethodsHook<T>) }
}

#[allow(non_snake_case)]
pub extern "C" fn xClose<T: WalHook>(
    wal: *mut Wal,
    db: *mut rusqlite::ffi::sqlite3,
    sync_flags: i32,
    n_buf: c_int,
    z_buf: *mut u8,
) -> c_int {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xClose.unwrap())(wal, db, sync_flags, n_buf, z_buf) }
}

#[allow(non_snake_case)]
pub extern "C" fn xLimit<T: WalHook>(wal: *mut Wal, limit: i64) {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xLimit.unwrap())(wal, limit) }
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginReadTransaction<T: WalHook>(wal: *mut Wal, changed: *mut i32) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xBeginReadTransaction.unwrap())(wal, changed) }
}

#[allow(non_snake_case)]
pub extern "C" fn xEndReadTransaction<T: WalHook>(wal: *mut Wal) {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xEndReadTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xFindFrame<T: WalHook>(wal: *mut Wal, pgno: u32, frame: *mut u32) -> c_int {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xFindFrame.unwrap())(wal, pgno, frame) }
}

#[allow(non_snake_case)]
pub extern "C" fn xReadFrame<T: WalHook>(
    wal: *mut Wal,
    frame: u32,
    n_out: c_int,
    p_out: *mut u8,
) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xReadFrame.unwrap())(wal, frame, n_out, p_out) }
}

#[allow(non_snake_case)]
pub extern "C" fn xDbsize<T: WalHook>(wal: *mut Wal) -> u32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xDbsize.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xBeginWriteTransaction<T: WalHook>(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xBeginWriteTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xEndWriteTransaction<T: WalHook>(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xEndWriteTransaction.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xUndo<T: WalHook>(
    wal: *mut Wal,
    func: Option<unsafe extern "C" fn(*mut c_void, u32) -> i32>,
    undo_ctx: *mut c_void,
) -> i32 {
    catch_panic!("xUndo", {
        assert!(!wal.is_null());
        let wal = unsafe { &mut *wal };
        let orig_methods = get_orig_methods::<T>(wal);
        let orig_xundo = orig_methods.xUndo.unwrap();
        T::on_undo(wal, func, undo_ctx, orig_xundo)
    })
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepoint<T: WalHook>(wal: *mut Wal, wal_data: *mut u32) {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xSavepoint.unwrap())(wal, wal_data) }
}

#[allow(non_snake_case)]
pub extern "C" fn xSavepointUndo<T: WalHook>(wal: *mut Wal, wal_data: *mut u32) -> i32 {
    catch_panic!("xSavepointUndo", {
        let wal = unsafe { &mut *wal };
        let orig_methods = get_orig_methods::<T>(wal);
        let orig_xsavepointundo = orig_methods.xSavepointUndo.unwrap();
        T::on_savepoint_undo(wal, wal_data, orig_xsavepointundo)
    })
}

#[allow(non_snake_case)]
pub extern "C" fn xFrames<T: WalHook>(
    wal: *mut Wal,
    page_size: c_int,
    page_headers: *mut PgHdr,
    size_after: u32,
    is_commit: c_int,
    sync_flags: c_int,
) -> c_int {
    catch_panic!("xFrames", {
        assert!(!wal.is_null());
        let wal = unsafe { &mut *wal };
        let orig_methods = get_orig_methods::<T>(wal);
        let orig_xframe = orig_methods.xFrames.unwrap();

        T::on_frames(
            wal,
            page_size,
            page_headers,
            size_after,
            is_commit,
            sync_flags,
            orig_xframe,
        )
    })
}

#[tracing::instrument(skip(wal, db))]
#[allow(non_snake_case)]
pub extern "C" fn xCheckpoint<T: WalHook>(
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
    catch_panic!("xCheckpoint", {
        let wal = unsafe { &mut *wal };
        let orig_methods = get_orig_methods::<T>(wal);
        let orig_xcheckpoint = orig_methods.xCheckpoint.unwrap();
        T::on_checkpoint(
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
            orig_xcheckpoint,
        )
    })
}

#[allow(non_snake_case)]
pub extern "C" fn xCallback<T: WalHook>(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xCallback.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xExclusiveMode<T: WalHook>(wal: *mut Wal, op: c_int) -> c_int {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xExclusiveMode.unwrap())(wal, op) }
}

#[allow(non_snake_case)]
pub extern "C" fn xHeapMemory<T: WalHook>(wal: *mut Wal) -> i32 {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xHeapMemory.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xFile<T: WalHook>(wal: *mut Wal) -> *mut sqlite3_file {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xFile.unwrap())(wal) }
}

#[allow(non_snake_case)]
pub extern "C" fn xDb<T: WalHook>(wal: *mut Wal, db: *mut rusqlite::ffi::sqlite3) {
    let orig_methods = unsafe { get_orig_methods::<T>(&mut *wal) };
    unsafe { (orig_methods.xDb.unwrap())(wal, db) }
}

#[allow(non_snake_case)]
pub extern "C" fn xPathnameLen<T: WalHook>(orig_len: i32) -> i32 {
    orig_len + 4
}

#[allow(non_snake_case)]
pub extern "C" fn xGetPathname<T: WalHook>(buf: *mut c_char, orig: *const c_char, orig_len: c_int) {
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
pub extern "C" fn xPreMainDbOpen<T: WalHook>(
    methods: *mut libsql_wal_methods,
    path: *const c_char,
) -> i32 {
    let orig_methods = unsafe { &*(*(methods as *mut WalMethodsHook<T>)).underlying_methods };
    unsafe { (orig_methods.xPreMainDbOpen.unwrap())(methods, path) }
}

unsafe impl<T> Send for WalMethodsHook<T> {}
unsafe impl<T> Sync for WalMethodsHook<T> {}

#[repr(C)]
#[allow(non_snake_case)]
pub struct WalMethodsHook<T> {
    pub methods: libsql_wal_methods,
    // user data
    underlying_methods: *mut libsql_wal_methods,
    _pth: PhantomData<T>,
}
