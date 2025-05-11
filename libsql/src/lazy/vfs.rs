use std::{
    marker::PhantomData,
    time::{Duration, SystemTime},
};

use anyhow::anyhow;
use libsql_sys::ffi;
use rand::RngCore;

/// sqlite3 can return extended code on success (like SQLITE_OK_SYMLINK)
/// in order to be fully compatible we must return full code on success too
#[derive(Debug)]
pub struct VfsResult<T> {
    pub value: T,
    pub rc: i32,
}

#[derive(thiserror::Error, Debug)]
pub enum VfsError {
    #[error("sqlite: rc={rc}, message={message}")]
    Sqlite { rc: i32, message: String },
    #[error("anyhow: msg={0}")]
    Anyhow(anyhow::Error),
}

pub trait VfsFile {
    fn path(&self) -> Option<&str>;

    // See https://www.sqlite.org/c3ref/io_methods.html for IO methods reference
    fn close(&mut self) -> Result<VfsResult<()>, VfsError>;
    fn read(&mut self, buf: &mut [u8], offset: i64) -> Result<VfsResult<()>, VfsError>;
    fn write(&mut self, buf: &[u8], offset: i64) -> Result<VfsResult<()>, VfsError>;
    fn truncate(&mut self, size: i64) -> Result<VfsResult<()>, VfsError>;
    fn sync(&mut self, flags: i32) -> Result<VfsResult<()>, VfsError>;
    fn file_size(&mut self) -> Result<VfsResult<i64>, VfsError>;

    fn lock(&mut self, upgrade_to: i32) -> Result<VfsResult<()>, VfsError>;
    fn unlock(&mut self, downgrade_to: i32) -> Result<VfsResult<()>, VfsError>;
    fn check_reserved_lock(&mut self) -> Result<VfsResult<bool>, VfsError>;

    fn file_control(
        &mut self,
        op: i32,
        arg: *mut std::ffi::c_void,
    ) -> Result<VfsResult<()>, VfsError>;
    fn sector_size(&mut self) -> i32;
    fn device_characteristics(&mut self) -> i32;

    fn shm_map(
        &mut self,
        region: i32,
        region_size: i32,
        extend: bool,
    ) -> Result<VfsResult<*mut std::ffi::c_void>, VfsError>;
    fn shm_unmap(&mut self, delete: bool) -> Result<VfsResult<()>, VfsError>;
    fn shm_lock(&mut self, offset: i32, count: i32, flags: i32) -> Result<VfsResult<()>, VfsError>;
    fn shm_barrier(&mut self);
}

// VFS represents *shared* instance of virtual file system manager entry point
// As it is shared - it implements Clone trait and can be safely cloned and passed around
// Internally, VFS wraps internal structures with Rc<Inner> which made cloning cheap and zero-alloc
pub trait Vfs: Clone
where
    Self: Sized,
{
    // File type must have C data layout with (*mut ffi::sqlite3_io_methods) pointer as a first field of the struct
    type File: VfsFile;

    // zero-terminated static string which will be used to register VFS in sqlite3 DB
    fn name(&self) -> &std::ffi::CStr;
    fn max_pathname(&self) -> i32;

    // See https://www.sqlite.org/c3ref/vfs.html for VFS methods reference
    fn open(
        &self,
        filename: Option<&std::ffi::CStr>,
        flags: i32,
        file: &mut Self::File,
    ) -> Result<VfsResult<i32>, VfsError>;
    fn delete(&self, filename: &std::ffi::CStr, sync_dir: bool) -> Result<VfsResult<()>, VfsError>;
    fn access(&self, filename: &std::ffi::CStr, flags: i32) -> Result<VfsResult<bool>, VfsError>;
    fn full_pathname(
        &self,
        filename: &std::ffi::CStr,
        full_buffer: &mut [u8],
    ) -> Result<VfsResult<()>, VfsError>;
    fn sleep(&self, duration: Duration) -> Result<VfsResult<()>, VfsError>;
}

pub type VfsName = std::ffi::CString;

/// Struct that holds all necessary objects after successfull registration
/// Instance of this struct must be valid for the lifetime of the sqlite3 db after successful registration
pub struct RegisteredVfs<V: Vfs> {
    native_vfs_struct: Box<ffi::sqlite3_vfs>,
    _phantom: PhantomData<V>,
}

impl<V: Vfs> Drop for RegisteredVfs<V> {
    fn drop(&mut self) {
        unsafe {
            ffi::sqlite3_vfs_unregister(&mut *self.native_vfs_struct as *mut ffi::sqlite3_vfs);
            let _ = Box::from_raw(self.native_vfs_struct.pAppData as *mut V);
        }
    }
}

pub fn register_vfs<V: Vfs>(vfs: V) -> anyhow::Result<RegisteredVfs<V>> {
    let vfs = Box::new(vfs);
    let mut native_vfs_struct = sqlite3_vfs(vfs);
    let rc = unsafe { ffi::sqlite3_vfs_register(&mut *native_vfs_struct, 0) };
    if rc != ffi::SQLITE_OK {
        Err(anyhow!("register failed: {}", rc))
    } else {
        Ok(RegisteredVfs {
            native_vfs_struct,
            _phantom: PhantomData,
        })
    }
}

fn sqlite3_vfs<V: Vfs>(vfs: Box<V>) -> Box<ffi::sqlite3_vfs> {
    Box::new(ffi::sqlite3_vfs {
        iVersion: 3,
        szOsFile: size_of::<V::File>() as i32,
        mxPathname: vfs.max_pathname(),
        pNext: std::ptr::null_mut(),
        zName: vfs.name().as_ptr() as *const std::ffi::c_char,
        pAppData: Box::into_raw(vfs) as *mut std::ffi::c_void,
        xOpen: Some(xOpen::<V>),
        xDelete: Some(xDelete::<V>),
        xAccess: Some(xAccess::<V>),
        xFullPathname: Some(xFullPathname::<V>),
        xCurrentTime: Some(xCurrentTime::<V>),
        xCurrentTimeInt64: Some(xCurrentTimeInt64::<V>),

        xSleep: Some(xSleep::<V>),
        xGetLastError: Some(xGetLastError::<V>),
        xRandomness: Some(xRandomness::<V>),

        // It's fine to omit these methods as these are "Interfaces for opening a shared library"
        xDlOpen: None,
        xDlError: None,
        xDlSym: None,
        xDlClose: None,

        // It's fine to omit these methods as "The xSetSystemCall(), xGetSystemCall(), and xNestSystemCall() interfaces are not used by the SQLite core"
        xSetSystemCall: None,
        xGetSystemCall: None,
        xNextSystemCall: None,
    })
}

fn sqlite3_io_methods<V: Vfs>() -> &'static ffi::sqlite3_io_methods {
    &ffi::sqlite3_io_methods {
        iVersion: 2,
        xClose: Some(xClose::<V>),
        xRead: Some(xRead::<V>),
        xWrite: Some(xWrite::<V>),
        xTruncate: Some(xTruncate::<V>),
        xSync: Some(xSync::<V>),
        xFileSize: Some(xFileSize::<V>),
        xLock: Some(xLock::<V>),
        xUnlock: Some(xUnlock::<V>),
        xCheckReservedLock: Some(xCheckReservedLock::<V>),
        xFileControl: Some(xFileControl::<V>),
        xSectorSize: Some(xSectorSize::<V>),
        xDeviceCharacteristics: Some(xDeviceCharacteristics::<V>),
        xShmMap: Some(xShmMap::<V>),
        xShmLock: Some(xShmLock::<V>),
        xShmBarrier: Some(xShmBarrier::<V>),
        xShmUnmap: Some(xShmUnmap::<V>),
        xFetch: None,
        xUnfetch: None,
    }
}

fn convert_err_to_rc(e: &VfsError) -> i32 {
    match e {
        VfsError::Sqlite { rc, .. } => *rc,
        VfsError::Anyhow(_) => {
            tracing::error!("vfs error: {:?}", e);
            ffi::SQLITE_IOERR
        }
    }
}

pub fn convert_rc_result<T>(rc: i32, value: T, message: &str) -> Result<VfsResult<T>, VfsError> {
    // "The least significant 8 bits of the result code define a broad category and are called the "primary result code"
    // see https://www.sqlite.org/rescode.html#primary_result_codes_versus_extended_result_codes
    if (rc & 0xff) != ffi::SQLITE_OK {
        let message = message.to_string();
        Err(VfsError::Sqlite { rc, message })
    } else {
        Ok(VfsResult { rc, value })
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xOpen<V: Vfs>(
    arg1: *mut ffi::sqlite3_vfs,
    zName: ffi::sqlite3_filename,
    arg2: *mut ffi::sqlite3_file,
    flags: std::os::raw::c_int,
    pOutFlags: *mut std::os::raw::c_int,
) -> std::os::raw::c_int {
    // SQLite expects that the sqlite3_file.pMethods element will be valid after xOpen returns regardless of the success or failure of the xOpen call
    (*arg2).pMethods = std::ptr::null();

    let vfs = (*arg1).pAppData as *mut V;
    let vfs_file = arg2 as *mut V::File;
    let name = if !zName.is_null() {
        Some(std::ffi::CStr::from_ptr(zName))
    } else {
        None
    };

    match (*vfs).open(name, flags, &mut *vfs_file) {
        Ok(VfsResult { value, rc }) => {
            if !pOutFlags.is_null() {
                (*pOutFlags) = value;
            }
            (*arg2).pMethods =
                std::ptr::from_ref(sqlite3_io_methods::<V>()) as *mut ffi::sqlite3_io_methods;

            rc
        }
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xDelete<V: Vfs>(
    arg1: *mut ffi::sqlite3_vfs,
    zName: *const ::std::os::raw::c_char,
    syncDir: std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs = (*arg1).pAppData as *mut V;
    let name = std::ffi::CStr::from_ptr(zName);

    match (*vfs).delete(name, syncDir != 0) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xAccess<V: Vfs>(
    arg1: *mut ffi::sqlite3_vfs,
    zName: *const ::std::os::raw::c_char,
    flags: std::os::raw::c_int,
    pResOut: *mut std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs = (*arg1).pAppData as *mut V;
    let name = std::ffi::CStr::from_ptr(zName);

    match (*vfs).access(name, flags) {
        Ok(VfsResult { value, rc }) => {
            *pResOut = if value { 1 } else { 0 };
            rc
        }
        Err(e) => convert_err_to_rc(&e),
    }
}

// This logic ported from SQLite implementation: https://github.com/sqlite/sqlite/blob/98772d6e75f4033373c806e4e44f675971e55e38/src/os_unix.c#L6908
// The logic here is to calculate current time and date as a Julian Day number
#[allow(non_snake_case)]
unsafe extern "C" fn xCurrentTime<V: Vfs>(
    _arg1: *mut ffi::sqlite3_vfs,
    arg2: *mut f64,
) -> ::std::os::raw::c_int {
    let mut now: ffi::sqlite3_int64 = 0;
    let rc = xCurrentTimeInt64::<V>(_arg1, &mut now);
    if rc == ffi::SQLITE_OK {
        *arg2 = (now as f64) / 86400000.0;
    }
    rc
}

// This logic ported from SQLite implementation: https://github.com/sqlite/sqlite/blob/98772d6e75f4033373c806e4e44f675971e55e38/src/os_unix.c#L6876
// The logic here is to calculate the number of milliseconds since the Julian epoch of noon in Greenwich on November 24, 4714 B.C according to the proleptic Gregorian calendar
#[allow(non_snake_case)]
#[allow(clippy::extra_unused_type_parameters)]
unsafe extern "C" fn xCurrentTimeInt64<V: Vfs>(
    _arg1: *mut ffi::sqlite3_vfs,
    arg2: *mut ffi::sqlite3_int64,
) -> ::std::os::raw::c_int {
    const JULIAN_EPOCH_OFFSET: i64 = 24405875i64 * 8640000i64;
    let Ok(unix_epoch_time) = SystemTime::now().duration_since(std::time::UNIX_EPOCH) else {
        return ffi::SQLITE_ERROR;
    };
    *arg2 = JULIAN_EPOCH_OFFSET + unix_epoch_time.as_millis() as i64;
    ffi::SQLITE_OK
}

#[allow(non_snake_case)]
unsafe extern "C" fn xSleep<V: Vfs>(
    arg1: *mut ffi::sqlite3_vfs,
    microseconds: ::std::os::raw::c_int,
) -> ::std::os::raw::c_int {
    let vfs = (*arg1).pAppData as *mut V;
    match (*vfs).sleep(Duration::from_micros(microseconds as u64)) {
        Ok(VfsResult { value: (), rc }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
#[allow(clippy::extra_unused_type_parameters)]
unsafe extern "C" fn xGetLastError<V: Vfs>(
    _arg1: *mut ffi::sqlite3_vfs,
    _arg2: ::std::os::raw::c_int,
    _arg3: *mut ::std::os::raw::c_char,
) -> ::std::os::raw::c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

#[allow(non_snake_case)]
#[allow(clippy::extra_unused_type_parameters)]
unsafe extern "C" fn xRandomness<V: Vfs>(
    _arg1: *mut ffi::sqlite3_vfs,
    nByte: ::std::os::raw::c_int,
    zOut: *mut ::std::os::raw::c_char,
) -> ::std::os::raw::c_int {
    // this is unexpected as RANDOM() seems to not use xRandomness from VFS
    rand::thread_rng().fill_bytes(std::slice::from_raw_parts_mut(
        zOut as *mut u8,
        nByte as usize,
    ));
    ffi::SQLITE_OK
}

#[allow(non_snake_case)]
unsafe extern "C" fn xFullPathname<V: Vfs>(
    arg1: *mut ffi::sqlite3_vfs,
    zName: *const std::os::raw::c_char,
    nOut: std::os::raw::c_int,
    zOut: *mut std::os::raw::c_char,
) -> std::os::raw::c_int {
    let vfs = (*arg1).pAppData as *mut V;
    let name = std::ffi::CStr::from_ptr(zName);
    let buffer = unsafe { std::slice::from_raw_parts_mut::<u8>(zOut as *mut u8, nOut as usize) };
    match (*vfs).full_pathname(name, buffer) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xClose<V: Vfs>(arg1: *mut ffi::sqlite3_file) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).close() {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xRead<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    arg2: *mut ::std::os::raw::c_void,
    iAmt: std::os::raw::c_int,
    iOfst: ffi::sqlite3_int64,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    let buffer = unsafe { std::slice::from_raw_parts_mut::<u8>(arg2 as *mut u8, iAmt as usize) };
    match (*vfs_file).read(buffer, iOfst) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xWrite<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    arg2: *const ::std::os::raw::c_void,
    iAmt: ::std::os::raw::c_int,
    iOfst: ffi::sqlite3_int64,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    let buffer = unsafe { std::slice::from_raw_parts_mut::<u8>(arg2 as *mut u8, iAmt as usize) };
    match (*vfs_file).write(buffer, iOfst) {
        Ok(VfsResult { rc, .. }) => ffi::SQLITE_OK,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xTruncate<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    size: ffi::sqlite3_int64,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).truncate(size) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xSync<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    flags: ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).sync(flags) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xFileSize<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    pSize: *mut ffi::sqlite3_int64,
) -> ::std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).file_size() {
        Ok(VfsResult { value, rc }) => {
            *pSize = value;
            rc
        }
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xLock<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    arg2: ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).lock(arg2) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xUnlock<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    arg2: ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).unlock(arg2) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xCheckReservedLock<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    pResOut: *mut ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).check_reserved_lock() {
        Ok(VfsResult { value, rc }) => {
            *pResOut = if value { 1 } else { 0 };
            rc
        }
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xFileControl<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    op: ::std::os::raw::c_int,
    pArg: *mut ::std::os::raw::c_void,
) -> ::std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).file_control(op, pArg) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xSectorSize<V: Vfs>(arg1: *mut ffi::sqlite3_file) -> ::std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    let sector_size = (*vfs_file).sector_size();
    sector_size
}

#[allow(non_snake_case)]
unsafe extern "C" fn xDeviceCharacteristics<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
) -> ::std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    let device_characteristics = (*vfs_file).device_characteristics();
    device_characteristics
}

#[allow(non_snake_case)]
unsafe extern "C" fn xShmMap<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    iPg: ::std::os::raw::c_int,
    pgsz: ::std::os::raw::c_int,
    arg2: ::std::os::raw::c_int,
    arg3: *mut *mut ::std::os::raw::c_void,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).shm_map(iPg, pgsz, arg2 != 0) {
        Ok(VfsResult { rc, value }) => {
            if !arg3.is_null() {
                unsafe { *arg3 = value };
            }
            rc
        }
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xShmLock<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    offset: ::std::os::raw::c_int,
    n: ::std::os::raw::c_int,
    flags: ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).shm_lock(offset, n, flags) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}

#[allow(non_snake_case)]
unsafe extern "C" fn xShmBarrier<V: Vfs>(arg1: *mut ffi::sqlite3_file) {
    let vfs_file: *mut <V as Vfs>::File = arg1 as *mut V::File;
    (*vfs_file).shm_barrier();
}

#[allow(non_snake_case)]
unsafe extern "C" fn xShmUnmap<V: Vfs>(
    arg1: *mut ffi::sqlite3_file,
    deleteFlag: ::std::os::raw::c_int,
) -> std::os::raw::c_int {
    let vfs_file = arg1 as *mut V::File;
    match (*vfs_file).shm_unmap(deleteFlag != 0) {
        Ok(VfsResult { rc, .. }) => rc,
        Err(e) => convert_err_to_rc(&e),
    }
}
