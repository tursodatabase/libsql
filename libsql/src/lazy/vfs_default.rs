use std::{rc::Rc, sync::Arc};

use libsql_sys::ffi;
use tokio::sync::Mutex;

use super::vfs::{convert_rc_result, Vfs, VfsError, VfsFile, VfsResult};

static DEFAULT_MAX_PATH_LENGTH: i32 = 1024;

#[repr(C)]
struct Sqlite3VfsInner {
    vfs: *mut ffi::sqlite3_vfs,
    name: std::ffi::CString,
}

pub struct Sqlite3Vfs {
    inner: Arc<Sqlite3VfsInner>,
}

impl Clone for Sqlite3Vfs {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

unsafe impl Send for Sqlite3Vfs {}
unsafe impl Sync for Sqlite3Vfs {}

// Wrapper around VFS returned by SQLite which help this crate to get full control over VFS execution
// So, we will install our custom io_methods which will be proxied to pMethods of inner sqlite_file
// Created by original VFS xOpen method
#[repr(C)]
pub struct Sqlite3VfsFile {
    io_methods: *mut ffi::sqlite3_io_methods,
    z_filename: *const std::ffi::c_char,
    inner: *mut ffi::sqlite3_file,
    inner_layout: std::alloc::Layout,
}

impl VfsFile for Sqlite3VfsFile {
    fn path(&self) -> Option<&str> {
        if self.z_filename.is_null() {
            None
        } else {
            unsafe { std::ffi::CStr::from_ptr(self.z_filename).to_str() }.ok()
        }
    }

    fn close(&mut self) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xClose.unwrap()(self.inner) };
        unsafe { std::alloc::dealloc(self.inner as *mut u8, self.inner_layout) };

        convert_rc_result(rc, (), "sqlite3_file::close failed")
    }

    fn read(&mut self, buf: &mut [u8], offset: i64) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*(*self.inner).pMethods).xRead.unwrap()(
                self.inner,
                buf.as_mut_ptr() as *mut std::ffi::c_void,
                buf.len() as i32,
                offset,
            )
        };
        convert_rc_result(rc, (), "sqlite3_file::read failed")
    }

    fn write(&mut self, buf: &[u8], offset: i64) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*(*self.inner).pMethods).xWrite.unwrap()(
                self.inner,
                buf.as_ptr() as *mut std::ffi::c_void,
                buf.len() as i32,
                offset,
            )
        };
        convert_rc_result(rc, (), "sqlite3_file::write failed")
    }

    fn truncate(&mut self, size: i64) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xTruncate.unwrap()(self.inner, size) };
        convert_rc_result(rc, (), "sqlite3_file::truncate failed")
    }

    fn sync(&mut self, flags: i32) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xSync.unwrap()(self.inner, flags) };
        convert_rc_result(rc, (), "sqlite3_file::sync failed")
    }

    fn file_size(&mut self) -> Result<VfsResult<i64>, VfsError> {
        let mut result: i64 = 0;
        let rc = unsafe { (*(*self.inner).pMethods).xFileSize.unwrap()(self.inner, &mut result) };
        convert_rc_result(rc, result, "sqlite3_file::file_size failed")
    }

    fn lock(&mut self, upgrade_to: i32) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xLock.unwrap()(self.inner, upgrade_to) };
        convert_rc_result(rc, (), "sqlite3_file::lock failed")
    }

    fn unlock(&mut self, downgrade_to: i32) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xUnlock.unwrap()(self.inner, downgrade_to) };
        convert_rc_result(rc, (), "sqlite3_file::unlock failed")
    }

    fn check_reserved_lock(&mut self) -> Result<VfsResult<bool>, VfsError> {
        let mut result: i32 = 0;
        let rc = unsafe {
            (*(*self.inner).pMethods).xCheckReservedLock.unwrap()(self.inner, &mut result)
        };
        convert_rc_result(rc, result != 0, "sqlite3_file::check_reserved_lock failed")
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn file_control(
        &mut self,
        op: i32,
        arg: *mut std::ffi::c_void,
    ) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe { (*(*self.inner).pMethods).xFileControl.unwrap()(self.inner, op, arg) };
        convert_rc_result(rc, (), "sqlite3_file::file_control failed")
    }

    fn sector_size(&mut self) -> i32 {
        unsafe { (*(*self.inner).pMethods).xSectorSize.unwrap()(self.inner) }
    }

    fn device_characteristics(&mut self) -> i32 {
        unsafe { (*(*self.inner).pMethods).xDeviceCharacteristics.unwrap()(self.inner) }
    }

    fn shm_map(
        &mut self,
        region: i32,
        region_size: i32,
        extend: bool,
    ) -> Result<VfsResult<*mut std::ffi::c_void>, VfsError> {
        let mut mapped: *mut std::ffi::c_void = std::ptr::null_mut();
        let rc = unsafe {
            (*(*self.inner).pMethods).xShmMap.unwrap()(
                self.inner,
                region,
                region_size,
                if extend { 1 } else { 0 },
                &mut mapped,
            )
        };
        convert_rc_result(rc, mapped, "sqlite3_file::shm_map failed")
    }

    fn shm_unmap(&mut self, delete: bool) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*(*self.inner).pMethods).xShmUnmap.unwrap()(self.inner, if delete { 1 } else { 0 })
        };
        convert_rc_result(rc, (), "sqlite3_file::shm_unmap failed")
    }

    fn shm_lock(&mut self, offset: i32, count: i32, flags: i32) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*(*self.inner).pMethods).xShmLock.unwrap()(self.inner, offset, count, flags)
        };
        convert_rc_result(rc, (), "sqlite3_file::shm_lock failed")
    }

    fn shm_barrier(&mut self) {
        unsafe { (*(*self.inner).pMethods).xShmBarrier.unwrap()(self.inner) };
    }
}

impl Vfs for Sqlite3Vfs {
    type File = Sqlite3VfsFile;

    fn name(&self) -> &std::ffi::CStr {
        &self.inner.name
    }

    fn max_pathname(&self) -> i32 {
        DEFAULT_MAX_PATH_LENGTH
    }

    fn open(
        &self,
        filename: Option<&std::ffi::CStr>,
        flags: i32,
        file: &mut Self::File,
    ) -> Result<VfsResult<i32>, VfsError> {
        let sz_os_file = unsafe { (*self.inner.vfs).szOsFile as usize };
        let filename_ptr = filename.map(|x| x.as_ptr()).unwrap_or(std::ptr::null());
        let file_layout = std::alloc::Layout::from_size_align(sz_os_file, 8).unwrap();
        let file_ptr = unsafe { std::alloc::alloc(file_layout) } as *mut ffi::sqlite3_file;

        let mut result: i32 = 0;
        let rc = unsafe {
            (*self.inner.vfs).xOpen.unwrap()(
                self.inner.vfs,
                filename_ptr,
                file_ptr,
                flags,
                &mut result,
            )
        };
        if rc == ffi::SQLITE_OK {
            file.inner = file_ptr;
            file.inner_layout = file_layout;
            file.z_filename = filename_ptr;
        } else {
            unsafe { std::alloc::dealloc(file_ptr as *mut u8, file_layout) };
        }
        convert_rc_result(rc, result, "sqlite3_vfs::open failed")
    }

    fn delete(&self, filename: &std::ffi::CStr, sync_dir: bool) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*self.inner.vfs).xDelete.unwrap()(
                self.inner.vfs,
                filename.as_ptr(),
                if sync_dir { 1 } else { 0 },
            )
        };
        convert_rc_result(rc, (), "sqlite3_vfs::delete failed")
    }

    fn access(&self, filename: &std::ffi::CStr, flags: i32) -> Result<VfsResult<bool>, VfsError> {
        let mut result: i32 = 0;
        let rc = unsafe {
            (*self.inner.vfs).xAccess.unwrap()(
                self.inner.vfs,
                filename.as_ptr(),
                flags,
                &mut result,
            )
        };
        convert_rc_result(rc, result != 0, "sqlite3_vfs::access failed")
    }

    fn full_pathname(
        &self,
        filename: &std::ffi::CStr,
        full_buffer: &mut [u8],
    ) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*self.inner.vfs).xFullPathname.unwrap()(
                self.inner.vfs,
                filename.as_ptr(),
                full_buffer.len() as i32,
                full_buffer.as_mut_ptr() as *mut std::ffi::c_char,
            )
        };
        convert_rc_result(rc, (), "sqlite3_vfs::full_pathname failed")
    }

    fn sleep(&self, duration: std::time::Duration) -> Result<VfsResult<()>, VfsError> {
        let rc = unsafe {
            (*self.inner.vfs).xSleep.unwrap()(self.inner.vfs, duration.as_micros() as i32)
        };
        convert_rc_result(rc, (), "sqlite3_vfs::sleep failed")
    }
}

pub fn get_default_vfs(name: &str) -> Sqlite3Vfs {
    let vfs = unsafe { ffi::sqlite3_vfs_find(std::ptr::null()) };
    assert!(!vfs.is_null(), "default vfs is not found");
    let inner = Arc::new(Sqlite3VfsInner {
        vfs,
        name: std::ffi::CString::new(name).unwrap(),
    });
    Sqlite3Vfs { inner }
}
