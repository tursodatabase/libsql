use std::{mem::ManuallyDrop, sync::Arc};

use anyhow::anyhow;
use lazy_static::lazy_static;
use libsql_sys::ffi;
use tokio::runtime::Runtime;

use super::{
    lazy::{PageServer, PullPagesReqBody},
    vfs::{Vfs, VfsError, VfsFile, VfsResult},
};

struct LazyVfsInner<V: Vfs, Page: PageServer> {
    vfs: V,
    name: std::ffi::CString,
    page_server: Arc<Page>,
}

pub struct LazyVfs<V: Vfs, P: PageServer> {
    inner: Arc<LazyVfsInner<V, P>>,
}

impl<V: Vfs, P: PageServer> Clone for LazyVfs<V, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[repr(C)]
pub struct LazyVfsFile<V: Vfs + Send + Sync, P: PageServer + Send + Sync> {
    file: ManuallyDrop<V::File>,
    inner: ManuallyDrop<Arc<LazyVfsInner<V, P>>>,
}

impl<V: Vfs, P: PageServer> LazyVfs<V, P> {
    pub fn new(name: &str, vfs: V, page_server: Arc<P>) -> Self {
        let name = std::ffi::CString::new(name).unwrap();
        Self {
            inner: Arc::new(LazyVfsInner {
                vfs,
                name,
                page_server,
            }),
        }
    }
}

const PAGE_SIZE: i64 = 4096;

lazy_static! {
    static ref RT: Runtime = tokio::runtime::Runtime::new().unwrap();
}

impl<V: Vfs + Send + Sync, P: PageServer + Send + Sync + 'static> VfsFile for LazyVfsFile<V, P> {
    fn path(&self) -> Option<&str> {
        self.file.path()
    }

    fn close(&mut self) -> Result<VfsResult<()>, VfsError> {
        self.file.close()
    }

    fn read(&mut self, buf: &mut [u8], offset: i64) -> Result<VfsResult<()>, VfsError> {
        tracing::info!("read: {}", offset);
        if self.file.path().is_some() && self.file.path().unwrap().ends_with(".db") {
            assert!(buf.len() > 0);
            let start_page_no = offset / PAGE_SIZE;
            let end_page_no = (offset + buf.len() as i64 - 1) / PAGE_SIZE;
            assert!(start_page_no == end_page_no);

            let page_no = (start_page_no as usize) + 1;
            let page_server = self.inner.page_server.clone();
            let revision = page_server.get_revision();
            if revision != "" {
                let pages = futures::executor::block_on({
                    page_server.pull_pages(&PullPagesReqBody {
                        start_revision: None,
                        end_revision: revision.clone(),
                        server_pages: vec![page_no],
                        client_pages: vec![],
                        accept_encodings: vec!["zstd".into()],
                    })
                })
                .map_err(|e| VfsError::Anyhow(e.into()))?;

                tracing::info!(
                    "got pages(revision={}): {} {}",
                    revision,
                    pages[0].0,
                    pages[0].1.len()
                );
                buf.copy_from_slice(&pages[0].1);
                return Ok(VfsResult {
                    value: (),
                    rc: ffi::SQLITE_OK,
                });
            }
        }
        self.file.read(buf, offset)
    }

    fn write(&mut self, buf: &[u8], offset: i64) -> Result<VfsResult<()>, VfsError> {
        tracing::info!("write");
        self.file.write(buf, offset)
    }

    fn truncate(&mut self, size: i64) -> Result<VfsResult<()>, VfsError> {
        tracing::info!("truncate");
        self.file.truncate(size)
    }

    fn sync(&mut self, flags: i32) -> Result<VfsResult<()>, VfsError> {
        self.file.sync(flags)
    }

    fn file_size(&mut self) -> Result<VfsResult<i64>, VfsError> {
        if self.inner.page_server.get_revision() != "" {
            return Ok(VfsResult {
                value: 533 * 4096,
                rc: ffi::SQLITE_OK,
            });
        }
        let file_size = self.file.file_size();
        tracing::info!("file_size: {:?}", file_size);
        file_size
    }

    fn lock(&mut self, upgrade_to: i32) -> Result<VfsResult<()>, VfsError> {
        self.file.lock(upgrade_to)
    }

    fn unlock(&mut self, downgrade_to: i32) -> Result<VfsResult<()>, VfsError> {
        self.file.unlock(downgrade_to)
    }

    fn check_reserved_lock(&mut self) -> Result<VfsResult<bool>, VfsError> {
        self.file.check_reserved_lock()
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn file_control(
        &mut self,
        op: i32,
        arg: *mut std::ffi::c_void,
    ) -> Result<VfsResult<()>, VfsError> {
        self.file.file_control(op, arg)
    }

    fn sector_size(&mut self) -> i32 {
        self.file.sector_size()
    }

    fn device_characteristics(&mut self) -> i32 {
        self.file.device_characteristics()
    }

    fn shm_map(
        &mut self,
        region: i32,
        region_size: i32,
        extend: bool,
    ) -> Result<VfsResult<*mut std::ffi::c_void>, VfsError> {
        self.file.shm_map(region, region_size, extend)
    }

    fn shm_unmap(&mut self, delete: bool) -> Result<VfsResult<()>, VfsError> {
        self.file.shm_unmap(delete)
    }

    fn shm_lock(&mut self, offset: i32, count: i32, flags: i32) -> Result<VfsResult<()>, VfsError> {
        self.file.shm_lock(offset, count, flags)
    }

    fn shm_barrier(&mut self) {
        self.file.shm_barrier();
    }
}

impl<V: Vfs + Send + Sync, P: PageServer + Send + Sync + 'static> Vfs for LazyVfs<V, P> {
    type File = LazyVfsFile<V, P>;

    fn name(&self) -> &std::ffi::CStr {
        &self.inner.name
    }

    fn max_pathname(&self) -> i32 {
        self.inner.vfs.max_pathname()
    }

    fn open(
        &self,
        filename: Option<&std::ffi::CStr>,
        flags: i32,
        file: &mut Self::File,
    ) -> Result<VfsResult<i32>, VfsError> {
        file.inner = ManuallyDrop::new(self.inner.clone());
        self.inner.vfs.open(filename, flags, &mut file.file)
    }

    fn delete(&self, filename: &std::ffi::CStr, sync_dir: bool) -> Result<VfsResult<()>, VfsError> {
        self.inner.vfs.delete(filename, sync_dir)
    }

    fn access(&self, filename: &std::ffi::CStr, flags: i32) -> Result<VfsResult<bool>, VfsError> {
        self.inner.vfs.access(filename, flags)
    }

    fn full_pathname(
        &self,
        filename: &std::ffi::CStr,
        full_buffer: &mut [u8],
    ) -> Result<VfsResult<()>, VfsError> {
        self.inner.vfs.full_pathname(filename, full_buffer)
    }

    fn sleep(&self, duration: std::time::Duration) -> Result<VfsResult<()>, VfsError> {
        self.inner.vfs.sleep(duration)
    }
}
