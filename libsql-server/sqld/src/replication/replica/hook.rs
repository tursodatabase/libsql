use std::os::unix::prelude::FileExt;
use std::{cell::RefCell, ffi::c_int, fs::File, rc::Rc};

use bytemuck::bytes_of;
use rusqlite::ffi::{PgHdr, SQLITE_ERROR};
use sqld_libsql_bindings::ffi::Wal;
use sqld_libsql_bindings::{ffi::types::XWalFrameFn, wal_hook::WalHook};

use crate::replication::frame::{Frame, FrameBorrowed};
use crate::replication::{FrameNo, WAL_PAGE_SIZE};

use super::meta::WalIndexMeta;
use super::snapshot::TempSnapshot;

#[derive(Debug)]
pub enum Frames {
    Vec(Vec<Frame>),
    Snapshot(TempSnapshot),
}

impl Frames {
    fn to_headers(&self) -> (*mut PgHdr, u64, u32) {
        match self {
            Frames::Vec(frames) => make_page_header(frames.iter().map(|f| &**f)),
            Frames::Snapshot(snap) => make_page_header(snap.iter()),
        }
    }
}

/// The injector hook hijacks a call to xframes, and replace the content of the call with it's own
/// frames.
/// The Caller must first call `set_frames`, passing the frames to be injected, then trigger a call
/// to xFrames from the libsql connection (see dummy write in `injector`), and can then collect the
/// result on the injection with `take_result`
#[derive(Clone)]
pub struct InjectorHook {
    inner: Rc<RefCell<InjectorHookInner>>,
}

impl InjectorHook {
    pub fn new(meta_file: File, meta: WalIndexMeta) -> Self {
        Self {
            inner: Rc::new(RefCell::new(InjectorHookInner {
                current_frames: None,
                result: None,
                meta_file,
                meta,
            })),
        }
    }

    fn with_inner_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut InjectorHookInner) -> R,
    {
        f(&mut self.inner.borrow_mut())
    }

    /// Set the hook's current frames
    pub fn set_frames(&self, frames: Frames) {
        self.with_inner_mut(|this| this.current_frames.replace(frames));
    }

    /// Take the result currently held by the hook.
    /// Panics if there is no result
    pub fn take_result(&self) -> anyhow::Result<FrameNo> {
        self.with_inner_mut(|this| this.result.take().expect("no result to take"))
    }
}

pub struct InjectorHookInner {
    /// slot for the frames to be applied by the next call to xframe
    current_frames: Option<Frames>,
    /// slot to store the result of the call to xframes.
    /// On success, returns the last applied frame_no
    result: Option<anyhow::Result<FrameNo>>,
    meta_file: File,
    meta: WalIndexMeta,
}

impl InjectorHookInner {
    unsafe fn inject_pages(
        &mut self,
        page_headers: *mut PgHdr,
        last_frame_no: u64,
        size_after: u32,
        sync_flags: i32,
        orig: XWalFrameFn,
        wal: *mut Wal,
    ) -> anyhow::Result<()> {
        self.pre_commit(last_frame_no)
            .expect("failed to write pre-commit frame_no");
        let ret = orig(wal, WAL_PAGE_SIZE, page_headers, size_after, 1, sync_flags);

        if ret == 0 {
            debug_assert!(all_applied(page_headers));
            self.post_commit()
                .expect("failed to write post-commit frame_no");
            // remove commited entries.
            tracing::trace!("applied frame batch");

            Ok(())
        } else {
            anyhow::bail!("failed to apply pages");
        }
    }

    /// Set the pre-commit frame_no, and flush the meta file
    fn pre_commit(&mut self, frame_no: u64) -> anyhow::Result<()> {
        self.meta.pre_commit_frame_no = frame_no;
        self.flush_meta()
    }

    /// Set the post-commit value to the pre-commit value.
    fn post_commit(&mut self) -> anyhow::Result<()> {
        self.meta.post_commit_frame_no = self.meta.pre_commit_frame_no;
        self.flush_meta()
    }

    fn flush_meta(&self) -> anyhow::Result<()> {
        self.meta_file.write_all_at(bytes_of(&self.meta), 0)?;

        Ok(())
    }
}

unsafe impl WalHook for InjectorHook {
    fn on_frames(
        &mut self,
        wal: *mut Wal,
        _page_size: c_int,
        _page_headers: *mut PgHdr,
        _size_after: u32,
        _is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        self.with_inner_mut(|this| {
            let Some(ref frames) = this.current_frames.take() else {
                return SQLITE_ERROR;
            };

            let (headers, last_frame_no, size_after) = frames.to_headers();

            // SAFETY: frame headers are valid for the duration of the call of apply_pages
            let result = unsafe {
                this.inject_pages(headers, last_frame_no, size_after, sync_flags, orig, wal)
            };

            free_page_header(headers);

            let result = result.map(|_| last_frame_no);
            this.result.replace(result);

            SQLITE_ERROR
        })
    }
}

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
/// return (headers, last_frame_no, size_after)
fn make_page_header<'a>(frames: impl Iterator<Item = &'a FrameBorrowed>) -> (*mut PgHdr, u64, u32) {
    let mut current_pg = std::ptr::null_mut();
    let mut last_frame_no = 0;
    let mut size_after = 0;

    let mut headers_count = 0;
    for frame in frames {
        if frame.header().frame_no > last_frame_no {
            last_frame_no = frame.header().frame_no;
            size_after = frame.header().size_after;
        }

        let page = PgHdr {
            pPage: std::ptr::null_mut(),
            pData: frame.page().as_ptr() as _,
            pExtra: std::ptr::null_mut(),
            pCache: std::ptr::null_mut(),
            pDirty: current_pg,
            pPager: std::ptr::null_mut(),
            pgno: frame.header().page_no,
            pageHash: 0,
            flags: 0,
            nRef: 0,
            pDirtyNext: std::ptr::null_mut(),
            pDirtyPrev: std::ptr::null_mut(),
        };
        headers_count += 1;
        current_pg = Box::into_raw(Box::new(page));
    }

    tracing::trace!("built {headers_count} page headers");

    (current_pg, last_frame_no, size_after)
}

/// frees the `PgHdr` list pointed at by `h`.
fn free_page_header(h: *const PgHdr) {
    let mut current = h;
    while !current.is_null() {
        let h: Box<PgHdr> = unsafe { Box::from_raw(current as _) };
        current = h.pDirty;
    }
}

/// Debug assertion. Make sure that all the pages have been applied
fn all_applied(headers: *const PgHdr) -> bool {
    let mut current = headers;
    while !current.is_null() {
        unsafe {
            // WAL appended
            if (*current).flags & 0x040 == 0 {
                return false;
            }
            current = (*current).pDirty;
        }
    }

    true
}
