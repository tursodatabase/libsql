use std::ffi::{c_int, CStr};
use std::marker::PhantomData;

use libsql_sys::ffi::{PgHdr, SQLITE_ERROR};
use libsql_sys::init_static_wal_method;
use libsql_sys::types::Wal;
use libsql_sys::{types::XWalFrameFn, wal_hook::WalHook};

use crate::frame::{Frame, FrameBorrowed};
use crate::{FrameNo, WAL_PAGE_SIZE};

use super::snapshot::TempSnapshot;

// Those are custom error codes returned by the replicator hook.
pub const LIBSQL_EXIT_REPLICATION: u32 = 200;
pub const LIBSQL_CONTINUE_REPLICATION: u32 = 201;

#[derive(Debug)]
pub enum Frames {
    Vec(Vec<Frame>),
    Snapshot(TempSnapshot),
}

#[derive(Debug)]
pub struct Headers<'a> {
    ptr: *mut PgHdr,
    _pth: PhantomData<&'a ()>,
}

impl<'a> Headers<'a> {
    // safety: ptr is guaranteed to be valid for 'a
    unsafe fn new(ptr: *mut PgHdr) -> Self {
        Self {
            ptr,
            _pth: PhantomData,
        }
    }

    fn as_ptr(&mut self) -> *mut PgHdr {
        self.ptr
    }

    fn all_applied(&self) -> bool {
        all_applied(self.ptr)
    }
}

impl Drop for Headers<'_> {
    fn drop(&mut self) {
        let mut current = self.ptr;
        while !current.is_null() {
            let h: Box<PgHdr> = unsafe { Box::from_raw(current as _) };
            current = h.pDirty;
        }
    }
}

impl Frames {
    fn to_headers(&self) -> (Headers, u64, u32) {
        match self {
            Frames::Vec(frames) => make_page_header(frames.iter().map(|f| &**f)),
            Frames::Snapshot(snap) => make_page_header(snap.iter()),
        }
    }
}

init_static_wal_method!(INJECTOR_METHODS, InjectorHook);

/// The injector hook hijacks a call to xframes, and replace the content of the call with it's own
/// frames.
/// The Caller must first call `set_frames`, passing the frames to be injected, then trigger a call
/// to xFrames from the libsql connection (see dummy write in `injector`), and can then collect the
/// result on the injection with `take_result`
pub enum InjectorHook {}

pub struct InjectorHookCtx {
    /// slot for the frames to be applied by the next call to xframe
    receiver: tokio::sync::mpsc::Receiver<Frames>,
    /// currently in a txn
    pub is_txn: bool,
    /// invoked before injecting frames
    pre_commit: Box<dyn Fn(FrameNo) -> anyhow::Result<()> + Send>,
    /// invoked after injecting frames
    post_commit: Box<dyn Fn(FrameNo) -> anyhow::Result<()> + Send>,
}

impl InjectorHookCtx {
    pub fn new(
        receiver: tokio::sync::mpsc::Receiver<Frames>,
        pre_commit: impl Fn(FrameNo) -> anyhow::Result<()> + 'static + Send,
        post_commit: impl Fn(FrameNo) -> anyhow::Result<()> + 'static + Send,
    ) -> Self {
        Self {
            receiver,
            is_txn: false,
            pre_commit: Box::new(pre_commit),
            post_commit: Box::new(post_commit),
        }
    }

    fn inject_pages(
        &mut self,
        mut page_headers: Headers,
        last_frame_no: u64,
        size_after: u32,
        sync_flags: i32,
        orig: XWalFrameFn,
        wal: *mut Wal,
    ) -> anyhow::Result<()> {
        self.is_txn = true;
        if size_after != 0 {
            (self.pre_commit)(last_frame_no)?;
        }

        let ret = unsafe {
            orig(
                wal,
                WAL_PAGE_SIZE,
                page_headers.as_ptr(),
                size_after,
                (size_after != 0) as _,
                sync_flags,
            )
        };

        if ret == 0 {
            debug_assert!(page_headers.all_applied());
            if size_after != 0 {
                (self.post_commit)(last_frame_no)?;
                self.is_txn = false;
            }
            tracing::trace!("applied frame batch");

            Ok(())
        } else {
            anyhow::bail!("failed to apply pages");
        }
    }
}

unsafe impl WalHook for InjectorHook {
    type Context = InjectorHookCtx;

    fn on_frames(
        wal: &mut Wal,
        _page_size: c_int,
        _page_headers: *mut PgHdr,
        _size_after: u32,
        _is_commit: c_int,
        sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        let wal_ptr = wal as *mut _;
        let ctx = Self::wal_extract_ctx(wal);
        loop {
            tracing::trace!("Waiting for a frame");
            match ctx.receiver.blocking_recv() {
                Some(frames) => {
                    let (headers, last_frame_no, size_after) = frames.to_headers();
                    let ret = ctx.inject_pages(
                        headers,
                        last_frame_no,
                        size_after,
                        sync_flags,
                        orig,
                        wal_ptr,
                    );

                    if let Err(e) = ret {
                        tracing::error!("replication error: {e}");
                        return SQLITE_ERROR as c_int;
                    }

                    if !ctx.is_txn {
                        return LIBSQL_CONTINUE_REPLICATION as c_int;
                    }
                }
                None => {
                    tracing::warn!("replication channel closed");
                    return LIBSQL_EXIT_REPLICATION as c_int;
                }
            }
        }
    }

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"frame_injector_hook\0").unwrap()
    }
}

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
/// return (headers, last_frame_no, size_after)
fn make_page_header<'a>(
    frames: impl Iterator<Item = &'a FrameBorrowed>,
) -> (Headers<'a>, u64, u32) {
    let mut first_pg: *mut PgHdr = std::ptr::null_mut();
    let mut current_pg;
    let mut last_frame_no = 0;
    let mut size_after = 0;

    let mut headers_count = 0;
    let mut prev_pg: *mut PgHdr = std::ptr::null_mut();
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
            pDirty: std::ptr::null_mut(),
            pPager: std::ptr::null_mut(),
            pgno: frame.header().page_no,
            pageHash: 0,
            flags: 0x02, // PGHDR_DIRTY - it works without the flag, but why risk it
            nRef: 0,
            pDirtyNext: std::ptr::null_mut(),
            pDirtyPrev: std::ptr::null_mut(),
        };
        headers_count += 1;
        current_pg = Box::into_raw(Box::new(page));
        if first_pg.is_null() {
            first_pg = current_pg;
        }
        if !prev_pg.is_null() {
            unsafe {
                (*prev_pg).pDirty = current_pg;
            }
        }
        prev_pg = current_pg;
    }

    tracing::trace!("built {headers_count} page headers");

    let headers = unsafe { Headers::new(first_pg) };
    (headers, last_frame_no, size_after)
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
