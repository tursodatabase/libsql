use std::ffi::{c_int, CStr};

use sqld_libsql_bindings::ffi::types::XWalFrameFn;
use sqld_libsql_bindings::init_static_wal_method;
use sqld_libsql_bindings::rusqlite::ffi::{libsql_wal as Wal, PgHdr};
use sqld_libsql_bindings::wal_hook::WalHook;

use crate::frame::FrameBorrowed;
use crate::LIBSQL_PAGE_SIZE;

use super::headers::Headers;
use super::FrameBuffer;

// Those are custom error codes returned by the replicator hook.
pub const LIBSQL_INJECT_FATAL: c_int = 200;
/// Injection succeeded, left on a open txn state
pub const LIBSQL_INJECT_OK_TXN: c_int = 201;
/// Injection succeeded
pub const LIBSQL_INJECT_OK: c_int = 202;

pub struct InjectorHookCtx {
    /// shared frame buffer
    buffer: FrameBuffer,
    /// currently in a txn
    is_txn: bool,
}

impl InjectorHookCtx {
    pub fn new(buffer: FrameBuffer) -> Self {
        Self {
            buffer,
            is_txn: false,
        }
    }

    fn inject_pages(
        &mut self,
        sync_flags: i32,
        orig: XWalFrameFn,
        wal: *mut Wal,
    ) -> Result<(), ()> {
        self.is_txn = true;
        let buffer = self.buffer.lock();
        let (mut headers, size_after) = make_page_header(buffer.iter().map(|f| &**f));

        let ret = unsafe {
            orig(
                wal,
                LIBSQL_PAGE_SIZE as _,
                headers.as_ptr(),
                size_after,
                (size_after != 0) as _,
                sync_flags,
            )
        };

        if ret == 0 {
            debug_assert!(headers.all_applied());
            if size_after != 0 {
                self.is_txn = false;
            }
            tracing::trace!("applied frame batch");

            Ok(())
        } else {
            tracing::error!("fatal replication error: failed to apply pages");
            Err(())
        }
    }
}

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
/// return (headers, last_frame_no, size_after)
fn make_page_header<'a>(frames: impl Iterator<Item = &'a FrameBorrowed>) -> (Headers<'a>, u32) {
    let mut first_pg: *mut PgHdr = std::ptr::null_mut();
    let mut current_pg;
    let mut size_after = 0;

    let mut headers_count = 0;
    let mut prev_pg: *mut PgHdr = std::ptr::null_mut();
    let mut frames = frames.peekable();
    while let Some(frame) = frames.next() {
        // the last frame in a batch marks the end of the txn
        if frames.peek().is_none() {
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
    (headers, size_after)
}

init_static_wal_method!(INJECTOR_METHODS, InjectorHook);

/// The injector hook hijacks a call to xframes, and replace the content of the call with it's own
/// frames.
/// The Caller must first call `set_frames`, passing the frames to be injected, then trigger a call
/// to xFrames from the libsql connection (see dummy write in `injector`), and can then collect the
/// result on the injection with `take_result`
pub enum InjectorHook {}

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
        let ret = ctx.inject_pages(sync_flags, orig, wal_ptr);
        if ret.is_err() {
            return LIBSQL_INJECT_FATAL;
        }

        ctx.buffer.lock().clear();

        if !ctx.is_txn {
            LIBSQL_INJECT_OK
        } else {
            LIBSQL_INJECT_OK_TXN
        }
    }

    fn name() -> &'static CStr {
        CStr::from_bytes_with_nul(b"frame_injector_hook\0").unwrap()
    }
}
