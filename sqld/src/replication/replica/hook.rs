use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::{cell::RefCell, ffi::c_int, fs::File, path::Path, rc::Rc};

use anyhow::bail;
use bytemuck::bytes_of;
use rusqlite::ffi::{PgHdr, SQLITE_ERROR};
use rusqlite::OpenFlags;
use sqld_libsql_bindings::ffi::Wal;
use sqld_libsql_bindings::{ffi::types::XWalFrameFn, open_with_regular_wal, wal_hook::WalHook};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::replication::frame::{Frame, FrameBorrowed};
use crate::replication::{FrameNo, WAL_PAGE_SIZE};
use crate::rpc::replication_log::rpc::HelloResponse;
use crate::HARD_RESET;

use super::error::ReplicationError;
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

#[derive(Debug)]
struct FrameApplyOp {
    frames: Frames,
    ret: oneshot::Sender<anyhow::Result<FrameNo>>,
}

pub struct FrameApplicatorHandle {
    handle: JoinHandle<anyhow::Result<()>>,
    sender: mpsc::Sender<FrameApplyOp>,
}

impl FrameApplicatorHandle {
    pub async fn new(db_path: PathBuf, hello: HelloResponse) -> anyhow::Result<(Self, FrameNo)> {
        let (sender, mut receiver) = mpsc::channel(16);
        let (ret, init_ok) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut applicator = match FrameApplicator::new_from_hello(&db_path, hello) {
                Ok((hook, last_applied_frame_no)) => {
                    ret.send(Ok(last_applied_frame_no)).unwrap();
                    hook
                }
                Err(e) => {
                    ret.send(Err(e)).unwrap();
                    return Ok(());
                }
            };

            while let Some(FrameApplyOp { frames, ret }) = receiver.blocking_recv() {
                let res = applicator.apply_frames(frames);
                if ret.send(res).is_err() {
                    bail!("frame application result must not be ignored.");
                }
            }

            Ok(())
        });

        let last_applied_frame_no = init_ok.await??;

        Ok((Self { handle, sender }, last_applied_frame_no))
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.handle.await?
    }

    pub async fn apply_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        let (ret, rcv) = oneshot::channel();
        self.sender.send(FrameApplyOp { frames, ret }).await?;
        rcv.await?
    }
}

pub struct FrameApplicator {
    conn: rusqlite::Connection,
    hook: ReplicationHook,
}

impl FrameApplicator {
    /// returns the replication hook and the currently applied frame_no
    pub fn new_from_hello(db_path: &Path, hello: HelloResponse) -> anyhow::Result<(Self, FrameNo)> {
        let (meta, file) = WalIndexMeta::read_from_path(db_path)?;
        let meta = match meta {
            Some(meta) => match meta.merge_from_hello(hello) {
                Ok(meta) => meta,
                Err(e @ ReplicationError::Lagging) => {
                    tracing::error!("Replica ahead of primary: hard-reseting replica");
                    HARD_RESET.notify_waiters();

                    bail!(e);
                }
                Err(_e @ ReplicationError::DbIncompatible) => bail!(ReplicationError::Exit),
                Err(e) => bail!(e),
            },
            None => WalIndexMeta::new_from_hello(hello)?,
        };

        Ok((Self::init(db_path, file, meta)?, meta.current_frame_no()))
    }

    fn init(db_path: &Path, meta_file: File, meta: WalIndexMeta) -> anyhow::Result<Self> {
        let hook = ReplicationHook {
            inner: Rc::new(RefCell::new(ReplicationHookInner {
                current_txn_frames: None,
                result: None,
                meta_file,
                meta,
            })),
        };
        let conn = open_with_regular_wal(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            hook.clone(),
            false, // bottomless replication is not enabled for replicas
        )?;

        Ok(Self { conn, hook })
    }

    fn apply_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        self.hook
            .inner
            .borrow_mut()
            .current_txn_frames
            .replace(frames);

        let _ = self.conn.execute(
            "create table if not exists __dummy__ (dummy); insert into __dummy__ values (1);",
            (),
        );

        {
            let mut inner = self.hook.inner.borrow_mut();
            inner.current_txn_frames.take();
            inner.result.take().unwrap()
        }
    }
}

#[derive(Clone)]
struct ReplicationHook {
    inner: Rc<RefCell<ReplicationHookInner>>,
}

pub struct ReplicationHookInner {
    /// slot for the frames to be applied by the next call to xframe
    current_txn_frames: Option<Frames>,
    /// slot to store the result of the call to xframes.
    /// On success, returns the last applied frame_no
    result: Option<anyhow::Result<FrameNo>>,
    meta_file: File,
    meta: WalIndexMeta,
}

impl ReplicationHookInner {
    unsafe fn apply_pages(
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

    fn post_commit(&mut self) -> anyhow::Result<()> {
        self.meta.post_commit_frame_no = self.meta.pre_commit_frame_no;
        self.flush_meta()
    }

    fn flush_meta(&self) -> anyhow::Result<()> {
        self.meta_file.write_all_at(bytes_of(&self.meta), 0)?;

        Ok(())
    }
}

unsafe impl WalHook for ReplicationHook {
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
        let mut this = self.inner.borrow_mut();
        let Some(ref frames) = this.current_txn_frames.take() else {
            return SQLITE_ERROR;
        };

        let (headers, last_frame_no, size_after) = frames.to_headers();

        // SAFETY: frame headers are valid for the duration of the call of apply_pages
        let result =
            unsafe { this.apply_pages(headers, last_frame_no, size_after, sync_flags, orig, wal) };

        free_page_header(headers);

        let result = result.map(|_| last_frame_no);
        this.result.replace(result);

        SQLITE_ERROR
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
