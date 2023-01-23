///! Replication logic for the write proxy database.
///!
///! The current RO replication for the write proxy database works by periodically polling the
///! writer node for new WAL pages to apply and apply them.
///!
///! A bit of trickery is required to make it work: in order to have the correct environment set up
///! to apply WAL pages, we trick libsql into performing a write to the WAL. This is what the
///! `PeriodicDbUpdater::run` method does. This will cause libsql's `xFrames` to get called. We
///! will intercept this call thanks to our implementation of `WalHook`, and then call the
///! underlying WAL methods with the WAL pages we got from the writer, instead of that from the
///! dummy write. We then purposedly make xFrame return an error to invalidate any state the dummy
///! write may have cause in-memory.
///!
///! This relies on the fact that the layout of the WAL from the reader will match that of the
///! writer. This is important because it relies on the `size_after` argument to xFrames from the
///! writer. If any write is made from the reader, the database will be in an invalid state.
use std::collections::VecDeque;
use std::ffi::c_int;
use std::fs::{File, OpenOptions};
use std::io::ErrorKind;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::time::Duration;

use anyhow::ensure;
use futures::StreamExt;
use rusqlite::ffi::SQLITE_ERROR;
use rusqlite::OpenFlags;
use tokio::runtime::Handle;
use tonic::transport::Channel;

use crate::libsql::ffi::{types::XWalFrameFn, PgHdr, Wal};
use crate::libsql::open_with_regular_wal;
use crate::libsql::wal_hook::WalHook;
use crate::rpc::wal_log::wal_log_rpc::wal_log_entry::Payload;
use crate::rpc::wal_log::wal_log_rpc::{wal_log_client::WalLogClient, LogOffset, WalLogEntry};
use crate::rpc::wal_log::wal_log_rpc::{Commit, Frame};

pub struct PeriodicDbUpdater {
    interval: Duration,
    db: rusqlite::Connection,
}

/// The `PeriodicUpdater` role is to periodically trigger a dummy write that will be intercepted by
/// its WAL hook.
impl PeriodicDbUpdater {
    pub async fn new(
        path: &Path,
        logger: WalLogClient<Channel>,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        let db = open_with_regular_wal(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            ReadReplicationHook::new(logger).await?,
        )?;

        Ok(Self { interval, db })
    }

    /// blocking!
    pub fn step(&mut self) {
        // dummy write that triggers a call to xFrame
        let _ = self.db.execute(
            "create table if not exists __dummy__ (dummy); insert into __dummy__ values (1);",
            (),
        );
        std::thread::sleep(self.interval);
    }
}

struct ReadReplicationHook {
    logger: WalLogClient<Channel>,
    fetch_frame_index: u64,
    /// Persistent last committed index used for restarts.
    /// The File should contain two little-endian u64:
    /// - The first one is the attempted commit index before the call xFrame
    /// - The second index is the actually committed index after xFrame
    /// After a flight of pages has been successfully written, the two numbers should be the same.
    /// On startup the two number are checked for consistency. If they differ, the database is
    /// considered corrupted, since it is impossible to know what the actually replicated index is.
    last_applied_index_file: File,
    last_applied_index: Option<u64>,
    /// Buffer for incoming frames
    buffer: VecDeque<WalLogEntry>,
    rt: Handle,
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
            current = (*current).dirty;
        }
    }

    true
}

unsafe impl WalHook for ReadReplicationHook {
    fn on_frames(
        &mut self,
        wal: *mut Wal,
        _page_size: c_int,
        _page_headers: *mut PgHdr,
        _size_after: u32,
        _is_commit: c_int,
        _sync_flags: c_int,
        orig: XWalFrameFn,
    ) -> c_int {
        let rt = self.rt.clone();
        if let Err(e) = rt.block_on(self.fetch_log_entries()) {
            tracing::error!("error fetching log entries: {e}");
            return SQLITE_ERROR;
        }

        while let Some((page_headers, truncate, commit)) = self.next_transaction() {
            tracing::trace!(commit = ?commit, truncate = truncate);
            let attempted_commit_index = self
                .last_applied_index
                .map(|x| x + truncate as u64 - 1)
                .unwrap_or_default();
            // pre-write index
            self.last_applied_index_file
                .write_all_at(&attempted_commit_index.to_le_bytes(), 0)
                .unwrap();
            let Commit {
                page_size,
                size_after,
                is_commit,
                sync_flags,
            } = commit;

            let ret = orig(
                wal,
                page_size,
                page_headers,
                size_after,
                is_commit as _,
                sync_flags,
            );

            if ret == 0 {
                debug_assert!(all_applied(page_headers));
                // persist new commited index
                self.last_applied_index_file
                    .write_all_at(&attempted_commit_index.to_le_bytes(), size_of::<u64>() as _)
                    .unwrap();
                self.last_applied_index.replace(attempted_commit_index);
                // remove commited entries.
                self.buffer.drain(..truncate);
                tracing::trace!("applied frame batch");
            } else {
                // should we retry?
                todo!("how to handle apply failure?");
            }

            free_page_header(page_headers);
        }
        // return error from dummy write.
        // this is a trick to prevent sqlite from keeping any state in memory after a dummy write
        SQLITE_ERROR
    }
}

/// Turn a list of `WalLogEntry` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
fn make_page_header<'a>(entries: impl Iterator<Item = &'a WalLogEntry>) -> *mut PgHdr {
    let mut current_pg = std::ptr::null_mut();

    let mut headers_count = 0;
    for entry in entries {
        if let Payload::Frame(Frame { page_no, data }) = entry.payload.as_ref().unwrap() {
            let page = PgHdr {
                page: std::ptr::null(),
                data: data.as_ptr() as _,
                extra: std::ptr::null(),
                pcache: std::ptr::null(),
                dirty: current_pg,
                pager: std::ptr::null(),
                pgno: *page_no,
                pagehash: 0,
                flags: 0,
            };
            headers_count += 1;
            current_pg = Box::into_raw(Box::new(page));
        }
    }

    tracing::trace!("built {headers_count} page headers");

    current_pg
}

/// frees the `PgHdr` list pointed at by `h`.
fn free_page_header(h: *const PgHdr) {
    let mut current = h;
    while !current.is_null() {
        let h: Box<PgHdr> = unsafe { Box::from_raw(current as _) };
        current = h.dirty;
    }
}

impl ReadReplicationHook {
    async fn new(logger: WalLogClient<Channel>) -> anyhow::Result<Self> {
        let last_applied_index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(".wal_index.iku")?;

        let mut buf = [0; 2 * size_of::<u64>()];
        let last_applied_index = match last_applied_index_file.read_exact_at(&mut buf, 0) {
            Ok(()) => {
                let pre = u64::from_le_bytes(buf[..size_of::<u64>()].try_into()?);
                let post = u64::from_le_bytes(buf[size_of::<u64>()..].try_into()?);
                ensure!(pre == post, "database corrupted");
                Some(pre)
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                // OK: the file was empty, we don't have a log yet.
                None
            }
            Err(e) => Err(e)?,
        };

        Ok(Self {
            logger,
            // ask for the frame right after the one we last applied
            fetch_frame_index: last_applied_index.map(|x| x + 1).unwrap_or_default(),
            last_applied_index_file,
            last_applied_index,
            buffer: Default::default(),
            rt: Handle::current(),
        })
    }

    /// Returns the next page headers list the log truncate count, and the commit frame for the
    /// next buffered transaction.
    ///
    /// The caller is responsible for freeing the page headers with the `free_page_header` function,
    /// and advancing the internal buffer with
    ///
    /// Note: It does not seem possible to batch transaction. I suspect that this is because the
    /// original implementation of the sqlite WAL overwrites when pages appear multiple times in
    /// the same transaction.
    fn next_transaction(&self) -> Option<(*mut PgHdr, usize, Commit)> {
        let (commit_idx, commit) =
            self.buffer
                .iter()
                .enumerate()
                .find_map(|(i, e)| match &e.payload {
                    Some(Payload::Commit(commit)) => Some((i, commit.clone())),
                    _ => None,
                })?;

        let headers = make_page_header(self.buffer.iter().take(commit_idx));

        Some((headers, commit_idx + 1, commit))
    }

    /// Asks the writer for new log frames to apply.
    async fn fetch_log_entries(&mut self) -> anyhow::Result<()> {
        // try to fetch next page.
        let start_offset = self.fetch_frame_index;
        let req = LogOffset { start_offset };

        let mut stream = self.logger.log_entries(req).await?.into_inner();
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            debug_assert_eq!(
                frame.index, self.fetch_frame_index,
                "out of order log frame"
            );
            self.fetch_frame_index = frame.index + 1;
            self.buffer.push_back(frame);
        }

        tracing::trace!(
            start_offset,
            self.fetch_frame_index,
            "received {} frames",
            self.fetch_frame_index - start_offset,
        );

        Ok(())
    }
}
