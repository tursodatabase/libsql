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
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use bytemuck::{bytes_of, try_from_bytes, Pod, Zeroable};
use crossbeam::channel::TryRecvError;
use futures::StreamExt;
use rusqlite::ffi::SQLITE_ERROR;
use rusqlite::OpenFlags;
use tokio::runtime::Handle;
use tonic::transport::Channel;
use tonic::Code;
use uuid::Uuid;

use crate::libsql::ffi::{types::XWalFrameFn, PgHdr, Wal};
use crate::libsql::open_with_regular_wal;
use crate::libsql::wal_hook::WalHook;
use crate::rpc::wal_log::wal_log_rpc::{wal_log_client::WalLogClient, LogOffset};
use crate::rpc::wal_log::wal_log_rpc::{HelloRequest, HelloResponse};
use crate::rpc::wal_log::NO_HELLO_ERROR_MSG;
use crate::wal_logger::{WalFrame, WAL_PAGE_SIZE};
use crate::HARD_RESET;

pub struct PeriodicDbUpdater {
    interval: Duration,
    db: rusqlite::Connection,
    abort_receiver: crossbeam::channel::Receiver<ReplicationError>,
}

/// The `PeriodicUpdater` role is to periodically trigger a dummy write that will be intercepted by
/// its WAL hook.
impl PeriodicDbUpdater {
    pub async fn new(
        path: &Path,
        logger: WalLogClient<Channel>,
        interval: Duration,
    ) -> anyhow::Result<Self> {
        let (hook, abort_receiver) = ReadReplicationHook::new(logger, path).await?;
        let path = path.to_owned();
        let db = tokio::task::spawn_blocking(move || {
            open_with_regular_wal(
                path,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                hook,
                false, // bottomless replication is not enabled for replicas
            )
        })
        .await??;

        Ok(Self {
            interval,
            db,
            abort_receiver,
        })
    }

    /// blocking!
    /// return whether to continue
    pub fn step(&mut self) -> anyhow::Result<bool> {
        match self.abort_receiver.try_recv() {
            Ok(e) => {
                // received an error from loop
                Err(e.into())
            }
            Err(TryRecvError::Empty) => {
                // dummy write that triggers a call to xFrame
                let _ = self.db.execute(
                    "create table if not exists __dummy__ (dummy); insert into __dummy__ values (1);",
                    (),
                );
                std::thread::sleep(self.interval);

                Ok(true)
            }

            Err(TryRecvError::Disconnected) => {
                // graceful exit
                Ok(false)
            }
        }
    }
}

struct ReadReplicationHook {
    logger: WalLogClient<Channel>,
    /// Persistent last committed index used for restarts.
    /// The File should contain two little-endian u64:
    /// - The first one is the attempted commit index before the call xFrame
    /// - The second index is the actually committed index after xFrame
    /// After a flight of pages has been successfully written, the two numbers should be the same.
    /// On startup the two number are checked for consistency. If they differ, the database is
    /// considered corrupted, since it is impossible to know what the actually replicated index is.
    wal_index_meta_file: File,
    wal_index_meta: WalIndexMeta,
    /// Buffer for incoming frames
    buffer: VecDeque<WalFrame>,
    rt: Handle,
    /// A channel to send error back to the polling loop.
    /// When an error occurs that causes an abort, this handle should be replaced with None, and
    /// the error sent. This means that if `abort_sender` is None, we should assume a previous
    /// abort.
    abort_sender: Option<crossbeam::channel::Sender<ReplicationError>>,
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

unsafe impl WalHook for ReadReplicationHook {
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
        // A fatal error has occured before, don't do anyhting
        if self.abort_sender.is_none() {
            return SQLITE_ERROR;
        }

        let rt = self.rt.clone();
        if let Err(e) = rt.block_on(self.fetch_log_entries()) {
            tracing::error!("error fetching log entries: {e}");
            return SQLITE_ERROR;
        }

        while let Some((page_headers, frame_count)) = self.next_transaction() {
            let size_after = self.buffer[frame_count - 1].header.size_after;
            assert_ne!(size_after, 0, "commit index points to non commit frame");

            tracing::trace!(commit = ?frame_count, size_after = size_after);
            self.inc_pre_commit(frame_count as u64)
                .expect("failed to write pre-commit index");
            let ret = unsafe { orig(wal, WAL_PAGE_SIZE, page_headers, size_after, 1, sync_flags) };

            if ret == 0 {
                debug_assert!(all_applied(page_headers));
                self.sync_post_commit()
                    .expect("failed to write post-commit index");
                // remove commited entries.
                self.buffer.drain(..frame_count);
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

/// Turn a list of `WalFrame` into a list of PgHdr.
/// The caller has the responsibility to free the returned headers.
fn make_page_header<'a>(frames: impl Iterator<Item = &'a WalFrame>) -> *mut PgHdr {
    let mut current_pg = std::ptr::null_mut();

    let mut headers_count = 0;
    for frame in frames {
        let page = PgHdr {
            pPage: std::ptr::null_mut(),
            pData: frame.data.as_ptr() as _,
            pExtra: std::ptr::null_mut(),
            pCache: std::ptr::null_mut(),
            pDirty: current_pg,
            pPager: std::ptr::null_mut(),
            pgno: frame.header.page_no,
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

    current_pg
}

/// frees the `PgHdr` list pointed at by `h`.
fn free_page_header(h: *const PgHdr) {
    let mut current = h;
    while !current.is_null() {
        let h: Box<PgHdr> = unsafe { Box::from_raw(current as _) };
        current = h.pDirty;
    }
}

#[repr(C)]
#[derive(Debug, Pod, Zeroable, Clone, Copy)]
struct WalIndexMeta {
    /// This is the anticipated next frame index to request
    pre_commit_index: u64,
    /// After we have written the frames back to the wal, we set this value to the same value as
    /// pre_commit_index
    /// On startup we check this value against the pre-commit value to check for consistency
    post_commit_index: u64,
    /// Generation Uuid
    generation_id: u128,
    /// Uuid of the database this instance is a replica of
    database_id: u128,
}

#[derive(Debug, thiserror::Error)]
enum ReplicationError {
    #[error("Replica is ahead of primary")]
    Lagging,
    #[error("Trying to replicate incompatible databases")]
    DbIncompatible,
    #[error("{0}")]
    Other(#[from] anyhow::Error),
    #[error("Replication loop exited")]
    Exit,
}

impl WalIndexMeta {
    fn read(meta_file: &File) -> anyhow::Result<Option<Self>> {
        let mut buf = [0; size_of::<WalIndexMeta>()];
        let meta = match meta_file.read_exact_at(&mut buf, 0) {
            Ok(()) => {
                meta_file.read_exact_at(&mut buf, 0)?;
                let meta = *try_from_bytes(&buf)
                    .map_err(|_| anyhow::anyhow!("invalid index meta file"))?;
                Some(meta)
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => None,
            Err(e) => Err(e)?,
        };

        Ok(meta)
    }

    /// attempts to merge two meta files.
    fn merge_from_hello(mut self, hello: HelloResponse) -> Result<Self, ReplicationError> {
        let hello_db_id = Uuid::from_str(&hello.database_id)
            .context("invalid database id from primary")?
            .as_u128();
        let hello_gen_id = Uuid::from_str(&hello.generation_id)
            .context("invalid generation id from primary")?
            .as_u128();

        if hello_db_id != self.database_id {
            return Err(ReplicationError::DbIncompatible);
        }

        if self.generation_id == hello_gen_id {
            Ok(self)
        } else if self.pre_commit_index <= hello.generation_start_index {
            // Ok: generation changed, but we aren't ahead of primary
            self.generation_id = hello_gen_id;
            Ok(self)
        } else {
            Err(ReplicationError::Lagging)
        }
    }

    fn new_from_hello(hello: HelloResponse) -> anyhow::Result<WalIndexMeta> {
        let database_id = Uuid::from_str(&hello.database_id)
            .context("invalid database id from primary")?
            .as_u128();
        let generation_id = Uuid::from_str(&hello.generation_id)
            .context("invalid generation id from primary")?
            .as_u128();

        Ok(Self {
            pre_commit_index: 0,
            post_commit_index: 0,
            generation_id,
            database_id,
        })
    }
}

impl ReadReplicationHook {
    async fn new(
        logger: WalLogClient<Channel>,
        db_path: &Path,
    ) -> anyhow::Result<(Self, crossbeam::channel::Receiver<ReplicationError>)> {
        let path = db_path.join("client_wal_index");
        let index_meta_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let (sender, receiver) = crossbeam::channel::bounded(1);
        let mut abort_sender = Some(sender);
        let meta = WalIndexMeta::read(&index_meta_file)?;
        let wal_index_meta =
            Self::perform_handshake(meta, logger.clone(), &mut abort_sender).await?;
        let this = Self {
            logger,
            wal_index_meta_file: index_meta_file,
            wal_index_meta,
            buffer: Default::default(),
            abort_sender,
            rt: Handle::current(),
        };

        Ok((this, receiver))
    }

    async fn perform_handshake(
        meta: Option<WalIndexMeta>,
        mut logger: WalLogClient<Channel>,
        abort_sender: &mut Option<crossbeam::channel::Sender<ReplicationError>>,
    ) -> anyhow::Result<WalIndexMeta> {
        let hello = logger.hello(HelloRequest {}).await?.into_inner();
        tracing::debug!(?hello);
        match meta {
            Some(meta) => match meta.merge_from_hello(hello) {
                Ok(meta) => Ok(meta),
                Err(e @ ReplicationError::Lagging) => {
                    tracing::error!("Replica ahead of primary: hard-reseting replica");
                    HARD_RESET.notify_waiters();

                    // We don't send an error here because we don't want the program to exit: the
                    // hard reset flow will take care of cleaning behind us and restart fresh.
                    abort_sender.take();

                    Err(e.into())
                }
                Err(e @ ReplicationError::DbIncompatible) => {
                    if let Some(sender) = abort_sender.take() {
                        let _ = sender.send(e);
                    }

                    Err(ReplicationError::Exit.into())
                }
                // non-fatal error
                Err(e) => Err(e.into()),
            },
            None => Ok(WalIndexMeta::new_from_hello(hello)?),
        }
    }

    fn flush_meta(&self) -> anyhow::Result<()> {
        self.wal_index_meta_file
            .write_all_at(bytes_of(&self.wal_index_meta), 0)?;

        Ok(())
    }

    /// Increment the pre-commit index by n, and flush the meta file
    fn inc_pre_commit(&mut self, n: u64) -> anyhow::Result<()> {
        self.wal_index_meta.pre_commit_index += n;
        self.flush_meta()
    }

    fn sync_post_commit(&mut self) -> anyhow::Result<()> {
        self.wal_index_meta.post_commit_index = self.wal_index_meta.pre_commit_index;
        self.flush_meta()
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
    fn next_transaction(&self) -> Option<(*mut PgHdr, usize)> {
        // nothing to do yet.
        if self.buffer.is_empty() {
            return None;
        }

        let frame_count = self
            .buffer
            .iter()
            .enumerate()
            .find_map(|(i, f)| (f.header.size_after != 0).then_some(i + 1))?; // early return if
                                                                              // missing commit frame.

        let headers = make_page_header(self.buffer.iter().take(frame_count));

        Some((headers, frame_count))
    }

    /// Asks the writer for new log frames to apply.
    async fn fetch_log_entries(&mut self) -> anyhow::Result<()> {
        let current_offset = self.current_frame_index();
        let req = LogOffset { current_offset };

        match self.logger.log_entries(req).await {
            Ok(stream) => {
                let mut stream = stream.into_inner();
                let mut frame_count = 0;
                while let Some(raw_frame) = stream.next().await {
                    let raw_frame = raw_frame?;
                    let frame = WalFrame::decode(raw_frame.data)?;
                    debug_assert_eq!(
                        frame.header.frame_id, self.wal_index_meta.pre_commit_index,
                        "out of order log frame"
                    );
                    frame_count += 1;
                    self.buffer.push_back(frame);
                }

                tracing::debug!(current_offset, frame_count,);

                Ok(())
            }
            Err(s) if s.code() == Code::FailedPrecondition && s.message() == NO_HELLO_ERROR_MSG => {
                tracing::info!("Primary restarted, perfoming hanshake again");
                self.wal_index_meta = Self::perform_handshake(
                    Some(self.wal_index_meta),
                    self.logger.clone(),
                    &mut self.abort_sender,
                )
                .await?;

                Ok(())
            }
            Err(e) => Err(e)?,
        }
    }

    /// Return the current frame index. None if we haven't received any frame yet
    fn current_frame_index(&self) -> Option<u64> {
        // This is how we detect a fresh start.
        // While we could _potentially_ already have requested for index 0, and commited *only*
        // frame 0, it's ok to reapply it.
        if self.buffer.is_empty() && self.wal_index_meta.pre_commit_index == 0 {
            None
        } else {
            // the next frame we want to fetch is the one that after the last we have commiter +
            // the ones we have buffered
            let index = self.wal_index_meta.pre_commit_index + self.buffer.len() as u64;
            Some(index)
        }
    }
}
