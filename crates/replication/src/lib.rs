mod client;
pub mod frame;
pub mod replica;

pub use client::pb;

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
use anyhow::Context;
pub use frame::{Frame, FrameHeader};
pub use replica::hook::{Frames, InjectorHookCtx};
use replica::snapshot::SnapshotFileHeader;
pub use replica::snapshot::TempSnapshot;
use tokio::sync::watch::Receiver;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use client::Client;

pub struct Replicator {
    pub frames_sender: Sender<Frames>,
    pub current_frame_no_notifier: tokio::sync::watch::Receiver<FrameNo>,
    // The hook context needs to live as long as the injector and have a stable memory address.
    // Safety: it must never ever be used directly! Ever. Really.
    _hook_ctx: Arc<parking_lot::Mutex<InjectorHookCtx>>,
    pub meta: Arc<parking_lot::Mutex<Option<replica::meta::WalIndexMeta>>>,
    pub injector: replica::injector::FrameInjector<'static>,
    pub client: Option<Client>,
    pub next_offset: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Writer {
    client: Client,
}

// FIXME: copy-pasted from sqld, it should be deduplicated in a single place
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct FramesRequest {
    pub next_offset: u64,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ReplicationFrames {
    pub frames: Vec<Frame>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Hello {
    pub generation_id: uuid::Uuid,
    pub generation_start_index: u64,
    pub database_id: uuid::Uuid,
}
// END COPYPASTA

impl Replicator {
    pub fn new(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let (applied_frame_notifier, current_frame_no_notifier) =
            tokio::sync::watch::channel(FrameNo::MAX);
        let meta = Arc::new(parking_lot::Mutex::new(None));
        let (frames_sender, receiver) = tokio::sync::mpsc::channel(1);

        let pre_commit = {
            let meta = meta.clone();
            move |fno| {
                let mut lock = meta.lock();
                let meta: &mut replica::meta::WalIndexMeta = match lock.as_mut() {
                    Some(meta) => meta,
                    None => anyhow::bail!("sync called before meta inialization"),
                };
                meta.pre_commit_frame_no = fno;
                // FIXME: consider how we want to enable storing metadata - in a file, like below? Or in an internal table?
                //meta_file.write_all_at(bytes_of(meta), 0)?;

                Ok(())
            }
        };

        let post_commit = {
            let meta = meta.clone();
            let notifier = applied_frame_notifier;
            move |fno| {
                let mut lock = meta.lock();
                let meta = match lock.as_mut() {
                    Some(meta) => meta,
                    None => anyhow::bail!("sync called before meta inialization"),
                };
                assert_eq!(meta.pre_commit_frame_no, fno);
                meta.post_commit_frame_no = fno;
                // FIXME: consider how we want to enable storing metadata - in a file, like below? Or in an internal table?
                //meta_file.write_all_at(bytes_of(meta), 0)?;
                let _ = notifier.send(fno);

                Ok(())
            }
        };

        let hook_ctx = Arc::new(parking_lot::Mutex::new(
            replica::hook::InjectorHookCtx::new(receiver, pre_commit, post_commit),
        ));
        // Safety: hook ctx reference is kept alive by the Arc<>, and is never used directly.
        let hook_ctx_ref = unsafe {
            std::mem::transmute::<
                &mut replica::hook::InjectorHookCtx,
                &'static mut replica::hook::InjectorHookCtx,
            >(&mut *hook_ctx.lock())
        };
        let injector = replica::injector::FrameInjector::new(path.as_ref(), hook_ctx_ref)?;

        Ok(Self {
            frames_sender,
            current_frame_no_notifier,
            _hook_ctx: hook_ctx,
            meta,
            injector,
            client: None,
            next_offset: AtomicU64::new(0),
        })
    }

    pub async fn init_metadata(
        &mut self,
        endpoint: impl AsRef<str>,
        auth_token: impl AsRef<str>,
    ) -> anyhow::Result<replica::meta::WalIndexMeta> {
        let client = Client::new(endpoint.as_ref().try_into()?, auth_token)?;

        let meta = client.hello().await?;

        self.client = Some(client);

        tracing::debug!("init_metadata: {meta:?}");
        Ok(meta)
    }

    pub fn update_metadata_from_snapshot_header(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<()> {
        // FIXME: I guess we should consider allowing async reads here
        use std::io::Read;
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)?;
        let mut buf: [u8; std::mem::size_of::<SnapshotFileHeader>()] =
            [0; std::mem::size_of::<SnapshotFileHeader>()];
        file.read_exact(&mut buf)?;
        let snapshot_header: SnapshotFileHeader = bytemuck::pod_read_unaligned(&buf);

        let mut meta = self.meta.lock();

        if let Some(meta) = &*meta {
            if meta.post_commit_frame_no != snapshot_header.start_frame_no {
                tracing::warn!(
                    "Snapshot header frame number {} does not match post-commit frame number {}",
                    snapshot_header.start_frame_no,
                    meta.post_commit_frame_no
                );
                anyhow::bail!(
                    "Snapshot header frame number {} does not match post-commit frame number {}",
                    snapshot_header.start_frame_no,
                    meta.post_commit_frame_no
                )
            }
        } else if snapshot_header.start_frame_no != 0 {
            tracing::warn!(
                "Cannot initialize metadata from snapshot header with frame number {} instead of 0",
                snapshot_header.start_frame_no
            );
            anyhow::bail!(
                "Cannot initialize metadata from snapshot header with frame number {} instead of 0",
                snapshot_header.start_frame_no
            )
        }
        // Metadata is loaded straight from the snapshot header and overwrites any previous values
        *meta = Some(replica::meta::WalIndexMeta {
            pre_commit_frame_no: snapshot_header.start_frame_no,
            post_commit_frame_no: snapshot_header.start_frame_no,
            generation_id: 1, // FIXME: where to obtain generation id from? Do we need it?
            database_id: snapshot_header.db_id,
        });
        Ok(())
    }

    pub fn writer(&self) -> anyhow::Result<Writer> {
        let client = self
            .client
            .clone()
            .context("FATAL trying to sync with no client, you need to call init_metadata first")?;

        Ok(Writer { client })
    }

    pub fn sync(&self, frames: Frames) -> anyhow::Result<()> {
        if let Frames::Snapshot(snapshot) = &frames {
            tracing::debug!(
                "Updating metadata from snapshot header {}",
                snapshot.path().display()
            );
            self.update_metadata_from_snapshot_header(snapshot.path())?;
        }
        let _ = self.frames_sender.blocking_send(frames);
        self.injector.step()?;
        Ok(())
    }

    // Syncs frames from HTTP, returns how many frames were applied
    pub async fn sync_from_http(&self) -> anyhow::Result<usize> {
        tracing::trace!("Syncing frames from HTTP");

        let frames = match self.fetch_log_entries(false).await {
            Ok(frames) => Ok(frames),
            Err(e) => {
                if let Some(status) = e.downcast_ref::<tonic::Status>() {
                    if status.code() == tonic::Code::FailedPrecondition {
                        self.fetch_log_entries(true).await
                    } else {
                        Err(e)
                    }
                } else {
                    Err(e)
                }
            }
        }?;

        let len = frames.len();
        self.next_offset.fetch_add(len as u64, Ordering::Relaxed);
        self.frames_sender.send(Frames::Vec(frames)).await?;
        self.injector.step()?;
        Ok(len)
    }

    async fn fetch_log_entries(&self, send_hello: bool) -> anyhow::Result<Vec<Frame>> {
        let client = self
            .client
            .clone()
            .context("FATAL trying to sync with no client, you need to call init_metadata first")?;

        if send_hello {
            // TODO: Should we update wal metadata?
            let _res = client.hello().await?;
        }

        client
            .batch_log_entries(self.next_offset.load(Ordering::Relaxed))
            .await
    }
}

impl Writer {
    pub async fn execute(
        &self,
        sql: &str,
        params: impl Into<pb::query::Params> + Send,
    ) -> anyhow::Result<u64> {
        tracing::trace!("executing remote sql statement: {sql}");
        let (write_frame_no, rows_affected) = self.client.execute(sql, params.into()).await?;

        tracing::trace!(
            "statement executed on remote waiting for frame_no: {}",
            write_frame_no
        );
        Ok(rows_affected)
    }
}
