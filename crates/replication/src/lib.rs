pub mod frame;
pub mod replica;

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
pub use frame::{Frame, FrameHeader};
pub use replica::hook::{Frames, InjectorHookCtx};
use replica::snapshot::SnapshotFileHeader;
pub use replica::snapshot::TempSnapshot;

use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");

    pub use tonic::transport::Endpoint;
    pub type Client = replication_log_client::ReplicationLogClient<tonic::transport::Channel>;
}
pub struct Replicator {
    pub frames_sender: Sender<Frames>,
    pub current_frame_no_notifier: tokio::sync::watch::Receiver<FrameNo>,
    // The hook context needs to live as long as the injector and have a stable memory address.
    // Safety: it must never ever be used directly! Ever. Really.
    _hook_ctx: Arc<parking_lot::Mutex<InjectorHookCtx>>,
    pub meta: Arc<parking_lot::Mutex<Option<replica::meta::WalIndexMeta>>>,
    pub injector: replica::injector::FrameInjector<'static>,
}

pub struct Client {
    pub inner: rpc::Client,
    pub stream: Option<tonic::Streaming<rpc::Frame>>,
}

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
        })
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

    pub fn sync(&mut self, frames: Frames) -> anyhow::Result<()> {
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

    pub async fn connect_to_rpc(
        addr: impl Into<tonic::transport::Endpoint>,
    ) -> anyhow::Result<(Client, replica::meta::WalIndexMeta)> {
        let mut client = rpc::Client::connect(addr).await?;
        let response = client.hello(rpc::HelloRequest {}).await?.into_inner();
        let client = Client {
            inner: client,
            stream: None,
        };
        // FIXME: not that simple, we need to figure out if we always start from frame 1?
        let meta = replica::meta::WalIndexMeta {
            pre_commit_frame_no: 0,
            post_commit_frame_no: 0,
            generation_id: response.generation_id.parse::<uuid::Uuid>()?.to_u128_le(),
            database_id: response.database_id.parse::<uuid::Uuid>()?.to_u128_le(),
        };
        tracing::debug!("Hello response: {response:?}");
        Ok((client, meta))
    }

    // Syncs frames from RPC, returns true if it succeeded in applying a whole transaction
    async fn sync_from_rpc_internal(&mut self, client: &mut Client) -> anyhow::Result<bool> {
        use futures::StreamExt;
        const MAX_REPLICA_REPLICATION_BUFFER_LEN: usize = 10_000_000 / 4096; // ~10MB
        tracing::trace!("Syncing frames from RPC");
        // Reuse the stream if it exists, otherwise create a new one
        let stream = match &mut client.stream {
            Some(stream) => stream,
            None => {
                tracing::trace!("Creating new stream");
                // FIXME: sqld code uses the frame_no_notifier here - investigate if so should we
                let next_offset = self.meta.lock().unwrap().pre_commit_frame_no;
                client.stream = Some(
                    client
                        .inner
                        .log_entries(rpc::LogOffset { next_offset })
                        .await?
                        .into_inner(),
                );
                client.stream.as_mut().unwrap()
            }
        };

        let mut buffer = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    let frame = Frame::try_from_bytes(frame.data)?;
                    tracing::trace!(
                        "Received frame {frame:?}, buffer has {} frames, size_after={}",
                        buffer.len(),
                        frame.header().size_after
                    );
                    buffer.push(frame.clone());
                    if frame.header().size_after != 0
                        || buffer.len() > MAX_REPLICA_REPLICATION_BUFFER_LEN
                    {
                        tracing::trace!("Sending {} frames to the injector", buffer.len());
                        let _ = self
                            .frames_sender
                            .send(Frames::Vec(std::mem::take(&mut buffer)))
                            .await;
                        // Let's return here to indicate that we made progress.
                        // There may be more data in the stream and it's fine, the user would just ask to sync again.
                        return Ok(frame.header().size_after != 0);
                    }
                }
                Some(Err(err))
                    if err.code() == tonic::Code::FailedPrecondition
                        && err.message() == "NEED_SNAPSHOT" =>
                {
                    tracing::info!("loading snapshot");
                    // remove any outstanding frames in the buffer that are not part of a
                    // transaction: they are now part of the snapshot.
                    buffer.clear();
                    let _ = stream;
                    self.sync_from_snapshot(client).await?;
                    return Ok(true);
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(true),
            }
        }
    }

    pub fn sync_from_rpc(&mut self, client: &mut Client) -> anyhow::Result<()> {
        let runtime = tokio::runtime::Handle::current();
        loop {
            let done = runtime.block_on(self.sync_from_rpc_internal(client))?;
            tracing::trace!("Injecting frames");
            self.injector.step()?;
            tracing::trace!("Injected frames");
            if done {
                break;
            }
        }
        Ok(())
    }

    async fn sync_from_snapshot(&mut self, client: &mut Client) -> anyhow::Result<()> {
        use futures::StreamExt;

        let next_offset = self.meta.lock().unwrap().pre_commit_frame_no;
        let frames = client
            .inner
            .snapshot(rpc::LogOffset { next_offset })
            .await?
            .into_inner();

        let stream = frames.map(|data| match data {
            Ok(frame) => Frame::try_from_bytes(frame.data),
            Err(e) => anyhow::bail!(e),
        });
        // FIXME: do not hardcode the temporary path for downloading snapshots
        let snap = TempSnapshot::from_stream("data.sqld".as_ref(), stream).await?;

        let _ = self.frames_sender.send(Frames::Snapshot(snap)).await;

        Ok(())
    }
}
