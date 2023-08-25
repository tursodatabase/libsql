mod client;
pub mod frame;
pub mod replica;

mod pb {
    #![allow(unreachable_pub)]
    #![allow(missing_docs)]
    include!("generated/wal_log.rs");

    pub use replication_log_client::ReplicationLogClient;
}

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
use anyhow::Context;
pub use frame::{Frame, FrameHeader};
pub use replica::hook::{Frames, InjectorHookCtx};
use replica::snapshot::SnapshotFileHeader;
pub use replica::snapshot::TempSnapshot;
use tonic::codegen::InterceptedService;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
// use tonic::transport::Channel;

use uuid::Uuid;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::client::H2cChannel;
use crate::pb::HelloRequest;

type RpcClient = pb::ReplicationLogClient<InterceptedService<H2cChannel, AuthInterceptor>>;

pub struct Replicator {
    pub frames_sender: Sender<Frames>,
    pub current_frame_no_notifier: tokio::sync::watch::Receiver<FrameNo>,
    // The hook context needs to live as long as the injector and have a stable memory address.
    // Safety: it must never ever be used directly! Ever. Really.
    _hook_ctx: Arc<parking_lot::Mutex<InjectorHookCtx>>,
    pub meta: Arc<parking_lot::Mutex<Option<replica::meta::WalIndexMeta>>>,
    pub injector: replica::injector::FrameInjector<'static>,
    pub client: Option<RpcClient>,
    pub next_offset: AtomicU64,
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
        let auth_token = format!("Bearer {}", auth_token.as_ref())
            .try_into()
            .context("Invalid auth token must be ascii")?;

        // TODO: Once fly fixes their proxy to correctly accept h2 we can drop
        // the h2c client but for now lets keep this commented.
        //
        //let channel = Channel::builder(
        //    endpoint
        //        .as_ref()
        //        .try_into()
        //        .context("Unable to convert endpoint into a Uri")?,
        //)
        //.http2_keep_alive_interval(std::time::Duration::from_secs(5))
        //.keep_alive_while_idle(true)
        //.tls_config(tonic::transport::ClientTlsConfig::new())?
        //.connect_lazy();

        let channel = H2cChannel::new();

        let mut client = pb::ReplicationLogClient::with_origin(
            InterceptedService::new(channel, AuthInterceptor(auth_token)),
            http::Uri::try_from(endpoint.as_ref()).unwrap(),
        );

        let response = client
            .hello(pb::HelloRequest::default())
            .await?
            .into_inner();

        let generation_id =
            Uuid::try_parse(&response.generation_id).context("Unable to parse generation id")?;
        let database_id =
            Uuid::try_parse(&response.database_id).context("Unable to parse database id")?;

        self.client = Some(client);

        // FIXME: not that simple, we need to figure out if we always start from frame 1?
        let meta = replica::meta::WalIndexMeta {
            pre_commit_frame_no: 0,
            post_commit_frame_no: 0,
            generation_id: generation_id.to_u128_le(),
            database_id: database_id.to_u128_le(),
        };
        tracing::debug!("Hello response: {response:?}");
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
        let mut client = self
            .client
            .clone()
            .context("FATAL trying to sync with no client, you need to call init_metadata first")?;

        if send_hello {
            // TODO: Should we update wal metadata?
            let _res = client.hello(HelloRequest {}).await?.into_inner();
        }

        let frames = client
            .batch_log_entries(pb::LogOffset {
                next_offset: self.next_offset.load(Ordering::Relaxed),
            })
            .await
            .context("Failed to fetch log entries")?
            .into_inner();
        let frames = frames
            .frames
            .into_iter()
            .map(|f| Frame::try_from_bytes(f.data))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(frames)
    }
}

#[derive(Clone)]
pub struct AuthInterceptor(MetadataValue<Ascii>);

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut().insert("x-authorization", self.0.clone());
        Ok(req)
    }
}
