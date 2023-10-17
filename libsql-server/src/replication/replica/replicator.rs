use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;

use bytemuck::bytes_of;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio::task::JoinSet;
use tokio::time::Duration;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::namespace::{NamespaceName, ResetCb, ResetOp};
use crate::replication::frame::Frame;
use crate::replication::replica::error::ReplicationError;
use crate::replication::replica::snapshot::TempSnapshot;
use crate::replication::FrameNo;
use crate::rpc::replication_log::rpc::{
    replication_log_client::ReplicationLogClient, HelloRequest, LogOffset,
};
use crate::rpc::replication_log::NEED_SNAPSHOT_ERROR_MSG;
use crate::rpc::{NAMESPACE_DOESNT_EXIST, NAMESPACE_METADATA_KEY};
use crate::BLOCKING_RT;

use super::hook::{Frames, InjectorHookCtx};
use super::injector::FrameInjector;
use super::meta::WalIndexMeta;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type Client = ReplicationLogClient<Channel>;

/// The `Replicator` duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator {
    client: Client,
    db_path: PathBuf,
    namespace: NamespaceName,
    meta: Arc<Mutex<Option<WalIndexMeta>>>,
    pub current_frame_no_notifier: watch::Receiver<Option<FrameNo>>,
    frames_sender: mpsc::Sender<Frames>,
    /// hard reset channel: send the namespace there, to reset it
    reset: ResetCb,
}

impl Replicator {
    pub async fn new(
        db_path: PathBuf,
        channel: Channel,
        uri: tonic::transport::Uri,
        namespace: NamespaceName,
        join_set: &mut JoinSet<anyhow::Result<()>>,
        reset: ResetCb,
    ) -> anyhow::Result<Self> {
        let client = Client::with_origin(channel, uri);
        let (applied_frame_notifier, current_frame_no_notifier) = watch::channel(None);
        let (frames_sender, receiver) = tokio::sync::mpsc::channel(1);

        let mut this = Self {
            namespace,
            client,
            db_path: db_path.clone(),
            current_frame_no_notifier,
            meta: Arc::new(Mutex::new(None)),
            frames_sender,
            reset,
        };

        this.try_perform_handshake().await?;

        let meta_file = Arc::new(WalIndexMeta::open(&db_path)?);
        let meta = this.meta.clone();

        let pre_commit = {
            let meta = meta.clone();
            let meta_file = meta_file.clone();
            move |fno| {
                let mut lock = meta.blocking_lock();
                let meta = lock
                    .as_mut()
                    .expect("commit called before meta inialization");
                meta.pre_commit_frame_no = fno;
                meta_file.write_all_at(bytes_of(meta), 0)?;

                Ok(())
            }
        };

        let post_commit = {
            let meta = meta.clone();
            let meta_file = meta_file;
            let notifier = applied_frame_notifier;
            move |fno| {
                let mut lock = meta.blocking_lock();
                let meta = lock
                    .as_mut()
                    .expect("commit called before meta inialization");
                assert_eq!(meta.pre_commit_frame_no, fno);
                meta.post_commit_frame_no = fno;
                meta_file.write_all_at(bytes_of(meta), 0)?;
                let _ = notifier.send(Some(fno));

                Ok(())
            }
        };

        let (snd, rcv) = oneshot::channel();
        let handle = BLOCKING_RT.spawn_blocking({
            move || -> anyhow::Result<()> {
                let ctx = InjectorHookCtx::new(receiver, pre_commit, post_commit);
                let mut injector = FrameInjector::new(&db_path, ctx)?;
                let _ = snd.send(());

                while injector.step()? {}

                Ok(())
            }
        });

        join_set.spawn(async move {
            handle.await??;
            Ok(())
        });

        // injector is ready:
        rcv.await?;

        Ok(this)
    }

    fn make_request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert_bin(
            NAMESPACE_METADATA_KEY,
            BinaryMetadataValue::from_bytes(self.namespace.as_slice()),
        );

        req
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            self.try_perform_handshake().await?;

            if let Err(e) = self.replicate().await {
                // Replication encountered an error. We log the error, and then shut down the
                // injector and propagate a potential panic from there.
                tracing::warn!("replication error: {e}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn handle_replication_error(&self, error: ReplicationError) -> crate::error::Error {
        match error {
            ReplicationError::Lagging => {
                tracing::error!("Replica ahead of primary: hard-reseting replica");
            }
            ReplicationError::DbIncompatible => {
                tracing::error!(
                    "Primary is attempting to replicate a different database, overwriting replica."
                );
            }
            _ => return error.into(),
        }

        (self.reset)(ResetOp::Reset(self.namespace.clone()));

        error.into()
    }

    async fn try_perform_handshake(&mut self) -> crate::Result<()> {
        let mut error_printed = false;
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            let req = self.make_request(HelloRequest {});
            match self.client.hello(req).await {
                Ok(resp) => {
                    let hello = resp.into_inner();

                    let mut lock = self.meta.lock().await;
                    let meta = match *lock {
                        Some(meta) => match meta.merge_from_hello(hello) {
                            Ok(meta) => meta,
                            Err(e) => return Err(self.handle_replication_error(e).await),
                        },
                        None => match WalIndexMeta::read_from_path(&self.db_path)? {
                            Some(meta) => match meta.merge_from_hello(hello) {
                                Ok(meta) => meta,
                                Err(e) => return Err(self.handle_replication_error(e).await),
                            },
                            None => WalIndexMeta::new_from_hello(hello)?,
                        },
                    };

                    *lock = Some(meta);

                    return Ok(());
                }
                Err(e)
                    if e.code() == Code::FailedPrecondition
                        && e.message() == NAMESPACE_DOESNT_EXIST =>
                {
                    tracing::info!("namespace `{}` doesn't exist, cleaning...", self.namespace);
                    (self.reset)(ResetOp::Destroy(self.namespace.clone()));

                    return Err(crate::error::Error::NamespaceDoesntExist(
                        self.namespace.to_string(),
                    ));
                }
                Err(e) if !error_printed => {
                    tracing::error!("error connecting to primary. retrying. error: {e}");
                    error_printed = true;
                }
                _ => (),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err(crate::error::Error::PrimaryConnectionTimeout)
    }

    async fn replicate(&mut self) -> anyhow::Result<()> {
        const MAX_REPLICA_REPLICATION_BUFFER_LEN: usize = 10_000_000 / 4096; // ~10MB
        let offset = LogOffset {
            // if current == FrameNo::Max then it means that we're starting fresh
            next_offset: self.next_offset(),
        };

        let req = self.make_request(offset);

        let mut stream = self.client.log_entries(req).await?.into_inner();

        let mut buffer = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    let frame = Frame::try_from_bytes(frame.data)?;
                    buffer.push(frame.clone());
                    if frame.header().size_after != 0
                        || buffer.len() > MAX_REPLICA_REPLICATION_BUFFER_LEN
                    {
                        let _ = self
                            .frames_sender
                            .send(Frames::Vec(std::mem::take(&mut buffer)))
                            .await;
                    }
                }
                Some(Err(err))
                    if err.code() == tonic::Code::FailedPrecondition
                        && err.message() == NEED_SNAPSHOT_ERROR_MSG =>
                {
                    tracing::debug!("loading snapshot");
                    // remove any outstanding frames in the buffer that are not part of a
                    // transaction: they are now part of the snapshot.
                    buffer.clear();
                    self.load_snapshot().await?;
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        let next_offset = self.next_offset();

        let req = self.make_request(LogOffset { next_offset });

        let frames = self.client.snapshot(req).await?.into_inner();

        let stream = frames.map(|data| match data {
            Ok(frame) => Frame::try_from_bytes(frame.data),
            Err(e) => anyhow::bail!(e),
        });
        let snap = TempSnapshot::from_stream(&self.db_path, stream).await?;

        let _ = self.frames_sender.send(Frames::Snapshot(snap)).await;

        Ok(())
    }

    fn next_offset(&mut self) -> FrameNo {
        self.current_frame_no().map(|x| x + 1).unwrap_or(0)
    }

    fn current_frame_no(&mut self) -> Option<FrameNo> {
        *self.current_frame_no_notifier.borrow_and_update()
    }
}
