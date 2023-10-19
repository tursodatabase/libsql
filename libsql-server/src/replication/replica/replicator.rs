use std::path::PathBuf;
use std::sync::Arc;

use futures::StreamExt;
use libsql_replication::frame::Frame;
use libsql_replication::injector::{Injector, Error as InjectError};
use parking_lot::Mutex;
use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tokio::time::Duration;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::DEFAULT_AUTO_CHECKPOINT;
use crate::namespace::{NamespaceName, ResetCb, ResetOp};
use crate::replication::replica::error::ReplicationError;
use crate::replication::FrameNo;
use crate::rpc::replication_log::rpc::{
    replication_log_client::ReplicationLogClient, HelloRequest, LogOffset,
};
use crate::rpc::replication_log::NEED_SNAPSHOT_ERROR_MSG;
use crate::rpc::{NAMESPACE_DOESNT_EXIST, NAMESPACE_METADATA_KEY};

use super::meta::WalIndexMeta;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type Client = ReplicationLogClient<Channel>;

/// The `Replicator` duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator {
    client: Client,
    namespace: NamespaceName,
    meta: WalIndexMeta,
    injector: Arc<Mutex<Injector>>,
    pub current_frame_no_notifier: watch::Sender<Option<FrameNo>>,
    reset: ResetCb,
}

const INJECTOR_BUFFER_CAPACITY: usize = 10;

impl Replicator {
    pub async fn new(
        db_path: PathBuf,
        channel: Channel,
        uri: tonic::transport::Uri,
        namespace: NamespaceName,
        reset: ResetCb,
    ) -> anyhow::Result<Self> {
        let (current_frame_no_notifier, _) = watch::channel(None);
        let injector = {
            let db_path = db_path.clone();
            spawn_blocking(move || Injector::new(&db_path, INJECTOR_BUFFER_CAPACITY, DEFAULT_AUTO_CHECKPOINT)).await??
        };
        let client = Client::with_origin(channel, uri);
        let meta = WalIndexMeta::open(&db_path).await?;

        let mut this = Self {
            namespace,
            client,
            current_frame_no_notifier,
            meta,
            injector: Arc::new(Mutex::new(injector)),
            reset,
        };

        this.try_perform_handshake().await?;

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

            loop {
                if let Err(e) = self.replicate().await {
                    // Replication encountered an error. We log the error, and then shut down the
                    // injector and propagate a potential panic from there.
                    tracing::warn!("replication error: {e}");
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn handle_replication_error(&self, error: ReplicationError) -> crate::error::Error {
        match error {
            ReplicationError::LogIncompatible => {
                tracing::error!("Primary's replication log incompatible with ours: repairing.");
                (self.reset)(ResetOp::Reset(self.namespace.clone()));
                error.into()
            }
            _ => error.into(),
        }
    }

    async fn try_perform_handshake(&mut self) -> crate::Result<()> {
        let mut error_printed = false;
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            let req = self.make_request(HelloRequest {});
            match self.client.hello(req).await {
                Ok(resp) => {
                    let hello = resp.into_inner();

                    if let Err(e) = self.meta.merge_hello(hello) {
                        return Err(self.handle_replication_error(e).await);
                    }

                    self.current_frame_no_notifier
                        .send_replace(self.meta.current_frame_no());

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
        let offset = LogOffset {
            next_offset: self.next_offset(),
        };

        let req = self.make_request(offset);

        let mut stream = self.client.log_entries(req).await?.into_inner();

        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    let frame = Frame::try_from(&*frame.data)?;
                    self.inject_frame(frame).await?;
                }
                Some(Err(err))
                    if err.code() == tonic::Code::FailedPrecondition
                        && err.message() == NEED_SNAPSHOT_ERROR_MSG =>
                {
                    tracing::debug!("loading snapshot");
                    // remove any outstanding frames in the buffer that are not part of a
                    // transaction: they are now part of the snapshot.
                    self.load_snapshot().await?;
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        self.injector.lock().clear_buffer();
        let next_offset = self.next_offset();

        let req = self.make_request(LogOffset { next_offset });

        // FIXME: check for unavailable snapshot and try again, or make primary wait for snapshot
        // to become available
        let frames = self.client.snapshot(req).await?.into_inner();

        let mut stream = frames.map(|data| match data {
            Ok(frame) => Ok(Frame::try_from(&*frame.data)?),
            Err(e) => anyhow::bail!(e),
        });

        while let Some(frame) = stream.next().await {
            let frame = frame?;
            self.inject_frame(frame).await?;
        }

        Ok(())
    }

    async fn inject_frame(&mut self, frame: Frame) -> anyhow::Result<()> {
        let injector = self.injector.clone();
        match spawn_blocking(move || injector.lock().inject_frame(frame)).await? {
            Ok(Some(commit_fno)) => {
                self.meta.set_commit_frame_no(commit_fno).await?;
                self.current_frame_no_notifier
                    .send_replace(Some(commit_fno));
            }
            Ok(None) => (),
            Err(e @ InjectError::FatalInjectError) => {
                // we conservatively nuke the replica and start replicating from scractch
                tracing::error!(
                    "fatal error replicating `{}` from primary, resetting namespace...",
                    self.namespace
                );
                (self.reset)(ResetOp::Destroy(self.namespace.clone()));
                Err(e)?
            }
            Err(e) => Err(e)?,
        }

        Ok(())
    }

    fn next_offset(&mut self) -> FrameNo {
        self.current_frame_no().map(|x| x + 1).unwrap_or(0)
    }

    fn current_frame_no(&mut self) -> Option<FrameNo> {
        self.meta.current_frame_no()
    }
}
