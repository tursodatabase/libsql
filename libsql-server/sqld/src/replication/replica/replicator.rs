use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use futures::StreamExt;
use tonic::transport::Channel;

use crate::replication::frame::Frame;
use crate::replication::replica::snapshot::TempSnapshot;
use crate::replication::FrameNo;
use crate::rpc::replication_log::rpc::{
    replication_log_client::ReplicationLogClient, HelloRequest, LogOffset,
};
use crate::rpc::replication_log::NEED_SNAPSHOT_ERROR_MSG;

use super::hook::Frames;
use super::injector::FrameInjectorHandle;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type Client = ReplicationLogClient<Channel>;

/// The `Replicator` duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator {
    client: Client,
    db_path: PathBuf,
    injector: Option<FrameInjectorHandle>,
    current_frame_no: FrameNo,
}

impl Replicator {
    pub fn new(db_path: PathBuf, channel: Channel, uri: tonic::transport::Uri) -> Self {
        let client = Client::with_origin(channel, uri);
        Self {
            client,
            db_path,
            injector: None,
            current_frame_no: FrameNo::MAX,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            if self.injector.is_none() {
                self.try_perform_handshake().await?;
            }
            let _ = self.replicate().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn try_perform_handshake(&mut self) -> anyhow::Result<()> {
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            if let Ok(resp) = self.client.hello(HelloRequest {}).await {
                let hello = resp.into_inner();
                if let Some(applicator) = self.injector.take() {
                    applicator.shutdown().await?;
                }
                let (applicator, last_applied_frame_no) =
                    FrameInjectorHandle::new(self.db_path.clone(), hello).await?;
                self.current_frame_no = last_applied_frame_no;
                self.injector.replace(applicator);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        bail!("couldn't connect to primary after {HANDSHAKE_MAX_RETRIES} tries ({HANDSHAKE_MAX_RETRIES} seconds)");
    }

    async fn replicate(&mut self) -> anyhow::Result<()> {
        let offset = LogOffset {
            // if current == FrameNo::Max then it means that we're starting fresh
            current_offset: self.current_frame_no(),
        };
        let mut stream = self.client.log_entries(offset).await?.into_inner();

        let mut buffer = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    let frame = Frame::try_from_bytes(frame.data)?;
                    buffer.push(frame.clone());
                    if frame.header().size_after != 0 {
                        self.flush_txn(std::mem::take(&mut buffer)).await?;
                    }
                }
                Some(Err(err))
                    if err.code() == tonic::Code::FailedPrecondition
                        && err.message() == NEED_SNAPSHOT_ERROR_MSG =>
                {
                    return self.load_snapshot().await;
                }
                Some(Err(e)) => {
                    // non fatal error. We shutdown the injector, and reset the protocol,
                    // attempting, to recover replication where we left it.
                    if let Some(injector) = self.injector.take() {
                        injector.shutdown().await?;
                    }
                    tracing::warn!("replication error: {e}");
                    return Ok(());
                }
                None => return Ok(()),
            }
        }
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        let frames = self
            .client
            .snapshot(LogOffset {
                current_offset: self.current_frame_no(),
            })
            .await?
            .into_inner();

        let stream = frames.map(|data| match data {
            Ok(frame) => Frame::try_from_bytes(frame.data),
            Err(e) => anyhow::bail!(e),
        });
        let snap = TempSnapshot::from_stream(&self.db_path, stream).await?;
        self.current_frame_no = self
            .injector
            .as_mut()
            .unwrap()
            .apply_frames(Frames::Snapshot(snap))
            .await?;

        Ok(())
    }

    async fn flush_txn(&mut self, frames: Vec<Frame>) -> anyhow::Result<()> {
        self.current_frame_no = self
            .injector
            .as_mut()
            .expect("invalid state")
            .apply_frames(Frames::Vec(frames))
            .await?;

        Ok(())
    }

    fn current_frame_no(&self) -> Option<FrameNo> {
        (self.current_frame_no != FrameNo::MAX).then_some(self.current_frame_no)
    }
}
