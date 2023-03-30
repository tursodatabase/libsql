use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use futures::StreamExt;
use tonic::transport::Channel;

use crate::replication::frame::Frame;
use crate::replication::FrameNo;
use crate::rpc::replication_log::rpc::{
    replication_log_client::ReplicationLogClient, HelloRequest, LogOffset,
};

use super::hook::FrameApplicatorHandle;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type Client = ReplicationLogClient<Channel>;

struct LogReplicator {
    client: Client,
    db_path: PathBuf,
    applicator: Option<FrameApplicatorHandle>,
    current_frame_no: FrameNo,
}

impl LogReplicator {
    async fn new(db_path: PathBuf, client: Client) -> anyhow::Result<Self> {
        Ok(Self {
            client,
            db_path,
            applicator: None,
            current_frame_no: FrameNo::MAX,
        })
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            if self.applicator.is_none() {
                self.try_perform_handshake().await?;
            }
            let _ = self.replicate().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }

    async fn try_perform_handshake(&mut self) -> anyhow::Result<()> {
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            if let Ok(resp) = self.client.hello(HelloRequest {}).await {
                let hello = resp.into_inner();
                if let Some(applicator) = self.applicator.take() {
                    applicator.shutdown().await?;
                }
                let applicator = FrameApplicatorHandle::new(self.db_path.clone(), hello).await?;
                self.applicator.replace(applicator);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        bail!("couldn't connect to primary after {HANDSHAKE_MAX_RETRIES} tries ({HANDSHAKE_MAX_RETRIES} seconds)");
    }

    async fn replicate(&mut self) -> anyhow::Result<()> {
        let offset = LogOffset {
            // if current == FrameNo::Max then it means that we're starting fresh
            current_offset: (self.current_frame_no != FrameNo::MAX)
                .then_some(self.current_frame_no),
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
                Some(Err(_)) => todo!(),
                None => return Ok(()),
            }
        }
    }

    async fn flush_txn(&mut self, frames: Vec<Frame>) -> anyhow::Result<()> {
        self.current_frame_no = self
            .applicator
            .as_mut()
            .expect("invalid state")
            .apply_frames(frames)
            .await?;

        Ok(())
    }
}
