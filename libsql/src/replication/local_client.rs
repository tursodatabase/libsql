use std::pin::Pin;
use std::path::Path;

use futures::TryStreamExt;
use libsql_replication::{replicator::{ReplicatorClient, Error}, frame::{Frame, FrameNo}, meta::WalIndexMeta};
use tokio_stream::Stream;

use crate::replication::replica::hook::Frames;

pub struct LocalClient {
    frames: Option<Frames>,
    meta: WalIndexMeta,
}

impl LocalClient {
    pub async fn new(path: &Path) -> anyhow::Result<Self> {
        let meta = WalIndexMeta::open(path).await?;
        Ok(Self {
            frames: None,
            meta,
        })
    }

    pub(crate) fn load_frames(&mut self, frames: Frames) {
        assert!(self.frames.is_none(), "frames not flushed before loading");
        self.frames.replace(frames);
    }
}

#[async_trait::async_trait]
impl ReplicatorClient for LocalClient {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

    /// Perform handshake with remote
    async fn handshake(&mut self) -> Result<(), Error> {
        todo!()
    }

    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        match self.frames.take() {
            Some(Frames::Vec(f)) => {
                let iter = f.into_iter().map(Ok);
                Ok(Box::pin(tokio_stream::iter(iter)))
            }
            Some(f @ Frames::Snapshot(_)) => {
                self.frames.replace(f);
                Err(Error::NeedSnapshot)
            },
            None => Ok(Box::pin(tokio_stream::empty()))
        }
    }

    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        match self.frames.take() {
            Some(Frames::Snapshot(frames)) => Ok(Box::pin(frames.map_err(Error::Client))),
            Some(Frames::Vec(_)) | None => Ok(Box::pin(tokio_stream::empty()))
        }
    }

    /// set the new commit frame_no
    async fn commit_frame_no(&mut self, frame_no: FrameNo) -> Result<(), Error> {
        self.meta.set_commit_frame_no(frame_no).await?;
        Ok(())
    }

    fn committed_frame_no(&self) -> Option<FrameNo> {
        self.meta.current_frame_no()
    }
}
