use std::pin::Pin;
use std::path::Path;

use futures::TryStreamExt;
use libsql_replication::{replicator::{ReplicatorClient, Error}, frame::{Frame, FrameNo}, meta::WalIndexMeta};
use tokio_stream::Stream;

use crate::replication::Frames;

/// A replicator client that doesn't perform networking, and is instead _loaded_ with the frames to
/// inject
pub struct LocalClient {
    frames: Option<Frames>,
    meta: WalIndexMeta,
}

impl LocalClient {
    pub(crate) async fn new(path: &Path) -> anyhow::Result<Self> {
        let mut meta = WalIndexMeta::open(path.parent().unwrap()).await?;
        meta.init_default();
        Ok(Self {
            frames: None,
            meta,
        })
    }

    /// Load `frames` into the client. The caller must ensure that client was flushed before
    /// calling this function again.
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
        Ok(())
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
            Some(Frames::Snapshot(frames)) => Ok(Box::pin(frames.into_stream_mut().map_ok(Frame::from).map_err(|e| Error::Client(Box::new(e))))),
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
