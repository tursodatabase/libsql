use std::pin::Pin;

use futures::{TryStreamExt, StreamExt};
use libsql_replication::{replicator::{ReplicatorClient, Error}, frame::{Frame, FrameNo}, rpc::replication::HelloResponse};
use tokio_stream::Stream;

use crate::replication::Frames;

/// A replicator client that doesn't perform networking, and is instead _loaded_ with the frames to
/// inject
pub struct LocalClient {
    frames: Option<Frames>,
}

impl LocalClient {
    pub(crate) async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            frames: None,
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
    async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
        Ok(None)
    }

    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self, _next_frame: FrameNo) -> Result<Self::FrameStream, Error> {
        // TODO: perform some assertion regarding the contained frames, relative to the
        // next_frame_no
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
    async fn snapshot(&mut self, _next_frame: FrameNo) -> Result<Self::FrameStream, Error> {
        match self.frames.take() {
            Some(Frames::Snapshot(frames)) => { 
                let size_after = frames.header().size_after.get();
                let stream = async_stream::try_stream! {
                    let s = frames.into_stream_mut().map_err(|e| Error::Client(Box::new(e))).peekable();
                    tokio::pin!(s);
                    while let Some(mut next) = s.as_mut().next().await.transpose()? {
                        if s.as_mut().peek().await.is_none() {
                            next.header_mut().size_after = size_after.into();
                        }
                        yield Frame::from(next);
                    }
                };

                Ok(Box::pin(stream))
            },
            Some(Frames::Vec(_)) | None => Ok(Box::pin(tokio_stream::empty()))
        }
    }
}

#[cfg(test)]
mod test {
    use libsql_replication::snapshot::SnapshotFile;

    use super::*;

    #[tokio::test]
    async fn snapshot_stream_commited() {
        let snapshot = SnapshotFile::open("assets/test/snapshot.snap").await.unwrap();
        let mut client = LocalClient::new().await.unwrap();
        client.load_frames(Frames::Snapshot(snapshot));
        let mut s = client.snapshot(0).await.unwrap();
        assert!(matches!(s.next().await, Some(Ok(_))));
        let last = s.next().await.unwrap().unwrap();
        assert_eq!(last.header().size_after.get(), 2);
        assert!(s.next().await.is_none());
    }
}
