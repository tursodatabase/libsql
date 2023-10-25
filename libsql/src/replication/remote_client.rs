use std::mem::size_of;
use std::path::Path;
use std::pin::Pin;

use bytes::Bytes;
use futures::StreamExt as _;
use libsql_replication::replicator::{ReplicatorClient, Error, map_frame_err};
use libsql_replication::meta::WalIndexMeta;
use libsql_replication::frame::{FrameNo, Frame, FrameHeader};
use libsql_replication::rpc::replication::{HelloRequest, LogOffset, verify_session_token, SESSION_TOKEN_KEY};
use tokio_stream::Stream;
use tonic::metadata::AsciiMetadataValue;

/// A remote replicator client, that pulls frames over RPC
pub struct RemoteClient {
    remote: super::client::Client,
    meta: WalIndexMeta,
    last_received: Option<FrameNo>,
    session_token: Option<Bytes>,
}

impl RemoteClient {
    pub(crate) async fn new(remote: super::client::Client, path: &Path) -> anyhow::Result<Self> {
        let meta = WalIndexMeta::open(path.parent().unwrap()).await?;
        Ok(Self {
            remote,
            meta,
            last_received: None,
            session_token: None,
        })
    }

    fn next_offset(&self) -> FrameNo {
        match self.last_received {
            Some(fno) => fno + 1,
            None => 0,
        }
    }
    
    fn make_request<T>(&self, req: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(req);
        if let Some(token) = self.session_token.clone() {
            // SAFETY: we always validate the token
            req.metadata_mut().insert(SESSION_TOKEN_KEY, unsafe { AsciiMetadataValue::from_shared_unchecked(token) });
        }

        req
    }
}

#[async_trait::async_trait]
impl ReplicatorClient for RemoteClient {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

    /// Perform handshake with remote
    async fn handshake(&mut self) -> Result<(), Error> {
        tracing::info!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest::default());
        match self.remote.replication.hello(req).await {
            Ok(resp) => {
                let hello = resp.into_inner();
                verify_session_token(&hello.session_token).map_err(Error::Client)?;
                self.session_token = Some(hello.session_token.clone());
                self.meta.init_from_hello(hello)?;

                Ok(())
            }
            Err(e) => Err(Error::Client(e.into()))?,
        }
    }

    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        let req = self.make_request(LogOffset { next_offset: self.next_offset() });
        let frames = self
            .remote
            .replication
            .batch_log_entries(req)
            .await
            .map_err(|e| Error::Client(e.into()))?
            .into_inner()
            .frames;

        if let Some(f) = frames.last() {
            let header: FrameHeader = bytemuck::pod_read_unaligned(&f.data[0..size_of::<FrameHeader>()]);
            self.last_received = Some(header.frame_no);
        }

        let frames_iter = frames
            .into_iter()
            .map(|f| Frame::try_from(&*f.data).map_err(|e| Error::Client(e.into())));

        let stream = tokio_stream::iter(frames_iter);

        Ok(Box::pin(stream))
    }

    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        let req = self.make_request(LogOffset { next_offset: self.next_offset() });
        let mut frames = self
            .remote
            .replication
            .snapshot(req)
            .await
            .map_err(|e| Error::Client(e.into()))?
            .into_inner()
            .map(map_frame_err)
            .peekable();

        {
            let frames = Pin::new(&mut frames);

            // the first frame is the one with the highest frame_no in the snapshot
            if let Some(Ok(f)) = frames.peek().await {
                self.last_received = Some(f.header().frame_no);
            }
        }

        Ok(Box::pin(frames))
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
