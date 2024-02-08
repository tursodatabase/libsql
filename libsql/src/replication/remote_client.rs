use std::pin::Pin;

use bytes::Bytes;
use futures::StreamExt as _;
use libsql_replication::frame::{Frame, FrameNo};
use libsql_replication::replicator::{map_frame_err, Error, ReplicatorClient};
use libsql_replication::rpc::replication::{
    verify_session_token, HelloRequest, LogOffset, SESSION_TOKEN_KEY, HelloResponse,
};
use tokio_stream::Stream;
use tonic::metadata::AsciiMetadataValue;

/// A remote replicator client, that pulls frames over RPC
pub struct RemoteClient {
    remote: super::client::Client,
    session_token: Option<Bytes>,
    last_handshake_replication_index: Option<FrameNo>,
}

impl RemoteClient {
    pub(crate) async fn new(remote: super::client::Client) -> anyhow::Result<Self> {
        Ok(Self {
            remote,
            session_token: None,
            last_handshake_replication_index: None,
        })
    }

    fn make_request<T>(&self, req: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(req);
        if let Some(token) = self.session_token.clone() {
            // SAFETY: we always validate the token
            req.metadata_mut().insert(SESSION_TOKEN_KEY, unsafe {
                AsciiMetadataValue::from_shared_unchecked(token)
            });
        }

        req
    }

    pub fn last_handshake_replication_index(&self) -> Option<u64> {
        self.last_handshake_replication_index
    }
}

#[async_trait::async_trait]
impl ReplicatorClient for RemoteClient {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

    /// Perform handshake with remote
    async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
        tracing::info!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest::new());
        let resp = self.remote.replication.hello(req).await?;
        let hello = resp.into_inner();
        verify_session_token(&hello.session_token).map_err(Error::Client)?;
        self.session_token = Some(hello.session_token.clone());
        let current_replication_index = hello.current_replication_index;
        self.last_handshake_replication_index = current_replication_index;

        Ok(Some(hello))
    }

    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self, next_offset: FrameNo) -> Result<Self::FrameStream, Error> {
        dbg!(next_offset);
        let req = self.make_request(LogOffset { next_offset });
        let frames = self
            .remote
            .replication
            .batch_log_entries(req)
            .await?
            .into_inner()
            .frames;

        let frames_iter = frames
            .into_iter()
            .map(|f| Frame::try_from(&*f.data).map_err(|e| Error::Client(e.into())));

        let stream = tokio_stream::iter(frames_iter);

        Ok(Box::pin(stream))
    }

    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self, next_offset: FrameNo) -> Result<Self::FrameStream, Error> {
        let req = self.make_request(LogOffset { next_offset });
        let frames = self
            .remote
            .replication
            .snapshot(req)
            .await?
            .into_inner()
            .map(map_frame_err)
            .peekable();

        Ok(Box::pin(frames))
    }
}
