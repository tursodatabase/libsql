use std::path::Path;
use std::pin::Pin;

use bytes::Bytes;
use futures::StreamExt as _;
use libsql_replication::frame::{Frame, FrameHeader, FrameNo};
use libsql_replication::meta::WalIndexMeta;
use libsql_replication::replicator::{map_frame_err, Error, ReplicatorClient};
use libsql_replication::rpc::replication::{
    verify_session_token, HelloRequest, LogOffset, SESSION_TOKEN_KEY,
};
use tokio_stream::Stream;
use tonic::metadata::AsciiMetadataValue;
use zerocopy::FromBytes;

/// A remote replicator client, that pulls frames over RPC
pub struct RemoteClient {
    remote: super::client::Client,
    meta: WalIndexMeta,
    last_received: Option<FrameNo>,
    session_token: Option<Bytes>,
    last_handshake_replication_index: Option<FrameNo>,
    // the replication log is dirty, reset the meta on next handshake
    dirty: bool,
}

impl RemoteClient {
    pub(crate) async fn new(remote: super::client::Client, path: &Path) -> anyhow::Result<Self> {
        let meta = WalIndexMeta::open_prefixed(path).await?;
        Ok(Self {
            remote,
            last_received: meta.current_frame_no(),
            meta,
            session_token: None,
            dirty: false,
            last_handshake_replication_index: None,
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
    async fn handshake(&mut self) -> Result<(), Error> {
        tracing::info!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest::new());
        let resp = self.remote.replication.hello(req).await?;
        let hello = resp.into_inner();
        verify_session_token(&hello.session_token).map_err(Error::Client)?;
        self.session_token = Some(hello.session_token.clone());
        if self.dirty {
            self.meta.reset();
            self.last_received = self.meta.current_frame_no();
            self.dirty = false;
        }
        let current_replication_index = hello.current_replication_index;
        if let Err(e) = self.meta.init_from_hello(hello) {
            // set the meta as dirty. The caller should catch the error and clean the db
            // file. On the next call to replicate, the db will be replicated from the new
            // log.
            if let libsql_replication::meta::Error::LogIncompatible = e {
                self.dirty = true;
            }

            Err(e)?;
        }
        self.last_handshake_replication_index = current_replication_index;
        self.meta.flush().await?;

        Ok(())
    }

    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        let req = self.make_request(LogOffset {
            next_offset: self.next_offset(),
        });
        let frames = self
            .remote
            .replication
            .batch_log_entries(req)
            .await?
            .into_inner()
            .frames;

        if let Some(f) = frames.last() {
            let header: FrameHeader = FrameHeader::read_from_prefix(&f.data)
                .ok_or_else(|| Error::Internal("invalid frame header".into()))?;
            self.last_received = Some(header.frame_no.get());
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
        let req = self.make_request(LogOffset {
            next_offset: self.next_offset(),
        });
        let mut frames = self
            .remote
            .replication
            .snapshot(req)
            .await?
            .into_inner()
            .map(map_frame_err)
            .peekable();

        {
            let frames = Pin::new(&mut frames);

            // the first frame is the one with the highest frame_no in the snapshot
            if let Some(Ok(f)) = frames.peek().await {
                self.last_received = Some(f.header().frame_no.get());
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

    fn rollback(&mut self) {
        self.last_received = self.committed_frame_no()
    }
}
