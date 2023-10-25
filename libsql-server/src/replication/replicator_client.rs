use std::path::Path;
use std::pin::Pin;

use bytes::Bytes;
use libsql_replication::frame::Frame;
use libsql_replication::meta::WalIndexMeta;
use libsql_replication::replicator::{map_frame_err, Error, ReplicatorClient};
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_replication::rpc::replication::{
    verify_session_token, HelloRequest, LogOffset, NAMESPACE_METADATA_KEY, NO_HELLO_ERROR_MSG,
    SESSION_TOKEN_KEY,
};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::{AsciiMetadataValue, BinaryMetadataValue};
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::rpc::NAMESPACE_DOESNT_EXIST;

pub struct Client {
    client: ReplicationLogClient<Channel>,
    meta: WalIndexMeta,
    pub current_frame_no_notifier: watch::Sender<Option<FrameNo>>,
    namespace: NamespaceName,
    session_token: Option<Bytes>,
}

impl Client {
    pub async fn new(
        namespace: NamespaceName,
        client: ReplicationLogClient<Channel>,
        path: &Path,
    ) -> crate::Result<Self> {
        let (current_frame_no_notifier, _) = watch::channel(None);
        let meta = WalIndexMeta::open(path).await?;

        Ok(Self {
            namespace,
            client,
            current_frame_no_notifier,
            meta,
            session_token: None,
        })
    }

    fn make_request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert_bin(
            NAMESPACE_METADATA_KEY,
            BinaryMetadataValue::from_bytes(self.namespace.as_slice()),
        );

        if let Some(token) = self.session_token.clone() {
            // SAFETY: we always check the session token
            req.metadata_mut().insert(SESSION_TOKEN_KEY, unsafe {
                AsciiMetadataValue::from_shared_unchecked(token)
            });
        }

        req
    }

    fn next_frame_no(&self) -> FrameNo {
        match *self.current_frame_no_notifier.borrow() {
            Some(fno) => fno + 1,
            None => 0,
        }
    }

    pub(crate) fn reset_token(&mut self) {
        self.session_token = None;
    }
}

#[derive(Debug, thiserror::Error)]
#[error("namespace doesn't exist")]
pub struct NamespaceDoesntExist;

#[async_trait::async_trait]
impl ReplicatorClient for Client {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

    async fn handshake(&mut self) -> Result<(), Error> {
        tracing::info!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest::default());
        match self.client.hello(req).await {
            Ok(resp) => {
                let hello = resp.into_inner();
                verify_session_token(&hello.session_token).map_err(Error::Client)?;
                self.session_token.replace(hello.session_token.clone());
                self.meta.init_from_hello(hello)?;
                self.current_frame_no_notifier
                    .send_replace(self.meta.current_frame_no());

                Ok(())
            }
            Err(e)
                if e.code() == Code::FailedPrecondition
                    && e.message() == NAMESPACE_DOESNT_EXIST =>
            {
                Err(Error::Fatal(NamespaceDoesntExist.into()))?
            }
            Err(e) => Err(Error::Client(e.into()))?,
        }
    }

    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
        };
        let req = self.make_request(offset);
        let stream = self
            .client
            .log_entries(req)
            .await
            .map_err(|e| Error::Client(e.into()))?
            .into_inner()
            .map(map_frame_err);

        Ok(Box::pin(stream))
    }

    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
        };
        let req = self.make_request(offset);
        let stream = self
            .client
            .snapshot(req)
            .await
            .map_err(map_status)?
            .into_inner()
            .map(map_frame_err);
        Ok(Box::pin(stream))
    }

    async fn commit_frame_no(
        &mut self,
        frame_no: libsql_replication::frame::FrameNo,
    ) -> Result<(), Error> {
        self.current_frame_no_notifier.send_replace(Some(frame_no));
        self.meta.set_commit_frame_no(frame_no).await?;

        Ok(())
    }

    fn committed_frame_no(&self) -> Option<FrameNo> {
        self.meta.current_frame_no()
    }
}

fn map_status(status: tonic::Status) -> Error {
    if status.code() == Code::FailedPrecondition && status.message() == NO_HELLO_ERROR_MSG {
        Error::NoHandshake
    } else {
        Error::Client(status.into())
    }
}
