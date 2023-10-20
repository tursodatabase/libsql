use std::path::Path;
use std::pin::Pin;

use libsql_replication::frame::Frame;
use libsql_replication::replicator::{Error, ReplicatorClient};
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_replication::rpc::replication::{HelloRequest, LogOffset, Frame as RpcFrame};
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::rpc::replication_log::NEED_SNAPSHOT_ERROR_MSG;
use crate::rpc::{NAMESPACE_DOESNT_EXIST, NAMESPACE_METADATA_KEY};

use super::error::ReplicationError;
use super::meta::WalIndexMeta;

pub struct Client {
    client: ReplicationLogClient<Channel>,
    meta: WalIndexMeta,
    pub current_frame_no_notifier: watch::Sender<Option<FrameNo>>,
    namespace: NamespaceName,
}

impl From<ReplicationError> for Error {
    fn from(error: ReplicationError) -> Self {
        match error {
            ReplicationError::LogIncompatible
            | ReplicationError::NamespaceDoesntExist(_)
            | ReplicationError::FailedToCommit(_) => Error::Fatal(error.into()),
            _ => Error::Client(error.into()),
        }
    }
}

impl Client {
    pub async fn new(namespace: NamespaceName, client: ReplicationLogClient<Channel>, path: &Path) -> crate::Result<Self> {
        let (current_frame_no_notifier, _) = watch::channel(None);
        let meta = WalIndexMeta::open(&path).await?;

        Ok(Self {
            namespace,
            client,
            current_frame_no_notifier,
            meta,
        })
    }

    fn make_request<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        req.metadata_mut().insert_bin(
            NAMESPACE_METADATA_KEY,
            BinaryMetadataValue::from_bytes(self.namespace.as_slice()),
        );

        req
    }

    fn next_frame_no(&self) -> FrameNo {
        match *self.current_frame_no_notifier.borrow() {
            Some(fno) => fno + 1,
            None => 0,
        }
    }
}

#[async_trait::async_trait]
impl ReplicatorClient for Client {
    type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

    async fn handshake(&mut self) -> Result<(), Error> {
        tracing::info!("Attempting to perform handshake with primary.");
        let req = self.make_request(HelloRequest {});
        match self.client.hello(req).await {
            Ok(resp) => {
                let hello = resp.into_inner();
                self.meta.merge_hello(hello)?;
                self.current_frame_no_notifier
                    .send_replace(self.meta.current_frame_no());

                Ok(())
            }
            Err(e)
                if e.code() == Code::FailedPrecondition
                    && e.message() == NAMESPACE_DOESNT_EXIST =>
            {
                Err(ReplicationError::NamespaceDoesntExist(
                    self.namespace.clone(),
                ))?
            }
            Err(e) => Err(ReplicationError::Other(e.into()))?,
        }
    }

    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
        };
        let req = self.make_request(offset);
        let stream = self.client
            .log_entries(req)
            .await
            .map_err(ReplicationError::Rpc)?
            .into_inner()
            .map(map_frame_err);

        Ok(Box::pin(stream))
    }

    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        let offset = LogOffset {
            next_offset: self.next_frame_no(),
        };
        let req = self.make_request(offset);
        let stream = self.client.snapshot(req).await
            .map_err(ReplicationError::Rpc)?
            .into_inner().map(map_frame_err);
        Ok(Box::pin(stream))
    }

    async fn commit_frame_no(&mut self, frame_no: libsql_replication::frame::FrameNo) -> Result<(), Error> {
        self.current_frame_no_notifier.send_replace(Some(frame_no));
        self.meta.set_commit_frame_no(frame_no).await?;

        Ok(())
    }
}

fn map_frame_err(f: Result<RpcFrame, tonic::Status>) -> Result<Frame, Error> {
    match f {
        Ok(frame) => Ok(Frame::try_from(&*frame.data).map_err(|_| ReplicationError::InvalidFrame)?),
        Err(err) if err.code() == tonic::Code::FailedPrecondition
                    && err.message() == NEED_SNAPSHOT_ERROR_MSG => {
                        Err(Error::NeedSnapshot)
        }
        Err(err) => Err(ReplicationError::Rpc(err))?
    }
}
