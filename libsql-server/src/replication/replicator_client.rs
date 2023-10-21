use std::path::Path;
use std::pin::Pin;

use libsql_replication::frame::Frame;
use libsql_replication::replicator::{Error, ReplicatorClient, map_frame_err};
use libsql_replication::rpc::replication::replication_log_client::ReplicationLogClient;
use libsql_replication::rpc::replication::{HelloRequest, LogOffset};
use libsql_replication::meta::WalIndexMeta;
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::rpc::{NAMESPACE_DOESNT_EXIST, NAMESPACE_METADATA_KEY};

pub struct Client {
    client: ReplicationLogClient<Channel>,
    meta: WalIndexMeta,
    pub current_frame_no_notifier: watch::Sender<Option<FrameNo>>,
    namespace: NamespaceName,
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
                self.meta.merge_hello(hello)?;
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
        let stream = self.client
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
        let stream = self.client.snapshot(req).await
            .map_err(|e| Error::Client(e.into()))?
            .into_inner().map(map_frame_err);
        Ok(Box::pin(stream))
    }

    async fn commit_frame_no(&mut self, frame_no: libsql_replication::frame::FrameNo) -> Result<(), Error> {
        self.current_frame_no_notifier.send_replace(Some(frame_no));
        self.meta.set_commit_frame_no(frame_no).await?;

        Ok(())
    }

    fn committed_frame_no(&self) -> Option<FrameNo> {
        self.meta.current_frame_no()
    }
}
