use std::mem::size_of;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use bytes::Bytes;
use futures::stream::BoxStream;
use libsql_replication::rpc::replication::log_offset::WalFlavor;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLog;
use libsql_replication::rpc::replication::{
    Frame as RpcFrame, Frames, HelloRequest, HelloResponse, LogOffset, NAMESPACE_DOESNT_EXIST,
};
use libsql_wal::io::StdIO;
use libsql_wal::registry::WalRegistry;
use libsql_wal::segment::Frame;
use libsql_wal::shared_wal::SharedWal;
use md5::{Digest as _, Md5};
use tokio_stream::Stream;
use tonic::Status;
use uuid::Uuid;

use crate::auth::Auth;
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::SqldStorage;

pub struct LibsqlReplicationService {
    registry: Arc<WalRegistry<StdIO, SqldStorage>>,
    store: NamespaceStore,
    user_auth_strategy: Option<Auth>,
    disable_namespaces: bool,
    session_token: Bytes,
}

impl LibsqlReplicationService {
    pub fn new(
        registry: Arc<WalRegistry<StdIO, SqldStorage>>,
        store: NamespaceStore,
        user_auth_strategy: Option<Auth>,
        disable_namespaces: bool,
    ) -> Self {
        let session_token = Uuid::new_v4().to_string().into();
        Self {
            registry,
            disable_namespaces,
            store,
            user_auth_strategy,
            session_token,
        }
    }

    async fn authenticate<T>(
        &self,
        req: &tonic::Request<T>,
        namespace: NamespaceName,
    ) -> Result<(), Status> {
        super::auth::authenticate(&self.store, req, namespace, &self.user_auth_strategy, false)
            .await
    }

    fn encode_session_token(&self, version: usize) -> Uuid {
        let mut sha = Md5::new();
        sha.update(&self.session_token[..]);
        sha.update(version.to_le_bytes());

        let num = sha.finalize();
        let num = u128::from_le_bytes(num.into());
        Uuid::from_u128(num)
    }
}

pin_project_lite::pin_project! {
    struct FrameStreamAdapter<S> {
        #[pin]
        inner: S,
        flavor: WalFlavor,
        shared: Arc<SharedWal<StdIO>>,
    }
}

impl<S> FrameStreamAdapter<S> {
    fn new(inner: S, flavor: WalFlavor, shared: Arc<SharedWal<StdIO>>) -> Self {
        Self {
            inner,
            flavor,
            shared,
        }
    }
}

impl<S> Stream for FrameStreamAdapter<S>
where
    S: Stream<Item = Result<Box<Frame>, libsql_wal::replication::Error>>,
{
    type Item = Result<RpcFrame, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.inner.poll_next(cx)) {
            Some(Ok(f)) => {
                match this.flavor {
                    WalFlavor::Libsql => {
                        let durable_frame_no = if f.header().is_commit() {
                            Some(this.shared.durable_frame_no())
                        } else {
                            None
                        };
                        // safety: frame implemements zerocopy traits, so it can safely be interpreted as a
                        // byte slize of the same size
                        let bytes: Box<[u8; size_of::<Frame>()]> =
                            unsafe { std::mem::transmute(f) };

                        let data = Bytes::from(bytes as Box<[u8]>);
                        Poll::Ready(Some(Ok(RpcFrame {
                            data,
                            timestamp: None,
                            durable_frame_no,
                        })))
                    }
                    WalFlavor::Sqlite => {
                        let header = libsql_replication::frame::FrameHeader {
                            frame_no: f.header().frame_no().into(),
                            checksum: 0.into(),
                            page_no: f.header().page_no().into(),
                            size_after: f.header().size_after().into(),
                        };

                        let frame = libsql_replication::frame::Frame::from_parts(&header, f.data());
                        Poll::Ready(Some(Ok(RpcFrame {
                            data: frame.bytes(),
                            timestamp: None,
                            durable_frame_no: None,
                        })))
                    }
                }
            }
            Some(Err(_e)) => todo!(),
            None => Poll::Ready(None),
        }
    }
}

#[tonic::async_trait]
impl ReplicationLog for LibsqlReplicationService {
    type LogEntriesStream = BoxStream<'static, Result<RpcFrame, Status>>;
    type SnapshotStream = BoxStream<'static, Result<RpcFrame, Status>>;

    #[tracing::instrument(skip_all, fields(namespace))]
    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;
        self.authenticate(&req, namespace.clone()).await?;
        let shared = self.registry.get_async(&namespace.into()).await.unwrap();
        let req = req.into_inner();
        // TODO: replicator should only accecpt NonZero
        let replicator = libsql_wal::replication::replicator::Replicator::new(
            shared.clone(),
            req.next_offset.max(1),
        );

        let flavor = req.wal_flavor();
        let stream = FrameStreamAdapter::new(replicator.into_frame_stream(), flavor, shared);
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn batch_log_entries(
        &self,
        _req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Frames>, Status> {
        todo!()
        // let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;
        // self.authenticate(&req, namespace.clone()).await?;
        // let shared = self.registry.get_async(&namespace.into()).await.unwrap();
        // let replicator = libsql_wal::replication::replicator::Replicator::new(shared, req.into_inner().next_offset);
        //
        // let frames = FrameStreamAdapter::new(replicator.into_frame_stream())
        //     .take_while(|)
        //     .collect::<Result<Vec<_>, Status>>().await?;
        // Ok(tonic::Response::new(Frames { frames }))
    }

    async fn hello(
        &self,
        req: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, Status> {
        let namespace = super::super::extract_namespace(self.disable_namespaces, &req)?;
        self.authenticate(&req, namespace.clone()).await?;

        let shared = self
            .registry
            .get_async(&namespace.clone().into())
            .await
            .unwrap();
        let log_id = shared.log_id();
        let current_replication_index = shared.last_committed_frame_no();
        let (config, version) = self
            .store
            .with(namespace, |ns| -> Result<_, Status> {
                let config = ns.config();
                let version = ns.config_version();
                Ok((config, version))
            })
            .await
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e.as_ref() {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })??;

        let session_hash = self.encode_session_token(version);

        let response = HelloResponse {
            log_id: log_id.to_string(),
            session_token: session_hash.to_string().into(),
            generation_id: Uuid::from_u128(0).to_string(),
            generation_start_index: 0,
            current_replication_index: Some(current_replication_index),
            config: Some(config.as_ref().into()),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        _req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        Err(Status::unimplemented(
            "no snapshot required with libsql wal",
        ))
    }
}
