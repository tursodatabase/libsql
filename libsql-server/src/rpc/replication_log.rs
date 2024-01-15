use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
pub use libsql_replication::rpc::replication as rpc;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLog;
use libsql_replication::rpc::replication::{
    Frame, Frames, HelloRequest, HelloResponse, LogOffset, NAMESPACE_DOESNT_EXIST,
    NEED_SNAPSHOT_ERROR_MSG, NO_HELLO_ERROR_MSG, SESSION_TOKEN_KEY,
};
use md5::{Digest, Md5};
use tokio_stream::StreamExt;
use tonic::transport::server::TcpConnectInfo;
use tonic::Status;
use uuid::Uuid;

use crate::auth::Auth;
use crate::connection::config::DatabaseConfig;
use crate::namespace::{NamespaceName, NamespaceStore, PrimaryNamespaceMaker};
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::{LogReadError, ReplicationLogger};
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

use super::extract_namespace;

pub struct ReplicationLogService {
    namespaces: NamespaceStore<PrimaryNamespaceMaker>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    auth: Option<Arc<Auth>>,
    disable_namespaces: bool,
    session_token: Bytes,

    //deprecated:
    generation_id: Uuid,
    replicas_with_hello: RwLock<HashSet<(SocketAddr, NamespaceName)>>,
}

pub const MAX_FRAMES_PER_BATCH: usize = 1024;

impl ReplicationLogService {
    pub fn new(
        namespaces: NamespaceStore<PrimaryNamespaceMaker>,
        idle_shutdown_layer: Option<IdleShutdownKicker>,
        auth: Option<Arc<Auth>>,
        disable_namespaces: bool,
    ) -> Self {
        let session_token = Uuid::new_v4().to_string().into();
        Self {
            namespaces,
            session_token,
            idle_shutdown_layer,
            auth,
            disable_namespaces,
            generation_id: Uuid::new_v4(),
            replicas_with_hello: Default::default(),
        }
    }

    async fn authenticate<T>(
        &self,
        req: &tonic::Request<T>,
        namespace: NamespaceName,
    ) -> Result<(), Status> {
        let namespace_jwt_key = self.namespaces.with(namespace, |ns| ns.jwt_key()).await;
        match namespace_jwt_key {
            Ok(Ok(jwt_key)) => {
                if let Some(auth) = &self.auth {
                    auth.authenticate_grpc(req, self.disable_namespaces, jwt_key)?;
                }
                Ok(())
            }
            Err(e) => match e.as_ref() {
                crate::error::Error::NamespaceDoesntExist(_) => {
                    if let Some(auth) = &self.auth {
                        auth.authenticate_grpc(req, self.disable_namespaces, None)?;
                    }
                    Ok(())
                }
                _ => Err(Status::internal(format!(
                    "Error fetching jwt key for a namespace: {}",
                    e
                ))),
            },
            Ok(Err(e)) => Err(Status::internal(format!(
                "Error fetching jwt key for a namespace: {}",
                e
            ))),
        }
    }

    fn verify_session_token<R>(
        &self,
        req: &tonic::Request<R>,
        version: usize,
    ) -> Result<(), Status> {
        let no_hello = || Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
        match req.metadata().get(SESSION_TOKEN_KEY) {
            Some(token) => {
                let session_token_hash = self.encode_session_token(version);

                if token.as_bytes() != session_token_hash.to_string().as_bytes() {
                    return no_hello();
                }
            }
            None => {
                // legacy: old replicas used stateful session management
                let replica_addr = req
                    .remote_addr()
                    .ok_or(Status::internal("No remote RPC address"))?;
                {
                    let namespace = extract_namespace(self.disable_namespaces, req)?;
                    let guard = self.replicas_with_hello.read().unwrap();
                    if !guard.contains(&(replica_addr, namespace)) {
                        return no_hello();
                    }
                }
            }
        }

        Ok(())
    }

    async fn logger_from_namespace<T>(
        &self,
        namespace: NamespaceName,
        req: &tonic::Request<T>,
        verify_session: bool,
    ) -> Result<(Arc<ReplicationLogger>, Arc<DatabaseConfig>, usize), Status> {
        let (logger, config, version) = self
            .namespaces
            .with(namespace, |ns| {
                let logger = ns.db.wal_manager.wrapped().logger().clone();
                let config = ns.config();
                let version = ns.config_version();

                (logger, config, version)
            })
            .await
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e.as_ref() {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })?;

        if verify_session {
            self.verify_session_token(req, version)?;
        }

        Ok((logger, config, version))
    }

    fn encode_session_token(&self, version: usize) -> Uuid {
        let mut sha = Md5::new();
        sha.update(&self.session_token[..]);
        sha.update(&version.to_le_bytes());

        let num = sha.finalize();
        let num = u128::from_le_bytes(num.into());
        Uuid::from_u128(num)
    }
}

fn map_frame_stream_output(
    r: Result<(libsql_replication::frame::Frame, Option<DateTime<Utc>>), LogReadError>,
) -> Result<Frame, Status> {
    match r {
        Ok((frame, ts)) => Ok(Frame {
            data: frame.bytes(),
            timestamp: ts.map(|ts| ts.timestamp_millis()),
        }),
        Err(LogReadError::SnapshotRequired) => Err(Status::new(
            tonic::Code::FailedPrecondition,
            NEED_SNAPSHOT_ERROR_MSG,
        )),
        Err(LogReadError::Error(e)) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        // this error should be caught before, but we handle it nicely anyways
        Err(LogReadError::Ahead) => Err(Status::new(
            tonic::Code::OutOfRange,
            "frame not yet available",
        )),
    }
}

pub struct StreamGuard<S> {
    s: S,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
}

impl<S> StreamGuard<S> {
    fn new(s: S, mut idle_shutdown_layer: Option<IdleShutdownKicker>) -> Self {
        if let Some(isl) = idle_shutdown_layer.as_mut() {
            isl.add_connected_replica()
        }
        Self {
            s,
            idle_shutdown_layer,
        }
    }
}

impl<S> Drop for StreamGuard<S> {
    fn drop(&mut self) {
        if let Some(isl) = self.idle_shutdown_layer.as_mut() {
            isl.remove_connected_replica()
        }
    }
}

impl<S: futures::stream::Stream + Unpin> futures::stream::Stream for StreamGuard<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().s).poll_next(cx)
    }
}

#[tonic::async_trait]
impl ReplicationLog for ReplicationLogService {
    type LogEntriesStream = BoxStream<'static, Result<Frame, Status>>;
    type SnapshotStream = BoxStream<'static, Result<Frame, Status>>;

    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

        self.authenticate(&req, namespace.clone()).await?;

        let (logger, _, _) = self.logger_from_namespace(namespace, &req, true).await?;

        let req = req.into_inner();

        let stream = StreamGuard::new(
            FrameStream::new(logger, req.next_offset, true, None)
                .map_err(|e| Status::internal(e.to_string()))?,
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output);

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn batch_log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Frames>, Status> {
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;
        self.authenticate(&req, namespace.clone()).await?;

        let (logger, _, _) = self.logger_from_namespace(namespace, &req, true).await?;

        let req = req.into_inner();

        let frames = StreamGuard::new(
            FrameStream::new(logger, req.next_offset, false, Some(MAX_FRAMES_PER_BATCH))
                .map_err(|e| Status::internal(e.to_string()))?,
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output)
        .collect::<Result<Vec<_>, _>>()
        .await?;

        Ok(tonic::Response::new(Frames { frames }))
    }

    async fn hello(
        &self,
        req: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, Status> {
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;
        self.authenticate(&req, namespace.clone()).await?;

        // legacy support
        if req.get_ref().handshake_version.is_none() {
            req.extensions().get::<TcpConnectInfo>().unwrap();
            let replica_addr = req
                .remote_addr()
                .ok_or(Status::internal("No remote RPC address"))?;

            {
                let mut guard = self.replicas_with_hello.write().unwrap();
                guard.insert((replica_addr, namespace.clone()));
            }
        }

        let (logger, config, version) = self.logger_from_namespace(namespace, &req, false).await?;

        let session_hash = self.encode_session_token(version);

        let response = HelloResponse {
            log_id: logger.log_id().to_string(),
            session_token: session_hash.to_string().into(),
            generation_id: self.generation_id.to_string(),
            generation_start_index: 0,
            current_replication_index: *logger.new_frame_notifier.borrow(),
            config: Some(config.as_ref().into()),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;
        self.authenticate(&req, namespace.clone()).await?;

        let (logger, _, _) = self.logger_from_namespace(namespace, &req, true).await?;

        let req = req.into_inner();

        let offset = req.next_offset;
        match logger.get_snapshot_file(offset).await {
            Ok(Some(snapshot)) => Ok(tonic::Response::new(Box::pin(
                snapshot_stream::make_snapshot_stream(snapshot, offset),
            ))),
            Ok(None) => Err(Status::new(tonic::Code::Unavailable, "snapshot not found")),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        }
    }
}

mod snapshot_stream {
    use futures::{Stream, StreamExt};
    use libsql_replication::frame::FrameNo;
    use libsql_replication::rpc::replication::Frame;
    use libsql_replication::snapshot::SnapshotFile;
    use tonic::Status;

    pub fn make_snapshot_stream(
        snapshot: SnapshotFile,
        offset: FrameNo,
    ) -> impl Stream<Item = Result<Frame, Status>> {
        let size_after = snapshot.header().size_after;
        let frames = snapshot.into_stream_mut_from(offset).peekable();
        async_stream::stream! {
            tokio::pin!(frames);
            while let Some(frame) = frames.next().await {
                match frame {
                    Ok(mut frame) => {
                        // this is the last frame we're sending for this snapshot, set the
                        // frame_no
                        if frames.as_mut().peek().await.is_none() {
                            frame.header_mut().size_after = size_after;
                        }

                        yield Ok(Frame {
                            data: libsql_replication::frame::Frame::from(frame).bytes(),
                            timestamp: None,
                        });
                    }
                    Err(e) => {
                        yield Err(Status::new(
                                tonic::Code::Internal,
                                e.to_string(),
                        ));
                        break;
                    }
                }
            }
        }
    }
}
