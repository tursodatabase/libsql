pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Status;

use crate::auth::Auth;
use crate::namespace::{NamespaceStore, PrimaryNamespaceMaker};
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::LogReadError;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;
use crate::DEFAULT_NAMESPACE_NAME;

use self::rpc::replication_log_server::ReplicationLog;
use self::rpc::{Frame, Frames, HelloRequest, HelloResponse, LogOffset};

pub struct ReplicationLogService {
    namespaces: Arc<NamespaceStore<PrimaryNamespaceMaker>>,
    replicas_with_hello: RwLock<HashSet<(SocketAddr, Bytes)>>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    auth: Option<Arc<Auth>>,
    disable_namespaces: bool,
}

pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";

impl ReplicationLogService {
    pub fn new(
        namespaces: Arc<NamespaceStore<PrimaryNamespaceMaker>>,
        idle_shutdown_layer: Option<IdleShutdownLayer>,
        auth: Option<Arc<Auth>>,
        disable_namespaces: bool,
    ) -> Self {
        Self {
            namespaces,
            replicas_with_hello: Default::default(),
            idle_shutdown_layer,
            auth,
            disable_namespaces,
        }
    }

    fn authenticate<T>(&self, req: &tonic::Request<T>) -> Result<(), Status> {
        if let Some(auth) = &self.auth {
            let _ = auth.authenticate_grpc(req)?;
        }

        Ok(())
    }

    fn extract_namespace<T>(&self, req: &tonic::Request<T>) -> Result<Bytes, Status> {
        if self.disable_namespaces {
            return Ok(Bytes::from_static(DEFAULT_NAMESPACE_NAME.as_bytes()));
        }

        if let Some(namespace) = req.metadata().get("x-namespace") {
            namespace
                .to_bytes()
                .map_err(|_| Status::invalid_argument("Metadata can't be converted into Bytes"))
        } else {
            Err(Status::invalid_argument("Missing x-namespace metadata"))
        }
    }
}

fn map_frame_stream_output(
    r: Result<crate::replication::frame::Frame, LogReadError>,
) -> Result<Frame, Status> {
    match r {
        Ok(frame) => Ok(Frame {
            data: frame.bytes(),
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
    idle_shutdown_layer: Option<IdleShutdownLayer>,
}

impl<S> StreamGuard<S> {
    fn new(s: S, mut idle_shutdown_layer: Option<IdleShutdownLayer>) -> Self {
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
        self.authenticate(&req)?;
        let namespace = self.extract_namespace(&req)?;

        let replica_addr = req
            .remote_addr()
            .ok_or(Status::internal("No remote RPC address"))?;
        let req = req.into_inner();
        {
            let guard = self.replicas_with_hello.read().unwrap();
            if !guard.contains(&(replica_addr, namespace.clone())) {
                return Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
            }
        }

        let logger = match self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
        {
            Ok(logger) => logger,
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to create database connection: {e}"
                )));
            }
        };

        let stream = StreamGuard::new(
            FrameStream::new(logger, req.next_offset, true),
            self.idle_shutdown_layer.clone(),
        )
        .map(map_frame_stream_output);

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn batch_log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Frames>, Status> {
        self.authenticate(&req)?;
        let namespace = self.extract_namespace(&req)?;

        let replica_addr = req
            .remote_addr()
            .ok_or(Status::internal("No remote RPC address"))?;
        let req = req.into_inner();
        {
            let guard = self.replicas_with_hello.read().unwrap();
            if !guard.contains(&(replica_addr, namespace.clone())) {
                return Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
            }
        }

        let logger = match self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
        {
            Ok(logger) => logger,
            Err(e) => {
                return Err(Status::internal(format!(
                    "failed to create database connection: {e}"
                )));
            }
        };

        let frames = StreamGuard::new(
            FrameStream::new(logger.clone(), req.next_offset, false),
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
        self.authenticate(&req)?;
        let namespace = self.extract_namespace(&req)?;

        let replica_addr = req
            .remote_addr()
            .ok_or(Status::internal("No remote RPC address"))?;

        {
            let mut guard = self.replicas_with_hello.write().unwrap();
            guard.insert((replica_addr, namespace.clone()));
        }

        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .unwrap();

        let response = HelloResponse {
            database_id: logger.database_id().unwrap().to_string(),
            generation_start_index: logger.generation.start_index,
            generation_id: logger.generation.id.to_string(),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        self.authenticate(&req)?;
        let namespace = self.extract_namespace(&req)?;

        let (sender, receiver) = mpsc::channel(10);
        let req = req.into_inner();
        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .unwrap();
        let offset = req.next_offset;
        match tokio::task::spawn_blocking(move || logger.get_snapshot_file(offset)).await {
            Ok(Ok(Some(snapshot))) => {
                tokio::task::spawn_blocking(move || {
                    let mut frames = snapshot.frames_iter_from(offset);
                    loop {
                        match frames.next() {
                            Some(Ok(data)) => {
                                let _ = sender.blocking_send(Ok(Frame { data }));
                            }
                            Some(Err(e)) => {
                                let _ = sender.blocking_send(Err(Status::new(
                                    tonic::Code::Internal,
                                    e.to_string(),
                                )));
                                break;
                            }
                            None => {
                                break;
                            }
                        }
                    }
                });

                Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
                    receiver,
                ))))
            }
            Ok(Ok(None)) => Err(Status::new(tonic::Code::Unavailable, "snapshot not found")),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.to_string())),
            Ok(Err(e)) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        }
    }
}
