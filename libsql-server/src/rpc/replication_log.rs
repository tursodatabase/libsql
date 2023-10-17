pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::collections::HashSet;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use futures::stream::BoxStream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::Status;

use crate::auth::Auth;
use crate::namespace::{NamespaceName, NamespaceStore, PrimaryNamespaceMaker};
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::LogReadError;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;
use crate::BLOCKING_RT;

use self::rpc::replication_log_server::ReplicationLog;
use self::rpc::{Frame, Frames, HelloRequest, HelloResponse, LogOffset};

use super::NAMESPACE_DOESNT_EXIST;

pub struct ReplicationLogService {
    namespaces: NamespaceStore<PrimaryNamespaceMaker>,
    replicas_with_hello: RwLock<HashSet<(SocketAddr, NamespaceName)>>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    auth: Option<Arc<Auth>>,
    disable_namespaces: bool,
}

pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";

pub const MAX_FRAMES_PER_BATCH: usize = 1024;

impl ReplicationLogService {
    pub fn new(
        namespaces: NamespaceStore<PrimaryNamespaceMaker>,
        idle_shutdown_layer: Option<IdleShutdownKicker>,
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
            let _ = auth.authenticate_grpc(req, self.disable_namespaces)?;
        }

        Ok(())
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
        self.authenticate(&req)?;
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

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

        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })?;

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
        self.authenticate(&req)?;
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

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

        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })?;

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
        self.authenticate(&req)?;
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

        use tonic::transport::server::TcpConnectInfo;

        req.extensions().get::<TcpConnectInfo>().unwrap();
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
            .map_err(|e| {
                if let crate::error::Error::NamespaceDoesntExist(_) = e {
                    Status::failed_precondition(NAMESPACE_DOESNT_EXIST)
                } else {
                    Status::internal(e.to_string())
                }
            })?;

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
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

        let (sender, receiver) = mpsc::channel(10);
        let req = req.into_inner();
        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .unwrap();
        let offset = req.next_offset;
        match BLOCKING_RT
            .spawn_blocking(move || logger.get_snapshot_file(offset))
            .await
        {
            Ok(Ok(Some(snapshot))) => {
                BLOCKING_RT.spawn_blocking(move || {
                    let mut frames = snapshot.frames_iter_from(offset);
                    loop {
                        match frames.next() {
                            Some(Ok(frame)) => {
                                let _ = sender.blocking_send(Ok(Frame {
                                    data: frame.bytes(),
                                }));
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
