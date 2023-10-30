use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
pub use libsql_replication::rpc::replication as rpc;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLog;
use libsql_replication::rpc::replication::{
    Frame, Frames, HelloRequest, HelloResponse, LogOffset, NEED_SNAPSHOT_ERROR_MSG,
    NO_HELLO_ERROR_MSG, SESSION_TOKEN_KEY,
};
use tokio_stream::StreamExt;
use tonic::Status;
use uuid::Uuid;

use crate::auth::Auth;
use crate::namespace::{NamespaceStore, PrimaryNamespaceMaker};
use crate::replication::primary::frame_stream::FrameStream;
use crate::replication::LogReadError;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

use super::NAMESPACE_DOESNT_EXIST;

pub struct ReplicationLogService {
    namespaces: NamespaceStore<PrimaryNamespaceMaker>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    auth: Option<Arc<Auth>>,
    disable_namespaces: bool,
    session_token: Bytes,
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
        }
    }

    fn authenticate<T>(&self, req: &tonic::Request<T>) -> Result<(), Status> {
        if let Some(auth) = &self.auth {
            let _ = auth.authenticate_grpc(req, self.disable_namespaces)?;
        }

        Ok(())
    }

    fn verify_session_token<R>(&self, req: &tonic::Request<R>) -> Result<(), Status> {
        let no_hello = || Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
        let Some(token) = req.metadata().get(SESSION_TOKEN_KEY) else {
            return no_hello();
        };
        if token.as_bytes() != self.session_token {
            return no_hello();
        }

        Ok(())
    }
}

fn map_frame_stream_output(
    r: Result<libsql_replication::frame::Frame, LogReadError>,
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
        self.verify_session_token(&req)?;

        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

        let req = req.into_inner();

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
        self.verify_session_token(&req)?;
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;

        let req = req.into_inner();
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
            log_id: logger.log_id().to_string(),
            session_token: self.session_token.clone(),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        self.authenticate(&req)?;
        let namespace = super::extract_namespace(self.disable_namespaces, &req)?;
        let req = req.into_inner();
        let logger = self
            .namespaces
            .with(namespace, |ns| ns.db.logger.clone())
            .await
            .unwrap();
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
