pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::replication::{FrameNo, LogReadError, ReplicationLogger};

use self::rpc::replication_log_server::ReplicationLog;
use self::rpc::{Frame, HelloRequest, HelloResponse, LogOffset};

pub struct ReplicationLogService {
    logger: Arc<ReplicationLogger>,
    replicas_with_hello: RwLock<HashSet<SocketAddr>>,
}

pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";

impl ReplicationLogService {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
        Self {
            logger,
            replicas_with_hello: RwLock::new(HashSet::<SocketAddr>::new()),
        }
    }

    fn stream_pages(&self, start_id: FrameNo) -> ReceiverStream<Result<Frame, Status>> {
        let logger = self.logger.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        tokio::task::spawn_blocking(move || {
            let mut offset = start_id;
            loop {
                // FIXME: add buffering of the log frames.
                let log_file = logger.log_file.read();
                match log_file.frame_bytes(offset) {
                    Err(LogReadError::Ahead) => break,
                    Ok(data) => {
                        // release lock asap
                        drop(log_file);
                        if let Err(e) = sender.blocking_send(Ok(Frame { data })) {
                            tracing::error!("failed to send frame: {e}");
                            break;
                        }
                        offset += 1;
                    }
                    Err(LogReadError::SnapshotRequired) => {
                        let _ = sender.blocking_send(Err(Status::new(
                            tonic::Code::FailedPrecondition,
                            NEED_SNAPSHOT_ERROR_MSG,
                        )));
                        break;
                    }
                    Err(LogReadError::Error(e)) => {
                        if let Err(e) = sender
                            .blocking_send(Err(Status::new(tonic::Code::Internal, e.to_string())))
                        {
                            tracing::error!("failed to send frame: {e}");
                        }
                        break;
                    }
                }
            }
        });

        ReceiverStream::new(receiver)
    }
}

#[tonic::async_trait]
impl ReplicationLog for ReplicationLogService {
    type LogEntriesStream = ReceiverStream<Result<Frame, Status>>;
    type SnapshotStream = ReceiverStream<Result<Frame, Status>>;

    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let replica_addr = req
            .remote_addr()
            .ok_or(Status::internal("No remote RPC address"))?;
        {
            let guard = self.replicas_with_hello.read().unwrap();
            if !guard.contains(&replica_addr) {
                return Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
            }
        }
        // if current_offset is None, then start sending from 0, otherwise return next frame
        let start_offset = req.into_inner().current_offset.map(|x| x + 1).unwrap_or(0);
        let stream = self.stream_pages(start_offset as _);
        Ok(tonic::Response::new(stream))
    }

    async fn hello(
        &self,
        req: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, Status> {
        let replica_addr = req
            .remote_addr()
            .ok_or(Status::internal("No remote RPC address"))?;
        {
            let mut guard = self.replicas_with_hello.write().unwrap();
            guard.insert(replica_addr);
        }
        let response = HelloResponse {
            database_id: self.logger.database_id().unwrap().to_string(),
            generation_start_index: self.logger.generation.start_index,
            generation_id: self.logger.generation.id.to_string(),
        };

        Ok(tonic::Response::new(response))
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let (sender, receiver) = mpsc::channel(10);
        let logger = self.logger.clone();
        let offset = req.into_inner().current_offset();
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

                Ok(tonic::Response::new(ReceiverStream::new(receiver)))
            }
            Ok(Ok(None)) => Err(Status::new(tonic::Code::Unavailable, "snapshot not found")),
            Err(e) => Err(Status::new(tonic::Code::Internal, e.to_string())),
            Ok(Err(e)) => Err(Status::new(tonic::Code::Internal, e.to_string())),
        }
    }
}
