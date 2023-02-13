pub mod rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::replication::logger::{FrameId, ReplicationLogger};

use self::rpc::replication_log_server::ReplicationLog;
use self::rpc::{Frame, HelloRequest, HelloResponse, LogOffset};

pub struct ReplicationLogService {
    logger: Arc<ReplicationLogger>,
    replicas_with_hello: RwLock<HashSet<SocketAddr>>,
}

pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";

impl ReplicationLogService {
    pub fn new(logger: Arc<ReplicationLogger>) -> Self {
        Self {
            logger,
            replicas_with_hello: RwLock::new(HashSet::<SocketAddr>::new()),
        }
    }

    fn stream_pages(&self, start_id: FrameId) -> ReceiverStream<Result<Frame, Status>> {
        let logger = self.logger.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        tokio::task::spawn_blocking(move || {
            let mut offset = start_id;
            loop {
                match logger.frame_bytes(offset) {
                    Ok(None) => break,
                    Ok(Some(data)) => {
                        if let Err(e) = sender.blocking_send(Ok(Frame { data })) {
                            tracing::error!("failed to send frame: {e}");
                            break;
                        }
                        offset += 1;
                    }
                    Err(e) => todo!("{e}"),
                }
            }
        });

        ReceiverStream::new(receiver)
    }
}

#[tonic::async_trait]
impl ReplicationLog for ReplicationLogService {
    type LogEntriesStream = ReceiverStream<Result<Frame, Status>>;

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
            database_id: self.logger.database_id.to_string(),
            generation_start_index: self.logger.generation.start_index,
            generation_id: self.logger.generation.id.to_string(),
        };

        Ok(tonic::Response::new(response))
    }
}
