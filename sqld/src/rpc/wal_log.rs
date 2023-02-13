pub mod wal_log_rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::net::SocketAddr;
use std::sync::Arc;

use crate::wal_logger::{FrameId, WalLogger};

use std::collections::HashSet;
use std::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use wal_log_rpc::wal_log_server::WalLog;

use self::wal_log_rpc::{Frame, HelloRequest, HelloResponse, LogOffset};

pub struct WalLogService {
    logger: Arc<WalLogger>,
    replicas_with_hello: RwLock<HashSet<SocketAddr>>,
}

const _NO_HELLO_ERROR_MSG: &str = "NO_HELLO";

impl WalLogService {
    pub fn new(logger: Arc<WalLogger>) -> Self {
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
impl WalLog for WalLogService {
    type LogEntriesStream = ReceiverStream<Result<Frame, Status>>;

    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        // TODO: Uncomment once replicas have the ability to handle NO_HELLO error
        //       Remember to rename _NO_HELLO_ERROR_MSG to NO_HELLO_ERROR_MSG
        //
        // let replica_addr = req
        //     .remote_addr()
        //     .ok_or(Status::internal("No remote RPC address"))?;
        // {
        //     let guard = self.replicas_with_hello.read().unwrap();
        //     if !guard.contains(&replica_addr) {
        //         return Err(Status::failed_precondition(NO_HELLO_ERROR_MSG));
        //     }
        // }
        let start_offset = req.into_inner().start_offset;
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
        Ok(tonic::Response::new(self.logger.hello_response().clone()))
    }
}
