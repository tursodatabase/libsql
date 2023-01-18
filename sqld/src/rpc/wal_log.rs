pub mod wal_log_rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("wal_log");
}

use std::sync::Arc;

use crate::wal_logger::{WalLogEntry, WalLogger};

use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use wal_log_rpc::wal_log_server::WalLog;

use self::wal_log_rpc::{wal_log_entry::Payload, Frame, LogOffset, WalLogEntry as RpcWalLogEntry};

pub struct WalLogService {
    logger: Arc<WalLogger>,
}

impl From<(u64, WalLogEntry)> for RpcWalLogEntry {
    fn from((index, entry): (u64, WalLogEntry)) -> Self {
        let payload = match entry {
            WalLogEntry::Frame { page_no, data } => Payload::Frame(Frame { page_no, data }),
            WalLogEntry::Commit {
                page_size,
                size_after,
                is_commit,
                sync_flags,
            } => Payload::Commit(wal_log_rpc::Commit {
                page_size,
                size_after,
                is_commit,
                sync_flags,
            }),
        };
        Self {
            index,
            payload: Some(payload),
        }
    }
}

impl WalLogService {
    pub fn new(logger: Arc<WalLogger>) -> Self {
        Self { logger }
    }

    fn stream_pages(&self, start_offset: usize) -> ReceiverStream<Result<RpcWalLogEntry, Status>> {
        let logger = self.logger.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        tokio::task::spawn_blocking(move || {
            let mut offset = start_offset;
            loop {
                match logger.get_entry(offset) {
                    Ok(None) => break,
                    Ok(Some(entry)) => {
                        let entry: RpcWalLogEntry = (offset as u64, entry).into();
                        let _ = sender.blocking_send(Ok(entry));
                        offset += 1;
                    }
                    Err(_) => todo!(),
                }
            }
        });

        ReceiverStream::new(receiver)
    }
}

#[tonic::async_trait]
impl WalLog for WalLogService {
    type LogEntriesStream = ReceiverStream<Result<RpcWalLogEntry, Status>>;
    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let start_offset = req.into_inner().start_offset;
        let stream = self.stream_pages(start_offset as _);
        Ok(tonic::Response::new(stream))
    }
}
