use hyper::Uri;
use tonic::{transport::Channel, Status};

use super::replication_log::rpc::replication_log_client::ReplicationLogClient;
use super::replication_log::rpc::replication_log_server::ReplicationLog;
use super::replication_log::rpc::{Frame, Frames, HelloRequest, HelloResponse, LogOffset};

/// A replication log service that proxies request to the primary.
pub struct ReplicationLogProxyService {
    client: ReplicationLogClient<Channel>,
}

impl ReplicationLogProxyService {
    pub fn new(channel: Channel, uri: Uri) -> Self {
        let client = ReplicationLogClient::with_origin(channel, uri);
        Self { client }
    }
}

#[tonic::async_trait]
impl ReplicationLog for ReplicationLogProxyService {
    type LogEntriesStream = tonic::codec::Streaming<Frame>;
    type SnapshotStream = tonic::codec::Streaming<Frame>;

    async fn log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::LogEntriesStream>, Status> {
        let mut client = self.client.clone();
        client.log_entries(req).await
    }

    async fn batch_log_entries(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Frames>, Status> {
        let mut client = self.client.clone();
        client.batch_log_entries(req).await
    }

    async fn hello(
        &self,
        req: tonic::Request<HelloRequest>,
    ) -> Result<tonic::Response<HelloResponse>, Status> {
        let mut client = self.client.clone();
        client.hello(req).await
    }

    async fn snapshot(
        &self,
        req: tonic::Request<LogOffset>,
    ) -> Result<tonic::Response<Self::SnapshotStream>, Status> {
        let mut client = self.client.clone();
        client.snapshot(req).await
    }
}
