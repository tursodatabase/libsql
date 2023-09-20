use hyper::Uri;
use tonic::transport::Channel;

use super::proxy::rpc::{
    self, proxy_client::ProxyClient, proxy_server::Proxy, Ack, DescribeRequest, DescribeResult,
    DisconnectMessage, ExecuteResults,
};

pub struct ReplicaProxyService {
    client: ProxyClient<Channel>,
}

impl ReplicaProxyService {
    pub fn new(channel: Channel, uri: Uri) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self { client }
    }
}

#[tonic::async_trait]
impl Proxy for ReplicaProxyService {
    async fn execute(
        &self,
        req: tonic::Request<rpc::ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        let mut client = self.client.clone();
        client.execute(req).await
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        let mut client = self.client.clone();
        client.disconnect(msg).await
    }

    async fn describe(
        &self,
        req: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResult>, tonic::Status> {
        let mut client = self.client.clone();
        client.describe(req).await
    }
}
