use std::sync::Arc;

use hyper::Uri;
use tonic::{transport::Channel, Request, Status};

use crate::auth::Auth;

use super::proxy::rpc::{
    self, proxy_client::ProxyClient, proxy_server::Proxy, Ack, DescribeRequest, DescribeResult,
    DisconnectMessage, ExecuteResults,
};

pub struct ReplicaProxyService {
    client: ProxyClient<Channel>,
    auth: Arc<Auth>,
}

impl ReplicaProxyService {
    pub fn new(channel: Channel, uri: Uri, auth: Arc<Auth>) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self { client, auth }
    }

    fn do_auth<T>(&self, req: &mut Request<T>) -> Result<(), Status> {
        let authenticated = self.auth.authenticate_grpc(req, false)?;

        authenticated.upgrade_grpc_request(req);

        Ok(())
    }
}

#[tonic::async_trait]
impl Proxy for ReplicaProxyService {
    async fn execute(
        &self,
        mut req: tonic::Request<rpc::ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        self.do_auth(&mut req)?;

        let mut client = self.client.clone();
        client.execute(req).await
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        mut msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        self.do_auth(&mut msg)?;

        let mut client = self.client.clone();
        client.disconnect(msg).await
    }

    async fn describe(
        &self,
        mut req: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResult>, tonic::Status> {
        self.do_auth(&mut req)?;

        let mut client = self.client.clone();
        client.describe(req).await
    }
}
