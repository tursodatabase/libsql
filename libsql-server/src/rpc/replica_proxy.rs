use std::sync::Arc;

use hyper::Uri;
use libsql_replication::rpc::proxy::{
    proxy_client::ProxyClient, proxy_server::Proxy, Ack, DescribeRequest, DescribeResult,
    DisconnectMessage, ExecReq, ExecResp, ExecuteResults, ProgramReq,
};
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request, Status};

use crate::auth::Auth;

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
    type StreamExecStream = tonic::codec::Streaming<ExecResp>;

    async fn stream_exec(
        &self,
        req: tonic::Request<tonic::Streaming<ExecReq>>,
    ) -> Result<tonic::Response<Self::StreamExecStream>, tonic::Status> {
        tracing::debug!("stream_exec");

        let (meta, ext, mut stream) = req.into_parts();
        let stream = async_stream::stream! {
            while let Some(it) = stream.next().await {
                match it {
                    Ok(it) => yield it,
                    Err(e) => {
                        // close the stream on error
                        tracing::error!("error proxying stream request: {e}");
                        break
                    },
                }
            }
        };
        let mut req = tonic::Request::from_parts(meta, ext, stream);
        self.do_auth(&mut req)?;
        let mut client = self.client.clone();
        client.stream_exec(req).await
    }

    async fn execute(
        &self,
        mut req: tonic::Request<ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        tracing::debug!("execute");
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
