use std::sync::Arc;

use hyper::Uri;
use libsql_replication::rpc::proxy::{
    proxy_client::ProxyClient, proxy_server::Proxy, Ack, DescribeRequest, DescribeResult,
    DisconnectMessage, ExecReq, ExecResp, ExecuteResults, ProgramReq,
};
use tokio_stream::StreamExt;
use tonic::{transport::Channel, Request, Status};

use crate::{
    auth::Auth,
    namespace::{NamespaceStore, ReplicaNamespaceMaker},
};

pub struct ReplicaProxyService {
    client: ProxyClient<Channel>,
    auth: Arc<Auth>,
    disable_namespaces: bool,
    namespaces: NamespaceStore<ReplicaNamespaceMaker>,
}

impl ReplicaProxyService {
    pub fn new(
        channel: Channel,
        uri: Uri,
        namespaces: NamespaceStore<ReplicaNamespaceMaker>,
        auth: Arc<Auth>,
        disable_namespaces: bool,
    ) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self {
            client,
            auth,
            disable_namespaces,
            namespaces,
        }
    }

    async fn do_auth<T>(&self, req: &mut Request<T>) -> Result<(), Status> {
        let namespace = super::extract_namespace(self.disable_namespaces, req)?;
        let namespace_jwt_key = self.namespaces.with(namespace, |ns| ns.jwt_key()).await;
        match namespace_jwt_key {
            Ok(Ok(jwt_key)) => {
                let authenticated = self.auth.authenticate_grpc(req, false, jwt_key)?;
                authenticated.upgrade_grpc_request(req);
                Ok(())
            }
            Err(e) => match e.as_ref() {
                crate::error::Error::NamespaceDoesntExist(_) => {
                    let authenticated = self.auth.authenticate_grpc(req, false, None)?;
                    authenticated.upgrade_grpc_request(req);
                    Ok(())
                }
                _ => Err(Status::internal(format!(
                    "Error fetching jwt key for a namespace: {}",
                    e
                ))),
            },
            Ok(Err(e)) => Err(Status::internal(format!(
                "Error fetching jwt key for a namespace: {}",
                e
            ))),
        }
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
        self.do_auth(&mut req).await?;
        let mut client = self.client.clone();
        client.stream_exec(req).await
    }

    async fn execute(
        &self,
        mut req: tonic::Request<ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        tracing::debug!("execute");
        self.do_auth(&mut req).await?;

        let mut client = self.client.clone();
        client.execute(req).await
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        mut msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        self.do_auth(&mut msg).await?;

        let mut client = self.client.clone();
        client.disconnect(msg).await
    }

    async fn describe(
        &self,
        mut req: tonic::Request<DescribeRequest>,
    ) -> Result<tonic::Response<DescribeResult>, tonic::Status> {
        self.do_auth(&mut req).await?;

        let mut client = self.client.clone();
        client.describe(req).await
    }
}
