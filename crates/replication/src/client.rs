mod pb {
    #![allow(unreachable_pub)]
    #![allow(missing_docs)]
    include!("generated/wal_log.rs");

    include!("generated/proxy.rs");

    pub use proxy_client::ProxyClient;
    pub use replication_log_client::ReplicationLogClient;
}

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use anyhow::Context as _;
use http::Uri;
use hyper_rustls::HttpsConnectorBuilder;
use tonic::{
    body::BoxBody, codegen::InterceptedService, metadata::AsciiMetadataValue, service::Interceptor,
};
use tonic_web::{GrpcWebCall, GrpcWebClientService};
use tower::{util::BoxCloneService, Service, ServiceBuilder};
use tower_http::{classify, trace, ServiceBuilderExt};
use uuid::Uuid;

use crate::{replica::meta::WalIndexMeta, Frame};

type ResponseBody = trace::ResponseBody<
    GrpcWebCall<hyper::Body>,
    classify::GrpcEosErrorsAsFailures,
    trace::DefaultOnBodyChunk,
    trace::DefaultOnEos,
    trace::DefaultOnFailure,
>;

#[derive(Clone)]
pub struct Client {
    replication: pb::ReplicationLogClient<InterceptedService<GrpcChannel, AuthInterceptor>>,
    proxy: pb::ProxyClient<InterceptedService<GrpcChannel, AuthInterceptor>>,
}

impl Client {
    pub fn new(origin: Uri, auth_token: impl AsRef<str>) -> anyhow::Result<Self> {
        let auth_token: AsciiMetadataValue = format!("Bearer {}", auth_token.as_ref())
            .try_into()
            .context("Invalid auth token must be ascii")?;

        let channel = GrpcChannel::new();

        let replication = pb::ReplicationLogClient::with_origin(
            InterceptedService::new(channel.clone(), AuthInterceptor(auth_token.clone())),
            origin.clone(),
        );

        let proxy = pb::ProxyClient::with_origin(
            InterceptedService::new(channel, AuthInterceptor(auth_token)),
            origin,
        );

        // Remove default tonic `8mb` message limits since fly may buffer
        // messages causing the msg len to be longer.
        let replication = replication.max_decoding_message_size(usize::MAX);
        let proxy = proxy.max_decoding_message_size(usize::MAX);

        Ok(Self { replication, proxy })
    }

    pub async fn hello(&self) -> anyhow::Result<WalIndexMeta> {
        let mut replication = self.replication.clone();
        let response = replication
            .hello(pb::HelloRequest::default())
            .await?
            .into_inner();

        let generation_id =
            Uuid::try_parse(&response.generation_id).context("Unable to parse generation id")?;
        let database_id =
            Uuid::try_parse(&response.database_id).context("Unable to parse database id")?;

        // FIXME: not that simple, we need to figure out if we always start from frame 1?
        let meta = WalIndexMeta {
            pre_commit_frame_no: 0,
            post_commit_frame_no: 0,
            generation_id: generation_id.to_u128_le(),
            database_id: database_id.to_u128_le(),
        };

        Ok(meta)
    }

    pub async fn batch_log_entries(&self, next_offset: u64) -> anyhow::Result<Vec<Frame>> {
        let mut client = self.replication.clone();
        let frames = client
            .batch_log_entries(pb::LogOffset { next_offset })
            .await?
            .into_inner();
        let frames = frames
            .frames
            .into_iter()
            .map(|f| Frame::try_from_bytes(f.data))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(frames)
    }

    pub async fn execute(&self, sql: &str) -> anyhow::Result<()> {
        let mut proxy = self.proxy.clone();

        proxy
            .execute(pb::ProgramReq {
                client_id: "embedded-replica".to_string(),
                pgm: Some(pb::Program {
                    steps: vec![pb::Step {
                        query: Some(pb::Query {
                            stmt: sql.to_string(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                }),
                ..Default::default()
            })
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GrpcChannel {
    client: BoxCloneService<http::Request<BoxBody>, http::Response<ResponseBody>, hyper::Error>,
}

impl GrpcChannel {
    pub fn new() -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();

        let client = hyper::Client::builder().build(https);
        let client = GrpcWebClientService::new(client);

        let svc = ServiceBuilder::new().trace_for_grpc().service(client);

        let client = BoxCloneService::new(svc);

        Self { client }
    }
}

impl Service<http::Request<BoxBody>> for GrpcChannel {
    type Response = http::Response<ResponseBody>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<BoxBody>) -> Self::Future {
        let fut = self.client.call(req);
        Box::pin(fut)
    }
}

#[derive(Clone)]
pub struct AuthInterceptor(AsciiMetadataValue);

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut().insert("x-authorization", self.0.clone());
        Ok(req)
    }
}
