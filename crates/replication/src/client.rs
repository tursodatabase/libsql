pub mod pb {
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
use tower::{Service, ServiceBuilder};
use tower_http::{classify, trace, ServiceBuilderExt};
use uuid::Uuid;

use crate::{replica::meta::WalIndexMeta, Frame};

use box_clone_service::BoxCloneService;

use self::pb::query_result::RowResult;

type ResponseBody = trace::ResponseBody<
    GrpcWebCall<hyper::Body>,
    classify::GrpcEosErrorsAsFailures,
    trace::DefaultOnBodyChunk,
    trace::DefaultOnEos,
    trace::DefaultOnFailure,
>;

#[derive(Debug, Clone)]
pub struct Client {
    client_id: Uuid,
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

        let client_id = Uuid::new_v4();

        Ok(Self {
            client_id,
            replication,
            proxy,
        })
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

    // TODO(lucio):
    // 1) Implement errors when a row is returned on a non returning query (execute)
    // 2) support row returns aka convert the row result in pb to the Rows struct in libsql
    //          (Should this be a trait object that impls RowInner or should we bake it into
    //          the old one?)
    pub async fn execute(
        &self,
        sql: &str,
        params: pb::query::Params,
    ) -> anyhow::Result<(u64, u64)> {
        let mut proxy = self.proxy.clone();

        let res = proxy
            .execute(pb::ProgramReq {
                client_id: self.client_id.to_string(),
                pgm: Some(pb::Program {
                    steps: vec![pb::Step {
                        query: Some(pb::Query {
                            stmt: sql.to_string(),
                            params: Some(params),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                }),
            })
            .await?
            .into_inner();

        let result = res
            .results
            .iter()
            .next()
            .expect("Expected at least one result");

        let affected_row_count = match &result.row_result {
            Some(RowResult::Row(row)) => row.affected_row_count,
            Some(RowResult::Error(e)) => {
                todo!("handle row result error: {:?}", e)
            }
            None => todo!("handle empty row result"),
        };

        Ok((res.current_frame_no, affected_row_count))
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

// Copied from https://docs.rs/tower/latest/tower/util/struct.BoxCloneService.html
// This is because in the tower version the trait object only implements `Send` which
// means we can't call clients from context that need `Sync` like an `async fn` that needs
// to be `Send` (must be sync as well to impl Send).
mod box_clone_service {
    use std::{
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };
    use tower::Service;
    use tower::ServiceExt;

    type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

    pub struct BoxCloneService<T, U, E>(
        Box<
            dyn CloneService<T, Response = U, Error = E, Future = BoxFuture<Result<U, E>>>
                + Send
                + Sync,
        >,
    );

    impl<T, U, E> BoxCloneService<T, U, E> {
        /// Create a new `BoxCloneService`.
        pub fn new<S>(inner: S) -> Self
        where
            S: Service<T, Response = U, Error = E> + Clone + Sync + Send + 'static,
            S::Future: Send + 'static,
        {
            let inner = inner.map_future(|f| Box::pin(f) as _);
            BoxCloneService(Box::new(inner))
        }
    }

    impl<T, U, E> Service<T> for BoxCloneService<T, U, E> {
        type Response = U;
        type Error = E;
        type Future = BoxFuture<Result<U, E>>;

        #[inline]
        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
            self.0.poll_ready(cx)
        }

        #[inline]
        fn call(&mut self, request: T) -> Self::Future {
            self.0.call(request)
        }
    }

    impl<T, U, E> Clone for BoxCloneService<T, U, E> {
        fn clone(&self) -> Self {
            Self(self.0.clone_box())
        }
    }

    trait CloneService<R>: Service<R> {
        fn clone_box(
            &self,
        ) -> Box<
            dyn CloneService<
                    R,
                    Response = Self::Response,
                    Error = Self::Error,
                    Future = Self::Future,
                > + Send
                + Sync,
        >;
    }

    impl<R, T> CloneService<R> for T
    where
        T: Service<R> + Send + Sync + Clone + 'static,
    {
        fn clone_box(
            &self,
        ) -> Box<
            dyn CloneService<R, Response = T::Response, Error = T::Error, Future = T::Future>
                + Send
                + Sync,
        > {
            Box::new(self.clone())
        }
    }

    impl<T, U, E> fmt::Debug for BoxCloneService<T, U, E> {
        fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
            fmt.debug_struct("BoxCloneService").finish()
        }
    }
}
