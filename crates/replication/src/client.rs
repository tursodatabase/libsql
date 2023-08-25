use std::{
    pin::Pin,
    task::{Context, Poll},
};

use hyper::Client;
use hyper_rustls::HttpsConnectorBuilder;
use tonic::body::BoxBody;
use tonic_web::{GrpcWebCall, GrpcWebClientService};
use tower::{util::BoxCloneService, Service, ServiceBuilder};
use tower_http::{classify, trace, ServiceBuilderExt};

type ResponseBody = trace::ResponseBody<
    GrpcWebCall<hyper::Body>,
    classify::GrpcEosErrorsAsFailures,
    trace::DefaultOnBodyChunk,
    trace::DefaultOnEos,
    trace::DefaultOnFailure,
>;

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

        let client = Client::builder().build(https);
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
