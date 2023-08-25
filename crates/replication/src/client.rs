use std::{
    pin::Pin,
    task::{Context, Poll},
};

use hyper::{client::HttpConnector, Client};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use tonic::body::BoxBody;
use tonic_web::{GrpcWebCall, GrpcWebClientService};
use tower::Service;

#[derive(Debug, Clone)]
pub struct H2cChannel {
    client: GrpcWebClientService<Client<HttpsConnector<HttpConnector>, GrpcWebCall<BoxBody>>>,
}

impl H2cChannel {
    #[allow(unused)]
    pub fn new() -> Self {
        let https = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .build();

        let client = Client::builder().build(https);
        let client = GrpcWebClientService::new(client);

        Self { client }
    }
}

impl Service<http::Request<BoxBody>> for H2cChannel {
    type Response = http::Response<GrpcWebCall<hyper::Body>>;
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
