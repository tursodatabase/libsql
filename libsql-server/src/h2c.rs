//! Module that provides `h2c` server adapters.

use std::marker::PhantomData;
use std::pin::Pin;

use axum::body::Body;
use bytes::Bytes;
use http::header;
use http::{Request, Response};
use http_body_util::BodyExt;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper::server::conn::http2::Builder as Http2Builder;
use tonic::transport::server::TcpConnectInfo;
use tower::Service;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A `MakeService` adapter for [`H2c`] that injects connection
/// info into the request extensions.
#[derive(Debug)]
pub struct H2cMaker<S> {
    s: S,
}

impl<S> Clone for H2cMaker<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            s: self.s.clone(),
        }
    }
}

impl<S> H2cMaker<S> {
    pub fn new(s: S) -> Self {
        Self { s }
    }
}

impl<S, C> Service<&C> for H2cMaker<S>
where
    S: Service<Request<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Sync + Send + 'static,
    S::Response: Send + 'static,
    C: crate::net::Conn,
{
    type Response = H2c<S>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, conn: &C) -> Self::Future {
        let connect_info = conn.connect_info();
        let s = self.s.clone();
        Box::pin(async move {
            Ok(H2c {
                s,
                connect_info,
            })
        })
    }
}

/// A service that can perform `h2c` upgrades.
#[derive(Debug)]
pub struct H2c<S> {
    s: S,
    connect_info: TcpConnectInfo,
}

impl<S> Clone for H2c<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            s: self.s.clone(),
            connect_info: self.connect_info.clone(),
        }
    }
}

// Service implementation for hyper 1.0's Incoming body type
impl<S, B> Service<Request<hyper::body::Incoming>> for H2c<S>
where
    S: Service<Request<Body>, Response = Response<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Sync + Send + 'static,
    S::Response: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError> + Send + Sync + 'static,
{
    type Response = Response<BoxBody>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<hyper::body::Incoming>) -> Self::Future {
        let mut svc = self.s.clone();
        let connect_info = self.connect_info.clone();

        Box::pin(async move {
            req.extensions_mut().insert(connect_info.clone());

            // Check if this request is a `h2c` upgrade
            if req.headers().get(header::UPGRADE) != Some(&http::HeaderValue::from_static("h2c")) {
                // Convert Incoming body to axum Body
                let (parts, incoming) = req.into_parts();
                let body = Body::from_stream(incoming);
                let req = Request::from_parts(parts, body);
                
                let res = svc.call(req).await.map_err(Into::into)?;
                // Box the body to erase type
                let (parts, body) = res.into_parts();
                return Ok(Response::from_parts(parts, body.boxed()));
            }

            tracing::debug!("Got a h2c upgrade request");

            // Spawn the upgrade handling
            tokio::spawn(async move {
                let upgraded_io = match hyper::upgrade::on(&mut req).await {
                    Ok(io) => TokioIo::new(io),
                    Err(e) => {
                        tracing::error!("Failed to upgrade h2c connection: {}", e);
                        return;
                    }
                };

                tracing::debug!("Successfully upgraded the connection, speaking h2 now");

                let executor = TokioExecutor::new();
                let conn = Http2Builder::new(executor);
                
                // Create a service for HTTP/2
                let svc = hyper::service::service_fn(move |r: Request<hyper::body::Incoming>| {
                    let svc_clone = svc.clone();
                    let connect_info = connect_info.clone();
                    async move {
                        // Convert Request<Incoming> to Request<Body>
                        let (parts, incoming) = r.into_parts();
                        let mut req = Request::from_parts(parts, Body::from_stream(incoming));
                        req.extensions_mut().insert(connect_info);
                        
                        let res = svc_clone.call(req).await.map_err(|e| Box::new(e) as BoxError)?;
                        // Box the body
                        let (parts, body) = res.into_parts();
                        Ok::<_, BoxError>(Response::from_parts(parts, body.boxed()))
                    }
                });

                if let Err(e) = conn.serve_connection(upgraded_io, svc).await {
                    tracing::error!("http2 connection error: {}", e);
                }
            });

            // Return 101 Switching Protocols
            let mut res = Response::new(BoxBody::default());
            *res.status_mut() = http::StatusCode::SWITCHING_PROTOCOLS;
            res.headers_mut()
                .insert(header::UPGRADE, http::HeaderValue::from_static("h2c"));

            Ok(res)
        })
    }
}
