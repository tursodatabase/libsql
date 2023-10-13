//! Module that provides `h2c` server adapters.
//!
//! # What is `h2c`?
//!
//! `h2c` is a http1.1 upgrade token that allows us to accept http2 without
//! going through tls/alpn while also accepting regular http1.1 requests. Since,
//! our server does not do TLS there is no way to negotiate that an incoming
//! connection is going to speak http2 or http1.1 so we must default to http1.1.
//!
//! # How does it work?
//!
//! The `H2c` service gets called on every http request that arrives to the
//! server and checks if the request has an `upgrade` header set. If this
//! header is set to `h2c` then it will start the upgrade process. If this
//! header is not set the request continues normally without any upgrades.
//!
//! The upgrade process is quite simple, if the correct header value is set
//! the server will spawn a background task, return status code `101`
//! (switching protocols) and will set the same upgrade header with `h2c` as
//! the value.
//!
//! The background task will wait for `hyper::upgrade::on` to complete. At this
//! point when `on` completes it returns an `IO` object that we can read/write from.
//! We then pass this into hyper's low level server connection type and force http2.
//! This means from the point that the client gets back the upgrade headers and correct
//! status code the connection will be immediealty speaking http2 and thus the upgrade
//! is complete.
//!
//! ┌───────────────┐      upgrade:h2c        ┌──────────────────┐
//! │ http::request ├────────────────────────►│ upgrade to http2 │
//! └─────┬─────────┘                         └────────┬─────────┘
//!       │                                            │
//!       │                                            │
//!       │                                            │
//!       │                                            │
//!       │                                            │
//!       │             ┌─────────────────┐            │
//!       └────────────►│call axum router │◄───────────┘
//!                     └─────────────────┘

use std::pin::Pin;

use axum::{body::BoxBody, http::HeaderValue};
use hyper::header;
use hyper::Body;
use hyper::{Request, Response};
use tonic::transport::server::TcpConnectInfo;
use tower::Service;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A `MakeService` adapter for [`H2c`] that injects connection
/// info into the request extensions.
#[derive(Debug, Clone)]
pub struct H2cMaker<S> {
    s: S,
}

impl<S> H2cMaker<S> {
    pub fn new(s: S) -> Self {
        Self { s }
    }
}

impl<S, C> Service<&C> for H2cMaker<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Sync + Send + 'static,
    S::Response: Send + 'static,
    C: crate::net::Conn,
{
    type Response = H2c<S>;

    type Error = hyper::Error;

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
        Box::pin(async move { Ok(H2c { s, connect_info }) })
    }
}

/// A service that can perform `h2c` upgrades and will
/// delegate calls to the inner service once a protocol
/// has been selected.
#[derive(Debug, Clone)]
pub struct H2c<S> {
    s: S,
    connect_info: TcpConnectInfo,
}

impl<S> Service<Request<Body>> for H2c<S>
where
    S: Service<Request<Body>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Sync + Send + 'static,
    S::Response: Send + 'static,
{
    type Response = hyper::Response<BoxBody>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: hyper::Request<Body>) -> Self::Future {
        let mut svc = self.s.clone();
        let connect_info = self.connect_info.clone();

        Box::pin(async move {
            req.extensions_mut().insert(connect_info.clone());

            // Check if this request is a `h2c` upgrade, if it is not pass
            // the request to the inner service, which in our case is the
            // axum router.
            if req.headers().get(header::UPGRADE) != Some(&HeaderValue::from_static("h2c")) {
                return svc.call(req).await.map_err(Into::into);
            }

            tracing::debug!("Got a h2c upgrade request");

            // We got a h2c header so lets spawn a task that will wait for the
            // upgrade to complete and start a http2 connection.
            tokio::spawn(async move {
                let upgraded_io = match hyper::upgrade::on(&mut req).await {
                    Ok(io) => io,
                    Err(e) => {
                        tracing::error!("Failed to upgrade h2c connection: {}", e);
                        return;
                    }
                };

                tracing::debug!("Successfully upgraded the connection, speaking h2 now");

                if let Err(e) = hyper::server::conn::Http::new()
                    .http2_only(true)
                    .serve_connection(
                        upgraded_io,
                        tower::service_fn(move |mut r: hyper::Request<hyper::Body>| {
                            r.extensions_mut().insert(connect_info.clone());
                            svc.call(r)
                        }),
                    )
                    .await
                {
                    tracing::error!("http2 connection error: {}", e);
                }
            });

            // Reply that we are switching protocols to h2
            let body = axum::body::boxed(axum::body::Empty::new());
            let mut res = hyper::Response::new(body);
            *res.status_mut() = hyper::StatusCode::SWITCHING_PROTOCOLS;
            res.headers_mut()
                .insert(header::UPGRADE, HeaderValue::from_static("h2c"));

            Ok(res)
        })
    }
}
