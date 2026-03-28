use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLogServer;
use libsql_replication::rpc::replication::{BoxReplicationService, NAMESPACE_METADATA_KEY};
use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;
use tokio_rustls::TlsAcceptor;
use tonic::Status;
use tower::util::option_layer;
use tower::Service;
use tower::ServiceBuilder;
use tower_http::trace::DefaultOnResponse;
use tracing::Span;

use crate::config::TlsConfig;
use crate::metrics::CLIENT_VERSION;
use crate::namespace::NamespaceName;
use crate::net::{Accept, Conn};
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

pub mod proxy;
pub mod replica_proxy;
pub mod replication;
pub mod streaming_exec;

pub async fn run_rpc_server<A: Accept>(
    proxy_service: ProxyService,
    mut acceptor: A,
    maybe_tls: Option<TlsConfig>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    service: BoxReplicationService,
) -> anyhow::Result<()> {
    let router = tonic::transport::Server::builder()
        .layer(&option_layer(idle_shutdown_layer))
        .add_service(ProxyServer::new(proxy_service))
        .add_service(ReplicationLogServer::new(service))
        .into_router();

    let svc = ServiceBuilder::new()
        .layer(
            tower_http::trace::TraceLayer::new_for_grpc()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        )
        .service(router);

    if let Some(tls_config) = maybe_tls {
        run_tls_server(&mut acceptor, svc, tls_config).await
    } else {
        run_plain_server(&mut acceptor, svc).await
    }
}

/// Wrapper service that converts hyper 1.0's Incoming body to tonic's BoxBody
#[derive(Clone)]
struct TonicServiceWrapper<S> {
    inner: S,
}

impl<S, B> Service<hyper::Request<hyper::body::Incoming>> for TonicServiceWrapper<S>
where
    S: Service<hyper::Request<tonic::body::BoxBody>, Response = hyper::Response<B>, Error = std::convert::Infallible> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    type Response = hyper::Response<B>;
    type Error = std::convert::Infallible;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: hyper::Request<hyper::body::Incoming>) -> Self::Future {
        // Convert Incoming body to tonic's BoxBody
        // Need to map the error type from io::Error to tonic::Status
        let (parts, body) = req.into_parts();
        let body = body.map_err(|e| tonic::Status::internal(format!("body error: {}", e)));
        let body = tonic::body::BoxBody::new(body);
        let req = hyper::Request::from_parts(parts, body);
        self.inner.call(req)
    }
}

async fn run_tls_server<A, S, B>(
    acceptor: &mut A,
    svc: S,
    tls_config: TlsConfig,
) -> anyhow::Result<()>
where
    A: Accept,
    S: tower::Service<hyper::Request<tonic::body::BoxBody>, Response = hyper::Response<B>, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    let cert_pem = tokio::fs::read_to_string(&tls_config.cert).await?;
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()?;

    let key_pem = tokio::fs::read_to_string(&tls_config.key).await?;
    let keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut key_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()?;
    let key = rustls::pki_types::PrivateKeyDer::try_from(keys.into_iter().next().ok_or_else(|| anyhow::anyhow!("no private keys found"))?)?

    let ca_cert_pem = std::fs::read_to_string(&tls_config.ca_cert)?;
    let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()?;

    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(ca_certs);
    let verifier = rustls::server::WebPkiClientVerifier::builder(roots.into())
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build client verifier: {}", e))?;
    let mut config = rustls::server::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)?;

    // Configure ALPN protocols for HTTP/2 and HTTP/1.1
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let tls_acceptor = TlsAcceptor::from(Arc::new(config));

    tracing::info!("serving internal rpc server with tls");
    
    let wrapped_svc = TonicServiceWrapper { inner: svc };

    // Drive the acceptor stream manually for hyper 1.0+ compatibility
    loop {
        let conn = match poll_fn(|cx| Pin::new(&mut *acceptor).poll_accept(cx)).await {
            Some(Ok(conn)) => conn,
            Some(Err(e)) => {
                tracing::error!("Accept error: {}", e);
                continue;
            }
            None => break,
        };

        let tls_acceptor = tls_acceptor.clone();
        let svc = wrapped_svc.clone();

        tokio::spawn(async move {
            let tls_stream = match tls_acceptor.accept(conn).await {
                Ok(tls_stream) => tls_stream,
                Err(err) => {
                    tracing::error!("failed to perform tls handshake: {:#}", err);
                    return;
                }
            };

            let io = TokioIo::new(tls_stream);

            if let Err(err) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                tracing::error!("failed to serve connection: {:#}", err);
            }
        });
    }

    Ok(())
}

async fn run_plain_server<A, S, B>(
    acceptor: &mut A,
    svc: S,
) -> anyhow::Result<()>
where
    A: Accept,
    S: tower::Service<hyper::Request<tonic::body::BoxBody>, Response = hyper::Response<B>, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    B: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
{
    tracing::info!("serving internal rpc server without tls");
    let wrapped_svc = TonicServiceWrapper { inner: svc };

    // Drive the acceptor stream manually for hyper 1.0+ compatibility
    loop {
        let conn = match poll_fn(|cx| Pin::new(&mut *acceptor).poll_accept(cx)).await {
            Some(Ok(conn)) => conn,
            Some(Err(e)) => {
                tracing::error!("Accept error: {}", e);
                continue;
            }
            None => break,
        };

        let svc = wrapped_svc.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(conn);

            if let Err(err) = ConnBuilder::new(TokioExecutor::new())
                .serve_connection(io, svc)
                .await
            {
                tracing::error!("failed to serve connection: {:#}", err);
            }
        });
    }

    Ok(())
}

fn extract_namespace<T>(
    disable_namespaces: bool,
    req: &tonic::Request<T>,
) -> Result<NamespaceName, Status> {
    if disable_namespaces {
        return Ok(NamespaceName::default());
    }

    if let Some(namespace) = req.metadata().get_bin(NAMESPACE_METADATA_KEY) {
        let bytes = namespace
            .to_bytes()
            .map_err(|_| Status::invalid_argument("Metadata can't be converted into Bytes"))?;
        NamespaceName::from_bytes(bytes)
            .map_err(|_| Status::invalid_argument("Invalid namespace name"))
    } else {
        Err(Status::invalid_argument("Missing x-namespace-bin metadata"))
    }
}

fn trace_request<B>(req: &hyper::Request<B>, span: &Span) {
    let _s = span.enter();

    tracing::debug!(
        "rpc request: {} {} {:?}",
        req.method(),
        req.uri(),
        req.headers()
    );

    if let Some(v) = req.headers().get("x-libsql-client-version") {
        if let Ok(s) = v.to_str() {
            metrics::increment_counter!(CLIENT_VERSION, "version" => s.to_string());
        }
    }
}
