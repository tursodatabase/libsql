use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use futures::Stream;
use libsql_replication::rpc::replication::replication_log_server::ReplicationLogServer;
use libsql_replication::rpc::replication::{BoxReplicationService, NAMESPACE_METADATA_KEY};
use rustls::pki_types::CertificateDer;
use rustls::RootCertStore;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::TlsAcceptor;
use tonic::transport::server::Connected;
use tonic::Status;
use tower::util::option_layer;
use tower_http::trace::DefaultOnResponse;
use tracing::Span;

use crate::config::TlsConfig;
use crate::metrics::CLIENT_VERSION;
use crate::namespace::NamespaceName;
use crate::net::Accept;
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

pub mod proxy;
pub mod replica_proxy;
pub mod replication;
pub mod streaming_exec;

pub async fn run_rpc_server<A: Accept>(
    proxy_service: ProxyService,
    acceptor: A,
    maybe_tls: Option<TlsConfig>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    service: BoxReplicationService,
) -> anyhow::Result<()> {
    // Build the tonic server with services
    let idle_layer = option_layer(idle_shutdown_layer);
    let mut server = tonic::transport::Server::builder()
        .layer(&idle_layer)
        .layer(
            tower_http::trace::TraceLayer::new_for_grpc()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        );

    let router = server
        .add_service(ProxyServer::new(proxy_service))
        .add_service(ReplicationLogServer::new(service));

    if let Some(tls_config) = maybe_tls {
        // TLS case
        let cert_pem = tokio::fs::read_to_string(&tls_config.cert).await?;
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()?;

        let key_pem = tokio::fs::read_to_string(&tls_config.key).await?;
        let keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut key_pem.as_bytes())
            .collect::<Result<Vec<_>, _>>()?;
        let key = rustls::pki_types::PrivateKeyDer::try_from(keys.into_iter().next().ok_or_else(|| anyhow::anyhow!("no private keys found"))?)?;

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

        // Create a stream of TLS connections from the acceptor
        let incoming = tls_incoming_stream(acceptor, tls_acceptor);

        // Serve with tonic's native server
        router.serve_with_incoming(incoming).await?;
    } else {
        tracing::info!("serving internal rpc server without tls");

        // Create a stream of connections from the acceptor
        let incoming = plain_incoming_stream(acceptor);

        // Serve with tonic's native server
        router.serve_with_incoming(incoming).await?;
    }

    Ok(())
}

fn tls_incoming_stream<A>(
    mut acceptor: A,
    tls_acceptor: TlsAcceptor,
) -> impl Stream<Item = Result<TlsStream<A::Connection>, anyhow::Error>>
where
    A: Accept,
{
    try_stream! {
        loop {
            let conn = match poll_fn(|cx| Pin::new(&mut acceptor).poll_accept(cx)).await {
                Some(Ok(conn)) => conn,
                Some(Err(e)) => {
                    tracing::error!("Accept error: {}", e);
                    continue;
                }
                None => break,
            };

            let tls_stream = match tls_acceptor.accept(conn).await {
                Ok(tls_stream) => tls_stream,
                Err(err) => {
                    tracing::error!("failed to perform tls handshake: {:#}", err);
                    continue;
                }
            };

            yield TlsStream(tls_stream);
        }
    }
}

fn plain_incoming_stream<A>(
    mut acceptor: A,
) -> impl Stream<Item = Result<A::Connection, anyhow::Error>>
where
    A: Accept,
{
    try_stream! {
        loop {
            let conn = match poll_fn(|cx| Pin::new(&mut acceptor).poll_accept(cx)).await {
                Some(Ok(conn)) => conn,
                Some(Err(e)) => {
                    tracing::error!("Accept error: {}", e);
                    continue;
                }
                None => break,
            };

            yield conn;
        }
    }
}

// Wrapper for TLS stream to implement Connected
pub struct TlsStream<S>(tokio_rustls::server::TlsStream<S>);

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl<S: Connected> Connected for TlsStream<S> {
    type ConnectInfo = S::ConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.0.get_ref().0.connect_info()
    }
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

fn trace_request<B>(req: &http::Request<B>, span: &Span) {
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
