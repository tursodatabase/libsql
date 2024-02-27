use std::sync::Arc;

use hyper_rustls::TlsAcceptor;
use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use rustls::server::AllowAnyAuthenticatedClient;
use rustls::RootCertStore;
use tonic::Status;
use tower::util::option_layer;
use tower::ServiceBuilder;
use tower_http::trace::DefaultOnResponse;
use tracing::Span;

use crate::config::TlsConfig;
use crate::metrics::CLIENT_VERSION;
use crate::namespace::{NamespaceName, NamespaceStore};
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
pub use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLogServer;
use crate::rpc::replication_log::ReplicationLogService;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;

pub mod proxy;
pub mod replica_proxy;
pub mod replication_log;
pub mod replication_log_proxy;
pub mod streaming_exec;

pub async fn run_rpc_server<A: crate::net::Accept>(
    proxy_service: ProxyService,
    acceptor: A,
    maybe_tls: Option<TlsConfig>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    namespaces: NamespaceStore,
    disable_namespaces: bool,
) -> anyhow::Result<()> {
    let logger_service = ReplicationLogService::new(
        namespaces.clone(),
        idle_shutdown_layer.clone(),
        None,
        disable_namespaces,
        false,
    );

    if let Some(tls_config) = maybe_tls {
        let cert_pem = tokio::fs::read_to_string(&tls_config.cert).await?;
        let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())?;
        let certs = certs
            .into_iter()
            .map(rustls::Certificate)
            .collect::<Vec<_>>();

        let key_pem = tokio::fs::read_to_string(&tls_config.key).await?;
        let keys = rustls_pemfile::pkcs8_private_keys(&mut key_pem.as_bytes())?;
        let key = rustls::PrivateKey(keys[0].clone());

        let ca_cert_pem = std::fs::read_to_string(&tls_config.ca_cert)?;
        let ca_certs = rustls_pemfile::certs(&mut ca_cert_pem.as_bytes())?;
        let ca_certs = ca_certs
            .into_iter()
            .map(rustls::Certificate)
            .collect::<Vec<_>>();

        let mut roots = RootCertStore::empty();
        ca_certs.iter().try_for_each(|c| roots.add(c))?;
        let verifier = AllowAnyAuthenticatedClient::new(roots);
        let config = rustls::server::ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(Arc::new(verifier))
            .with_single_cert(certs, key)?;

        let acceptor = TlsAcceptor::builder()
            .with_tls_config(config)
            .with_all_versions_alpn()
            .with_acceptor(acceptor);

        let router = tonic::transport::Server::builder()
            .layer(&option_layer(idle_shutdown_layer))
            .add_service(ProxyServer::new(proxy_service))
            .add_service(ReplicationLogServer::new(logger_service))
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

        tracing::info!("serving internal rpc server with tls");
        let h2c = crate::h2c::H2cMaker::new(svc);
        hyper::server::Server::builder(acceptor).serve(h2c).await?;
    } else {
        let proxy = ProxyServer::new(proxy_service);
        let replication = ReplicationLogServer::new(logger_service);

        let router = tonic::transport::Server::builder()
            .layer(&option_layer(idle_shutdown_layer))
            .add_service(proxy)
            .add_service(replication)
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

        let h2c = crate::h2c::H2cMaker::new(svc);

        tracing::info!("serving internal rpc server without tls");

        hyper::server::Server::builder(acceptor).serve(h2c).await?;
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
