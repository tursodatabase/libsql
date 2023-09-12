use anyhow::Context;
use bytes::Bytes;
use tonic::Status;
use tower::util::option_layer;

use crate::config::TlsConfig;
use crate::namespace::{NamespaceStore, PrimaryNamespaceMaker};
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
pub use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLogServer;
use crate::rpc::replication_log::ReplicationLogService;
use crate::utils::services::idle_shutdown::IdleShutdownKicker;
use crate::DEFAULT_NAMESPACE_NAME;

pub mod proxy;
pub mod replica_proxy;
pub mod replication_log;
pub mod replication_log_proxy;

/// A tonic error code to signify that a namespace doesn't exist.
pub const NAMESPACE_DOESNT_EXIST: &str = "NAMESPACE_DOESNT_EXIST";
pub(crate) const NAMESPACE_METADATA_KEY: &str = "x-namespace-bin";

#[allow(clippy::too_many_arguments)]
pub async fn run_rpc_server<A: crate::net::Accept>(
    acceptor: A,
    maybe_tls: Option<TlsConfig>,
    idle_shutdown_layer: Option<IdleShutdownKicker>,
    namespaces: NamespaceStore<PrimaryNamespaceMaker>,
    disable_namespaces: bool,
) -> anyhow::Result<()> {
    let proxy_service = ProxyService::new(namespaces.clone(), None, disable_namespaces);
    let logger_service = ReplicationLogService::new(
        namespaces.clone(),
        idle_shutdown_layer.clone(),
        None,
        disable_namespaces,
    );

    // tracing::info!("serving write proxy server at {addr}");

    let mut builder = tonic::transport::Server::builder();
    if let Some(tls_config) = maybe_tls {
        let cert_pem = std::fs::read_to_string(&tls_config.cert)?;
        let key_pem = std::fs::read_to_string(&tls_config.key)?;
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

        let ca_cert_pem = std::fs::read_to_string(&tls_config.ca_cert)?;
        let ca_cert = tonic::transport::Certificate::from_pem(ca_cert_pem);

        let tls_config = tonic::transport::ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(ca_cert);
        builder = builder
            .tls_config(tls_config)
            .context("Failed to read the TSL config of RPC server")?;
    }
    let router = builder
        .layer(&option_layer(idle_shutdown_layer))
        .add_service(ProxyServer::new(proxy_service))
        .add_service(ReplicationLogServer::new(logger_service))
        .into_router();

    let h2c = crate::h2c::H2cMaker::new(router);
    hyper::server::Server::builder(acceptor)
        .serve(h2c)
        .await
        .context("http server")?;
    Ok(())
}

fn extract_namespace<T>(
    disable_namespaces: bool,
    req: &tonic::Request<T>,
) -> Result<Bytes, Status> {
    if disable_namespaces {
        return Ok(Bytes::from_static(DEFAULT_NAMESPACE_NAME.as_bytes()));
    }

    if let Some(namespace) = req.metadata().get_bin(NAMESPACE_METADATA_KEY) {
        namespace
            .to_bytes()
            .map_err(|_| Status::invalid_argument("Metadata can't be converted into Bytes"))
    } else {
        Err(Status::invalid_argument("Missing x-namespace-bin metadata"))
    }
}
