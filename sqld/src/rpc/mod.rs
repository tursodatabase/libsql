use anyhow::Context;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower::util::option_layer;

use crate::database::factory::DbFactory;
use crate::replication::ReplicationLogger;
use crate::rpc::proxy::rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
use crate::rpc::replication_log::rpc::replication_log_server::ReplicationLogServer;
use crate::rpc::replication_log::ReplicationLogService;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;

pub mod proxy;
pub mod replication_log;

#[allow(clippy::too_many_arguments)]
pub async fn run_rpc_server(
    addr: SocketAddr,
    tls: bool,
    cert_path: Option<PathBuf>,
    key_path: Option<PathBuf>,
    ca_cert_path: Option<PathBuf>,
    factory: Arc<dyn DbFactory>,
    logger: Arc<ReplicationLogger>,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
) -> anyhow::Result<()> {
    let proxy_service = ProxyService::new(factory);
    let logger_service = ReplicationLogService::new(logger);

    tracing::info!("serving write proxy server at {addr}");

    let mut builder = tonic::transport::Server::builder();
    if tls {
        let cert_pem = std::fs::read_to_string(cert_path.unwrap())?;
        let key_pem = std::fs::read_to_string(key_path.unwrap())?;
        let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

        let ca_cert_pem = std::fs::read_to_string(ca_cert_path.unwrap())?;
        let ca_cert = tonic::transport::Certificate::from_pem(ca_cert_pem);

        let tls_config = tonic::transport::ServerTlsConfig::new()
            .identity(identity)
            .client_ca_root(ca_cert);
        builder = builder
            .tls_config(tls_config)
            .context("Failed to read the TSL config of RPC server")?;
    }
    builder
        .layer(&option_layer(idle_shutdown_layer))
        .add_service(ProxyServer::new(proxy_service))
        .add_service(ReplicationLogServer::new(logger_service))
        .serve(addr)
        .await?;

    Ok(())
}
