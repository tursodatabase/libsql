use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::database::service::DbFactory;
use crate::rpc::proxy::proxy_rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;
use crate::rpc::wal_log::wal_log_rpc::wal_log_server::WalLogServer;
use crate::rpc::wal_log::WalLogService;
use crate::wal_logger::WalLogger;

pub mod proxy;
pub mod wal_log;

pub async fn run_rpc_server<F>(
    addr: SocketAddr,
    tls: bool,
    cert: Option<PathBuf>,
    key: Option<PathBuf>,
    factory: F,
    logger: Arc<WalLogger>,
) -> anyhow::Result<()>
where
    F: DbFactory + 'static,
    F::Db: Sync + Send + Clone,
    F::Future: Sync,
{
    let proxy_service = ProxyService::new(factory);
    let logger_service = WalLogService::new(logger);

    tracing::info!("serving write proxy server at {addr}");

    let mut builder = tonic::transport::Server::builder();
    if tls {
        let cert = std::fs::read_to_string(cert.unwrap())?;
        let key = std::fs::read_to_string(key.unwrap())?;
        let server_identity = tonic::transport::Identity::from_pem(&cert, &key);
        builder = builder
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(server_identity))?;
    }
    builder
        .add_service(ProxyServer::new(proxy_service))
        .add_service(WalLogServer::new(logger_service))
        .serve(addr)
        .await?;

    Ok(())
}
