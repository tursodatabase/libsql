use std::net::SocketAddr;

use crate::database::service::DbFactory;
use crate::rpc::proxy::proxy_rpc::proxy_server::ProxyServer;
use crate::rpc::proxy::ProxyService;

pub mod proxy;

pub async fn run_rpc_server<F>(addr: SocketAddr, factory: F) -> anyhow::Result<()>
where
    F: DbFactory + 'static,
    F::Db: Sync + Send + Clone,
{
    let proxy_service = ProxyService::new(factory);

    tracing::info!("serving write proxy server at {addr}");
    tonic::transport::Server::builder()
        .add_service(ProxyServer::new(proxy_service))
        .serve(addr)
        .await?;

    Ok(())
}
