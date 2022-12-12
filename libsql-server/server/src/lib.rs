use std::net::SocketAddr;
use std::path::PathBuf;

#[cfg(feature = "fdb")]
use std::sync::{Arc, Mutex};

use anyhow::Result;
use database::libsql::LibSqlDb;
use database::service::DbFactoryService;

use crate::postgres::service::PgConnectionFactory;
use crate::server::Server;

mod database;
mod postgres;
mod query;
mod query_analysis;
mod rpc_server;
mod server;
mod wal;

pub mod proxy_rpc {
    #![allow(clippy::all)]
    tonic::include_proto!("proxy");
}

pub async fn run_server(
    db_path: PathBuf,
    tcp_addr: SocketAddr,
    ws_addr: Option<SocketAddr>,
    fdb_config_path: Option<String>,
    writer_rpc_addr: Option<String>,
    rpc_server_addr: Option<SocketAddr>,
) -> Result<()> {
    let mut server = Server::new();
    server.bind_tcp(tcp_addr).await?;

    if let Some(addr) = ws_addr {
        server.bind_ws(addr).await?;
    }

    let vwal_methods = match &fdb_config_path {
        #[cfg(feature = "fdb")]
        Some(_path) => Some(Arc::new(Mutex::new(wal::WalMethods::new(
            fdb_config_path.clone(),
        )?))),
        #[cfg(not(feature = "fdb"))]
        Some(_path) => panic!("not compiled with fdb"),
        None => None,
    };

    let service = DbFactoryService::new(move || {
        let db_path = db_path.clone();
        let vwal_methods = vwal_methods.clone();
        async move { LibSqlDb::new(db_path, vwal_methods) }
    });
    let factory = PgConnectionFactory::new(service);
    server.serve(factory).await;

    Ok(())
}
