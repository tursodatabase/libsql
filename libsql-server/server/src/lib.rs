use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use database::service::DbFactoryService;
use database::sqlite::SQLiteDb;

use crate::postgres::service::PgConnectionFactory;
use crate::server::Server;

mod database;
mod postgres;
mod query;
mod query_analysis;
mod server;
mod wal;

pub async fn run_server(
    db_path: PathBuf,
    tcp_addr: SocketAddr,
    ws_addr: Option<SocketAddr>,
) -> Result<()> {
    let mut server = Server::new();
    server.bind_tcp(tcp_addr).await?;

    if let Some(addr) = ws_addr {
        server.bind_ws(addr).await?;
    }

    let service = DbFactoryService::new(move || {
        let db_path = db_path.clone();
        async move { SQLiteDb::new(db_path) }
    });
    let factory = PgConnectionFactory::new(service);
    server.serve(factory).await;

    Ok(())
}
