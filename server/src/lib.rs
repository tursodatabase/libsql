use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "mwal_backend")]
use std::sync::Mutex;

use anyhow::Result;
use database::libsql::LibSqlDb;
use database::service::DbFactoryService;
use database::write_proxy::WriteProxyDbFactory;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use query::{Queries, QueryResult};
use rpc::run_rpc_server;
use tower::load::Constant;
use tower::{Service, ServiceExt};
use wal_logger::{WalLogger, WalLoggerHook};

use crate::postgres::service::PgConnectionFactory;
use crate::server::Server;

mod database;
mod http;
mod libsql;
mod postgres;
mod query;
mod query_analysis;
pub mod rpc;
mod server;
mod wal_logger;

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum Backend {
    Libsql,
    #[cfg(feature = "mwal_backend")]
    Mwal,
}

pub struct Config {
    pub db_path: PathBuf,
    pub tcp_addr: SocketAddr,
    pub ws_addr: Option<SocketAddr>,
    pub http_addr: Option<SocketAddr>,
    pub backend: Backend,
    #[cfg(feature = "mwal_backend")]
    pub mwal_addr: Option<String>,
    pub writer_rpc_addr: Option<String>,
    pub rpc_server_addr: Option<SocketAddr>,
}

async fn run_service<S>(service: S, config: Config) -> Result<()>
where
    S: Service<(), Error = anyhow::Error> + Sync + Send + 'static + Clone,
    S::Response: Service<Queries, Response = Vec<QueryResult>, Error = anyhow::Error> + Sync + Send,
    S::Future: Send + Sync,
    <S::Response as Service<Queries>>::Future: Send,
{
    let mut server = Server::new();
    server.bind_tcp(config.tcp_addr).await?;

    if let Some(addr) = config.ws_addr {
        server.bind_ws(addr).await?;
    }

    let mut handles = FuturesUnordered::new();
    let factory = PgConnectionFactory::new(service.clone());
    handles.push(tokio::spawn(server.serve(factory)));

    if let Some(addr) = config.http_addr {
        let handle = tokio::spawn(http::run_http(
            addr,
            service.map_response(|s| Constant::new(s, 1)),
        ));

        handles.push(handle);
    }

    while let Some(res) = handles.next().await {
        res??;
    }

    Ok(())
}

pub async fn run_server(config: Config) -> Result<()> {
    tracing::trace!("Backend: {:?}", config.backend);
    #[cfg(feature = "mwal_backend")]
    if config.backend == Backend::Mwal {
        std::env::set_var("MVSQLITE_DATA_PLANE", config.mwal_addr.as_ref().unwrap());
    }

    #[cfg(feature = "mwal_backend")]
    let vwal_methods = config
        .mwal_addr
        .as_ref()
        .map(|_| Arc::new(Mutex::new(mwal::ffi::libsql_wal_methods::new())));

    match config.writer_rpc_addr {
        Some(ref addr) => {
            let factory = WriteProxyDbFactory::new(
                addr,
                config.db_path.clone(),
                #[cfg(feature = "mwal_backend")]
                vwal_methods,
            )
            .await?;
            let service = DbFactoryService::new(factory);
            run_service(service, config).await?;
        }
        None => {
            let logger = Arc::new(WalLogger::open("wallog")?);
            let logger_clone = logger.clone();
            let path_clone = config.db_path.clone();
            let db_factory = move || {
                let db_path = path_clone.clone();
                #[cfg(feature = "mwal_backend")]
                let vwal_methods = vwal_methods.clone();
                let hook = WalLoggerHook::new(logger.clone());
                async move {
                    LibSqlDb::new(
                        db_path,
                        #[cfg(feature = "mwal_backend")]
                        vwal_methods,
                        hook,
                    )
                }
            };
            let service = DbFactoryService::new(db_factory.clone());
            if let Some(addr) = config.rpc_server_addr {
                tokio::spawn(run_rpc_server(addr, db_factory, logger_clone));
            }
            run_service(service, config).await?;
        }
    }

    Ok(())
}
