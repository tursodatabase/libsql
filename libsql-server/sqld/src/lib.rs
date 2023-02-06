use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(feature = "mwal_backend")]
use std::sync::Mutex;
use std::task::{Context, Poll};

use anyhow::Context as AnyhowContext;
use database::libsql::LibSqlDb;
use database::service::DbFactoryService;
use database::write_proxy::WriteProxyDbFactory;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use query::{Queries, QueryResult};
use rpc::run_rpc_server;
use tower::load::Constant;
use tower::{Service, ServiceExt};
use wal_logger::{WalLogger, WalLoggerHook};

use crate::error::Error;
use crate::postgres::service::PgConnectionFactory;
use crate::server::Server;

pub use sqld_libsql_bindings as libsql;

mod database;
mod error;
mod http;
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

type Result<T> = std::result::Result<T, Error>;

pub struct Config {
    pub db_path: PathBuf,
    pub tcp_addr: Option<SocketAddr>,
    pub ws_addr: Option<SocketAddr>,
    pub http_addr: Option<SocketAddr>,
    pub http_auth: Option<String>,
    pub enable_http_console: bool,
    pub backend: Backend,
    #[cfg(feature = "mwal_backend")]
    pub mwal_addr: Option<String>,
    pub writer_rpc_addr: Option<String>,
    pub writer_rpc_tls: bool,
    pub writer_rpc_cert: Option<PathBuf>,
    pub writer_rpc_key: Option<PathBuf>,
    pub writer_rpc_ca_cert: Option<PathBuf>,
    pub rpc_server_addr: Option<SocketAddr>,
    pub rpc_server_tls: bool,
    pub rpc_server_cert: Option<PathBuf>,
    pub rpc_server_key: Option<PathBuf>,
    pub rpc_server_ca_cert: Option<PathBuf>,
}

async fn run_service<S>(service: S, config: Config) -> anyhow::Result<()>
where
    S: Service<(), Error = Error> + Sync + Send + 'static + Clone,
    S::Response: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
    S::Future: Send + Sync,
    <S::Response as Service<Queries>>::Future: Send,
{
    let mut server = Server::new();

    if let Some(addr) = config.tcp_addr {
        server.bind_tcp(addr).await?;
    }

    if let Some(addr) = config.ws_addr {
        server.bind_ws(addr).await?;
    }

    let mut handles = FuturesUnordered::new();
    let factory = PgConnectionFactory::new(service.clone());
    handles.push(tokio::spawn(server.serve(factory)));

    if let Some(addr) = config.http_addr {
        let authorizer =
            http::auth::parse_auth(config.http_auth).context("failed to parse HTTP auth config")?;
        let handle = tokio::spawn(http::run_http(
            addr,
            authorizer,
            service.map_response(|s| Constant::new(s, 1)),
            config.enable_http_console,
        ));

        handles.push(handle);
    }

    while let Some(res) = handles.next().await {
        res??;
    }

    Ok(())
}

pin_project_lite::pin_project! {
    struct FutOrNever<F> {
        #[pin]
        inner: Option<F>,
    }
}

impl<T> From<Option<T>> for FutOrNever<T> {
    fn from(inner: Option<T>) -> Self {
        Self { inner }
    }
}

impl<F: Future> Future for FutOrNever<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.as_pin_mut() {
            Some(fut) => fut.poll(cx),
            None => Poll::Pending,
        }
    }
}

pub async fn run_server(config: Config) -> anyhow::Result<()> {
    tracing::trace!("Backend: {:?}", config.backend);
    #[cfg(feature = "mwal_backend")]
    if config.backend == Backend::Mwal {
        std::env::set_var("MVSQLITE_DATA_PLANE", config.mwal_addr.as_ref().unwrap());
    }

    #[cfg(feature = "mwal_backend")]
    let vwal_methods = config.mwal_addr.as_ref().map(|_| {
        Arc::new(Mutex::new(
            sqld_libsql_bindings::mwal::ffi::libsql_wal_methods::new(),
        ))
    });

    match config.writer_rpc_addr {
        Some(ref addr) => {
            let factory = WriteProxyDbFactory::new(
                addr,
                config.writer_rpc_tls,
                config.writer_rpc_cert.clone(),
                config.writer_rpc_key.clone(),
                config.writer_rpc_ca_cert.clone(),
                config.db_path.clone(),
                #[cfg(feature = "mwal_backend")]
                vwal_methods,
            )
            .await
            .context("failed to start WriteProxy DB")?;
            let service = DbFactoryService::new(factory);
            run_service(service, config).await?;
        }
        None => {
            let logger = Arc::new(WalLogger::open("wallog").context("failed to open WalLogger")?);
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
            let rpc_fut: FutOrNever<_> = config
                .rpc_server_addr
                .map(|addr| {
                    tokio::spawn(run_rpc_server(
                        addr,
                        config.rpc_server_tls,
                        config.rpc_server_cert.clone(),
                        config.rpc_server_key.clone(),
                        config.rpc_server_ca_cert.clone(),
                        db_factory,
                        logger_clone,
                    ))
                })
                .into();

            let svc_fut = run_service(service, config);

            tokio::select! {
                ret = rpc_fut => {
                    match ret {
                        Ok(Ok(_)) => {
                            tracing::info!("Rpc server exited, terminating program.");
                        }
                        Err(e) => {
                            tracing::error!("Rpc server exited with error: {e}");
                        }
                        Ok(Err(e)) => {
                            tracing::error!("Rpc server exited with error: {e}");
                        }
                    }
                }
                ret = svc_fut => {
                    match ret {
                        Ok(_) => tracing::info!("Server exited"),
                        Err(e) => tracing::error!("Server exited with error: {e}"),
                    }
                }
            }
        }
    }

    Ok(())
}
