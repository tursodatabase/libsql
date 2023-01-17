mod replication;

use std::future::{ready, Ready};
use std::path::PathBuf;
#[cfg(feature = "mwal_backend")]
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::TryRecvError;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::query::{self, QueryResponse, QueryResult};
use crate::query_analysis::{final_state, State};
use crate::rpc::proxy::proxy_rpc::proxy_client::ProxyClient;
use crate::rpc::proxy::proxy_rpc::query_result::RowResult;
use crate::rpc::proxy::proxy_rpc::{DisconnectMessage, Queries};

use super::{libsql::LibSqlDb, service::DbFactory, Database};
use replication::PeriodicDbUpdater;

#[derive(Clone)]
pub struct WriteProxyDbFactory {
    write_proxy: ProxyClient<Channel>,
    db_path: PathBuf,
    #[cfg(feature = "mwal_backend")]
    vwal_methods: Option<Arc<std::sync::Mutex<mwal::ffi::libsql_wal_methods>>>,
    /// abort handle: abort db update loop on drop
    _abort_handle: crossbeam::channel::Sender<()>,
}

impl WriteProxyDbFactory {
    pub async fn new(
        addr: &str,
        tls: bool,
        cert: Option<PathBuf>,
        db_path: PathBuf,
        #[cfg(feature = "mwal_backend")] vwal_methods: Option<
            Arc<std::sync::Mutex<mwal::ffi::libsql_wal_methods>>,
        >,
    ) -> anyhow::Result<Self> {
        let mut channel = Channel::from_shared(addr.to_string())?;
        if tls {
            let cert = std::fs::read_to_string(cert.unwrap())?;
            let ca = tonic::transport::Certificate::from_pem(&cert);
            let tls = tonic::transport::ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name("example.com");
            channel = channel.tls_config(tls)?;
        }
        let conn = channel.connect().await?;
        let write_proxy = ProxyClient::new(conn);
        let mut db_updater =
            PeriodicDbUpdater::new(&db_path, addr.to_string(), Duration::from_secs(1)).await?;
        let (_abort_handle, receiver) = crossbeam::channel::bounded::<()>(1);
        tokio::task::spawn_blocking(move || loop {
            // must abort
            if let Err(TryRecvError::Disconnected) = receiver.try_recv() {
                break;
            }
            db_updater.step();
        });
        Ok(Self {
            write_proxy,
            db_path,
            #[cfg(feature = "mwal_backend")]
            vwal_methods,
            _abort_handle,
        })
    }
}

impl DbFactory for WriteProxyDbFactory {
    type Future = Ready<anyhow::Result<Self::Db>>;

    type Db = WriteProxyDatabase;

    fn create(&self) -> Self::Future {
        ready(WriteProxyDatabase::new(
            self.write_proxy.clone(),
            self.db_path.clone(),
            #[cfg(feature = "mwal_backend")]
            self.vwal_methods.clone(),
        ))
    }
}

pub struct WriteProxyDatabase {
    read_db: LibSqlDb,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<State>,
    client_id: Uuid,
}

impl WriteProxyDatabase {
    fn new(
        write_proxy: ProxyClient<Channel>,
        path: PathBuf,
        #[cfg(feature = "mwal_backend")] vwal_methods: Option<
            Arc<std::sync::Mutex<mwal::ffi::libsql_wal_methods>>,
        >,
    ) -> anyhow::Result<Self> {
        let read_db = LibSqlDb::new(
            path,
            #[cfg(feature = "mwal_backend")]
            vwal_methods,
            (),
        )?;
        Ok(Self {
            read_db,
            write_proxy,
            state: Mutex::new(State::Init),
            client_id: Uuid::new_v4(),
        })
    }
}

#[async_trait::async_trait]
impl Database for WriteProxyDatabase {
    async fn execute(&self, queries: query::Queries) -> anyhow::Result<(Vec<QueryResult>, State)> {
        let mut state = self.state.lock().await;
        if *state == State::Init
            && queries.iter().all(|q| q.stmt.is_read_only())
            && final_state(*state, queries.iter().map(|s| &s.stmt)) == State::Init
        {
            self.read_db.execute(queries).await
        } else {
            let queries = Queries {
                queries: queries.into_iter().map(|q| q.stmt.stmt).collect(),
                client_id: self.client_id.as_bytes().to_vec(),
            };
            let mut client = self.write_proxy.clone();
            match client.execute(queries).await {
                Ok(r) => {
                    let execute_result = r.into_inner();
                    *state = execute_result.state().into();
                    let results = execute_result
                        .results
                        .into_iter()
                        .map(|r| -> QueryResult {
                            let result = r.row_result.unwrap();
                            match result {
                                RowResult::Row(res) => Ok(QueryResponse::ResultSet(res.into())),
                                RowResult::Error(e) => Err(e.into()),
                            }
                        })
                        .collect();

                    Ok((results, *state))
                }
                Err(e) => {
                    // Set state to invalid, so next call is sent to remote, and we have a chance
                    // to recover state.
                    *state = State::Invalid;
                    anyhow::bail!("rpc error: {e}")
                }
            }
        }
    }
}

impl Drop for WriteProxyDatabase {
    fn drop(&mut self) {
        // best effort attempt to disconnect
        let mut remote = self.write_proxy.clone();
        let client_id = self.client_id.as_bytes().to_vec();
        tokio::spawn(async move {
            let _ = remote.disconnect(DisconnectMessage { client_id }).await;
        });
    }
}
