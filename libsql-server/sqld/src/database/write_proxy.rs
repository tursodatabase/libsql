use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::transport::Channel;
use uuid::Uuid;

use crate::error::Error;
use crate::query::{QueryResponse, QueryResult};
use crate::query_analysis::State;
use crate::rpc::proxy::rpc::proxy_client::ProxyClient;
use crate::rpc::proxy::rpc::query_result::RowResult;
use crate::rpc::proxy::rpc::DisconnectMessage;
use crate::stats::Stats;
use crate::Result;

use super::Program;
use super::{factory::DbFactory, libsql::LibSqlDb, Database};

#[derive(Clone)]
pub struct WriteProxyDbFactory {
    client: ProxyClient<Channel>,
    db_path: PathBuf,
    stats: Stats,
}

impl WriteProxyDbFactory {
    pub fn new(
        db_path: PathBuf,
        channel: Channel,
        uri: tonic::transport::Uri,
        stats: Stats,
    ) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self {
            client,
            db_path,
            stats,
        }
    }
}

#[async_trait::async_trait]
impl DbFactory for WriteProxyDbFactory {
    async fn create(&self) -> Result<Arc<dyn Database>> {
        let db = WriteProxyDatabase::new(
            self.client.clone(),
            self.db_path.clone(),
            self.stats.clone(),
        )?;
        Ok(Arc::new(db))
    }
}

pub struct WriteProxyDatabase {
    read_db: LibSqlDb,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<State>,
    client_id: Uuid,
}

impl WriteProxyDatabase {
    fn new(write_proxy: ProxyClient<Channel>, path: PathBuf, stats: Stats) -> Result<Self> {
        let read_db = LibSqlDb::new(path, (), false, stats)?;
        Ok(Self {
            read_db,
            write_proxy,
            state: Mutex::new(State::Init),
            client_id: Uuid::new_v4(),
        })
    }

    async fn execute_remote(
        &self,
        pgm: Program,
        state: &mut State,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        let mut client = self.write_proxy.clone();
        let req = crate::rpc::proxy::rpc::ProgramReq {
            client_id: self.client_id.to_string(),
            pgm: Some(pgm.into()),
        };
        match client.execute(req).await {
            Ok(r) => {
                let execute_result = r.into_inner();
                *state = execute_result.state().into();
                let results = execute_result
                    .results
                    .into_iter()
                    .map(|r| -> Option<QueryResult> {
                        let result = r.row_result?;
                        match result {
                            RowResult::Row(res) => Some(Ok(QueryResponse::ResultSet(res.into()))),
                            RowResult::Error(e) => Some(Err(Error::RpcQueryError(e))),
                        }
                    })
                    .collect();

                Ok((results, *state))
            }
            Err(e) => {
                // Set state to invalid, so next call is sent to remote, and we have a chance
                // to recover state.
                *state = State::Invalid;
                Err(Error::RpcQueryExecutionError(e))
            }
        }
    }
}

#[async_trait::async_trait]
impl Database for WriteProxyDatabase {
    async fn execute_program(
        &self,
        pgm: Program,
        auth: crate::auth::Authenticated,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        let mut state = self.state.lock().await;
        if *state == State::Init && pgm.is_read_only() {
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let (results, new_state) = self.read_db.execute_program(pgm.clone(), auth).await?;
            if new_state != State::Init {
                self.read_db.rollback(auth).await?;
                self.execute_remote(pgm, &mut state).await
            } else {
                Ok((results, new_state))
            }
        } else {
            self.execute_remote(pgm, &mut state).await
        }
    }
}

impl Drop for WriteProxyDatabase {
    fn drop(&mut self) {
        // best effort attempt to disconnect
        let mut remote = self.write_proxy.clone();
        let client_id = self.client_id.to_string();
        tokio::spawn(async move {
            let _ = remote.disconnect(DisconnectMessage { client_id }).await;
        });
    }
}
