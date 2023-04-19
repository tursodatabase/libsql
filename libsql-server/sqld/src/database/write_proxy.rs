use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex as PMutex;
use tokio::sync::{watch, Mutex};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::auth::{Authenticated, Authorized};
use crate::error::Error;
use crate::query::{QueryResponse, QueryResult};
use crate::query_analysis::State;
use crate::replication::FrameNo;
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
    extensions: Vec<PathBuf>,
    stats: Stats,
    applied_frame_no_receiver: watch::Receiver<FrameNo>,
}

impl WriteProxyDbFactory {
    pub fn new(
        db_path: PathBuf,
        extensions: Vec<PathBuf>,
        channel: Channel,
        uri: tonic::transport::Uri,
        stats: Stats,
        applied_frame_no_receiver: watch::Receiver<FrameNo>,
    ) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self {
            client,
            db_path,
            extensions,
            stats,
            applied_frame_no_receiver,
        }
    }
}

#[async_trait::async_trait]
impl DbFactory for WriteProxyDbFactory {
    async fn create(&self) -> Result<Arc<dyn Database>> {
        let db = WriteProxyDatabase::new(
            self.client.clone(),
            self.db_path.clone(),
            self.extensions.clone(),
            self.stats.clone(),
            self.applied_frame_no_receiver.clone(),
        )?;
        Ok(Arc::new(db))
    }
}

pub struct WriteProxyDatabase {
    read_db: LibSqlDb,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<State>,
    client_id: Uuid,
    /// FrameNo of the last write performed by this connection on the primary.
    /// any subsequent read on this connection must wait for the replicator to catch up with this
    /// frame_no
    last_write_frame_no: PMutex<FrameNo>,
    /// Notifier from the repliator of the currently applied frameno
    applied_frame_no_receiver: watch::Receiver<FrameNo>,
}

impl WriteProxyDatabase {
    fn new(
        write_proxy: ProxyClient<Channel>,
        path: PathBuf,
        extensions: Vec<PathBuf>,
        stats: Stats,
        applied_frame_no_receiver: watch::Receiver<FrameNo>,
    ) -> Result<Self> {
        let read_db = LibSqlDb::new(path, extensions, (), false, stats)?;
        Ok(Self {
            read_db,
            write_proxy,
            state: Mutex::new(State::Init),
            client_id: Uuid::new_v4(),
            last_write_frame_no: PMutex::new(FrameNo::MAX),
            applied_frame_no_receiver,
        })
    }

    async fn execute_remote(
        &self,
        pgm: Program,
        state: &mut State,
        auth: Authenticated,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        let mut client = self.write_proxy.clone();
        let authorized: Option<i32> = match auth {
            Authenticated::Anonymous => None,
            Authenticated::Authorized(Authorized::ReadOnly) => Some(0),
            Authenticated::Authorized(Authorized::FullAccess) => Some(1),
        };
        let req = crate::rpc::proxy::rpc::ProgramReq {
            client_id: self.client_id.to_string(),
            pgm: Some(pgm.into()),
            authorized,
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

                self.update_last_write_frame_no(execute_result.current_frame_no);

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

    fn update_last_write_frame_no(&self, new_frame_no: FrameNo) {
        let mut last_frame_no = self.last_write_frame_no.lock();
        if *last_frame_no == FrameNo::MAX || new_frame_no > *last_frame_no {
            *last_frame_no = new_frame_no
        }
    }

    /// wait for the replicator to have caught up with our current write frame_no
    async fn wait_replication_sync(&self) -> Result<()> {
        let current_frame_no = *self.last_write_frame_no.lock();

        if current_frame_no == FrameNo::MAX {
            return Ok(());
        }

        let mut receiver = self.applied_frame_no_receiver.clone();
        let mut last_applied = *receiver.borrow_and_update();

        while last_applied < current_frame_no {
            receiver
                .changed()
                .await
                .map_err(|_| Error::ReplicatorExited)?;
            last_applied = *receiver.borrow_and_update();
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Database for WriteProxyDatabase {
    async fn execute_program(
        &self,
        pgm: Program,
        auth: Authenticated,
    ) -> Result<(Vec<Option<QueryResult>>, State)> {
        let mut state = self.state.lock().await;
        if *state == State::Init && pgm.is_read_only() {
            self.wait_replication_sync().await?;
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let (results, new_state) = self.read_db.execute_program(pgm.clone(), auth).await?;
            if new_state != State::Init {
                self.read_db.rollback(auth).await?;
                self.execute_remote(pgm, &mut state, auth).await
            } else {
                Ok((results, new_state))
            }
        } else {
            self.execute_remote(pgm, &mut state, auth).await
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
