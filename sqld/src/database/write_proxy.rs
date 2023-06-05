use std::path::PathBuf;

use parking_lot::Mutex as PMutex;
use rusqlite::types::ValueRef;
use sqld_libsql_bindings::wal_hook::TRANSPARENT_METHODS;
use tokio::sync::{watch, Mutex};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::auth::{Authenticated, Authorized};
use crate::error::Error;
use crate::query::Value;
use crate::query_analysis::State;
use crate::query_result_builder::{
    Column, QueryBuilderConfig, QueryResultBuilder, QueryResultBuilderError,
};
use crate::replication::FrameNo;
use crate::rpc::proxy::rpc::proxy_client::ProxyClient;
use crate::rpc::proxy::rpc::query_result::RowResult;
use crate::rpc::proxy::rpc::{DisconnectMessage, ExecuteResults};
use crate::stats::Stats;
use crate::Result;

use super::Program;
use super::{factory::DbFactory, libsql::LibSqlDb, Database, DescribeResult};

#[derive(Clone)]
pub struct WriteProxyDbFactory {
    client: ProxyClient<Channel>,
    db_path: PathBuf,
    extensions: Vec<PathBuf>,
    stats: Stats,
    applied_frame_no_receiver: watch::Receiver<FrameNo>,
    max_response_size: u64,
}

impl WriteProxyDbFactory {
    pub fn new(
        db_path: PathBuf,
        extensions: Vec<PathBuf>,
        channel: Channel,
        uri: tonic::transport::Uri,
        stats: Stats,
        applied_frame_no_receiver: watch::Receiver<FrameNo>,
        max_response_size: u64,
    ) -> Self {
        let client = ProxyClient::with_origin(channel, uri);
        Self {
            client,
            db_path,
            extensions,
            stats,
            applied_frame_no_receiver,
            max_response_size,
        }
    }
}

#[async_trait::async_trait]
impl DbFactory for WriteProxyDbFactory {
    type Db = WriteProxyDatabase;
    async fn create(&self) -> Result<Self::Db> {
        let db = WriteProxyDatabase::new(
            self.client.clone(),
            self.db_path.clone(),
            self.extensions.clone(),
            self.stats.clone(),
            self.applied_frame_no_receiver.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
            },
        )
        .await?;
        Ok(db)
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
    builder_config: QueryBuilderConfig,
}

fn execute_results_to_builder<B: QueryResultBuilder>(
    execute_result: ExecuteResults,
    mut builder: B,
    config: &QueryBuilderConfig,
) -> Result<B> {
    builder.init(config)?;
    for result in execute_result.results {
        match result.row_result {
            Some(RowResult::Row(rows)) => {
                builder.begin_step()?;
                builder.cols_description(rows.column_descriptions.iter().map(|c| Column {
                    name: &c.name,
                    decl_ty: c.decltype.as_deref(),
                }))?;

                builder.begin_rows()?;
                for row in rows.rows {
                    builder.begin_row()?;
                    for value in row.values {
                        let value: Value = bincode::deserialize(&value.data)
                            // something is wrong, better stop right here
                            .map_err(QueryResultBuilderError::from_any)?;
                        builder.add_row_value(ValueRef::from(&value))?;
                    }
                    builder.finish_row()?;
                }

                builder.finish_rows()?;

                builder.finish_step(rows.affected_row_count, rows.last_insert_rowid)?;
            }
            Some(RowResult::Error(err)) => {
                builder.begin_step()?;
                builder.step_error(Error::RpcQueryError(err))?;
                builder.finish_step(0, None)?;
            }
            None => (),
        }
    }

    builder.finish()?;

    Ok(builder)
}

impl WriteProxyDatabase {
    async fn new(
        write_proxy: ProxyClient<Channel>,
        path: PathBuf,
        extensions: Vec<PathBuf>,
        stats: Stats,
        applied_frame_no_receiver: watch::Receiver<FrameNo>,
        builder_config: QueryBuilderConfig,
    ) -> Result<Self> {
        let read_db = LibSqlDb::new(
            path,
            extensions,
            &TRANSPARENT_METHODS,
            (),
            stats,
            builder_config,
        )
        .await?;
        Ok(Self {
            read_db,
            write_proxy,
            state: Mutex::new(State::Init),
            client_id: Uuid::new_v4(),
            last_write_frame_no: PMutex::new(FrameNo::MAX),
            applied_frame_no_receiver,
            builder_config,
        })
    }

    async fn execute_remote<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        state: &mut State,
        auth: Authenticated,
        builder: B,
    ) -> Result<(B, State)> {
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
                let current_frame_no = execute_result.current_frame_no;
                let builder =
                    execute_results_to_builder(execute_result, builder, &self.builder_config)?;
                self.update_last_write_frame_no(current_frame_no);

                Ok((builder, *state))
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
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
    ) -> Result<(B, State)> {
        let mut state = self.state.lock().await;
        if *state == State::Init && pgm.is_read_only() {
            self.wait_replication_sync().await?;
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let (builder, new_state) = self
                .read_db
                .execute_program(pgm.clone(), auth, builder)
                .await?;
            if new_state != State::Init {
                self.read_db.rollback(auth).await?;
                self.execute_remote(pgm, &mut state, auth, builder).await
            } else {
                Ok((builder, new_state))
            }
        } else {
            self.execute_remote(pgm, &mut state, auth, builder).await
        }
    }

    async fn describe(&self, sql: String, auth: Authenticated) -> Result<DescribeResult> {
        self.wait_replication_sync().await?;
        self.read_db.describe(sql, auth).await
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

#[cfg(test)]
pub mod test {
    use arbitrary::{Arbitrary, Unstructured};
    use rand::Fill;

    use super::*;
    use crate::query_result_builder::test::test_driver;

    /// generate an arbitraty rpc value. see build.rs for usage.
    pub fn arbitrary_rpc_value(u: &mut Unstructured) -> arbitrary::Result<Vec<u8>> {
        let data = bincode::serialize(&crate::query::Value::arbitrary(u)?).unwrap();

        Ok(data)
    }

    /// In this test, we generate random ExecuteResults, and ensures that the `execute_results_to_builder` drives the builder FSM correctly.
    #[test]
    fn test_execute_results_to_builder() {
        test_driver(1000, |b| {
            let mut data = [0; 10_000];
            data.try_fill(&mut rand::thread_rng()).unwrap();
            let mut un = Unstructured::new(&data);
            let res = ExecuteResults::arbitrary(&mut un).unwrap();
            execute_results_to_builder(res, b, &QueryBuilderConfig::default())
        });
    }
}
