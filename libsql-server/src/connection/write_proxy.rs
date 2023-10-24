use std::path::PathBuf;
use std::sync::Arc;

use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use parking_lot::Mutex as PMutex;
use rusqlite::types::ValueRef;
use sqld_libsql_bindings::wal_hook::{TransparentMethods, TRANSPARENT_METHODS};
use tokio::sync::{watch, Mutex};
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

use crate::auth::Authenticated;
use crate::error::Error;
use crate::metrics::REQUESTS_PROXIED;
use crate::namespace::NamespaceName;
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
use crate::{Result, DEFAULT_AUTO_CHECKPOINT};

use super::config::DatabaseConfigStore;
use super::libsql::{LibSqlConnection, MakeLibSqlConn};
use super::program::DescribeResult;
use super::Connection;
use super::{MakeConnection, Program};

pub struct MakeWriteProxyConn {
    client: ProxyClient<Channel>,
    stats: Arc<Stats>,
    applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    max_response_size: u64,
    max_total_response_size: u64,
    namespace: NamespaceName,
    make_read_only_conn: MakeLibSqlConn<TransparentMethods>,
}

impl MakeWriteProxyConn {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        db_path: PathBuf,
        extensions: Arc<[PathBuf]>,
        channel: Channel,
        uri: tonic::transport::Uri,
        stats: Arc<Stats>,
        config_store: Arc<DatabaseConfigStore>,
        applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        max_response_size: u64,
        max_total_response_size: u64,
        namespace: NamespaceName,
    ) -> crate::Result<Self> {
        let client = ProxyClient::with_origin(channel, uri);
        let make_read_only_conn = MakeLibSqlConn::new(
            db_path.clone(),
            &TRANSPARENT_METHODS,
            || (),
            stats.clone(),
            config_store.clone(),
            extensions.clone(),
            max_response_size,
            max_total_response_size,
            DEFAULT_AUTO_CHECKPOINT,
            applied_frame_no_receiver.clone(),
        )
        .await?;

        Ok(Self {
            client,
            stats,
            applied_frame_no_receiver,
            max_response_size,
            max_total_response_size,
            namespace,
            make_read_only_conn,
        })
    }
}

#[async_trait::async_trait]
impl MakeConnection for MakeWriteProxyConn {
    type Connection = WriteProxyConnection;
    async fn create(&self) -> Result<Self::Connection> {
        let db = WriteProxyConnection::new(
            self.client.clone(),
            self.stats.clone(),
            self.applied_frame_no_receiver.clone(),
            QueryBuilderConfig {
                max_size: Some(self.max_response_size),
                max_total_size: Some(self.max_total_response_size),
                auto_checkpoint: DEFAULT_AUTO_CHECKPOINT,
            },
            self.namespace.clone(),
            self.make_read_only_conn.create().await?,
        )
        .await?;
        Ok(db)
    }
}

#[derive(Debug)]
pub struct WriteProxyConnection {
    /// Lazily initialized read connection
    read_conn: LibSqlConnection<TransparentMethods>,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<State>,
    client_id: Uuid,
    /// FrameNo of the last write performed by this connection on the primary.
    /// any subsequent read on this connection must wait for the replicator to catch up with this
    /// frame_no
    last_write_frame_no: PMutex<Option<FrameNo>>,
    /// Notifier from the repliator of the currently applied frameno
    applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    builder_config: QueryBuilderConfig,
    stats: Arc<Stats>,
    namespace: NamespaceName,
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

    builder.finish(execute_result.current_frame_no)?;

    Ok(builder)
}

impl WriteProxyConnection {
    #[allow(clippy::too_many_arguments)]
    async fn new(
        write_proxy: ProxyClient<Channel>,
        stats: Arc<Stats>,
        applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
        builder_config: QueryBuilderConfig,
        namespace: NamespaceName,
        read_conn: LibSqlConnection<TransparentMethods>,
    ) -> Result<Self> {
        Ok(Self {
            read_conn,
            write_proxy,
            state: Mutex::new(State::Init),
            client_id: Uuid::new_v4(),
            last_write_frame_no: PMutex::new(None),
            applied_frame_no_receiver,
            builder_config,
            stats,
            namespace,
        })
    }

    async fn execute_remote<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        state: &mut State,
        auth: Authenticated,
        builder: B,
    ) -> Result<(B, State)> {
        REQUESTS_PROXIED.increment(1);

        self.stats.inc_write_requests_delegated();
        let mut client = self.write_proxy.clone();

        let mut req = Request::new(crate::rpc::proxy::rpc::ProgramReq {
            client_id: self.client_id.to_string(),
            pgm: Some(pgm.into()),
        });

        let namespace = BinaryMetadataValue::from_bytes(self.namespace.as_slice());
        req.metadata_mut()
            .insert_bin(NAMESPACE_METADATA_KEY, namespace);
        auth.upgrade_grpc_request(&mut req);

        match client.execute(req).await {
            Ok(r) => {
                let execute_result = r.into_inner();
                *state = execute_result.state().into();
                let current_frame_no = execute_result.current_frame_no;
                let builder =
                    execute_results_to_builder(execute_result, builder, &self.builder_config)?;
                if let Some(current_frame_no) = current_frame_no {
                    self.update_last_write_frame_no(current_frame_no);
                }

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
        if last_frame_no.is_none() || new_frame_no > last_frame_no.unwrap() {
            *last_frame_no = Some(new_frame_no);
        }
    }

    /// wait for the replicator to have caught up with the replication_index if `Some` or our
    /// current write frame_no
    async fn wait_replication_sync(&self, replication_index: Option<FrameNo>) -> Result<()> {
        let current_fno = replication_index.or_else(|| *self.last_write_frame_no.lock());
        match current_fno {
            Some(current_frame_no) => {
                let mut receiver = self.applied_frame_no_receiver.clone();
                receiver
                    .wait_for(|last_applied| match last_applied {
                        Some(x) => *x >= current_frame_no,
                        None => true,
                    })
                    .await
                    .map_err(|_| Error::ReplicatorExited)?;

                Ok(())
            }
            None => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl Connection for WriteProxyConnection {
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<(B, State)> {
        let mut state = self.state.lock().await;

        // This is a fresh namespace, and it is not replicated yet, proxy the first request.
        if self.applied_frame_no_receiver.borrow().is_none() {
            self.execute_remote(pgm, &mut state, auth, builder).await
        } else if *state == State::Init && pgm.is_read_only() {
            self.wait_replication_sync(replication_index).await?;
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let (builder, new_state) = self
                .read_conn
                .execute_program(pgm.clone(), auth.clone(), builder, replication_index)
                .await?;
            if new_state != State::Init {
                self.read_conn.rollback(auth.clone()).await?;
                self.execute_remote(pgm, &mut state, auth, builder).await
            } else {
                Ok((builder, new_state))
            }
        } else {
            self.execute_remote(pgm, &mut state, auth, builder).await
        }
    }

    async fn describe(
        &self,
        sql: String,
        auth: Authenticated,
        replication_index: Option<FrameNo>,
    ) -> Result<DescribeResult> {
        self.wait_replication_sync(replication_index).await?;
        self.read_conn.describe(sql, auth, replication_index).await
    }

    async fn is_autocommit(&self) -> Result<bool> {
        let state = self.state.lock().await;
        Ok(match *state {
            State::Txn => false,
            State::Init | State::Invalid => true,
        })
    }

    async fn checkpoint(&self) -> Result<()> {
        self.wait_replication_sync(None).await?;
        self.read_conn.checkpoint().await
    }

    async fn vacuum_if_needed(&self) -> Result<()> {
        tracing::warn!("vacuum is not supported on write proxy");
        Ok(())
    }

    fn diagnostics(&self) -> String {
        format!("{:?}", self.state)
    }
}

impl Drop for WriteProxyConnection {
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
    // use arbitrary::{Arbitrary, Unstructured};
    // use rand::Fill;
    //
    // use super::*;
    // use crate::query_result_builder::test::test_driver;

    // In this test, we generate random ExecuteResults, and ensures that the `execute_results_to_builder` drives the builder FSM correctly.
    // #[test]
    // fn test_execute_results_to_builder() {
    //     test_driver(1000, |b| {
    //         let mut data = [0; 10_000];
    //         data.try_fill(&mut rand::thread_rng()).unwrap();
    //         let mut un = Unstructured::new(&data);
    //         let res = ExecuteResults::arbitrary(&mut un).unwrap();
    //         execute_results_to_builder(res, b, &QueryBuilderConfig::default())
    //     });
    // }
}
