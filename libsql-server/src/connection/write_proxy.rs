use std::path::PathBuf;
use std::sync::Arc;

use futures_core::future::BoxFuture;
use futures_core::Stream;
use libsql_replication::rpc::proxy::proxy_client::ProxyClient;
use libsql_replication::rpc::proxy::{
    exec_req, exec_resp, ExecReq, ExecResp, StreamDescribeReq, StreamProgramReq,
};
use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use parking_lot::Mutex as PMutex;
use sqld_libsql_bindings::wal_hook::{TransparentMethods, TRANSPARENT_METHODS};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_stream::StreamExt;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Streaming};

use crate::auth::Authenticated;
use crate::connection::program::{DescribeCol, DescribeParam};
use crate::error::Error;
use crate::namespace::NamespaceName;
use crate::query_analysis::TxnStatus;
use crate::query_result_builder::{QueryBuilderConfig, QueryResultBuilder};
use crate::replication::FrameNo;
use crate::stats::Stats;
use crate::{Result, DEFAULT_AUTO_CHECKPOINT};

use super::config::DatabaseConfigStore;
use super::libsql::{LibSqlConnection, MakeLibSqlConn};
use super::program::DescribeResponse;
use super::Connection;
use super::{MakeConnection, Program};

pub type RpcStream = Streaming<ExecResp>;

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
    type Connection = WriteProxyConnection<RpcStream>;
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

pub struct WriteProxyConnection<R> {
    /// Lazily initialized read connection
    read_conn: LibSqlConnection<TransparentMethods>,
    write_proxy: ProxyClient<Channel>,
    state: Mutex<TxnStatus>,
    /// FrameNo of the last write performed by this connection on the primary.
    /// any subsequent read on this connection must wait for the replicator to catch up with this
    /// frame_no
    last_write_frame_no: PMutex<Option<FrameNo>>,
    /// Notifier from the repliator of the currently applied frameno
    applied_frame_no_receiver: watch::Receiver<Option<FrameNo>>,
    builder_config: QueryBuilderConfig,
    stats: Arc<Stats>,
    namespace: NamespaceName,

    remote_conn: Mutex<Option<RemoteConnection<R>>>,
}

impl WriteProxyConnection<RpcStream> {
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
            state: Mutex::new(TxnStatus::Init),
            last_write_frame_no: Default::default(),
            applied_frame_no_receiver,
            builder_config,
            stats,
            namespace,
            remote_conn: Default::default(),
        })
    }

    async fn with_remote_conn<F, Ret>(
        &self,
        auth: Authenticated,
        builder_config: QueryBuilderConfig,
        cb: F,
    ) -> crate::Result<Ret>
    where
        F: FnOnce(&mut RemoteConnection) -> BoxFuture<'_, crate::Result<Ret>>,
    {
        let mut remote_conn = self.remote_conn.lock().await;
        if remote_conn.is_some() {
            cb(remote_conn.as_mut().unwrap()).await
        } else {
            let conn = RemoteConnection::connect(
                self.write_proxy.clone(),
                self.namespace.clone(),
                auth,
                builder_config,
            )
            .await?;
            let conn = remote_conn.insert(conn);
            cb(conn).await
        }
    }

    async fn execute_remote<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        status: &mut TxnStatus,
        auth: Authenticated,
        builder: B,
    ) -> Result<B> {
        self.stats.inc_write_requests_delegated();
        *status = TxnStatus::Invalid;
        let res = self
            .with_remote_conn(auth, self.builder_config, |conn| {
                Box::pin(conn.execute(pgm, builder))
            })
            .await;

        let (builder, new_status, new_frame_no) = match res {
            Ok(res) => res,
            Err(e @ (Error::PrimaryStreamDisconnect | Error::PrimaryStreamMisuse)) => {
                // drop the connection, and reset the state.
                self.remote_conn.lock().await.take();
                *status = TxnStatus::Init;
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        *status = new_status;
        if let Some(current_frame_no) = new_frame_no {
            self.update_last_write_frame_no(current_frame_no);
        }

        Ok(builder)
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

struct RemoteConnection<R = Streaming<ExecResp>> {
    response_stream: R,
    request_sender: mpsc::Sender<ExecReq>,
    current_request_id: u32,
    builder_config: QueryBuilderConfig,
}

impl RemoteConnection {
    async fn connect(
        mut client: ProxyClient<Channel>,
        namespace: NamespaceName,
        auth: Authenticated,
        builder_config: QueryBuilderConfig,
    ) -> crate::Result<Self> {
        let (request_sender, receiver) = mpsc::channel(1);

        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
        let mut req = Request::new(stream);
        let namespace = BinaryMetadataValue::from_bytes(namespace.as_slice());
        req.metadata_mut()
            .insert_bin(NAMESPACE_METADATA_KEY, namespace);
        auth.upgrade_grpc_request(&mut req);
        let response_stream = client.stream_exec(req).await.unwrap().into_inner();

        Ok(Self {
            response_stream,
            request_sender,
            current_request_id: 0,
            builder_config,
        })
    }
}

impl<R> RemoteConnection<R>
where
    R: Stream<Item = Result<ExecResp, tonic::Status>> + Unpin,
{
    /// Perform a request on to the remote peer, and call message_cb for every message received for
    /// that request. message cb should return whether to expect more message for that request.
    async fn make_request(
        &mut self,
        req: exec_req::Request,
        mut response_cb: impl FnMut(exec_resp::Response) -> crate::Result<bool>,
    ) -> crate::Result<()> {
        let request_id = self.current_request_id;
        self.current_request_id += 1;

        let req = ExecReq {
            request_id,
            request: Some(req),
        };

        self.request_sender
            .send(req)
            .await
            .map_err(|_| Error::PrimaryStreamDisconnect)?;

        while let Some(resp) = self.response_stream.next().await {
            match resp {
                Ok(resp) => {
                    // there was an interuption, and we moved to the next query
                    if resp.request_id > request_id {
                        return Err(Error::PrimaryStreamInterupted);
                    }

                    // we can ignore response for previously interupted requests
                    if resp.request_id < request_id {
                        continue;
                    }

                    if !response_cb(resp.response.ok_or(Error::PrimaryStreamMisuse)?)? {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("received an error from connection stream: {e}");
                    return Err(Error::PrimaryStreamDisconnect);
                }
            }
        }

        Ok(())
    }

    async fn execute<B: QueryResultBuilder>(
        &mut self,
        program: Program,
        mut builder: B,
    ) -> crate::Result<(B, TxnStatus, Option<FrameNo>)> {
        let mut txn_status = TxnStatus::Invalid;
        let mut new_frame_no = None;
        let builder_config = self.builder_config;
        let cb = |response: exec_resp::Response| match response {
            exec_resp::Response::ProgramResp(resp) => {
                crate::rpc::streaming_exec::apply_program_resp_to_builder(
                    &builder_config,
                    &mut builder,
                    resp,
                    |last_frame_no, status| {
                        txn_status = status;
                        new_frame_no = last_frame_no;
                    },
                )
            }
            exec_resp::Response::DescribeResp(_) => Err(Error::PrimaryStreamMisuse),
            exec_resp::Response::Error(e) => Err(Error::RpcQueryError(e)),
        };

        self.make_request(
            exec_req::Request::Execute(StreamProgramReq {
                pgm: Some(program.into()),
            }),
            cb,
        )
        .await?;

        Ok((builder, txn_status, new_frame_no))
    }

    #[allow(dead_code)] // reference implementation
    async fn describe(&mut self, stmt: String) -> crate::Result<DescribeResponse> {
        let mut out = None;
        let cb = |response: exec_resp::Response| match response {
            exec_resp::Response::DescribeResp(resp) => {
                out = Some(DescribeResponse {
                    params: resp
                        .params
                        .into_iter()
                        .map(|p| DescribeParam { name: p.name })
                        .collect(),
                    cols: resp
                        .cols
                        .into_iter()
                        .map(|c| DescribeCol {
                            name: c.name,
                            decltype: c.decltype,
                        })
                        .collect(),
                    is_explain: resp.is_explain,
                    is_readonly: resp.is_readonly,
                });

                Ok(false)
            }
            exec_resp::Response::Error(e) => Err(Error::RpcQueryError(e)),
            exec_resp::Response::ProgramResp(_) => Err(Error::PrimaryStreamMisuse),
        };

        self.make_request(exec_req::Request::Describe(StreamDescribeReq { stmt }), cb)
            .await?;

        out.ok_or(Error::PrimaryStreamMisuse)
    }
}

#[async_trait::async_trait]
impl Connection for WriteProxyConnection<RpcStream> {
    async fn execute_program<B: QueryResultBuilder>(
        &self,
        pgm: Program,
        auth: Authenticated,
        builder: B,
        replication_index: Option<FrameNo>,
    ) -> Result<B> {
        let mut state = self.state.lock().await;

        // This is a fresh namespace, and it is not replicated yet, proxy the first request.
        if self.applied_frame_no_receiver.borrow().is_none() {
            self.execute_remote(pgm, &mut state, auth, builder).await
        } else if *state == TxnStatus::Init && pgm.is_read_only() {
            // set the state to invalid before doing anything, and set it to a valid state after.
            *state = TxnStatus::Invalid;
            self.wait_replication_sync(replication_index).await?;
            // We know that this program won't perform any writes. We attempt to run it on the
            // replica. If it leaves an open transaction, then this program is an interactive
            // transaction, so we rollback the replica, and execute again on the primary.
            let builder = self
                .read_conn
                .execute_program(pgm.clone(), auth.clone(), builder, replication_index)
                .await?;
            let new_state = self.read_conn.txn_status()?;
            if new_state != TxnStatus::Init {
                self.read_conn.rollback(auth.clone()).await?;
                self.execute_remote(pgm, &mut state, auth, builder).await
            } else {
                *state = new_state;
                Ok(builder)
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
    ) -> Result<Result<DescribeResponse>> {
        self.wait_replication_sync(replication_index).await?;
        self.read_conn.describe(sql, auth, replication_index).await
    }

    async fn is_autocommit(&self) -> Result<bool> {
        let state = self.state.lock().await;
        Ok(match *state {
            TxnStatus::Txn => false,
            TxnStatus::Init | TxnStatus::Invalid => true,
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

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::rpc::streaming_exec::test::random_valid_program_resp;

    #[tokio::test]
    // in this test we do a roundtrip: generate a random valid program, stream it to
    // RemoteConnection, and make sure that the remote connection drives the builder with the same
    // state transitions.
    async fn validate_random_stream_response() {
        for _ in 0..10 {
            let (response_stream, validator) = random_valid_program_resp(500, 150);
            let (request_sender, _request_recver) = mpsc::channel(1);
            let mut remote = RemoteConnection {
                response_stream: response_stream.map(Ok),
                request_sender,
                current_request_id: 0,
                builder_config: QueryBuilderConfig::default(),
            };

            remote
                .execute(Program::seq(&[]), validator)
                .await
                .unwrap()
                .0
                .into_ret();
        }
    }
}
