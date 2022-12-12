use std::collections::HashMap;
use std::net::SocketAddr;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use uuid::Uuid;

use crate::database::service::DbFactory;
use crate::database::Database;
use crate::proxy_rpc::proxy_server::{Proxy, ProxyServer as RpcProxyServer};
use crate::proxy_rpc::{
    error::ErrorCode as RpcErrorCode, query_result::Result as RpcResult, Ack, DisconnectMessage,
    Error as RpcError, QueryResult as RpcQueryResult, SimpleQuery,
};
use crate::query::{ErrorCode, QueryResponse, QueryResult};
use crate::query_analysis::Statements;

struct ProxyServer<F: DbFactory> {
    clients: RwLock<HashMap<Uuid, F::Db>>,
    factory: F,
}

impl<F: DbFactory> ProxyServer<F> {
    fn new(factory: F) -> Self {
        Self {
            clients: Default::default(),
            factory,
        }
    }
}

impl From<QueryResult> for RpcQueryResult {
    fn from(other: QueryResult) -> Self {
        match other {
            Ok(QueryResponse::ResultSet(q)) => {
                let rows = q.into();
                RpcQueryResult {
                    error: None,
                    rows: Some(rows),
                    result: RpcResult::Ok.into(),
                }
            }
            Ok(QueryResponse::Ack) => todo!(),
            Err(e) => {
                let code = match e.code {
                    ErrorCode::SQLError => RpcErrorCode::SqlError,
                    ErrorCode::TxBusy => RpcErrorCode::TxBusy,
                    ErrorCode::TxTimeout => RpcErrorCode::TxTimeout,
                    ErrorCode::Internal => RpcErrorCode::Internal,
                };

                let err = RpcError {
                    code: code.into(),
                    message: e.msg,
                };

                RpcQueryResult {
                    error: Some(err),
                    rows: None,
                    result: RpcResult::Err.into(),
                }
            }
        }
    }
}

#[tonic::async_trait]
impl<F> Proxy for ProxyServer<F>
where
    F: DbFactory,
    F::Db: Send + Sync + Clone,
    F::Future: Send + Sync,
{
    async fn query(
        &self,
        req: tonic::Request<SimpleQuery>,
    ) -> Result<tonic::Response<RpcQueryResult>, tonic::Status> {
        let SimpleQuery { client_id, q } = req.into_inner();
        let client_id = Uuid::from_slice(&client_id).unwrap();

        let lock = self.clients.upgradable_read().await;
        let db = match lock.get(&client_id) {
            Some(db) => db.clone(),
            None => {
                let db = self.factory.create().await.unwrap();
                tracing::debug!("connected: {client_id}");
                let mut lock = RwLockUpgradableReadGuard::upgrade(lock).await;
                lock.insert(client_id, db.clone());
                db
            }
        };

        tracing::debug!("executing request for {client_id}: {q}");
        let stmts = Statements::parse(q).unwrap();
        let result = db.execute(stmts).await;

        Ok(tonic::Response::new(result.into()))
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        let DisconnectMessage { client_id } = msg.into_inner();
        let client_id = Uuid::from_slice(&client_id).unwrap();

        tracing::debug!("disconnected: {client_id}");

        self.clients.write().await.remove(&client_id);

        Ok(tonic::Response::new(Ack {}))
    }
}

pub async fn run_proxy_server<F>(addr: SocketAddr, factory: F) -> anyhow::Result<()>
where
    F: DbFactory + 'static,
    F::Db: Sync + Send + Clone,
{
    let server = ProxyServer::new(factory);
    tracing::info!("serving write proxy server at {addr}");
    tonic::transport::Server::builder()
        .add_service(RpcProxyServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
