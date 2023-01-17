use std::collections::HashMap;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use uuid::Uuid;

use crate::database::service::DbFactory;
use crate::database::Database;
use crate::query::{Params, Query};
use crate::query_analysis::Statement;
use crate::rpc::proxy::proxy_rpc::execute_results::State;
use proxy_rpc::proxy_server::Proxy;
use proxy_rpc::{Ack, DisconnectMessage, Queries};

use self::proxy_rpc::ExecuteResults;

pub mod proxy_rpc {
    #![allow(clippy::all)]

    use crate::query::{self, QueryError, QueryResponse};

    use self::{error::ErrorCode, execute_results::State, query_result::RowResult};
    tonic::include_proto!("proxy");

    impl From<query::QueryResult> for RowResult {
        fn from(other: query::QueryResult) -> Self {
            match other {
                Ok(QueryResponse::ResultSet(set)) => RowResult::Row(set.into()),
                Err(e) => RowResult::Error(e.into()),
            }
        }
    }

    impl From<QueryError> for Error {
        fn from(other: QueryError) -> Self {
            Error {
                code: ErrorCode::from(other.code).into(),
                message: other.msg,
            }
        }
    }

    impl From<ErrorCode> for query::ErrorCode {
        fn from(other: ErrorCode) -> Self {
            match other {
                ErrorCode::SqlError => query::ErrorCode::SQLError,
                ErrorCode::TxBusy => query::ErrorCode::TxBusy,
                ErrorCode::TxTimeout => query::ErrorCode::TxTimeout,
                ErrorCode::Internal => query::ErrorCode::Internal,
            }
        }
    }

    impl From<query::ErrorCode> for ErrorCode {
        fn from(other: query::ErrorCode) -> Self {
            match other {
                query::ErrorCode::SQLError => ErrorCode::SqlError,
                query::ErrorCode::TxBusy => ErrorCode::TxBusy,
                query::ErrorCode::TxTimeout => ErrorCode::TxTimeout,
                query::ErrorCode::Internal => ErrorCode::Internal,
            }
        }
    }

    impl From<Error> for QueryError {
        fn from(other: Error) -> Self {
            Self::new(other.code().into(), other.message)
        }
    }

    impl From<query::QueryResult> for QueryResult {
        fn from(other: query::QueryResult) -> Self {
            let res = match other {
                Ok(query::QueryResponse::ResultSet(q)) => {
                    let rows = q.into();
                    RowResult::Row(rows)
                }
                Err(e) => RowResult::Error(e.into()),
            };

            QueryResult {
                row_result: Some(res),
            }
        }
    }

    impl From<crate::query_analysis::State> for State {
        fn from(other: crate::query_analysis::State) -> Self {
            match other {
                crate::query_analysis::State::Txn => Self::Txn,
                crate::query_analysis::State::Init => Self::Init,
                crate::query_analysis::State::Invalid => Self::Invalid,
            }
        }
    }

    impl From<State> for crate::query_analysis::State {
        fn from(other: State) -> Self {
            match other {
                State::Txn => crate::query_analysis::State::Txn,
                State::Init => crate::query_analysis::State::Init,
                State::Invalid => crate::query_analysis::State::Invalid,
            }
        }
    }
}

pub struct ProxyService<F: DbFactory> {
    clients: RwLock<HashMap<Uuid, F::Db>>,
    factory: F,
}

impl<F: DbFactory> ProxyService<F> {
    pub fn new(factory: F) -> Self {
        Self {
            clients: Default::default(),
            factory,
        }
    }
}

#[tonic::async_trait]
impl<F> Proxy for ProxyService<F>
where
    F: DbFactory,
    F::Db: Send + Sync + Clone,
    F::Future: Send + Sync,
{
    async fn execute(
        &self,
        req: tonic::Request<Queries>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        let Queries { client_id, queries } = req.into_inner();
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

        tracing::debug!("executing request for {client_id}");
        let queries = queries
            .iter()
            .map(|q| {
                // FIXME: we assume the statement is valid because we trust the caller to have verified
                // it before: do proper error handling instead
                let stmt = Statement::parse(q)
                    .next()
                    .transpose()
                    .unwrap()
                    .unwrap_or_default();
                Query {
                    stmt,
                    params: Params::new(),
                }
            })
            .collect();

        let (results, state) = db.execute(queries).await.unwrap();
        let results = results.into_iter().map(|r| r.into()).collect();

        Ok(tonic::Response::new(ExecuteResults {
            results,
            state: State::from(state).into(),
        }))
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
