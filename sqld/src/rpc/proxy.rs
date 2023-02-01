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

    use crate::error::Error as SqldError;
    use crate::query::QueryResponse;

    use self::{error::ErrorCode, execute_results::State, query_result::RowResult};
    tonic::include_proto!("proxy");

    impl From<crate::query::QueryResult> for RowResult {
        fn from(other: crate::query::QueryResult) -> Self {
            match other {
                Ok(QueryResponse::ResultSet(set)) => RowResult::Row(set.into()),
                Err(e) => RowResult::Error(e.into()),
            }
        }
    }

    impl From<SqldError> for Error {
        fn from(other: SqldError) -> Self {
            Error {
                message: other.to_string(),
                code: ErrorCode::from(other).into(),
            }
        }
    }

    impl From<SqldError> for ErrorCode {
        fn from(other: SqldError) -> Self {
            match other {
                SqldError::LibSqlInvalidQueryParams(_) => ErrorCode::SqlError,
                SqldError::LibSqlTxTimeout(_) => ErrorCode::TxTimeout,
                SqldError::LibSqlTxBusy => ErrorCode::TxBusy,
                _ => ErrorCode::Internal,
            }
        }
    }

    impl From<crate::query::QueryResult> for QueryResult {
        fn from(other: crate::query::QueryResult) -> Self {
            let res = match other {
                Ok(crate::query::QueryResponse::ResultSet(q)) => {
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

    impl TryFrom<crate::query::Params> for query::Params {
        type Error = SqldError;
        fn try_from(value: crate::query::Params) -> Result<Self, Self::Error> {
            match value {
                crate::query::Params::Named(params) => {
                    let iter = params.into_iter().map(|(k, v)| -> Result<_, SqldError> {
                        let v = Value {
                            data: bincode::serialize(&v)?,
                        };
                        Ok((k, v))
                    });
                    let (names, values) = itertools::process_results(iter, |i| i.unzip())?;
                    Ok(Self::Named(Named { names, values }))
                }
                crate::query::Params::Positional(params) => {
                    let values = params
                        .iter()
                        .map(|v| {
                            Ok(Value {
                                data: bincode::serialize(&v)?,
                            })
                        })
                        .collect::<Result<Vec<_>, SqldError>>()?;
                    Ok(Self::Positional(Positional { values }))
                }
            }
        }
    }

    impl TryFrom<query::Params> for crate::query::Params {
        type Error = SqldError;

        fn try_from(value: query::Params) -> Result<Self, Self::Error> {
            match value {
                query::Params::Positional(pos) => {
                    let params = pos
                        .values
                        .into_iter()
                        .map(|v| bincode::deserialize(&v.data).map_err(|e| e.into()))
                        .collect::<Result<Vec<_>, SqldError>>()?;
                    Ok(Self::Positional(params))
                }
                query::Params::Named(named) => {
                    let values = named.values.iter().map(|v| bincode::deserialize(&v.data));
                    let params = itertools::process_results(values, |values| {
                        named.names.into_iter().zip(values).collect()
                    })?;
                    Ok(Self::Named(params))
                }
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
            .into_iter()
            .map(|q| {
                // FIXME: we assume the statement is valid because we trust the caller to have verified
                // it before: do proper error handling instead

                let stmt = Statement::parse(&q.stmt)
                    .next()
                    .transpose()
                    .unwrap()
                    .unwrap_or_default();
                Ok(Query {
                    stmt,
                    params: Params::try_from(q.params.unwrap())?,
                })
            })
            .collect::<crate::Result<Vec<_>>>()
            .map_err(|_| {
                tonic::Status::new(tonic::Code::Internal, "failed to deserialize query")
            })?;

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
