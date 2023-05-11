use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use tokio::sync::watch;
use uuid::Uuid;

use crate::auth::{Authenticated, Authorized};
use crate::database::factory::DbFactory;
use crate::database::{Database, Program};
use crate::replication::FrameNo;

use self::rpc::execute_results::State;
use self::rpc::proxy_server::Proxy;
use self::rpc::{Ack, DisconnectMessage, ExecuteResults};

pub mod rpc {
    #![allow(clippy::all)]

    use std::sync::Arc;

    use anyhow::Context;

    use crate::query::QueryResponse;
    use crate::query_analysis::Statement;
    use crate::{database, error::Error as SqldError};

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

    impl From<Option<crate::query::QueryResult>> for QueryResult {
        fn from(other: Option<crate::query::QueryResult>) -> Self {
            let res = match other {
                Some(Ok(crate::query::QueryResponse::ResultSet(q))) => {
                    let rows = q.into();
                    Some(RowResult::Row(rows))
                }
                Some(Err(e)) => Some(RowResult::Error(e.into())),
                None => None,
            };

            QueryResult { row_result: res }
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

    impl TryFrom<Program> for database::Program {
        type Error = anyhow::Error;

        fn try_from(pgm: Program) -> Result<Self, Self::Error> {
            let steps = pgm
                .steps
                .into_iter()
                .map(TryInto::try_into)
                .collect::<anyhow::Result<_>>()?;

            Ok(Self::new(steps))
        }
    }

    impl TryFrom<Step> for database::Step {
        type Error = anyhow::Error;

        fn try_from(step: Step) -> Result<Self, Self::Error> {
            Ok(Self {
                query: step.query.context("step is missing query")?.try_into()?,
                cond: step.cond.map(TryInto::try_into).transpose()?,
            })
        }
    }

    impl TryFrom<Cond> for database::Cond {
        type Error = anyhow::Error;

        fn try_from(cond: Cond) -> Result<Self, Self::Error> {
            let cond = match cond.cond {
                Some(cond::Cond::Ok(OkCond { step })) => Self::Ok { step: step as _ },
                Some(cond::Cond::Err(ErrCond { step })) => Self::Err { step: step as _ },
                Some(cond::Cond::Not(cond)) => Self::Not {
                    cond: Box::new((*cond.cond.context("empty `not` condition")?).try_into()?),
                },
                Some(cond::Cond::And(AndCond { conds })) => Self::And {
                    conds: conds
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<anyhow::Result<_>>()?,
                },
                Some(cond::Cond::Or(OrCond { conds })) => Self::Or {
                    conds: conds
                        .into_iter()
                        .map(TryInto::try_into)
                        .collect::<anyhow::Result<_>>()?,
                },
                None => anyhow::bail!("invalid condition"),
            };

            Ok(cond)
        }
    }

    impl TryFrom<Query> for crate::query::Query {
        type Error = anyhow::Error;

        fn try_from(query: Query) -> Result<Self, Self::Error> {
            let stmt = Statement::parse(&query.stmt)
                .next()
                .context("invalid empty statement")??;

            Ok(Self {
                stmt,
                params: query
                    .params
                    .context("missing params in query")?
                    .try_into()?,
                want_rows: !query.skip_rows,
            })
        }
    }

    impl From<database::Program> for Program {
        fn from(pgm: database::Program) -> Self {
            // TODO: use unwrap_or_clone when stable
            let steps = match Arc::try_unwrap(pgm.steps) {
                Ok(steps) => steps,
                Err(arc) => (*arc).clone(),
            };

            Self {
                steps: steps.into_iter().map(|s| s.into()).collect(),
            }
        }
    }

    impl From<crate::query::Query> for Query {
        fn from(query: crate::query::Query) -> Self {
            Self {
                stmt: query.stmt.stmt,
                params: Some(query.params.try_into().unwrap()),
                skip_rows: !query.want_rows,
            }
        }
    }

    impl From<database::Step> for Step {
        fn from(step: database::Step) -> Self {
            Self {
                cond: step.cond.map(|c| c.into()),
                query: Some(step.query.into()),
            }
        }
    }

    impl From<database::Cond> for Cond {
        fn from(cond: database::Cond) -> Self {
            let cond = match cond {
                database::Cond::Ok { step } => cond::Cond::Ok(OkCond { step: step as i64 }),
                database::Cond::Err { step } => cond::Cond::Err(ErrCond { step: step as i64 }),
                database::Cond::Not { cond } => cond::Cond::Not(Box::new(NotCond {
                    cond: Some(Box::new(Cond::from(*cond))),
                })),
                database::Cond::Or { conds } => cond::Cond::Or(OrCond {
                    conds: conds.into_iter().map(|c| c.into()).collect(),
                }),
                database::Cond::And { conds } => cond::Cond::And(AndCond {
                    conds: conds.into_iter().map(|c| c.into()).collect(),
                }),
            };

            Self { cond: Some(cond) }
        }
    }
}

pub struct ProxyService {
    clients: RwLock<HashMap<Uuid, Arc<dyn Database>>>,
    factory: Arc<dyn DbFactory>,
    new_frame_notifier: watch::Receiver<FrameNo>,
}

impl ProxyService {
    pub fn new(factory: Arc<dyn DbFactory>, new_frame_notifier: watch::Receiver<FrameNo>) -> Self {
        Self {
            clients: Default::default(),
            factory,
            new_frame_notifier,
        }
    }
}

#[tonic::async_trait]
impl Proxy for ProxyService {
    async fn execute(
        &self,
        req: tonic::Request<rpc::ProgramReq>,
    ) -> Result<tonic::Response<ExecuteResults>, tonic::Status> {
        let req = req.into_inner();
        let pgm = Program::try_from(req.pgm.unwrap())
            .map_err(|e| tonic::Status::new(tonic::Code::InvalidArgument, e.to_string()))?;
        let client_id = Uuid::from_str(&req.client_id).unwrap();

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

        let auth = match req.authorized {
            Some(0) => Authenticated::Authorized(Authorized::ReadOnly),
            Some(1) => Authenticated::Authorized(Authorized::FullAccess),
            Some(_) => {
                return Err(tonic::Status::new(
                    tonic::Code::PermissionDenied,
                    "invalid authorization level",
                ))
            }
            None => Authenticated::Anonymous,
        };
        tracing::debug!("executing request for {client_id}");
        let (results, state) = db
            .execute_program(pgm, auth)
            .await
            .map_err(|e| tonic::Status::new(tonic::Code::PermissionDenied, e.to_string()))?;
        let results = results.into_iter().map(|r| r.into()).collect();
        let current_frame_no = *self.new_frame_notifier.borrow();

        Ok(tonic::Response::new(ExecuteResults {
            results,
            state: State::from(state).into(),
            current_frame_no,
        }))
    }

    //TODO: also handle cleanup on peer disconnect
    async fn disconnect(
        &self,
        msg: tonic::Request<DisconnectMessage>,
    ) -> Result<tonic::Response<Ack>, tonic::Status> {
        let DisconnectMessage { client_id } = msg.into_inner();
        let client_id = Uuid::from_str(&client_id).unwrap();

        tracing::debug!("disconnected: {client_id}");

        self.clients.write().await.remove(&client_id);

        Ok(tonic::Response::new(Ack {}))
    }
}
