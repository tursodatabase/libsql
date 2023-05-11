use std::fmt::Debug;
use std::sync::Arc;

use bytes::Bytes;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribeResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::api::{ClientInfo, Type, DEFAULT_NAME};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::Describe;
use pgwire::messages::PgWireBackendMessage;

use crate::database::Database;
use crate::query::{Params, Query, QueryResponse, ResultSet, Value};
use crate::query_analysis::Statement;

/// This is a dummy handler, it's sole role is to send the response back to the client.
pub struct QueryHandler {
    database: Arc<dyn Database>,
    query_parser: Arc<NoopQueryParser>,
    portal_store: Arc<MemPortalStore<String>>,
}

impl QueryHandler {
    pub fn new(database: Arc<dyn Database>) -> Self {
        Self {
            database,
            query_parser: Arc::new(NoopQueryParser::new()),
            portal_store: Arc::new(MemPortalStore::new()),
        }
    }

    async fn handle_queries(
        &self,
        queries: Vec<Query>,
        col_defs: bool,
    ) -> PgWireResult<Vec<Response>> {
        let auth = crate::auth::Authenticated::Authorized(crate::auth::Authorized::FullAccess);
        //FIXME: handle poll_ready error
        match self.database.execute_batch(queries, auth).await {
            Ok((resp, _)) => {
                let ret = resp
                    .into_iter()
                    .map(|r| match r {
                        Some(Ok(QueryResponse::ResultSet(mut set))) => {
                            set.include_column_defs = col_defs;
                            set.into()
                        }
                        Some(Err(e)) => Response::Error(
                            ErrorInfo::new("ERROR".into(), "XX000".into(), e.to_string()).into(),
                        ),
                        None => ResultSet::empty(false).into(),
                    })
                    .collect();
                Ok(ret)
            }
            Err(e) => Err(PgWireError::ApiError(e.into())),
        }
    }
}

#[async_trait::async_trait]
impl SimpleQueryHandler for QueryHandler {
    async fn do_query<'q, 'b: 'q, C>(
        &'b self,
        _client: &C,
        query: &'q str,
    ) -> PgWireResult<Vec<Response<'q>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let queries = Statement::parse(query)
            .map(|s| {
                s.map(|stmt| Query {
                    stmt,
                    params: Params::empty(),
                    want_rows: true,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>();

        match queries {
            Ok(queries) => self.handle_queries(queries, true).await,
            Err(e) => Err(PgWireError::UserError(
                ErrorInfo::new("ERROR".to_string(), "XX000".to_string(), e.to_string()).into(),
            )),
        }
    }
}

const REQUEST_DESCRIBE: &str = "SQLD_REQUEST_DESCRIBE";

#[async_trait::async_trait]
impl ExtendedQueryHandler for QueryHandler {
    type Statement = String;
    type PortalStore = MemPortalStore<Self::Statement>;
    type QueryParser = NoopQueryParser;

    async fn do_query<'q, 'b: 'q, C>(
        &'b self,
        client: &mut C,
        portal: &'q Portal<String>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'q>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug_assert_eq!(
            portal.statement().parameter_types().len(),
            portal.parameter_len()
        );

        let stmt = Statement::parse(portal.statement().statement())
            .next()
            .transpose()
            .map_err(|e| {
                PgWireError::UserError(
                    ErrorInfo::new("ERROR".into(), "XX000".into(), e.to_string()).into(),
                )
            })?
            .unwrap_or_default();

        let params = parse_params(portal.statement().parameter_types(), portal.parameters());

        let query = Query {
            stmt,
            params,
            want_rows: true,
        };
        let include_col_defs = client.metadata_mut().remove(REQUEST_DESCRIBE).is_some();
        self.handle_queries(vec![query], include_col_defs)
            .await
            .map(|mut res| {
                assert_eq!(res.len(), 1);
                res.pop().unwrap()
            })
    }

    async fn do_describe<C>(
        &self,
        _client: &mut C,
        _stmt: &StoredStatement<Self::Statement>,
        _parameter_type_infer: bool,
    ) -> PgWireResult<DescribeResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        unreachable!()
    }

    async fn on_describe<C>(&self, client: &mut C, message: Describe) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let name = message.name().as_deref().unwrap_or(DEFAULT_NAME);
        if self.portal_store().get_portal(name).is_some() {
            client
                .metadata_mut()
                .insert(REQUEST_DESCRIBE.to_owned(), "on".to_owned());
        } else {
            return Err(PgWireError::PortalNotFound(name.to_owned()));
        }
        Ok(())
    }

    fn portal_store(&self) -> Arc<Self::PortalStore> {
        self.portal_store.clone()
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }
}

fn parse_params(types: &[Type], data: &[Option<Bytes>]) -> Params {
    let mut params = Vec::with_capacity(data.len());
    for (val, ty) in data.iter().zip(types) {
        let value = if val.is_none() {
            Value::Null
        } else if ty == &Type::VARCHAR || ty == &Type::TEXT {
            let s = String::from_utf8(val.as_ref().unwrap().to_vec()).unwrap();
            Value::Text(s)
        } else if ty == &Type::INT8 {
            let v = i64::from_be_bytes((val.as_ref().unwrap()[..8]).try_into().unwrap());
            Value::Integer(v)
        } else if ty == &Type::BYTEA {
            Value::Blob(val.as_ref().unwrap().to_vec())
        } else if ty == &Type::FLOAT8 {
            let val = f64::from_be_bytes(val.as_ref().unwrap()[..8].try_into().unwrap());
            Value::Real(val)
        } else {
            unimplemented!("unsupported type")
        };

        params.push(value);
    }

    Params::new_positional(params)
}
