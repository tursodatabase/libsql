use std::fmt::Debug;
use std::future::poll_fn;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures::{io, Sink, SinkExt};
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribeResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::{MemPortalStore, PortalStore};
use pgwire::api::{ClientInfo, Type, DEFAULT_NAME};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::Describe;
use pgwire::messages::response::{ReadyForQuery, READY_STATUS_IDLE};
use pgwire::messages::startup::SslRequest;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::PgWireMessageServerCodec;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tower::Service;

use crate::error::Error;
use crate::query::{Params, Queries, Query, QueryResponse, QueryResult, Value};
use crate::query_analysis::Statement;
use crate::server::AsyncPeekable;

/// This is a dummy handler, it's sole role is to send the response back to the client.
pub struct QueryHandler<S> {
    state: Arc<Mutex<S>>,
    query_parser: Arc<NoopQueryParser>,
    portal_store: Arc<MemPortalStore<String>>,
}

impl<'a, S> QueryHandler<S> {
    pub fn new(s: Arc<Mutex<S>>) -> Self {
        Self {
            state: s,
            query_parser: Arc::new(NoopQueryParser::new()),
            portal_store: Arc::new(MemPortalStore::new()),
        }
    }

    async fn handle_queries(&self, queries: Queries, col_defs: bool) -> PgWireResult<Vec<Response>>
    where
        S: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
        S::Future: Send,
    {
        let mut s = self.state.lock().await;
        //FIXME: handle poll_ready error
        poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
        match s.call(queries).await {
            Ok(responses) => Ok(responses
                .into_iter()
                .map(|r| match r {
                    Ok(QueryResponse::ResultSet(mut set)) => {
                        set.include_column_defs = col_defs;
                        set.into()
                    }
                    Err(e) => Response::Error(
                        ErrorInfo::new("ERROR".into(), "XX000".into(), e.to_string()).into(),
                    ),
                })
                .collect()),

            Err(e) => Err(PgWireError::ApiError(e.into())),
        }
    }
}

#[async_trait::async_trait]
impl<S> SimpleQueryHandler for QueryHandler<S>
where
    S: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
    S::Future: Send,
{
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
impl<S> ExtendedQueryHandler for QueryHandler<S>
where
    S: Service<Queries, Response = Vec<QueryResult>, Error = Error> + Sync + Send,
    S::Future: Send,
{
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

        let query = Query { stmt, params };
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

// from https://docs.rs/pgwire/latest/src/pgwire/tokio.rs.html#230-283
pub async fn process_error<S>(
    socket: &mut Framed<S, PgWireMessageServerCodec>,
    error: PgWireError,
) -> Result<(), io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    match error {
        PgWireError::UserError(error_info) => {
            socket
                .feed(PgWireBackendMessage::ErrorResponse((*error_info).into()))
                .await?;

            socket
                .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    READY_STATUS_IDLE,
                )))
                .await?;
            socket.flush().await?;
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            socket
                .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    READY_STATUS_IDLE,
                )))
                .await?;
            socket.flush().await?;
        }
        _ => {
            // Internal error
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string());
            socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            socket.close().await?;
        }
    }

    Ok(())
}

pub async fn peek_for_sslrequest<I>(socket: &mut I, ssl_supported: bool) -> Result<bool, io::Error>
where
    I: AsyncWrite + AsyncRead + AsyncPeekable + Unpin,
{
    let mut ssl = false;
    let mut buf = [0u8; SslRequest::BODY_SIZE];
    loop {
        let size = socket.peek(&mut buf).await?;
        if size == 0 {
            break;
        }
        if size == SslRequest::BODY_SIZE {
            let mut buf_ref = buf.as_ref();
            // skip first 4 bytes
            buf_ref.get_i32();
            if buf_ref.get_i32() == SslRequest::BODY_MAGIC_NUMBER {
                // the socket is sending sslrequest, read the first 8 bytes
                // skip first 8 bytes
                socket.read_exact(&mut [0u8; SslRequest::BODY_SIZE]).await?;
                // ssl configured
                if ssl_supported {
                    ssl = true;
                    socket.write_all(b"S").await?;
                } else {
                    socket.write_all(b"N").await?;
                }
            }
            break;
        }
    }

    Ok(ssl)
}
