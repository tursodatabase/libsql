use std::future::poll_fn;

use bytes::{Buf, Bytes};
use futures::{io, SinkExt};
use once_cell::sync::Lazy;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::{ReadyForQuery, READY_STATUS_IDLE};
use pgwire::messages::startup::SslRequest;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::PgWireMessageServerCodec;
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::codec::Framed;
use tower::Service;

use crate::query::{Queries, Query, QueryResponse, QueryResult, Value};
use crate::query_analysis::Statement;
use crate::server::AsyncPeekable;

// TODO: more robust parsing
static VAR_REPLACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\$(?P<digits>\d*)"#).unwrap());

/// This is a dummy handler, it's sole role is to send the response back to the client.
pub struct QueryHandler<'a, S>(Mutex<&'a mut S>);

impl<'a, S> QueryHandler<'a, S> {
    pub fn new(s: &'a mut S) -> Self {
        Self(Mutex::new(s))
    }

    async fn handle_queries(&self, queries: Queries) -> PgWireResult<Vec<Response>>
    where
        S: Service<Queries, Response = Vec<QueryResult>, Error = anyhow::Error> + Sync + Send,
        S::Future: Send,
    {
        let mut s = self.0.lock().await;
        //FIXME: handle poll_ready error
        poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
        match s.call(queries).await {
            Ok(responses) => Ok(responses
                .into_iter()
                .map(|r| match r {
                    Ok(QueryResponse::ResultSet(set)) => set.into(),
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
impl<'a, S> SimpleQueryHandler for QueryHandler<'a, S>
where
    S: Service<Queries, Response = Vec<QueryResult>, Error = anyhow::Error> + Sync + Send,
    S::Future: Send,
{
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let queries = Statement::parse(query)
            .map(|s| {
                s.map(|stmt| Query {
                    stmt,
                    params: Vec::new(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>();

        match queries {
            Ok(queries) => self.handle_queries(queries).await,
            Err(e) => Err(PgWireError::UserError(
                ErrorInfo::new("ERROR".to_string(), "XX000".to_string(), e.to_string()).into(),
            )),
        }
    }
}

#[async_trait::async_trait]
impl<'a, S> ExtendedQueryHandler for QueryHandler<'a, S>
where
    S: Service<Queries, Response = Vec<QueryResult>, Error = anyhow::Error> + Sync + Send,
    S::Future: Send,
{
    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        debug_assert_eq!(portal.parameter_types().len(), portal.parameter_len());

        let patched_statement = VAR_REPLACE_RE.replace(portal.statement(), "?$digits");
        let stmt = Statement::parse(&patched_statement)
            .next()
            .transpose()
            .map_err(|e| {
                PgWireError::UserError(
                    ErrorInfo::new("ERROR".into(), "XX000".into(), e.to_string()).into(),
                )
            })?
            .unwrap_or_default();

        let params = parse_params(portal.parameter_types(), portal.parameters());

        let query = Query { stmt, params };
        self.handle_queries(vec![query]).await.map(|mut res| {
            assert_eq!(res.len(), 1);
            res.pop().unwrap()
        })
    }
}

fn parse_params(types: &[Type], data: &[Option<Bytes>]) -> Vec<Value> {
    let mut params = Vec::with_capacity(types.len());
    for (val, ty) in data.iter().zip(types) {
        let value = if val.is_none() {
            Value::Null
        } else if ty == &Type::VARCHAR {
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

    params
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
