use std::borrow::Cow;
use std::future::poll_fn;

use bytes::Buf;
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

use crate::query::{Query, QueryError, QueryResponse, Value};
use crate::server::AsyncPeekable;

// TODO: more robust parsing
static VAR_REPLACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\$(?P<digits>\d*)"#).unwrap());

/// This is a dummy handler, it's sole role is to send the response back to the client.
pub struct QueryHandler<'a, S>(Mutex<&'a mut S>);

impl<'a, S> QueryHandler<'a, S> {
    pub fn new(s: &'a mut S) -> Self {
        Self(Mutex::new(s))
    }

    async fn handle_query(&self, query: Cow<'_, str>, params: Vec<Value>) -> PgWireResult<Response>
    where
        S: Service<Query, Response = QueryResponse, Error = QueryError> + Sync + Send,
        S::Future: Send,
    {
        let query = Query::SimpleQuery(query.into_owned(), params);
        let mut s = self.0.lock().await;
        //TODO: handle poll_ready error
        poll_fn(|cx| s.poll_ready(cx)).await.unwrap();
        match self.0.lock().await.call(query).await {
            Ok(resp) => match resp {
                QueryResponse::ResultSet(set) => Ok(set.into()),
                QueryResponse::Ack => unreachable!(),
            },
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait::async_trait]
impl<'a, S> SimpleQueryHandler for QueryHandler<'a, S>
where
    S: Service<Query, Response = QueryResponse, Error = QueryError> + Sync + Send,
    S::Future: Send,
{
    async fn do_query<C>(&self, _client: &C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.handle_query(Cow::Borrowed(query), Vec::new())
            .await
            .map(|r| vec![r])
    }
}

#[async_trait::async_trait]
impl<'a, S> ExtendedQueryHandler for QueryHandler<'a, S>
where
    S: Service<Query, Response = QueryResponse, Error = QueryError> + Sync + Send,
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
        let statement = VAR_REPLACE_RE.replace(portal.statement(), "?$digits");
        let mut params = Vec::with_capacity(portal.parameter_len());
        for (val, ty) in portal.parameters().iter().zip(portal.parameter_types()) {
            let value = if val.is_none() {
                Value::Null
            } else if ty == &Type::VARCHAR {
                let s = String::from_utf8(val.as_ref().unwrap().to_vec()).unwrap();
                Value::Text(s)
            } else if ty == &Type::INT8 {
                let v = i64::from_be_bytes((val.as_ref().unwrap()[..8]).try_into().unwrap());
                Value::Integer(v)
            } else {
                todo!()
            };
            params.push(value);
        }

        self.handle_query(statement, params).await
    }
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
