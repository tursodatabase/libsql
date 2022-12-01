use bytes::Buf;
use futures::{io, stream, SinkExt, StreamExt};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{text_query_response, FieldInfo, Response, TextDataRowEncoder};
use pgwire::api::{ClientInfo, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::{ReadyForQuery, READY_STATUS_IDLE};
use pgwire::messages::startup::SslRequest;
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::PgWireMessageServerCodec;
use rusqlite::types::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::Framed;

use crate::coordinator::query::{QueryResponse, QueryResult};
use crate::server::AsyncPeekable;

pub struct SimpleHandler(pub QueryResult);

#[async_trait::async_trait]
impl SimpleQueryHandler for SimpleHandler {
    async fn do_query<C>(&self, _client: &C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // TODO: find a way to prevent unecessary clones.
        match &self.0 {
            Ok(resp) => match resp {
                QueryResponse::ResultSet(col_names, rows) => {
                    let nr_cols = col_names.len();
                    let field_infos = col_names
                        .iter()
                        .map(move |(name, ty)| {
                            let ty = match ty {
                                Some(ty) => match ty.as_str() {
                                    "integer" => Type::INT8,
                                    "real" => Type::NUMERIC,
                                    "text" => Type::VARCHAR,
                                    "blob" => Type::BYTEA,
                                    _ => Type::UNKNOWN,
                                },
                                None => Type::UNKNOWN,
                            };
                            FieldInfo::new(name.into(), None, None, ty)
                        })
                        .collect();
                    let data_row_stream = stream::iter(rows.clone().into_iter()).map(move |row| {
                        let mut encoder = TextDataRowEncoder::new(nr_cols);
                        for col in &row {
                            match col {
                                Value::Null => {
                                    encoder.append_field(None::<&u8>)?;
                                }
                                Value::Integer(i) => {
                                    encoder.append_field(Some(&i))?;
                                }
                                Value::Real(f) => {
                                    encoder.append_field(Some(&f))?;
                                }
                                Value::Text(t) => {
                                    encoder.append_field(Some(&t))?;
                                }
                                Value::Blob(b) => {
                                    encoder.append_field(Some(&hex::encode(b)))?;
                                }
                            }
                        }
                        encoder.finish()
                    });
                    return Ok(vec![Response::Query(text_query_response(
                        field_infos,
                        data_row_stream,
                    ))]);
                }
                QueryResponse::Ack => return Ok(vec![]),
            },
            Err(e) => Err(e.clone().into()),
        }
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
