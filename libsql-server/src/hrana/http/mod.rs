use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::Stream;
use http_body_util::{BodyExt, Full};
use libsql_hrana::proto;
use parking_lot::Mutex;
use serde::{de::DeserializeOwned, Serialize};
use std::pin::Pin;
use std::sync::Arc;
use std::task;

use super::{batch, cursor, Encoding, ProtocolError, Version};
use crate::connection::{MakeConnection, RequestContext};
use crate::database::Connection;
use crate::hrana::http::stream::StreamError;

mod request;
pub(crate) mod stream;

pub struct Server {
    self_url: Option<String>,
    baton_key: [u8; 32],
    stream_state: Mutex<stream::ServerStreamState>,
}

#[derive(Debug, Copy, Clone)]
pub enum Endpoint {
    Pipeline,
    Cursor,
}

impl Server {
    pub fn new(self_url: Option<String>) -> Self {
        Self {
            self_url,
            baton_key: rand::random(),
            stream_state: Mutex::new(stream::ServerStreamState::new()),
        }
    }

    pub async fn run_expire(&self) {
        stream::run_expire(self).await
    }

    pub async fn handle_request(
        &self,
        connection_maker: Arc<dyn MakeConnection<Connection = Connection>>,
        ctx: RequestContext,
        req: http::Request<axum::body::Body>,
        endpoint: Endpoint,
        version: Version,
        encoding: Encoding,
    ) -> Result<http::Response<Full<Bytes>>> {
        handle_request(
            self,
            connection_maker,
            ctx,
            req,
            endpoint,
            version,
            encoding,
        )
        .await
        .map_err(|e| {
            tracing::error!("hrana server: {}", e);
            e
        })
        .or_else(|err| {
            err.downcast::<StreamError>()
                .map(|err| stream_error_response(err, encoding))
        })
        .or_else(|err| err.downcast::<ProtocolError>().map(protocol_error_response))
    }

    pub(crate) fn stream_state(&self) -> &Mutex<stream::ServerStreamState> {
        &self.stream_state
    }
}

pub(crate) async fn handle_index() -> http::Response<Full<Bytes>> {
    text_response(
        http::StatusCode::OK,
        "Hello, this is HTTP API v2 (Hrana over HTTP)".into(),
    )
}

async fn handle_request(
    server: &Server,
    connection_maker: Arc<dyn MakeConnection<Connection = Connection>>,
    ctx: RequestContext,
    req: http::Request<axum::body::Body>,
    endpoint: Endpoint,
    version: Version,
    encoding: Encoding,
) -> Result<http::Response<Full<Bytes>>> {
    match endpoint {
        Endpoint::Pipeline => {
            handle_pipeline(server, connection_maker, ctx, req, version, encoding).await
        }
        Endpoint::Cursor => {
            handle_cursor(server, connection_maker, ctx, req, version, encoding).await
        }
    }
}

async fn handle_pipeline(
    server: &Server,
    connection_maker: Arc<dyn MakeConnection<Connection = Connection>>,
    ctx: RequestContext,
    req: http::Request<axum::body::Body>,
    version: Version,
    encoding: Encoding,
) -> Result<http::Response<Full<Bytes>>> {
    let req_body: proto::PipelineReqBody = read_decode_request(req, encoding).await?;
    let mut stream_guard =
        stream::acquire(server, connection_maker, req_body.baton.as_deref()).await?;

    let mut results = Vec::with_capacity(req_body.requests.len());
    for request in req_body.requests.into_iter() {
        tracing::debug!("pipeline:{{ {:?}, {:?} }}", version, request);
        let result = request::handle(&mut stream_guard, ctx.clone(), request, version).await?;
        results.push(result);
    }

    let resp_body = proto::PipelineRespBody {
        baton: stream_guard.release(),
        base_url: server.self_url.clone(),
        results,
    };
    Ok(encode_response(http::StatusCode::OK, &resp_body, encoding))
}

async fn handle_cursor(
    server: &Server,
    connection_maker: Arc<dyn MakeConnection<Connection = Connection>>,
    ctx: RequestContext,
    req: http::Request<axum::body::Body>,
    version: Version,
    encoding: Encoding,
) -> Result<http::Response<Full<Bytes>>> {
    let req_body: proto::CursorReqBody = read_decode_request(req, encoding).await?;
    let stream_guard = stream::acquire(server, connection_maker, req_body.baton.as_deref()).await?;

    let mut join_set = tokio::task::JoinSet::new();
    let mut cursor_hnd = cursor::CursorHandle::spawn(&mut join_set);
    let db = stream_guard.get_db_owned()?;
    let sqls = stream_guard.sqls();
    let pgm = batch::proto_batch_to_program(&req_body.batch, sqls, version)?;
    cursor_hnd.open(db, ctx, pgm, req_body.batch.replication_index);

    let resp_body = proto::CursorRespBody {
        baton: stream_guard.release(),
        base_url: server.self_url.clone(),
    };
    
    // In hyper 1.0, we need to collect the cursor stream into bytes
    // This is a simplified approach - collect all chunks
    let mut all_bytes = Vec::new();
    
    // First chunk is the resp_body
    all_bytes.extend_from_slice(&encode_stream_item(&resp_body, encoding));
    
    // Then poll the cursor for more entries
    let cursor_stream = CursorStream {
        resp_body: None,
        join_set,
        cursor_hnd,
        encoding,
    };
    
    use futures::stream::StreamExt;
    let chunks: Vec<_> = cursor_stream.collect().await;
    for chunk in chunks {
        all_bytes.extend_from_slice(&chunk?);
    }
    
    let body = Full::new(Bytes::from(all_bytes));
    let content_type = match encoding {
        Encoding::Json => "text/plain",
        Encoding::Protobuf => "application/octet-stream",
    };

    Ok(http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(body)
        .unwrap())
}

struct CursorStream {
    resp_body: Option<proto::CursorRespBody>,
    join_set: tokio::task::JoinSet<()>,
    cursor_hnd: cursor::CursorHandle,
    encoding: Encoding,
}

impl Stream for CursorStream {
    type Item = Result<Bytes>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut task::Context,
    ) -> task::Poll<Option<Result<Bytes>>> {
        let this = self.get_mut();

        if let Some(resp_body) = this.resp_body.take() {
            let chunk = encode_stream_item(&resp_body, this.encoding);
            return task::Poll::Ready(Some(Ok(chunk)));
        }

        match this.join_set.poll_join_next(cx) {
            task::Poll::Pending => {}
            task::Poll::Ready(Some(Ok(()))) => {}
            task::Poll::Ready(Some(Err(err))) => panic!("Cursor task crashed: {}", err),
            task::Poll::Ready(None) => {}
        };

        match this.cursor_hnd.poll_fetch(cx) {
            task::Poll::Pending => task::Poll::Pending,
            task::Poll::Ready(None) => task::Poll::Ready(None),
            task::Poll::Ready(Some(Ok(entry))) => {
                let chunk = encode_stream_item(&entry.entry, this.encoding);
                task::Poll::Ready(Some(Ok(chunk)))
            }
            task::Poll::Ready(Some(Err(err))) => task::Poll::Ready(Some(Err(err))),
        }
    }
}

fn encode_stream_item<T: Serialize + prost::Message>(item: &T, encoding: Encoding) -> Bytes {
    let mut data: Vec<u8>;
    match encoding {
        Encoding::Json => {
            data = serde_json::to_vec(item).unwrap();
            data.push(b'\n');
        }
        Encoding::Protobuf => {
            data = <T as prost::Message>::encode_length_delimited_to_vec(item);
        }
    }
    Bytes::from(data)
}

async fn read_decode_request<T: DeserializeOwned + prost::Message + Default>(
    req: http::Request<axum::body::Body>,
    encoding: Encoding,
) -> Result<T> {
    let collected = req.into_body().collect().await
        .context("Could not read request body")?;
    let req_body = collected.to_bytes();
    match encoding {
        Encoding::Json => serde_json::from_slice(&req_body)
            .map_err(|err| ProtocolError::JsonDeserialize { source: err })
            .context("Could not deserialize JSON request body"),
        Encoding::Protobuf => <T as prost::Message>::decode(req_body)
            .map_err(|err| ProtocolError::ProtobufDecode { source: err })
            .context("Could not decode Protobuf request body"),
    }
}

fn protocol_error_response(err: ProtocolError) -> http::Response<Full<Bytes>> {
    text_response(http::StatusCode::BAD_REQUEST, err.to_string())
}

fn stream_error_response(err: StreamError, encoding: Encoding) -> http::Response<Full<Bytes>> {
    let status = match err {
        StreamError::StreamExpired => http::StatusCode::BAD_REQUEST,
    };
    encode_response(
        status,
        &proto::Error {
            message: err.to_string(),
            code: err.code().into(),
        },
        encoding,
    )
}

fn encode_response<T: Serialize + prost::Message>(
    status: http::StatusCode,
    resp_body: &T,
    encoding: Encoding,
) -> http::Response<Full<Bytes>> {
    let (resp_body, content_type) = match encoding {
        Encoding::Json => (serde_json::to_vec(resp_body).unwrap(), "application/json"),
        Encoding::Protobuf => (
            <T as prost::Message>::encode_to_vec(resp_body),
            "application/x-protobuf",
        ),
    };
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from(resp_body)))
        .unwrap()
}

fn text_response(status: http::StatusCode, resp_body: String) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "text/plain")
        .body(Full::new(Bytes::from(resp_body)))
        .unwrap()
}
