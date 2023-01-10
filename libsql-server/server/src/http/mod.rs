use std::collections::HashMap;
use std::future::poll_fn;
use std::{convert::Infallible, net::SocketAddr};

use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use hyper::body::to_bytes;
use hyper::server::conn::AddrStream;
use hyper::service::make_service_fn;
use hyper::{Body, Method, Request, Response};
use serde::Deserialize;
use serde_json::{json, Number};
use tokio::sync::{mpsc, oneshot};
use tower::balance::pool;
use tower::load::Load;
use tower::{service_fn, BoxError, MakeService, Service};

use crate::query::{self, Query, QueryError, QueryResponse, ResultSet};

impl TryFrom<query::Value> for serde_json::Value {
    type Error = anyhow::Error;

    fn try_from(value: query::Value) -> Result<Self, Self::Error> {
        let value = match value {
            query::Value::Null => serde_json::Value::Null,
            query::Value::Integer(i) => serde_json::Value::Number(Number::from(i)),
            query::Value::Real(x) => serde_json::Value::Number(
                Number::from_f64(x).ok_or_else(|| anyhow::anyhow!("invalid float value"))?,
            ),
            query::Value::Text(s) => serde_json::Value::String(s),
            query::Value::Blob(v) => serde_json::Value::String(BASE64_STANDARD_NO_PAD.encode(v)),
        };

        Ok(value)
    }
}

fn query_response_to_json(rows: QueryResponse) -> anyhow::Result<Bytes> {
    let QueryResponse::ResultSet(ResultSet { columns, rows }) = rows;
    let mut values = Vec::with_capacity(rows.len());
    for row in rows {
        let val = row
            .values
            .into_iter()
            .zip(columns.iter().map(|c| &c.name))
            .try_fold(
                HashMap::<_, serde_json::Value>::new(),
                |mut map, (value, name)| -> anyhow::Result<_> {
                    map.insert(name.to_string(), value.try_into()?);
                    Ok(map)
                },
            )?;

        values.push(val);
    }

    let mut buffer = BytesMut::new().writer();
    serde_json::to_writer(&mut buffer, &values)?;

    Ok(buffer.into_inner().freeze())
}

fn error(msg: &str, code: u16) -> Response<Body> {
    let err = json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

async fn handle_query(
    mut req: Request<Body>,
    sender: mpsc::Sender<(oneshot::Sender<Result<QueryResponse, BoxError>>, Query)>,
) -> anyhow::Result<Response<Body>> {
    let bytes = to_bytes(req.body_mut()).await?;
    let req: HttpQueryRequest = serde_json::from_slice(&bytes)?;
    let (s, resp) = oneshot::channel();
    // TODO: send query batch instead
    let _ = sender
        .send((s, Query::SimpleQuery(req.statements.join(";"), Vec::new())))
        .await;

    let result = resp.await;
    match result {
        Ok(Ok(rows)) => {
            let json = query_response_to_json(rows)?;
            Ok(Response::new(Body::from(json)))
        }
        Ok(Err(err)) => Ok(error(&err.to_string(), 400)),
        Err(_) => Ok(error("internal error", 500)),
    }
}

async fn handle_request(
    req: Request<Body>,
    sender: mpsc::Sender<(oneshot::Sender<Result<QueryResponse, BoxError>>, Query)>,
) -> anyhow::Result<Response<Body>> {
    match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => handle_query(req, sender).await,
        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
    }
}

pub async fn run_http<F>(addr: SocketAddr, db_factory: F) -> anyhow::Result<()>
where
    F: MakeService<(), Query> + Send + 'static,
    F::Service: Load + Service<Query, Response = QueryResponse, Error = QueryError>,
    <F::Service as Load>::Metric: std::fmt::Debug,
    F::MakeError: Into<BoxError>,
    F::Error: Into<BoxError>,
    <F as MakeService<(), Query>>::Service: Send,
    <F as MakeService<(), Query>>::Future: Send,
    <<F as MakeService<(), Query>>::Service as Service<Query>>::Future: Send,
{
    tracing::info!("listening for HTTP requests on {addr}");

    let (sender, mut receiver) = mpsc::channel(1024);
    let server =
        hyper::server::Server::bind(&addr).serve(make_service_fn(move |_: &AddrStream| {
            let sender = sender.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| handle_request(req, sender.clone())))
            }
        }));

    tokio::spawn(async move {
        let mut pool = pool::Builder::new().build(db_factory, ());
        while let Some((resp, query)) = receiver.recv().await {
            if let Err(e) = poll_fn(|c| pool.poll_ready(c)).await {
                tracing::error!("Connection pool error: {e}");
                continue;
            }

            let fut = pool.call(query);
            tokio::spawn(async move {
                let _ = resp.send(fut.await);
            });
        }
    });

    server.await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct HttpQueryRequest {
    statements: Vec<String>,
}
