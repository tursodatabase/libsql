mod hrana_over_http;
pub mod stats;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use bytes::{BufMut, Bytes, BytesMut};
use hyper::body::to_bytes;
use hyper::{Body, Method, Request, Response, StatusCode};
use serde::Serialize;
use serde_json::{json, Number};
use tokio::sync::{mpsc, oneshot};
use tonic::codegen::http;
use tower::ServiceBuilder;
use tower_http::trace::DefaultOnResponse;
use tower_http::{compression::CompressionLayer, cors};
use tracing::{Level, Span};

use crate::auth::{Auth, Authenticated};
use crate::database::factory::DbFactory;
use crate::error::Error;
use crate::hrana;
use crate::http::types::HttpQuery;
use crate::query::{self, Query, QueryResult, ResultSet};
use crate::query_analysis::{predict_final_state, State, Statement};
use crate::stats::Stats;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;

use self::types::QueryObject;

impl TryFrom<query::Value> for serde_json::Value {
    type Error = Error;

    fn try_from(value: query::Value) -> Result<Self, Self::Error> {
        let value = match value {
            query::Value::Null => serde_json::Value::Null,
            query::Value::Integer(i) => serde_json::Value::Number(Number::from(i)),
            query::Value::Real(x) => {
                serde_json::Value::Number(Number::from_f64(x).ok_or_else(|| {
                    Error::DbValueError(format!(
                        "Cannot to convert database value `{x}` to a JSON number"
                    ))
                })?)
            }
            query::Value::Text(s) => serde_json::Value::String(s),
            query::Value::Blob(v) => serde_json::json!({
                "base64": BASE64_STANDARD_NO_PAD.encode(v),
            }),
        };

        Ok(value)
    }
}

/// Encodes a query response rows into json
#[derive(Debug, Serialize)]
struct RowsResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    message: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
enum ResultResponse {
    Results(RowsResponse),
    Error(ErrorResponse),
}

fn query_response_to_json(results: Vec<Option<QueryResult>>) -> anyhow::Result<Bytes> {
    fn result_set_to_json(
        ResultSet { columns, rows, .. }: ResultSet,
    ) -> anyhow::Result<RowsResponse> {
        let mut out_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out_row = Vec::with_capacity(row.values.len());
            for value in row.values {
                out_row.push(value.try_into()?);
            }

            out_rows.push(out_row);
        }

        Ok(RowsResponse {
            columns: columns.into_iter().map(|c| c.name).collect(),
            rows: out_rows,
        })
    }

    let json = results
        .into_iter()
        .map(|r| match r {
            Some(Ok(query::QueryResponse::ResultSet(set))) => {
                Ok(Some(ResultResponse::Results(result_set_to_json(set)?)))
            }
            Some(Err(e)) => Ok(Some(ResultResponse::Error(ErrorResponse {
                message: e.to_string(),
            }))),
            None => Ok(None),
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let mut buffer = BytesMut::new().writer();
    serde_json::to_writer(&mut buffer, &json)?;
    Ok(buffer.into_inner().freeze())
}

fn error(msg: &str, code: StatusCode) -> Response<Body> {
    let err = json!({ "error": msg });
    Response::builder()
        .status(code)
        .body(Body::from(serde_json::to_vec(&err).unwrap()))
        .unwrap()
}

fn parse_queries(queries: Vec<QueryObject>) -> anyhow::Result<Vec<Query>> {
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut iter = Statement::parse(&query.q);
        let stmt = iter.next().transpose()?.unwrap_or_default();
        if iter.next().is_some() {
            anyhow::bail!(
                "found more than one command in a single statement string. It is allowed to issue only one command per string."
            );
        }
        let query = Query {
            stmt,
            params: query.params.0,
            want_rows: true,
        };

        out.push(query);
    }

    match predict_final_state(State::Init, out.iter().map(|q| &q.stmt)) {
        State::Txn => anyhow::bail!("interactive transaction not allowed in HTTP queries"),
        State::Init => (),
        // maybe we should err here, but let's sqlite deal with that.
        State::Invalid => (),
    }

    Ok(out)
}

fn parse_payload(data: &[u8]) -> Result<HttpQuery, Response<Body>> {
    match serde_json::from_slice(data) {
        Ok(data) => Ok(data),
        Err(e) => Err(error(&e.to_string(), http::status::StatusCode::BAD_REQUEST)),
    }
}

async fn handle_query(
    mut req: Request<Body>,
    auth: Authenticated,
    db_factory: Arc<dyn DbFactory>,
) -> anyhow::Result<Response<Body>> {
    let bytes = to_bytes(req.body_mut()).await?;
    let req = match parse_payload(&bytes) {
        Ok(req) => req,
        Err(resp) => return Ok(resp),
    };

    let batch = match parse_queries(req.statements) {
        Ok(queries) => queries,
        Err(e) => return Ok(error(&e.to_string(), StatusCode::BAD_REQUEST)),
    };

    let db = db_factory.create().await?;

    match db.execute_batch_or_rollback(batch, auth).await {
        Ok((rows, _)) => {
            let json = query_response_to_json(rows)?;
            Ok(Response::builder()
                .header("Content-Type", "application/json")
                .body(Body::from(json))?)
        }
        Err(e) => Ok(error(
            &format!("internal error: {e}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn show_console() -> anyhow::Result<Response<Body>> {
    Ok(Response::new(Body::from(std::include_str!("console.html"))))
}

fn handle_health() -> Response<Body> {
    // return empty OK
    Response::new(Body::empty())
}

async fn handle_upgrade(
    upgrade_tx: &mpsc::Sender<hrana::Upgrade>,
    req: Request<Body>,
) -> Response<Body> {
    let (response_tx, response_rx) = oneshot::channel();
    let _: Result<_, _> = upgrade_tx
        .send(hrana::Upgrade {
            request: req,
            response_tx,
        })
        .await;

    match response_rx.await {
        Ok(response) => response,
        Err(_) => Response::builder()
            .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
            .body("sqld was not able to process the HTTP upgrade".into())
            .unwrap(),
    }
}

async fn handle_request(
    auth: Arc<Auth>,
    req: Request<Body>,
    upgrade_tx: mpsc::Sender<hrana::Upgrade>,
    db_factory: Arc<dyn DbFactory>,
    enable_console: bool,
    stats: Stats,
) -> anyhow::Result<Response<Body>> {
    if hyper_tungstenite::is_upgrade_request(&req) {
        return Ok(handle_upgrade(&upgrade_tx, req).await);
    }

    let auth_header = req.headers().get(hyper::header::AUTHORIZATION);
    let auth = match auth.authenticate_http(auth_header) {
        Ok(auth) => auth,
        Err(err) => {
            return Ok(Response::builder()
                .status(hyper::StatusCode::UNAUTHORIZED)
                .body(err.to_string().into())
                .unwrap());
        }
    };

    match (req.method(), req.uri().path()) {
        (&Method::POST, "/") => handle_query(req, auth, db_factory.clone()).await,
        (&Method::GET, "/version") => Ok(handle_version()),
        (&Method::GET, "/console") if enable_console => show_console().await,
        (&Method::GET, "/health") => Ok(handle_health()),
        (&Method::GET, "/v1") => hrana_over_http::handle_index(req).await,
        (&Method::POST, "/v1/execute") => {
            hrana_over_http::handle_execute(db_factory, auth, req).await
        }
        (&Method::POST, "/v1/batch") => hrana_over_http::handle_batch(db_factory, auth, req).await,
        (&Method::GET, "/v1/stats") => Ok(stats::handle_stats(&stats)),
        _ => Ok(Response::builder().status(404).body(Body::empty()).unwrap()),
    }
}

fn handle_version() -> Response<Body> {
    let version = env!("CARGO_PKG_VERSION");
    Response::new(Body::from(version.as_bytes()))
}

// TODO: refactor
#[allow(clippy::too_many_arguments)]
pub async fn run_http(
    addr: SocketAddr,
    auth: Arc<Auth>,
    db_factory: Arc<dyn DbFactory>,
    upgrade_tx: mpsc::Sender<hrana::Upgrade>,
    enable_console: bool,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
) -> anyhow::Result<()> {
    tracing::info!("listening for HTTP requests on {addr}");

    fn trace_request<B>(req: &Request<B>, _span: &Span) {
        tracing::info!("got request: {} {}", req.method(), req.uri());
    }
    let service = ServiceBuilder::new()
        .option_layer(idle_shutdown_layer)
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        )
        .layer(CompressionLayer::new())
        .layer(
            cors::CorsLayer::new()
                .allow_methods(cors::AllowMethods::any())
                .allow_headers(cors::Any)
                .allow_origin(cors::Any),
        )
        .service_fn(move |req| {
            handle_request(
                auth.clone(),
                req,
                upgrade_tx.clone(),
                db_factory.clone(),
                enable_console,
                stats.clone(),
            )
        });

    let server = hyper::server::Server::bind(&addr).serve(tower::make::Shared::new(service));

    server.await.context("Http server exited with an error")?;

    Ok(())
}
