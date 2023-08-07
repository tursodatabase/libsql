mod h2c;
mod hrana_over_http_1;
mod result_builder;
pub mod stats;
mod types;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::{FromRef, FromRequest, FromRequestParts, State as AxumState};
use axum::http::request::Parts;
use axum::http::HeaderValue;
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::Router;
use axum_extra::middleware::option_layer;
use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use hyper::server::conn::AddrIncoming;
use hyper::{header, Body, Request, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Number;
use tokio::sync::{mpsc, oneshot};
use tonic::transport::Server;
use tower_http::trace::DefaultOnResponse;
use tower_http::{compression::CompressionLayer, cors};
use tracing::{Level, Span};

use crate::auth::{Auth, Authenticated};
use crate::database::factory::DbFactory;
use crate::database::Database;
use crate::error::Error;
use crate::hrana;
use crate::http::types::HttpQuery;
use crate::query::{self, Query};
use crate::query_analysis::{predict_final_state, State, Statement};
use crate::query_result_builder::QueryResultBuilder;
use crate::replication::ReplicationLogger;
use crate::rpc::replication_log::ReplicationLogService;
use crate::stats::Stats;
use crate::utils::services::idle_shutdown::IdleShutdownLayer;
use crate::version;

use self::result_builder::JsonHttpPayloadBuilder;
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

fn parse_queries(queries: Vec<QueryObject>) -> crate::Result<Vec<Query>> {
    let mut out = Vec::with_capacity(queries.len());
    for query in queries {
        let mut iter = Statement::parse(&query.q);
        let stmt = iter.next().transpose()?.unwrap_or_default();
        if iter.next().is_some() {
            return Err(Error::FailedToParse("found more than one command in a single statement string. It is allowed to issue only one command per string.".to_string()));
        }
        let query = Query {
            stmt,
            params: query.params.0,
            want_rows: true,
        };

        out.push(query);
    }

    match predict_final_state(State::Init, out.iter().map(|q| &q.stmt)) {
        State::Txn => {
            return Err(Error::QueryError(
                "interactive transaction not allowed in HTTP queries".to_string(),
            ))
        }
        State::Init => (),
        // maybe we should err here, but let's sqlite deal with that.
        State::Invalid => (),
    }

    Ok(out)
}

async fn handle_query<D: Database>(
    auth: Authenticated,
    AxumState(state): AxumState<AppState<D>>,
    Json(query): Json<HttpQuery>,
) -> Result<axum::response::Response, Error> {
    let AppState { db_factory, .. } = state;

    let batch = parse_queries(query.statements)?;

    let db = db_factory.create().await?;

    let builder = JsonHttpPayloadBuilder::new();
    let (builder, _) = db.execute_batch_or_rollback(batch, auth, builder).await?;

    let res = (
        [(header::CONTENT_TYPE, "application/json")],
        builder.into_ret(),
    );
    Ok(res.into_response())
}

async fn show_console<D>(
    AxumState(AppState { enable_console, .. }): AxumState<AppState<D>>,
) -> impl IntoResponse {
    if enable_console {
        Html(std::include_str!("console.html")).into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}

async fn handle_health() -> Response<Body> {
    // return empty OK
    Response::new(Body::empty())
}

async fn handle_upgrade<D>(
    AxumState(AppState { upgrade_tx, .. }): AxumState<AppState<D>>,
    req: Request<Body>,
) -> impl IntoResponse {
    if !hyper_tungstenite::is_upgrade_request(&req) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let (response_tx, response_rx) = oneshot::channel();
    let _: Result<_, _> = upgrade_tx
        .send(hrana::ws::Upgrade {
            request: req,
            response_tx,
        })
        .await;

    match response_rx.await {
        Ok(response) => response.into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "sqld was not able to process the HTTP upgrade",
        )
            .into_response(),
    }
}

async fn handle_version() -> Response<Body> {
    let version = version::version();
    Response::new(Body::from(version))
}

async fn handle_hrana_v2<D: Database>(
    AxumState(state): AxumState<AppState<D>>,
    auth: Authenticated,
    req: Request<Body>,
) -> Result<Response<Body>, Error> {
    let server = state.hrana_http_srv;

    let res = server.handle_pipeline(auth, req).await?;

    Ok(res)
}

async fn handle_fallback() -> impl IntoResponse {
    (StatusCode::NOT_FOUND).into_response()
}

/// Router wide state that each request has access too via
/// axum's `State` extractor.
pub(crate) struct AppState<D> {
    auth: Arc<Auth>,
    db_factory: Arc<dyn DbFactory<Db = D>>,
    upgrade_tx: mpsc::Sender<hrana::ws::Upgrade>,
    hrana_http_srv: Arc<hrana::http::Server<D>>,
    enable_console: bool,
    stats: Stats,
}

impl<D> Clone for AppState<D> {
    fn clone(&self) -> Self {
        Self {
            auth: self.auth.clone(),
            db_factory: self.db_factory.clone(),
            upgrade_tx: self.upgrade_tx.clone(),
            hrana_http_srv: self.hrana_http_srv.clone(),
            enable_console: self.enable_console,
            stats: self.stats.clone(),
        }
    }
}

// TODO: refactor
#[allow(clippy::too_many_arguments)]
pub async fn run_http<D: Database>(
    addr: SocketAddr,
    auth: Arc<Auth>,
    db_factory: Arc<dyn DbFactory<Db = D>>,
    upgrade_tx: mpsc::Sender<hrana::ws::Upgrade>,
    hrana_http_srv: Arc<hrana::http::Server<D>>,
    enable_console: bool,
    idle_shutdown_layer: Option<IdleShutdownLayer>,
    stats: Stats,
    logger: Option<Arc<ReplicationLogger>>,
) -> anyhow::Result<()> {
    let state = AppState {
        auth,
        db_factory,
        upgrade_tx,
        hrana_http_srv,
        enable_console,
        stats,
    };

    tracing::info!("listening for HTTP requests on {addr}");

    fn trace_request<B>(req: &Request<B>, _span: &Span) {
        tracing::debug!("got request: {} {}", req.method(), req.uri());
    }

    let app = Router::new()
        .route("/", post(handle_query))
        .route("/", get(handle_upgrade))
        .route("/version", get(handle_version))
        .route("/console", get(show_console))
        .route("/health", get(handle_health))
        .route("/v1/stats", get(stats::handle_stats))
        .route("/v1", get(hrana_over_http_1::handle_index))
        .route("/v1/execute", post(hrana_over_http_1::handle_execute))
        .route("/v1/batch", post(hrana_over_http_1::handle_batch))
        .route("/v2", get(crate::hrana::http::handle_index))
        .route("/v2/pipeline", post(handle_hrana_v2))
        .with_state(state);

    let layered_app = app
        .layer(option_layer(idle_shutdown_layer.clone()))
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
        );

    // Merge the grpc based axum router into our regular http router
    let router = if let Some(logger) = logger {
        let logger_rpc = ReplicationLogService::new(logger, idle_shutdown_layer);
        let grpc_router = Server::builder()
            .add_service(crate::rpc::ReplicationLogServer::new(logger_rpc))
            .into_router();

        layered_app.merge(grpc_router)
    } else {
        layered_app
    };

    let router = router.fallback(handle_fallback);
    let h2c = h2c::H2cMaker::new(router);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    hyper::server::Server::builder(AddrIncoming::from_listener(listener)?)
        .tcp_nodelay(true)
        .serve(h2c)
        .await
        .context("http server")?;

    Ok(())
}

/// Axum authenticated extractor
#[tonic::async_trait]
impl<S> FromRequestParts<S> for Authenticated
where
    Arc<Auth>: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = Error;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth = <Arc<Auth> as FromRef<S>>::from_ref(state);

        let auth_header = parts.headers.get(hyper::header::AUTHORIZATION);
        let auth = auth.authenticate_http(auth_header)?;

        Ok(auth)
    }
}

impl<D> FromRef<AppState<D>> for Arc<Auth> {
    fn from_ref(input: &AppState<D>) -> Self {
        input.auth.clone()
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct Json<T>(pub T);

#[tonic::async_trait]
impl<S, T, B> FromRequest<S, B> for Json<T>
where
    T: DeserializeOwned,
    B: hyper::body::HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    S: Send + Sync,
{
    type Rejection = axum::extract::rejection::JsonRejection;

    async fn from_request(mut req: Request<B>, state: &S) -> Result<Self, Self::Rejection> {
        let headers = req.headers_mut();

        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );

        axum::Json::from_request(req, state)
            .await
            .map(|t| Json(t.0))
    }
}
