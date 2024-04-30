use anyhow::Context as _;
use axum::extract::{FromRef, Path, State};
use axum::routing::delete;
use axum::Json;
use chrono::NaiveDateTime;
use futures::TryStreamExt;
use hyper::{Body, Request};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::cell::OnceCell;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::io::ReaderStream;
use tower_http::trace::DefaultOnResponse;
use url::Url;

use crate::auth::parse_jwt_key;
use crate::connection::config::DatabaseConfig;
use crate::error::{Error, LoadDumpError};
use crate::hrana;
use crate::namespace::{DumpStream, NamespaceName, NamespaceStore, RestoreOption};
use crate::net::Connector;
use crate::LIBSQL_PAGE_SIZE;

pub mod stats;

#[derive(Clone)]
struct Metrics {
    handle: Option<PrometheusHandle>,
}

impl Metrics {
    fn render(&self) -> String {
        self.handle.as_ref().map(|h| h.render()).unwrap_or_default()
    }
}

struct AppState<C> {
    namespaces: NamespaceStore,
    user_http_server: Arc<hrana::http::Server>,
    connector: C,
    metrics: Metrics,
}

impl<C> FromRef<Arc<AppState<C>>> for Metrics {
    fn from_ref(input: &Arc<AppState<C>>) -> Self {
        input.metrics.clone()
    }
}

static PROM_HANDLE: Mutex<OnceCell<PrometheusHandle>> = Mutex::new(OnceCell::new());

pub async fn run<A, C>(
    acceptor: A,
    user_http_server: Arc<hrana::http::Server>,
    namespaces: NamespaceStore,
    connector: C,
    disable_metrics: bool,
    shutdown: Arc<Notify>,
) -> anyhow::Result<()>
where
    A: crate::net::Accept,
    C: Connector,
{
    let app_label = std::env::var("SQLD_APP_LABEL").ok();
    let ver = env!("CARGO_PKG_VERSION");

    let prom_handle = if !disable_metrics {
        let lock = PROM_HANDLE.lock();
        let prom_handle = lock.get_or_init(|| {
            tracing::info!("initializing prometheus metrics");
            let b = PrometheusBuilder::new().idle_timeout(
                metrics_util::MetricKindMask::ALL,
                Some(Duration::from_secs(120)),
            );

            if let Some(app_label) = app_label {
                b.add_global_label("app", app_label)
                    .add_global_label("version", ver)
                    .install_recorder()
                    .unwrap()
            } else {
                b.install_recorder().unwrap()
            }
        });

        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                crate::metrics::SERVER_COUNT.set(1.0);
            }
        });

        Some(prom_handle.clone())
    } else {
        None
    };

    fn trace_request<B>(req: &Request<B>, span: &tracing::Span) {
        let _s = span.enter();

        tracing::debug!("{} {} {:?}", req.method(), req.uri(), req.headers());
    }

    metrics::increment_counter!("libsql_server_count");

    use axum::routing::{get, post};
    let metrics = Metrics {
        handle: prom_handle,
    };
    let router = axum::Router::new()
        .route("/", get(handle_get_index))
        .route(
            "/v1/namespaces/:namespace/config",
            get(handle_get_config).post(handle_post_config),
        )
        .route(
            "/v1/namespaces/:namespace/fork/:to",
            post(handle_fork_namespace),
        )
        .route(
            "/v1/namespaces/:namespace/create",
            post(handle_create_namespace),
        )
        .route("/v1/namespaces/:namespace", delete(handle_delete_namespace))
        .route("/v1/namespaces/:namespace/stats", get(stats::handle_stats))
        .route(
            "/v1/namespaces/:namespace/stats/:stats_type",
            delete(stats::handle_delete_stats),
        )
        .route("/v1/diagnostics", get(handle_diagnostics))
        .route("/metrics", get(handle_metrics))
        .with_state(Arc::new(AppState {
            namespaces,
            connector,
            user_http_server,
            metrics,
        }))
        .layer(
            tower_http::trace::TraceLayer::new_for_http()
                .on_request(trace_request)
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::DEBUG)
                        .latency_unit(tower_http::LatencyUnit::Micros),
                ),
        );

    hyper::server::Server::builder(acceptor)
        .serve(router.into_make_service())
        .with_graceful_shutdown(shutdown.notified())
        .await
        .context("Could not bind admin HTTP API server")?;

    Ok(())
}

async fn handle_get_index() -> &'static str {
    "Welcome to the sqld admin API"
}

async fn handle_metrics(State(metrics): State<Metrics>) -> String {
    metrics.render()
}

async fn handle_get_config<C: Connector>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<HttpDatabaseConfig>> {
    let store = app_state
        .namespaces
        .config_store(NamespaceName::from_string(namespace)?)
        .await?;
    let config = store.get();
    let max_db_size = bytesize::ByteSize::b(config.max_db_pages * LIBSQL_PAGE_SIZE);
    let resp = HttpDatabaseConfig {
        block_reads: config.block_reads,
        block_writes: config.block_writes,
        block_reason: config.block_reason.clone(),
        max_db_size: Some(max_db_size),
        heartbeat_url: config.heartbeat_url.clone().map(|u| u.into()),
        jwt_key: config.jwt_key.clone(),
        allow_attach: config.allow_attach,
        txn_timeout_s: config.txn_timeout.map(|d| d.as_secs() as u64),
    };

    Ok(Json(resp))
}

async fn handle_diagnostics<C>(
    State(app_state): State<Arc<AppState<C>>>,
) -> crate::Result<Json<Vec<String>>> {
    use crate::connection::Connection;
    use hrana::http::stream;

    let server = app_state.user_http_server.as_ref();
    let stream_state = server.stream_state().lock();
    let handles = stream_state.handles();
    let mut diagnostics: Vec<String> = Vec::with_capacity(handles.len());
    for handle in handles.values() {
        let handle_info: String = match handle {
            stream::Handle::Available(stream) => match &stream.db {
                Some(db) => db.diagnostics(),
                None => "[BUG] available-but-closed".into(),
            },
            stream::Handle::Acquired => "acquired".into(),
            stream::Handle::Expired => "expired".into(),
        };
        diagnostics.push(handle_info);
    }
    drop(stream_state);

    tracing::trace!("diagnostics: {diagnostics:?}");
    Ok(Json(diagnostics))
}

#[derive(Debug, Deserialize, Serialize)]
struct HttpDatabaseConfig {
    block_reads: bool,
    block_writes: bool,
    #[serde(default)]
    block_reason: Option<String>,
    #[serde(default)]
    max_db_size: Option<bytesize::ByteSize>,
    #[serde(default)]
    heartbeat_url: Option<String>,
    #[serde(default)]
    jwt_key: Option<String>,
    #[serde(default)]
    allow_attach: bool,
    #[serde(default)]
    txn_timeout_s: Option<u64>,
}

async fn handle_post_config<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<String>,
    Json(req): Json<HttpDatabaseConfig>,
) -> crate::Result<()> {
    if let Some(jwt_key) = req.jwt_key.as_deref() {
        // Check that the jwt key is correct
        parse_jwt_key(jwt_key)?;
    }
    let store = app_state
        .namespaces
        .config_store(NamespaceName::from_string(namespace)?)
        .await?;
    let mut config = (*store.get()).clone();
    config.block_reads = req.block_reads;
    config.block_writes = req.block_writes;
    config.block_reason = req.block_reason;
    config.allow_attach = req.allow_attach;
    config.txn_timeout = req.txn_timeout_s.map(Duration::from_secs);
    if let Some(size) = req.max_db_size {
        config.max_db_pages = size.as_u64() / LIBSQL_PAGE_SIZE;
    }
    if let Some(url) = req.heartbeat_url {
        config.heartbeat_url = Some(Url::parse(&url)?);
    }
    config.jwt_key = req.jwt_key;

    store.store(config).await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct CreateNamespaceReq {
    dump_url: Option<Url>,
    max_db_size: Option<bytesize::ByteSize>,
    heartbeat_url: Option<String>,
    bottomless_db_id: Option<String>,
    jwt_key: Option<String>,
    txn_timeout_s: Option<u64>,
    max_row_size: Option<u64>,
    /// If true, current namespace acts as a DB used solely for multi-db schema updates.
    #[serde(default)]
    shared_schema: bool,
    /// If some, this is a [NamespaceName] reference to a shared schema DB.
    #[serde(default)]
    shared_schema_name: Option<NamespaceName>,
    #[serde(default)]
    allow_attach: bool,
}

async fn handle_create_namespace<C: Connector>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<NamespaceName>,
    Json(req): Json<CreateNamespaceReq>,
) -> crate::Result<()> {
    let mut config = DatabaseConfig::default();

    if let Some(jwt_key) = req.jwt_key {
        // Check that the jwt key is correct
        parse_jwt_key(&jwt_key)?;
        config.jwt_key = Some(jwt_key);
    }

    if req.shared_schema_name.is_some() && req.dump_url.is_some() {
        return Err(Error::SharedSchemaUsageError(
            "database using shared schema database cannot be created from a dump".to_string(),
        ));
    }

    if let Some(ns) = req.shared_schema_name {
        if req.shared_schema {
            return Err(Error::SharedSchemaCreationError(
                "shared schema database cannot reference another shared schema".to_string(),
            ));
        }
        // TODO: move this check into meta store
        if !app_state.namespaces.exists(&ns) {
            return Err(Error::NamespaceDoesntExist(ns.to_string()));
        }

        config.shared_schema_name = Some(ns);
    }

    let dump = match req.dump_url {
        Some(ref url) => {
            RestoreOption::Dump(dump_stream_from_url(url, app_state.connector.clone()).await?)
        }
        None => RestoreOption::Latest,
    };

    config.bottomless_db_id = req.bottomless_db_id;
    config.is_shared_schema = req.shared_schema;
    config.heartbeat_url = req.heartbeat_url.as_deref().map(Url::parse).transpose()?;
    config.txn_timeout = req.txn_timeout_s.map(Duration::from_secs);
    config.max_row_size = req.max_row_size.unwrap_or(config.max_row_size);
    config.allow_attach = req.allow_attach;
    if let Some(max_db_size) = req.max_db_size {
        config.max_db_pages = max_db_size.as_u64() / LIBSQL_PAGE_SIZE;
    }

    app_state.namespaces.create(namespace, dump, config).await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct ForkNamespaceReq {
    timestamp: NaiveDateTime,
}

async fn handle_fork_namespace<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path((from, to)): Path<(String, String)>,
    req: Option<Json<ForkNamespaceReq>>,
) -> crate::Result<()> {
    let timestamp = req.map(|v| v.timestamp);
    let from = NamespaceName::from_string(from)?;
    let to = NamespaceName::from_string(to)?;
    let from_store = app_state.namespaces.config_store(from.clone()).await?;
    let from_config = from_store.get();
    if from_config.is_shared_schema {
        return Err(Error::SharedSchemaUsageError(
            "database cannot be forked from a shared schema".to_string(),
        ));
    }
    let to_config = (*from_config).clone();
    app_state
        .namespaces
        .fork(from, to, to_config, timestamp)
        .await?;

    Ok(())
}

async fn dump_stream_from_url<C>(url: &Url, connector: C) -> Result<DumpStream, LoadDumpError>
where
    C: Connector,
{
    match url.scheme() {
        "http" | "https" => {
            let client = hyper::client::Client::builder().build::<_, Body>(connector);
            let uri = url
                .as_str()
                .parse()
                .map_err(|_| LoadDumpError::InvalidDumpUrl)?;
            let resp = client.get(uri).await?;
            let body = resp
                .into_body()
                .map_err(|e| std::io::Error::new(ErrorKind::Other, e));
            Ok(Box::new(body))
        }
        "file" => {
            let path = PathBuf::from(url.path());
            if !path.is_absolute() {
                return Err(LoadDumpError::DumpFilePathNotAbsolute);
            }

            if !path.try_exists()? {
                return Err(LoadDumpError::DumpFileDoesntExist);
            }

            let f = tokio::fs::File::open(path).await?;

            Ok(Box::new(ReaderStream::new(f)))
        }
        scheme => Err(LoadDumpError::UnsupportedUrlScheme(scheme.to_string())),
    }
}

#[derive(Deserialize, Default)]
struct DeleteNamespaceReq {
    #[serde(default)]
    pub keep_backup: bool,
}

async fn handle_delete_namespace<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<String>,
    payload: Option<Json<DeleteNamespaceReq>>,
) -> crate::Result<()> {
    let prune_all = match payload {
        Some(req) => !req.keep_backup,
        None => true,
    };

    app_state
        .namespaces
        .destroy(NamespaceName::from_string(namespace)?, prune_all)
        .await?;
    Ok(())
}
