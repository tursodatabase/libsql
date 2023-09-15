use anyhow::Context as _;
use axum::extract::{Path, State};
use axum::routing::delete;
use axum::Json;
use chrono::NaiveDateTime;
use futures::TryStreamExt;
use serde::Deserialize;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio_util::io::ReaderStream;
use url::Url;
use uuid::Uuid;

use crate::connection::config::DatabaseConfig;
use crate::error::LoadDumpError;
use crate::namespace::{DumpStream, MakeNamespace, NamespaceStore, RestoreOption};

pub mod stats;

struct AppState<M: MakeNamespace> {
    namespaces: NamespaceStore<M>,
}

pub async fn run<M, A>(acceptor: A, namespaces: NamespaceStore<M>) -> anyhow::Result<()>
where
    A: crate::net::Accept,
    M: MakeNamespace,
{
    use axum::routing::{get, post};
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
        .route(
            "/v1/namespaces/:namespace/restore",
            post(handle_restore_namespace),
        )
        .route("/v1/namespaces/:namespace", delete(handle_delete_namespace))
        .route("/v1/namespaces/:namespace/stats", get(stats::handle_stats))
        .with_state(Arc::new(AppState { namespaces }));

    hyper::server::Server::builder(acceptor)
        .serve(router.into_make_service())
        .await
        .context("Could not bind admin HTTP API server")?;
    Ok(())
}

async fn handle_get_index() -> &'static str {
    "Welcome to the sqld admin API"
}

async fn handle_get_config<M: MakeNamespace>(
    State(app_state): State<Arc<AppState<M>>>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<Arc<DatabaseConfig>>> {
    let store = app_state.namespaces.config_store(namespace.into()).await?;
    Ok(Json(store.get()))
}

#[derive(Debug, Deserialize)]
struct BlockReq {
    block_reads: bool,
    block_writes: bool,
    #[serde(default)]
    block_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateNamespaceReq {
    dump_url: Option<Url>,
}

async fn handle_post_config<M: MakeNamespace>(
    State(app_state): State<Arc<AppState<M>>>,
    Path(namespace): Path<String>,
    Json(req): Json<BlockReq>,
) -> crate::Result<()> {
    let store = app_state.namespaces.config_store(namespace.into()).await?;
    let mut config = (*store.get()).clone();
    config.block_reads = req.block_reads;
    config.block_writes = req.block_writes;
    config.block_reason = req.block_reason;

    store.store(config)?;

    Ok(())
}

async fn handle_create_namespace<M: MakeNamespace>(
    State(app_state): State<Arc<AppState<M>>>,
    Path(namespace): Path<String>,
    Json(req): Json<CreateNamespaceReq>,
) -> crate::Result<()> {
    let dump = match req.dump_url {
        Some(ref url) => RestoreOption::Dump(dump_stream_from_url(url).await?),
        None => RestoreOption::Latest,
    };

    app_state.namespaces.create(namespace.into(), dump).await?;
    Ok(())
}

async fn handle_fork_namespace<M: MakeNamespace>(
    State(app_state): State<Arc<AppState<M>>>,
    Path((from, to)): Path<(String, String)>,
) -> crate::Result<()> {
    app_state.namespaces.fork(from.into(), to.into()).await?;
    Ok(())
}

async fn dump_stream_from_url(url: &Url) -> Result<DumpStream, LoadDumpError> {
    match url.scheme() {
        "http" => {
            let client = hyper::client::Client::new();
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
            let path = url
                .to_file_path()
                .map_err(|_| LoadDumpError::InvalidDumpUrl)?;
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

async fn handle_delete_namespace<F: MakeNamespace>(
    State(app_state): State<Arc<AppState<F>>>,
    Path(namespace): Path<String>,
) -> crate::Result<()> {
    app_state.namespaces.destroy(namespace.into()).await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct RestoreReq {
    generation: Option<Uuid>,
    timestamp: Option<NaiveDateTime>,
}

async fn handle_restore_namespace<F: MakeNamespace>(
    State(app_state): State<Arc<AppState<F>>>,
    Path(namespace): Path<String>,
    Json(req): Json<RestoreReq>,
) -> crate::Result<()> {
    let restore_option = match (req.generation, req.timestamp) {
        (None, None) => RestoreOption::Latest,
        (Some(generation), None) => RestoreOption::Generation(generation),
        (None, Some(timestamp)) => RestoreOption::PointInTime(timestamp),
        (Some(_), Some(_)) => return Err(crate::Error::ConflictingRestoreParameters),
    };
    app_state
        .namespaces
        .reset(namespace.into(), restore_option)
        .await?;
    Ok(())
}
