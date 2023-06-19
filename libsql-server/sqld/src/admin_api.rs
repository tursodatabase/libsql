use anyhow::Context as _;
use axum::{extract::State, Json};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::database::config::{DatabaseConfig, DatabaseConfigStore};

struct AppState {
    db_config_store: Arc<DatabaseConfigStore>,
}

pub async fn run_admin_api(
    addr: SocketAddr,
    db_config_store: Arc<DatabaseConfigStore>,
) -> anyhow::Result<()> {
    use axum::routing::{get, post};
    let router = axum::Router::new()
        .route("/", get(handle_get_index))
        .route("/v1/config", get(handle_get_config))
        .route("/v1/block", post(handle_post_block))
        .with_state(Arc::new(AppState { db_config_store }));

    let server = hyper::Server::try_bind(&addr)
        .context("Could not bind admin HTTP API server")?
        .serve(router.into_make_service());

    tracing::info!(
        "Listening for admin HTTP API requests on {}",
        server.local_addr()
    );
    server.await?;
    Ok(())
}

async fn handle_get_index() -> &'static str {
    "Welcome to the sqld admin API"
}

async fn handle_get_config(State(app_state): State<Arc<AppState>>) -> Json<Arc<DatabaseConfig>> {
    Json(app_state.db_config_store.get())
}

#[derive(Debug, Deserialize)]
struct BlockReq {
    block_reads: bool,
    block_writes: bool,
    #[serde(default)]
    block_reason: Option<String>,
}

async fn handle_post_block(
    State(app_state): State<Arc<AppState>>,
    Json(req): Json<BlockReq>,
) -> (axum::http::StatusCode, &'static str) {
    let mut config = (*app_state.db_config_store.get()).clone();
    config.block_reads = req.block_reads;
    config.block_writes = req.block_writes;
    config.block_reason = req.block_reason;

    match app_state.db_config_store.store(config) {
        Ok(()) => (axum::http::StatusCode::OK, "OK"),
        Err(err) => {
            tracing::warn!("Could not store database config: {err}");
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "Failed")
        }
    }
}
