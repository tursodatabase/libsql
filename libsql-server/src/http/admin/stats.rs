use std::sync::Arc;

use itertools::Itertools;
use serde::Serialize;

use axum::extract::{Path, State};
use axum::Json;
use uuid::Uuid;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::stats::{QueryStats, SlowestQuery, Stats, TopQuery};

use super::AppState;

#[derive(Serialize)]
pub struct StatsResponse {
    pub id: Option<Uuid>,
    pub rows_read_count: u64,
    pub rows_written_count: u64,
    pub storage_bytes_used: u64,
    pub write_requests_delegated: u64,
    pub replication_index: FrameNo,
    pub top_queries: Vec<TopQuery>,
    pub slowest_queries: Vec<SlowestQuery>,
    pub embedded_replica_frames_replicated: u64,
    pub queries: QueriesStatsResponse,
}

impl From<&Stats> for StatsResponse {
    fn from(stats: &Stats) -> Self {
        Self {
            id: stats.id(),
            rows_read_count: stats.rows_read(),
            rows_written_count: stats.rows_written(),
            storage_bytes_used: stats.storage_bytes_used(),
            write_requests_delegated: stats.write_requests_delegated(),
            replication_index: stats.get_current_frame_no(),
            embedded_replica_frames_replicated: stats.get_embedded_replica_frames_replicated(),
            top_queries: stats
                .top_queries()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect(),
            slowest_queries: stats
                .slowest_queries()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect(),
            queries: QueriesStatsResponse::from(stats),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

#[derive(Serialize)]
pub struct QueriesStatsResponse {
    pub id: Option<Uuid>,
    pub count: u64,
    pub elapsed_ms: u64,
    pub stats: Vec<QueryAndStats>,
}

impl From<&Stats> for QueriesStatsResponse {
    fn from(stats: &Stats) -> Self {
        let count = stats.get_query_count();
        let elapsed_ms = stats.get_query_latency();
        let queries = stats.get_queries().read().unwrap();
        Self {
            count,
            elapsed_ms,
            id: queries.id(),
            stats: queries
                .stats()
                .iter()
                .map(|(k, v)| QueryAndStats {
                    query: k.clone(),
                    elapsed_ms: v.elapsed.as_millis() as u64,
                    stat: v.clone(),
                })
                .collect_vec(),
        }
    }
}

#[derive(Serialize)]
pub struct QueryAndStats {
    pub query: String,
    pub elapsed_ms: u64,
    #[serde(flatten)]
    pub stat: QueryStats,
}

pub(super) async fn handle_stats<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<StatsResponse>> {
    let stats = app_state
        .namespaces
        .stats(NamespaceName::from_string(namespace)?)
        .await?;
    let resp: StatsResponse = stats.as_ref().into();

    Ok(Json(resp))
}

pub(super) async fn handle_delete_stats<C>(
    State(app_state): State<Arc<AppState<C>>>,
    Path((namespace, stats_type)): Path<(String, String)>,
) -> crate::Result<()> {
    let stats = app_state
        .namespaces
        .stats(NamespaceName::from_string(namespace)?)
        .await?;
    match stats_type.as_str() {
        "top" => stats.reset_top_queries(),
        "slowest" => stats.reset_slowest_queries(),
        _ => return Err(crate::error::Error::Internal("Invalid stats type".into())),
    }

    Ok(())
}
