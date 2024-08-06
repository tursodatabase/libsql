use std::sync::Arc;
use std::time::Duration;

use hdrhistogram::Histogram;
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
    pub query_count: u64,
    pub elapsed_ms: f64,
    pub queries: Option<QueriesStatsResponse>,
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
            query_count: stats.get_query_count(),
            elapsed_ms: stats.get_query_latency() as f64 / 1_000.0,
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
            queries: stats.into(),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

#[derive(Serialize, Default)]
pub struct QueriesLatencyStats {
    pub sum: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
}

impl QueriesLatencyStats {
    fn from(hist: &Histogram<u32>, sum: &Duration) -> Self {
        QueriesLatencyStats {
            sum: sum.as_micros() as f64 / 1000.0,
            p50: hist.value_at_percentile(50.0) as f64 / 1000.0,
            p75: hist.value_at_percentile(75.0) as f64 / 1000.0,
            p90: hist.value_at_percentile(90.0) as f64 / 1000.0,
            p95: hist.value_at_percentile(95.0) as f64 / 1000.0,
            p99: hist.value_at_percentile(99.0) as f64 / 1000.0,
            p999: hist.value_at_percentile(99.9) as f64 / 1000.0,
        }
    }
}

#[derive(Serialize, Default)]
pub struct QueriesStatsResponse {
    pub id: Uuid,
    pub created_at: u64,
    pub count: u64,
    pub stats: Vec<QueryAndStats>,
    pub elapsed: QueriesLatencyStats,
}

impl From<&Stats> for Option<QueriesStatsResponse> {
    fn from(stats: &Stats) -> Self {
        let queries = stats.get_queries().read().unwrap();
        if queries.as_ref().map_or(true, |q| q.expired()) {
            Self::default()
        } else {
            let queries = queries.as_ref().unwrap();
            Some(QueriesStatsResponse {
                id: queries.id(),
                created_at: queries.created_at().timestamp() as u64,
                count: queries.count(),
                elapsed: QueriesLatencyStats::from(queries.hist(), &queries.elapsed()),
                stats: queries
                    .stats()
                    .iter()
                    .map(|(k, v)| QueryAndStats {
                        query: k.clone(),
                        elapsed_ms: v.elapsed.as_micros() as f64 / 1000.0,
                        stat: v.clone(),
                    })
                    .collect_vec(),
            })
        }
    }
}

#[derive(Serialize)]
pub struct QueryAndStats {
    pub query: String,
    pub elapsed_ms: f64,
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
