use std::sync::Arc;

use serde::Serialize;

use axum::extract::{Path, State};
use axum::Json;

use crate::namespace::{MakeNamespace, NamespaceName};
use crate::replication::FrameNo;
use crate::stats::{Stats, TopQuery};

use super::AppState;

#[derive(Serialize)]
pub struct StatsResponse {
    pub rows_read_count: u64,
    pub rows_written_count: u64,
    pub storage_bytes_used: u64,
    pub write_requests_delegated: u64,
    pub replication_index: FrameNo,
    pub top_queries: Vec<TopQuery>,
}

impl From<&Stats> for StatsResponse {
    fn from(stats: &Stats) -> Self {
        Self {
            rows_read_count: stats.rows_read(),
            rows_written_count: stats.rows_written(),
            storage_bytes_used: stats.storage_bytes_used(),
            write_requests_delegated: stats.write_requests_delegated(),
            replication_index: stats.get_current_frame_no(),
            top_queries: stats
                .top_queries()
                .read()
                .unwrap()
                .iter()
                .cloned()
                .collect(),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

pub(super) async fn handle_stats<M: MakeNamespace, C>(
    State(app_state): State<Arc<AppState<M, C>>>,
    Path(namespace): Path<String>,
) -> crate::Result<Json<StatsResponse>> {
    let stats = app_state
        .namespaces
        .stats(NamespaceName::from_string(namespace)?)
        .await?;
    let resp: StatsResponse = stats.as_ref().into();

    Ok(Json(resp))
}
