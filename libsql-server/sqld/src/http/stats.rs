use hyper::{Body, Response};
use serde::Serialize;

use axum::extract::State as AxumState;

use crate::stats::Stats;

use super::AppState;

#[derive(Serialize)]
pub struct StatsResponse {
    pub rows_read_count: u64,
    pub rows_written_count: u64,
    pub storage_bytes_used: u64,
    pub write_requests_delegated: u64,
}

impl From<&Stats> for StatsResponse {
    fn from(stats: &Stats) -> Self {
        Self {
            rows_read_count: stats.rows_read(),
            rows_written_count: stats.rows_written(),
            storage_bytes_used: stats.storage_bytes_used(),
            write_requests_delegated: stats.write_requests_delegated(),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

pub(crate) async fn handle_stats<D>(
    AxumState(AppState { stats, .. }): AxumState<AppState<D>>,
) -> Response<Body> {
    let resp: StatsResponse = stats.into();

    let payload = serde_json::to_vec(&resp).unwrap();
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Body::from(payload))
        .unwrap()
}
