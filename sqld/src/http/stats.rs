use hyper::{Body, Response};
use serde::Serialize;

use axum::extract::{FromRef, State as AxumState};

use crate::{namespace::MakeNamespace, stats::Stats};

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

impl<F: MakeNamespace> FromRef<AppState<F>> for Stats {
    fn from_ref(input: &AppState<F>) -> Self {
        input.stats.clone()
    }
}

pub(crate) async fn handle_stats(AxumState(stats): AxumState<Stats>) -> Response<Body> {
    let resp: StatsResponse = stats.into();

    let payload = serde_json::to_vec(&resp).unwrap();
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Body::from(payload))
        .unwrap()
}
