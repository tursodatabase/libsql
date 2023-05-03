use hyper::{Body, Response};
use serde::Serialize;

use crate::stats::Stats;

#[derive(Serialize)]
pub struct StatsResponse {
    pub rows_read_count: u64,
    pub rows_written_count: u64,
    pub storage_bytes_used: u64,
}

impl From<&Stats> for StatsResponse {
    fn from(stats: &Stats) -> Self {
        Self {
            rows_read_count: stats.rows_read(),
            rows_written_count: stats.rows_written(),
            storage_bytes_used: stats.storage_bytes_used(),
        }
    }
}

impl From<Stats> for StatsResponse {
    fn from(stats: Stats) -> Self {
        (&stats).into()
    }
}

pub fn handle_stats(stats: &Stats) -> Response<Body> {
    let resp: StatsResponse = stats.into();

    let payload = serde_json::to_vec(&resp).unwrap();
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Body::from(payload))
        .unwrap()
}
