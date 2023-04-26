use hyper::{Body, Response};
use serde::Serialize;

use crate::stats::Stats;

#[derive(Serialize)]
pub struct StatsResponse {
    pub rows_read_count: usize,
    pub rows_written_count: usize,
}

pub fn handle_stats(stats: &Stats) -> Response<Body> {
    let resp = StatsResponse {
        rows_read_count: stats.rows_read(),
        rows_written_count: stats.rows_written(),
    };

    let payload = serde_json::to_vec(&resp).unwrap();
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Body::from(payload))
        .unwrap()
}
