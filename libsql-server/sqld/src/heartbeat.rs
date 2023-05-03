use std::time::Duration;
use tokio::time::sleep;

use crate::http::stats::StatsResponse;
use crate::stats::Stats;

pub async fn server_heartbeat(
    url: String,
    auth: Option<String>,
    update_period: Duration,
    stats: Stats,
) {
    let client = reqwest::Client::new();
    loop {
        sleep(update_period).await;
        let body = StatsResponse::from(&stats);
        let request = client.post(&url);
        let request = if let Some(ref auth) = auth {
            request.header("Authorization", auth.clone())
        } else {
            request
        };
        let request = request.json(&body);
        if let Err(err) = request.send().await {
            tracing::warn!("Error sending heartbeat: {}", err);
        }
    }
}
