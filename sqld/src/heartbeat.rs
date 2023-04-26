use anyhow::Result;
use std::time::Duration;
use tokio::time::sleep;

use crate::stats::Stats;

pub async fn server_heartbeat(
    url: String,
    auth: Option<String>,
    update_period: Duration,
    stats: Stats,
) -> Result<()> {
    let client = reqwest::Client::new();
    loop {
        sleep(update_period).await;
        let body = serde_json::json!({
            "rows_read": stats.rows_read(),
            "rows_written": stats.rows_written(),
        });
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
