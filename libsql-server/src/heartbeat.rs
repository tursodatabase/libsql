#![allow(clippy::mutable_key_type)]

use std::collections::HashMap;
use std::sync::Weak;
use std::time::Duration;
use url::Url;

use tokio::sync::mpsc;

use crate::http::admin::stats::StatsResponse;
use crate::namespace::NamespaceName;
use crate::stats::Stats;

pub async fn server_heartbeat(
    url: Url,
    auth: Option<String>,
    update_period: Duration,
    mut stats_subs: mpsc::Receiver<(NamespaceName, Weak<Stats>)>,
) {
    let mut watched = HashMap::new();
    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(update_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            Some((ns, stats)) = stats_subs.recv() => {
                watched.insert(ns, stats);
            }
            _ = interval.tick() => {
                send_stats(&mut watched, &client, &url, auth.as_deref()).await;
            }
        };
    }
}

async fn send_stats(
    watched: &mut HashMap<NamespaceName, Weak<Stats>>,
    client: &reqwest::Client,
    url: &Url,
    auth: Option<&str>,
) {
    // first send all the stats...
    for (ns, stats) in watched.iter() {
        if let Some(stats) = stats.upgrade() {
            let body = StatsResponse::from(stats.as_ref());
            let mut url = url.clone();
            url.path_segments_mut().unwrap().push(ns.as_str());
            let request = client.post(url);
            let request = if let Some(ref auth) = auth {
                request.header("Authorization", auth.to_string())
            } else {
                request
            };
            let request = request.json(&body);
            if let Err(err) = request.send().await {
                tracing::warn!("Error sending heartbeat: {}", err);
            }
        }
    }

    // ..and then remove all expired subscription
    watched.retain(|_, s| s.upgrade().is_some());
}
