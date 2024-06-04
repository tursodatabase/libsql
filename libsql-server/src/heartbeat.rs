#![allow(clippy::mutable_key_type)]

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;
use tokio::time::Interval;
use url::Url;

use tokio::sync::{mpsc, Semaphore};

use crate::http::admin::stats::StatsResponse;
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::NamespaceName;
use crate::stats::Stats;

pub async fn server_heartbeat(
    url: Option<Url>,
    auth: Option<String>,
    update_period: Duration,
    mut stats_subs: mpsc::Receiver<(NamespaceName, MetaStoreHandle, Weak<Stats>)>,
) {
    let mut watched = HashMap::new();
    let client = reqwest::Client::new();
    let mut interval = tokio::time::interval(update_period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let semaphore = Arc::new(Semaphore::new(128));

    loop {
        let wait_for_next_tick = next_tick(&mut interval, &semaphore, 128);

        tokio::select! {
            Some((ns, handle, stats)) = stats_subs.recv() => {
                watched.insert(ns, (handle, stats));
            }
            _ =  wait_for_next_tick => {
                send_stats(&mut watched, &client, url.as_ref(), auth.as_deref(), &semaphore).await;
            }
        };
    }
}

/// Wait for all the permits to be available again, this should work as long as its called after
/// the last `send_stats` is called since the sempaphore waits in a queue.
async fn next_tick(interval: &mut Interval, semaphore: &Arc<Semaphore>, permits: u32) {
    let permit = semaphore.acquire_many(permits).await;
    drop(permit);

    interval.tick().await;
}

async fn send_stats(
    watched: &mut HashMap<NamespaceName, (MetaStoreHandle, Weak<Stats>)>,
    client: &reqwest::Client,
    url: Option<&Url>,
    auth: Option<&str>,
    semaphore: &Arc<Semaphore>,
) {
    // first send all the stats...
    for (ns, (config_store, stats)) in watched.iter() {
        if let Some(stats) = stats.upgrade() {
            let body = StatsResponse::from(stats.as_ref());

            let mut heartbeat_url = if let Some(url) = url {
                url.clone()
            } else {
                let config = config_store.get();
                if let Some(url) = config.heartbeat_url.as_ref() {
                    url.clone()
                } else {
                    tracing::debug!(
                        "No heartbeat url for namespace {}. Can't send stats!",
                        ns.as_str()
                    );
                    continue;
                }
            };

            heartbeat_url.path_segments_mut().unwrap().push(ns.as_str());

            let request = client.post(heartbeat_url);

            let request = if let Some(ref auth) = auth {
                request.header("Authorization", auth.to_string())
            } else {
                request
            };

            let request = request.json(&body);

            let semaphore = semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await;

                if let Err(err) = request.send().await {
                    tracing::warn!("Error sending heartbeat: {}", err);
                }
            });
        }
    }

    // ..and then remove all expired subscription
    watched.retain(|_, (_, s)| s.upgrade().is_some());
}
