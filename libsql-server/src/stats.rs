use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Weak};

use chrono::{DateTime, DurationRound, Utc};
use hdrhistogram::Histogram;
use metrics::{counter, gauge, histogram, increment_counter};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};
use uuid::Uuid;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopQuery {
    #[serde(skip)]
    pub weight: u64,
    pub rows_written: u64,
    pub rows_read: u64,
    pub query: String,
}

impl TopQuery {
    fn new(query: String, rows_read: u64, rows_written: u64) -> Self {
        Self {
            weight: rows_read + rows_written,
            rows_read,
            rows_written,
            query,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlowestQuery {
    pub elapsed_ms: u64,
    pub query: String,
    pub rows_written: u64,
    pub rows_read: u64,
}

impl SlowestQuery {
    fn new(query: String, elapsed_ms: u64, rows_read: u64, rows_written: u64) -> Self {
        Self {
            elapsed_ms,
            query,
            rows_read,
            rows_written,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryStats {
    #[serde(skip)]
    pub elapsed: Duration,
    pub count: u64,
    pub rows_written: u64,
    pub rows_read: u64,
}

impl QueryStats {
    fn new(elapsed: Duration, rows_read: u64, rows_written: u64) -> Self {
        Self {
            elapsed,
            count: 1,
            rows_read,
            rows_written,
        }
    }

    fn merge(&self, another: &QueryStats) -> Self {
        Self {
            elapsed: self.elapsed + another.elapsed,
            count: self.count + another.count,
            rows_read: self.rows_read + another.rows_read,
            rows_written: self.rows_written + another.rows_written,
        }
    }
}

#[derive(Debug)]
pub struct QueriesStats {
    id: Uuid,
    stats: HashMap<String, QueryStats>,
    elapsed: Duration,
    stats_threshold: u64,
    hist: Histogram<u32>,
    expires_at: Option<Instant>,
    created_at: DateTime<Utc>,
}

impl QueriesStats {
    fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            stats: HashMap::new(),
            elapsed: Duration::default(),
            stats_threshold: 0,
            hist: Histogram::<u32>::new(3).unwrap(),
            expires_at: None,
            created_at: Utc::now(),
        }
    }

    fn with_expiration(expires_at: Instant) -> Self {
        let mut this = Self::new();
        this.expires_at = Some(expires_at);
        this
    }

    fn register_query(&mut self, sql: &String, stat: QueryStats) {
        self.elapsed += stat.elapsed;
        let _ = self.hist.record(stat.elapsed.as_micros() as u64);

        let (aggregated, new) = match self.stats.get(sql) {
            Some(aggregated) => (aggregated.merge(&stat), false),
            None => (stat, true),
        };

        if (aggregated.elapsed.as_micros() as u64) < self.stats_threshold {
            return;
        }

        self.stats.insert(sql.clone(), aggregated);

        if !new || self.stats.len() <= 30 {
            return;
        }

        while self.stats.len() > 30 {
            self.pop()
        }

        self.update_threshold();
    }

    fn min(&self) -> Option<(&String, &QueryStats)> {
        self.stats
            .iter()
            .min_by(|a, b| a.1.elapsed.cmp(&b.1.elapsed))
    }

    fn update_threshold(&mut self) {
        if let Some((_, v)) = self.min() {
            self.stats_threshold = v.elapsed.as_micros() as u64;
        }
    }

    fn pop(&mut self) {
        let min = self.min();
        if let Some(min) = min {
            self.stats.remove(&min.0.clone());
        }
    }

    pub(crate) fn id(&self) -> Uuid {
        self.id
    }

    pub(crate) fn stats(&self) -> &HashMap<String, QueryStats> {
        &self.stats
    }

    pub(crate) fn hist(&self) -> &Histogram<u32> {
        &self.hist
    }

    pub(crate) fn expired(&self) -> bool {
        self.expires_at.map_or(false, |exp| exp < Instant::now())
    }

    pub(crate) fn elapsed(&self) -> &Duration {
        &self.elapsed
    }

    pub(crate) fn count(&self) -> u64 {
        self.hist.len() as u64
    }

    pub(crate) fn created_at(&self) -> &DateTime<Utc> {
        &self.created_at
    }
}

#[derive(Debug, Default, Clone)]
pub struct StatsUpdateMessage {
    pub sql: String,
    pub elapsed: Duration,
    pub rows_read: u64,
    pub rows_written: u64,
    pub mem_used: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Stats {
    #[serde(skip)]
    namespace: NamespaceName,
    #[serde(skip)]
    sender: Option<mpsc::Sender<StatsUpdateMessage>>,

    #[serde(default)]
    id: Option<Uuid>,
    #[serde(default)]
    rows_written: AtomicU64,
    #[serde(default)]
    rows_read: AtomicU64,
    #[serde(default)]
    storage_bytes_used: AtomicU64,
    // number of write requests delegated from a replica to primary
    #[serde(default)]
    write_requests_delegated: AtomicU64,
    #[serde(default)]
    current_frame_no: AtomicU64,
    // Lowest value in currently stored top queries
    #[serde(default)]
    top_query_threshold: AtomicU64,
    #[serde(default)]
    top_queries: Arc<RwLock<BTreeSet<TopQuery>>>,
    // Lowest value in currently stored slowest queries
    #[serde(default)]
    slowest_query_threshold: AtomicU64,
    #[serde(default)]
    slowest_queries: Arc<RwLock<BTreeSet<SlowestQuery>>>,
    #[serde(default)]
    embedded_replica_frames_replicated: AtomicU64,
    #[serde(default)]
    query_count: AtomicU64,
    #[serde(default)]
    query_latency: AtomicU64,
    #[serde(skip)]
    queries: Arc<RwLock<Option<QueriesStats>>>,
}

impl Stats {
    pub async fn new(
        namespace: NamespaceName,
        db_path: &Path,
        join_set: &mut JoinSet<anyhow::Result<()>>,
    ) -> anyhow::Result<Arc<Self>> {
        let stats_path = db_path.join("stats.json");
        let mut this = if stats_path.try_exists()? {
            let data = tokio::fs::read_to_string(&stats_path).await?;
            serde_json::from_str(&data)?
        } else {
            Stats::default()
        };

        if this.id.is_none() {
            this.id = Some(Uuid::new_v4());
        }

        let (update_sender, update_receiver) = mpsc::channel(256);

        this.namespace = namespace;
        this.sender = Some(update_sender);

        let this = Arc::new(this);

        join_set.spawn(spawn_stats_persist_thread(
            Arc::downgrade(&this),
            stats_path.to_path_buf(),
        ));

        join_set.spawn(spawn_stats_thread(Arc::downgrade(&this), update_receiver));

        Ok(this)
    }

    pub fn send(&self, msg: StatsUpdateMessage) {
        if let Some(sender) = &self.sender {
            let _ = sender.blocking_send(msg);
        }
    }

    pub fn update(&self, msg: StatsUpdateMessage) {
        let sql = msg.sql;
        let rows_read = msg.rows_read;
        let rows_written = msg.rows_written;
        let mem_used = msg.mem_used;
        let elapsed = msg.elapsed;
        let elapsed_ms = elapsed.as_millis() as u64;
        let rows_read = if rows_read == 0 && rows_written == 0 {
            1
        } else {
            rows_read
        };
        let weight = rows_read + rows_written;

        histogram!("libsql_server_statement_execution_time", elapsed);
        histogram!("libsql_server_statement_mem_used_bytes", mem_used as f64);

        if rows_read >= 10_000 || rows_written >= 1_000 {
            let sql = if sql.len() >= 512 {
                &sql[..512]
            } else {
                &sql[..]
            };

            tracing::info!(
                "high read ({}) or write ({}) query: {}",
                rows_read,
                rows_written,
                sql
            );
        }

        self.inc_rows_read(rows_read);
        self.inc_rows_written(rows_written);
        self.inc_query(elapsed);
        self.register_query(
            &sql,
            crate::stats::QueryStats::new(elapsed, rows_read, rows_written),
        );
        if self.qualifies_as_top_query(weight) {
            self.add_top_query(crate::stats::TopQuery::new(
                sql.clone(),
                rows_read,
                rows_written,
            ));
        }
        if self.qualifies_as_slowest_query(elapsed_ms) {
            self.add_slowest_query(crate::stats::SlowestQuery::new(
                sql.clone(),
                elapsed_ms,
                rows_read,
                rows_written,
            ));
        }

        self.update_query_metrics(rows_read, rows_written, mem_used, elapsed_ms)
    }

    /// increments the number of written rows by n
    fn inc_rows_written(&self, n: u64) {
        counter!("libsql_server_rows_written", n, "namespace" => self.namespace.to_string());
        self.rows_written.fetch_add(n, Ordering::Relaxed);
    }

    fn inc_query(&self, d: Duration) {
        let micros = d.as_micros() as u64;
        counter!("libsql_server_query_count", 1, "namespace" => self.namespace.to_string());
        counter!("libsql_server_query_latency", micros / 1000, "namespace" => self.namespace.to_string());
        self.query_count.fetch_add(1, Ordering::Relaxed);
        self.query_latency.fetch_add(micros, Ordering::Relaxed);
    }

    /// increments the number of read rows by n
    fn inc_rows_read(&self, n: u64) {
        counter!("libsql_server_rows_read", n, "namespace" => self.namespace.to_string());
        self.rows_read.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_storage_bytes_used(&self, n: u64) {
        gauge!("libsql_server_storage", n as f64, "namespace" => self.namespace.to_string());
        self.storage_bytes_used.store(n, Ordering::Relaxed);
    }

    /// returns the total number of rows read since this database was created
    pub(crate) fn rows_read(&self) -> u64 {
        self.rows_read.load(Ordering::Relaxed)
    }

    /// returns the total number of rows written since this database was created
    pub(crate) fn rows_written(&self) -> u64 {
        self.rows_written.load(Ordering::Relaxed)
    }

    /// returns the total number of bytes used by the database (excluding uncheckpointed WAL entries)
    pub(crate) fn storage_bytes_used(&self) -> u64 {
        self.storage_bytes_used.load(Ordering::Relaxed)
    }

    /// increments the number of the write requests which were delegated from a replica to primary
    pub fn inc_write_requests_delegated(&self) {
        increment_counter!("libsql_server_write_requests_delegated", "namespace" => self.namespace.to_string());
        self.write_requests_delegated
            .fetch_add(1, Ordering::Relaxed);
    }

    /// increments the number of the write requests which were delegated from a replica to primary
    pub fn inc_embedded_replica_frames_replicated(&self) {
        increment_counter!("libsql_server_embedded_replica_frames_replicated", "namespace" => self.namespace.to_string());
        self.embedded_replica_frames_replicated
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_embedded_replica_frames_replicated(&self) -> u64 {
        self.embedded_replica_frames_replicated
            .load(Ordering::Relaxed)
    }

    pub fn write_requests_delegated(&self) -> u64 {
        self.write_requests_delegated.load(Ordering::Relaxed)
    }

    pub fn set_current_frame_no(&self, fno: FrameNo) {
        gauge!("libsql_server_current_frame_no", fno as f64, "namespace" => self.namespace.to_string());
        self.current_frame_no.store(fno, Ordering::Relaxed);
    }

    pub(crate) fn get_current_frame_no(&self) -> FrameNo {
        self.current_frame_no.load(Ordering::Relaxed)
    }

    pub(crate) fn get_query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    pub(crate) fn get_query_latency(&self) -> u64 {
        self.query_latency.load(Ordering::Relaxed)
    }

    pub(crate) fn get_queries(&self) -> &Arc<RwLock<Option<QueriesStats>>> {
        &self.queries
    }

    fn register_query(&self, sql: &String, stat: QueryStats) {
        let mut queries = self.queries.write().unwrap();
        if queries.as_ref().map_or(true, |q| q.expired()) {
            *queries = Some(QueriesStats::with_expiration(next_hour()))
        }
        queries.as_mut().unwrap().register_query(sql, stat)
    }

    fn add_top_query(&self, query: TopQuery) {
        let mut top_queries = self.top_queries.write().unwrap();
        tracing::debug!(
            "top query: {},{}:{}",
            query.rows_read,
            query.rows_written,
            query.query
        );
        top_queries.insert(query);
        if top_queries.len() > 10 {
            top_queries.pop_first();
            self.top_query_threshold
                .store(top_queries.first().unwrap().weight, Ordering::Relaxed);
        }
    }

    fn qualifies_as_top_query(&self, weight: u64) -> bool {
        weight >= self.top_query_threshold.load(Ordering::Relaxed)
    }

    pub(crate) fn top_queries(&self) -> &Arc<RwLock<BTreeSet<TopQuery>>> {
        &self.top_queries
    }

    pub(crate) fn reset_top_queries(&self) {
        self.top_queries.write().unwrap().clear();
        self.top_query_threshold.store(0, Ordering::Relaxed);
    }

    fn add_slowest_query(&self, query: SlowestQuery) {
        let mut slowest_queries = self.slowest_queries.write().unwrap();
        tracing::debug!("slowest query: {}: {}", query.elapsed_ms, query.query);
        slowest_queries.insert(query);
        if slowest_queries.len() > 10 {
            slowest_queries.pop_first();
            self.slowest_query_threshold.store(
                slowest_queries.first().unwrap().elapsed_ms,
                Ordering::Relaxed,
            );
        }
    }

    fn qualifies_as_slowest_query(&self, elapsed_ms: u64) -> bool {
        elapsed_ms >= self.slowest_query_threshold.load(Ordering::Relaxed)
    }

    pub(crate) fn slowest_queries(&self) -> &Arc<RwLock<BTreeSet<SlowestQuery>>> {
        &self.slowest_queries
    }

    pub(crate) fn reset_slowest_queries(&self) {
        self.slowest_queries.write().unwrap().clear();
        self.slowest_query_threshold.store(0, Ordering::Relaxed);
    }

    // TOOD: Update these metrics with namespace labels in the future so we can localize
    // issues to a specific namespace.
    fn update_query_metrics(&self, rows_read: u64, rows_written: u64, mem_used: u64, elapsed: u64) {
        increment_counter!("libsql_server_query_count");
        counter!("libsql_server_query_latency", elapsed);
        counter!("libsql_server_query_rows_read", rows_read);
        counter!("libsql_server_query_rows_written", rows_written);
        counter!("libsql_server_query_mem_used", mem_used);
    }

    pub(crate) fn id(&self) -> Option<Uuid> {
        self.id
    }
}

async fn spawn_stats_thread(
    stats: Weak<Stats>,
    mut receiver: mpsc::Receiver<StatsUpdateMessage>,
) -> anyhow::Result<()> {
    loop {
        match (receiver.recv().await, stats.upgrade()) {
            (Some(msg), Some(stats)) => stats.update(msg),
            _ => return Ok(()),
        }
    }
}

async fn spawn_stats_persist_thread(stats: Weak<Stats>, path: PathBuf) -> anyhow::Result<()> {
    loop {
        if let Err(e) = try_persist_stats(stats.clone(), &path).await {
            tracing::error!("error persisting stats file: {e}");
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn try_persist_stats(stats: Weak<Stats>, path: &Path) -> anyhow::Result<()> {
    let temp_path = path.with_extension("tmp");
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&temp_path)
        .await?;
    file.set_len(0).await?;
    file.write_all(&serde_json::to_vec(&stats)?).await?;
    file.flush().await?;
    tokio::fs::rename(temp_path, path).await?;
    Ok(())
}

fn next_hour() -> Instant {
    let utc_now = Utc::now();
    let next_hour = (utc_now + chrono::Duration::hours(1))
        .duration_trunc(chrono::Duration::hours(1))
        .unwrap();
    Instant::now() + (next_hour - utc_now).to_std().unwrap()
}
