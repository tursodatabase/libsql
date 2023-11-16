use crate::backup::WalCopier;
use crate::read::BatchReader;
use crate::transaction_cache::TransactionPageCache;
use crate::uuid_utils::decode_unix_timestamp;
use crate::wal::WalFileReader;
use anyhow::{anyhow, bail};
use arc_swap::ArcSwapOption;
use async_compression::tokio::write::{GzipEncoder, ZstdEncoder};
use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::list_objects::builders::ListObjectsFluentBuilder;
use aws_sdk_s3::operation::list_objects::ListObjectsOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config};
use bytes::{Buf, Bytes};
use chrono::{NaiveDateTime, TimeZone, Utc};
use std::io::SeekFrom;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::watch::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tokio::time::{timeout_at, Instant};
use uuid::{NoContext, Uuid};

/// Maximum number of generations that can participate in database restore procedure.
/// This effectively means that at least one in [MAX_RESTORE_STACK_DEPTH] number of
/// consecutive generations has to have a snapshot included.
const MAX_RESTORE_STACK_DEPTH: usize = 100;

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug)]
pub struct Replicator {
    pub client: Client,

    /// Frame number, incremented whenever a new frame is written from SQLite.
    next_frame_no: Arc<AtomicU32>,
    /// Last frame which has been requested to be sent to S3.
    /// Always: [last_sent_frame_no] <= [next_frame_no].
    last_sent_frame_no: Arc<AtomicU32>,
    /// Last frame which has been confirmed as stored locally outside of WAL file.
    /// Always: [last_committed_frame_no] <= [last_sent_frame_no].
    last_committed_frame_no: Receiver<Result<u32>>,
    flush_trigger: Sender<()>,
    snapshot_waiter: Receiver<Result<Option<Uuid>>>,
    snapshot_notifier: Arc<Sender<Result<Option<Uuid>>>>,

    pub page_size: usize,
    restore_transaction_page_swap_after: u32,
    restore_transaction_cache_fpath: Arc<str>,
    generation: Arc<ArcSwapOption<Uuid>>,
    verify_crc: bool,
    pub bucket: String,
    pub db_path: String,
    pub db_name: String,

    use_compression: CompressionKind,
    max_frames_per_batch: usize,
    s3_upload_max_parallelism: usize,
    _join_set: JoinSet<()>,
}

#[derive(Debug)]
pub struct FetchedResults {
    pub pages: Vec<(i32, Bytes)>,
    pub next_marker: Option<String>,
}

#[derive(Debug)]
pub enum RestoreAction {
    SnapshotMainDbFile,
    ReuseGeneration(Uuid),
}

#[derive(Clone, Debug)]
pub struct Options {
    pub create_bucket_if_not_exists: bool,
    /// If `true` when restoring, frames checksums will be verified prior their pages being flushed
    /// into the main database file.
    pub verify_crc: bool,
    /// Kind of compression algorithm used on the WAL frames to be sent to S3.
    pub use_compression: CompressionKind,
    pub aws_endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: Option<String>,
    pub db_id: Option<String>,
    /// Bucket directory name where all S3 objects are backed up. General schema is:
    /// - `{db-name}-{uuid-v7}` subdirectories:
    ///   - `.meta` file with database page size and initial WAL checksum.
    ///   - Series of files `{first-frame-no}-{last-frame-no}.{compression-kind}` containing
    ///     the batches of frames from which the restore will be made.
    pub bucket_name: String,
    /// Max number of WAL frames per S3 object.
    pub max_frames_per_batch: usize,
    /// Max time before next frame of batched frames should be synced. This works in the case
    /// when we don't explicitly run into `max_frames_per_batch` threshold and the corresponding
    /// checkpoint never commits.
    pub max_batch_interval: Duration,
    /// Maximum number of S3 file upload requests that may happen in parallel.
    pub s3_upload_max_parallelism: usize,
    /// When recovering a transaction, if number of affected pages is greater than page swap,
    /// start flushing these pages on disk instead of keeping them in memory.
    pub restore_transaction_page_swap_after: u32,
    /// When recovering a transaction, when its page cache needs to be swapped onto local file,
    /// this field contains a path for a file to be used.
    pub restore_transaction_cache_fpath: String,
    /// Max number of retries for S3 operations
    pub s3_max_retries: u32,
}

impl Options {
    pub async fn client_config(&self) -> Result<Config> {
        let mut loader = aws_config::from_env();
        if let Some(endpoint) = self.aws_endpoint.as_deref() {
            loader = loader.endpoint_url(endpoint);
        }
        let region = self
            .region
            .clone()
            .ok_or(anyhow!("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION was not set"))?;
        let access_key_id = self
            .access_key_id
            .clone()
            .ok_or(anyhow!("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID was not set"))?;
        let secret_access_key = self.secret_access_key.clone().ok_or(anyhow!(
            "LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY was not set"
        ))?;
        let conf = aws_sdk_s3::config::Builder::from(&loader.load().await)
            .force_path_style(true)
            .region(Region::new(region))
            .credentials_provider(Credentials::new(
                access_key_id,
                secret_access_key,
                None,
                None,
                "Static",
            ))
            .retry_config(
                aws_sdk_s3::config::retry::RetryConfig::standard()
                    .with_max_attempts(self.s3_max_retries),
            )
            .build();
        Ok(conf)
    }

    pub fn from_env() -> Result<Self> {
        fn env_var(key: &str) -> Result<String> {
            match std::env::var(key) {
                Ok(res) => Ok(res),
                Err(_) => bail!("{} environment variable not set", key),
            }
        }
        fn env_var_or<S: ToString>(key: &str, default_value: S) -> String {
            match std::env::var(key) {
                Ok(res) => res,
                Err(_) => default_value.to_string(),
            }
        }

        let db_id = env_var("LIBSQL_BOTTOMLESS_DATABASE_ID").ok();
        let aws_endpoint = env_var("LIBSQL_BOTTOMLESS_ENDPOINT").ok();
        let bucket_name = env_var_or("LIBSQL_BOTTOMLESS_BUCKET", "bottomless");
        let max_batch_interval = Duration::from_secs(
            env_var_or("LIBSQL_BOTTOMLESS_BATCH_INTERVAL_SECS", 15).parse::<u64>()?,
        );
        let access_key_id = env_var("LIBSQL_BOTTOMLESS_AWS_ACCESS_KEY_ID").ok();
        let secret_access_key = env_var("LIBSQL_BOTTOMLESS_AWS_SECRET_ACCESS_KEY").ok();
        let region = env_var("LIBSQL_BOTTOMLESS_AWS_DEFAULT_REGION").ok();
        let max_frames_per_batch =
            env_var_or("LIBSQL_BOTTOMLESS_BATCH_MAX_FRAMES", 10000).parse::<usize>()?;
        let s3_upload_max_parallelism =
            env_var_or("LIBSQL_BOTTOMLESS_S3_PARALLEL_MAX", 32).parse::<usize>()?;
        let restore_transaction_page_swap_after =
            env_var_or("LIBSQL_BOTTOMLESS_RESTORE_TXN_SWAP_THRESHOLD", 1000).parse::<u32>()?;
        let restore_transaction_cache_fpath =
            env_var_or("LIBSQL_BOTTOMLESS_RESTORE_TXN_FILE", ".bottomless.restore");
        let use_compression =
            CompressionKind::parse(&env_var_or("LIBSQL_BOTTOMLESS_COMPRESSION", "zstd"))
                .map_err(|e| anyhow!("unknown compression kind: {}", e))?;
        let verify_crc = match env_var_or("LIBSQL_BOTTOMLESS_VERIFY_CRC", true)
            .to_lowercase()
            .as_ref()
        {
            "yes" | "true" | "1" | "y" | "t" => true,
            "no" | "false" | "0" | "n" | "f" => false,
            other => bail!(
                "Invalid LIBSQL_BOTTOMLESS_VERIFY_CRC environment variable: {}",
                other
            ),
        };
        let s3_max_retries = env_var_or("LIBSQL_BOTTOMLESS_S3_MAX_RETRIES", 10).parse::<u32>()?;
        Ok(Options {
            db_id,
            create_bucket_if_not_exists: true,
            verify_crc,
            use_compression,
            max_batch_interval,
            max_frames_per_batch,
            s3_upload_max_parallelism,
            restore_transaction_page_swap_after,
            aws_endpoint,
            access_key_id,
            secret_access_key,
            region,
            restore_transaction_cache_fpath,
            bucket_name,
            s3_max_retries,
        })
    }
}

impl Replicator {
    pub const UNSET_PAGE_SIZE: usize = usize::MAX;

    pub async fn new<S: Into<String>>(db_path: S) -> Result<Self> {
        Self::with_options(db_path, Options::from_env()?).await
    }

    pub async fn with_options<S: Into<String>>(db_path: S, options: Options) -> Result<Self> {
        let config = options.client_config().await?;
        let client = Client::from_conf(config);
        let bucket = options.bucket_name.clone();
        let generation = Arc::new(ArcSwapOption::default());

        match client.head_bucket().bucket(&bucket).send().await {
            Ok(_) => tracing::info!("Bucket {} exists and is accessible", bucket),
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => {
                if options.create_bucket_if_not_exists {
                    tracing::info!("Bucket {} not found, recreating", bucket);
                    client.create_bucket().bucket(&bucket).send().await?;
                } else {
                    tracing::error!("Bucket {} does not exist", bucket);
                    return Err(SdkError::ServiceError(err).into());
                }
            }
            Err(e) => {
                tracing::error!("Bucket checking error: {}", e);
                return Err(e.into());
            }
        }

        let db_path = db_path.into();
        let db_name = if let Some(db_id) = options.db_id.clone() {
            db_id
        } else {
            bail!("database id was not set")
        };
        tracing::debug!("Database path: '{}', name: '{}'", db_path, db_name);

        let (flush_trigger, mut flush_trigger_rx) = channel(());
        let (last_committed_frame_no_sender, last_committed_frame_no) = channel(Ok(0));

        let next_frame_no = Arc::new(AtomicU32::new(1));
        let last_sent_frame_no = Arc::new(AtomicU32::new(0));

        let mut _join_set = JoinSet::new();

        let (frames_outbox, mut frames_inbox) = tokio::sync::mpsc::channel(64);
        let _local_backup = {
            let mut copier = WalCopier::new(
                bucket.clone(),
                db_name.clone().into(),
                generation.clone(),
                &db_path,
                options.max_frames_per_batch,
                options.use_compression,
                frames_outbox,
            );
            let next_frame_no = next_frame_no.clone();
            let last_sent_frame_no = last_sent_frame_no.clone();
            let batch_interval = options.max_batch_interval;
            _join_set.spawn(async move {
                loop {
                    let timeout = Instant::now() + batch_interval;
                    let trigger = match timeout_at(timeout, flush_trigger_rx.changed()).await {
                        Ok(Ok(())) => true,
                        Ok(Err(_)) => {
                            return;
                        }
                        Err(_) => {
                            true // timeout reached
                        }
                    };
                    if trigger {
                        let next_frame = next_frame_no.load(Ordering::Acquire);
                        let last_sent_frame =
                            last_sent_frame_no.swap(next_frame - 1, Ordering::Acquire);
                        let frames = (last_sent_frame + 1)..next_frame;

                        if !frames.is_empty() {
                            let res = copier.flush(frames).await;
                            if last_committed_frame_no_sender.send(res).is_err() {
                                // Replicator was probably dropped and therefore corresponding
                                // receiver has been closed
                                return;
                            }
                        }
                    }
                }
            })
        };

        let _s3_upload = {
            let client = client.clone();
            let bucket = options.bucket_name.clone();
            let max_parallelism = options.s3_upload_max_parallelism;
            _join_set.spawn(async move {
                let sem = Arc::new(tokio::sync::Semaphore::new(max_parallelism));
                let mut join_set = JoinSet::new();
                while let Some(fdesc) = frames_inbox.recv().await {
                    tracing::trace!("Received S3 upload request: {}", fdesc);
                    let start = Instant::now();
                    let sem = sem.clone();
                    let permit = sem.acquire_owned().await.unwrap();
                    let client = client.clone();
                    let bucket = bucket.clone();
                    join_set.spawn(async move {
                        let fpath = format!("{}/{}", bucket, fdesc);
                        let body = ByteStream::from_path(&fpath).await.unwrap();
                        if let Err(e) = client
                            .put_object()
                            .bucket(bucket)
                            .key(fdesc)
                            .body(body)
                            .send()
                            .await
                        {
                            tracing::error!("Failed to send {} to S3: {}", fpath, e);
                        } else {
                            tokio::fs::remove_file(&fpath).await.unwrap();
                            let elapsed = Instant::now() - start;
                            tracing::debug!("Uploaded to S3: {} in {:?}", fpath, elapsed);
                        }
                        drop(permit);
                    });
                }
            })
        };
        let (snapshot_notifier, snapshot_waiter) = channel(Ok(None));
        Ok(Self {
            client,
            bucket,
            page_size: Self::UNSET_PAGE_SIZE,
            generation,
            next_frame_no,
            last_sent_frame_no,
            flush_trigger,
            last_committed_frame_no,
            verify_crc: options.verify_crc,
            db_path,
            db_name,
            snapshot_waiter,
            snapshot_notifier: Arc::new(snapshot_notifier),
            restore_transaction_page_swap_after: options.restore_transaction_page_swap_after,
            restore_transaction_cache_fpath: options.restore_transaction_cache_fpath.into(),
            use_compression: options.use_compression,
            max_frames_per_batch: options.max_frames_per_batch,
            s3_upload_max_parallelism: options.s3_upload_max_parallelism,
            _join_set,
        })
    }

    pub fn next_frame_no(&self) -> u32 {
        self.next_frame_no.load(Ordering::Acquire)
    }

    pub fn last_known_frame(&self) -> u32 {
        self.next_frame_no() - 1
    }

    pub fn last_sent_frame_no(&self) -> u32 {
        self.last_sent_frame_no.load(Ordering::Acquire)
    }

    pub fn compression_kind(&self) -> CompressionKind {
        self.use_compression
    }

    pub async fn wait_until_snapshotted(&mut self) -> Result<bool> {
        if let Ok(generation) = self.generation() {
            if !self.main_db_exists_and_not_empty().await {
                tracing::debug!("Not snapshotting, the main db file does not exist or is empty");
                let _ = self.snapshot_notifier.send(Ok(Some(generation)));
                return Ok(false);
            }
            tracing::debug!("waiting for generation snapshot {} to complete", generation);
            let res = self
                .snapshot_waiter
                .wait_for(|result| match result {
                    Ok(Some(gen)) => *gen == generation,
                    Ok(None) => false,
                    Err(_) => true,
                })
                .await?;
            match res.deref() {
                Ok(_) => Ok(true),
                Err(e) => Err(anyhow!("Failed snapshot generation {}: {}", generation, e)),
            }
        } else {
            Ok(false)
        }
    }

    /// Waits until the commit for a given frame_no or higher was given.
    pub async fn wait_until_committed(&mut self, frame_no: u32) -> Result<u32> {
        let res = self
            .last_committed_frame_no
            .wait_for(|result| match result {
                Ok(last_committed) => *last_committed >= frame_no,
                Err(_) => true,
            })
            .await?;

        match res.deref() {
            Ok(last_committed) => {
                tracing::trace!(
                    "Confirmed commit of frame no. {} (waited for >= {})",
                    last_committed,
                    frame_no
                );
                Ok(*last_committed)
            }
            Err(e) => Err(anyhow!("Failed to flush frames: {}", e)),
        }
    }

    /// Returns number of frames waiting to be replicated.
    pub fn pending_frames(&self) -> u32 {
        self.next_frame_no() - self.last_sent_frame_no() - 1
    }

    // The database can use different page size - as soon as it's known,
    // it should be communicated to the replicator via this call.
    // NOTICE: in practice, WAL journaling mode does not allow changing page sizes,
    // so verifying that it hasn't changed is a panic check. Perhaps in the future
    // it will be useful, if WAL ever allows changing the page size.
    pub fn set_page_size(&mut self, page_size: usize) -> Result<()> {
        if self.page_size != page_size {
            tracing::trace!("Setting page size to: {}", page_size);
        }
        if self.page_size != Self::UNSET_PAGE_SIZE && self.page_size != page_size {
            return Err(anyhow::anyhow!(
                "Cannot set page size to {}, it was already set to {}",
                page_size,
                self.page_size
            ));
        }
        self.page_size = page_size;
        Ok(())
    }

    // Gets an object from the current bucket
    fn get_object(&self, key: String) -> GetObjectFluentBuilder {
        self.client.get_object().bucket(&self.bucket).key(key)
    }

    // Lists objects from the current bucket
    fn list_objects(&self) -> ListObjectsFluentBuilder {
        self.client.list_objects().bucket(&self.bucket)
    }

    fn reset_frames(&mut self, frame_no: u32) {
        let last_sent = self.last_sent_frame_no();
        self.next_frame_no.store(frame_no + 1, Ordering::Release);
        self.last_sent_frame_no
            .store(last_sent.min(frame_no), Ordering::Release);
    }

    // Generates a new generation UUID v7, which contains a timestamp and is binary-sortable.
    // This timestamp goes back in time - that allows us to list newest generations
    // first in the S3-compatible bucket, under the assumption that fetching newest generations
    // is the most common operation.
    // NOTICE: at the time of writing, uuid v7 is an unstable feature of the uuid crate
    fn generate_generation() -> Uuid {
        let ts = uuid::timestamp::Timestamp::now(uuid::NoContext);
        Self::generation_from_timestamp(ts)
    }

    fn generation_from_timestamp(ts: uuid::Timestamp) -> Uuid {
        let (seconds, nanos) = ts.to_unix();
        let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
        let synthetic_ts = uuid::Timestamp::from_unix(uuid::NoContext, seconds, nanos);
        crate::uuid_utils::new_v7(synthetic_ts)
    }

    pub fn generation_to_timestamp(generation: &Uuid) -> Option<uuid::Timestamp> {
        let ts = decode_unix_timestamp(generation);
        let (seconds, nanos) = ts.to_unix();
        let (seconds, nanos) = (253370761200 - seconds, 999999999 - nanos);
        Some(uuid::Timestamp::from_unix(NoContext, seconds, nanos))
    }

    // Starts a new generation for this replicator instance
    pub fn new_generation(&mut self) -> Option<Uuid> {
        let curr = Self::generate_generation();
        let prev = self.set_generation(curr);
        if let Some(prev) = prev {
            if prev != curr {
                // try to store dependency between previous and current generation
                tracing::trace!("New generation {} (parent: {})", curr, prev);
                self.store_dependency(prev, curr)
            }
        }
        prev
    }

    // Sets a generation for this replicator instance. This function
    // should be called if a generation number from S3-compatible storage
    // is reused in this session.
    pub fn set_generation(&mut self, generation: Uuid) -> Option<Uuid> {
        let prev_generation = self.generation.swap(Some(Arc::new(generation)));
        self.reset_frames(0);
        if let Some(prev) = prev_generation.as_deref() {
            tracing::debug!("Generation changed from {} -> {}", prev, generation);
            Some(*prev)
        } else {
            tracing::debug!("Generation set {}", generation);
            None
        }
    }

    pub fn generation(&self) -> Result<Uuid> {
        let guard = self.generation.load();
        guard
            .as_deref()
            .cloned()
            .ok_or(anyhow!("Replicator generation was not initialized"))
    }

    /// Request to store dependency between current generation and its predecessor on S3 object.
    /// This works asynchronously on best-effort rules, as putting object to S3 introduces an
    /// extra undesired latency and this method may be called during SQLite checkpoint.
    fn store_dependency(&self, prev: Uuid, curr: Uuid) {
        let key = format!("{}-{}/.dep", self.db_name, curr);
        let request =
            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(ByteStream::from(Bytes::copy_from_slice(
                    prev.into_bytes().as_slice(),
                )));
        tokio::spawn(async move {
            if let Err(e) = request.send().await {
                tracing::error!(
                    "Failed to store dependency between generations {} -> {}: {}",
                    prev,
                    curr,
                    e
                );
            } else {
                tracing::trace!(
                    "Stored dependency between parent ({}) and child ({})",
                    prev,
                    curr
                );
            }
        });
    }

    pub async fn get_dependency(&self, generation: &Uuid) -> Result<Option<Uuid>> {
        let key = format!("{}-{}/.dep", self.db_name, generation);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        match resp {
            Ok(out) => {
                let bytes = out.body.collect().await?.into_bytes();
                let prev_generation = Uuid::from_bytes(bytes.as_ref().try_into()?);
                Ok(Some(prev_generation))
            }
            Err(SdkError::ServiceError(se)) => match se.into_err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }

    // Returns the current last valid frame in the replicated log
    pub fn peek_last_valid_frame(&self) -> u32 {
        self.next_frame_no().saturating_sub(1)
    }

    // Sets the last valid frame in the replicated log.
    pub fn register_last_valid_frame(&mut self, frame: u32) {
        let last_valid_frame = self.peek_last_valid_frame();
        if frame != last_valid_frame {
            // If frame >=  last_valid_frame, it comes from a transaction large enough
            // that it got split to multiple xFrames calls. In this case, we just
            // update the last_valid_frame to this one, all good.
            if last_valid_frame != 0 && frame < last_valid_frame {
                tracing::error!(
                    "[BUG] Local max valid frame is {}, while replicator thinks it's {}",
                    frame,
                    last_valid_frame
                );
            }
            self.reset_frames(frame);
        }
    }

    /// Submit next `frame_count` of frames to be replicated.
    pub fn submit_frames(&mut self, frame_count: u32) {
        let prev = self.next_frame_no.fetch_add(frame_count, Ordering::SeqCst);
        let last_sent = self.last_sent_frame_no();
        let most_recent = prev + frame_count - 1;
        if most_recent - last_sent >= self.max_frames_per_batch as u32 {
            self.request_flush();
        }
    }

    pub fn request_flush(&self) {
        tracing::trace!("Requesting flush");
        let _ = self.flush_trigger.send(());
    }

    // Drops uncommitted frames newer than given last valid frame
    pub fn rollback_to_frame(&mut self, last_valid_frame: u32) {
        // NOTICE: O(size), can be optimized to O(removed) if ever needed
        self.reset_frames(last_valid_frame);
        tracing::debug!("Rolled back to {}", last_valid_frame);
    }

    // Tries to read the local change counter from the given database file
    async fn read_change_counter(reader: &mut File) -> Result<[u8; 4]> {
        let mut counter = [0u8; 4];
        reader.seek(std::io::SeekFrom::Start(24)).await?;
        reader.read_exact(&mut counter).await?;
        Ok(counter)
    }

    // Tries to read the local page size from the given database file
    async fn read_page_size(reader: &mut File) -> Result<usize> {
        reader.seek(SeekFrom::Start(16)).await?;
        let page_size = reader.read_u16().await?;
        if page_size == 1 {
            Ok(65536)
        } else {
            Ok(page_size as usize)
        }
    }

    // Returns the compressed database file path and its change counter, extracted
    // from the header of page1 at offset 24..27 (as per SQLite documentation).
    pub async fn maybe_compress_main_db_file(
        db_path: &Path,
        compression: CompressionKind,
    ) -> Result<ByteStream> {
        match compression {
            CompressionKind::None => Ok(ByteStream::from_path(db_path).await?),
            CompressionKind::Gzip => {
                let mut reader = File::open(db_path).await?;
                let gzip_path = Self::db_compressed_path(db_path, "gz");
                let compressed_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&gzip_path)
                    .await?;
                let mut writer = GzipEncoder::new(compressed_file);
                let size = tokio::io::copy(&mut reader, &mut writer).await?;
                writer.shutdown().await?;
                tracing::debug!(
                    "Compressed database file ({} bytes) into `{}`",
                    size,
                    gzip_path.display()
                );
                Ok(ByteStream::from_path(gzip_path).await?)
            }
            CompressionKind::Zstd => {
                let mut reader = File::open(db_path).await?;
                let zstd_path = Self::db_compressed_path(db_path, "zstd");
                let compressed_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .truncate(true)
                    .open(&zstd_path)
                    .await?;
                let mut writer = ZstdEncoder::new(compressed_file);
                let size = tokio::io::copy(&mut reader, &mut writer).await?;
                writer.shutdown().await?;
                tracing::debug!(
                    "Compressed database file ({} bytes) into `{}`",
                    size,
                    zstd_path.display()
                );
                Ok(ByteStream::from_path(zstd_path).await?)
            }
        }
    }

    fn db_compressed_path(db_path: &Path, suffix: &'static str) -> PathBuf {
        let mut compressed_path: PathBuf = db_path.to_path_buf();
        compressed_path.pop();
        compressed_path.join(format!("db.{suffix}"))
    }

    fn restore_db_path(&self) -> PathBuf {
        let mut gzip_path = PathBuf::from(&self.db_path);
        gzip_path.pop();
        gzip_path.join("data.tmp")
    }

    // Replicates local WAL pages to S3, if local WAL is present.
    // This function is called under the assumption that if local WAL
    // file is present, it was already detected to be newer than its
    // remote counterpart.
    pub async fn maybe_replicate_wal(&mut self) -> Result<()> {
        let wal = match WalFileReader::open(&format!("{}-wal", &self.db_path)).await {
            Ok(Some(file)) => file,
            _ => {
                tracing::info!("Local WAL not present - not replicating");
                return Ok(());
            }
        };

        self.store_metadata(wal.page_size(), wal.checksum()).await?;

        let frame_count = wal.frame_count().await;
        tracing::trace!("Local WAL pages: {}", frame_count);
        self.submit_frames(frame_count);
        self.request_flush();
        let last_written_frame = self.wait_until_committed(frame_count - 1).await?;
        tracing::info!("Backed up WAL frames up to {}", last_written_frame);
        let pending_frames = self.pending_frames();
        if pending_frames != 0 {
            tracing::warn!(
                "Uncommitted WAL entries: {} frames in total",
                pending_frames
            );
        }
        tracing::info!("Local WAL replicated");
        Ok(())
    }

    // Check if the local database file exists and contains data
    async fn main_db_exists_and_not_empty(&self) -> bool {
        let file = match File::open(&self.db_path).await {
            Ok(file) => file,
            Err(_) => return false,
        };
        match file.metadata().await {
            Ok(metadata) => metadata.len() > 0,
            Err(_) => false,
        }
    }

    pub fn skip_snapshot_for_current_generation(&self) {
        let generation = self.generation.load().as_deref().cloned();
        let _ = self.snapshot_notifier.send(Ok(generation));
    }

    // Sends the main database file to S3 - if -wal file is present, it's replicated
    // too - it means that the local file was detected to be newer than its remote
    // counterpart.
    pub async fn snapshot_main_db_file(&mut self) -> Result<Option<JoinHandle<()>>> {
        if !self.main_db_exists_and_not_empty().await {
            let generation = self.generation()?;
            tracing::debug!(
                "Not snapshotting {}, the main db file does not exist or is empty",
                generation
            );
            let _ = self.snapshot_notifier.send(Ok(Some(generation)));
            return Ok(None);
        }
        let generation = self.generation()?;
        let start_ts = Instant::now();
        let client = self.client.clone();
        let change_counter = {
            let mut db_file = File::open(&self.db_path).await?;
            Self::read_change_counter(&mut db_file).await?
        };
        let snapshot_req = client.put_object().bucket(self.bucket.clone()).key(format!(
            "{}-{}/db.{}",
            self.db_name, generation, self.use_compression
        ));

        /* FIXME: we can't rely on the change counter in WAL mode:
         ** "In WAL mode, changes to the database are detected using the wal-index and
         ** so the change counter is not needed. Hence, the change counter might not be
         ** incremented on each transaction in WAL mode."
         ** Instead, we need to consult WAL checksums.
         */
        let change_counter_key = format!("{}-{}/.changecounter", self.db_name, generation);
        let change_counter_req = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(change_counter_key)
            .body(ByteStream::from(Bytes::copy_from_slice(
                change_counter.as_ref(),
            )));
        let snapshot_notifier = self.snapshot_notifier.clone();
        let compression = self.use_compression;
        let db_path = PathBuf::from(self.db_path.clone());
        let handle = tokio::spawn(async move {
            tracing::trace!("Start snapshotting generation {}", generation);
            let start = Instant::now();
            let body = match Self::maybe_compress_main_db_file(&db_path, compression).await {
                Ok(file) => file,
                Err(e) => {
                    tracing::error!(
                        "Failed to compress db file (generation {}): {:?}",
                        generation,
                        e
                    );
                    let _ = snapshot_notifier.send(Err(e));
                    return;
                }
            };
            let mut result = snapshot_req.body(body).send().await;
            if let Err(e) = result {
                tracing::error!(
                    "Failed to upload snapshot for generation {}: {:?}",
                    generation,
                    e
                );
                let _ = snapshot_notifier.send(Err(e.into()));
                return;
            }
            result = change_counter_req.send().await;
            if let Err(e) = result {
                tracing::error!(
                    "Failed to upload change counter for generation {}: {:?}",
                    generation,
                    e
                );
                let _ = snapshot_notifier.send(Err(e.into()));
                return;
            }
            let _ = snapshot_notifier.send(Ok(Some(generation)));
            let elapsed = Instant::now() - start;
            tracing::debug!("Snapshot upload finished (took {:?})", elapsed);
            // cleanup gzip/zstd database snapshot if exists
            for suffix in &["gz", "zstd"] {
                let _ = tokio::fs::remove_file(Self::db_compressed_path(&db_path, suffix)).await;
            }
        });
        let elapsed = Instant::now() - start_ts;
        tracing::debug!("Scheduled DB snapshot {} (took {:?})", generation, elapsed);

        Ok(Some(handle))
    }

    // Returns newest replicated generation, or None, if one is not found.
    // FIXME: assumes that this bucket stores *only* generations for databases,
    // it should be more robust and continue looking if the first item does not
    // match the <db-name>-<generation-uuid>/ pattern.
    pub async fn latest_generation_before(
        &self,
        timestamp: Option<&NaiveDateTime>,
    ) -> Option<Uuid> {
        let mut next_marker: Option<String> = None;
        let prefix = format!("{}-", self.db_name);
        let threshold = timestamp.map(|ts| ts.timestamp() as u64);
        loop {
            let mut request = self.list_objects().prefix(prefix.clone());
            if threshold.is_none() {
                request = request.max_keys(1);
            }
            if let Some(marker) = next_marker.take() {
                request = request.marker(marker);
            }
            let response = request.send().await.ok()?;
            let objs = response.contents()?;
            if objs.is_empty() {
                break;
            }
            let mut last_key = None;
            let mut last_gen = None;
            for obj in objs {
                let key = obj.key();
                last_key = key;
                if let Some(key) = last_key {
                    let key = match key.find('/') {
                        Some(index) => &key[self.db_name.len() + 1..index],
                        None => key,
                    };
                    if Some(key) != last_gen {
                        last_gen = Some(key);
                        if let Ok(generation) = Uuid::parse_str(key) {
                            match threshold.as_ref() {
                                None => return Some(generation),
                                Some(threshold) => match Self::generation_to_timestamp(&generation)
                                {
                                    None => {
                                        tracing::warn!(
                                            "Generation {} is not valid UUID v7",
                                            generation
                                        );
                                    }
                                    Some(ts) => {
                                        let (unix_seconds, _) = ts.to_unix();
                                        if tracing::enabled!(tracing::Level::DEBUG) {
                                            let ts = Utc
                                                .timestamp_millis_opt((unix_seconds * 1000) as i64)
                                                .unwrap()
                                                .to_rfc3339();
                                            tracing::debug!(
                                                "Generation candidate: {} - timestamp: {}",
                                                generation,
                                                ts
                                            );
                                        }
                                        if &unix_seconds <= threshold {
                                            return Some(generation);
                                        }
                                    }
                                },
                            }
                        }
                    }
                }
            }
            next_marker = last_key.map(String::from);
        }
        None
    }

    // Tries to fetch the remote database change counter from given generation
    pub async fn get_remote_change_counter(&self, generation: &Uuid) -> Result<[u8; 4]> {
        let mut remote_change_counter = [0u8; 4];
        if let Ok(response) = self
            .get_object(format!("{}-{}/.changecounter", self.db_name, generation))
            .send()
            .await
        {
            response
                .body
                .collect()
                .await?
                .copy_to_slice(&mut remote_change_counter)
        }
        Ok(remote_change_counter)
    }

    // Returns the number of pages stored in the local WAL file, or 0, if there aren't any.
    async fn get_local_wal_page_count(&mut self) -> u32 {
        match WalFileReader::open(&format!("{}-wal", &self.db_path)).await {
            Ok(None) => 0,
            Ok(Some(wal)) => {
                let page_size = wal.page_size();
                if self.set_page_size(page_size as usize).is_err() {
                    return 0;
                }
                wal.frame_count().await
            }
            Err(_) => 0,
        }
    }

    // Parses the frame and page number from given key.
    // Format: <db-name>-<generation>/<first-frame-no>-<last-frame-no>-<timestamp>.<compression-kind>
    pub fn parse_frame_range(key: &str) -> Option<(u32, u32, u64, CompressionKind)> {
        let frame_delim = key.rfind('/')?;
        let frame_suffix = &key[(frame_delim + 1)..];
        let timestamp_delim = frame_suffix.rfind('-')?;
        let last_frame_delim = frame_suffix[..timestamp_delim].rfind('-')?;
        let compression_delim = frame_suffix.rfind('.')?;
        let first_frame_no = frame_suffix[0..last_frame_delim].parse::<u32>().ok()?;
        let last_frame_no = frame_suffix[(last_frame_delim + 1)..timestamp_delim]
            .parse::<u32>()
            .ok()?;
        let timestamp = frame_suffix[(timestamp_delim + 1)..compression_delim]
            .parse::<u64>()
            .ok()?;
        let compression_kind =
            CompressionKind::parse(&frame_suffix[(compression_delim + 1)..]).ok()?;
        Some((first_frame_no, last_frame_no, timestamp, compression_kind))
    }

    /// Restores the database state from given remote generation
    /// On success, returns the RestoreAction, and whether the database was recovered from backup.
    async fn restore_from(
        &mut self,
        generation: Uuid,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(RestoreAction, bool)> {
        if let Some(tombstone) = self.get_tombstone().await? {
            if let Some(timestamp) = Self::generation_to_timestamp(&generation) {
                if tombstone.timestamp() as u64 >= timestamp.to_unix().0 {
                    bail!(
                        "Couldn't restore from generation {}. Database '{}' has been tombstoned at {}.",
                        generation,
                        self.db_name,
                        tombstone
                    );
                }
            }
        }

        let start_ts = Instant::now();
        // first check if there are any remaining files that we didn't manage to upload
        // on time in the last run
        self.upload_remaining_files(&generation).await?;

        let last_frame = self.get_last_consistent_frame(&generation).await?;
        tracing::debug!("Last consistent remote frame in generation {generation}: {last_frame}.");
        if let Some(action) = self.compare_with_local(generation, last_frame).await? {
            return Ok((action, false));
        }

        // at this point we know, we should do a full restore

        let restore_path = self.restore_db_path();
        let _ = tokio::fs::remove_file(&restore_path).await; // remove previous (failed) restoration
        match self
            .full_restore(&restore_path, generation, timestamp, last_frame)
            .await
        {
            Ok(result) => {
                let elapsed = Instant::now() - start_ts;
                tracing::info!("Finished database restoration in {:?}", elapsed);
                tokio::fs::rename(&restore_path, &self.db_path).await?;
                let _ = self.remove_wal_files().await; // best effort, WAL files may not exists
                Ok(result)
            }
            Err(e) => {
                tracing::error!("failed to restore the database: {}. Rollback", e);
                let _ = tokio::fs::remove_file(restore_path).await;
                Err(e)
            }
        }
    }

    async fn full_restore(
        &mut self,
        restore_path: &Path,
        generation: Uuid,
        timestamp: Option<NaiveDateTime>,
        last_frame: u32,
    ) -> Result<(RestoreAction, bool)> {
        tracing::debug!("Restoring database to `{}`", restore_path.display());
        let mut db = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(restore_path)
            .await?;

        let mut restore_stack = Vec::new();

        // If the db file is not present, the database could have been empty
        let mut current = Some(generation);
        while let Some(curr) = current.take() {
            // stash current generation - we'll use it to replay WAL across generations since the
            // last snapshot
            restore_stack.push(curr);
            let restored = self.restore_from_snapshot(&curr, &mut db).await?;
            if restored {
                break;
            } else {
                if restore_stack.len() > MAX_RESTORE_STACK_DEPTH {
                    bail!("Restoration failed: maximum number of generations to restore from was reached.");
                }
                tracing::debug!("No snapshot found on the generation {}", curr);
                // there was no snapshot to restore from, it means that we either:
                // 1. Have only WAL to restore from - case when we're at the initial generation
                //    of the database.
                // 2. Snapshot never existed - in that case try to reach for parent generation
                //    of the current one and read snapshot from there.
                current = self.get_dependency(&curr).await?;
                if let Some(prev) = &current {
                    tracing::debug!("Rolling restore back from generation {} to {}", curr, prev);
                }
            }
        }

        tracing::trace!(
            "Restoring database from {} generations",
            restore_stack.len()
        );

        let mut applied_wal_frame = false;
        while let Some(gen) = restore_stack.pop() {
            if let Some((page_size, checksum)) = self.get_metadata(&gen).await? {
                self.set_page_size(page_size as usize)?;
                let last_frame = if restore_stack.is_empty() {
                    // we're at the last generation to restore from, it may still being written to
                    // so we constraint the restore to a frame checked at the beginning of the
                    // restore procedure
                    Some(last_frame)
                } else {
                    None
                };
                self.restore_wal(
                    &gen,
                    page_size as usize,
                    last_frame,
                    checksum,
                    timestamp,
                    &mut db,
                )
                .await?;
                applied_wal_frame = true;
            } else {
                tracing::info!(".meta object not found, skipping WAL restore.");
            };
        }

        db.shutdown().await?;

        if applied_wal_frame {
            tracing::info!("WAL file has been applied onto database file in generation {}. Requesting snapshot.", generation);
            Ok::<_, anyhow::Error>((RestoreAction::SnapshotMainDbFile, true))
        } else {
            tracing::info!("Reusing generation {}.", generation);
            // since WAL was not applied, we can reuse the latest generation
            Ok::<_, anyhow::Error>((RestoreAction::ReuseGeneration(generation), true))
        }
    }

    /// Compares S3 generation backup state against current local database file to determine
    /// if we are up to date (returned restore action) or should we perform restoration.
    async fn compare_with_local(
        &mut self,
        generation: Uuid,
        last_consistent_frame: u32,
    ) -> Result<Option<RestoreAction>> {
        // Check if the database needs to be restored by inspecting the database
        // change counter and the WAL size.
        let local_counter = match File::open(&self.db_path).await {
            Ok(mut db) => {
                // While reading the main database file for the first time,
                // page size from an existing database should be set.
                if let Ok(page_size) = Self::read_page_size(&mut db).await {
                    self.set_page_size(page_size)?;
                }
                Self::read_change_counter(&mut db).await.unwrap_or([0u8; 4])
            }
            Err(_) => [0u8; 4],
        };

        if local_counter != [0u8; 4] {
            // if a non-empty database file exists always treat it as new and more up to date,
            // skipping the restoration process and calling for a new generation to be made
            return Ok(Some(RestoreAction::SnapshotMainDbFile));
        }

        let remote_counter = self.get_remote_change_counter(&generation).await?;
        tracing::debug!("Counters: l={:?}, r={:?}", local_counter, remote_counter);

        let wal_pages = self.get_local_wal_page_count().await;
        // We impersonate as a given generation, since we're comparing against local backup at that
        // generation. This is used later in [Self::new_generation] to create a dependency between
        // this generation and a new one.
        self.generation.store(Some(Arc::new(generation)));
        match local_counter.cmp(&remote_counter) {
            std::cmp::Ordering::Equal => {
                tracing::debug!(
                    "Consistent: {}; wal pages: {}",
                    last_consistent_frame,
                    wal_pages
                );
                match wal_pages.cmp(&last_consistent_frame) {
                    std::cmp::Ordering::Equal => {
                        tracing::info!(
                            "Remote generation is up-to-date, reusing it in this session"
                        );
                        self.reset_frames(wal_pages + 1);
                        Ok(Some(RestoreAction::ReuseGeneration(generation)))
                    }
                    std::cmp::Ordering::Greater => {
                        tracing::info!("Local change counter matches the remote one, but local WAL contains newer data from generation {}, which needs to be replicated.", generation);
                        Ok(Some(RestoreAction::SnapshotMainDbFile))
                    }
                    std::cmp::Ordering::Less => Ok(None),
                }
            }
            std::cmp::Ordering::Greater => {
                tracing::info!("Local change counter is larger than its remote counterpart - a new snapshot needs to be replicated (generation: {})", generation);
                Ok(Some(RestoreAction::SnapshotMainDbFile))
            }
            std::cmp::Ordering::Less => Ok(None),
        }
    }

    async fn restore_from_snapshot(&mut self, generation: &Uuid, db: &mut File) -> Result<bool> {
        let algos_to_try = match self.use_compression {
            CompressionKind::None => &[
                CompressionKind::None,
                CompressionKind::Zstd,
                CompressionKind::Gzip,
            ],
            CompressionKind::Gzip => &[
                CompressionKind::Gzip,
                CompressionKind::Zstd,
                CompressionKind::None,
            ],
            CompressionKind::Zstd => &[
                CompressionKind::Zstd,
                CompressionKind::Gzip,
                CompressionKind::None,
            ],
        };

        for algo in algos_to_try {
            let main_db_path = match algo {
                CompressionKind::None => format!("{}-{}/db.db", self.db_name, generation),
                CompressionKind::Gzip => format!("{}-{}/db.gz", self.db_name, generation),
                CompressionKind::Zstd => format!("{}-{}/db.zstd", self.db_name, generation),
            };
            if let Ok(db_file) = self.get_object(main_db_path).send().await {
                let mut body_reader = db_file.body.into_async_read();
                let db_size = match algo {
                    CompressionKind::None => tokio::io::copy(&mut body_reader, db).await?,
                    CompressionKind::Gzip => {
                        let mut decompress_reader =
                            async_compression::tokio::bufread::GzipDecoder::new(
                                tokio::io::BufReader::new(body_reader),
                            );
                        tokio::io::copy(&mut decompress_reader, db).await?
                    }
                    CompressionKind::Zstd => {
                        let mut decompress_reader =
                            async_compression::tokio::bufread::ZstdDecoder::new(
                                tokio::io::BufReader::new(body_reader),
                            );
                        tokio::io::copy(&mut decompress_reader, db).await?
                    }
                };
                db.flush().await?;

                let page_size = Self::read_page_size(db).await?;
                self.set_page_size(page_size)?;
                tracing::info!("Restored the main database file ({} bytes)", db_size);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn restore_wal(
        &self,
        generation: &Uuid,
        page_size: usize,
        last_consistent_frame: Option<u32>,
        mut checksum: (u32, u32),
        utc_time: Option<NaiveDateTime>,
        db: &mut File,
    ) -> Result<bool> {
        let prefix = format!("{}-{}/", self.db_name, generation);
        let mut page_buf = {
            let mut v = Vec::with_capacity(page_size);
            v.spare_capacity_mut();
            unsafe { v.set_len(page_size) };
            v
        };
        let mut next_marker = None;
        let mut applied_wal_frame = false;
        'restore_wal: loop {
            let mut list_request = self.list_objects().prefix(&prefix);
            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker);
            }
            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(objs) => objs,
                None => {
                    tracing::debug!("No objects found in generation {}", generation);
                    break;
                }
            };
            let mut pending_pages = TransactionPageCache::new(
                self.restore_transaction_page_swap_after,
                page_size as u32,
                self.restore_transaction_cache_fpath.clone(),
            );
            let mut last_received_frame_no = 0;
            for obj in objs {
                let key = obj
                    .key()
                    .ok_or_else(|| anyhow::anyhow!("Failed to get key for an object"))?;
                tracing::debug!("Loading {}", key);

                let (first_frame_no, last_frame_no, timestamp, compression_kind) =
                    match Self::parse_frame_range(key) {
                        Some(result) => result,
                        None => {
                            if !key.ends_with(".gz")
                                && !key.ends_with(".zstd")
                                && !key.ends_with(".db")
                                && !key.ends_with(".meta")
                                && !key.ends_with(".dep")
                                && !key.ends_with(".changecounter")
                            {
                                tracing::warn!("Failed to parse frame/page from key {}", key);
                            }
                            continue;
                        }
                    };
                if first_frame_no != last_received_frame_no + 1 {
                    tracing::warn!("Missing series of consecutive frames. Last applied frame: {}, next found: {}. Stopping the restoration process",
                            last_received_frame_no, first_frame_no);
                    break;
                }
                if let Some(frame) = last_consistent_frame {
                    if last_frame_no > frame {
                        tracing::warn!("Remote log contains frame {} larger than last consistent frame ({}), stopping the restoration process",
                                last_frame_no, frame);
                        break;
                    }
                }
                if let Some(threshold) = utc_time.as_ref() {
                    match NaiveDateTime::from_timestamp_opt(timestamp as i64, 0) {
                        Some(timestamp) => {
                            if &timestamp > threshold {
                                tracing::info!("Frame batch {} has timestamp more recent than expected {}. Stopping recovery.", key, timestamp);
                                break 'restore_wal; // reached end of restoration timestamp
                            }
                        }
                        _ => {
                            tracing::trace!("Couldn't parse requested frame batch {} timestamp. Stopping recovery.", key);
                            break 'restore_wal;
                        }
                    }
                }
                let frame = self.get_object(key.into()).send().await?;
                let mut frameno = first_frame_no;
                let mut reader = BatchReader::new(
                    frameno,
                    tokio_util::io::StreamReader::new(frame.body),
                    self.page_size,
                    compression_kind,
                );

                while let Some(frame) = reader.next_frame_header().await? {
                    let pgno = frame.pgno();
                    let page_size = self.page_size;
                    reader.next_page(&mut page_buf).await?;
                    if self.verify_crc {
                        checksum = frame.verify(checksum, &page_buf)?;
                    }
                    pending_pages.insert(pgno, &page_buf).await?;
                    if frame.is_committed() {
                        let pending_pages = std::mem::replace(
                            &mut pending_pages,
                            TransactionPageCache::new(
                                self.restore_transaction_page_swap_after,
                                page_size as u32,
                                self.restore_transaction_cache_fpath.clone(),
                            ),
                        );
                        pending_pages.flush(db).await?;
                        applied_wal_frame = true;
                    }
                    frameno += 1;
                    last_received_frame_no += 1;
                }
                db.flush().await?;
            }
            next_marker = response
                .is_truncated()
                .then(|| objs.last().map(|elem| elem.key().unwrap().to_string()))
                .flatten();
            if next_marker.is_none() {
                tracing::trace!("Restored DB from S3 backup using generation {}", generation);
                break;
            }
        }
        Ok(applied_wal_frame)
    }

    async fn remove_wal_files(&self) -> Result<()> {
        tracing::debug!("Overwriting any existing WAL file: {}-wal", &self.db_path);
        tokio::fs::remove_file(&format!("{}-wal", &self.db_path)).await?;
        tokio::fs::remove_file(&format!("{}-shm", &self.db_path)).await?;
        Ok(())
    }

    /// Restores the database state from newest remote generation
    /// On success, returns the RestoreAction, and whether the database was recovered from backup.
    pub async fn restore(
        &mut self,
        generation: Option<Uuid>,
        timestamp: Option<NaiveDateTime>,
    ) -> Result<(RestoreAction, bool)> {
        let generation = match generation {
            Some(gen) => gen,
            None => match self.latest_generation_before(timestamp.as_ref()).await {
                Some(gen) => gen,
                None => {
                    tracing::debug!("No generation found, nothing to restore");
                    return Ok((RestoreAction::SnapshotMainDbFile, false));
                }
            },
        };

        let (action, recovered) = self.restore_from(generation, timestamp).await?;
        tracing::info!(
            "Restoring from generation {generation}: action={action:?}, recovered={recovered}"
        );
        Ok((action, recovered))
    }

    pub async fn get_last_consistent_frame(&self, generation: &Uuid) -> Result<u32> {
        let prefix = format!("{}-{}/", self.db_name, generation);
        let mut marker: Option<String> = None;
        let mut last_frame = 0;
        while {
            let mut list_objects = self.list_objects().prefix(&prefix);
            if let Some(marker) = marker.take() {
                list_objects = list_objects.marker(marker);
            }
            let response = list_objects.send().await?;
            marker = Self::try_get_last_frame_no(response, &mut last_frame);
            marker.is_some()
        } {}
        Ok(last_frame)
    }

    fn try_get_last_frame_no(response: ListObjectsOutput, frame_no: &mut u32) -> Option<String> {
        let objs = response.contents()?;
        let mut last_key = None;
        for obj in objs.iter() {
            last_key = Some(obj.key()?);
            if let Some(key) = last_key {
                if let Some((_, last_frame_no, _, _)) = Self::parse_frame_range(key) {
                    *frame_no = last_frame_no;
                }
            }
        }
        last_key.map(String::from)
    }

    async fn upload_remaining_files(&self, generation: &Uuid) -> Result<()> {
        let prefix = format!("{}-{}", self.db_name, generation);
        let dir = format!("{}/{}-{}", self.bucket, self.db_name, generation);
        if tokio::fs::try_exists(&dir).await? {
            let mut files = tokio::fs::read_dir(&dir).await?;
            let sem = Arc::new(tokio::sync::Semaphore::new(self.s3_upload_max_parallelism));
            while let Some(file) = files.next_entry().await? {
                let fpath = file.path();
                if let Some(key) = Self::fpath_to_key(&fpath, &prefix) {
                    tracing::trace!("Requesting upload of the remaining backup file: {}", key);
                    let permit = sem.clone().acquire_owned().await?;
                    let bucket = self.bucket.clone();
                    let key = key.to_string();
                    let client = self.client.clone();
                    tokio::spawn(async move {
                        let body = ByteStream::from_path(&fpath).await.unwrap();
                        if let Err(e) = client
                            .put_object()
                            .bucket(bucket)
                            .key(key.clone())
                            .body(body)
                            .send()
                            .await
                        {
                            tracing::error!("Failed to send {} to S3: {}", key, e);
                        } else {
                            tokio::fs::remove_file(&fpath).await.unwrap();
                            tracing::trace!("Uploaded to S3: {}", key);
                        }
                        drop(permit);
                    });
                }
            }
            // wait for all started upload tasks to finish
            let _ = sem
                .acquire_many(self.s3_upload_max_parallelism as u32)
                .await?;
            if let Err(e) = tokio::fs::remove_dir(&dir).await {
                tracing::warn!("Couldn't remove backed up directory {}: {}", dir, e);
            }
        }
        Ok(())
    }

    fn fpath_to_key<'a>(fpath: &'a Path, dir: &str) -> Option<&'a str> {
        let str = fpath.to_str()?;
        if str.ends_with(".db")
            | str.ends_with(".gz")
            | str.ends_with(".zstd")
            | str.ends_with(".raw")
            | str.ends_with(".meta")
            | str.ends_with(".dep")
            | str.ends_with(".changecounter")
        {
            let idx = str.rfind(dir)?;
            return Some(&str[idx..]);
        }
        None
    }

    async fn store_metadata(&self, page_size: u32, checksum: (u32, u32)) -> Result<()> {
        let generation = self.generation()?;
        let key = format!("{}-{}/.meta", self.db_name, generation);
        tracing::debug!(
            "Storing metadata at '{}': page size - {}, crc - {},{}",
            key,
            page_size,
            checksum.0,
            checksum.1,
        );
        let mut body = Vec::with_capacity(12);
        body.extend_from_slice(page_size.to_be_bytes().as_slice());
        body.extend_from_slice(checksum.0.to_be_bytes().as_slice());
        body.extend_from_slice(checksum.1.to_be_bytes().as_slice());
        let _ = self
            .client
            .put_object()
            .bucket(self.bucket.clone())
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_metadata(&self, generation: &Uuid) -> Result<Option<(u32, (u32, u32))>> {
        let key = format!("{}-{}/.meta", self.db_name, generation);
        if let Ok(obj) = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            let mut data = obj.body.collect().await?;
            let page_size = data.get_u32();
            let checksum = (data.get_u32(), data.get_u32());
            Ok(Some((page_size, checksum)))
        } else {
            Ok(None)
        }
    }

    /// Marks current replicator database as deleted, invalidating all generations.
    pub async fn delete_all(&self, older_than: Option<NaiveDateTime>) -> Result<DeleteAll> {
        tracing::info!(
            "Called for tombstoning of all contents of the '{}' database",
            self.db_name
        );
        let key = format!("{}.tombstone", self.db_name);
        let threshold = older_than.unwrap_or(NaiveDateTime::MAX);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(
                threshold.timestamp().to_be_bytes().to_vec(),
            ))
            .send()
            .await?;
        let delete_task = DeleteAll::new(
            self.client.clone(),
            self.bucket.clone(),
            self.db_name.clone(),
            threshold,
        );
        Ok(delete_task)
    }

    /// Checks if current replicator database has been marked as deleted.
    pub async fn get_tombstone(&self) -> Result<Option<NaiveDateTime>> {
        let key = format!("{}.tombstone", self.db_name);
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await;
        match resp {
            Ok(out) => {
                let mut buf = [0u8; 8];
                out.body.collect().await?.copy_to_slice(&mut buf);
                let timestamp = i64::from_be_bytes(buf);
                let tombstone = NaiveDateTime::from_timestamp_opt(timestamp, 0);
                Ok(tombstone)
            }
            Err(SdkError::ServiceError(se)) => match se.into_err() {
                GetObjectError::NoSuchKey(_) => Ok(None),
                e => Err(e.into()),
            },
            Err(e) => Err(e.into()),
        }
    }
}

/// This structure is returned by [Replicator::delete_all] after tombstoning (soft deletion) has
/// been confirmed. It may be called using [DeleteAll::commit] to trigger a follow up procedure that
/// performs hard deletion of corresponding S3 objects.
#[derive(Debug)]
pub struct DeleteAll {
    client: Client,
    bucket: String,
    db_name: String,
    threshold: NaiveDateTime,
}

impl DeleteAll {
    fn new(client: Client, bucket: String, db_name: String, threshold: NaiveDateTime) -> Self {
        DeleteAll {
            client,
            bucket,
            db_name,
            threshold,
        }
    }

    pub fn threshold(&self) -> &NaiveDateTime {
        &self.threshold
    }

    /// Performs hard deletion of all bottomless generations older than timestamp provided in
    /// current request.
    pub async fn commit(self) -> Result<u32> {
        let mut next_marker = None;
        let mut removed_count = 0;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .set_delimiter(Some("/".to_string()))
                .prefix(&self.db_name);

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let prefixes = match response.common_prefixes() {
                Some(prefixes) => prefixes,
                None => {
                    tracing::debug!("no generations found to delete");
                    return Ok(0);
                }
            };

            for prefix in prefixes {
                if let Some(prefix) = &prefix.prefix {
                    let prefix = &prefix[self.db_name.len() + 1..prefix.len() - 1];
                    let uuid = Uuid::try_parse(prefix)?;
                    if let Some(datetime) = Replicator::generation_to_timestamp(&uuid) {
                        if datetime.to_unix().0 >= self.threshold.timestamp() as u64 {
                            continue;
                        }
                        tracing::debug!("Removing generation {}", uuid);
                        self.remove(uuid).await?;
                        removed_count += 1;
                    }
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                break;
            }
        }
        tracing::debug!("Removed {} generations", removed_count);
        self.remove_tombstone().await?;
        Ok(removed_count)
    }

    pub async fn remove_tombstone(&self) -> Result<()> {
        let key = format!("{}.tombstone", self.db_name);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    async fn remove(&self, generation: Uuid) -> Result<()> {
        let mut removed = 0;
        let mut next_marker = None;
        loop {
            let mut list_request = self
                .client
                .list_objects()
                .bucket(&self.bucket)
                .prefix(format!("{}-{}/", &self.db_name, generation));

            if let Some(marker) = next_marker {
                list_request = list_request.marker(marker)
            }

            let response = list_request.send().await?;
            let objs = match response.contents() {
                Some(prefixes) => prefixes,
                None => {
                    return Ok(());
                }
            };

            for obj in objs {
                if let Some(key) = obj.key() {
                    tracing::trace!("Removing {}", key);
                    self.client
                        .delete_object()
                        .bucket(&self.bucket)
                        .key(key)
                        .send()
                        .await?;
                    removed += 1;
                }
            }

            next_marker = response.next_marker().map(|s| s.to_owned());
            if next_marker.is_none() {
                tracing::trace!("Removed {} snapshot generations", removed);
                return Ok(());
            }
        }
    }
}

pub struct Context {
    pub replicator: Replicator,
    pub runtime: tokio::runtime::Runtime,
}

#[derive(Debug, Clone, Copy, Default, Ord, PartialOrd, Eq, PartialEq)]
pub enum CompressionKind {
    #[default]
    None,
    Gzip,
    Zstd,
}

impl CompressionKind {
    pub fn parse(kind: &str) -> std::result::Result<Self, &str> {
        match kind {
            "gz" | "gzip" => Ok(CompressionKind::Gzip),
            "raw" | "" => Ok(CompressionKind::None),
            "zstd" => Ok(CompressionKind::Zstd),
            other => Err(other),
        }
    }
}

impl std::fmt::Display for CompressionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionKind::None => write!(f, "raw"),
            CompressionKind::Gzip => write!(f, "gz"),
            CompressionKind::Zstd => write!(f, "zstd"),
        }
    }
}
