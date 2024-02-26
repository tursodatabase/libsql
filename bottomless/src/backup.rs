use crate::replicator::CompressionKind;
use crate::wal::WalFileReader;
use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwapOption;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct WalCopier {
    outbox: Sender<SendReq>,
    use_compression: CompressionKind,
    max_frames_per_batch: usize,
    wal_path: String,
    bucket: String,
    db_name: Arc<str>,
    generation: Arc<ArcSwapOption<Uuid>>,
}

impl WalCopier {
    pub fn new(
        bucket: String,
        db_name: Arc<str>,
        generation: Arc<ArcSwapOption<Uuid>>,
        db_path: &str,
        max_frames_per_batch: usize,
        use_compression: CompressionKind,
        outbox: Sender<SendReq>,
    ) -> Self {
        WalCopier {
            bucket,
            db_name,
            generation,
            wal_path: format!("{}-wal", db_path),
            outbox,
            max_frames_per_batch,
            use_compression,
        }
    }

    pub async fn flush(&mut self, frames: Range<u32>) -> Result<u32> {
        tracing::trace!("flushing frames [{}..{})", frames.start, frames.end);
        if frames.is_empty() {
            tracing::trace!("Trying to flush empty frame range");
            return Ok(frames.start - 1);
        }
        let mut wal = match WalFileReader::open(&self.wal_path).await? {
            Some(wal) => wal,
            None => return Err(anyhow!("WAL file not found: `{}`", self.wal_path)),
        };
        let generation = if let Some(generation) = self.generation.load_full() {
            generation
        } else {
            bail!("Generation has not been set");
        };
        let dir = format!("{}/{}-{}", self.bucket, self.db_name, generation);
        if frames.start == 1 {
            // before writing the first batch of frames - init directory
            // and store .meta object with basic info
            tracing::info!("initializing local backup directory: {:?}", dir);
            tokio::fs::create_dir_all(&dir).await?;
            let meta_path = format!("{}/.meta", dir);
            let mut meta_file = tokio::fs::File::create(&meta_path).await?;
            let buf = {
                let page_size = wal.page_size();
                let (checksum_1, checksum_2) = wal.checksum();
                let mut buf = [0u8; 12];
                buf[0..4].copy_from_slice(page_size.to_be_bytes().as_slice());
                buf[4..8].copy_from_slice(checksum_1.to_be_bytes().as_slice());
                buf[8..12].copy_from_slice(checksum_2.to_be_bytes().as_slice());
                buf
            };
            meta_file.write_all(buf.as_ref()).await?;
            meta_file.flush().await?;
            let msg = format!("{}-{}/.meta", self.db_name, generation);
            if self.outbox.send(SendReq::new(msg)).await.is_err() {
                return Err(anyhow!("couldn't initialize local backup dir: {}", dir));
            }
        }
        tracing::trace!("Flushing {} frames locally.", frames.len());

        for start in frames.clone().step_by(self.max_frames_per_batch) {
            let period_start = Instant::now();
            let timestamp = chrono::Utc::now().timestamp() as u64;
            let end = (start + self.max_frames_per_batch as u32).min(frames.end);
            let len = (end - start) as usize;
            let fdesc = format!(
                "{}-{}/{:012}-{:012}-{}.{}",
                self.db_name,
                generation,
                start,
                end - 1,
                timestamp, // generally timestamps fit in 10 chars but don't make assumptions
                self.use_compression
            );
            let mut out = tokio::fs::File::create(&format!("{}/{}", self.bucket, fdesc)).await?;

            wal.seek_frame(start).await?;
            match self.use_compression {
                CompressionKind::None => {
                    wal.copy_frames(&mut out, len).await?;
                    out.shutdown().await?;
                }
                CompressionKind::Gzip => {
                    let mut gzip = async_compression::tokio::write::GzipEncoder::new(&mut out);
                    wal.copy_frames(&mut gzip, len).await?;
                    gzip.shutdown().await?;
                }
                CompressionKind::Zstd => {
                    let mut zstd = async_compression::tokio::write::ZstdEncoder::new(&mut out);
                    wal.copy_frames(&mut zstd, len).await?;
                    zstd.shutdown().await?;
                }
            }
            if tracing::enabled!(tracing::Level::DEBUG) {
                let elapsed = Instant::now() - period_start;
                let file_len = out.metadata().await?.len();
                tracing::debug!("written {} bytes to {} in {:?}", file_len, fdesc, elapsed);
            }
            drop(out);
            if self
                .outbox
                .send(SendReq::wal_segment(fdesc, start, end - 1))
                .await
                .is_err()
            {
                tracing::warn!(
                    "WAL local cloning ended prematurely. Last cloned frame no.: {}",
                    end - 1
                );
                return Ok(end - 1);
            }
        }
        Ok(frames.end - 1)
    }
}

pub(crate) struct SendReq {
    /// Path to a file to be uploaded.
    pub path: String,
    /// If uploaded file refers to WAL segment, this field contains range of frames it contains.
    pub frames: Option<RangeInclusive<u32>>,
}

impl SendReq {
    pub fn new(path: String) -> Self {
        SendReq { path, frames: None }
    }
    /// Creates a send request for a WAL segment, given its file path and \[start,end] frames
    /// (both sides inclusive).
    pub fn wal_segment(path: String, start_frame: u32, end_frame: u32) -> Self {
        SendReq {
            path,
            frames: Some(start_frame..=end_frame),
        }
    }
}
