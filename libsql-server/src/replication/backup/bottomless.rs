use crate::replication::backup::{Backup, Restore, RestoreOptions};
use async_stream::try_stream;
use async_tempfile::{Error, TempFile};
use async_trait::async_trait;
use futures_core::{ready, Stream};
use libsql_replication::frame::FrameMut;
use libsql_replication::snapshot::{SnapshotFile, SnapshotFileHeader};
use opendal::raw::HttpClient;
use opendal::{Entry, Operator};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;
use tokio::pin;
use tokio_stream::StreamExt;
use uuid::{NoContext, Timestamp, Uuid};
use zerocopy::AsBytes;

#[derive(Debug, Clone)]
pub struct S3BackupService {
    db_id: Arc<str>,
    generation: Uuid,
    operator: Operator,
}

impl S3BackupService {
    pub async fn open_s3(options: Options) -> super::Result<Self> {
        let operator = Self::create_operator(&options.s3).await?;
        let generation = Self::setup_generation(&operator, &options.db_id).await?;

        Ok(S3BackupService {
            db_id: options.db_id,
            generation,
            operator,
        })
    }

    pub fn db_id(&self) -> &str {
        &self.db_id
    }

    pub fn current_generation(&self) -> &Uuid {
        &self.generation
    }

    async fn create_operator(client_options: &S3Options) -> super::Result<Operator> {
        let mut builder = opendal::services::S3::default();
        builder.bucket(&client_options.bucket);
        builder.region(&client_options.region);
        if let Some(endpoint) = client_options.aws_endpoint.as_deref() {
            builder.endpoint(endpoint);
        }
        if let Some(access_key_id) = client_options.access_key_id.as_deref() {
            builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = client_options.secret_access_key.as_deref() {
            builder.secret_access_key(secret_access_key);
        }
        if let Some(http_client) = client_options.client.clone() {
            builder.http_client(http_client);
        }
        Ok(Operator::new(builder)?.finish())
    }

    async fn setup_generation(o: &Operator, db_id: &str) -> super::Result<Uuid> {
        let latest = Self::latest_generation_with(o, db_id, None).await?;
        if let Some(generation) = latest {
            //FIXME: this generation may still be used by living process. We need to confirm that this generation is no
            //       longer used.
            tracing::info!("reusing previous backup generation `{generation}`");
            Ok(generation)
        } else {
            let generation = Uuid::v7_reversed(); // timestamp in reversed order
            tracing::info!("created new backup generation `{generation}`");
            Ok(generation)
        }
    }

    pub fn snapshot_path(&self, header: &SnapshotFileHeader, tier: u8) -> String {
        let db_id = self.db_id();
        let generation = self.current_generation();
        let tier = 9 - tier; // tiers are reverse ordered
        let start_frame = header.start_frame_no;
        let end_frame = header.end_frame_no;
        let timestamp_secs = Timestamp::now(NoContext).to_unix().0;
        format!("{db_id}/{generation}/{tier}-{start_frame:020}-{end_frame:020}-{timestamp_secs}")
    }

    pub fn current_generation_path_prefix(&self) -> String {
        let db_id = self.db_id();
        let generation = self.current_generation();
        format!("{db_id}/{generation}/")
    }

    pub fn restoration_stream(
        self,
        options: RestoreOptions,
    ) -> impl Stream<Item = super::Result<FrameMut>> {
        try_stream! {
            let up_to = options.point_in_time.unwrap_or(Timestamp::now(NoContext));
            let unix_secs = up_to.to_unix().0;
            let generation = if let Some(generation) = options.generation {
                generation
            } else if let Some(generation) = Self::latest_generation_with(&self.operator, self.db_id(), Some(up_to)).await? {
                generation
            } else {
                tracing::info!("stopping database restoration: no matching backup generation found");
                return; // there's no matching backup to restore from
            };
            tracing::info!("restoring database to {} using generation {}", display_date(unix_secs), generation);
            let mut generation_stack = vec![generation];
            let mut next_frame = 1;
            while let Some(generation) = generation_stack.pop() {
                let mut lister = self
                    .operator
                    .lister_with(&format!("{}/{}/", self.db_id(), generation))
                    .await?;
                while let Some(res) = lister.next().await {
                    let entry = res?;
                    tracing::trace!("restoring found entry: `{}`", entry.path());
                    match EntryKind::parse(&entry)? {
                        EntryKind::Dependency => {
                            drop(lister);
                            let parent = self.operator.read_with(entry.path()).await?;
                            let parent = Uuid::from_slice(&parent)?;
                            tracing::debug!("generation `{generation}` is dependent on `{parent}`");
                            generation_stack.push(generation);
                            generation_stack.push(parent);
                            break;
                        }
                        EntryKind::Snapshot {
                            timestamp,
                            ..
                        } => {
                            let timestamp_secs = timestamp.to_unix().0;
                            if timestamp_secs <= unix_secs {
                                let temp_file = self
                                    .into_snapshot_file(&entry)
                                    .await?;
                                let snapshot = SnapshotFile::open(temp_file.file_path()).await?;
                                let snapshot_stream = snapshot.into_stream_mut();
                                pin!(snapshot_stream);
                                while let Some(res) = snapshot_stream.next().await {
                                    let frame = res?;
                                    let frame_no: u64 = frame.header().frame_no.into();
                                    if frame_no != next_frame {
                                        Err(super::Error::MissingFrames(next_frame, frame_no))?;
                                    } else {
                                        next_frame += 1;
                                        yield frame;
                                    }
                                }
                            } else {
                                tracing::debug!("skipping snapshot {} - timestmap {} reached end of restoration period",
                                    entry.name(),
                                    display_date(timestamp_secs)
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    async fn latest_generation_with(
        operator: &Operator,
        db_id: &str,
        up_to: Option<Timestamp>,
    ) -> super::Result<Option<Uuid>> {
        let mut lister = operator.lister_with(&format!("{db_id}/")).await?;
        let ts = up_to.map(|ts| ts.to_unix().0).unwrap_or(u64::MAX);
        while let Some(res) = lister.next().await {
            let e = res?;
            let path = e.path();
            let mut slices = path.split('/');
            let _db_id = slices.next();
            if let Some(generation) = slices.next() {
                let generation = Uuid::parse_str(generation)?;
                let gen_timestamp = generation.timestamp_reversed();
                if gen_timestamp.to_unix().0 <= ts {
                    return Ok(Some(generation));
                }
            } else {
                break;
            }
        }
        Ok(None)
    }

    async fn into_snapshot_file(&self, entry: &Entry) -> super::Result<TempFile> {
        let mut reader = self.operator.reader_with(entry.path()).await?;
        let mut temp_file = TempFile::new().await.map_err(|e| match e {
            Error::Io(e) => e,
            Error::InvalidDirectory | Error::InvalidFile => {
                panic!("invalid file for temporary snapshot file")
            }
        })?;
        tokio::io::copy(&mut reader, &mut temp_file).await?;
        Ok(temp_file)
    }
}

#[async_trait]
impl Backup for S3BackupService {
    async fn backup(&mut self, snapshot: SnapshotFile) -> super::Result<()> {
        use tokio::io::AsyncWriteExt;
        /// Adapter between `tokio::io::AsyncRead` and `futures_core::AsyncRead`.
        struct AsyncReader(tokio::fs::File);
        impl futures::AsyncRead for AsyncReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<std::io::Result<usize>> {
                use tokio::io::AsyncRead;
                let pinned = unsafe { Pin::new_unchecked(&mut self.0) };
                let mut buf = ReadBuf::new(buf);
                let res = ready!(pinned.poll_read(cx, &mut buf));
                match res {
                    Ok(()) => Poll::Ready(Ok(buf.filled().len())),
                    Err(e) => Poll::Ready(Err(e.into())),
                }
            }
        }

        let header = snapshot.header();
        let path = self.snapshot_path(header, 0);
        let mut w = self.operator.writer(&path).await?;
        w.write_all(header.as_bytes()).await?;
        let file = AsyncReader(snapshot.into_file());
        w.copy(file).await?;
        w.shutdown().await?;
        Ok(())
    }
}

#[async_trait]
impl Restore for S3BackupService {
    type Stream = Pin<Box<dyn Stream<Item = super::Result<FrameMut>> + Send>>;

    async fn restore(&mut self, options: RestoreOptions) -> super::Result<Self::Stream> {
        let this = self.clone();
        Ok(Box::pin(this.restoration_stream(options)))
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    pub s3: S3Options,
    pub db_id: Arc<str>,
    pub db_path: PathBuf,
    pub restore: RestoreOptions,
}

#[derive(Debug, Clone)]
pub struct S3Options {
    pub client: Option<HttpClient>,
    pub aws_endpoint: Option<Arc<str>>,
    pub access_key_id: Option<Arc<str>>,
    pub secret_access_key: Option<Arc<str>>,
    pub region: Arc<str>,
    pub bucket: Arc<str>,
}

#[derive(Debug)]
pub enum EntryKind {
    Dependency,
    Snapshot {
        tier: u8,
        start_frame: u64,
        end_frame: u64,
        timestamp: Timestamp,
    },
}

impl EntryKind {
    pub fn parse(entry: &opendal::Entry) -> Result<Self, super::Error> {
        let name = entry.name();
        if name == ".dep" {
            Ok(Self::Dependency)
        } else if let Some(e) = Self::try_parse_snapshot_entry_name(name) {
            Ok(e)
        } else {
            Err(super::Error::SnapshotRestoreFailed(
                entry.path().to_string(),
            ))
        }
    }

    fn try_parse_snapshot_entry_name(name: &str) -> Option<EntryKind> {
        let mut slices = name.split('-');
        let tier = 9 - slices.next()?.parse::<u8>().ok()?;
        let start_frame = slices.next()?.parse::<u64>().ok()?;
        let end_frame = slices.next()?.parse::<u64>().ok()?;
        let unix_secs = slices.next()?.parse::<u64>().ok()?;
        let timestamp = Timestamp::from_unix(NoContext, unix_secs, 0);
        Some(EntryKind::Snapshot {
            tier,
            start_frame,
            end_frame,
            timestamp,
        })
    }
}

trait UuidExt {
    fn v7_reversed() -> Self;
    fn timestamp_reversed(&self) -> Timestamp;
}

impl UuidExt for Uuid {
    fn v7_reversed() -> Self {
        let (secs, nanos) = Timestamp::now(NoContext).to_unix();
        let timestamp = Timestamp::from_unix(NoContext, u64::MAX - secs, u32::MAX - nanos);
        Uuid::new_v7(timestamp)
    }

    fn timestamp_reversed(&self) -> Timestamp {
        let bytes = self.as_bytes();

        let millis: u64 = (bytes[0] as u64) << 40
            | (bytes[1] as u64) << 32
            | (bytes[2] as u64) << 24
            | (bytes[3] as u64) << 16
            | (bytes[4] as u64) << 8
            | (bytes[5] as u64);

        let seconds = u64::MAX - millis / 1000;
        Timestamp::from_unix(NoContext, seconds, 0)
    }
}

fn display_date(unix_seconds: u64) -> String {
    use chrono::{DateTime, NaiveDateTime, Utc};
    DateTime::<Utc>::from_naive_utc_and_offset(
        NaiveDateTime::from_timestamp_opt(unix_seconds as i64, 0).unwrap(),
        Utc,
    )
    .to_rfc3339()
}

#[cfg(test)]
mod test {}
