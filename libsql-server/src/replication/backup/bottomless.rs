use crate::replication::backup::{Backup, Restore, RestoreOptions};
use async_stream::try_stream;
use async_tempfile::{Error, TempFile};
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::{ready, Stream};
use libsql_replication::frame::FrameMut;
use libsql_replication::snapshot::{SnapshotFile, SnapshotFileHeader};
use opendal::raw::HttpClient;
use opendal::{Entry, Operator};
use std::cmp::Ordering;
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
pub struct BackupSession {
    db_id: Arc<str>,
    generation: Uuid,
    operator: Operator,
}

impl BackupSession {
    pub async fn open<O>(options: Options<O>) -> super::Result<Self>
    where
        O: OperatorOptions,
    {
        let operator = options.operator_options.create_operator()?;
        let mut session = BackupSession {
            db_id: options.db_id,
            generation: Uuid::v7_reversed(),
            operator,
        };
        session
            .setup_generation(options.restore.change_counter)
            .await?;
        Ok(session)
    }

    pub fn db_id(&self) -> &str {
        &self.db_id
    }

    pub fn current_generation(&self) -> &Uuid {
        &self.generation
    }

    async fn read_change_counter(&self, generation: &Uuid) -> super::Result<u64> {
        let path = self.change_counter_path(generation);
        let bytes = self.operator.read_with(&path).await?;
        let change_counter = u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| super::Error::ChangeCounterError(generation.clone()))?,
        );
        Ok(change_counter)
    }

    async fn write_change_counter(&self, change_counter: u64) -> super::Result<()> {
        let generation = self.current_generation();
        let path = self.change_counter_path(generation);
        self.operator
            .write_with(
                &path,
                Bytes::copy_from_slice(change_counter.to_be_bytes().as_slice()),
            )
            .await?;
        Ok(())
    }

    fn change_counter_path(&self, generation: &Uuid) -> String {
        let db_id = self.db_id();
        format!("{db_id}/{generation}/.changecounter")
    }

    async fn setup_generation(&mut self, change_counter: u64) -> super::Result<()> {
        let latest = self.latest_generation_with(None).await?;
        let generation = if let Some(generation) = latest {
            //FIXME: this generation may still be used by living process. We need to confirm that this generation is no
            //       longer used.
            let remote_change_counter = self.read_change_counter(&generation).await?;
            match change_counter.cmp(&remote_change_counter) {
                Ordering::Equal => {
                    tracing::info!("reusing previous backup generation `{generation}`");
                    generation
                }
                Ordering::Less => {
                    tracing::info!("remote change counter ({remote_change_counter}) is higher than local one ({change_counter}) - restore required.");
                    todo!() // current local database state is behind remote backup, we should restore
                }
                Ordering::Greater => {
                    tracing::info!("local change counter ({change_counter}) is higher than remote one ({remote_change_counter}). We need to snapshot database.");
                    todo!() // remote backup is behind local database state, we should start new generation and upload the database snapshot
                }
            }
        } else {
            let generation = Uuid::v7_reversed(); // timestamp in reversed order
            tracing::info!("created new backup generation `{generation}`");
            generation
        };
        self.generation = generation;
        Ok(())
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
            } else if let Some(generation) = self.latest_generation_with(Some(up_to)).await? {
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
                        EntryKind::ChangeCounter => {
                            let remote_change_counter = self.read_change_counter(&generation).await?;
                            let local_change_counter = options.change_counter;
                            if generation_stack.is_empty() && local_change_counter > remote_change_counter {
                                // this is the last generation, we expect that is should be more up-to-date than local replica
                                tracing::info!("local change counter ({local_change_counter}) is higher than remote ({remote_change_counter}) - skipping restoration.");
                                return;
                            }
                        }
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
        &self,
        up_to: Option<Timestamp>,
    ) -> super::Result<Option<Uuid>> {
        let db_id = self.db_id();
        let mut lister = self.operator.lister_with(&format!("{db_id}/")).await?;
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
impl Backup for BackupSession {
    async fn backup(&mut self, change_counter: u64, snapshot: SnapshotFile) -> super::Result<()> {
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

        self.write_change_counter(change_counter).await?;

        Ok(())
    }
}

#[async_trait]
impl Restore for BackupSession {
    type Stream = Pin<Box<dyn Stream<Item = super::Result<FrameMut>> + Send>>;

    async fn restore(&mut self, options: RestoreOptions) -> super::Result<Self::Stream> {
        let this = self.clone();
        Ok(Box::pin(this.restoration_stream(options)))
    }
}

#[derive(Debug, Clone)]
pub struct Options<O: OperatorOptions> {
    /// Options used to create a client to remote passive data store ie. AWS S3.
    pub operator_options: O,
    /// Unique database identifier.
    pub db_id: Arc<str>,
    /// Options used for database restoration.
    pub restore: RestoreOptions,
}

pub trait OperatorOptions: Clone {
    fn create_operator(&self) -> super::Result<Operator>;
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

impl OperatorOptions for S3Options {
    fn create_operator(&self) -> super::Result<Operator> {
        let mut builder = opendal::services::S3::default();
        builder.bucket(&self.bucket);
        builder.region(&self.region);
        if let Some(endpoint) = self.aws_endpoint.as_deref() {
            builder.endpoint(endpoint);
        }
        if let Some(access_key_id) = self.access_key_id.as_deref() {
            builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = self.secret_access_key.as_deref() {
            builder.secret_access_key(secret_access_key);
        }
        if let Some(http_client) = self.client.clone() {
            builder.http_client(http_client);
        }
        Ok(Operator::new(builder)?.finish())
    }
}

#[derive(Debug)]
pub enum EntryKind {
    Dependency,
    ChangeCounter,
    Snapshot {
        tier: u8,
        start_frame: u64,
        end_frame: u64,
        timestamp: Timestamp,
    },
}

impl EntryKind {
    pub fn parse(entry: &opendal::Entry) -> Result<Self, super::Error> {
        match entry.name() {
            ".dep" => Ok(Self::Dependency),
            ".changecounter" => Ok(Self::ChangeCounter),
            name => {
                if let Some(e) = Self::try_parse_snapshot_entry_name(name) {
                    Ok(e)
                } else {
                    Err(super::Error::SnapshotRestoreFailed(
                        entry.path().to_string(),
                    ))
                }
            }
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
