use crate::replication::backup::{Backup, Restore, RestoreOptions};
use async_stream::try_stream;
use async_tempfile::{Error, TempFile};
use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;
use libsql_replication::frame::FrameMut;
use libsql_replication::snapshot::{SnapshotFile, SnapshotFileHeader};
use opendal::raw::HttpClient;
use opendal::{Entry, Operator};
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::pin;
use uuid::{NoContext, Timestamp, Uuid};

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
            .setup_generation(options.restore.last_known_frame_no)
            .await?;
        Ok(session)
    }

    pub fn db_id(&self) -> &str {
        &self.db_id
    }

    pub fn current_generation(&self) -> &Uuid {
        &self.generation
    }

    /// Read last frame number in given `generation`.
    async fn last_frame_no(&self, generation: &Uuid) -> super::Result<u64> {
        let mut entries = self
            .operator
            .lister_with(&self.generation_path(generation))
            .await?;
        let mut frame_no = 0;
        while let Some(res) = entries.next().await {
            let entry = res?;
            if let Ok(EntryKind::Snapshot { end_frame, .. }) = EntryKind::parse(&entry) {
                frame_no = frame_no.max(end_frame);
            }
        }
        Ok(frame_no)
    }

    async fn setup_generation(&mut self, last_frame_no: u64) -> super::Result<()> {
        let latest = self.latest_generation_with(None).await?;
        if let Some(generation) = latest {
            //FIXME: this generation may still be used by living process. We need to confirm that this generation is no
            //       longer used.
            self.generation = generation;
            let remote_frame_no = self.last_frame_no(&generation).await?;
            if remote_frame_no < last_frame_no {
                let new_gen = Uuid::v7_reversed(); // timestamp in reversed order
                self.write_dependency(&generation, &new_gen, remote_frame_no)
                    .await?;
                tracing::info!("created generation `{new_gen}` dependent on `{generation}`");
                self.generation = new_gen;
            }
        } else {
            let generation = Uuid::v7_reversed(); // timestamp in reversed order
            tracing::info!("created generation `{generation}`");
            self.generation = generation;
        };
        Ok(())
    }

    /// Saves the information that the `child` generation is dependent on `parent` generation up to `frame_no`.
    async fn write_dependency(
        &self,
        parent: &Uuid,
        child: &Uuid,
        frame_no: u64,
    ) -> super::Result<()> {
        let db_id = self.db_id();
        let path = format!("{db_id}/{child}/.dep");
        let mut writer = self.operator.writer_with(&path).await?;
        writer.write_all(parent.as_ref()).await?;
        writer.write_all(frame_no.to_be_bytes().as_slice()).await?;
        writer.shutdown().await?;
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

    pub fn generation_path(&self, generation: &Uuid) -> String {
        let db_id = self.db_id();
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
            let mut generation_stack = vec![(generation, options.last_known_frame_no)];
            let mut last_frame = 0;
            while let Some((generation, max_gen_frame)) = generation_stack.pop() {
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
                            let bytes = self.operator.read_with(entry.path()).await?;
                            let parent = Uuid::from_slice(&bytes[0..16])?;
                            // `max_parent_frame` is used when a new generation is based on point-in-time recovery from
                            // previous generation. In such cases we don't apply all frames from parent generation, but
                            // only until specific frame. Then child generation continues from that frame onward:
                            //
                            // parent frames: | 1 | 2 | 3 | 4 | 5 | 6 |
                            //  child frames:                 | 5 | 6 | 7 | 8 |
                            //                                ^-- child generation was created after point in time restoration from parent
                            let max_parent_frame = u64::from_be_bytes(bytes[16..20].try_into().unwrap());
                            tracing::debug!("generation `{generation}` is dependent on `{parent}` up to frame {max_parent_frame}");
                            generation_stack.push((generation, max_gen_frame));
                            generation_stack.push((parent, max_parent_frame));
                            break;
                        }
                        EntryKind::Snapshot {
                            timestamp,
                            end_frame,
                            ..
                        } => {
                            let timestamp_secs = timestamp.to_unix().0;
                            if end_frame <= max_gen_frame && timestamp_secs <= unix_secs {
                                let temp_file = self
                                    .into_snapshot_file(&entry)
                                    .await?;
                                let snapshot = SnapshotFile::open(temp_file.file_path()).await?;
                                let snapshot_header = snapshot.header();
                                let start_frame_no: u64 = snapshot_header.start_frame_no.into();
                                if last_frame + 1 != start_frame_no {
                                    Err(super::Error::MissingFrames(last_frame, start_frame_no))?;
                                } else {
                                    last_frame = snapshot_header.end_frame_no.into();
                                }
                                let snapshot_stream = snapshot.into_stream_mut();
                                pin!(snapshot_stream);
                                while let Some(res) = snapshot_stream.next().await {
                                    let frame = res?;
                                    yield frame;
                                }
                            } else {
                                tracing::debug!("skipping over snapshot {} - timestamp {} / frame {} reached end of restore",
                                    entry.name(),
                                    display_date(timestamp_secs),
                                    end_frame
                                );
                                continue;
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
    async fn backup(&mut self, snapshot: SnapshotFile) -> super::Result<()> {
        use tokio::io::AsyncWriteExt;

        let header = snapshot.header();
        let path = self.snapshot_path(header, 0);
        let mut w = self.operator.writer(&path).await?;
        snapshot.write(&mut w).await?;
        w.shutdown().await?;

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
        let timestamp = Timestamp::from_unix(NoContext, 253370761200 - secs, 999999999 - nanos);
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

        let seconds = millis / 1000;
        let nanos = ((millis % 1000) * 1_000_000) as u32;
        Timestamp::from_unix(NoContext, 253370761200 - seconds, 999999999 - nanos)
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
mod test {
    use crate::replication::backup::bottomless::{BackupSession, OperatorOptions, Options};
    use crate::replication::backup::{Backup, Restore, RestoreOptions};
    use crate::LIBSQL_PAGE_SIZE;
    use async_stream::stream;
    use async_tempfile::TempFile;
    use futures_core::Stream;
    use libsql_replication::frame::{FrameBorrowed, FrameHeader};
    use libsql_replication::snapshot::{SnapshotFile, SnapshotFileHeader};
    use opendal::Operator;
    use tokio::io::AsyncWriteExt;
    use tokio_stream::StreamExt;
    use tracing::log::LevelFilter;
    use zerocopy::AsBytes;

    #[tokio::test]
    async fn basic() {
        let _ = env_logger::builder()
            .is_test(true)
            .filter_level(LevelFilter::Trace)
            .build();
        let mut session = BackupSession::open(Options {
            operator_options: MockOptions,
            db_id: "test-db".into(),
            restore: RestoreOptions::default(),
        })
        .await
        .unwrap();

        let expected_pages: Vec<&'static [u8]> = vec![
            b"hello", // 1st frame
            b"world", // 2nd frame
        ];
        let snapshots = vec![
            expected_pages.clone(), // 1st snapshot file
        ];
        let mut snapshots = generate_snapshots(snapshots);

        // backup incoming data
        while let Some(tmp_file) = snapshots.next().await {
            let snapshot = SnapshotFile::open(tmp_file.file_path()).await.unwrap();
            session.backup(snapshot).await.unwrap();
        }

        // try to restore data
        let mut frames = session.restore(RestoreOptions::default()).await.unwrap();
        let mut i = 0;
        while let Some(frame) = frames.next().await {
            let frame = frame.unwrap();
            let page = frame.page();
            let expected = expected_pages[i];
            assert!(page.starts_with(expected), "page no {i}");
            i += 1;
        }
    }

    fn generate_snapshots<'a, I: IntoIterator<Item = Vec<&'a [u8]>>>(
        segments: I,
    ) -> impl Stream<Item = TempFile> {
        Box::pin(stream! {
            let mut frame_no = 1u64;
            for pages in segments.into_iter() {
                let mut tmp = TempFile::new().await.unwrap();
                let frame_count = pages.len() as u64;
                let start_frame_no = frame_no;
                let end_frame_no = start_frame_no + frame_count;
                frame_no += frame_count;

                let header = SnapshotFileHeader {
                    log_id: 0.into(),
                    start_frame_no: start_frame_no.into(),
                    end_frame_no: end_frame_no.into(),
                    frame_count: frame_count.into(),
                    size_after: 1.into(),
                    _pad: Default::default(),
                };
                tmp.write_all(header.as_bytes()).await.unwrap();

                let mut page = [0u8; LIBSQL_PAGE_SIZE as usize];
                let mut page_no = 1;
                for data in pages {
                    let page = page.as_mut_slice();
                    page[0..data.len()].copy_from_slice(data);
                    let frame_header = FrameHeader {
                        frame_no: (frame_no - page_no + 1).into(),
                        checksum: Default::default(),
                        page_no: (page_no as u32).into(),
                        size_after: ((page_no * LIBSQL_PAGE_SIZE) as u32).into(),
                    };
                    let frame = FrameBorrowed::from_parts(&frame_header, &page);
                    tmp.write_all(frame.as_bytes()).await.unwrap();
                    page_no += 1;
                }
                tmp.flush().await.unwrap();
                yield tmp;
            }
        })
    }

    #[derive(Clone, Copy)]
    struct MockOptions;

    impl OperatorOptions for MockOptions {
        fn create_operator(&self) -> crate::replication::backup::Result<Operator> {
            let builder = opendal::services::Memory::default();
            Ok(Operator::new(builder)?.finish())
        }
    }
}
