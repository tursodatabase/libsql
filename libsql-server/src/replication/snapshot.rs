use std::collections::HashSet;
use std::io::SeekFrom;
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use bytemuck::bytes_of;
use futures::TryStreamExt;
use libsql_replication::frame::FrameMut;
use libsql_replication::snapshot::{SnapshotFile, SnapshotFileHeader};
use once_cell::sync::Lazy;
use regex::Regex;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use uuid::Uuid;

use crate::namespace::NamespaceName;

use super::primary::logger::LogFile;
use super::FrameNo;

/// This is the ratio of the space required to store snapshot vs size of the actual database.
/// When this ratio is exceeded, compaction is triggered.
const SNAPHOT_SPACE_AMPLIFICATION_FACTOR: u64 = 2;
/// The maximum amount of snapshot allowed before a compaction is required
const MAX_SNAPSHOT_NUMBER: usize = 32;

/// returns (db_id, start_frame_no, end_frame_no) for the given snapshot name
fn parse_snapshot_name(name: &str) -> Option<(Uuid, u64, u64)> {
    static SNAPSHOT_FILE_MATCHER: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?x)
            # match database id
            (\w{8}-\w{4}-\w{4}-\w{4}-\w{12})-
            # match start frame_no
            (\d*)-
            # match end frame_no
            (\d*).snap",
        )
        .unwrap()
    });
    let Some(captures) = SNAPSHOT_FILE_MATCHER.captures(name) else {
        return None;
    };
    let db_id = captures.get(1).unwrap();
    let start_index: u64 = captures.get(2).unwrap().as_str().parse().unwrap();
    let end_index: u64 = captures.get(3).unwrap().as_str().parse().unwrap();

    Some((
        Uuid::from_str(db_id.as_str()).unwrap(),
        start_index,
        end_index,
    ))
}

fn snapshot_list(db_path: &Path) -> impl Stream<Item = anyhow::Result<String>> + '_ {
    async_stream::try_stream! {
        let mut entries = tokio::fs::read_dir(snapshot_dir_path(db_path)).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let Some(name) = path.file_name() else {
                continue;
            };
            let Some(name_str) = name.to_str() else {
                continue;
            };

            yield name_str.to_string();
        }
    }
}

/// Return snapshot file containing "logically" frame_no
pub async fn find_snapshot_file(
    db_path: &Path,
    frame_no: FrameNo,
) -> anyhow::Result<Option<SnapshotFile>> {
    let snapshot_dir_path = snapshot_dir_path(db_path);
    let snapshots = snapshot_list(db_path);
    tokio::pin!(snapshots);
    while let Some(name) = snapshots.next().await.transpose()? {
        let Some((_, start_frame_no, end_frame_no)) = parse_snapshot_name(&name) else {
            continue;
        };
        // we're looking for the frame right after the last applied frame on the replica
        if (start_frame_no..=end_frame_no).contains(&frame_no) {
            let snapshot_path = snapshot_dir_path.join(&name);
            tracing::debug!("found snapshot for frame {frame_no} at {snapshot_path:?}");
            let snapshot_file = SnapshotFile::open(&snapshot_path).await?;
            return Ok(Some(snapshot_file));
        }
    }

    Ok(None)
}

#[derive(Clone, Debug)]
pub struct LogCompactor {
    sender: mpsc::Sender<(LogFile, PathBuf, u32)>,
}

pub type SnapshotCallback = Box<dyn Fn(&Path) -> anyhow::Result<()> + Send + Sync>;
pub type NamespacedSnapshotCallback =
    Arc<dyn Fn(&Path, &NamespaceName) -> anyhow::Result<()> + Send + Sync>;

impl LogCompactor {
    pub fn new(db_path: &Path, log_id: Uuid, callback: SnapshotCallback) -> anyhow::Result<Self> {
        // we create a 0 sized channel, in order to create backpressure when we can't
        // keep up with snapshop creation: if there isn't any ongoind comptaction task processing,
        // the compact does not block, and the log is compacted in the background. Otherwise, the
        // block until there is a free slot to perform compaction.
        let (sender, mut receiver) = mpsc::channel::<(LogFile, PathBuf, u32)>(1);
        let mut merger = SnapshotMerger::new(db_path, log_id)?;
        let db_path = db_path.to_path_buf();
        let snapshot_dir_path = snapshot_dir_path(&db_path);
        let _handle = tokio::task::spawn(async move {
            while let Some((file, log_path, size_after)) = receiver.recv().await {
                match perform_compaction(&db_path, file, log_id).await {
                    Ok((snapshot_name, snapshot_frame_count)) => {
                        tracing::info!("snapshot `{snapshot_name}` successfully created");

                        let snapshot_file = snapshot_dir_path.join(&snapshot_name);
                        if let Err(e) = (*callback)(&snapshot_file) {
                            tracing::error!("failed to call snapshot callback: {e}");
                            break;
                        }

                        if let Err(e) = merger
                            .register_snapshot(snapshot_name, snapshot_frame_count, size_after)
                            .await
                        {
                            tracing::error!(
                                "failed to register snapshot with snapshot merger: {e}"
                            );
                            break;
                        }

                        if let Err(e) = std::fs::remove_file(&log_path) {
                            tracing::error!(
                                "failed to remove old log file `{}`: {e}",
                                log_path.display()
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::error!("fatal error creating snapshot: {e}");
                        break;
                    }
                }
            }
        });

        Ok(Self { sender })
    }

    /// Sends a compaction task to the background compaction thread. Blocks if a compaction task is
    /// already ongoing.
    pub fn compact(&self, file: LogFile, path: PathBuf, size_after: u32) -> anyhow::Result<()> {
        self.sender
            .blocking_send((file, path, size_after))
            .context("failed to compact log: log compactor thread exited")?;

        Ok(())
    }
}

struct SnapshotMerger {
    /// Sending part of a channel of (snapshot_name, snapshot_frame_count, db_page_count) to the merger thread
    sender: mpsc::Sender<(String, u64, u32)>,
    handle: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
}

impl SnapshotMerger {
    fn new(db_path: &Path, log_id: Uuid) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel(1);

        let db_path = db_path.to_path_buf();
        let handle = tokio::task::spawn(async move {
            Self::run_snapshot_merger_loop(receiver, &db_path, log_id).await
        });

        Ok(Self {
            sender,
            handle: Some(handle),
        })
    }

    fn should_compact(snapshots: &[(String, u64)], db_page_count: u32) -> bool {
        let snapshots_size: u64 = snapshots.iter().map(|(_, s)| *s).sum();
        snapshots_size >= SNAPHOT_SPACE_AMPLIFICATION_FACTOR * db_page_count as u64
            || snapshots.len() > MAX_SNAPSHOT_NUMBER
    }

    async fn run_snapshot_merger_loop(
        mut receiver: mpsc::Receiver<(String, u64, u32)>,
        db_path: &Path,
        log_id: Uuid,
    ) -> anyhow::Result<()> {
        let mut snapshots = Self::init_snapshot_info_list(db_path).await?;
        while let Some((name, size, db_page_count)) = receiver.recv().await {
            snapshots.push((name, size));
            if Self::should_compact(&snapshots, db_page_count) {
                let compacted_snapshot_info =
                    Self::merge_snapshots(&snapshots, db_path, log_id).await?;
                snapshots.clear();
                snapshots.push(compacted_snapshot_info);
            }
        }

        Ok(())
    }

    /// Reads the snapshot dir and returns the list of snapshots along with their size, sorted in
    /// chronological order.
    ///
    /// TODO: if the process was kill in the midst of merging snapshot, then the compacted snapshot
    /// can exist alongside the snapshots it's supposed to have compacted. This is the place to
    /// perform the cleanup.
    async fn init_snapshot_info_list(db_path: &Path) -> anyhow::Result<Vec<(String, u64)>> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        if !snapshot_dir_path.exists() {
            return Ok(Vec::new());
        }

        let mut temp = Vec::new();

        let snapshots = snapshot_list(db_path);
        tokio::pin!(snapshots);
        while let Some(snapshot_name) = snapshots.next().await.transpose()? {
            let snapshot_path = snapshot_dir_path.join(&snapshot_name);
            let snapshot =
                tokio::runtime::Handle::current().block_on(SnapshotFile::open(&snapshot_path))?;
            temp.push((
                snapshot_name,
                snapshot.header().frame_count,
                snapshot.header().start_frame_no,
            ))
        }

        temp.sort_by_key(|(_, _, id)| *id);

        Ok(temp
            .into_iter()
            .map(|(name, count, _)| (name, count))
            .collect())
    }

    async fn merge_snapshots(
        snapshots: &[(String, u64)],
        db_path: &Path,
        log_id: Uuid,
    ) -> anyhow::Result<(String, u64)> {
        let mut builder = SnapshotBuilder::new(db_path, log_id).await?;
        let snapshot_dir_path = snapshot_dir_path(db_path);
        let mut size_after = None;
        for (name, _) in snapshots.iter().rev() {
            let snapshot = SnapshotFile::open(&snapshot_dir_path.join(name)).await?;
            // The size after the merged snapshot is the size after the first snapshot to be merged
            if size_after.is_none() {
                size_after.replace(snapshot.header().size_after);
            }
            builder
                .append_frames(snapshot.into_stream_mut().map_err(|e| anyhow::anyhow!(e)))
                .await?;
        }

        let (_, start_frame_no, _) = parse_snapshot_name(&snapshots[0].0).unwrap();
        let (_, _, end_frame_no) = parse_snapshot_name(&snapshots.last().unwrap().0).unwrap();

        builder.header.start_frame_no = start_frame_no;
        builder.header.end_frame_no = end_frame_no;
        builder.header.size_after = size_after.unwrap();

        let compacted_snapshot_infos = builder.finish().await?;

        for (name, _) in snapshots.iter() {
            tokio::fs::remove_file(&snapshot_dir_path.join(name)).await?;
        }

        Ok(compacted_snapshot_infos)
    }

    async fn register_snapshot(
        &mut self,
        snapshot_name: String,
        snapshot_frame_count: u64,
        db_page_count: u32,
    ) -> anyhow::Result<()> {
        if self
            .sender
            .send((snapshot_name, snapshot_frame_count, db_page_count))
            .await
            .is_err()
        {
            if let Some(handle) = self.handle.take() {
                handle.await??;
            }

            anyhow::bail!("failed to register snapshot with log merger: thread exited");
        }

        Ok(())
    }
}

/// An utility to build a snapshots from log frames
struct SnapshotBuilder {
    seen_pages: HashSet<u32>,
    header: SnapshotFileHeader,
    snapshot_file: tokio::io::BufWriter<async_tempfile::TempFile>,
    db_path: PathBuf,
    last_seen_frame_no: u64,
}

fn snapshot_dir_path(db_path: &Path) -> PathBuf {
    db_path.join("snapshots")
}

impl SnapshotBuilder {
    async fn new(db_path: &Path, log_id: Uuid) -> anyhow::Result<Self> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        std::fs::create_dir_all(&snapshot_dir_path)?;
        let mut f = tokio::io::BufWriter::new(async_tempfile::TempFile::new().await?);
        // reserve header space
        f.write_all(&[0; size_of::<SnapshotFileHeader>()]).await?;

        Ok(Self {
            seen_pages: HashSet::new(),
            header: SnapshotFileHeader {
                log_id: log_id.as_u128(),
                start_frame_no: u64::MAX,
                end_frame_no: u64::MIN,
                frame_count: 0,
                size_after: 0,
                _pad: 0,
            },
            snapshot_file: f,
            db_path: db_path.to_path_buf(),
            last_seen_frame_no: u64::MAX,
        })
    }

    /// append frames to the snapshot. Frames must be in decreasing frame_no order.
    async fn append_frames(
        &mut self,
        frames: impl Stream<Item = anyhow::Result<FrameMut>>,
    ) -> anyhow::Result<()> {
        // We iterate on the frames starting from the end of the log and working our way backward. We
        // make sure that only the most recent version of each file is present in the resulting
        // snapshot.
        //
        // The snapshot file contains the most recent version of each page, in descending frame
        // number order. That last part is important for when we read it later on.
        tokio::pin!(frames);
        while let Some(frame) = frames.next().await {
            let mut frame = frame?;
            assert!(frame.header().frame_no < self.last_seen_frame_no);
            self.last_seen_frame_no = frame.header().frame_no;
            if frame.header().frame_no < self.header.start_frame_no {
                self.header.start_frame_no = frame.header().frame_no;
            }

            if frame.header().frame_no > self.header.end_frame_no {
                self.header.end_frame_no = frame.header().frame_no;
                self.header.size_after = frame.header().size_after;
            }

            // set all frames as non-commit frame in a snapshot, and let the client decide when to
            // commit. This is ok because the client will stream frames backward until caught up,
            // and then commit.
            frame.header_mut().size_after = 0;

            if !self.seen_pages.contains(&frame.header().page_no) {
                self.seen_pages.insert(frame.header().page_no);
                let data = frame.as_slice();
                self.snapshot_file.write_all(data).await?;
                self.header.frame_count += 1;
            }
        }

        Ok(())
    }

    /// Persist the snapshot, and returns the name and size is frame on the snapshot.
    async fn finish(mut self) -> anyhow::Result<(String, u64)> {
        self.snapshot_file.flush().await?;
        let mut file = self.snapshot_file.into_inner();
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(bytes_of(&self.header)).await?;
        let snapshot_name = format!(
            "{}-{}-{}.snap",
            Uuid::from_u128(self.header.log_id),
            self.header.start_frame_no,
            self.header.end_frame_no,
        );

        file.sync_all().await?;

        tokio::fs::rename(
            file.file_path(),
            snapshot_dir_path(&self.db_path).join(&snapshot_name),
        )
        .await?;

        Ok((snapshot_name, self.header.frame_count))
    }
}

async fn perform_compaction(
    db_path: &Path,
    file_to_compact: LogFile,
    log_id: Uuid,
) -> anyhow::Result<(String, u64)> {
    let mut builder = SnapshotBuilder::new(db_path, log_id).await?;
    builder
        .append_frames(file_to_compact.into_rev_stream_mut())
        .await?;
    builder.finish().await
}

#[cfg(test)]
mod test {
    use std::fs::read;
    use std::time::Duration;

    use bytemuck::pod_read_unaligned;
    use bytes::Bytes;
    use libsql_replication::frame::Frame;
    use tempfile::tempdir;

    use crate::replication::primary::logger::WalPage;
    use crate::replication::snapshot::SnapshotFile;

    use super::*;

    #[tokio::test]
    async fn compact_file_create_snapshot() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut log_file = LogFile::new(temp.as_file().try_clone().unwrap(), 0, None).unwrap();
        let log_id = Uuid::new_v4();
        log_file.header.log_id = log_id.as_u128();
        log_file.write_header().unwrap();

        // add 50 pages, each one in two versions
        for _ in 0..2 {
            for i in 0..25 {
                let data = std::iter::repeat(0).take(4096).collect::<Bytes>();
                let page = WalPage {
                    page_no: i,
                    size_after: i + 1,
                    data,
                };
                log_file.push_page(&page).unwrap();
            }
        }

        log_file.commit().unwrap();

        let dump_dir = tempdir().unwrap();
        let compactor = LogCompactor::new(dump_dir.path(), log_id, Box::new(|_| Ok(()))).unwrap();
        tokio::task::spawn_blocking({
            let compactor = compactor.clone();
            move || {
                compactor
                    .compact(log_file, temp.path().to_path_buf(), 25)
                    .unwrap()
            }
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        let snapshot_path =
            snapshot_dir_path(dump_dir.path()).join(format!("{}-{}-{}.snap", log_id, 0, 49));
        let snapshot = read(&snapshot_path).unwrap();
        let header: SnapshotFileHeader =
            pod_read_unaligned(&snapshot[..std::mem::size_of::<SnapshotFileHeader>()]);

        assert_eq!(header.start_frame_no, 0);
        assert_eq!(header.end_frame_no, 49);
        assert_eq!(header.frame_count, 25);
        assert_eq!(header.log_id, log_id.as_u128());
        assert_eq!(header.size_after, 25);

        let mut seen_frames = HashSet::new();
        let mut seen_page_no = HashSet::new();
        let data = &snapshot[std::mem::size_of::<SnapshotFileHeader>()..];
        data.chunks(LogFile::FRAME_SIZE).for_each(|f| {
            let frame = Frame::try_from(f).unwrap();
            assert!(!seen_frames.contains(&frame.header().frame_no));
            assert!(!seen_page_no.contains(&frame.header().page_no));
            seen_page_no.insert(frame.header().page_no);
            seen_frames.insert(frame.header().frame_no);
            assert!(frame.header().frame_no >= 25);
        });

        assert_eq!(seen_frames.len(), 25);
        assert_eq!(seen_page_no.len(), 25);

        let snapshot_file = SnapshotFile::open(&snapshot_path).await.unwrap();

        let frames = snapshot_file.into_stream_mut_from(0);
        tokio::pin!(frames);
        let mut expected_frame_no = 49;
        while let Some(frame) = frames.next().await {
            let frame = frame.unwrap();
            assert_eq!(frame.header().frame_no, expected_frame_no);
            expected_frame_no -= 1;
        }

        assert_eq!(expected_frame_no, 24);
    }
}
