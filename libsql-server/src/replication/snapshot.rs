use std::collections::HashSet;
use std::fs::File;
use std::io::BufWriter;
use std::io::Write;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Context;
use bytemuck::{bytes_of, pod_read_unaligned, Pod, Zeroable};
use bytes::BytesMut;
use crossbeam::channel::bounded;
use once_cell::sync::Lazy;
use regex::Regex;
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::namespace::NamespaceName;

use super::frame::Frame;
use super::primary::logger::LogFile;
use super::FrameNo;

/// This is the ratio of the space required to store snapshot vs size of the actual database.
/// When this ratio is exceeded, compaction is triggered.
const SNAPHOT_SPACE_AMPLIFICATION_FACTOR: u64 = 2;
/// The maximum amount of snapshot allowed before a compaction is required
const MAX_SNAPSHOT_NUMBER: usize = 32;

#[derive(Debug, Copy, Clone, Zeroable, Pod, PartialEq, Eq)]
#[repr(C)]
pub struct SnapshotFileHeader {
    /// id of the database
    pub db_id: u128,
    /// first frame in the snapshot
    pub start_frame_no: u64,
    /// end frame in the snapshot
    pub end_frame_no: u64,
    /// number of frames in the snapshot
    pub frame_count: u64,
    /// safe of the database after applying the snapshot
    pub size_after: u32,
    pub _pad: u32,
}

pub struct SnapshotFile {
    file: File,
    header: SnapshotFileHeader,
}

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

fn snapshot_list(db_path: &Path) -> anyhow::Result<impl Iterator<Item = String>> {
    let mut entries = std::fs::read_dir(snapshot_dir_path(db_path))?;
    Ok(std::iter::from_fn(move || {
        for entry in entries.by_ref() {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            let Some(name) = path.file_name() else {
                continue;
            };
            let Some(name_str) = name.to_str() else {
                continue;
            };

            return Some(name_str.to_string());
        }
        None
    }))
}

/// Return snapshot file containing "logically" frame_no
pub fn find_snapshot_file(
    db_path: &Path,
    frame_no: FrameNo,
) -> anyhow::Result<Option<SnapshotFile>> {
    let snapshot_dir_path = snapshot_dir_path(db_path);
    for name in snapshot_list(db_path)? {
        let Some((_, start_frame_no, end_frame_no)) = parse_snapshot_name(&name) else {
            continue;
        };
        // we're looking for the frame right after the last applied frame on the replica
        if (start_frame_no..=end_frame_no).contains(&frame_no) {
            let snapshot_path = snapshot_dir_path.join(&name);
            tracing::debug!("found snapshot for frame {frame_no} at {snapshot_path:?}");
            let snapshot_file = SnapshotFile::open(&snapshot_path)?;
            return Ok(Some(snapshot_file));
        }
    }

    Ok(None)
}

impl SnapshotFile {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut header_buf = [0; size_of::<SnapshotFileHeader>()];
        file.read_exact_at(&mut header_buf, 0)?;
        let header: SnapshotFileHeader = pod_read_unaligned(&header_buf);

        Ok(Self { file, header })
    }

    /// Iterator on the frames contained in the snapshot file, in reverse frame_no order.
    pub fn frames_iter(&self) -> impl Iterator<Item = anyhow::Result<Frame>> + '_ {
        let mut current_offset = 0;
        std::iter::from_fn(move || {
            if current_offset >= self.header.frame_count {
                return None;
            }
            let read_offset = size_of::<SnapshotFileHeader>() as u64
                + current_offset * LogFile::FRAME_SIZE as u64;
            current_offset += 1;
            let mut buf = BytesMut::zeroed(LogFile::FRAME_SIZE);
            match self.file.read_exact_at(&mut buf, read_offset as _) {
                Ok(_) => match Frame::try_from_bytes(buf.freeze()) {
                    Ok(frame) => Some(Ok(frame)),
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e.into())),
            }
        })
    }

    /// Like `frames_iter`, but stops as soon as a frame with frame_no <= `frame_no` is reached
    pub fn frames_iter_from(
        &self,
        frame_no: u64,
    ) -> impl Iterator<Item = anyhow::Result<Frame>> + '_ {
        let mut iter = self.frames_iter();
        std::iter::from_fn(move || match iter.next() {
            Some(Ok(frame)) => {
                if frame.header().frame_no < frame_no {
                    None
                } else {
                    Some(Ok(frame))
                }
            }
            other => other,
        })
    }
}

#[derive(Clone, Debug)]
pub struct LogCompactor {
    sender: crossbeam::channel::Sender<(LogFile, PathBuf, u32)>,
}

pub type SnapshotCallback = Box<dyn Fn(&Path) -> anyhow::Result<()> + Send + Sync>;
pub type NamespacedSnapshotCallback =
    Arc<dyn Fn(&Path, &NamespaceName) -> anyhow::Result<()> + Send + Sync>;

impl LogCompactor {
    pub fn new(db_path: &Path, db_id: u128, callback: SnapshotCallback) -> anyhow::Result<Self> {
        // we create a 0 sized channel, in order to create backpressure when we can't
        // keep up with snapshop creation: if there isn't any ongoind comptaction task processing,
        // the compact does not block, and the log is compacted in the background. Otherwise, the
        // block until there is a free slot to perform compaction.
        let (sender, receiver) = bounded::<(LogFile, PathBuf, u32)>(0);
        let mut merger = SnapshotMerger::new(db_path, db_id)?;
        let db_path = db_path.to_path_buf();
        let snapshot_dir_path = snapshot_dir_path(&db_path);
        let _handle = std::thread::spawn(move || {
            while let Ok((file, log_path, size_after)) = receiver.recv() {
                match perform_compaction(&db_path, file, db_id) {
                    Ok((snapshot_name, snapshot_frame_count)) => {
                        tracing::info!("snapshot `{snapshot_name}` successfully created");

                        let snapshot_file = snapshot_dir_path.join(&snapshot_name);
                        if let Err(e) = (*callback)(&snapshot_file) {
                            tracing::error!("failed to call snapshot callback: {e}");
                            break;
                        }

                        if let Err(e) = merger.register_snapshot(
                            snapshot_name,
                            snapshot_frame_count,
                            size_after,
                        ) {
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
            .send((file, path, size_after))
            .context("failed to compact log: log compactor thread exited")?;

        Ok(())
    }
}

struct SnapshotMerger {
    /// Sending part of a channel of (snapshot_name, snapshot_frame_count, db_page_count) to the merger thread
    sender: mpsc::Sender<(String, u64, u32)>,
    handle: Option<JoinHandle<anyhow::Result<()>>>,
}

impl SnapshotMerger {
    fn new(db_path: &Path, db_id: u128) -> anyhow::Result<Self> {
        let (sender, receiver) = mpsc::channel();

        let db_path = db_path.to_path_buf();
        let handle =
            std::thread::spawn(move || Self::run_snapshot_merger_loop(receiver, &db_path, db_id));

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

    fn run_snapshot_merger_loop(
        receiver: mpsc::Receiver<(String, u64, u32)>,
        db_path: &Path,
        db_id: u128,
    ) -> anyhow::Result<()> {
        let mut snapshots = Self::init_snapshot_info_list(db_path)?;
        while let Ok((name, size, db_page_count)) = receiver.recv() {
            snapshots.push((name, size));
            if Self::should_compact(&snapshots, db_page_count) {
                let compacted_snapshot_info = Self::merge_snapshots(&snapshots, db_path, db_id)?;
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
    fn init_snapshot_info_list(db_path: &Path) -> anyhow::Result<Vec<(String, u64)>> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        if !snapshot_dir_path.exists() {
            return Ok(Vec::new());
        }

        let mut temp = Vec::new();
        for snapshot_name in snapshot_list(db_path)? {
            let snapshot_path = snapshot_dir_path.join(&snapshot_name);
            let snapshot = SnapshotFile::open(&snapshot_path)?;
            temp.push((
                snapshot_name,
                snapshot.header.frame_count,
                snapshot.header.start_frame_no,
            ))
        }

        temp.sort_by_key(|(_, _, id)| *id);

        Ok(temp
            .into_iter()
            .map(|(name, count, _)| (name, count))
            .collect())
    }

    fn merge_snapshots(
        snapshots: &[(String, u64)],
        db_path: &Path,
        db_id: u128,
    ) -> anyhow::Result<(String, u64)> {
        let mut builder = SnapshotBuilder::new(db_path, db_id)?;
        let snapshot_dir_path = snapshot_dir_path(db_path);
        for (name, _) in snapshots.iter().rev() {
            let snapshot = SnapshotFile::open(&snapshot_dir_path.join(name))?;
            let iter = snapshot.frames_iter();
            builder.append_frames(iter)?;
        }

        let (_, start_frame_no, _) = parse_snapshot_name(&snapshots[0].0).unwrap();
        let (_, _, end_frame_no) = parse_snapshot_name(&snapshots.last().unwrap().0).unwrap();

        builder.header.start_frame_no = start_frame_no;
        builder.header.end_frame_no = end_frame_no;

        let compacted_snapshot_infos = builder.finish()?;

        for (name, _) in snapshots.iter() {
            std::fs::remove_file(&snapshot_dir_path.join(name))?;
        }

        Ok(compacted_snapshot_infos)
    }

    fn register_snapshot(
        &mut self,
        snapshot_name: String,
        snapshot_frame_count: u64,
        db_page_count: u32,
    ) -> anyhow::Result<()> {
        if self
            .sender
            .send((snapshot_name, snapshot_frame_count, db_page_count))
            .is_err()
        {
            if let Some(handle) = self.handle.take() {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("snapshot merger thread panicked"))??;
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
    snapshot_file: BufWriter<NamedTempFile>,
    db_path: PathBuf,
    last_seen_frame_no: u64,
}

fn snapshot_dir_path(db_path: &Path) -> PathBuf {
    db_path.join("snapshots")
}

impl SnapshotBuilder {
    fn new(db_path: &Path, db_id: u128) -> anyhow::Result<Self> {
        let snapshot_dir_path = snapshot_dir_path(db_path);
        std::fs::create_dir_all(&snapshot_dir_path)?;
        let mut target = BufWriter::new(NamedTempFile::new_in(&snapshot_dir_path)?);
        // reserve header space
        target.write_all(&[0; size_of::<SnapshotFileHeader>()])?;

        Ok(Self {
            seen_pages: HashSet::new(),
            header: SnapshotFileHeader {
                db_id,
                start_frame_no: u64::MAX,
                end_frame_no: u64::MIN,
                frame_count: 0,
                size_after: 0,
                _pad: 0,
            },
            snapshot_file: target,
            db_path: db_path.to_path_buf(),
            last_seen_frame_no: u64::MAX,
        })
    }

    /// append frames to the snapshot. Frames must be in decreasing frame_no order.
    fn append_frames(
        &mut self,
        frames: impl Iterator<Item = anyhow::Result<Frame>>,
    ) -> anyhow::Result<()> {
        // We iterate on the frames starting from the end of the log and working our way backward. We
        // make sure that only the most recent version of each file is present in the resulting
        // snapshot.
        //
        // The snapshot file contains the most recent version of each page, in descending frame
        // number order. That last part is important for when we read it later on.
        for frame in frames {
            let frame = frame?;
            assert!(frame.header().frame_no < self.last_seen_frame_no);
            self.last_seen_frame_no = frame.header().frame_no;
            if frame.header().frame_no < self.header.start_frame_no {
                self.header.start_frame_no = frame.header().frame_no;
            }

            if frame.header().frame_no > self.header.end_frame_no {
                self.header.end_frame_no = frame.header().frame_no;
                self.header.size_after = frame.header().size_after;
            }

            if !self.seen_pages.contains(&frame.header().page_no) {
                self.seen_pages.insert(frame.header().page_no);
                self.snapshot_file.write_all(frame.as_slice())?;
                self.header.frame_count += 1;
            }
        }

        Ok(())
    }

    /// Persist the snapshot, and returns the name and size is frame on the snapshot.
    fn finish(mut self) -> anyhow::Result<(String, u64)> {
        self.snapshot_file.flush()?;
        let file = self.snapshot_file.into_inner()?;
        file.as_file().write_all_at(bytes_of(&self.header), 0)?;
        let snapshot_name = format!(
            "{}-{}-{}.snap",
            Uuid::from_u128(self.header.db_id),
            self.header.start_frame_no,
            self.header.end_frame_no,
        );

        file.persist(snapshot_dir_path(&self.db_path).join(&snapshot_name))?;

        Ok((snapshot_name, self.header.frame_count))
    }
}

fn perform_compaction(
    db_path: &Path,
    file_to_compact: LogFile,
    db_id: u128,
) -> anyhow::Result<(String, u64)> {
    let mut builder = SnapshotBuilder::new(db_path, db_id)?;
    builder.append_frames(file_to_compact.rev_frames_iter()?)?;
    builder.finish()
}

#[cfg(test)]
mod test {
    use std::fs::read;
    use std::{thread, time::Duration};

    use bytemuck::pod_read_unaligned;
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::replication::primary::logger::WalPage;
    use crate::replication::snapshot::SnapshotFile;

    use super::*;

    #[test]
    fn compact_file_create_snapshot() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut log_file = LogFile::new(temp.as_file().try_clone().unwrap(), 0, None).unwrap();
        let db_id = Uuid::new_v4();
        log_file.header.db_id = db_id.as_u128();
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
        let compactor =
            LogCompactor::new(dump_dir.path(), db_id.as_u128(), Box::new(|_| Ok(()))).unwrap();
        compactor
            .compact(log_file, temp.path().to_path_buf(), 25)
            .unwrap();

        thread::sleep(Duration::from_secs(1));

        let snapshot_path =
            snapshot_dir_path(dump_dir.path()).join(format!("{}-{}-{}.snap", db_id, 0, 49));
        let snapshot = read(&snapshot_path).unwrap();
        let header: SnapshotFileHeader =
            pod_read_unaligned(&snapshot[..std::mem::size_of::<SnapshotFileHeader>()]);

        assert_eq!(header.start_frame_no, 0);
        assert_eq!(header.end_frame_no, 49);
        assert_eq!(header.frame_count, 25);
        assert_eq!(header.db_id, db_id.as_u128());
        assert_eq!(header.size_after, 25);

        let mut seen_frames = HashSet::new();
        let mut seen_page_no = HashSet::new();
        let data = &snapshot[std::mem::size_of::<SnapshotFileHeader>()..];
        data.chunks(LogFile::FRAME_SIZE).for_each(|f| {
            let frame = Frame::try_from_bytes(Bytes::copy_from_slice(f)).unwrap();
            assert!(!seen_frames.contains(&frame.header().frame_no));
            assert!(!seen_page_no.contains(&frame.header().page_no));
            seen_page_no.insert(frame.header().page_no);
            seen_frames.insert(frame.header().frame_no);
            assert!(frame.header().frame_no >= 25);
        });

        assert_eq!(seen_frames.len(), 25);
        assert_eq!(seen_page_no.len(), 25);

        let snapshot_file = SnapshotFile::open(&snapshot_path).unwrap();

        let frames = snapshot_file.frames_iter_from(0);
        let mut expected_frame_no = 49;
        for frame in frames {
            let frame = frame.unwrap();
            assert_eq!(frame.header().frame_no, expected_frame_no);
            expected_frame_no -= 1;
        }

        assert_eq!(expected_frame_no, 24);
    }
}
