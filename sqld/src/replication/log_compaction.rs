use std::collections::HashSet;
use std::fs::create_dir_all;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::{Path, PathBuf};

use bytemuck::bytes_of;
use crossbeam::channel::{bounded, Sender};
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::replication::logger::FrameHeader;

use super::logger::LogFile;
use super::snapshot::SnapshotFileHeader;

#[derive(Clone)]
pub struct LogCompactor {
    sender: Sender<(LogFile, PathBuf, u32)>,
}

impl LogCompactor {
    pub fn new(path: PathBuf) -> Self {
        // we create a 0 sized channel, in order to create backpressure when we can't
        // keep up with snapshop creation: if there isn't any ongoind comptaction task processing,
        // the compact does not block, and the log is compacted in the background. Otherwise, the
        // block until there is a free slot to perform compaction.
        let (sender, receiver) = bounded::<(LogFile, PathBuf, u32)>(0);
        let _handle = std::thread::spawn(move || {
            while let Ok((file, log_path, size_after)) = receiver.recv() {
                match perform_compaction(&path, size_after, file) {
                    Ok(name) => {
                        tracing::info!("snapshot `{name}` successfully created");
                        if let Err(e) = std::fs::remove_file(&log_path) {
                            tracing::error!(
                                "failed to remove old log file `{}`: {e}",
                                log_path.display()
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!("fatal error creating snapshot: {e}");
                    }
                }
            }
        });

        Self { sender }
    }

    /// Sends a compaction task to the background compaction thread. Blocks if a compaction task is
    /// already ongoing.
    pub fn compact(&self, file: LogFile, path: PathBuf, size_after: u32) -> anyhow::Result<()> {
        self.sender.send((file, path, size_after))?;
        Ok(())
    }
}

fn perform_compaction(
    db_path: &Path,
    size_after: u32,
    file_to_compact: LogFile,
) -> anyhow::Result<String> {
    let header = *file_to_compact.header();
    let mut snapshot_header = SnapshotFileHeader {
        db_id: header.db_id,
        start_frame_id: header.start_frame_id,
        end_frame_index: header.start_frame_id + header.frame_count - 1,
        size_after,
        frame_count: 0,
        _pad: 0,
    };

    let snapshot_file = NamedTempFile::new_in(db_path)?;
    snapshot_file
        .as_file()
        .write_all_at(bytes_of(&snapshot_header), 0)?;
    let mut seen = HashSet::new();
    let mut frame_count = 0;
    // We iterate on the frames starting from the end of the log and working our way backward. We
    // make sure that only the most recent version of each file is present in the resulting
    // snapshot.
    //
    // The snapshot file contains the most recent version of each page, in descending frame id
    // order. That last part is important for when we read it later on.
    for frame in file_to_compact.rev_frames_iter()? {
        let frame = frame?;
        let page_no = frame.header.page_no;
        if !seen.contains(&page_no) {
            let byte_offset = size_of::<SnapshotFileHeader>() + frame_count * LogFile::FRAME_SIZE;
            seen.insert(page_no);
            snapshot_file
                .as_file()
                .write_all_at(bytes_of(&frame.header), byte_offset as u64)?;
            snapshot_file
                .as_file()
                .write_all_at(&frame.data, (byte_offset + size_of::<FrameHeader>()) as u64)?;

            frame_count += 1;
        }
    }

    // update snapshot header
    snapshot_header.frame_count = frame_count as _;
    snapshot_file
        .as_file()
        .write_all_at(bytes_of(&snapshot_header), 0)?;

    let snapshot_name = format!(
        "{}-{}-{}.snap",
        Uuid::from_u128(header.db_id),
        header.start_frame_id,
        header.start_frame_id + header.frame_count - 1,
    );

    // persist the snapshot
    let snapshot_dir = db_path.join("snapshots");
    create_dir_all(&snapshot_dir)?;
    snapshot_file.persist(snapshot_dir.join(&snapshot_name))?;

    Ok(snapshot_name)
}

#[cfg(test)]
mod test {
    use std::fs::read;
    use std::{thread, time::Duration};

    use bytemuck::{pod_read_unaligned, try_from_bytes};
    use bytes::Bytes;
    use tempfile::tempdir;

    use crate::replication::logger::{
        FrameHeader, LogFileHeader, WalFrame, WAL_MAGIC, WAL_PAGE_SIZE,
    };
    use crate::replication::snapshot::SnapshotFile;

    use super::*;

    #[test]
    fn compact_file_create_snapshot() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let mut log_file = LogFile::new(temp.as_file().try_clone().unwrap(), 0).unwrap();
        let db_id = Uuid::new_v4();
        let expected_header = LogFileHeader {
            magic: WAL_MAGIC,
            start_checksum: 0,
            db_id: db_id.as_u128(),
            start_frame_id: 0,
            frame_count: 50,
            version: 0,
            page_size: WAL_PAGE_SIZE,
            _pad: 0,
        };
        log_file.write_header(&expected_header).unwrap();

        // add 50 pages, each one in two versions
        let mut frame_id = 0;
        for _ in 0..2 {
            for i in 0..25 {
                let frame_header = FrameHeader {
                    frame_id,
                    checksum: 0,
                    page_no: i,
                    size_after: i + 1,
                };
                let data = std::iter::repeat(0).take(4096).collect();
                let frame = WalFrame {
                    header: frame_header,
                    data,
                };
                log_file.push_frame(frame).unwrap();

                frame_id += 1;
            }
        }

        let dump_dir = tempdir().unwrap();
        let compactor = LogCompactor::new(dump_dir.path().to_path_buf());
        compactor
            .compact(log_file, temp.path().to_path_buf(), 25)
            .unwrap();

        thread::sleep(Duration::from_secs(1));

        let snapshot_path = dump_dir
            .path()
            .join("snapshots")
            .join(format!("{}-{}-{}.snap", db_id, 0, 49));
        let snapshot = read(&snapshot_path).unwrap();
        let header: &SnapshotFileHeader =
            try_from_bytes(&snapshot[..std::mem::size_of::<SnapshotFileHeader>()]).unwrap();

        assert_eq!(header.start_frame_id, 0);
        assert_eq!(header.end_frame_index, 49);
        assert_eq!(header.frame_count, 25);
        assert_eq!(header.db_id, db_id.as_u128());
        assert_eq!(header.size_after, 25);

        let mut seen_frames = HashSet::new();
        let mut seen_page_no = HashSet::new();
        let data = &snapshot[std::mem::size_of::<SnapshotFileHeader>()..];
        data.chunks(LogFile::FRAME_SIZE).for_each(|f| {
            let frame = WalFrame::decode(Bytes::copy_from_slice(f)).unwrap();
            assert!(!seen_frames.contains(&frame.header.frame_id));
            assert!(!seen_page_no.contains(&frame.header.page_no));
            seen_page_no.insert(frame.header.page_no);
            seen_frames.insert(frame.header.frame_id);
            assert!(frame.header.frame_id >= 25);
        });

        assert_eq!(seen_frames.len(), 25);
        assert_eq!(seen_page_no.len(), 25);

        let snapshot_file = SnapshotFile::open(&snapshot_path).unwrap();

        let frames = snapshot_file.frames_iter_until(0);
        let mut expected_frame_id = 49;
        for frame in frames {
            let frame = frame.unwrap();
            let header: FrameHeader = pod_read_unaligned(&frame[..size_of::<FrameHeader>()]);
            assert_eq!(header.frame_id, expected_frame_id);
            expected_frame_id -= 1;
        }

        assert_eq!(expected_frame_id, 24);
    }
}
