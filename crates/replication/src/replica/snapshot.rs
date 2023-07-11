use std::path::{Path, PathBuf};

use futures::{Stream, StreamExt};
use tempfile::NamedTempFile;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::frame::{Frame, FrameBorrowed};

#[derive(Debug)]
pub struct TempSnapshot {
    path: PathBuf,
    map: memmap::Mmap,
    delete_on_drop: bool,
}

// Transplanted directly from sqld: replication/snapshot.rs
#[derive(Debug, Copy, Clone, PartialEq, bytemuck::Pod, bytemuck::Zeroable, Eq)]
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
// end of transplant

impl TempSnapshot {
    pub fn from_snapshot_file(path: &Path) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path).unwrap();
        let mut map_options = memmap::MmapOptions::new();
        // Skip the snapshot file header
        map_options.offset(std::mem::size_of::<SnapshotFileHeader>() as u64);
        let map = unsafe { map_options.map(&file)? };

        Ok(Self {
            path: path.to_owned(),
            map,
            delete_on_drop: false,
        })
    }

    pub async fn from_stream(
        db_path: &Path,
        mut s: impl Stream<Item = anyhow::Result<Frame>> + Unpin,
    ) -> anyhow::Result<Self> {
        let temp_dir = db_path.join("temp");
        tokio::fs::create_dir_all(&temp_dir).await?;
        let file = NamedTempFile::new_in(temp_dir)?;
        let tokio_file = tokio::fs::File::from_std(file.as_file().try_clone()?);

        let mut tokio_file = BufWriter::new(tokio_file);
        while let Some(frame) = s.next().await {
            let frame = frame?;
            tokio_file.write_all(frame.as_slice()).await?;
        }

        tokio_file.flush().await?;

        let (file, path) = file.keep()?;

        let map = unsafe { memmap::Mmap::map(&file)? };

        Ok(Self {
            path,
            map,
            delete_on_drop: true,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn iter(&self) -> impl Iterator<Item = &FrameBorrowed> {
        self.map.chunks(Frame::SIZE).map(FrameBorrowed::from_bytes)
    }
}

impl Drop for TempSnapshot {
    fn drop(&mut self) {
        if self.delete_on_drop {
            let path = std::mem::take(&mut self.path);
            let _ = std::fs::remove_file(path);
        }
    }
}
