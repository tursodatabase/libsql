use std::path::{Path, PathBuf};

use futures::{Stream, StreamExt};
use tempfile::NamedTempFile;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::replication::frame::{Frame, FrameBorrowed};

#[derive(Debug)]
pub struct TempSnapshot {
    path: PathBuf,
    map: memmap::Mmap,
}

impl TempSnapshot {
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

        Ok(Self { path, map })
    }

    pub fn iter(&self) -> impl Iterator<Item = &FrameBorrowed> {
        self.map.chunks(Frame::SIZE).map(FrameBorrowed::from_bytes)
    }
}

impl Drop for TempSnapshot {
    fn drop(&mut self) {
        let path = std::mem::take(&mut self.path);
        let _ = std::fs::remove_file(path);
    }
}
