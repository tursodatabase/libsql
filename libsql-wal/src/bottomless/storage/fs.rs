use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::bottomless::job::CompactedSegmentDataHeader;
use crate::bottomless::{Error, Result};
use crate::io::{FileExt, Io};
use crate::name::NamespaceName;

use super::Storage;

pub struct FsStorage<I> {
    prefix: PathBuf,
    io: Arc<I>,
}

impl<I: Io> FsStorage<I> {
    fn new(prefix: PathBuf, io: I) -> Result<Self> {
        io.create_dir_all(&prefix.join("segments")).unwrap();

        Ok(FsStorage {
            prefix,
            io: Arc::new(io),
        })
    }
}

// TODO(lucio): handle errors for fs module
impl<I: Io> Storage for FsStorage<I> {
    type Config = ();

    fn store(
        &self,
        config: &Self::Config,
        meta: super::SegmentMeta,
        segment_data: impl crate::io::file::FileExt,
        segment_index: Vec<u8>,
    ) -> impl Future<Output = Result<()>> + Send {
        let key = format!(
            "{}-{}-{}.segment",
            meta.start_frame_no,
            meta.end_frame_no,
            meta.created_at.timestamp()
        );

        let path = self.prefix.join("segments").join(key);

        let buf = Vec::with_capacity(dbg!(segment_data.len().unwrap()) as usize);

        let f = self.io.open(true, false, true, dbg!(&path)).unwrap();
        async move {
            let (buf, res) = segment_data.read_exact_at_async(buf, 0).await;

            let (_, res) = f.write_all_at_async(buf, 0).await;
            res.unwrap();

            Ok(())
        }
    }

    async fn fetch_segment(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<()> {
        let dir = self.prefix.join("segments");

        // TODO(lucio): optimization would be to cache this list, since we update the files in the
        // store fn we can keep track without having to go to the OS each time.
        let mut dirs = tokio::fs::read_dir(dir).await?;

        while let Some(entry) = dirs.next_entry().await? {
            let file = entry.file_name();
            let key = file.to_str().unwrap().split(".").next().unwrap();
            let mut comp = key.split("-");

            let start_frame = comp.next().unwrap();
            let end_frame = comp.next().unwrap();

            let start_frame: u64 = start_frame.parse().unwrap();
            let end_frame: u64 = end_frame.parse().unwrap();

            if start_frame <= frame_no && end_frame >= frame_no {
                #[cfg(debug_assertions)]
                {
                    use crate::io::buf::ZeroCopyBuf;

                    let header_buf = ZeroCopyBuf::<CompactedSegmentDataHeader>::new_uninit();
                    let file = self
                        .io
                        .open(false, true, false, dbg!(&entry.path()))
                        .unwrap();
                    let (header_buf, res) = file.read_exact_at_async(header_buf, 0).await;
                    res.unwrap();

                    let header = header_buf.get_ref();
                    let start_frame_from_header = header.start_frame_no.get();
                    let end_frame_from_header = header.end_frame_no.get();

                    // TOOD(lucio): convert these into errors before prod
                    assert_eq!(start_frame, start_frame_from_header);
                    assert_eq!(end_frame, end_frame_from_header);
                }

                self.io.hard_link(&entry.path(), dest_path)?;

                return Ok(());
            }
        }

        Err(Error::Store("".into()))
    }

    async fn meta(
        &self,
        _config: &Self::Config,
        _namespace: NamespaceName,
    ) -> Result<super::DbMeta> {
        todo!()
    }

    fn default_config(&self) -> std::sync::Arc<Self::Config> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::Utc;
    use tempfile::{tempdir, NamedTempFile};
    use uuid::Uuid;
    use zerocopy::{AsBytes, FromZeroes};

    use super::*;
    use crate::{bottomless::Storage, io::StdIO};

    #[tokio::test]
    async fn read_write() {
        let dir = tempdir().unwrap();

        let fs = FsStorage::new(dir.path().into(), StdIO::default()).unwrap();

        let namespace = NamespaceName::from_string("".into());
        let mut segment = CompactedSegmentDataHeader {
            start_frame_no: 0.into(),
            frame_count: 10.into(),
            segment_id: 0.into(),
            end_frame_no: 64.into(),
        };

        fs.store(
            &(),
            crate::bottomless::storage::SegmentMeta {
                namespace: namespace.clone(),
                segment_id: Uuid::new_v4(),
                start_frame_no: 0,
                end_frame_no: 64,
                created_at: Utc::now(),
            },
            segment.as_bytes().to_vec(),
            Vec::new(),
        )
        .await
        .unwrap();

        let path = dir.path().join("fetched_segment");
        fs.fetch_segment(&(), namespace.clone(), 5, &path)
            .await
            .unwrap();

        let mut file = std::fs::File::open(path).unwrap();
        let mut header: CompactedSegmentDataHeader = CompactedSegmentDataHeader::new_zeroed();

        file.read_exact(header.as_bytes_mut()).unwrap();

        assert_eq!(header.start_frame_no.get(), 0);
        assert_eq!(header.end_frame_no.get(), 64);
    }
}
