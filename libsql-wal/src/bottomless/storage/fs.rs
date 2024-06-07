use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use zerocopy::FromBytes;

use crate::bottomless::{Error, Result};
use crate::io::{FileExt, Io};
use crate::name::NamespaceName;
use crate::segment::SegmentHeader;

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

        let buf = Vec::with_capacity(segment_data.len().unwrap() as usize);

        let f = self.io.open(true, true, true, &path).unwrap();
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
        dest: impl AsyncWrite,
    ) -> Result<()> {
        let dir = self.prefix.join("segments");

        // TODO(lucio): optimization would be to cache this list, since we update the files in the
        // store fn we can keep track without having to go to the OS each time.
        let mut dirs = tokio::fs::read_dir(dir).await?;

        while let Some(dir) = dirs.next_entry().await? {
            let file = dir.file_name();
            let key = file.to_str().unwrap().split(".").next().unwrap();
            let mut comp = key.split("-");

            let start_frame = comp.next().unwrap();
            let end_frame = comp.next().unwrap();

            let start_frame: u64 = start_frame.parse().unwrap();
            let end_frame: u64 = end_frame.parse().unwrap();

            if start_frame <= frame_no && end_frame >= frame_no {
                let file = self.io.open(true, true, false, &dir.path()).unwrap();

                let buf = Vec::new();
                let (mut buf, res) = file.read_exact_at_async(buf, 0).await;
                res.unwrap();

                // Assert the header from the segment matches the key in its path
                let header = SegmentHeader::ref_from_prefix(&buf[..]).unwrap();
                let start_frame_from_header = header.start_frame_no.get();
                let end_frame_from_header = header.last_commited_frame_no.get();

                // TOOD(lucio): convert these into errors before prod
                assert_eq!(start_frame, start_frame_from_header);
                assert_eq!(end_frame, end_frame_from_header);

                tokio::pin!(dest);
                dest.write_all(&mut buf[..]).await.unwrap();

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
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{bottomless::Storage, io::StdIO};

    #[tokio::test]
    async fn read_write() {
        let dir = std::env::temp_dir();

        let fs = FsStorage::new(dir, StdIO::default()).unwrap();

        let namespace = NamespaceName::from_string("".into());
        let segment = vec![0u8; 4096];

        fs.store(
            &(),
            crate::bottomless::storage::SegmentMeta {
                namespace: namespace.clone(),
                segment_id: Uuid::new_v4(),
                start_frame_no: 0,
                end_frame_no: 64,
                created_at: Utc::now(),
            },
            segment,
            Vec::new(),
        )
        .await
        .unwrap();

        let mut dest = Vec::new();
        fs.fetch_segment(&(), namespace.clone(), 5, &mut dest)
            .await
            .unwrap();
    }
}
