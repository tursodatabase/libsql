use crate::wal::{WalFileReader, WalFrameHeader};
use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipEncoder;
use std::ops::Range;

pub(crate) struct BatchWriter {
    frames: Range<u32>,
    last_frame_crc: u64,
    use_compression: bool,
}

impl BatchWriter {
    pub fn new(use_compression: bool, frames: Range<u32>) -> Self {
        BatchWriter {
            last_frame_crc: 0,
            use_compression,
            frames,
        }
    }

    pub async fn read_frames(&mut self, wal: &mut WalFileReader) -> Result<Option<Vec<u8>>> {
        if self.frames.is_empty() {
            tracing::trace!("Attempting to flush an empty buffer");
            return Ok(None);
        }
        tracing::trace!(
            "Flushing frame range: [{}..{}) (total: {} frames)",
            self.frames.start,
            self.frames.end,
            self.frames.len()
        );
        wal.seek_frame(self.frames.start).await?;
        let capacity = self.frames.len() * wal.frame_size() as usize;
        let mut buf = Vec::with_capacity(capacity);
        buf.spare_capacity_mut();
        unsafe { buf.set_len(capacity) };
        let frames_read = wal.read_frame_range(buf.as_mut()).await?;
        if frames_read != self.frames.len() {
            return Err(anyhow!(
                "Specified write request was {} frames, but only {} were found in WAL.",
                self.frames.len(),
                frames_read
            ));
        }
        self.last_frame_crc = {
            let last_frame_offset = (frames_read - 1) * wal.frame_size() as usize;
            let header: [u8; WalFrameHeader::SIZE] = (&buf
                [last_frame_offset..(last_frame_offset + WalFrameHeader::SIZE)])
                .try_into()
                .unwrap();
            WalFrameHeader::from(header).crc()
        };
        let data = if self.use_compression {
            let mut gzip = GzipEncoder::new(&buf[..]);
            let mut compressed = Vec::with_capacity(capacity);
            tokio::io::copy(&mut gzip, &mut compressed).await?;
            tracing::trace!(
                "Compressed {} frames into {}B",
                frames_read,
                compressed.len()
            );
            compressed
        } else {
            buf
        };
        Ok(Some(data))
    }
}
