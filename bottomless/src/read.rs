use crate::replicator::CompressionKind;
use crate::wal::WalFrameHeader;
use anyhow::Result;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use std::io::ErrorKind;
use std::pin::Pin;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, BufReader};

type AsyncByteReader = dyn AsyncRead + Send + Sync;

pub struct BatchReader {
    reader: Pin<Box<AsyncByteReader>>,
    next_frame_no: u32,
}

impl BatchReader {
    pub fn new(
        init_frame_no: u32,
        content_stream: impl AsyncBufRead + Send + Sync + 'static,
        page_size: usize,
        use_compression: CompressionKind,
    ) -> Self {
        BatchReader {
            next_frame_no: init_frame_no,
            reader: match use_compression {
                CompressionKind::None => {
                    let reader =
                        BufReader::with_capacity(page_size + WalFrameHeader::SIZE, content_stream);
                    Box::pin(reader)
                }
                CompressionKind::Gzip => {
                    let gzip = GzipDecoder::new(content_stream);
                    Box::pin(gzip)
                }
                CompressionKind::Zstd => {
                    let zstd = ZstdDecoder::new(content_stream);
                    Box::pin(zstd)
                }
            },
        }
    }

    /// Reads next frame header without frame body (WAL page).
    pub async fn next_frame_header(&mut self) -> Result<Option<WalFrameHeader>> {
        let mut buf = [0u8; WalFrameHeader::SIZE];
        let res = self.reader.read_exact(&mut buf).await;
        match res {
            Ok(_) => Ok(Some(WalFrameHeader::from(buf))),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Reads the next frame stored in a current batch.
    /// Returns a frame number or `None` if no frame was remaining in the buffer.
    pub async fn next_page(&mut self, page_buf: &mut [u8]) -> Result<()> {
        self.reader.read_exact(page_buf).await?;
        self.next_frame_no += 1;
        Ok(())
    }

    pub fn next_frame_no(&self) -> u32 {
        self.next_frame_no
    }
}
