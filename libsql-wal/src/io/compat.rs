use std::io;

use bytes::BytesMut;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;

use super::FileExt;

/// Copy from src that implements AsyncRead to the detination file, returning how many bytes have
/// been copied
pub async fn copy_to_file<R, F>(mut src: R, dst: &F) -> io::Result<usize>
where
    F: FileExt,
    R: AsyncRead + Unpin,
{
    let mut dst_offset = 0u64;
    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        let n = src.read_buf(&mut buffer).await?;
        if n == 0 {
            return Ok(dst_offset as usize);
        }
        let (b, ret) = dst.write_all_at_async(buffer, dst_offset).await;
        ret?;
        dst_offset += n as u64;
        buffer = b;
        buffer.clear();
    }
}
