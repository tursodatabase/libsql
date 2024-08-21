use std::fs::File;
use std::future::Future;
use std::io::{self, ErrorKind, IoSlice, Result, Write};

use libsql_sys::wal::either::Either;

use super::buf::{IoBuf, IoBufMut};

pub trait FileExt: Send + Sync + 'static {
    fn len(&self) -> io::Result<u64>;
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        let mut written = 0;

        while written != buf.len() {
            written += self.write_at(&buf[written..], offset + written as u64)?;
        }

        Ok(())
    }
    fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let mut read = 0;

        while read != buf.len() {
            let n = self.read_at(&mut buf[read..], offset + read as u64)?;
            if n == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "unexpected end-of-file",
                ));
            }
            read += n;
        }

        Ok(())
    }

    fn sync_all(&self) -> Result<()>;

    fn set_len(&self, len: u64) -> Result<()>;

    fn cursor(&self, offset: u64) -> Cursor<Self>
    where
        Self: Sized,
    {
        Cursor {
            file: self,
            offset,
            count: 0,
        }
    }

    #[must_use]
    fn read_exact_at_async<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<()>)> + Send;

    #[must_use]
    fn read_at_async<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<usize>)> + Send;

    #[must_use]
    fn write_all_at_async<B: IoBuf + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<()>)> + Send;
}

impl<U, V> FileExt for Either<U, V>
where
    V: FileExt,
    U: FileExt,
{
    fn len(&self) -> io::Result<u64> {
        match self {
            Either::A(x) => x.len(),
            Either::B(x) => x.len(),
        }
    }

    fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
        match self {
            Either::A(x) => x.write_at_vectored(bufs, offset),
            Either::B(x) => x.write_at_vectored(bufs, offset),
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        match self {
            Either::A(x) => x.write_at(buf, offset),
            Either::B(x) => x.write_at(buf, offset),
        }
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        match self {
            Either::A(x) => x.read_at(buf, offset),
            Either::B(x) => x.read_at(buf, offset),
        }
    }

    fn sync_all(&self) -> Result<()> {
        match self {
            Either::A(x) => x.sync_all(),
            Either::B(x) => x.sync_all(),
        }
    }

    fn set_len(&self, len: u64) -> Result<()> {
        match self {
            Either::A(x) => x.set_len(len),
            Either::B(x) => x.set_len(len),
        }
    }

    fn read_exact_at_async<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<()>)> + Send {
        async move {
            match self {
                Either::A(x) => x.read_exact_at_async(buf, offset).await,
                Either::B(x) => x.read_exact_at_async(buf, offset).await,
            }
        }
    }

    fn read_at_async<B: IoBufMut + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<usize>)> + Send {
        async move {
            match self {
                Either::A(x) => x.read_at_async(buf, offset).await,
                Either::B(x) => x.read_at_async(buf, offset).await,
            }
        }
    }

    fn write_all_at_async<B: IoBuf + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> impl Future<Output = (B, Result<()>)> + Send {
        async move {
            match self {
                Either::A(x) => x.write_all_at_async(buf, offset).await,
                Either::B(x) => x.write_all_at_async(buf, offset).await,
            }
        }
    }
}

impl FileExt for File {
    fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
        Ok(nix::sys::uio::pwritev(self, bufs, offset as _)?)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        Ok(nix::sys::uio::pwrite(self, buf, offset as _)?)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        let n = nix::sys::uio::pread(self, buf, offset as _)?;
        Ok(n)
    }

    fn sync_all(&self) -> Result<()> {
        std::fs::File::sync_all(self)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        std::fs::File::set_len(self, len)
    }

    async fn read_exact_at_async<B: IoBufMut + Send + 'static>(
        &self,
        mut buf: B,
        offset: u64,
    ) -> (B, Result<()>) {
        let file = self.try_clone().unwrap();
        let (buffer, ret) = tokio::task::spawn_blocking(move || {
            // let mut read = 0;

            let chunk = unsafe {
                let len = buf.bytes_total();
                let ptr = buf.stable_mut_ptr();
                std::slice::from_raw_parts_mut(ptr, len)
            };

            let ret = file.read_exact_at(chunk, offset);
            if ret.is_ok() {
                unsafe {
                    buf.set_init(buf.bytes_total());
                }
            }
            (buf, ret)
        })
        .await
        .unwrap();

        (buffer, ret)
    }

    async fn read_at_async<B: IoBufMut + Send + 'static>(
        &self,
        mut buf: B,
        offset: u64,
    ) -> (B, Result<usize>) {
        let file = self.try_clone().unwrap();
        let (buffer, ret) = tokio::task::spawn_blocking(move || {
            // let mut read = 0;

            let chunk = unsafe {
                let len = buf.bytes_total();
                let ptr = buf.stable_mut_ptr();
                std::slice::from_raw_parts_mut(ptr, len)
            };

            let ret = file.read_at(chunk, offset);
            if let Ok(n) = ret {
                unsafe {
                    buf.set_init(n);
                }
            }
            (buf, ret)
        })
        .await
        .unwrap();

        (buffer, ret)
    }

    async fn write_all_at_async<B: IoBuf + Send + 'static>(
        &self,
        buf: B,
        offset: u64,
    ) -> (B, Result<()>) {
        let file = self.try_clone().unwrap();
        let (buffer, ret) = tokio::task::spawn_blocking(move || {
            let buffer = unsafe { std::slice::from_raw_parts(buf.stable_ptr(), buf.bytes_init()) };
            let ret = file.write_all_at(buffer, offset);
            (buf, ret)
        })
        .await
        .unwrap();

        (buffer, ret)
    }

    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

#[derive(Debug)]
pub struct Cursor<'a, T> {
    file: &'a T,
    offset: u64,
    count: u64,
}

impl<T> Cursor<'_, T> {
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl<T: FileExt> Write for Cursor<'_, T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let count = self.file.write_at(buf, self.offset + self.count)?;
        self.count += count as u64;
        Ok(count)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct BufCopy<W> {
    w: W,
    buf: Vec<u8>,
}

impl<W> BufCopy<W> {
    pub fn new(w: W) -> Self {
        Self { w, buf: Vec::new() }
    }

    pub fn into_parts(self) -> (W, Vec<u8>) {
        let Self { w, buf } = self;
        (w, buf)
    }

    pub fn get_ref(&self) -> &W {
        &self.w
    }
}

impl<W: Write> Write for BufCopy<W> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let count = self.w.write(buf)?;
        self.buf.extend_from_slice(&buf[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> Result<()> {
        self.w.flush()
    }
}

#[cfg(test)]
mod test {
    use std::io::Read;

    use tempfile::tempfile;

    use super::*;

    #[tokio::test]
    async fn test_write_async() {
        let mut file = tempfile().unwrap();

        let buf = vec![1u8; 12345];
        let (buf, ret) = file.write_all_at_async(buf, 0).await;
        ret.unwrap();
        assert_eq!(buf.len(), 12345);
        assert!(buf.iter().all(|x| *x == 1));

        let buf = vec![2u8; 50];
        let (buf, ret) = file.write_all_at_async(buf, 12345).await;
        ret.unwrap();
        assert_eq!(buf.len(), 50);
        assert!(buf.iter().all(|x| *x == 2));

        let mut out = Vec::new();
        file.read_to_end(&mut out).unwrap();
        assert!(out[0..12345].iter().all(|x| *x == 1));
        assert!(out[12345..].iter().all(|x| *x == 2));
    }

    #[tokio::test]
    async fn test_read() {
        let mut file = tempfile().unwrap();

        file.write_all(&[1; 12345]).unwrap();
        file.write_all(&[2; 50]).unwrap();

        let buf = vec![0u8; 12345];
        let (buf, ret) = file.read_exact_at_async(buf, 0).await;
        ret.unwrap();
        assert_eq!(buf.len(), 12345);
        assert!(buf.iter().all(|x| *x == 1));

        let buf = vec![2u8; 50];
        let (buf, ret) = file.read_exact_at_async(buf, 12345).await;
        ret.unwrap();
        assert_eq!(buf.len(), 50);
        assert!(buf.iter().all(|x| *x == 2));
    }
}
