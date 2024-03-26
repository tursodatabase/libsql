use std::fs::File;
use std::io::{self, ErrorKind, IoSlice, Result, Write};

pub trait FileExt {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<()>;
    fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> Result<usize>;
    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize>;

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

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
}

impl FileExt for File {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        let mut written = 0;

        while written != buf.len() {
            written += nix::sys::uio::pwrite(self, &buf[written..], offset as _)?;
        }

        Ok(())
    }

    fn write_at_vectored(&self, bufs: &[IoSlice], offset: u64) -> Result<usize> {
        Ok(nix::sys::uio::pwritev(self, bufs, offset as _)?)
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> Result<usize> {
        Ok(nix::sys::uio::pwrite(self, buf, offset as _)?)
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        let mut read = 0;

        while read != buf.len() {
            let n = nix::sys::uio::pread(self, &mut buf[read..], (offset + read as u64) as _)?;
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

    fn sync_all(&self) -> Result<()> {
        std::fs::File::sync_all(self)
    }

    fn set_len(&self, len: u64) -> Result<()> {
        std::fs::File::set_len(self, len)
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
