use std::path::Path;
use std::sync::Arc;
use std::{future::Future, io};

use chrono::{DateTime, Utc};
use rand::{rngs::ThreadRng, thread_rng, Rng};
use uuid::Uuid;

pub use self::file::FileExt;

pub mod buf;
pub mod compat;
pub mod file;

pub trait Io: Send + Sync + 'static {
    type File: FileExt;
    type TempFile: FileExt;
    type Rng: Rng;

    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    /// TODO: when adding an async variant make sure all places where async is needed are replaced
    fn open(
        &self,
        create_new: bool,
        read: bool,
        write: bool,
        path: &Path,
    ) -> io::Result<Self::File>;

    // todo: create an async counterpart
    fn tempfile(&self) -> io::Result<Self::TempFile>;
    fn now(&self) -> DateTime<Utc>;
    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()>;
    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::Rng) -> R;
    fn uuid(&self) -> uuid::Uuid {
        self.with_rng(|rng| {
            let n: u128 = rng.gen();
            Uuid::from_u128(n)
        })
    }

    fn remove_file_async(&self, path: &Path) -> impl Future<Output = io::Result<()>> + Send;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StdIO(pub(crate) ());

impl Io for StdIO {
    type File = std::fs::File;
    type TempFile = std::fs::File;
    type Rng = ThreadRng;

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn open(
        &self,
        create_new: bool,
        read: bool,
        write: bool,
        path: &Path,
    ) -> io::Result<Self::File> {
        std::fs::OpenOptions::new()
            .create_new(create_new)
            .create(write)
            .read(read)
            .write(write)
            .open(path)
    }

    fn tempfile(&self) -> io::Result<Self::TempFile> {
        tempfile::tempfile()
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn uuid(&self) -> Uuid {
        Uuid::new_v4()
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        std::fs::hard_link(src, dst)
    }

    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::Rng) -> R,
    {
        f(&mut thread_rng())
    }

    async fn remove_file_async(&self, path: &Path) -> io::Result<()> {
        tokio::fs::remove_file(path).await
    }
}

impl<T: Io> Io for Arc<T> {
    type File = T::File;
    type TempFile = T::TempFile;
    type Rng = T::Rng;

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.as_ref().create_dir_all(path)
    }

    fn open(
        &self,
        create_new: bool,
        read: bool,
        write: bool,
        path: &Path,
    ) -> io::Result<Self::File> {
        self.as_ref().open(create_new, read, write, path)
    }

    fn tempfile(&self) -> io::Result<Self::TempFile> {
        self.as_ref().tempfile()
    }

    fn now(&self) -> DateTime<Utc> {
        self.as_ref().now()
    }

    fn uuid(&self) -> Uuid {
        self.as_ref().uuid()
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.as_ref().hard_link(src, dst)
    }

    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::Rng) -> R,
    {
        self.as_ref().with_rng(f)
    }

    async fn remove_file_async(&self, path: &Path) -> io::Result<()> {
        self.as_ref().remove_file_async(path).await
    }
}

pub struct Inspect<W, F> {
    inner: W,
    f: F,
}

impl<W, F> Inspect<W, F> {
    pub fn new(inner: W, f: F) -> Self {
        Self { inner, f }
    }

    pub(crate) fn into_inner(self) -> W {
        self.inner
    }
}

impl<W, F> io::Write for Inspect<W, F>
where
    W: io::Write,
    for<'a> F: FnMut(&'a [u8]),
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (self.f)(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
