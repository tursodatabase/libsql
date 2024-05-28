use std::io;
use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use self::file::FileExt;

pub mod buf;
pub mod file;

pub trait Io: Send + Sync + 'static {
    type File: FileExt;
    type TempFile: FileExt;

    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
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
    fn uuid(&self) -> Uuid;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StdIO(pub(crate) ());

impl Io for StdIO {
    type File = std::fs::File;
    type TempFile = std::fs::File;

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
}

impl<T: Io> Io for Arc<T> {
    type File = T::File;
    type TempFile = T::TempFile;

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
}
