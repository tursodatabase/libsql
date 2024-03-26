use std::io;
use std::path::Path;

use self::file::FileExt;

pub mod file;

pub trait FileSystem {
    type File: FileExt;

    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn open(
        &self,
        create_new: bool,
        read: bool,
        write: bool,
        path: &Path,
    ) -> io::Result<Self::File>;
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StdFs(pub(crate) ());

impl FileSystem for StdFs {
    type File = std::fs::File;

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
}
