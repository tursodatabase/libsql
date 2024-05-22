use std::io;
use std::path::Path;

use self::file::FileExt;

pub mod file;
pub mod buf;

pub trait FileSystem: Send + Sync + 'static {
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
}

#[derive(Default, Debug, Clone, Copy)]
pub struct StdFs(pub(crate) ());

impl FileSystem for StdFs {
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
}
