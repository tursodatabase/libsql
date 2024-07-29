#![allow(async_fn_in_trait, dead_code)]

pub mod checkpointer;
pub mod error;
pub mod io;
pub mod registry;
pub mod replication;
pub mod segment;
pub mod shared_wal;
pub mod storage;
pub mod transaction;
pub mod wal;

const LIBSQL_MAGIC: u64 = u64::from_be_bytes(*b"LIBSQL\0\0");

#[cfg(any(debug_assertions, test))]
pub mod test {
    use std::fs::OpenOptions;
    use std::path::PathBuf;
    use std::path::Path;
    use std::sync::Arc;

    use libsql_sys::rusqlite::OpenFlags;
    use libsql_sys::name::NamespaceName;
    use tempfile::{tempdir, TempDir};
    use tokio::sync::mpsc;

    use crate::checkpointer::LibsqlCheckpointer;
    use crate::io::Io;
    use crate::io::StdIO;
    use crate::registry::WalRegistry;
    use crate::shared_wal::SharedWal;
    use crate::storage::TestStorage;
    use crate::wal::{LibsqlWal, LibsqlWalManager};

    pub struct TestEnv<IO: Io = StdIO> {
        pub tmp: Arc<TempDir>,
        pub registry: Arc<WalRegistry<IO, TestStorage<IO>>>,
        pub wal: LibsqlWalManager<IO, TestStorage<IO>>,
    }

    impl TestEnv {
        pub fn new() -> Self {
            Self::new_store(false)
        }

        pub fn new_store(store: bool) -> Self {
            TestEnv::new_io(StdIO(()), store)
        }
    }

    impl<IO: Io + Clone> TestEnv<IO> {
        pub fn new_io(io: IO, store: bool) -> Self {
            let tmp = tempdir().unwrap();
            Self::new_io_and_tmp(io, tmp.into(), store)
        }

        pub fn new_io_and_tmp(io: IO, tmp: Arc<TempDir>, store: bool) -> Self {
            let resolver = |path: &Path| {
                let name = path
                    .parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap();
                NamespaceName::from_string(name.to_string())
            };

            let (sender, receiver) = mpsc::channel(128);
            let registry = Arc::new(
                WalRegistry::new_with_io(
                    io.clone(),
                    tmp.path().join("test/wals"),
                    TestStorage::new_io(store, io),
                    sender,
                )
                .unwrap(),
            );
            if store {
                let checkpointer = LibsqlCheckpointer::new(registry.clone(), receiver, 5);
                tokio::spawn(checkpointer.run());
            }
            let wal = LibsqlWalManager::new(registry.clone(), Arc::new(resolver));

            Self { tmp, registry, wal }
        }

        pub fn shared(&self, namespace: &str) -> Arc<SharedWal<IO>> {
            let path = self.tmp.path().join(namespace).join("data");
            let registry = self.registry.clone();
            let namespace = NamespaceName::from_string(namespace.into());
            registry.clone().open(path.as_ref(), &namespace).unwrap()
        }

        pub fn db_path(&self, namespace: &str) -> PathBuf {
            self.tmp.path().join(namespace)
        }

        pub fn open_conn(
            &self,
            namespace: &'static str,
        ) -> libsql_sys::Connection<LibsqlWal<IO>> {
            let path = self.db_path(namespace);
            let wal = self.wal.clone();
            std::fs::create_dir_all(&path).unwrap();
            libsql_sys::Connection::open(
                path.join("data"),
                OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
                wal,
                100000,
                None,
            )
            .unwrap()
        }

        pub fn db_file(&self, namespace: &str) -> std::fs::File {
            let path = self.db_path(namespace);
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap()
        }
    }

    pub fn seal_current_segment<IO: Io>(shared: &SharedWal<IO>) {
        let mut tx = shared.begin_read(99999).into();
        shared.upgrade(&mut tx).unwrap();
        {
            let mut guard = tx.as_write_mut().unwrap().lock();
            guard.commit();
            shared.swap_current(&mut guard).unwrap();
        }
        tx.end();
    }
}
