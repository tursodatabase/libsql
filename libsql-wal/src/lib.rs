#![allow(async_fn_in_trait)]

pub mod bottomless;
pub mod error;
pub mod io;
pub mod registry;
pub mod replication;
pub mod segment;
pub mod shared_wal;
pub mod transaction;
pub mod wal;

#[cfg(test)]
pub(crate) mod test {
    use std::{path::Path, sync::Arc};

    use libsql_sys::{name::NamespaceName, rusqlite::OpenFlags};
    use tempfile::{tempdir, TempDir};

    use crate::{io::StdIO, registry::WalRegistry, shared_wal::SharedWal, wal::{LibsqlWal, LibsqlWalManager}};

    pub struct TestEnv {
        pub tmp: TempDir,
        pub registry: Arc<WalRegistry<StdIO>>,
        pub wal: LibsqlWalManager<StdIO>,
    }

    impl TestEnv {
        pub fn new() -> Self {
            let tmp = tempdir().unwrap();
            let resolver = |path: &Path| {
                let name = path.file_name().unwrap().to_str().unwrap();
                NamespaceName::from_string(name.to_string())
            };

            let registry =
                Arc::new(WalRegistry::new(tmp.path().join("test/wals"), resolver, ()).unwrap());
            let wal = LibsqlWalManager::new(registry.clone());

            Self {
                tmp,
                registry,
                wal,
            }
        }

        pub fn shared(&self, namespace: &str) -> Arc<SharedWal<StdIO>> {
            let path = self.tmp.path().join(namespace).join("data");
            self.registry.clone().open(path.as_ref()).unwrap()
        }

        pub fn open_conn(&self, namespace: &str) -> libsql_sys::Connection<LibsqlWal<StdIO>> {
            let path = self.tmp.path().join(namespace);
            std::fs::create_dir_all(&path).unwrap();
            libsql_sys::Connection::open(
                path.join("data"),
                OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
                self.wal.clone(),
                100000,
                None,
            )
                .unwrap()
        }
    }

    pub fn seal_current_segment(shared: &SharedWal<StdIO>) {
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
