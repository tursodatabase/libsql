#![allow(dead_code)]

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use libsql_replication::frame::FrameBorrowed;
use libsql_replication::snapshot::SnapshotFile;
use libsql_replication::snapshot::SnapshotFileHeader;
use libsql_sys::wal::{Sqlite3Wal, Sqlite3WalManager};
use parking_lot::Mutex;
use rusqlite::named_params;
use rusqlite::OptionalExtension;
use tempfile::NamedTempFile;
use uuid::Uuid;
use zerocopy::{AsBytes, FromZeroes};

use crate::connection::libsql::open_conn_active_checkpoint;
use crate::namespace::NamespaceName;

use super::FrameNo;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("error handling snapshot file: {0}")]
    SnapshotFile(#[from] libsql_replication::snapshot::Error),
}

#[derive(Clone)]
pub struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
    /// path to the temporary directory
    snapshots_path: PathBuf,
    temp_path: PathBuf,
    // TODO: use a pool to allow concurrent read and writes.
    conn: Mutex<libsql_sys::Connection<Sqlite3Wal>>,
}

impl SnapshotStoreInner {
    fn snapshots_path(&self, namespace: &NamespaceName) -> PathBuf {
        self.snapshots_path.join(namespace.as_str())
    }
}

impl SnapshotStoreInner {
    fn register(
        &self,
        namespace: &NamespaceName,
        start_frame_no: FrameNo,
        end_frame_no: FrameNo,
        snapshot_id: Uuid,
        exec_before: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        assert!(start_frame_no <= end_frame_no);
        let mut conn = self.conn.lock();
        let txn = conn.transaction()?;
        exec_before()?;
        {
            let mut stmt =
                txn.prepare_cached("INSERT INTO snapshots VALUES (?, ?, ?, ?, date(), 0)")?;
            stmt.execute((
                namespace.as_str(),
                start_frame_no,
                end_frame_no,
                snapshot_id.to_string(),
            ))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// delete all snapshots for a namespace
    pub fn delete_all(&self, namespace: &NamespaceName) -> Result<()> {
        let conn = self.conn.lock();
        conn.execute(
            "DELETE FROM snapshots WHERE namespace = ?",
            [namespace.as_str()],
        )?;
        let snapshots_path = self.snapshots_path(namespace);
        if snapshots_path.try_exists()? {
            std::fs::remove_dir_all(self.snapshots_path(namespace))?;
        }

        Ok(())
    }
}

impl SnapshotStore {
    pub async fn new(base_path: &Path) -> Result<Self> {
        let store_path = base_path.join("snapshot-store");
        let snapshots_path = store_path.join("snapshots");
        tokio::fs::create_dir_all(&snapshots_path).await?;

        let temp_path = base_path.join("tmp");
        tokio::fs::create_dir_all(&temp_path).await?;
        let conn = open_conn_active_checkpoint(
            &store_path,
            Sqlite3WalManager::default(),
            None,
            1000,
            None,
        )?;
        // set the user version for future migration.
        conn.pragma_update(None, "user_version", 1)?;
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS snapshots (
                namespace TEXT NOT NULL,
                start_frame_no INTEGER,
                end_frame_no INTEGER,
                snapshot_id TEXT NOT NULL UNIQUE,
                created_at DATE NOT NULL,
                durable INTEGER
                )"#,
            (),
        )?;

        let inner = Arc::new(SnapshotStoreInner {
            snapshots_path,
            temp_path,
            conn: Mutex::new(conn),
        });

        Ok(Self { inner })
    }

    pub fn builder(&self, namespace_name: NamespaceName, db_size: u32) -> Result<SnapshotBuilder> {
        let mut snapshot_file = NamedTempFile::new_in(&self.inner.temp_path)?;

        snapshot_file
            .write_all(SnapshotFileHeader::new_zeroed().as_bytes())
            .unwrap();

        Ok(SnapshotBuilder {
            snapshot_file,
            store: self.inner.clone(),
            frame_count: 0,
            end_frame_no: 0,
            last_seen_frame_no: None,
            db_size,
            name: namespace_name,
        })
    }

    /// Returns the biggest snapshot for namespace that contains frame_no
    // FIXME: make async?
    pub fn find(&self, namespace: &NamespaceName, frame_no: FrameNo) -> Result<Option<Uuid>> {
        let conn = self.inner.conn.lock();
        let mut stmt = conn.prepare_cached(
            r#"SELECT snapshot_id FROM snapshots 
            WHERE namespace = :namespace
                AND start_frame_no <= :fno
                AND end_frame_no >= :fno
            ORDER BY (end_frame_no - start_frame_no) DESC
            LIMIT 1"#,
        )?;
        let snapshot_id = stmt
            .query_row(
                named_params!( ":namespace": namespace.as_str(), ":fno": frame_no),
                |row| {
                    let s = row.get_ref(0)?.as_str()?;
                    Ok(Uuid::from_str(s).unwrap())
                },
            )
            .optional()?;

        Ok(snapshot_id)
    }

    pub async fn find_file(
        &self,
        namespace: &NamespaceName,
        frame_no: FrameNo,
    ) -> Result<Option<SnapshotFile>> {
        match self.find(namespace, frame_no)? {
            Some(snapshot_id) => {
                let path = self
                    .inner
                    .snapshots_path(namespace)
                    .join(snapshot_id.to_string());
                let file = SnapshotFile::open(path, None).await?;
                Ok(Some(file))
            }
            None => Ok(None),
        }
    }

    pub async fn delete_all(&self, namespace: NamespaceName) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.delete_all(&namespace))
            .await
            .unwrap()?;

        Ok(())
    }
}

pub struct SnapshotBuilder {
    /// Temporary file to hold the snapshot, before it's persisted with as self.name
    snapshot_file: NamedTempFile,
    store: Arc<SnapshotStoreInner>,
    frame_count: u64,
    end_frame_no: FrameNo,
    last_seen_frame_no: Option<FrameNo>,
    db_size: u32,
    name: NamespaceName,
}

impl SnapshotBuilder {
    pub fn add_frame(&mut self, frame: &FrameBorrowed) -> Result<()> {
        let frame_no = frame.header().frame_no.get();
        match self.last_seen_frame_no {
            Some(last_seen) => {
                assert!(last_seen > frame_no);
            }
            None => {
                self.end_frame_no = frame_no;
            }
        }

        self.last_seen_frame_no = Some(frame_no);
        self.frame_count += 1;

        self.snapshot_file.write_all(frame.as_bytes()).unwrap();

        Ok(())
    }

    pub fn finish(mut self, start_frame_no: FrameNo) -> Result<()> {
        self.snapshot_file
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
        // TODO handle error.
        let end_frame_no = self.end_frame_no;
        let header = SnapshotFileHeader {
            log_id: 0.into(),
            start_frame_no: start_frame_no.into(),
            end_frame_no: end_frame_no.into(),
            frame_count: self.frame_count.into(),
            size_after: self.db_size.into(),
            _pad: Default::default(),
        };
        self.snapshot_file.write_all(header.as_bytes()).unwrap();
        self.snapshot_file.flush().unwrap();
        let snapshot_id = Uuid::new_v4();
        let snapshot_dir = self.store.snapshots_path(&self.name);
        std::fs::create_dir_all(&snapshot_dir)?;
        let snapshot_path = snapshot_dir.join(snapshot_id.to_string());
        // The snapshot it first persisted before it is registered with the store. We do that under
        // a write transaction. If the we fail to persist, then the snapshot is not registered in
        // the db, and we attempt to remove it. If we crashed before we registered it but after we
        // persist it, it will be cleaned on the next store startup.
        let ret = self.store.register(
            &self.name,
            start_frame_no,
            end_frame_no,
            snapshot_id,
            || {
                self.snapshot_file.persist(&snapshot_path).unwrap();
                Ok(())
            },
        );

        if ret.is_err() {
            // We ignore the error because this is a best effort to cleanup. The file may not exist
            // because we failed to persist it, and is cleaned by the tempfile.
            let _ = std::fs::remove_file(&snapshot_path);
        }

        ret
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::*;
    #[tokio::test]
    async fn insert_and_find_snapshot() {
        let tmp = tempdir().unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let id = Uuid::new_v4();
        store.inner.register(&name, 1, 17, id, || Ok(())).unwrap();
        assert_eq!(store.find(&name, 1).unwrap().unwrap(), id);
        assert_eq!(store.find(&name, 5).unwrap().unwrap(), id);
        assert_eq!(store.find(&name, 17).unwrap().unwrap(), id);
        assert!(store.find(&name, 0).unwrap().is_none());
        assert!(store.find(&name, 18).unwrap().is_none());
        assert!(store.find(&name, 999).unwrap().is_none());
    }

    #[tokio::test]
    async fn bigger_snapshot_is_returned() {
        let tmp = tempdir().unwrap();
        let store = SnapshotStore::new(tmp.path()).await.unwrap();
        let name = NamespaceName::from_string("test".into()).unwrap();
        let id_small = Uuid::new_v4();
        store
            .inner
            .register(&name, 1, 17, id_small, || Ok(()))
            .unwrap();
        let id_big = Uuid::new_v4();
        store
            .inner
            .register(&name, 1, 35, id_big, || Ok(()))
            .unwrap();
        assert_eq!(store.find(&name, 1).unwrap().unwrap(), id_big);

        // try again but inserting the big first
        let id_big = Uuid::new_v4();
        store
            .inner
            .register(&name, 46, 175, id_big, || Ok(()))
            .unwrap();
        let id_small = Uuid::new_v4();
        store
            .inner
            .register(&name, 42, 61, id_small, || Ok(()))
            .unwrap();
        assert_eq!(store.find(&name, 50).unwrap().unwrap(), id_big);
    }
}
