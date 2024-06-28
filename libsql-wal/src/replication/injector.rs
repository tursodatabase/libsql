//! The injector is the module in charge of injecting frames into a replica database.

use std::sync::Arc;

use crate::error::Result;
use crate::io::Io;
use crate::segment::Frame;
use crate::shared_wal::SharedWal;
use crate::transaction::TxGuard;

/// The injector takes frames and injects them in the wal.
pub struct Injector<'a, IO: Io> {
    // The wal to which we are injecting
    wal: Arc<SharedWal<IO>>,
    buffer: Vec<Box<Frame>>,
    /// capacity of the frame buffer
    capacity: usize,
    tx: TxGuard<'a, IO::File>,
    max_tx_frame_no: u64,
}

impl<'a, IO: Io> Injector<'a, IO> {
    pub fn new(
        wal: Arc<SharedWal<IO>>,
        tx: TxGuard<'a, IO::File>,
        buffer_capacity: usize,
    ) -> Result<Self> {
        Ok(Self {
            wal,
            buffer: Vec::with_capacity(buffer_capacity),
            capacity: buffer_capacity,
            tx,
            max_tx_frame_no: 0,
        })
    }

    pub async fn insert_frame(&mut self, frame: Box<Frame>) -> Result<()> {
        let size_after = frame.size_after();
        self.max_tx_frame_no = self.max_tx_frame_no.max(frame.header().frame_no());
        self.buffer.push(frame);

        if size_after.is_some() || self.capacity == self.buffer.len() {
            self.flush(size_after).await?;
        }

        Ok(())
    }

    async fn flush(&mut self, size_after: Option<u32>) -> Result<()> {
        let buffer = std::mem::take(&mut self.buffer);
        let current = self.wal.current.load();
        let commit_data = size_after.map(|size| (size, self.max_tx_frame_no));
        if commit_data.is_some() {
            self.max_tx_frame_no = 0;
        }
        let buffer = current
            .insert_frames(buffer, commit_data, &mut self.tx)
            .await?;
        self.buffer = buffer;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{path::Path, sync::Arc};

    use libsql_sys::name::NamespaceName;
    use libsql_sys::rusqlite::OpenFlags;
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    use crate::{
        registry::WalRegistry, replication::replicator::Replicator, wal::LibsqlWalManager,
    };

    use super::*;

    #[tokio::test]
    async fn inject_basic() {
        // setup primary
        let primary_tmp = tempdir().unwrap();
        let resolver = |path: &Path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            NamespaceName::from_string(name.to_string())
        };

        let primary_registry =
            Arc::new(WalRegistry::new(primary_tmp.path().join("test/wals"), resolver, ()).unwrap());
        let primary_wal_manager = LibsqlWalManager::new(primary_registry.clone());

        let db_path = primary_tmp.path().join("test/data");
        let primary_conn = libsql_sys::Connection::open(
            db_path.clone(),
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            primary_wal_manager.clone(),
            100000,
            None,
        )
        .unwrap();

        let primary_shared = primary_registry.open(&db_path).unwrap();

        let mut replicator = Replicator::new(primary_shared.clone(), 1);
        let stream = replicator.frame_stream();

        tokio::pin!(stream);

        // setup replica
        let replica_tmp = tempdir().unwrap();
        let resolver = |path: &Path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            NamespaceName::from_string(name.to_string())
        };

        let replica_registry =
            Arc::new(WalRegistry::new(replica_tmp.path().join("test/wals"), resolver, ()).unwrap());
        let replica_wal_manager = LibsqlWalManager::new(replica_registry.clone());

        let db_path = replica_tmp.path().join("test/data");
        let replica_conn = libsql_sys::Connection::open(
            db_path.clone(),
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            replica_wal_manager.clone(),
            100000,
            None,
        )
        .unwrap();

        let replica_shared = replica_registry.open(&db_path).unwrap();

        let mut tx = crate::transaction::Transaction::Read(replica_shared.begin_read(42));
        replica_shared.upgrade(&mut tx).unwrap();
        let guard = tx.as_write_mut().unwrap().lock();
        let mut injector = Injector::new(replica_shared.clone(), guard, 10).unwrap();

        primary_conn.execute("create table test (x)", ()).unwrap();

        for _ in 0..2 {
            let frame = stream.next().await.unwrap().unwrap();
            injector.insert_frame(Box::new(frame)).await.unwrap();
        }

        replica_conn
            .query_row("select count(*) from test", (), |r| {
                assert_eq!(r.get_unwrap::<_, usize>(0), 0);
                Ok(())
            })
            .unwrap();

        primary_conn
            .execute("insert into test values (123)", ())
            .unwrap();
        primary_conn
            .execute("insert into test values (123)", ())
            .unwrap();
        primary_conn
            .execute("insert into test values (123)", ())
            .unwrap();

        let frame = stream.next().await.unwrap().unwrap();
        injector.insert_frame(Box::new(frame)).await.unwrap();

        replica_conn
            .query_row("select count(*) from test", (), |r| {
                assert_eq!(r.get_unwrap::<_, usize>(0), 3);
                Ok(())
            })
            .unwrap();
    }
}
