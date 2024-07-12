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
    use tokio_stream::StreamExt;

    use crate::replication::replicator::Replicator;
    use crate::test::TestEnv;

    use super::*;

    #[tokio::test]
    async fn inject_basic() {
        let primary_env = TestEnv::new();
        let primary_conn = primary_env.open_conn("test");
        let primary_shared = primary_env.shared("test");

        let mut replicator = Replicator::new(primary_shared.clone(), 1);
        let stream = replicator.frame_stream();

        tokio::pin!(stream);

        // setup replica
        let replica_env = TestEnv::new();
        let replica_conn = replica_env.open_conn("test");
        let replica_shared = replica_env.shared("test");

        let mut tx = crate::transaction::Transaction::Read(replica_shared.begin_read(42));
        replica_shared.upgrade(&mut tx).unwrap();
        let guard = tx.as_write_mut().unwrap().lock();
        let mut injector = Injector::new(replica_shared.clone(), guard, 10).unwrap();

        primary_conn.execute("create table test (x)", ()).unwrap();

        primary_shared.last_committed_frame_no();
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
