//! The injector is the module in charge of injecting frames into a replica database.

use std::sync::Arc;

use tokio_stream::{Stream, StreamExt};

use crate::error::Result;
use crate::io::Io;
use crate::segment::Frame;
use crate::shared_wal::SharedWal;
use crate::transaction::{Transaction, TxGuardOwned};

/// The injector takes frames and injects them in the wal.
pub struct Injector<IO: Io> {
    // The wal to which we are injecting
    wal: Arc<SharedWal<IO>>,
    buffer: Vec<Box<Frame>>,
    /// capacity of the frame buffer
    capacity: usize,
    tx: TxGuardOwned<IO::File>,
    max_tx_frame_no: u64,
}

impl<IO: Io> Injector<IO> {
    pub fn new(
        wal: Arc<SharedWal<IO>>,
        buffer_capacity: usize,
    ) -> Result<Self> {
        let mut tx = Transaction::Read(wal.begin_read(u64::MAX));
        wal.upgrade(&mut tx)?;
        let tx = tx.into_write().unwrap_or_else(|_| unreachable!()).into_lock_owned();
        Ok(Self {
            wal,
            buffer: Vec::with_capacity(buffer_capacity),
            capacity: buffer_capacity,
            tx,
            max_tx_frame_no: 0,
        })
    }

    pub async fn inject_stream(&mut self, stream: impl Stream<Item = Result<Box<Frame>>>) -> Result<()> {
        tokio::pin!(stream);
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    self.insert_frame(frame).await?;
                },
                Some(Err(e)) => return Err(e),
                None => return Ok(()),
            }
        }
    }

    pub async fn insert_frame(&mut self, frame: Box<Frame>) -> Result<Option<u64>> {
        let size_after = frame.size_after();
        self.max_tx_frame_no = self.max_tx_frame_no.max(frame.header().frame_no());
        self.buffer.push(frame);

        if size_after.is_some() || self.capacity == self.buffer.len() {
            self.flush(size_after).await?;
        }

        Ok(size_after.map(|_| self.max_tx_frame_no))
    }

    pub async fn flush(&mut self, size_after: Option<u32>) -> Result<()> {
        let buffer = std::mem::take(&mut self.buffer);
        let current = self.wal.current.load();
        let commit_data = size_after.map(|size| (size, self.max_tx_frame_no));
        if commit_data.is_some() {
            self.max_tx_frame_no = 0;
        }
        let buffer = current
            .inject_frames(buffer, commit_data, &mut self.tx)
            .await?;
        self.buffer = buffer;
        self.buffer.clear();

        Ok(())
    }

    pub fn rollback(&mut self) {
        self.buffer.clear();
        self.tx.reset(0);
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

        let replicator = Replicator::new(primary_shared.clone(), 1);
        let stream = replicator.into_frame_stream();

        tokio::pin!(stream);

        // setup replica
        let replica_env = TestEnv::new();
        let replica_conn = replica_env.open_conn("test");
        let replica_shared = replica_env.shared("test");

        let mut injector = Injector::new(replica_shared.clone(), 10).unwrap();

        primary_conn.execute("create table test (x)", ()).unwrap();

        primary_shared.last_committed_frame_no();
        for _ in 0..2 {
            let frame = stream.next().await.unwrap().unwrap();
            injector.insert_frame(frame).await.unwrap();
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
        injector.insert_frame(frame).await.unwrap();

        replica_conn
            .query_row("select count(*) from test", (), |r| {
                assert_eq!(r.get_unwrap::<_, usize>(0), 3);
                Ok(())
            })
            .unwrap();
    }
}
