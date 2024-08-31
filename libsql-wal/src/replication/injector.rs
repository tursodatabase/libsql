//! The injector is the module in charge of injecting frames into a replica database.

use std::sync::Arc;

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
    tx: Option<TxGuardOwned<IO::File>>,
    max_tx_frame_no: u64,
    previous_durable_frame_no: u64,
}

impl<IO: Io> Injector<IO> {
    pub fn new(wal: Arc<SharedWal<IO>>, buffer_capacity: usize) -> Result<Self> {
        Ok(Self {
            wal,
            buffer: Vec::with_capacity(buffer_capacity),
            capacity: buffer_capacity,
            tx: None,
            max_tx_frame_no: 0,
            previous_durable_frame_no: 0,
        })
    }

    pub fn set_durable(&mut self, durable_frame_no: u64) {
        let mut old = self.wal.durable_frame_no.lock();
        if *old <= durable_frame_no {
            self.previous_durable_frame_no = *old;
            *old = durable_frame_no;
        } else {
            todo!("primary reported older frame_no than current");
        }
    }

    pub fn current_durable(&self) -> u64 {
        *self.wal.durable_frame_no.lock()
    }

    pub fn maybe_begin_txn(&mut self) -> Result<()> {
        if self.tx.is_none() {
            let mut tx = Transaction::Read(self.wal.begin_read(u64::MAX));
            self.wal.upgrade(&mut tx)?;
            let tx = tx
                .into_write()
                .unwrap_or_else(|_| unreachable!())
                .into_lock_owned();
            assert!(self.tx.replace(tx).is_none());
        }

        Ok(())
    }

    pub async fn insert_frame(&mut self, frame: Box<Frame>) -> Result<Option<u64>> {
        self.maybe_begin_txn()?;
        let size_after = frame.size_after();
        self.max_tx_frame_no = self.max_tx_frame_no.max(frame.header().frame_no());
        self.buffer.push(frame);

        if size_after.is_some() || self.capacity == self.buffer.len() {
            self.flush(size_after).await?;
        }

        Ok(size_after.map(|_| self.max_tx_frame_no))
    }

    pub async fn flush(&mut self, size_after: Option<u32>) -> Result<()> {
        if !self.buffer.is_empty() && self.tx.is_some() {
            let last_committed_frame_no = self.max_tx_frame_no;
            {
                let tx = self.tx.as_mut().expect("we just checked that tx was there");
                let buffer = std::mem::take(&mut self.buffer);
                let current = self.wal.current.load();
                let commit_data = size_after.map(|size| (size, self.max_tx_frame_no));
                if commit_data.is_some() {
                    self.max_tx_frame_no = 0;
                }
                let buffer = current.inject_frames(buffer, commit_data, tx).await?;
                self.buffer = buffer;
                self.buffer.clear();
            }

            if size_after.is_some() {
                let mut tx = self.tx.take().unwrap();
                self.wal
                    .new_frame_notifier
                    .send_replace(last_committed_frame_no);
                // the strategy to swap the current log is to do it on change of durable boundary,
                // when we have caught up with the current durable frame_no
                if self.current_durable() != self.previous_durable_frame_no
                    && self.current_durable() >= self.max_tx_frame_no
                {
                    let wal = self.wal.clone();
                    // FIXME: tokio dependency here is annoying, we need an async version of swap_current.
                    tokio::task::spawn_blocking(move || {
                        tx.commit();
                        wal.swap_current(&tx)
                    })
                    .await
                    .unwrap()?
                }
            }
        }

        Ok(())
    }

    pub fn rollback(&mut self) {
        self.buffer.clear();
        if let Some(tx) = self.tx.as_mut() {
            tx.reset(0);
        }
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
