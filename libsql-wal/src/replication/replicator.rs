use std::sync::Arc;

use roaring::RoaringBitmap;
use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};

use crate::io::Io;
use crate::replication::Error;
use crate::segment::Frame;
use crate::shared_wal::SharedWal;

use super::Result;

pub struct Replicator<IO: Io> {
    shared: Arc<SharedWal<IO>>,
    new_frame_notifier: watch::Receiver<u64>,
    next_frame_no: u64,
}

impl<IO: Io> Replicator<IO> {
    pub fn new(shared: Arc<SharedWal<IO>>, next_frame_no: u64) -> Self {
        let new_frame_notifier = shared.new_frame_notifier.subscribe();
        Self {
            shared,
            new_frame_notifier,
            next_frame_no,
        }
    }

    /// Stream frames from this replicator. The replicator will wait for new frames to become
    /// available, and never return.
    ///
    /// The replicator keeps track of how much progress has been made by the replica, and will
    /// attempt to find the next frames to send with following strategy:
    /// - First, replicate as much as possible from the current log
    /// - The, if we still haven't caught up with `self.start_frame_no`, we select the next frames
    /// to replicate from tail of current.
    /// - Finally, if we still haven't reached `self.start_frame_no`, read from durable storage
    /// (todo: maybe the replica should read from durable storage directly?)
    ///
    /// In a single replication step, the replicator guarantees that a minimal set of frames is
    /// sent to the replica.
    #[tracing::instrument(skip(self))]
    pub fn into_frame_stream(mut self) -> impl Stream<Item = Result<Box<Frame>>> + Send {
        async_stream::try_stream! {
            loop {
                // First we decide up to what frame_no we want to replicate in this step. If we are
                // already up to date, wait for something to happen
                tracing::debug!(next_frame_no = self.next_frame_no);
                let most_recent_frame_no = *self
                    .new_frame_notifier
                    .wait_for(|fno| *fno >= self.next_frame_no)
                    .await
                    .expect("channel cannot be closed because we hold a ref to the sending end");

                tracing::debug!(most_recent_frame_no, "new frame_no available");

                let mut commit_frame_no = 0;
                // we have stuff to replicate
                if most_recent_frame_no >= self.next_frame_no {
                    // first replicate the most recent version of each page from the current
                    // segment. We also return how far we have replicated from the current log
                    let current = self.shared.current.load();
                    let mut seen = RoaringBitmap::new();
                    let (stream, replicated_until, size_after) = current.frame_stream_from(self.next_frame_no, &mut seen);
                    let should_replicate_from_tail = replicated_until != self.next_frame_no;

                    {
                        tokio::pin!(stream);

                        let mut stream = stream.peekable();

                        tracing::debug!(replicated_until, "replicating from current log");
                        loop {
                            let Some(frame) = stream.next().await else { break };
                            let mut frame = frame.map_err(|e| Error::CurrentSegment(e.into()))?;
                            commit_frame_no = frame.header().frame_no().max(commit_frame_no);
                            if stream.peek().await.is_none() && !should_replicate_from_tail {
                                frame.header_mut().set_size_after(size_after);
                                self.next_frame_no = commit_frame_no + 1;
                            }

                            yield frame
                        }
                    }

                    // Replicating from the current segment wasn't enough to bring us up to date,
                    // wee need to take frames from the sealed segments.
                    if should_replicate_from_tail {
                        let replicated_until = {
                            let (stream, replicated_until) = current
                                .tail()
                                .stream_pages_from(replicated_until, self.next_frame_no, &mut seen).await;
                            tokio::pin!(stream);

                        tracing::debug!(replicated_until, "replicating from tail");
                            let mut stream = stream.peekable();

                            let should_replicate_from_storage = replicated_until != self.next_frame_no;

                            loop {
                                let Some(frame) = stream.next().await else { break };
                                let mut frame = frame.map_err(|e| Error::SealedSegment(e.into()))?;
                                commit_frame_no = frame.header().frame_no().max(commit_frame_no);
                                if stream.peek().await.is_none() && !should_replicate_from_storage {
                                    frame.header_mut().set_size_after(size_after);
                                    self.next_frame_no = commit_frame_no + 1;
                                }

                                yield frame
                            }

                            should_replicate_from_storage.then_some(replicated_until)
                        };

                        // Replicating from sealed segments was not enough, so we replicate from
                        // durable storage
                        if let Some(replicated_until) = replicated_until {
                            tracing::debug!("replicating from durable storage");
                            let stream = self
                                .shared
                                .stored_segments
                                .stream(&mut seen, replicated_until, self.next_frame_no)
                                .peekable();

                            tokio::pin!(stream);

                            loop {
                                let Some(frame) = stream.next().await else { break };
                                let mut frame = frame?;
                                commit_frame_no = frame.header().frame_no().max(commit_frame_no);
                                if stream.peek().await.is_none() {
                                    frame.header_mut().set_size_after(size_after);
                                    self.next_frame_no = commit_frame_no + 1;
                                }

                                yield frame
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tempfile::NamedTempFile;
    use tokio_stream::StreamExt;

    use crate::io::FileExt;
    use crate::test::{seal_current_segment, TestEnv};

    use super::*;

    #[tokio::test]
    async fn stream_from_current_log() {
        let env = TestEnv::new();
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();

        for _ in 0..50 {
            conn.execute("insert into test values (randomblob(128))", ())
                .unwrap();
        }

        let replicator = Replicator::new(shared.clone(), 1);

        let tmp = NamedTempFile::new().unwrap();
        let stream = replicator.into_frame_stream();
        tokio::pin!(stream);
        let mut last_frame_no = 0;
        let mut size_after;
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            // the last frame should commit
            size_after = frame.header().size_after();
            last_frame_no = last_frame_no.max(frame.header().frame_no());
            let offset = (frame.header().page_no() - 1) * 4096;
            tmp.as_file()
                .write_all_at(frame.data(), offset as _)
                .unwrap();
            if size_after != 0 {
                break;
            }
        }

        assert_eq!(size_after, 4);
        assert_eq!(last_frame_no, 55);

        {
            let conn = libsql_sys::rusqlite::Connection::open(tmp.path()).unwrap();
            conn.query_row("select count(0) from test", (), |row| {
                let count = row.get_unwrap::<_, usize>(0);
                assert_eq!(count, 50);
                Ok(())
            })
            .unwrap();
        }

        seal_current_segment(&shared);

        for _ in 0..50 {
            conn.execute("insert into test values (randomblob(128))", ())
                .unwrap();
        }

        let mut size_after;
        loop {
            let frame = stream.next().await.unwrap().unwrap();
            assert!(frame.header().frame_no() > last_frame_no);
            size_after = frame.header().size_after();
            // the last frame should commit
            let offset = (frame.header().page_no() - 1) * 4096;
            tmp.as_file()
                .write_all_at(frame.data(), offset as _)
                .unwrap();
            if size_after != 0 {
                break;
            }
        }

        assert_eq!(size_after, 6);

        {
            let conn = libsql_sys::rusqlite::Connection::open(tmp.path()).unwrap();
            conn.query_row("select count(0) from test", (), |row| {
                let count = row.get_unwrap::<_, usize>(0);
                assert_eq!(count, 100);
                Ok(())
            })
            .unwrap();
        }

        // replicate everything from scratch again
        {
            let tmp = NamedTempFile::new().unwrap();
            let replicator = Replicator::new(shared.clone(), 1);
            let stream = replicator.into_frame_stream();

            tokio::pin!(stream);

            loop {
                let frame = stream.next().await.unwrap().unwrap();
                // the last frame should commit
                let offset = (frame.header().page_no() - 1) * 4096;
                tmp.as_file()
                    .write_all_at(frame.data(), offset as _)
                    .unwrap();
                if frame.header().size_after() != 0 {
                    break;
                }
            }

            let conn = libsql_sys::rusqlite::Connection::open(tmp.path()).unwrap();
            conn.query_row("select count(0) from test", (), |row| {
                let count = row.get_unwrap::<_, usize>(0);
                assert_eq!(count, 100);
                Ok(())
            })
            .unwrap();
        }
    }

    #[tokio::test]
    async fn stream_from_storage() {
        let env = TestEnv::new_store(true);
        let conn = env.open_conn("test");
        let shared = env.shared("test");

        conn.execute("create table test (x)", ()).unwrap();

        conn.execute("insert into test values (randomblob(128))", ())
            .unwrap();

        tokio::task::spawn_blocking({
            let shared = shared.clone();
            move || seal_current_segment(&shared)
        })
        .await
        .unwrap();

        conn.execute("create table test2 (x)", ()).unwrap();
        conn.execute("insert into test2 values (randomblob(128))", ())
            .unwrap();

        tokio::task::spawn_blocking({
            let shared = shared.clone();
            move || seal_current_segment(&shared)
        })
        .await
        .unwrap();

        while !shared.current.load().tail().is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let db_content = std::fs::read(&env.db_path("test").join("data")).unwrap();

        let replicator = Replicator::new(shared, 1);
        let stream = replicator.into_frame_stream().take(3);

        tokio::pin!(stream);

        let tmp = NamedTempFile::new().unwrap();
        let mut replica_content = vec![0u8; db_content.len()];
        while let Some(f) = stream.next().await {
            let frame = f.unwrap();
            let offset = (frame.header().page_no() as usize - 1) * 4096;
            tmp.as_file()
                .write_all_at(frame.data(), offset as u64)
                .unwrap();
            replica_content[offset..offset + 4096].copy_from_slice(frame.data());
        }

        assert_eq!(db_payload(&replica_content), db_payload(&db_content));
    }

    fn db_payload(db: &[u8]) -> &[u8] {
        let size = (db.len() / 4096) * 4096;
        &db[..size]
    }
}
