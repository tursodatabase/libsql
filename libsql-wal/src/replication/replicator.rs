use std::sync::Arc;

use tokio::sync::watch;
use tokio_stream::{Stream, StreamExt};

use crate::error::Result;
use crate::io::Io;
use crate::segment::Frame;
use crate::shared_wal::SharedWal;

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

    pub fn frame_stream(&mut self) -> impl Stream<Item = Result<Frame>> + '_ {
        async_stream::try_stream! {
            loop {
                let _most_recent_frame_no = *self
                    .new_frame_notifier
                    .wait_for(|fno| *fno >= self.next_frame_no)
                    .await
                    .expect("channel cannot be closed because we hold a ref to the sending end");

                let current = self.shared.current.load();
                let current_start = current.with_header(|h| h.start_frame_no.get());

                // we can read from the current segment.
                // in the current segment, frames are ordered by frame no, so we can start reading from
                // the end until we hit the current frame_no
                if self.next_frame_no >= current_start {
                    let stream = current.frame_stream_from(self.next_frame_no);
                    let mut new_current_frame_no = 0;
                    tokio::pin!(stream);
                    loop {
                        match stream.try_next().await? {
                            Some(frame) => {
                                new_current_frame_no = new_current_frame_no.max(frame.header().frame_no());
                                yield frame;
                            }
                            None => break
                        }
                    }

                    self.next_frame_no = new_current_frame_no + 1;
                } else {
                    todo!("handle frame not in current log");
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;
    use std::time::Duration;

    use insta::assert_debug_snapshot;
    use libsql_sys::rusqlite::OpenFlags;
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    use crate::registry::WalRegistry;
    use crate::wal::LibsqlWalManager;
    use libsql_sys::name::NamespaceName;

    use super::*;

    #[tokio::test]
    async fn stream_from_current_log() {
        let tmp = tempdir().unwrap();
        let resolver = |path: &Path| {
            let name = path.file_name().unwrap().to_str().unwrap();
            NamespaceName::from_string(name.to_string())
        };

        let registry =
            Arc::new(WalRegistry::new(tmp.path().join("test/wals"), resolver, ()).unwrap());
        let wal_manager = LibsqlWalManager::new(registry.clone());

        let db_path = tmp.path().join("test/data");
        let conn = libsql_sys::Connection::open(
            db_path.clone(),
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            wal_manager.clone(),
            100000,
            None,
        )
        .unwrap();

        let shared = registry.open(&db_path).unwrap();

        let mut replicator = Replicator::new(shared.clone(), 1);
        let stream = replicator.frame_stream();
        tokio::pin!(stream);

        conn.execute("create table test (x)", ()).unwrap();

        let frame = stream.try_next().await.unwrap().unwrap();
        assert_debug_snapshot!(frame.header());

        let frame = stream.try_next().await.unwrap().unwrap();
        assert_debug_snapshot!(frame.header());

        // no more frames for now...
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.try_next())
                .await
                .is_err()
        );

        conn.execute("insert into test values (123)", ()).unwrap();

        let frame = stream.try_next().await.unwrap().unwrap();
        assert_eq!(frame.header().frame_no(), 3);
        assert_eq!(frame.header().size_after(), 2);

        // no more frames for now...
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.try_next())
                .await
                .is_err()
        );

        let mut replicator = Replicator::new(shared, 1);
        let stream = replicator.frame_stream();

        tokio::pin!(stream);

        let frame = stream.try_next().await.unwrap().unwrap();
        assert_debug_snapshot!(frame.header());

        let frame = stream.try_next().await.unwrap().unwrap();
        assert_debug_snapshot!(frame.header());

        // no more frames for now...
        assert!(
            tokio::time::timeout(Duration::from_millis(100), stream.try_next())
                .await
                .is_err()
        );
    }
}
