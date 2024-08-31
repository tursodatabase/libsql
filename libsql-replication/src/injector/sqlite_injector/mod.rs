use std::path::Path;
use std::sync::Arc;
use std::{collections::VecDeque, path::PathBuf};

use parking_lot::Mutex;
use rusqlite::OpenFlags;
use tokio::task::spawn_blocking;

use crate::frame::{Frame, FrameNo};
use crate::rpc::replication::Frame as RpcFrame;

use self::injector_wal::{
    InjectorWal, InjectorWalManager, LIBSQL_INJECT_FATAL, LIBSQL_INJECT_OK, LIBSQL_INJECT_OK_TXN,
};

use super::error::Result;
use super::{Error, Injector};

mod headers;
mod injector_wal;

pub type FrameBuffer = Arc<Mutex<VecDeque<Frame>>>;

pub struct SqliteInjector {
    pub(in super::super) inner: Arc<Mutex<SqliteInjectorInner>>,
}

impl Injector for SqliteInjector {
    async fn inject_frame(&mut self, frame: RpcFrame) -> Result<Option<FrameNo>> {
        let inner = self.inner.clone();
        let frame =
            Frame::try_from(&frame.data[..]).map_err(|e| Error::FatalInjectError(e.into()))?;
        spawn_blocking(move || inner.lock().inject_frame(frame))
            .await
            .unwrap()
    }

    async fn rollback(&mut self) {
        let inner = self.inner.clone();
        spawn_blocking(move || inner.lock().rollback())
            .await
            .unwrap();
    }

    async fn flush(&mut self) -> Result<Option<FrameNo>> {
        let inner = self.inner.clone();
        spawn_blocking(move || inner.lock().flush()).await.unwrap()
    }

    #[inline]
    fn durable_frame_no(&mut self, _frame_no: u64) {}
}

impl SqliteInjector {
    pub async fn new(
        path: PathBuf,
        capacity: usize,
        auto_checkpoint: u32,
        encryption_config: Option<libsql_sys::EncryptionConfig>,
    ) -> super::Result<Self> {
        let inner = spawn_blocking(move || {
            SqliteInjectorInner::new(path, capacity, auto_checkpoint, encryption_config)
        })
        .await
        .unwrap()?;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

pub(in super::super) struct SqliteInjectorInner {
    /// The injector is in a transaction state
    is_txn: bool,
    /// Buffer for holding current transaction frames
    buffer: FrameBuffer,
    /// Maximum capacity of the frame buffer
    capacity: usize,
    /// Injector connection
    // connection must be dropped before the hook context
    connection: Arc<Mutex<libsql_sys::Connection<InjectorWal>>>,
    biggest_uncommitted_seen: FrameNo,

    // Connection config items used to recreate the injection connection
    path: PathBuf,
    encryption_config: Option<libsql_sys::EncryptionConfig>,
    auto_checkpoint: u32,
}

/// Methods from this trait are called before and after performing a frame injection.
/// This trait trait is used to record the last committed frame_no to the log.
/// The implementer can persist the pre and post commit frame no, and compare them in the event of
/// a crash; if the pre and post commit frame_no don't match, then the log may be corrupted.
impl SqliteInjectorInner {
    fn new(
        path: impl AsRef<Path>,
        capacity: usize,
        auto_checkpoint: u32,
        encryption_config: Option<libsql_sys::EncryptionConfig>,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();

        let buffer = FrameBuffer::default();
        let wal_manager = InjectorWalManager::new(buffer.clone());
        let connection = libsql_sys::Connection::open(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            wal_manager,
            auto_checkpoint,
            encryption_config.clone(),
        )?;

        Ok(Self {
            is_txn: false,
            buffer,
            capacity,
            connection: Arc::new(Mutex::new(connection)),
            biggest_uncommitted_seen: 0,

            path,
            encryption_config,
            auto_checkpoint,
        })
    }

    /// Inject a frame into the log. If this was a commit frame, returns Ok(Some(FrameNo)).
    pub fn inject_frame(&mut self, frame: Frame) -> Result<Option<FrameNo>, Error> {
        let frame_close_txn = frame.header().size_after.get() != 0;
        self.buffer.lock().push_back(frame);
        if frame_close_txn || self.buffer.lock().len() >= self.capacity {
            return self.flush();
        }

        Ok(None)
    }

    pub fn rollback(&mut self) {
        self.clear_buffer();
        let conn = self.connection.lock();
        let mut rollback = conn.prepare_cached("ROLLBACK").unwrap();
        let _ = rollback.execute(());
        self.is_txn = false;
    }

    /// Flush the buffer to libsql WAL.
    /// Trigger a dummy write, and flush the cache to trigger a call to xFrame. The buffer's frame
    /// are then injected into the wal.
    pub fn flush(&mut self) -> Result<Option<FrameNo>, Error> {
        match self.try_flush() {
            Err(e) => {
                // something went wrong, rollback the connection to make sure we can retry in a
                // clean state
                self.biggest_uncommitted_seen = 0;
                self.rollback();
                Err(e)
            }
            Ok(ret) => Ok(ret),
        }
    }

    fn try_flush(&mut self) -> Result<Option<FrameNo>, Error> {
        if !self.is_txn {
            self.begin_txn()?;
        }

        let lock = self.buffer.lock();
        // the frames in the buffer are either monotonically increasing (log) or decreasing
        // (snapshot). Either way, we want to find the biggest frameno we're about to commit, and
        // that is either the front or the back of the buffer
        let last_frame_no = match lock.back().zip(lock.front()) {
            Some((b, f)) => f.header().frame_no.get().max(b.header().frame_no.get()),
            None => {
                tracing::trace!("nothing to inject");
                return Ok(None);
            }
        };

        self.biggest_uncommitted_seen = self.biggest_uncommitted_seen.max(last_frame_no);

        drop(lock);

        let connection = self.connection.lock();
        // use prepare cached to avoid parsing the same statement over and over again.
        let mut stmt =
            connection.prepare_cached("INSERT INTO libsql_temp_injection VALUES (42)")?;

        // We execute the statement, and then force a call to xframe if necesacary. If the execute
        // succeeds, then xframe wasn't called, in this case, we call cache_flush, and then process
        // the error.
        // It is unexpected that execute flushes, but it is possible, so we handle that case.
        match stmt.execute(()).and_then(|_| connection.cache_flush()) {
            Ok(_) => panic!("replication hook was not called"),
            Err(e) => {
                if let Some(err) = e.sqlite_error() {
                    if err.extended_code == LIBSQL_INJECT_OK {
                        // refresh schema
                        connection.pragma_update(None, "writable_schema", "reset")?;
                        let mut rollback = connection.prepare_cached("ROLLBACK")?;
                        let _ = rollback.execute(());
                        self.is_txn = false;
                        assert!(self.buffer.lock().is_empty());
                        let commit_frame_no = self.biggest_uncommitted_seen;
                        self.biggest_uncommitted_seen = 0;
                        return Ok(Some(commit_frame_no));
                    } else if err.extended_code == LIBSQL_INJECT_OK_TXN {
                        self.is_txn = true;
                        assert!(self.buffer.lock().is_empty());
                        return Ok(None);
                    } else if err.extended_code == LIBSQL_INJECT_FATAL {
                        return Err(Error::FatalInjectError(e.into()));
                    }
                }

                Err(Error::FatalInjectError(e.into()))
            }
        }
    }

    fn begin_txn(&mut self) -> Result<(), Error> {
        let mut conn = self.connection.lock();

        {
            let wal_manager = InjectorWalManager::new(self.buffer.clone());
            let new_conn = libsql_sys::Connection::open(
                &self.path,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                wal_manager,
                self.auto_checkpoint,
                self.encryption_config.clone(),
            )?;

            let _ = std::mem::replace(&mut *conn, new_conn);
        }

        conn.pragma_update(None, "writable_schema", "true")?;

        let mut stmt = conn.prepare_cached("BEGIN IMMEDIATE")?;
        stmt.execute(())?;
        // we create a dummy table. This table MUST not be persisted, otherwise the replica schema
        // would differ with the primary's.
        let mut stmt =
            conn.prepare_cached("CREATE TABLE IF NOT EXISTS libsql_temp_injection (x)")?;
        stmt.execute(())?;

        Ok(())
    }

    pub fn clear_buffer(&mut self) {
        self.buffer.lock().clear()
    }

    #[cfg(test)]
    pub fn is_txn(&self) -> bool {
        self.is_txn
    }
}

#[cfg(test)]
mod test {
    use crate::frame::FrameBorrowed;
    use std::mem::size_of;

    use super::*;
    /// this this is generated by creating a table test, inserting 5 rows into it, and then
    /// truncating the wal file of it's header.
    const WAL: &[u8] = include_bytes!("../../../assets/test/test_wallog");

    fn wal_log() -> impl Iterator<Item = Frame> {
        WAL.chunks(size_of::<FrameBorrowed>())
            .map(|b| Frame::try_from(b).unwrap())
    }

    #[test]
    fn test_simple_inject_frames() {
        let temp = tempfile::tempdir().unwrap();

        let mut injector =
            SqliteInjectorInner::new(temp.path().join("data"), 10, 10000, None).unwrap();
        let log = wal_log();
        for frame in log {
            injector.inject_frame(frame).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_frames_split_txn() {
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector =
            SqliteInjectorInner::new(temp.path().join("data"), 1, 10000, None).unwrap();
        let log = wal_log();
        for frame in log {
            injector.inject_frame(frame).unwrap();
        }

        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();

        conn.query_row("SELECT COUNT(*) FROM test", (), |row| {
            assert_eq!(row.get::<_, usize>(0).unwrap(), 5);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_inject_partial_txn_isolated() {
        let temp = tempfile::tempdir().unwrap();

        // inject one frame at a time
        let mut injector =
            SqliteInjectorInner::new(temp.path().join("data"), 10, 1000, None).unwrap();
        let mut frames = wal_log();

        assert!(injector
            .inject_frame(frames.next().unwrap())
            .unwrap()
            .is_none());
        let conn = rusqlite::Connection::open(temp.path().join("data")).unwrap();
        assert!(conn
            .query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .is_err());

        while injector
            .inject_frame(frames.next().unwrap())
            .unwrap()
            .is_none()
        {}

        // reset schema
        conn.pragma_update(None, "writable_schema", "reset")
            .unwrap();
        conn.query_row("SELECT COUNT(*) FROM test", (), |_| Ok(()))
            .unwrap();
    }
}
