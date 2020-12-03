///! Busy handler (when the database is locked)
use std::convert::TryInto;
use std::mem;
use std::os::raw::{c_int, c_void};
use std::panic::catch_unwind;
use std::ptr;
use std::time::Duration;

use crate::ffi;
use crate::{Connection, InnerConnection, Result};

impl Connection {
    /// Set a busy handler that sleeps for a specified amount of time when a
    /// table is locked. The handler will sleep multiple times until at
    /// least "ms" milliseconds of sleeping have accumulated.
    ///
    /// Calling this routine with an argument equal to zero turns off all busy
    /// handlers.
    ///
    /// There can only be a single busy handler for a particular database
    /// connection at any given moment. If another busy handler was defined
    /// (using [`busy_handler`](Connection::busy_handler)) prior to calling this routine, that other
    /// busy handler is cleared.
    ///
    /// Newly created connections currently have a default busy timeout of 5000ms, but this may be
    /// subject to change.
    pub fn busy_timeout(&self, timeout: Duration) -> Result<()> {
        let ms: i32 = timeout
            .as_secs()
            .checked_mul(1000)
            .and_then(|t| t.checked_add(timeout.subsec_millis().into()))
            .and_then(|t| t.try_into().ok())
            .expect("too big");
        self.db.borrow_mut().busy_timeout(ms)
    }

    /// Register a callback to handle `SQLITE_BUSY` errors.
    ///
    /// If the busy callback is `None`, then `SQLITE_BUSY` is returned
    /// immediately upon encountering the lock. The argument to the busy
    /// handler callback is the number of times that the
    /// busy handler has been invoked previously for the
    /// same locking event. If the busy callback returns `false`, then no
    /// additional attempts are made to access the
    /// database and `SQLITE_BUSY` is returned to the
    /// application. If the callback returns `true`, then another attempt
    /// is made to access the database and the cycle repeats.
    ///
    /// There can only be a single busy handler defined for each database
    /// connection. Setting a new busy handler clears any previously set
    /// handler. Note that calling [`busy_timeout()`](Connection::busy_timeout) or evaluating `PRAGMA
    /// busy_timeout=N` will change the busy handler and thus
    /// clear any previously set busy handler.
    ///
    /// Newly created connections default to a [`busy_timeout()`](Connection::busy_timeout) handler
    /// with a timeout of 5000ms, although this is subject to change.
    pub fn busy_handler(&self, callback: Option<fn(i32) -> bool>) -> Result<()> {
        unsafe extern "C" fn busy_handler_callback(p_arg: *mut c_void, count: c_int) -> c_int {
            let handler_fn: fn(i32) -> bool = mem::transmute(p_arg);
            if let Ok(true) = catch_unwind(|| handler_fn(count)) {
                1
            } else {
                0
            }
        }
        let mut c = self.db.borrow_mut();
        let r = match callback {
            Some(f) => unsafe {
                ffi::sqlite3_busy_handler(c.db(), Some(busy_handler_callback), f as *mut c_void)
            },
            None => unsafe { ffi::sqlite3_busy_handler(c.db(), None, ptr::null_mut()) },
        };
        c.decode_result(r)
    }
}

impl InnerConnection {
    #[inline]
    fn busy_timeout(&mut self, timeout: c_int) -> Result<()> {
        let r = unsafe { ffi::sqlite3_busy_timeout(self.db, timeout) };
        self.decode_result(r)
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time::Duration;

    use crate::{Connection, Error, ErrorCode, Result, TransactionBehavior};

    #[test]
    fn test_default_busy() -> Result<()> {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db3");

        let mut db1 = Connection::open(&path)?;
        let tx1 = db1.transaction_with_behavior(TransactionBehavior::Exclusive)?;
        let db2 = Connection::open(&path)?;
        let r: Result<()> = db2.query_row("PRAGMA schema_version", [], |_| unreachable!());
        match r.unwrap_err() {
            Error::SqliteFailure(err, _) => {
                assert_eq!(err.code, ErrorCode::DatabaseBusy);
            }
            err => panic!("Unexpected error {}", err),
        }
        tx1.rollback()
    }

    #[test]
    #[ignore] // FIXME: unstable
    fn test_busy_timeout() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db3");

        let db2 = Connection::open(&path).unwrap();
        db2.busy_timeout(Duration::from_secs(1)).unwrap();

        let (rx, tx) = sync_channel(0);
        let child = thread::spawn(move || {
            let mut db1 = Connection::open(&path).unwrap();
            let tx1 = db1
                .transaction_with_behavior(TransactionBehavior::Exclusive)
                .unwrap();
            rx.send(1).unwrap();
            thread::sleep(Duration::from_millis(100));
            tx1.rollback().unwrap();
        });

        assert_eq!(tx.recv().unwrap(), 1);
        let _ = db2
            .query_row("PRAGMA schema_version", [], |row| row.get::<_, i32>(0))
            .expect("unexpected error");

        child.join().unwrap();
    }

    #[test]
    #[ignore] // FIXME: unstable
    fn test_busy_handler() {
        static CALLED: AtomicBool = AtomicBool::new(false);
        fn busy_handler(_: i32) -> bool {
            CALLED.store(true, Ordering::Relaxed);
            thread::sleep(Duration::from_millis(100));
            true
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.db3");

        let db2 = Connection::open(&path).unwrap();
        db2.busy_handler(Some(busy_handler)).unwrap();

        let (rx, tx) = sync_channel(0);
        let child = thread::spawn(move || {
            let mut db1 = Connection::open(&path).unwrap();
            let tx1 = db1
                .transaction_with_behavior(TransactionBehavior::Exclusive)
                .unwrap();
            rx.send(1).unwrap();
            thread::sleep(Duration::from_millis(100));
            tx1.rollback().unwrap();
        });

        assert_eq!(tx.recv().unwrap(), 1);
        let _ = db2
            .query_row("PRAGMA schema_version", [], |row| row.get::<_, i32>(0))
            .expect("unexpected error");
        assert_eq!(CALLED.load(Ordering::Relaxed), true);

        child.join().unwrap();
    }
}
