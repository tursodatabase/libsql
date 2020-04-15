//! [Unlock Notification](http://sqlite.org/unlock_notify.html)

use std::os::raw::c_int;
#[cfg(feature = "unlock_notify")]
use std::os::raw::c_void;
#[cfg(feature = "unlock_notify")]
use std::panic::catch_unwind;
#[cfg(feature = "unlock_notify")]
use std::sync::{Condvar, Mutex};

use crate::ffi;

#[cfg(feature = "unlock_notify")]
struct UnlockNotification {
    cond: Condvar,      // Condition variable to wait on
    mutex: Mutex<bool>, // Mutex to protect structure
}

#[cfg(feature = "unlock_notify")]
#[allow(clippy::mutex_atomic)]
impl UnlockNotification {
    fn new() -> UnlockNotification {
        UnlockNotification {
            cond: Condvar::new(),
            mutex: Mutex::new(false),
        }
    }

    fn fired(&self) {
        let mut flag = self.mutex.lock().unwrap();
        *flag = true;
        self.cond.notify_one();
    }

    fn wait(&self) {
        let mut fired = self.mutex.lock().unwrap();
        while !*fired {
            fired = self.cond.wait(fired).unwrap();
        }
    }
}

/// This function is an unlock-notify callback
#[cfg(feature = "unlock_notify")]
unsafe extern "C" fn unlock_notify_cb(ap_arg: *mut *mut c_void, n_arg: c_int) {
    use std::slice::from_raw_parts;
    let args = from_raw_parts(ap_arg as *const &UnlockNotification, n_arg as usize);
    for un in args {
        let _ = catch_unwind(std::panic::AssertUnwindSafe(|| un.fired()));
    }
}

#[cfg(feature = "unlock_notify")]
pub unsafe fn is_locked(db: *mut ffi::sqlite3, rc: c_int) -> bool {
    rc == ffi::SQLITE_LOCKED_SHAREDCACHE
        || (rc & 0xFF) == ffi::SQLITE_LOCKED
            && ffi::sqlite3_extended_errcode(db) == ffi::SQLITE_LOCKED_SHAREDCACHE
}

/// This function assumes that an SQLite API call (either `sqlite3_prepare_v2()`
/// or `sqlite3_step()`) has just returned `SQLITE_LOCKED`. The argument is the
/// associated database connection.
///
/// This function calls `sqlite3_unlock_notify()` to register for an
/// unlock-notify callback, then blocks until that callback is delivered
/// and returns `SQLITE_OK`. The caller should then retry the failed operation.
///
/// Or, if `sqlite3_unlock_notify()` indicates that to block would deadlock
/// the system, then this function returns `SQLITE_LOCKED` immediately. In
/// this case the caller should not retry the operation and should roll
/// back the current transaction (if any).
#[cfg(feature = "unlock_notify")]
pub unsafe fn wait_for_unlock_notify(db: *mut ffi::sqlite3) -> c_int {
    let un = UnlockNotification::new();
    /* Register for an unlock-notify callback. */
    let rc = ffi::sqlite3_unlock_notify(
        db,
        Some(unlock_notify_cb),
        &un as *const UnlockNotification as *mut c_void,
    );
    debug_assert!(
        rc == ffi::SQLITE_LOCKED || rc == ffi::SQLITE_LOCKED_SHAREDCACHE || rc == ffi::SQLITE_OK
    );
    if rc == ffi::SQLITE_OK {
        un.wait();
    }
    rc
}

#[cfg(not(feature = "unlock_notify"))]
pub unsafe fn is_locked(_db: *mut ffi::sqlite3, _rc: c_int) -> bool {
    unreachable!()
}

#[cfg(not(feature = "unlock_notify"))]
pub unsafe fn wait_for_unlock_notify(_db: *mut ffi::sqlite3) -> c_int {
    unreachable!()
}

#[cfg(feature = "unlock_notify")]
#[cfg(test)]
mod test {
    use crate::{Connection, OpenFlags, Result, Transaction, TransactionBehavior, NO_PARAMS};
    use std::sync::mpsc::sync_channel;
    use std::thread;
    use std::time;

    #[test]
    fn test_unlock_notify() {
        let url = "file::memory:?cache=shared";
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI;
        let db1 = Connection::open_with_flags(url, flags).unwrap();
        db1.execute_batch("CREATE TABLE foo (x)").unwrap();
        let (rx, tx) = sync_channel(0);
        let child = thread::spawn(move || {
            let mut db2 = Connection::open_with_flags(url, flags).unwrap();
            let tx2 = Transaction::new(&mut db2, TransactionBehavior::Immediate).unwrap();
            tx2.execute_batch("INSERT INTO foo VALUES (42)").unwrap();
            rx.send(1).unwrap();
            let ten_millis = time::Duration::from_millis(10);
            thread::sleep(ten_millis);
            tx2.commit().unwrap();
        });
        assert_eq!(tx.recv().unwrap(), 1);
        let the_answer: Result<i64> = db1.query_row("SELECT x FROM foo", NO_PARAMS, |r| r.get(0));
        assert_eq!(42i64, the_answer.unwrap());
        child.join().unwrap();
    }
}
