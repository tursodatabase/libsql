//! [Unlock Notification](http://sqlite.org/unlock_notify.html)

#[cfg(feature = "unlock_notify")]
use std::sync::{Condvar, Mutex};
use std::os::raw::c_int;
#[cfg(feature = "unlock_notify")]
use std::os::raw::c_void;

use ffi;

#[cfg(feature = "unlock_notify")]
struct UnlockNotification {
    cond: Condvar,      // Condition variable to wait on
    mutex: Mutex<bool>, // Mutex to protect structure
}

#[cfg(feature = "unlock_notify")]
impl UnlockNotification {
    fn new() -> UnlockNotification {
        UnlockNotification {
            cond: Condvar::new(),
            mutex: Mutex::new(false),
        }
    }

    fn fired(&mut self) {
        *self.mutex.lock().unwrap() = true;
        self.cond.notify_one();
    }

    fn wait(&mut self) -> bool {
        let mut fired = self.mutex.lock().unwrap();
        if !*fired {
            fired = self.cond.wait(fired).unwrap();
        }
        *fired
    }
}

/// This function is an unlock-notify callback
#[cfg(feature = "unlock_notify")]
unsafe extern "C" fn unlock_notify_cb(ap_arg: *mut *mut c_void, n_arg: c_int) {
    /*int i;
  for(i=0; i<nArg; i++){
    UnlockNotification *p = (UnlockNotification *)apArg[i];
    pthread_mutex_lock(&p->mutex);
    p->fired = 1;
    pthread_cond_signal(&p->cond);
    pthread_mutex_unlock(&p->mutex);
  }*/
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
pub fn wait_for_unlock_notify(db: *mut ffi::sqlite3) -> c_int {
    let mut un = UnlockNotification::new();
    /* Register for an unlock-notify callback. */
    let rc = unsafe {
        ffi::sqlite3_unlock_notify(
            db,
            Some(unlock_notify_cb),
            &mut un as *mut UnlockNotification as *mut c_void,
        )
    };
    debug_assert!(rc == ffi::SQLITE_LOCKED || rc == ffi::SQLITE_OK);
    if rc == ffi::SQLITE_OK {
        un.wait();
    }
    rc
}

#[cfg(not(feature = "unlock_notify"))]
pub fn wait_for_unlock_notify(_db: *mut ffi::sqlite3) -> c_int {
    unreachable!()
}
