//! [Unlock Notification](http://sqlite.org/unlock_notify.html)

use std::sync::{Mutex, Condvar};
use std::os::raw::{c_char, c_int, c_void};

use ffi;
use InnerConnection;

struct UnlockNotification {
    cond: Condvar, // Condition variable to wait on
    mutex: Mutex<bool>, // Mutex to protect structure
}

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

impl InnerConnection {
    fn blocking_prepare(&mut self,
            z_sql: *const c_char,
            n_byte: c_int,
            pp_stmt: *mut *mut ffi::sqlite3_stmt,
            pz_tail: *mut *const c_char) -> c_int {
        let mut rc;
        loop {
            rc = unsafe {
                ffi::sqlite3_prepare_v2(self.db, z_sql, n_byte, pp_stmt, pz_tail)
            };
            if rc != ffi::SQLITE_LOCKED {
                break;
            }
            rc = self.wait_for_unlock_notify();
            if rc != ffi::SQLITE_OK {
                break;
            }
        }
        rc
    }

    fn wait_for_unlock_notify(&mut self) -> c_int {
        let mut un = UnlockNotification::new();
        /* Register for an unlock-notify callback. */
        let rc = unsafe { ffi::sqlite3_unlock_notify(self.db, Some(unlock_notify_cb), &mut un as *mut UnlockNotification as *mut c_void) };
        debug_assert!(rc == ffi::SQLITE_LOCKED || rc == ffi::SQLITE_OK);
        if rc == ffi::SQLITE_OK {
            un.wait();
        }
        rc
    }
}