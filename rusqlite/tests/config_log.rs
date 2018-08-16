//! This file contains unit tests for `rusqlite::trace::config_log`. This
//! function affects SQLite process-wide and so is not safe to run as a normal
//! #[test] in the library.

#[cfg(feature = "trace")]
#[macro_use]
extern crate lazy_static;
extern crate rusqlite;

#[cfg(feature = "trace")]
fn main() {
    use std::os::raw::c_int;
    use std::sync::Mutex;

    lazy_static! {
        static ref LOGS_RECEIVED: Mutex<Vec<(c_int, String)>> = Mutex::new(Vec::new());
    }

    fn log_handler(err: c_int, message: &str) {
        let mut logs_received = LOGS_RECEIVED.lock().unwrap();
        logs_received.push((err, message.to_owned()));
    }

    use rusqlite::trace;

    unsafe { trace::config_log(Some(log_handler)) }.unwrap();
    trace::log(10, "First message from rusqlite");
    unsafe { trace::config_log(None) }.unwrap();
    trace::log(11, "Second message from rusqlite");

    let logs_received = LOGS_RECEIVED.lock().unwrap();
    assert_eq!(logs_received.len(), 1);
    assert_eq!(logs_received[0].0, 10);
    assert_eq!(logs_received[0].1, "First message from rusqlite");
}

#[cfg(not(feature = "trace"))]
fn main() {}
