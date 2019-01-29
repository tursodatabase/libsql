//! Ensure we reject connections when SQLite is in single-threaded mode, as it
//! would violate safety if multiple Rust threads tried to use connections.

use rusqlite::ffi;
use rusqlite::Connection;

#[test]
#[should_panic]
fn test_error_when_singlethread_mode() {
    // put SQLite into single-threaded mode
    unsafe {
        if ffi::sqlite3_config(ffi::SQLITE_CONFIG_SINGLETHREAD) != ffi::SQLITE_OK {
            return;
        }
        if ffi::sqlite3_initialize() != ffi::SQLITE_OK {
            return;
        }
    }

    let _ = Connection::open_in_memory().unwrap();
}
