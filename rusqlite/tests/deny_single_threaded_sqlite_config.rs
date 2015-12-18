//! Ensure we reject connections when SQLite is in single-threaded mode, as it
//! would violate safety if multiple Rust threads tried to use connections.

extern crate rusqlite;
extern crate libsqlite3_sys as ffi;

use rusqlite::Connection;

#[test]
fn test_error_when_singlethread_mode() {
    // put SQLite into single-threaded mode
    unsafe {
        // 1 == SQLITE_CONFIG_SINGLETHREAD
        assert_eq!(ffi::sqlite3_config(1), ffi::SQLITE_OK);
        println!("{}", ffi::sqlite3_mutex_alloc(0) as u64);
    }

    let result = Connection::open_in_memory();
    assert!(result.is_err());
}
