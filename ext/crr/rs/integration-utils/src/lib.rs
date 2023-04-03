use core::ffi::c_char;
use sqlite::{Connection, ManagedConnection, ResultCode};
use sqlite_nostd as sqlite;

pub fn opendb() -> Result<CRConnection, ResultCode> {
    let connection = sqlite::open(sqlite::strlit!(":memory:"))?;
    connection.enable_load_extension(true)?;
    connection.load_extension("../../dbg/crsqlite", None)?;
    Ok(CRConnection { db: connection })
}

pub struct CRConnection {
    pub db: ManagedConnection,
}

impl Drop for CRConnection {
    fn drop(&mut self) {
        if let Err(_) = self.db.exec_safe("SELECT crsql_finalize()") {
            panic!("Failed to finalize cr sql statements");
        }
    }
}

// macro_rules! wrap_fn {
//     ( $name:ident, $body:expr ) => {

//         fn $name() $body
//     };
// }

#[macro_export]
macro_rules! counter_setup {
    ( $count:expr ) => {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static COUNTER: AtomicUsize = AtomicUsize::new($count);

        fn decrement_counter() {
            if COUNTER.fetch_sub(1, Ordering::SeqCst) == 1 {
                sqlite::shutdown();
            }
        }
    };
}

// Macro to allow `afterAll` tear down once all tests complete
// Works by bumping a static counter on each fn def
// then by calling `afterAll` which checks the counter
