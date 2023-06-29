pub struct Database {
    raw: *mut sqlite3_sys::sqlite3,
}

impl Database {
    pub fn open(url: &str) -> Database {
        let mut raw = std::ptr::null_mut();
        /*
        let err = unsafe {
            sqlite3_sys::sqlite3_open_v2(
                url.as_ptr() as *const i8,
                &mut raw,
                sqlite3_sys::SQLITE_OPEN_READWRITE | sqlite3_sys::SQLITE_OPEN_CREATE,
                std::ptr::null(),
            )
        };
        match err {
            sqlite3_sys::SQLITE_OK => {}
            _ => {
                panic!("sqlite3_open_v2 failed: {}", err);
            }
        }
        */
        Database { raw }
    }

    pub fn close(&self) {
        println!("Closing database");
    }
}

pub struct Connection {
}

impl Connection {
    pub fn disconnect(&self) {
        println!("Disconnecting");
    }
}

pub struct Result {
}

impl Result {
    pub fn row_count(&self) -> i32 {
        0
    }

    pub fn column_count(&self) -> i32 {
        0
    }
}