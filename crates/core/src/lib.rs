pub mod errors;

use errors::Error;

type Result<T> = std::result::Result<T, Error>;

pub struct Database {
    pub url: String,
}

impl Database {
    pub fn open(url: String) -> Database {
        Database { url }
    }

    pub fn close(&self) {}
}

pub struct Connection {
    raw: *mut sqlite3_sys::sqlite3,
}

unsafe impl Send for Connection {} // TODO: is this safe?

impl Connection {
    pub fn connect(db: &Database) -> Result<Connection> {
        let mut raw = std::ptr::null_mut();
        let url = db.url.clone();
        let err = unsafe {
            // FIXME: switch to libsql_sys
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
                return Err(Error::ConnectionFailed(url.clone()));
            }
        }
        Ok(Connection { raw })
    }

    pub fn disconnect(&self) {
        unsafe {
            sqlite3_sys::sqlite3_close_v2(self.raw);
        }
    }

    pub fn execute(&self, sql: String) -> ResultSet {
        ResultSet { raw: self.raw, sql }
    }
}

pub struct ResultSet {
    raw: *mut sqlite3_sys::sqlite3,
    sql: String,
}

impl futures::Future for ResultSet {
    type Output = Result<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let err = unsafe {
            sqlite3_sys::sqlite3_exec(
                self.raw,
                self.sql.as_ptr() as *const i8,
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        let ret = match err {
            sqlite3_sys::SQLITE_OK => {
                Ok(())
            }
            _ => {
                Err(Error::QueryFailed(self.sql.clone()))
            }
        };
        std::task::Poll::Ready(ret)
    }
}

impl ResultSet {
    pub fn wait(&mut self) -> Result<()> {
        Ok(futures::executor::block_on(self)?)
    }

    pub fn row_count(&self) -> i32 {
        0
    }

    pub fn column_count(&self) -> i32 {
        0
    }
}
