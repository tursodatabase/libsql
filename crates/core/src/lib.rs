pub mod errors;

use errors::Error;

type Result<T> = std::result::Result<T, Error>;

pub struct Database {
    pub url: String,
}

impl Database {
    pub fn open<S: Into<String>>(url: S) -> Database {
        Database { url: url.into() }
    }

    pub fn close(&self) {}

    pub fn connect(&self) -> Result<Connection> {
        Connection::connect(self)
    }
}

pub struct Connection {
    raw: *mut sqlite3_sys::sqlite3,
}

unsafe impl Send for Connection {} // TODO: is this safe?

impl Connection {
    pub(crate) fn connect(db: &Database) -> Result<Connection> {
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
                return Err(Error::ConnectionFailed(url));
            }
        }
        Ok(Connection { raw })
    }

    pub fn disconnect(&self) {
        unsafe {
            sqlite3_sys::sqlite3_close_v2(self.raw);
        }
    }

    pub fn execute<S: Into<String>>(&self, sql: S) -> ResultSet {
        ResultSet { raw: self.raw, sql: sql.into() }
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
        _cx: &mut std::task::Context<'_>,
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
            sqlite3_sys::SQLITE_OK => Ok(()),
            _ => Err(Error::QueryFailed(self.sql.clone())),
        };
        std::task::Poll::Ready(ret)
    }
}

impl ResultSet {
    pub fn wait(&mut self) -> Result<()> {
        futures::executor::block_on(self)
    }

    pub fn row_count(&self) -> i32 {
        0
    }

    pub fn column_count(&self) -> i32 {
        0
    }
}
