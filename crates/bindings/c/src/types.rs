pub struct libsql_database {
    pub(crate) db: libsql::Database,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_database_t {
    ptr: *const libsql_database,
}

impl libsql_database_t {
    pub fn null() -> libsql_database_t {
        libsql_database_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql::Database {
        &unsafe { &*(self.ptr) }.db
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::Database {
        let ptr_mut = self.ptr as *mut libsql_database;
        &mut unsafe { &mut (*ptr_mut) }.db
    }
}

#[allow(clippy::from_over_into)]
impl From<&libsql_database> for libsql_database_t {
    fn from(value: &libsql_database) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_database> for libsql_database_t {
    fn from(value: &mut libsql_database) -> Self {
        Self { ptr: value }
    }
}

pub struct libsql_connection {
    pub(crate) conn: libsql::Connection,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_connection_t {
    ptr: *const libsql_connection,
}

impl libsql_connection_t {
    pub fn null() -> libsql_connection_t {
        libsql_connection_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql::Connection {
        &unsafe { &*(self.ptr) }.conn
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::Connection {
        let ptr_mut = self.ptr as *mut libsql_connection;
        &mut unsafe { &mut (*ptr_mut) }.conn
    }
}

#[allow(clippy::from_over_into)]
impl From<&libsql_connection> for libsql_connection_t {
    fn from(value: &libsql_connection) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_connection> for libsql_connection_t {
    fn from(value: &mut libsql_connection) -> Self {
        Self { ptr: value }
    }
}

pub struct libsql_rows {
    pub(crate) result: libsql::Rows,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_rows_t {
    ptr: *const libsql_rows,
}

impl libsql_rows_t {
    pub fn null() -> libsql_rows_t {
        libsql_rows_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql::Rows {
        &unsafe { &*(self.ptr) }.result
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::Rows {
        let ptr_mut = self.ptr as *mut libsql_rows;
        &mut unsafe { &mut (*ptr_mut) }.result
    }
}

#[allow(clippy::from_over_into)]
impl From<&libsql_rows> for libsql_rows_t {
    fn from(value: &libsql_rows) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_rows> for libsql_rows_t {
    fn from(value: &mut libsql_rows) -> Self {
        Self { ptr: value }
    }
}

pub struct libsql_rows_future<'a> {
    pub(crate) result: libsql::RowsFuture<'a>,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_rows_future_t {
    ptr: *const libsql_rows_future,
}

impl libsql_rows_future_t {
    pub fn null() -> libsql_rows_future_t {
        libsql_rows_future_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql::RowsFuture {
        &unsafe { &*(self.ptr) }.result
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::RowsFuture {
        let ptr_mut = self.ptr as *mut libsql_rows_future;
        &mut unsafe { &mut (*ptr_mut) }.result
    }
}

#[allow(clippy::from_over_into)]
impl From<&libsql_rows_future> for libsql_rows_future_t {
    fn from(value: &libsql_rows_future) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_rows_future> for libsql_rows_future_t {
    fn from(value: &mut libsql_rows_future) -> Self {
        Self { ptr: value }
    }
}
