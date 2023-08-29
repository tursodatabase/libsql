#[derive(Clone, Debug)]
#[repr(C)]
pub struct blob {
    pub ptr: *const std::ffi::c_char,
    pub len: std::ffi::c_int,
}

pub struct libsql_database {
    pub(crate) db: libsql::v2::Database,
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

    pub fn get_ref(&self) -> &libsql::v2::Database {
        &unsafe { &*(self.ptr) }.db
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::v2::Database {
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
    pub(crate) conn: libsql::v2::Connection,
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

    pub fn get_ref(&self) -> &libsql::v2::Connection {
        &unsafe { &*(self.ptr) }.conn
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::v2::Connection {
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
    pub(crate) result: libsql::v2::Rows,
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

    pub fn get_ref(&self) -> &libsql::v2::Rows {
        &unsafe { &*(self.ptr) }.result
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::v2::Rows {
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

pub struct libsql_rows_future {
    pub(crate) result: libsql::RowsFuture,
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
pub struct libsql_row {
    pub(crate) result: libsql::v2::Row,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_row_t {
    ptr: *const libsql_row,
}

impl libsql_row_t {
    pub fn null() -> libsql_row_t {
        libsql_row_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql::v2::Row {
        &unsafe { &*(self.ptr) }.result
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql::v2::Row {
        let ptr_mut = self.ptr as *mut libsql_row;
        &mut unsafe { &mut (*ptr_mut) }.result
    }
}

impl From<&libsql_row> for libsql_row_t {
    fn from(value: &libsql_row) -> Self {
        Self { ptr: value }
    }
}

impl From<&mut libsql_row> for libsql_row_t {
    fn from(value: &mut libsql_row) -> Self {
        Self { ptr: value }
    }
}
