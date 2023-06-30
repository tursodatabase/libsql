pub struct libsql_database {
    pub(crate) db: libsql_core::Database,
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

    pub fn get_ref(&self) -> &libsql_core::Database {
        &unsafe { &*(self.ptr) }.db
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql_core::Database {
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
    pub(crate) conn: libsql_core::Connection,
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

    pub fn get_ref(&self) -> &libsql_core::Connection {
        &unsafe { &*(self.ptr) }.conn
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql_core::Connection {
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

pub struct libsql_result {
    pub(crate) result: libsql_core::ResultSet,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_result_t {
    ptr: *const libsql_result,
}

impl libsql_result_t {
    pub fn null() -> libsql_result_t {
        libsql_result_t {
            ptr: std::ptr::null(),
        }
    }

    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    pub fn get_ref(&self) -> &libsql_core::ResultSet {
        &unsafe { &*(self.ptr) }.result
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_ref_mut(&self) -> &mut libsql_core::ResultSet {
        let ptr_mut = self.ptr as *mut libsql_result;
        &mut unsafe { &mut (*ptr_mut) }.result
    }
}

#[allow(clippy::from_over_into)]
impl From<&libsql_result> for libsql_result_t {
    fn from(value: &libsql_result) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_result> for libsql_result_t {
    fn from(value: &mut libsql_result) -> Self {
        Self { ptr: value }
    }
}
