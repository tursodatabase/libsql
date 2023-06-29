pub struct libsql_database {
    pub(crate) db: libsql::Database,
}

#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct libsql_database_ref {
    ptr: *const libsql_database,
}

impl libsql_database_ref {
    pub fn null() -> libsql_database_ref {
        libsql_database_ref {
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
impl From<&libsql_database> for libsql_database_ref {
    fn from(value: &libsql_database) -> Self {
        Self { ptr: value }
    }
}

#[allow(clippy::from_over_into)]
impl From<&mut libsql_database> for libsql_database_ref {
    fn from(value: &mut libsql_database) -> Self {
        Self { ptr: value }
    }
}
