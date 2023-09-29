extern crate alloc;

use alloc::boxed::Box;
use alloc::ffi::{IntoStringError, NulError};
use alloc::vec::Vec;
use alloc::{ffi::CString, string::String};
use core::array::TryFromSliceError;
use core::ffi::{c_char, c_int, c_void, CStr};
use core::ptr::null_mut;
use core::{error::Error, slice, str::Utf8Error};

#[cfg(not(feature = "std"))]
use num_derive::FromPrimitive;
#[cfg(not(feature = "std"))]
use num_traits::FromPrimitive;

pub use sqlite3_allocator::*;
pub use sqlite3_capi::*;

// https://www.sqlite.org/c3ref/c_alter_table.html
#[derive(FromPrimitive, PartialEq, Debug)]
pub enum ActionCode {
    COPY = 0,
    CREATE_INDEX = 1,
    CREATE_TABLE = 2,
    CREATE_TEMP_INDEX = 3,
    CREATE_TEMP_TABLE = 4,
    CREATE_TEMP_TRIGGER = 5,
    CREATE_TEMP_VIEW = 6,
    CREATE_TRIGGER = 7,
    CREATE_VIEW = 8,
    DELETE = 9,
    DROP_INDEX = 10,
    DROP_TABLE = 11,
    DROP_TEMP_INDEX = 12,
    DROP_TEMP_TABLE = 13,
    DROP_TEMP_TRIGGER = 14,
    DROP_TEMP_VIEW = 15,
    DROP_TRIGGER = 16,
    DROP_VIEW = 17,
    INSERT = 18,
    PRAGMA = 19,
    READ = 20,
    SELECT = 21,
    TRANSACTION = 22,
    UPDATE = 23,
    ATTACH = 24,
    DETACH = 25,
    ALTER_TABLE = 26,
    REINDEX = 27,
    ANALYZE = 28,
    CREATE_VTABLE = 29,
    DROP_VTABLE = 30,
    FUNCTION = 31,
    SAVEPOINT = 32,
    RECURSIVE = 33,
}

#[derive(FromPrimitive, PartialEq, Debug)]
pub enum ResultCode {
    OK = 0,
    ERROR = 1,
    INTERNAL = 2,
    PERM = 3,
    ABORT = 4,
    BUSY = 5,
    LOCKED = 6,
    NOMEM = 7,
    READONLY = 8,
    INTERRUPT = 9,
    IOERR = 10,
    CORRUPT = 11,
    NOTFOUND = 12,
    FULL = 13,
    CANTOPEN = 14,
    PROTOCOL = 15,
    EMPTY = 16,
    SCHEMA = 17,
    TOOBIG = 18,
    CONSTRAINT = 19,
    MISMATCH = 20,
    MISUSE = 21,
    NOLFS = 22,
    AUTH = 23,
    FORMAT = 24,
    RANGE = 25,
    NOTADB = 26,
    NOTICE = 27,
    WARNING = 28,
    ROW = 100,
    DONE = 101,
    ERROR_MISSING_COLLSEQ = bindings::SQLITE_ERROR_MISSING_COLLSEQ as isize,
    ERROR_RETRY = bindings::SQLITE_ERROR_RETRY as isize,
    ERROR_SNAPSHOT = bindings::SQLITE_ERROR_SNAPSHOT as isize,
    IOERR_READ = bindings::SQLITE_IOERR_READ as isize,
    IOERR_SHORT_READ = bindings::SQLITE_IOERR_SHORT_READ as isize,
    IOERR_WRITE = bindings::SQLITE_IOERR_WRITE as isize,
    IOERR_FSYNC = bindings::SQLITE_IOERR_FSYNC as isize,
    IOERR_DIR_FSYNC = bindings::SQLITE_IOERR_DIR_FSYNC as isize,
    IOERR_TRUNCATE = bindings::SQLITE_IOERR_TRUNCATE as isize,
    IOERR_FSTAT = bindings::SQLITE_IOERR_FSTAT as isize,
    IOERR_UNLOCK = bindings::SQLITE_IOERR_UNLOCK as isize,
    IOERR_RDLOCK = bindings::SQLITE_IOERR_RDLOCK as isize,
    IOERR_DELETE = bindings::SQLITE_IOERR_DELETE as isize,
    IOERR_BLOCKED = bindings::SQLITE_IOERR_BLOCKED as isize,
    IOERR_NOMEM = bindings::SQLITE_IOERR_NOMEM as isize,
    IOERR_ACCESS = bindings::SQLITE_IOERR_ACCESS as isize,
    IOERR_CHECKRESERVEDLOCK = bindings::SQLITE_IOERR_CHECKRESERVEDLOCK as isize,
    IOERR_LOCK = bindings::SQLITE_IOERR_LOCK as isize,
    IOERR_CLOSE = bindings::SQLITE_IOERR_CLOSE as isize,
    IOERR_DIR_CLOSE = bindings::SQLITE_IOERR_DIR_CLOSE as isize,
    IOERR_SHMOPEN = bindings::SQLITE_IOERR_SHMOPEN as isize,
    IOERR_SHMSIZE = bindings::SQLITE_IOERR_SHMSIZE as isize,
    IOERR_SHMLOCK = bindings::SQLITE_IOERR_SHMLOCK as isize,
    IOERR_SHMMAP = bindings::SQLITE_IOERR_SHMMAP as isize,
    IOERR_SEEK = bindings::SQLITE_IOERR_SEEK as isize,
    IOERR_DELETE_NOENT = bindings::SQLITE_IOERR_DELETE_NOENT as isize,
    IOERR_MMAP = bindings::SQLITE_IOERR_MMAP as isize,
    IOERR_GETTEMPPATH = bindings::SQLITE_IOERR_GETTEMPPATH as isize,
    IOERR_CONVPATH = bindings::SQLITE_IOERR_CONVPATH as isize,
    IOERR_VNODE = bindings::SQLITE_IOERR_VNODE as isize,
    IOERR_AUTH = bindings::SQLITE_IOERR_AUTH as isize,
    IOERR_BEGIN_ATOMIC = bindings::SQLITE_IOERR_BEGIN_ATOMIC as isize,
    IOERR_COMMIT_ATOMIC = bindings::SQLITE_IOERR_COMMIT_ATOMIC as isize,
    IOERR_ROLLBACK_ATOMIC = bindings::SQLITE_IOERR_ROLLBACK_ATOMIC as isize,
    IOERR_DATA = bindings::SQLITE_IOERR_DATA as isize,
    IOERR_CORRUPTFS = bindings::SQLITE_IOERR_CORRUPTFS as isize,
    LOCKED_SHAREDCACHE = bindings::SQLITE_LOCKED_SHAREDCACHE as isize,
    LOCKED_VTAB = bindings::SQLITE_LOCKED_VTAB as isize,
    BUSY_RECOVERY = bindings::SQLITE_BUSY_RECOVERY as isize,
    BUSY_SNAPSHOT = bindings::SQLITE_BUSY_SNAPSHOT as isize,
    BUSY_TIMEOUT = bindings::SQLITE_BUSY_TIMEOUT as isize,
    CANTOPEN_NOTEMPDIR = bindings::SQLITE_CANTOPEN_NOTEMPDIR as isize,
    CANTOPEN_ISDIR = bindings::SQLITE_CANTOPEN_ISDIR as isize,
    CANTOPEN_FULLPATH = bindings::SQLITE_CANTOPEN_FULLPATH as isize,
    CANTOPEN_CONVPATH = bindings::SQLITE_CANTOPEN_CONVPATH as isize,
    CANTOPEN_DIRTYWAL = bindings::SQLITE_CANTOPEN_DIRTYWAL as isize,
    CANTOPEN_SYMLINK = bindings::SQLITE_CANTOPEN_SYMLINK as isize,
    CORRUPT_VTAB = bindings::SQLITE_CORRUPT_VTAB as isize,
    CORRUPT_SEQUENCE = bindings::SQLITE_CORRUPT_SEQUENCE as isize,
    CORRUPT_INDEX = bindings::SQLITE_CORRUPT_INDEX as isize,
    READONLY_RECOVERY = bindings::SQLITE_READONLY_RECOVERY as isize,
    READONLY_CANTLOCK = bindings::SQLITE_READONLY_CANTLOCK as isize,
    READONLY_ROLLBACK = bindings::SQLITE_READONLY_ROLLBACK as isize,
    READONLY_DBMOVED = bindings::SQLITE_READONLY_DBMOVED as isize,
    READONLY_CANTINIT = bindings::SQLITE_READONLY_CANTINIT as isize,
    READONLY_DIRECTORY = bindings::SQLITE_READONLY_DIRECTORY as isize,
    ABORT_ROLLBACK = bindings::SQLITE_ABORT_ROLLBACK as isize,
    CONSTRAINT_CHECK = bindings::SQLITE_CONSTRAINT_CHECK as isize,
    CONSTRAINT_COMMITHOOK = bindings::SQLITE_CONSTRAINT_COMMITHOOK as isize,
    CONSTRAINT_FOREIGNKEY = bindings::SQLITE_CONSTRAINT_FOREIGNKEY as isize,
    CONSTRAINT_FUNCTION = bindings::SQLITE_CONSTRAINT_FUNCTION as isize,
    CONSTRAINT_NOTNULL = bindings::SQLITE_CONSTRAINT_NOTNULL as isize,
    CONSTRAINT_PRIMARYKEY = bindings::SQLITE_CONSTRAINT_PRIMARYKEY as isize,
    CONSTRAINT_TRIGGER = bindings::SQLITE_CONSTRAINT_TRIGGER as isize,
    CONSTRAINT_UNIQUE = bindings::SQLITE_CONSTRAINT_UNIQUE as isize,
    CONSTRAINT_VTAB = bindings::SQLITE_CONSTRAINT_VTAB as isize,
    CONSTRAINT_ROWID = bindings::SQLITE_CONSTRAINT_ROWID as isize,
    CONSTRAINT_PINNED = bindings::SQLITE_CONSTRAINT_PINNED as isize,
    CONSTRAINT_DATATYPE = bindings::SQLITE_CONSTRAINT_DATATYPE as isize,
    NOTICE_RECOVER_WAL = bindings::SQLITE_NOTICE_RECOVER_WAL as isize,
    NOTICE_RECOVER_ROLLBACK = bindings::SQLITE_NOTICE_RECOVER_ROLLBACK as isize,
    WARNING_AUTOINDEX = bindings::SQLITE_WARNING_AUTOINDEX as isize,
    AUTH_USER = bindings::SQLITE_AUTH_USER as isize,
    OK_LOAD_PERMANENTLY = bindings::SQLITE_OK_LOAD_PERMANENTLY as isize,
    OK_SYMLINK = bindings::SQLITE_OK_SYMLINK as isize,

    NULL = 5000,
}

impl core::fmt::Display for ResultCode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ResultCode {}

impl From<Utf8Error> for ResultCode {
    fn from(_error: Utf8Error) -> Self {
        ResultCode::FORMAT
    }
}

impl From<TryFromSliceError> for ResultCode {
    fn from(_error: TryFromSliceError) -> Self {
        ResultCode::RANGE
    }
}

impl From<NulError> for ResultCode {
    fn from(_error: NulError) -> Self {
        ResultCode::NOMEM
    }
}

impl From<IntoStringError> for ResultCode {
    fn from(_error: IntoStringError) -> Self {
        ResultCode::FORMAT
    }
}

#[derive(FromPrimitive, PartialEq, Debug)]
pub enum ColumnType {
    Integer = 1,
    Float = 2,
    Text = 3,
    Blob = 4,
    Null = 5,
}

pub fn open(filename: *const c_char) -> Result<ManagedConnection, ResultCode> {
    let mut db = core::ptr::null_mut();
    let rc =
        ResultCode::from_i32(sqlite3_capi::open(filename, &mut db as *mut *mut sqlite3)).unwrap();
    if rc == ResultCode::OK {
        Ok(ManagedConnection { db })
    } else {
        Err(rc)
    }
}

pub fn randomness(blob: &mut [u8]) {
    sqlite3_capi::randomness(blob.len() as c_int, blob.as_mut_ptr() as *mut c_void)
}

pub struct ManagedConnection {
    pub db: *mut sqlite3,
}

pub trait Connection {
    fn commit_hook(
        &self,
        callback: Option<xCommitHook>,
        user_data: *mut c_void,
    ) -> Option<xCommitHook>;

    fn create_function_v2(
        &self,
        name: &str,
        n_arg: i32,
        flags: u32,
        user_data: Option<*mut c_void>,
        func: Option<xFunc>,
        step: Option<xStep>,
        final_func: Option<xFinal>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode>;

    fn create_module_v2(
        &self,
        name: &str,
        module: *const module,
        user_data: Option<*mut c_void>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode>;

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn enable_load_extension(&self, enable: bool) -> Result<ResultCode, ResultCode>;

    fn errcode(&self) -> ResultCode;
    fn errmsg(&self) -> Result<String, IntoStringError>;

    /// sql should be a null terminated string! However you find is most efficient to craft those,
    /// hence why we have no opinion on &str vs String vs CString vs whatever
    /// todo: we should make some sort of opaque type to force null termination
    /// this is inehritly unsafe
    unsafe fn exec(&self, sql: *const c_char) -> Result<ResultCode, ResultCode>;

    fn exec_safe(&self, sql: &str) -> Result<ResultCode, ResultCode>;

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn load_extension(
        &self,
        filename: &str,
        entrypoint: Option<&str>,
    ) -> Result<ResultCode, ResultCode>;

    fn next_stmt(&self, s: Option<*mut stmt>) -> Option<*mut stmt>;

    fn prepare_v2(&self, sql: &str) -> Result<ManagedStmt, ResultCode>;

    fn prepare_v3(&self, sql: &str, flags: u32) -> Result<ManagedStmt, ResultCode>;

    fn set_authorizer(
        &self,
        x_auth: Option<XAuthorizer>,
        user_data: *mut c_void,
    ) -> Result<ResultCode, ResultCode>;

    fn get_autocommit(&self) -> bool;
}

impl Connection for ManagedConnection {
    fn commit_hook(
        &self,
        callback: Option<xCommitHook>,
        user_data: *mut c_void,
    ) -> Option<xCommitHook> {
        self.db.commit_hook(callback, user_data)
    }

    /// TODO: create_function is infrequent enough that we can pay the cost of the copy rather than
    /// take a c_char
    fn create_function_v2(
        &self,
        name: &str,
        n_arg: i32,
        flags: u32,
        user_data: Option<*mut c_void>,
        func: Option<xFunc>,
        step: Option<xStep>,
        final_func: Option<xFinal>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode> {
        self.db.create_function_v2(
            name, n_arg, flags, user_data, func, step, final_func, destroy,
        )
    }

    fn set_authorizer(
        &self,
        x_auth: Option<XAuthorizer>,
        user_data: *mut c_void,
    ) -> Result<ResultCode, ResultCode> {
        self.db.set_authorizer(x_auth, user_data)
    }

    fn create_module_v2(
        &self,
        name: &str,
        module: *const module,
        user_data: Option<*mut c_void>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode> {
        self.db.create_module_v2(name, module, user_data, destroy)
    }

    #[inline]
    fn next_stmt(&self, s: Option<*mut stmt>) -> Option<*mut stmt> {
        self.db.next_stmt(s)
    }

    #[inline]
    fn prepare_v2(&self, sql: &str) -> Result<ManagedStmt, ResultCode> {
        self.db.prepare_v2(sql)
    }

    #[inline]
    fn prepare_v3(&self, sql: &str, flags: u32) -> Result<ManagedStmt, ResultCode> {
        self.db.prepare_v3(sql, flags)
    }

    #[inline]
    unsafe fn exec(&self, sql: *const c_char) -> Result<ResultCode, ResultCode> {
        self.db.exec(sql)
    }

    #[inline]
    fn exec_safe(&self, sql: &str) -> Result<ResultCode, ResultCode> {
        self.db.exec_safe(sql)
    }

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn enable_load_extension(&self, enable: bool) -> Result<ResultCode, ResultCode> {
        self.db.enable_load_extension(enable)
    }

    #[inline]
    fn errmsg(&self) -> Result<String, IntoStringError> {
        self.db.errmsg()
    }

    #[inline]
    fn errcode(&self) -> ResultCode {
        self.db.errcode()
    }

    #[inline]
    fn get_autocommit(&self) -> bool {
        self.db.get_autocommit()
    }

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn load_extension(
        &self,
        filename: &str,
        entrypoint: Option<&str>,
    ) -> Result<ResultCode, ResultCode> {
        self.db.load_extension(filename, entrypoint)
    }
}

impl Drop for ManagedConnection {
    fn drop(&mut self) {
        // todo: iterate over all stmts and finalize them?
        let rc = sqlite3_capi::close(self.db);
        if rc != 0 {
            // This seems aggressive...
            // The alternative is to make users manually drop connections and manually finalize
            // stmts :/
            // Or we could not panic.. but then you will unknowningly have memory
            // leaks in your app. The reason being that a failure to close the db
            // does not release the memory of that db.
            panic!(
                "SQLite returned error {:?} when trying to close the db!",
                rc
            );
        }
    }
}

impl Connection for *mut sqlite3 {
    fn commit_hook(
        &self,
        callback: Option<xCommitHook>,
        user_data: *mut c_void,
    ) -> Option<xCommitHook> {
        commit_hook(*self, callback, user_data)
    }

    fn create_function_v2(
        &self,
        name: &str,
        n_arg: i32,
        flags: u32,
        user_data: Option<*mut c_void>,
        func: Option<xFunc>,
        step: Option<xStep>,
        final_func: Option<xFinal>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode> {
        if let Ok(name) = CString::new(name) {
            convert_rc(create_function_v2(
                *self,
                name.as_ptr(),
                n_arg,
                flags as c_int,
                user_data.unwrap_or(core::ptr::null_mut()),
                func,
                step,
                final_func,
                destroy,
            ))
        } else {
            Err(ResultCode::NOMEM)
        }
    }

    fn create_module_v2(
        &self,
        name: &str,
        module: *const module,
        user_data: Option<*mut c_void>,
        destroy: Option<xDestroy>,
    ) -> Result<ResultCode, ResultCode> {
        if let Ok(name) = CString::new(name) {
            convert_rc(create_module_v2(
                *self,
                name.as_ptr(),
                module,
                user_data.unwrap_or(core::ptr::null_mut()),
                destroy,
            ))
        } else {
            Err(ResultCode::NOMEM)
        }
    }

    #[inline]
    fn prepare_v2(&self, sql: &str) -> Result<ManagedStmt, ResultCode> {
        let mut stmt = core::ptr::null_mut();
        let mut tail = core::ptr::null();
        let rc = ResultCode::from_i32(prepare_v2(
            *self,
            sql.as_ptr() as *const c_char,
            sql.len() as i32,
            &mut stmt as *mut *mut stmt,
            &mut tail as *mut *const c_char,
        ))
        .unwrap();
        if rc == ResultCode::OK {
            Ok(ManagedStmt { stmt: stmt })
        } else {
            Err(rc)
        }
    }

    #[inline]
    fn prepare_v3(&self, sql: &str, flags: u32) -> Result<ManagedStmt, ResultCode> {
        let mut stmt = core::ptr::null_mut();
        let mut tail = core::ptr::null();
        let rc = ResultCode::from_i32(prepare_v3(
            *self,
            sql.as_ptr() as *const c_char,
            sql.len() as i32,
            flags,
            &mut stmt as *mut *mut stmt,
            &mut tail as *mut *const c_char,
        ))
        .unwrap();
        if rc == ResultCode::OK {
            Ok(ManagedStmt { stmt: stmt })
        } else {
            Err(rc)
        }
    }

    #[inline]
    unsafe fn exec(&self, sql: *const c_char) -> Result<ResultCode, ResultCode> {
        convert_rc(exec(*self, sql))
    }

    #[inline]
    fn exec_safe(&self, sql: &str) -> Result<ResultCode, ResultCode> {
        if let Ok(sql) = CString::new(sql) {
            convert_rc(exec(*self, sql.as_ptr()))
        } else {
            return Err(ResultCode::NOMEM);
        }
    }

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn enable_load_extension(&self, enable: bool) -> Result<ResultCode, ResultCode> {
        convert_rc(enable_load_extension(*self, if enable { 1 } else { 0 }))
    }

    #[cfg(all(feature = "static", not(feature = "omit_load_extension")))]
    fn load_extension(
        &self,
        filename: &str,
        entrypoint: Option<&str>,
    ) -> Result<ResultCode, ResultCode> {
        if let Ok(filename) = CString::new(filename) {
            if let Some(entrypoint) = entrypoint {
                if let Ok(entrypoint) = CString::new(entrypoint) {
                    convert_rc(load_extension(
                        *self,
                        filename.as_ptr(),
                        entrypoint.as_ptr(),
                        core::ptr::null_mut(),
                    ))
                } else {
                    Err(ResultCode::NOMEM)
                }
            } else {
                convert_rc(load_extension(
                    *self,
                    filename.as_ptr(),
                    core::ptr::null(),
                    core::ptr::null_mut(),
                ))
            }
        } else {
            Err(ResultCode::NOMEM)
        }
    }

    #[inline]
    fn next_stmt(&self, s: Option<*mut stmt>) -> Option<*mut stmt> {
        let s = if let Some(s) = s {
            s
        } else {
            core::ptr::null_mut()
        };

        let ptr = next_stmt(*self, s);
        if ptr.is_null() {
            None
        } else {
            Some(ptr)
        }
    }

    fn set_authorizer(
        &self,
        x_auth: Option<XAuthorizer>,
        user_data: *mut c_void,
    ) -> Result<ResultCode, ResultCode> {
        convert_rc(set_authorizer(*self, x_auth, user_data))
    }

    fn errmsg(&self) -> Result<String, IntoStringError> {
        errmsg(*self).into_string()
    }

    fn errcode(&self) -> ResultCode {
        ResultCode::from_i32(errcode(*self)).unwrap()
    }

    fn get_autocommit(&self) -> bool {
        get_autocommit(*self) != 0
    }
}

pub fn convert_rc(rc: i32) -> Result<ResultCode, ResultCode> {
    let rc = ResultCode::from_i32(rc).unwrap_or(ResultCode::ABORT);
    if rc == ResultCode::OK {
        Ok(rc)
    } else {
        Err(rc)
    }
}

pub struct ManagedStmt {
    pub stmt: *mut stmt,
}

impl ManagedStmt {
    pub fn reset(&self) -> Result<ResultCode, ResultCode> {
        convert_rc(reset(self.stmt))
    }

    pub fn step(&self) -> Result<ResultCode, ResultCode> {
        let rc = ResultCode::from_i32(step(self.stmt)).unwrap();
        if (rc == ResultCode::ROW) || (rc == ResultCode::DONE) {
            Ok(rc)
        } else {
            Err(rc)
        }
    }

    #[inline]
    pub fn column_count(&self) -> i32 {
        column_count(self.stmt)
    }

    /// Calls to `step` or addiitonal calls to `column_name` will invalidate the
    /// returned string. Unclear if there's any way to capture this
    /// behavior in the type system.
    #[inline]
    pub fn column_name(&self, i: i32) -> Result<&str, ResultCode> {
        let ptr = column_name(self.stmt, i);
        if ptr.is_null() {
            Err(ResultCode::NULL)
        } else {
            Ok(
                unsafe {
                    core::str::from_utf8_unchecked(core::ffi::CStr::from_ptr(ptr).to_bytes())
                },
            )
        }
    }

    #[inline]
    pub fn column_type(&self, i: i32) -> Result<ColumnType, ResultCode> {
        ColumnType::from_i32(column_type(self.stmt, i)).ok_or(ResultCode::NULL)
    }

    #[inline]
    pub fn column_text(&self, i: i32) -> Result<&str, ResultCode> {
        let len = column_bytes(self.stmt, i);
        let ptr = column_text_ptr(self.stmt, i);
        if ptr.is_null() {
            Err(ResultCode::NULL)
        } else {
            Ok(unsafe {
                let slice = core::slice::from_raw_parts(ptr as *const u8, len as usize);
                core::str::from_utf8_unchecked(slice)
            })
        }
    }

    #[inline]
    pub fn column_blob(&self, i: i32) -> Result<&[u8], ResultCode> {
        let len = column_bytes(self.stmt, i);
        let ptr = column_blob(self.stmt, i);
        if ptr.is_null() {
            Err(ResultCode::NULL)
        } else {
            Ok(unsafe { core::slice::from_raw_parts(ptr as *const u8, len as usize) })
        }
    }

    #[inline]
    pub fn column_double(&self, i: i32) -> Result<f64, ResultCode> {
        Ok(column_double(self.stmt, i))
    }

    #[inline]
    pub fn column_int(&self, i: i32) -> Result<i32, ResultCode> {
        Ok(column_int(self.stmt, i))
    }

    #[inline]
    pub fn column_int64(&self, i: i32) -> Result<i64, ResultCode> {
        Ok(column_int64(self.stmt, i))
    }

    #[inline]
    pub fn column_value(&self, i: i32) -> Result<*mut value, ResultCode> {
        let ptr = column_value(self.stmt, i);
        if ptr.is_null() {
            Err(ResultCode::NULL)
        } else {
            Ok(ptr)
        }
    }

    pub fn bind_blob(&self, i: i32, val: &[u8], d: Destructor) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_blob(
            self.stmt,
            i,
            val.as_ptr() as *const c_void,
            val.len() as i32,
            d,
        ))
    }

    #[inline]
    pub fn bind_value(&self, i: i32, val: *mut value) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_value(self.stmt, i, val))
    }

    #[inline]
    pub fn bind_text(&self, i: i32, text: &str, d: Destructor) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_text(
            self.stmt,
            i,
            text.as_ptr() as *const c_char,
            text.len() as i32,
            d,
        ))
    }

    #[inline]
    pub fn bind_int64(&self, i: i32, val: i64) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_int64(self.stmt, i, val))
    }

    #[inline]
    pub fn bind_int(&self, i: i32, val: i32) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_int(self.stmt, i, val))
    }

    #[inline]
    pub fn bind_null(&self, i: i32) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_null(self.stmt, i))
    }

    #[inline]
    pub fn clear_bindings(&self) -> Result<ResultCode, ResultCode> {
        convert_rc(clear_bindings(self.stmt))
    }
}

impl Drop for ManagedStmt {
    fn drop(&mut self) {
        finalize(self.stmt);
    }
}

pub trait Context {
    /// Pass and give ownership of the string to SQLite.
    /// SQLite will not copy the string.
    /// This method will correctly drop the string when SQLite is finished
    /// using it.
    fn result_text_owned(&self, text: String);
    fn result_text_transient(&self, text: &str);
    fn result_text_static(&self, text: &str);
    fn result_blob_owned(&self, blob: Vec<u8>);
    fn result_blob_transient(&self, blob: &[u8]);
    fn result_blob_static(&self, blob: &[u8]);
    fn result_error(&self, text: &str);
    fn result_error_code(&self, code: ResultCode);
    fn result_value(&self, value: *mut value);
    fn result_double(&self, value: f64);
    fn result_int64(&self, value: i64);
    fn result_null(&self);
    fn db_handle(&self) -> *mut sqlite3;
    fn user_data(&self) -> *mut c_void;
}

impl Context for *mut context {
    #[inline]
    fn result_null(&self) {
        result_null(*self)
    }

    /// Passes ownership of the blob to SQLite without copying.
    /// The blob must have been allocated with `sqlite3_malloc`!
    #[inline]
    fn result_text_owned(&self, text: String) {
        let (ptr, len, _) = text.into_raw_parts();
        result_text(
            *self,
            ptr as *const c_char,
            len as i32,
            // TODO: this drop code does not seem to work
            // Valgrind tells us we have a memory leak when using `result_text_owned`
            Destructor::CUSTOM(droprust),
        );
    }

    /// Takes a reference to a string, has SQLite copy the contents
    /// and take ownership of the copy.
    #[inline]
    fn result_text_transient(&self, text: &str) {
        result_text(
            *self,
            text.as_ptr() as *mut c_char,
            text.len() as i32,
            Destructor::TRANSIENT,
        );
    }

    /// Takes a reference to a string that will outlive SQLite's use of the string.
    /// SQLite will not copy this string.
    #[inline]
    fn result_text_static(&self, text: &str) {
        result_text(
            *self,
            text.as_ptr() as *mut c_char,
            text.len() as i32,
            Destructor::STATIC,
        );
    }

    /// Passes ownership of the blob to SQLite without copying.
    /// The blob must have been allocated with `sqlite3_malloc`!
    #[inline]
    fn result_blob_owned(&self, blob: Vec<u8>) {
        let (ptr, len, _) = blob.into_raw_parts();
        result_blob(*self, ptr, len as i32, Destructor::CUSTOM(droprust));
    }

    /// SQLite will make a copy of the blob
    #[inline]
    fn result_blob_transient(&self, blob: &[u8]) {
        result_blob(
            *self,
            blob.as_ptr(),
            blob.len() as i32,
            Destructor::TRANSIENT,
        );
    }

    #[inline]
    fn result_blob_static(&self, blob: &[u8]) {
        result_blob(*self, blob.as_ptr(), blob.len() as i32, Destructor::STATIC);
    }

    #[inline]
    fn result_error(&self, text: &str) {
        result_error(*self, text);
    }

    #[inline]
    fn result_error_code(&self, code: ResultCode) {
        result_error_code(*self, code as c_int);
    }

    #[inline]
    fn result_value(&self, value: *mut value) {
        result_value(*self, value);
    }

    #[inline]
    fn result_double(&self, value: f64) {
        result_double(*self, value);
    }

    #[inline]
    fn result_int64(&self, value: i64) {
        result_int64(*self, value);
    }

    #[inline]
    fn db_handle(&self) -> *mut sqlite3 {
        context_db_handle(*self)
    }

    #[inline]
    fn user_data(&self) -> *mut c_void {
        user_data(*self)
    }
}

pub trait Stmt {
    fn sql(&self) -> &str;
    fn bind_blob(&self, i: i32, val: &[u8], d: Destructor) -> Result<ResultCode, ResultCode>;
    /// Gives SQLite ownership of the blob and has SQLite free it.
    fn bind_blob_owned(&self, i: i32, val: Vec<u8>) -> Result<ResultCode, ResultCode>;
    fn bind_value(&self, i: i32, val: *mut value) -> Result<ResultCode, ResultCode>;
    fn bind_text(&self, i: i32, text: &str, d: Destructor) -> Result<ResultCode, ResultCode>;
    fn bind_text_owned(&self, i: i32, text: String) -> Result<ResultCode, ResultCode>;
    fn bind_int64(&self, i: i32, val: i64) -> Result<ResultCode, ResultCode>;
    fn bind_int(&self, i: i32, val: i32) -> Result<ResultCode, ResultCode>;
    fn bind_double(&self, i: i32, val: f64) -> Result<ResultCode, ResultCode>;
    fn bind_null(&self, i: i32) -> Result<ResultCode, ResultCode>;

    fn clear_bindings(&self) -> Result<ResultCode, ResultCode>;

    fn column_value(&self, i: i32) -> *mut value;
    fn column_int64(&self, i: i32) -> int64;
    fn column_int(&self, i: i32) -> i32;
    fn column_blob(&self, i: i32) -> &[u8];
    fn column_double(&self, i: i32) -> f64;
    fn column_text(&self, i: i32) -> &str;
    fn column_bytes(&self, i: i32) -> i32;

    fn finalize(&self) -> Result<ResultCode, ResultCode>;

    fn reset(&self) -> Result<ResultCode, ResultCode>;
    fn step(&self) -> Result<ResultCode, ResultCode>;
}

impl Stmt for *mut stmt {
    fn sql(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(core::ffi::CStr::from_ptr(sql(*self)).to_bytes()) }
    }

    #[inline]
    fn bind_blob(&self, i: i32, val: &[u8], d: Destructor) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_blob(
            *self,
            i,
            val.as_ptr() as *const c_void,
            val.len() as i32,
            d,
        ))
    }

    #[inline]
    fn bind_blob_owned(&self, i: i32, val: Vec<u8>) -> Result<ResultCode, ResultCode> {
        let (ptr, len, _) = val.into_raw_parts();
        convert_rc(bind_blob(
            *self,
            i,
            ptr as *const c_void,
            len as i32,
            Destructor::CUSTOM(droprust),
        ))
    }

    #[inline]
    fn bind_double(&self, i: i32, val: f64) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_double(*self, i, val))
    }

    #[inline]
    fn bind_value(&self, i: i32, val: *mut value) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_value(*self, i, val))
    }

    #[inline]
    fn bind_text(&self, i: i32, text: &str, d: Destructor) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_text(
            *self,
            i,
            text.as_ptr() as *const c_char,
            text.len() as i32,
            d,
        ))
    }

    #[inline]
    fn bind_text_owned(&self, i: i32, text: String) -> Result<ResultCode, ResultCode> {
        let (ptr, len, _) = text.into_raw_parts();
        convert_rc(bind_text(
            *self,
            i,
            ptr as *const c_char,
            len as i32,
            Destructor::CUSTOM(droprust),
        ))
    }

    #[inline]
    fn bind_int64(&self, i: i32, val: i64) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_int64(*self, i, val))
    }

    #[inline]
    fn bind_int(&self, i: i32, val: i32) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_int(*self, i, val))
    }

    #[inline]
    fn bind_null(&self, i: i32) -> Result<ResultCode, ResultCode> {
        convert_rc(bind_null(*self, i))
    }

    #[inline]
    fn clear_bindings(&self) -> Result<ResultCode, ResultCode> {
        convert_rc(clear_bindings(*self))
    }

    #[inline]
    fn column_value(&self, i: i32) -> *mut value {
        column_value(*self, i)
    }

    #[inline]
    fn column_int64(&self, i: i32) -> int64 {
        column_int64(*self, i)
    }

    #[inline]
    fn column_int(&self, i: i32) -> i32 {
        column_int(*self, i)
    }

    #[inline]
    fn column_blob(&self, i: i32) -> &[u8] {
        let len = column_bytes(*self, i);
        let ptr = column_blob(*self, i);
        unsafe { core::slice::from_raw_parts(ptr as *const u8, len as usize) }
    }

    #[inline]
    fn column_double(&self, i: i32) -> f64 {
        column_double(*self, i)
    }

    #[inline]
    fn column_text(&self, i: i32) -> &str {
        column_text(*self, i)
    }

    #[inline]
    fn column_bytes(&self, i: i32) -> i32 {
        column_bytes(*self, i)
    }

    #[inline]
    fn reset(&self) -> Result<ResultCode, ResultCode> {
        convert_rc(reset(*self))
    }

    #[inline]
    fn step(&self) -> Result<ResultCode, ResultCode> {
        match ResultCode::from_i32(step(*self)) {
            Some(ResultCode::ROW) => Ok(ResultCode::ROW),
            Some(ResultCode::DONE) => Ok(ResultCode::DONE),
            Some(rc) => Err(rc),
            None => Err(ResultCode::ERROR),
        }
    }

    #[inline]
    fn finalize(&self) -> Result<ResultCode, ResultCode> {
        match ResultCode::from_i32(finalize(*self)) {
            Some(ResultCode::OK) => Ok(ResultCode::OK),
            Some(rc) => Err(rc),
            None => Err(ResultCode::ABORT),
        }
    }
}

pub trait Value {
    fn blob(&self) -> &[u8];
    fn double(&self) -> f64;
    fn int(&self) -> i32;
    fn int64(&self) -> i64;
    fn text(&self) -> &str;
    fn bytes(&self) -> i32;
    fn value_type(&self) -> ColumnType;
}

impl Value for *mut value {
    #[inline]
    fn value_type(&self) -> ColumnType {
        ColumnType::from_i32(value_type(*self)).unwrap()
    }

    #[inline]
    fn blob(&self) -> &[u8] {
        value_blob(*self)
    }

    #[inline]
    fn double(&self) -> f64 {
        value_double(*self)
    }

    #[inline]
    fn int(&self) -> i32 {
        value_int(*self)
    }

    #[inline]
    fn int64(&self) -> i64 {
        value_int64(*self)
    }

    #[inline]
    fn text(&self) -> &str {
        value_text(*self)
    }

    #[inline]
    fn bytes(&self) -> i32 {
        value_bytes(*self)
    }
}

pub trait StrRef {
    fn set(&self, val: &str);
}

impl StrRef for *mut *mut c_char {
    /**
     * Sets the error message, copying the contents of `val`.
     * If the error has already been set, future calls to `set` are ignored.
     */
    fn set(&self, val: &str) {
        unsafe {
            if **self != null_mut() {
                return;
            }
            if let Ok(cstring) = CString::new(val) {
                **self = cstring.into_raw();
            } else {
                if let Ok(s) = CString::new("Failed setting error message.") {
                    **self = s.into_raw();
                }
            }
        }
    }
}

// TODO: on `T` can I enforce that T has a pointer of name `base` as first item to `vtab`?
pub trait VTabRef {
    fn set<T>(&self, val: Box<T>);
}

impl VTabRef for *mut *mut vtab {
    fn set<T>(&self, val: Box<T>) {
        unsafe {
            let raw_val = Box::into_raw(val);
            **self = raw_val.cast::<sqlite3_capi::vtab>();
        }
    }
}

pub trait CursorRef {
    fn set<T>(&self, val: Box<T>);
}

impl CursorRef for *mut *mut vtab_cursor {
    fn set<T>(&self, val: Box<T>) {
        unsafe {
            let raw_val = Box::into_raw(val);
            **self = raw_val.cast::<sqlite3_capi::vtab_cursor>();
        }
    }
}

pub trait VTab {
    fn set_err(&self, val: &str);
}

impl VTab for *mut vtab {
    /**
     * Sets the error message, copying the contents of `val`.
     * If the error has already been set, future calls to `set` are ignored.
     */
    fn set_err(&self, val: &str) {
        unsafe {
            if (**self).zErrMsg != null_mut() {
                return;
            }
            if let Ok(e) = CString::new(val) {
                (**self).zErrMsg = e.into_raw();
            }
        }
    }
}

// from: https://github.com/asg017/sqlite-loadable-rs/blob/main/src/table.rs#L722
pub struct VTabArgs<'a> {
    /// Name of the module being invoked, the argument in the USING clause.
    /// Example: `"CREATE VIRTUAL TABLE xxx USING custom_vtab"` would have
    /// a `module_name` of `"custom_vtab"`.
    /// Sourced from `argv[0]`
    pub module_name: &'a str,
    /// Name of the database where the virtual table will be created,
    /// typically `"main"` or `"temp"` or another name from an
    /// [`ATTACH`'ed database](https://www.sqlite.org/lang_attach.html).
    /// Sourced from `argv[1]`
    pub database_name: &'a str,

    /// Name of the table being created.
    /// Example: `"CREATE VIRTUAL TABLE xxx USING custom_vtab"` would
    /// have a `table_name` of `"xxx"`.
    /// Sourced from `argv[2]`
    pub table_name: &'a str,
    /// The remaining arguments given in the constructor of the virtual
    /// table, inside `CREATE VIRTUAL TABLE xxx USING custom_vtab(...)`.
    /// Sourced from `argv[3:]`
    pub arguments: Vec<&'a str>,
}

/// Generally do not use this. Does a bunch of copying.
fn c_string_to_str<'a>(c: *const c_char) -> Result<&'a str, Utf8Error> {
    let s = unsafe { CStr::from_ptr(c).to_str()? };
    Ok(s)
}

pub fn parse_vtab_args<'a>(
    argc: c_int,
    argv: *const *const c_char,
) -> Result<VTabArgs<'a>, Utf8Error> {
    let raw_args = unsafe { slice::from_raw_parts(argv, argc as usize) };
    let mut args = Vec::with_capacity(argc as usize);
    for arg in raw_args {
        args.push(c_string_to_str(*arg)?);
    }

    // SQLite guarantees that argv[0-2] will be filled, hence the .expects() -
    // If SQLite is wrong, then may god save our souls
    let module_name = args
        .get(0)
        .expect("argv[0] should be the name of the module");
    let database_name = args
        .get(1)
        .expect("argv[1] should be the name of the database the module is in");
    let table_name = args
        .get(2)
        .expect("argv[2] should be the name of the virtual table");
    let arguments = &args[3..];

    Ok(VTabArgs {
        module_name,
        database_name,
        table_name,
        arguments: arguments.to_vec(),
    })
}

pub fn declare_vtab(db: *mut sqlite3, def: &str) -> Result<ResultCode, ResultCode> {
    let cstring = CString::new(def)?;
    let ret = sqlite3_capi::declare_vtab(db, cstring.as_ptr());
    convert_rc(ret)
}

pub fn vtab_config(db: *mut sqlite3, options: u32) -> Result<ResultCode, ResultCode> {
    let rc = sqlite3_capi::vtab_config(db, options);
    convert_rc(rc)
}

// type xCreateC = extern "C" fn(
//     *mut sqlite3,
//     *mut c_void,
//     c_int,
//     *const *const c_char,
//     *mut *mut vtab,
//     *mut *mut c_char,
// ) -> c_int;

// // return a lambda that invokes f appropriately?
// pub const fn xCreate(
//     f: fn(
//         db: *mut sqlite3,
//         aux: *mut c_void,
//         args: Vec<&str>,
//         tab: *mut *mut vtab,   // declare tab for them?
//         err: *mut *mut c_char, // box?
//     ) -> Result<ResultCode, ResultCode>,
// ) -> xCreateC {
//     move |db, aux, argc, argv, ppvtab, errmsg| match f(db, aux, str_args, ppvtab, errmsg) {
//         Ok(rc) => rc as c_int,
//         Err(rc) => rc as c_int,
//     }
// }

// *mut sqlite3, *mut c_void, Vec<&str>, *mut *mut vtab, *mut *mut c_char
