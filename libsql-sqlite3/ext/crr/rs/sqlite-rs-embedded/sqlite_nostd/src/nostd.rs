extern crate alloc;

use alloc::ffi::IntoStringError;
use alloc::vec::Vec;
use alloc::{ffi::CString, string::String};
use core::ffi::{c_char, c_int, c_void};

#[cfg(not(feature = "std"))]
use num_derive::FromPrimitive;
#[cfg(not(feature = "std"))]
use num_traits::FromPrimitive;

pub use sqlite3_allocator::*;
pub use sqlite3_capi::*;

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

pub struct ManagedConnection {
    db: *mut sqlite3,
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

    #[inline]
    fn next_stmt(&self, s: Option<*mut stmt>) -> Option<*mut stmt> {
        self.db.next_stmt(s)
    }

    #[inline]
    fn prepare_v2(&self, sql: &str) -> Result<ManagedStmt, ResultCode> {
        self.db.prepare_v2(sql)
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

    fn errmsg(&self) -> Result<String, IntoStringError> {
        errmsg(*self).into_string()
    }

    fn errcode(&self) -> ResultCode {
        ResultCode::from_i32(errcode(*self)).unwrap()
    }
}

fn convert_rc(rc: i32) -> Result<ResultCode, ResultCode> {
    let rc = ResultCode::from_i32(rc).unwrap();
    if rc == ResultCode::OK {
        Ok(rc)
    } else {
        Err(rc)
    }
}

pub struct ManagedStmt {
    stmt: *mut stmt,
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
        Ok(column_text(self.stmt, i))
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
    fn result_text_static(&self, text: &'static str);
    fn result_blob_owned(&self, blob: Vec<u8>);
    fn result_blob_shared(&self, blob: &[u8]);
    fn result_blob_static(&self, blob: &'static [u8]);
    fn result_error(&self, text: &str);
    fn result_error_code(&self, code: ResultCode);
    fn result_null(&self);
    fn db_handle(&self) -> *mut sqlite3;
}

impl Context for *mut context {
    #[inline]
    fn result_null(&self) {
        result_null(*self)
    }

    /// TODO: do not use this right now! The drop is not dropping according to valgrind.
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

    /// Takes a reference to a string that is statically allocated.
    /// SQLite will not copy this string.
    #[inline]
    fn result_text_static(&self, text: &'static str) {
        result_text(
            *self,
            text.as_ptr() as *mut c_char,
            text.len() as i32,
            Destructor::STATIC,
        );
    }

    /// Passes ownership of the blob to SQLite without copying.
    /// SQLite will drop the blob when it is finished with it.
    #[inline]
    fn result_blob_owned(&self, blob: Vec<u8>) {
        let (ptr, len, _) = blob.into_raw_parts();
        result_blob(*self, ptr, len as i32, Destructor::CUSTOM(droprust));
    }

    #[inline]
    fn result_blob_shared(&self, blob: &[u8]) {
        result_blob(
            *self,
            blob.as_ptr(),
            blob.len() as i32,
            Destructor::TRANSIENT,
        );
    }

    #[inline]
    fn result_blob_static(&self, blob: &'static [u8]) {
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
    fn db_handle(&self) -> *mut sqlite3 {
        context_db_handle(*self)
    }
}

pub trait Stmt {
    fn sql(&self) -> &str;
}

impl Stmt for *mut stmt {
    fn sql(&self) -> &str {
        unsafe { core::str::from_utf8_unchecked(core::ffi::CStr::from_ptr(sql(*self)).to_bytes()) }
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
