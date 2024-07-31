#![allow(non_snake_case, non_camel_case_types, clippy::type_complexity)]
#![cfg_attr(test, allow(deref_nullptr))] // https://github.com/rust-lang/rust-bindgen/issues/2066

use std::default::Default;
use std::error;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::os::raw::c_int;

#[cfg(feature = "wasmtime-bindings")]
pub use libsql_wasm::{
    libsql_compile_wasm_module, libsql_free_wasm_module, libsql_run_wasm, libsql_wasm_engine_new,
};

pub use bindgen::*;
mod bindgen {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/bindgen.rs"));
}

#[must_use]
pub fn SQLITE_STATIC() -> sqlite3_destructor_type {
    None
}

#[must_use]
pub fn SQLITE_TRANSIENT() -> sqlite3_destructor_type {
    Some(unsafe { mem::transmute(-1_isize) })
}

impl Default for sqlite3_vtab {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

impl Default for sqlite3_vtab_cursor {
    fn default() -> Self {
        unsafe { mem::zeroed() }
    }
}

pub struct PageHdrIter<'a> {
    current_ptr: *const PgHdr,
    page_size: usize,
    _pth: PhantomData<&'a ()>,
}

impl<'a> PageHdrIter<'a> {
    pub fn new(current_ptr: *const PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
            _pth: PhantomData,
        }
    }

    pub fn current_ptr(&self) -> *const PgHdr {
        self.current_ptr
    }
}

impl<'a> std::iter::Iterator for PageHdrIter<'a> {
    type Item = (u32, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts(current_hdr.pData as *const u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data));
        self.current_ptr = current_hdr.pDirty;
        item
    }
}

pub struct PageHdrIterMut<'a> {
    current_ptr: *mut PgHdr,
    page_size: usize,
    _pth: PhantomData<&'a ()>,
}

impl<'a> PageHdrIterMut<'a> {
    pub fn new(current_ptr: *mut PgHdr, page_size: usize) -> Self {
        Self {
            current_ptr,
            page_size,
            _pth: PhantomData,
        }
    }
}

impl<'a> std::iter::Iterator for PageHdrIterMut<'a> {
    type Item = (u32, &'a mut [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_ptr.is_null() {
            return None;
        }
        let current_hdr: &PgHdr = unsafe { &*self.current_ptr };
        let raw_data =
            unsafe { std::slice::from_raw_parts_mut(current_hdr.pData as *mut u8, self.page_size) };
        let item = Some((current_hdr.pgno, raw_data));
        self.current_ptr = current_hdr.pDirty;
        item
    }
}

/// Error Codes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Internal logic error in SQLite
    InternalMalfunction,
    /// Access permission denied
    PermissionDenied,
    /// Callback routine requested an abort
    OperationAborted,
    /// The database file is locked
    DatabaseBusy,
    /// A table in the database is locked
    DatabaseLocked,
    /// A malloc() failed
    OutOfMemory,
    /// Attempt to write a readonly database
    ReadOnly,
    /// Operation terminated by sqlite3_interrupt()
    OperationInterrupted,
    /// Some kind of disk I/O error occurred
    SystemIoFailure,
    /// The database disk image is malformed
    DatabaseCorrupt,
    /// Unknown opcode in sqlite3_file_control()
    NotFound,
    /// Insertion failed because database is full
    DiskFull,
    /// Unable to open the database file
    CannotOpen,
    /// Database lock protocol error
    FileLockingProtocolFailed,
    /// The database schema changed
    SchemaChanged,
    /// String or BLOB exceeds size limit
    TooBig,
    /// Abort due to constraint violation
    ConstraintViolation,
    /// Data type mismatch
    TypeMismatch,
    /// Library used incorrectly
    ApiMisuse,
    /// Uses OS features not supported on host
    NoLargeFileSupport,
    /// Authorization denied
    AuthorizationForStatementDenied,
    /// 2nd parameter to sqlite3_bind out of range
    ParameterOutOfRange,
    /// File opened that is not a database file
    NotADatabase,
    /// SQL error or missing database
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Error {
    pub code: ErrorCode,
    pub extended_code: c_int,
}

impl Error {
    #[must_use]
    pub fn new(result_code: c_int) -> Error {
        let code = match result_code & 0xff {
            SQLITE_INTERNAL => ErrorCode::InternalMalfunction,
            SQLITE_PERM => ErrorCode::PermissionDenied,
            SQLITE_ABORT => ErrorCode::OperationAborted,
            SQLITE_BUSY => ErrorCode::DatabaseBusy,
            SQLITE_LOCKED => ErrorCode::DatabaseLocked,
            SQLITE_NOMEM => ErrorCode::OutOfMemory,
            SQLITE_READONLY => ErrorCode::ReadOnly,
            SQLITE_INTERRUPT => ErrorCode::OperationInterrupted,
            SQLITE_IOERR => ErrorCode::SystemIoFailure,
            SQLITE_CORRUPT => ErrorCode::DatabaseCorrupt,
            SQLITE_NOTFOUND => ErrorCode::NotFound,
            SQLITE_FULL => ErrorCode::DiskFull,
            SQLITE_CANTOPEN => ErrorCode::CannotOpen,
            SQLITE_PROTOCOL => ErrorCode::FileLockingProtocolFailed,
            SQLITE_SCHEMA => ErrorCode::SchemaChanged,
            SQLITE_TOOBIG => ErrorCode::TooBig,
            SQLITE_CONSTRAINT => ErrorCode::ConstraintViolation,
            SQLITE_MISMATCH => ErrorCode::TypeMismatch,
            SQLITE_MISUSE => ErrorCode::ApiMisuse,
            SQLITE_NOLFS => ErrorCode::NoLargeFileSupport,
            SQLITE_AUTH => ErrorCode::AuthorizationForStatementDenied,
            SQLITE_RANGE => ErrorCode::ParameterOutOfRange,
            SQLITE_NOTADB => ErrorCode::NotADatabase,
            _ => ErrorCode::Unknown,
        };

        Error {
            code,
            extended_code: result_code,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Error code {}: {}",
            self.extended_code,
            code_to_str(self.extended_code)
        )
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        code_to_str(self.extended_code)
    }
}

#[must_use]
pub fn code_to_str(code: c_int) -> &'static str {
    match code {
        SQLITE_OK        => "Successful result",
        SQLITE_ERROR     => "SQL error or missing database",
        SQLITE_INTERNAL  => "Internal logic error in SQLite",
        SQLITE_PERM      => "Access permission denied",
        SQLITE_ABORT     => "Callback routine requested an abort",
        SQLITE_BUSY      => "The database file is locked",
        SQLITE_LOCKED    => "A table in the database is locked",
        SQLITE_NOMEM     => "A malloc() failed",
        SQLITE_READONLY  => "Attempt to write a readonly database",
        SQLITE_INTERRUPT => "Operation terminated by sqlite3_interrupt()",
        SQLITE_IOERR     => "Some kind of disk I/O error occurred",
        SQLITE_CORRUPT   => "The database disk image is malformed",
        SQLITE_NOTFOUND  => "Unknown opcode in sqlite3_file_control()",
        SQLITE_FULL      => "Insertion failed because database is full",
        SQLITE_CANTOPEN  => "Unable to open the database file",
        SQLITE_PROTOCOL  => "Database lock protocol error",
        SQLITE_EMPTY     => "Database is empty",
        SQLITE_SCHEMA    => "The database schema changed",
        SQLITE_TOOBIG    => "String or BLOB exceeds size limit",
        SQLITE_CONSTRAINT=> "Abort due to constraint violation",
        SQLITE_MISMATCH  => "Data type mismatch",
        SQLITE_MISUSE    => "Library used incorrectly",
        SQLITE_NOLFS     => "Uses OS features not supported on host",
        SQLITE_AUTH      => "Authorization denied",
        SQLITE_FORMAT    => "Auxiliary database format error",
        SQLITE_RANGE     => "2nd parameter to sqlite3_bind out of range",
        SQLITE_NOTADB    => "File opened that is not a database file",
        SQLITE_NOTICE    => "Notifications from sqlite3_log()",
        SQLITE_WARNING   => "Warnings from sqlite3_log()",
        SQLITE_ROW       => "sqlite3_step() has another row ready",
        SQLITE_DONE      => "sqlite3_step() has finished executing",

        SQLITE_ERROR_MISSING_COLLSEQ   => "SQLITE_ERROR_MISSING_COLLSEQ",
        SQLITE_ERROR_RETRY   => "SQLITE_ERROR_RETRY",
        SQLITE_ERROR_SNAPSHOT   => "SQLITE_ERROR_SNAPSHOT",

        SQLITE_IOERR_READ              => "Error reading from disk",
        SQLITE_IOERR_SHORT_READ        => "Unable to obtain number of requested bytes (file truncated?)",
        SQLITE_IOERR_WRITE             => "Error writing to disk",
        SQLITE_IOERR_FSYNC             => "Error flushing data to persistent storage (fsync)",
        SQLITE_IOERR_DIR_FSYNC         => "Error calling fsync on a directory",
        SQLITE_IOERR_TRUNCATE          => "Error attempting to truncate file",
        SQLITE_IOERR_FSTAT             => "Error invoking fstat to get file metadata",
        SQLITE_IOERR_UNLOCK            => "I/O error within xUnlock of a VFS object",
        SQLITE_IOERR_RDLOCK            => "I/O error within xLock of a VFS object (trying to obtain a read lock)",
        SQLITE_IOERR_DELETE            => "I/O error within xDelete of a VFS object",
        SQLITE_IOERR_BLOCKED           => "SQLITE_IOERR_BLOCKED", // no longer used
        SQLITE_IOERR_NOMEM             => "Out of memory in I/O layer",
        SQLITE_IOERR_ACCESS            => "I/O error within xAccess of a VFS object",
        SQLITE_IOERR_CHECKRESERVEDLOCK => "I/O error within then xCheckReservedLock method",
        SQLITE_IOERR_LOCK              => "I/O error in the advisory file locking layer",
        SQLITE_IOERR_CLOSE             => "I/O error within the xClose method",
        SQLITE_IOERR_DIR_CLOSE         => "SQLITE_IOERR_DIR_CLOSE", // no longer used
        SQLITE_IOERR_SHMOPEN           => "I/O error within the xShmMap method (trying to open a new shared-memory segment)",
        SQLITE_IOERR_SHMSIZE           => "I/O error within the xShmMap method (trying to resize an existing shared-memory segment)",
        SQLITE_IOERR_SHMLOCK           => "SQLITE_IOERR_SHMLOCK", // no longer used
        SQLITE_IOERR_SHMMAP            => "I/O error within the xShmMap method (trying to map a shared-memory segment into process address space)",
        SQLITE_IOERR_SEEK              => "I/O error within the xRead or xWrite (trying to seek within a file)",
        SQLITE_IOERR_DELETE_NOENT      => "File being deleted does not exist",
        SQLITE_IOERR_MMAP              => "I/O error while trying to map or unmap part of the database file into process address space",
        SQLITE_IOERR_GETTEMPPATH       => "VFS is unable to determine a suitable directory for temporary files",
        SQLITE_IOERR_CONVPATH          => "cygwin_conv_path() system call failed",
        SQLITE_IOERR_VNODE             => "SQLITE_IOERR_VNODE", // not documented?
        SQLITE_IOERR_AUTH              => "SQLITE_IOERR_AUTH",
        SQLITE_IOERR_BEGIN_ATOMIC      => "SQLITE_IOERR_BEGIN_ATOMIC",
        SQLITE_IOERR_COMMIT_ATOMIC     => "SQLITE_IOERR_COMMIT_ATOMIC",
        SQLITE_IOERR_ROLLBACK_ATOMIC   => "SQLITE_IOERR_ROLLBACK_ATOMIC",
        SQLITE_IOERR_DATA              => "SQLITE_IOERR_DATA",

        SQLITE_LOCKED_SHAREDCACHE      => "Locking conflict due to another connection with a shared cache",
        SQLITE_LOCKED_VTAB             => "SQLITE_LOCKED_VTAB",

        SQLITE_BUSY_RECOVERY           => "Another process is recovering a WAL mode database file",
        SQLITE_BUSY_SNAPSHOT           => "Cannot promote read transaction to write transaction because of writes by another connection",
        SQLITE_BUSY_TIMEOUT            => "SQLITE_BUSY_TIMEOUT",

        SQLITE_CANTOPEN_NOTEMPDIR      => "SQLITE_CANTOPEN_NOTEMPDIR", // no longer used
        SQLITE_CANTOPEN_ISDIR          => "Attempted to open directory as file",
        SQLITE_CANTOPEN_FULLPATH       => "Unable to convert filename into full pathname",
        SQLITE_CANTOPEN_CONVPATH       => "cygwin_conv_path() system call failed",
        SQLITE_CANTOPEN_SYMLINK        => "SQLITE_CANTOPEN_SYMLINK",

        SQLITE_CORRUPT_VTAB            => "Content in the virtual table is corrupt",
        SQLITE_CORRUPT_SEQUENCE        => "SQLITE_CORRUPT_SEQUENCE",
        SQLITE_CORRUPT_INDEX           => "SQLITE_CORRUPT_INDEX",

        SQLITE_READONLY_RECOVERY       => "WAL mode database file needs recovery (requires write access)",
        SQLITE_READONLY_CANTLOCK       => "Shared-memory file associated with WAL mode database is read-only",
        SQLITE_READONLY_ROLLBACK       => "Database has hot journal that must be rolled back (requires write access)",
        SQLITE_READONLY_DBMOVED        => "Database cannot be modified because database file has moved",
        SQLITE_READONLY_CANTINIT       => "SQLITE_READONLY_CANTINIT",
        SQLITE_READONLY_DIRECTORY      => "SQLITE_READONLY_DIRECTORY",

        SQLITE_ABORT_ROLLBACK          => "Transaction was rolled back",

        SQLITE_CONSTRAINT_CHECK        => "A CHECK constraint failed",
        SQLITE_CONSTRAINT_COMMITHOOK   => "Commit hook caused rollback",
        SQLITE_CONSTRAINT_FOREIGNKEY   => "Foreign key constraint failed",
        SQLITE_CONSTRAINT_FUNCTION     => "Error returned from extension function",
        SQLITE_CONSTRAINT_NOTNULL      => "A NOT NULL constraint failed",
        SQLITE_CONSTRAINT_PRIMARYKEY   => "A PRIMARY KEY constraint failed",
        SQLITE_CONSTRAINT_TRIGGER      => "A RAISE function within a trigger fired",
        SQLITE_CONSTRAINT_UNIQUE       => "A UNIQUE constraint failed",
        SQLITE_CONSTRAINT_VTAB         => "An application-defined virtual table error occurred",
        SQLITE_CONSTRAINT_ROWID        => "A non-unique rowid occurred",
        SQLITE_CONSTRAINT_PINNED       => "SQLITE_CONSTRAINT_PINNED",
        SQLITE_CONSTRAINT_DATATYPE     => "SQLITE_CONSTRAINT_DATATYPE",

        SQLITE_NOTICE_RECOVER_WAL      => "A WAL mode database file was recovered",
        SQLITE_NOTICE_RECOVER_ROLLBACK => "Hot journal was rolled back",

        SQLITE_WARNING_AUTOINDEX       => "Automatic indexing used - database might benefit from additional indexes",

        SQLITE_AUTH_USER               => "SQLITE_AUTH_USER", // not documented?

        _ => "Unknown error code",
    }
}
