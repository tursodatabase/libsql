#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum Error {
    LibError(std::ffi::c_int),
    Bug(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::LibError(e) => write!(f, "LibError({})", e),
            Self::Bug(e) => write!(f, "Bug({})", e),
        }
    }
}

impl From<i32> for Error {
    fn from(e: i32) -> Self {
        Self::LibError(e as std::ffi::c_int)
    }
}

impl From<u32> for Error {
    fn from(e: u32) -> Self {
        Self::LibError(e as std::ffi::c_int)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub mod libsql {
    use std::error;
    use std::fmt;
    use std::os::raw::c_int;

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
                crate::ffi::SQLITE_INTERNAL => ErrorCode::InternalMalfunction,
                crate::ffi::SQLITE_PERM => ErrorCode::PermissionDenied,
                crate::ffi::SQLITE_ABORT => ErrorCode::OperationAborted,
                crate::ffi::SQLITE_BUSY => ErrorCode::DatabaseBusy,
                crate::ffi::SQLITE_LOCKED => ErrorCode::DatabaseLocked,
                crate::ffi::SQLITE_NOMEM => ErrorCode::OutOfMemory,
                crate::ffi::SQLITE_READONLY => ErrorCode::ReadOnly,
                crate::ffi::SQLITE_INTERRUPT => ErrorCode::OperationInterrupted,
                crate::ffi::SQLITE_IOERR => ErrorCode::SystemIoFailure,
                crate::ffi::SQLITE_CORRUPT => ErrorCode::DatabaseCorrupt,
                crate::ffi::SQLITE_NOTFOUND => ErrorCode::NotFound,
                crate::ffi::SQLITE_FULL => ErrorCode::DiskFull,
                crate::ffi::SQLITE_CANTOPEN => ErrorCode::CannotOpen,
                crate::ffi::SQLITE_PROTOCOL => ErrorCode::FileLockingProtocolFailed,
                crate::ffi::SQLITE_SCHEMA => ErrorCode::SchemaChanged,
                crate::ffi::SQLITE_TOOBIG => ErrorCode::TooBig,
                crate::ffi::SQLITE_CONSTRAINT => ErrorCode::ConstraintViolation,
                crate::ffi::SQLITE_MISMATCH => ErrorCode::TypeMismatch,
                crate::ffi::SQLITE_MISUSE => ErrorCode::ApiMisuse,
                crate::ffi::SQLITE_NOLFS => ErrorCode::NoLargeFileSupport,
                crate::ffi::SQLITE_AUTH => ErrorCode::AuthorizationForStatementDenied,
                crate::ffi::SQLITE_RANGE => ErrorCode::ParameterOutOfRange,
                crate::ffi::SQLITE_NOTADB => ErrorCode::NotADatabase,
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

    // Result codes.
    // Note: These are not public because our bindgen bindings export whichever
    // constants are present in the current version of SQLite. We repeat them here
    // so we don't have to worry about which version of SQLite added which
    // constants, and we only use them to implement code_to_str below.

    // Extended result codes.

    const SQLITE_ERROR_MISSING_COLLSEQ: c_int = crate::ffi::SQLITE_ERROR | (1 << 8);
    const SQLITE_ERROR_RETRY: c_int = crate::ffi::SQLITE_ERROR | (2 << 8);
    const SQLITE_ERROR_SNAPSHOT: c_int = crate::ffi::SQLITE_ERROR | (3 << 8);

    const SQLITE_IOERR_BEGIN_ATOMIC: c_int = crate::ffi::SQLITE_IOERR | (29 << 8);
    const SQLITE_IOERR_COMMIT_ATOMIC: c_int = crate::ffi::SQLITE_IOERR | (30 << 8);
    const SQLITE_IOERR_ROLLBACK_ATOMIC: c_int = crate::ffi::SQLITE_IOERR | (31 << 8);
    const SQLITE_IOERR_DATA: c_int = crate::ffi::SQLITE_IOERR | (32 << 8);

    const SQLITE_LOCKED_VTAB: c_int = crate::ffi::SQLITE_LOCKED | (2 << 8);

    const SQLITE_BUSY_TIMEOUT: c_int = crate::ffi::SQLITE_BUSY | (3 << 8);

    const SQLITE_CANTOPEN_SYMLINK: c_int = crate::ffi::SQLITE_CANTOPEN | (6 << 8);

    const SQLITE_CORRUPT_SEQUENCE: c_int = crate::ffi::SQLITE_CORRUPT | (2 << 8);
    const SQLITE_CORRUPT_INDEX: c_int = crate::ffi::SQLITE_CORRUPT | (3 << 8);

    const SQLITE_READONLY_CANTINIT: c_int = crate::ffi::SQLITE_READONLY | (5 << 8);
    const SQLITE_READONLY_DIRECTORY: c_int = crate::ffi::SQLITE_READONLY | (6 << 8);

    const SQLITE_CONSTRAINT_PINNED: c_int = crate::ffi::SQLITE_CONSTRAINT | (11 << 8);
    const SQLITE_CONSTRAINT_DATATYPE: c_int = crate::ffi::SQLITE_CONSTRAINT | (12 << 8);

    #[must_use]
    pub fn code_to_str(code: c_int) -> &'static str {
        match code {
            crate::ffi::SQLITE_OK        => "Successful result",
            crate::ffi::SQLITE_ERROR     => "SQL error or missing database",
            crate::ffi::SQLITE_INTERNAL  => "Internal logic error in SQLite",
            crate::ffi::SQLITE_PERM      => "Access permission denied",
            crate::ffi::SQLITE_ABORT     => "Callback routine requested an abort",
            crate::ffi::SQLITE_BUSY      => "The database file is locked",
            crate::ffi::SQLITE_LOCKED    => "A table in the database is locked",
            crate::ffi::SQLITE_NOMEM     => "A malloc() failed",
            crate::ffi::SQLITE_READONLY  => "Attempt to write a readonly database",
            crate::ffi::SQLITE_INTERRUPT => "Operation terminated by sqlite3_interrupt()",
            crate::ffi::SQLITE_IOERR     => "Some kind of disk I/O error occurred",
            crate::ffi::SQLITE_CORRUPT   => "The database disk image is malformed",
            crate::ffi::SQLITE_NOTFOUND  => "Unknown opcode in sqlite3_file_control()",
            crate::ffi::SQLITE_FULL      => "Insertion failed because database is full",
            crate::ffi::SQLITE_CANTOPEN  => "Unable to open the database file",
            crate::ffi::SQLITE_PROTOCOL  => "Database lock protocol error",
            crate::ffi::SQLITE_EMPTY     => "Database is empty",
            crate::ffi::SQLITE_SCHEMA    => "The database schema changed",
            crate::ffi::SQLITE_TOOBIG    => "String or BLOB exceeds size limit",
            crate::ffi::SQLITE_CONSTRAINT=> "Abort due to constraint violation",
            crate::ffi::SQLITE_MISMATCH  => "Data type mismatch",
            crate::ffi::SQLITE_MISUSE    => "Library used incorrectly",
            crate::ffi::SQLITE_NOLFS     => "Uses OS features not supported on host",
            crate::ffi::SQLITE_AUTH      => "Authorization denied",
            crate::ffi::SQLITE_FORMAT    => "Auxiliary database format error",
            crate::ffi::SQLITE_RANGE     => "2nd parameter to sqlite3_bind out of range",
            crate::ffi::SQLITE_NOTADB    => "File opened that is not a database file",
            crate::ffi::SQLITE_NOTICE    => "Notifications from sqlite3_log()",
            crate::ffi::SQLITE_WARNING   => "Warnings from sqlite3_log()",
            crate::ffi::SQLITE_ROW       => "sqlite3_step() has another row ready",
            crate::ffi::SQLITE_DONE      => "sqlite3_step() has finished executing",

            SQLITE_ERROR_MISSING_COLLSEQ   => "SQLITE_ERROR_MISSING_COLLSEQ",
            SQLITE_ERROR_RETRY   => "SQLITE_ERROR_RETRY",
            SQLITE_ERROR_SNAPSHOT   => "SQLITE_ERROR_SNAPSHOT",

            crate::ffi::SQLITE_IOERR_READ              => "Error reading from disk",
            crate::ffi::SQLITE_IOERR_SHORT_READ        => "Unable to obtain number of requested bytes (file truncated?)",
            crate::ffi::SQLITE_IOERR_WRITE             => "Error writing to disk",
            crate::ffi::SQLITE_IOERR_FSYNC             => "Error flushing data to persistent storage (fsync)",
            crate::ffi::SQLITE_IOERR_DIR_FSYNC         => "Error calling fsync on a directory",
            crate::ffi::SQLITE_IOERR_TRUNCATE          => "Error attempting to truncate file",
            crate::ffi::SQLITE_IOERR_FSTAT             => "Error invoking fstat to get file metadata",
            crate::ffi::SQLITE_IOERR_UNLOCK            => "I/O error within xUnlock of a VFS object",
            crate::ffi::SQLITE_IOERR_RDLOCK            => "I/O error within xLock of a VFS object (trying to obtain a read lock)",
            crate::ffi::SQLITE_IOERR_DELETE            => "I/O error within xDelete of a VFS object",
            crate::ffi::SQLITE_IOERR_BLOCKED           => "SQLITE_IOERR_BLOCKED", // no longer used
            crate::ffi::SQLITE_IOERR_NOMEM             => "Out of memory in I/O layer",
            crate::ffi::SQLITE_IOERR_ACCESS            => "I/O error within xAccess of a VFS object",
            crate::ffi::SQLITE_IOERR_CHECKRESERVEDLOCK => "I/O error within then xCheckReservedLock method",
            crate::ffi::SQLITE_IOERR_LOCK              => "I/O error in the advisory file locking layer",
            crate::ffi::SQLITE_IOERR_CLOSE             => "I/O error within the xClose method",
            crate::ffi::SQLITE_IOERR_DIR_CLOSE         => "SQLITE_IOERR_DIR_CLOSE", // no longer used
            crate::ffi::SQLITE_IOERR_SHMOPEN           => "I/O error within the xShmMap method (trying to open a new shared-memory segment)",
            crate::ffi::SQLITE_IOERR_SHMSIZE           => "I/O error within the xShmMap method (trying to resize an existing shared-memory segment)",
            crate::ffi::SQLITE_IOERR_SHMLOCK           => "SQLITE_IOERR_SHMLOCK", // no longer used
            crate::ffi::SQLITE_IOERR_SHMMAP            => "I/O error within the xShmMap method (trying to map a shared-memory segment into process address space)",
            crate::ffi::SQLITE_IOERR_SEEK              => "I/O error within the xRead or xWrite (trying to seek within a file)",
            crate::ffi::SQLITE_IOERR_DELETE_NOENT      => "File being deleted does not exist",
            crate::ffi::SQLITE_IOERR_MMAP              => "I/O error while trying to map or unmap part of the database file into process address space",
            crate::ffi::SQLITE_IOERR_GETTEMPPATH       => "VFS is unable to determine a suitable directory for temporary files",
            crate::ffi::SQLITE_IOERR_CONVPATH          => "cygwin_conv_path() system call failed",
            crate::ffi::SQLITE_IOERR_VNODE             => "SQLITE_IOERR_VNODE", // not documented?
            crate::ffi::SQLITE_IOERR_AUTH              => "SQLITE_IOERR_AUTH",
            SQLITE_IOERR_BEGIN_ATOMIC      => "SQLITE_IOERR_BEGIN_ATOMIC",
            SQLITE_IOERR_COMMIT_ATOMIC     => "SQLITE_IOERR_COMMIT_ATOMIC",
            SQLITE_IOERR_ROLLBACK_ATOMIC   => "SQLITE_IOERR_ROLLBACK_ATOMIC",
            SQLITE_IOERR_DATA   => "SQLITE_IOERR_DATA",

            crate::ffi::SQLITE_LOCKED_SHAREDCACHE      => "Locking conflict due to another connection with a shared cache",
            SQLITE_LOCKED_VTAB             => "SQLITE_LOCKED_VTAB",

            crate::ffi::SQLITE_BUSY_RECOVERY           => "Another process is recovering a WAL mode database file",
            crate::ffi::SQLITE_BUSY_SNAPSHOT           => "Cannot promote read transaction to write transaction because of writes by another connection",
            SQLITE_BUSY_TIMEOUT           => "SQLITE_BUSY_TIMEOUT",

            crate::ffi::SQLITE_CANTOPEN_NOTEMPDIR      => "SQLITE_CANTOPEN_NOTEMPDIR", // no longer used
            crate::ffi::SQLITE_CANTOPEN_ISDIR          => "Attempted to open directory as file",
            crate::ffi::SQLITE_CANTOPEN_FULLPATH       => "Unable to convert filename into full pathname",
            crate::ffi::SQLITE_CANTOPEN_CONVPATH       => "cygwin_conv_path() system call failed",
            SQLITE_CANTOPEN_SYMLINK       => "SQLITE_CANTOPEN_SYMLINK",

            crate::ffi::SQLITE_CORRUPT_VTAB            => "Content in the virtual table is corrupt",
            SQLITE_CORRUPT_SEQUENCE        => "SQLITE_CORRUPT_SEQUENCE",
            SQLITE_CORRUPT_INDEX        => "SQLITE_CORRUPT_INDEX",

            crate::ffi::SQLITE_READONLY_RECOVERY       => "WAL mode database file needs recovery (requires write access)",
            crate::ffi::SQLITE_READONLY_CANTLOCK       => "Shared-memory file associated with WAL mode database is read-only",
            crate::ffi::SQLITE_READONLY_ROLLBACK       => "Database has hot journal that must be rolled back (requires write access)",
            crate::ffi::SQLITE_READONLY_DBMOVED        => "Database cannot be modified because database file has moved",
            SQLITE_READONLY_CANTINIT       => "SQLITE_READONLY_CANTINIT",
            SQLITE_READONLY_DIRECTORY      => "SQLITE_READONLY_DIRECTORY",

            crate::ffi::SQLITE_ABORT_ROLLBACK          => "Transaction was rolled back",

            crate::ffi::SQLITE_CONSTRAINT_CHECK        => "A CHECK constraint failed",
            crate::ffi::SQLITE_CONSTRAINT_COMMITHOOK   => "Commit hook caused rollback",
            crate::ffi::SQLITE_CONSTRAINT_FOREIGNKEY   => "Foreign key constraint failed",
            crate::ffi::SQLITE_CONSTRAINT_FUNCTION     => "Error returned from extension function",
            crate::ffi::SQLITE_CONSTRAINT_NOTNULL      => "A NOT NULL constraint failed",
            crate::ffi::SQLITE_CONSTRAINT_PRIMARYKEY   => "A PRIMARY KEY constraint failed",
            crate::ffi::SQLITE_CONSTRAINT_TRIGGER      => "A RAISE function within a trigger fired",
            crate::ffi::SQLITE_CONSTRAINT_UNIQUE       => "A UNIQUE constraint failed",
            crate::ffi::SQLITE_CONSTRAINT_VTAB         => "An application-defined virtual table error occurred",
            crate::ffi::SQLITE_CONSTRAINT_ROWID        => "A non-unique rowid occurred",
            SQLITE_CONSTRAINT_PINNED        => "SQLITE_CONSTRAINT_PINNED",
            SQLITE_CONSTRAINT_DATATYPE        => "SQLITE_CONSTRAINT_DATATYPE",

            crate::ffi::SQLITE_NOTICE_RECOVER_WAL      => "A WAL mode database file was recovered",
            crate::ffi::SQLITE_NOTICE_RECOVER_ROLLBACK => "Hot journal was rolled back",

            crate::ffi::SQLITE_WARNING_AUTOINDEX       => "Automatic indexing used - database might benefit from additional indexes",

            crate::ffi::SQLITE_AUTH_USER               => "SQLITE_AUTH_USER", // not documented?

            _ => "Unknown error code",
        }
    }
}
