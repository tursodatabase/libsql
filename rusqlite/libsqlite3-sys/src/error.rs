use std::error;
use std::fmt;
use std::os::raw::c_int;

/// Error Codes
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    SystemIOFailure,
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
    APIMisuse,
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
    pub fn new(result_code: c_int) -> Error {
        let code = match result_code & 0xff {
            super::SQLITE_INTERNAL => ErrorCode::InternalMalfunction,
            super::SQLITE_PERM => ErrorCode::PermissionDenied,
            super::SQLITE_ABORT => ErrorCode::OperationAborted,
            super::SQLITE_BUSY => ErrorCode::DatabaseBusy,
            super::SQLITE_LOCKED => ErrorCode::DatabaseLocked,
            super::SQLITE_NOMEM => ErrorCode::OutOfMemory,
            super::SQLITE_READONLY => ErrorCode::ReadOnly,
            super::SQLITE_INTERRUPT => ErrorCode::OperationInterrupted,
            super::SQLITE_IOERR => ErrorCode::SystemIOFailure,
            super::SQLITE_CORRUPT => ErrorCode::DatabaseCorrupt,
            super::SQLITE_NOTFOUND => ErrorCode::NotFound,
            super::SQLITE_FULL => ErrorCode::DiskFull,
            super::SQLITE_CANTOPEN => ErrorCode::CannotOpen,
            super::SQLITE_PROTOCOL => ErrorCode::FileLockingProtocolFailed,
            super::SQLITE_SCHEMA => ErrorCode::SchemaChanged,
            super::SQLITE_TOOBIG => ErrorCode::TooBig,
            super::SQLITE_CONSTRAINT => ErrorCode::ConstraintViolation,
            super::SQLITE_MISMATCH => ErrorCode::TypeMismatch,
            super::SQLITE_MISUSE => ErrorCode::APIMisuse,
            super::SQLITE_NOLFS => ErrorCode::NoLargeFileSupport,
            super::SQLITE_AUTH => ErrorCode::AuthorizationForStatementDenied,
            super::SQLITE_RANGE => ErrorCode::ParameterOutOfRange,
            super::SQLITE_NOTADB => ErrorCode::NotADatabase,
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

const SQLITE_NOTICE: c_int = 27;
const SQLITE_WARNING: c_int = 28;

// Extended result codes.

const SQLITE_ERROR_MISSING_COLLSEQ: c_int = super::SQLITE_ERROR | (1 << 8);
const SQLITE_ERROR_RETRY: c_int = super::SQLITE_ERROR | (2 << 8);
const SQLITE_ERROR_SNAPSHOT: c_int = super::SQLITE_ERROR | (3 << 8);

const SQLITE_IOERR_SHMOPEN: c_int = super::SQLITE_IOERR | (18 << 8);
const SQLITE_IOERR_SHMSIZE: c_int = super::SQLITE_IOERR | (19 << 8);
const SQLITE_IOERR_SHMLOCK: c_int = super::SQLITE_IOERR | (20 << 8);
const SQLITE_IOERR_SHMMAP: c_int = super::SQLITE_IOERR | (21 << 8);
const SQLITE_IOERR_SEEK: c_int = super::SQLITE_IOERR | (22 << 8);
const SQLITE_IOERR_DELETE_NOENT: c_int = super::SQLITE_IOERR | (23 << 8);
const SQLITE_IOERR_MMAP: c_int = super::SQLITE_IOERR | (24 << 8);
const SQLITE_IOERR_GETTEMPPATH: c_int = super::SQLITE_IOERR | (25 << 8);
const SQLITE_IOERR_CONVPATH: c_int = super::SQLITE_IOERR | (26 << 8);
const SQLITE_IOERR_VNODE: c_int = super::SQLITE_IOERR | (27 << 8);
const SQLITE_IOERR_AUTH: c_int = super::SQLITE_IOERR | (28 << 8);
const SQLITE_IOERR_BEGIN_ATOMIC: c_int = super::SQLITE_IOERR | (29 << 8);
const SQLITE_IOERR_COMMIT_ATOMIC: c_int = super::SQLITE_IOERR | (30 << 8);
const SQLITE_IOERR_ROLLBACK_ATOMIC: c_int = super::SQLITE_IOERR | (31 << 8);

const SQLITE_LOCKED_SHAREDCACHE: c_int = super::SQLITE_LOCKED | (1 << 8);
const SQLITE_LOCKED_VTAB: c_int = super::SQLITE_LOCKED | (2 << 8);

const SQLITE_BUSY_RECOVERY: c_int = super::SQLITE_BUSY | (1 << 8);
const SQLITE_BUSY_SNAPSHOT: c_int = super::SQLITE_BUSY | (2 << 8);

const SQLITE_CANTOPEN_NOTEMPDIR: c_int = super::SQLITE_CANTOPEN | (1 << 8);
const SQLITE_CANTOPEN_ISDIR: c_int = super::SQLITE_CANTOPEN | (2 << 8);
const SQLITE_CANTOPEN_FULLPATH: c_int = super::SQLITE_CANTOPEN | (3 << 8);
const SQLITE_CANTOPEN_CONVPATH: c_int = super::SQLITE_CANTOPEN | (4 << 8);
const SQLITE_CANTOPEN_SYMLINK: c_int = super::SQLITE_CANTOPEN | (6 << 8);

const SQLITE_CORRUPT_VTAB: c_int = super::SQLITE_CORRUPT | (1 << 8);
const SQLITE_CORRUPT_SEQUENCE: c_int = super::SQLITE_CORRUPT | (2 << 8);

const SQLITE_READONLY_RECOVERY: c_int = super::SQLITE_READONLY | (1 << 8);
const SQLITE_READONLY_CANTLOCK: c_int = super::SQLITE_READONLY | (2 << 8);
const SQLITE_READONLY_ROLLBACK: c_int = super::SQLITE_READONLY | (3 << 8);
const SQLITE_READONLY_DBMOVED: c_int = super::SQLITE_READONLY | (4 << 8);
const SQLITE_READONLY_CANTINIT: c_int = super::SQLITE_READONLY | (5 << 8);
const SQLITE_READONLY_DIRECTORY: c_int = super::SQLITE_READONLY | (6 << 8);

const SQLITE_ABORT_ROLLBACK: c_int = super::SQLITE_ABORT | (2 << 8);

const SQLITE_CONSTRAINT_CHECK: c_int = super::SQLITE_CONSTRAINT | (1 << 8);
const SQLITE_CONSTRAINT_COMMITHOOK: c_int = super::SQLITE_CONSTRAINT | (2 << 8);
const SQLITE_CONSTRAINT_FOREIGNKEY: c_int = super::SQLITE_CONSTRAINT | (3 << 8);
const SQLITE_CONSTRAINT_FUNCTION: c_int = super::SQLITE_CONSTRAINT | (4 << 8);
const SQLITE_CONSTRAINT_NOTNULL: c_int = super::SQLITE_CONSTRAINT | (5 << 8);
const SQLITE_CONSTRAINT_PRIMARYKEY: c_int = super::SQLITE_CONSTRAINT | (6 << 8);
const SQLITE_CONSTRAINT_TRIGGER: c_int = super::SQLITE_CONSTRAINT | (7 << 8);
const SQLITE_CONSTRAINT_UNIQUE: c_int = super::SQLITE_CONSTRAINT | (8 << 8);
const SQLITE_CONSTRAINT_VTAB: c_int = super::SQLITE_CONSTRAINT | (9 << 8);
const SQLITE_CONSTRAINT_ROWID: c_int = super::SQLITE_CONSTRAINT | (10 << 8);
const SQLITE_CONSTRAINT_PINNED: c_int = super::SQLITE_CONSTRAINT | (11 << 8);

const SQLITE_NOTICE_RECOVER_WAL: c_int = SQLITE_NOTICE | (1 << 8);
const SQLITE_NOTICE_RECOVER_ROLLBACK: c_int = SQLITE_NOTICE | (2 << 8);

const SQLITE_WARNING_AUTOINDEX: c_int = SQLITE_WARNING | (1 << 8);

const SQLITE_AUTH_USER: c_int = super::SQLITE_AUTH | (1 << 8);

pub fn code_to_str(code: c_int) -> &'static str {
    match code {
        super::SQLITE_OK        => "Successful result",
        super::SQLITE_ERROR     => "SQL error or missing database",
        super::SQLITE_INTERNAL  => "Internal logic error in SQLite",
        super::SQLITE_PERM      => "Access permission denied",
        super::SQLITE_ABORT     => "Callback routine requested an abort",
        super::SQLITE_BUSY      => "The database file is locked",
        super::SQLITE_LOCKED    => "A table in the database is locked",
        super::SQLITE_NOMEM     => "A malloc() failed",
        super::SQLITE_READONLY  => "Attempt to write a readonly database",
        super::SQLITE_INTERRUPT => "Operation terminated by sqlite3_interrupt()",
        super::SQLITE_IOERR     => "Some kind of disk I/O error occurred",
        super::SQLITE_CORRUPT   => "The database disk image is malformed",
        super::SQLITE_NOTFOUND  => "Unknown opcode in sqlite3_file_control()",
        super::SQLITE_FULL      => "Insertion failed because database is full",
        super::SQLITE_CANTOPEN  => "Unable to open the database file",
        super::SQLITE_PROTOCOL  => "Database lock protocol error",
        super::SQLITE_EMPTY     => "Database is empty",
        super::SQLITE_SCHEMA    => "The database schema changed",
        super::SQLITE_TOOBIG    => "String or BLOB exceeds size limit",
        super::SQLITE_CONSTRAINT=> "Abort due to constraint violation",
        super::SQLITE_MISMATCH  => "Data type mismatch",
        super::SQLITE_MISUSE    => "Library used incorrectly",
        super::SQLITE_NOLFS     => "Uses OS features not supported on host",
        super::SQLITE_AUTH      => "Authorization denied",
        super::SQLITE_FORMAT    => "Auxiliary database format error",
        super::SQLITE_RANGE     => "2nd parameter to sqlite3_bind out of range",
        super::SQLITE_NOTADB    => "File opened that is not a database file",
        SQLITE_NOTICE    => "Notifications from sqlite3_log()",
        SQLITE_WARNING   => "Warnings from sqlite3_log()",
        super::SQLITE_ROW       => "sqlite3_step() has another row ready",
        super::SQLITE_DONE      => "sqlite3_step() has finished executing",

        SQLITE_ERROR_MISSING_COLLSEQ   => "SQLITE_ERROR_MISSING_COLLSEQ",
        SQLITE_ERROR_RETRY   => "SQLITE_ERROR_RETRY",
        SQLITE_ERROR_SNAPSHOT   => "SQLITE_ERROR_SNAPSHOT",

        super::SQLITE_IOERR_READ              => "Error reading from disk",
        super::SQLITE_IOERR_SHORT_READ        => "Unable to obtain number of requested bytes (file truncated?)",
        super::SQLITE_IOERR_WRITE             => "Error writing to disk",
        super::SQLITE_IOERR_FSYNC             => "Error flushing data to persistent storage (fsync)",
        super::SQLITE_IOERR_DIR_FSYNC         => "Error calling fsync on a directory",
        super::SQLITE_IOERR_TRUNCATE          => "Error attempting to truncate file",
        super::SQLITE_IOERR_FSTAT             => "Error invoking fstat to get file metadata",
        super::SQLITE_IOERR_UNLOCK            => "I/O error within xUnlock of a VFS object",
        super::SQLITE_IOERR_RDLOCK            => "I/O error within xLock of a VFS object (trying to obtain a read lock)",
        super::SQLITE_IOERR_DELETE            => "I/O error within xDelete of a VFS object",
        super::SQLITE_IOERR_BLOCKED           => "SQLITE_IOERR_BLOCKED", // no longer used
        super::SQLITE_IOERR_NOMEM             => "Out of memory in I/O layer",
        super::SQLITE_IOERR_ACCESS            => "I/O error within xAccess of a VFS object",
        super::SQLITE_IOERR_CHECKRESERVEDLOCK => "I/O error within then xCheckReservedLock method",
        super::SQLITE_IOERR_LOCK              => "I/O error in the advisory file locking layer",
        super::SQLITE_IOERR_CLOSE             => "I/O error within the xClose method",
        super::SQLITE_IOERR_DIR_CLOSE         => "SQLITE_IOERR_DIR_CLOSE", // no longer used
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

        SQLITE_LOCKED_SHAREDCACHE      => "Locking conflict due to another connection with a shared cache",
        SQLITE_LOCKED_VTAB             => "SQLITE_LOCKED_VTAB",

        SQLITE_BUSY_RECOVERY           => "Another process is recovering a WAL mode database file",
        SQLITE_BUSY_SNAPSHOT           => "Cannot promote read transaction to write transaction because of writes by another connection",

        SQLITE_CANTOPEN_NOTEMPDIR      => "SQLITE_CANTOPEN_NOTEMPDIR", // no longer used
        SQLITE_CANTOPEN_ISDIR          => "Attempted to open directory as file",
        SQLITE_CANTOPEN_FULLPATH       => "Unable to convert filename into full pathname",
        SQLITE_CANTOPEN_CONVPATH       => "cygwin_conv_path() system call failed",
        SQLITE_CANTOPEN_SYMLINK       => "SQLITE_CANTOPEN_SYMLINK",

        SQLITE_CORRUPT_VTAB            => "Content in the virtual table is corrupt",
        SQLITE_CORRUPT_SEQUENCE        => "SQLITE_CORRUPT_SEQUENCE",

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
        SQLITE_CONSTRAINT_PINNED        => "SQLITE_CONSTRAINT_PINNED",

        SQLITE_NOTICE_RECOVER_WAL      => "A WAL mode database file was recovered",
        SQLITE_NOTICE_RECOVER_ROLLBACK => "Hot journal was rolled back",

        SQLITE_WARNING_AUTOINDEX       => "Automatic indexing used - database might benefit from additional indexes",

        SQLITE_AUTH_USER               => "SQLITE_AUTH_USER", // not documented?

        _ => "Unknown error code",
    }
}
