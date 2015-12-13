use libc::c_int;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    InternalMalfunction,
    PermissionDenied,
    OperationAborted,
    DatabaseBusy,
    DatabaseLocked,
    OutOfMemory,
    ReadOnly,
    OperationInterrupted,
    SystemIOFailure,
    DatabaseCorrupt,
    NotFound,
    DiskFull,
    CannotOpen,
    FileLockingProtocolFailed,
    SchemaChanged,
    TooBig,
    ConstraintViolation,
    TypeMismatch,
    APIMisuse,
    NoLargeFileSupport,
    AuthorizationForStatementDenied,
    ParameterOutOfRange,
    NotADatabase,
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
            SQLITE_INTERNAL  => ErrorCode::InternalMalfunction,
            SQLITE_PERM      => ErrorCode::PermissionDenied,
            SQLITE_ABORT     => ErrorCode::OperationAborted,
            SQLITE_BUSY      => ErrorCode::DatabaseBusy,
            SQLITE_LOCKED    => ErrorCode::DatabaseLocked,
            SQLITE_NOMEM     => ErrorCode::OutOfMemory,
            SQLITE_READONLY  => ErrorCode::ReadOnly,
            SQLITE_INTERRUPT => ErrorCode::OperationInterrupted,
            SQLITE_IOERR     => ErrorCode::SystemIOFailure,
            SQLITE_CORRUPT   => ErrorCode::DatabaseCorrupt,
            SQLITE_NOTFOUND  => ErrorCode::NotFound,
            SQLITE_FULL      => ErrorCode::DiskFull,
            SQLITE_CANTOPEN  => ErrorCode::CannotOpen,
            SQLITE_PROTOCOL  => ErrorCode::FileLockingProtocolFailed,
            SQLITE_SCHEMA    => ErrorCode::SchemaChanged,
            SQLITE_TOOBIG    => ErrorCode::TooBig,
            SQLITE_CONSTRAINT=> ErrorCode::ConstraintViolation,
            SQLITE_MISMATCH  => ErrorCode::TypeMismatch,
            SQLITE_MISUSE    => ErrorCode::APIMisuse,
            SQLITE_NOLFS     => ErrorCode::NoLargeFileSupport,
            SQLITE_AUTH      => ErrorCode::AuthorizationForStatementDenied,
            SQLITE_RANGE     => ErrorCode::ParameterOutOfRange,
            SQLITE_NOTADB    => ErrorCode::NotADatabase,
            _                => ErrorCode::Unknown,
        };

        Error {
            code: code,
            extended_code: result_code,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error code {}: {}", self.extended_code, code_to_str(self.extended_code))
    }
}

// Result codes.

pub const SQLITE_OK        : c_int =   0;
pub const SQLITE_ERROR     : c_int =   1;
pub const SQLITE_INTERNAL  : c_int =   2;
pub const SQLITE_PERM      : c_int =   3;
pub const SQLITE_ABORT     : c_int =   4;
pub const SQLITE_BUSY      : c_int =   5;
pub const SQLITE_LOCKED    : c_int =   6;
pub const SQLITE_NOMEM     : c_int =   7;
pub const SQLITE_READONLY  : c_int =   8;
pub const SQLITE_INTERRUPT : c_int =   9;
pub const SQLITE_IOERR     : c_int =  10;
pub const SQLITE_CORRUPT   : c_int =  11;
pub const SQLITE_NOTFOUND  : c_int =  12;
pub const SQLITE_FULL      : c_int =  13;
pub const SQLITE_CANTOPEN  : c_int =  14;
pub const SQLITE_PROTOCOL  : c_int =  15;
pub const SQLITE_EMPTY     : c_int =  16;
pub const SQLITE_SCHEMA    : c_int =  17;
pub const SQLITE_TOOBIG    : c_int =  18;
pub const SQLITE_CONSTRAINT: c_int =  19;
pub const SQLITE_MISMATCH  : c_int =  20;
pub const SQLITE_MISUSE    : c_int =  21;
pub const SQLITE_NOLFS     : c_int =  22;
pub const SQLITE_AUTH      : c_int =  23;
pub const SQLITE_FORMAT    : c_int =  24;
pub const SQLITE_RANGE     : c_int =  25;
pub const SQLITE_NOTADB    : c_int =  26;
pub const SQLITE_NOTICE    : c_int =  27;
pub const SQLITE_WARNING   : c_int =  28;
pub const SQLITE_ROW       : c_int = 100;
pub const SQLITE_DONE      : c_int = 101;

// Extended result codes.

pub const SQLITE_IOERR_READ              : c_int = (SQLITE_IOERR | (1<<8));
pub const SQLITE_IOERR_SHORT_READ        : c_int = (SQLITE_IOERR | (2<<8));
pub const SQLITE_IOERR_WRITE             : c_int = (SQLITE_IOERR | (3<<8));
pub const SQLITE_IOERR_FSYNC             : c_int = (SQLITE_IOERR | (4<<8));
pub const SQLITE_IOERR_DIR_FSYNC         : c_int = (SQLITE_IOERR | (5<<8));
pub const SQLITE_IOERR_TRUNCATE          : c_int = (SQLITE_IOERR | (6<<8));
pub const SQLITE_IOERR_FSTAT             : c_int = (SQLITE_IOERR | (7<<8));
pub const SQLITE_IOERR_UNLOCK            : c_int = (SQLITE_IOERR | (8<<8));
pub const SQLITE_IOERR_RDLOCK            : c_int = (SQLITE_IOERR | (9<<8));
pub const SQLITE_IOERR_DELETE            : c_int = (SQLITE_IOERR | (10<<8));
pub const SQLITE_IOERR_BLOCKED           : c_int = (SQLITE_IOERR | (11<<8));
pub const SQLITE_IOERR_NOMEM             : c_int = (SQLITE_IOERR | (12<<8));
pub const SQLITE_IOERR_ACCESS            : c_int = (SQLITE_IOERR | (13<<8));
pub const SQLITE_IOERR_CHECKRESERVEDLOCK : c_int = (SQLITE_IOERR | (14<<8));
pub const SQLITE_IOERR_LOCK              : c_int = (SQLITE_IOERR | (15<<8));
pub const SQLITE_IOERR_CLOSE             : c_int = (SQLITE_IOERR | (16<<8));
pub const SQLITE_IOERR_DIR_CLOSE         : c_int = (SQLITE_IOERR | (17<<8));
pub const SQLITE_IOERR_SHMOPEN           : c_int = (SQLITE_IOERR | (18<<8));
pub const SQLITE_IOERR_SHMSIZE           : c_int = (SQLITE_IOERR | (19<<8));
pub const SQLITE_IOERR_SHMLOCK           : c_int = (SQLITE_IOERR | (20<<8));
pub const SQLITE_IOERR_SHMMAP            : c_int = (SQLITE_IOERR | (21<<8));
pub const SQLITE_IOERR_SEEK              : c_int = (SQLITE_IOERR | (22<<8));
pub const SQLITE_IOERR_DELETE_NOENT      : c_int = (SQLITE_IOERR | (23<<8));
pub const SQLITE_IOERR_MMAP              : c_int = (SQLITE_IOERR | (24<<8));
pub const SQLITE_IOERR_GETTEMPPATH       : c_int = (SQLITE_IOERR | (25<<8));
pub const SQLITE_IOERR_CONVPATH          : c_int = (SQLITE_IOERR | (26<<8));
pub const SQLITE_IOERR_VNODE             : c_int = (SQLITE_IOERR | (27<<8));
pub const SQLITE_LOCKED_SHAREDCACHE      : c_int = (SQLITE_LOCKED |  (1<<8));
pub const SQLITE_BUSY_RECOVERY           : c_int = (SQLITE_BUSY   |  (1<<8));
pub const SQLITE_BUSY_SNAPSHOT           : c_int = (SQLITE_BUSY   |  (2<<8));
pub const SQLITE_CANTOPEN_NOTEMPDIR      : c_int = (SQLITE_CANTOPEN | (1<<8));
pub const SQLITE_CANTOPEN_ISDIR          : c_int = (SQLITE_CANTOPEN | (2<<8));
pub const SQLITE_CANTOPEN_FULLPATH       : c_int = (SQLITE_CANTOPEN | (3<<8));
pub const SQLITE_CANTOPEN_CONVPATH       : c_int = (SQLITE_CANTOPEN | (4<<8));
pub const SQLITE_CORRUPT_VTAB            : c_int = (SQLITE_CORRUPT | (1<<8));
pub const SQLITE_READONLY_RECOVERY       : c_int = (SQLITE_READONLY | (1<<8));
pub const SQLITE_READONLY_CANTLOCK       : c_int = (SQLITE_READONLY | (2<<8));
pub const SQLITE_READONLY_ROLLBACK       : c_int = (SQLITE_READONLY | (3<<8));
pub const SQLITE_READONLY_DBMOVED        : c_int = (SQLITE_READONLY | (4<<8));
pub const SQLITE_ABORT_ROLLBACK          : c_int = (SQLITE_ABORT | (2<<8));
pub const SQLITE_CONSTRAINT_CHECK        : c_int = (SQLITE_CONSTRAINT | (1<<8));
pub const SQLITE_CONSTRAINT_COMMITHOOK   : c_int = (SQLITE_CONSTRAINT | (2<<8));
pub const SQLITE_CONSTRAINT_FOREIGNKEY   : c_int = (SQLITE_CONSTRAINT | (3<<8));
pub const SQLITE_CONSTRAINT_FUNCTION     : c_int = (SQLITE_CONSTRAINT | (4<<8));
pub const SQLITE_CONSTRAINT_NOTNULL      : c_int = (SQLITE_CONSTRAINT | (5<<8));
pub const SQLITE_CONSTRAINT_PRIMARYKEY   : c_int = (SQLITE_CONSTRAINT | (6<<8));
pub const SQLITE_CONSTRAINT_TRIGGER      : c_int = (SQLITE_CONSTRAINT | (7<<8));
pub const SQLITE_CONSTRAINT_UNIQUE       : c_int = (SQLITE_CONSTRAINT | (8<<8));
pub const SQLITE_CONSTRAINT_VTAB         : c_int = (SQLITE_CONSTRAINT | (9<<8));
pub const SQLITE_CONSTRAINT_ROWID        : c_int = (SQLITE_CONSTRAINT |(10<<8));
pub const SQLITE_NOTICE_RECOVER_WAL      : c_int = (SQLITE_NOTICE | (1<<8));
pub const SQLITE_NOTICE_RECOVER_ROLLBACK : c_int = (SQLITE_NOTICE | (2<<8));
pub const SQLITE_WARNING_AUTOINDEX       : c_int = (SQLITE_WARNING | (1<<8));
pub const SQLITE_AUTH_USER               : c_int = (SQLITE_AUTH | (1<<8));

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
        SQLITE_LOCKED_SHAREDCACHE      => "Locking conflict due to another connection with a shared cache",
        SQLITE_BUSY_RECOVERY           => "Another process is recovering a WAL mode database file",
        SQLITE_BUSY_SNAPSHOT           => "Cannot promote read transaction to write transaction because of writes by another connection",
        SQLITE_CANTOPEN_NOTEMPDIR      => "SQLITE_CANTOPEN_NOTEMPDIR", // no longer used
        SQLITE_CANTOPEN_ISDIR          => "Attempted to open directory as file",
        SQLITE_CANTOPEN_FULLPATH       => "Unable to convert filename into full pathname",
        SQLITE_CANTOPEN_CONVPATH       => "cygwin_conv_path() system call failed",
        SQLITE_CORRUPT_VTAB            => "Content in the virtual table is corrupt",
        SQLITE_READONLY_RECOVERY       => "WAL mode database file needs recovery (requires write access)",
        SQLITE_READONLY_CANTLOCK       => "Shared-memory file associated with WAL mode database is read-only",
        SQLITE_READONLY_ROLLBACK       => "Database has hot journal that must be rolled back (requires write access)",
        SQLITE_READONLY_DBMOVED        => "Database cannot be modified because database file has moved",
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
        SQLITE_NOTICE_RECOVER_WAL      => "A WAL mode database file was recovered",
        SQLITE_NOTICE_RECOVER_ROLLBACK => "Hot journal was rolled back",
        SQLITE_WARNING_AUTOINDEX       => "Automatic indexing used - database might benefit from additional indexes",
        SQLITE_AUTH_USER               => "SQLITE_AUTH_USER", // not documented?

        _ => "Unknown error code",
    }
}
