#include "sqlite3.h"
#include "src/wal.h"
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define LIBSQL_IMPORT(name) extern __attribute__((import_module("libsql_host"), import_name(name)))

LIBSQL_IMPORT("close") int libsql_wasi_close(sqlite3_file*);
LIBSQL_IMPORT("read") int libsql_wasi_read(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
LIBSQL_IMPORT("write") int libsql_wasi_write(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);
LIBSQL_IMPORT("truncate") int libsql_wasi_truncate(sqlite3_file*, sqlite3_int64 size);
LIBSQL_IMPORT("sync") int libsql_wasi_sync(sqlite3_file*, int flags);
LIBSQL_IMPORT("file_size") int libsql_wasi_file_size(sqlite3_file*, sqlite3_int64 *pSize);

typedef struct libsql_wasi_file {
    const struct sqlite3_io_methods* pMethods;
    int64_t fd;
} libsql_wasi_file;

// We're running in exclusive mode, so locks are noops.
// We need to handle locking in the host.
static int libsql_wasi_lock(sqlite3_file* f, int eLock) {
    (void)f, (void)eLock;
    return SQLITE_OK;
}

static int libsql_wasi_unlock(sqlite3_file* f, int eLock) {
    (void)f, (void)eLock;
    return SQLITE_OK;
}

static int libsql_wasi_check_reserved_lock(sqlite3_file* f, int *pResOut) {
    (void)f, (void)pResOut;
    return SQLITE_OK;
}

static int libsql_wasi_device_characteristics(sqlite3_file* f) {
    (void)f;
    return SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_SAFE_APPEND | SQLITE_IOCAP_SEQUENTIAL;
}

static int libsql_wasi_file_control(sqlite3_file* f, int opcode, void* arg) {
    (void)opcode, (void)f, (void)arg;
    return SQLITE_NOTFOUND;
}

static int libsql_wasi_sector_size(sqlite3_file* f) {
    (void)f;
    return 512;
}

static const sqlite3_io_methods wasi_io_methods = {
    .iVersion = 1,
    .xClose = &libsql_wasi_close,
    .xRead = &libsql_wasi_read,
    .xWrite = &libsql_wasi_write,
    .xTruncate = &libsql_wasi_truncate,
    .xSync = &libsql_wasi_sync,
    .xFileSize = &libsql_wasi_file_size,
    .xLock = &libsql_wasi_lock,
    .xUnlock = &libsql_wasi_unlock,
    .xCheckReservedLock = &libsql_wasi_check_reserved_lock,
    .xFileControl = &libsql_wasi_file_control,
    .xSectorSize = &libsql_wasi_sector_size,
    .xDeviceCharacteristics = &libsql_wasi_device_characteristics,
};

LIBSQL_IMPORT("open_fd") int64_t libsql_wasi_open_fd(const char *zName, int flags);
LIBSQL_IMPORT("delete") int libsql_wasi_delete(sqlite3_vfs*, const char *zName, int syncDir);
LIBSQL_IMPORT("access") int libsql_wasi_access(sqlite3_vfs*, const char *zName, int flags, int *pResOut);
LIBSQL_IMPORT("full_pathname") int libsql_wasi_full_pathname(sqlite3_vfs*, const char *zName, int nOut, char *zOut);
LIBSQL_IMPORT("randomness") int libsql_wasi_randomness(sqlite3_vfs*, int nByte, char *zOut);
LIBSQL_IMPORT("sleep") int libsql_wasi_sleep(sqlite3_vfs*, int microseconds);
LIBSQL_IMPORT("current_time") int libsql_wasi_current_time(sqlite3_vfs*, double*);
LIBSQL_IMPORT("get_last_error") int libsql_wasi_get_last_error(sqlite3_vfs*, int, char*);
LIBSQL_IMPORT("current_time_64") int libsql_wasi_current_time_64(sqlite3_vfs*, sqlite3_int64*);

int libsql_wasi_vfs_open(sqlite3_vfs *vfs, const char *zName, sqlite3_file *file_, int flags, int *pOutFlags) {
    libsql_wasi_file *file = (libsql_wasi_file*)file_;
    file->fd = libsql_wasi_open_fd(zName, flags);
    if (file->fd == 0) {
        return SQLITE_CANTOPEN;
    }
    file->pMethods = &wasi_io_methods;
    return SQLITE_OK;
}

sqlite3_vfs libsql_wasi_vfs = {
    .iVersion = 2,
    .szOsFile = sizeof(libsql_wasi_file),
    .mxPathname = 100,
    .zName = "libsql_wasi",

    .xOpen = &libsql_wasi_vfs_open,
    .xDelete = &libsql_wasi_delete,
    .xAccess = &libsql_wasi_access,
    .xFullPathname = &libsql_wasi_full_pathname,
    .xRandomness = &libsql_wasi_randomness,
    .xSleep = &libsql_wasi_sleep,
    .xCurrentTime = &libsql_wasi_current_time,
    .xGetLastError = &libsql_wasi_get_last_error,
    .xCurrentTimeInt64 = &libsql_wasi_current_time_64,
};

libsql_wal_methods *the_wal_methods = NULL;

int libsql_wasi_wal_open(sqlite3_vfs* vfs, sqlite3_file* f, const char* path, int no_shm_mode, long long max_size, struct libsql_wal_methods* wal_methods, libsql_wal** wal) {
    fprintf(stderr, "Opening virtual WAL at %s: %s\n", path, wal_methods->zName);
    return the_wal_methods->xOpen(vfs, f, path, no_shm_mode, max_size, wal_methods, wal);
}

int libsql_wasi_wal_close(libsql_wal* wal, sqlite3* db, int sync_flags, int nBuf, unsigned char* zBuf) {
    return the_wal_methods->xClose(wal, db, sync_flags, nBuf, zBuf);
}

void libsql_wasi_wal_limit(libsql_wal* wal, long long limit) {
    return the_wal_methods->xLimit(wal, limit);
}

int libsql_wasi_wal_begin_read_transaction(libsql_wal* wal, int* out) {
    return the_wal_methods->xBeginReadTransaction(wal, out);
}

void libsql_wasi_wal_end_read_transaction(libsql_wal* wal) {
    return the_wal_methods->xEndReadTransaction(wal);
}

int libsql_wasi_wal_find_frame(libsql_wal* wal, unsigned int frame, unsigned int* out) {
    return the_wal_methods->xFindFrame(wal, frame, out);
}

int libsql_wasi_wal_read_frame(libsql_wal* wal, unsigned int frame, int n, unsigned char* out) {
    return the_wal_methods->xReadFrame(wal, frame, n, out);
}

unsigned int libsql_wasi_wal_dbsize(libsql_wal* wal) {
    return the_wal_methods->xDbsize(wal);
}

int libsql_wasi_wal_begin_write_transaction(libsql_wal* wal) {
    return the_wal_methods->xBeginWriteTransaction(wal);
}

int libsql_wasi_wal_end_write_transaction(libsql_wal* wal) {
    return the_wal_methods->xEndWriteTransaction(wal);
}

int libsql_wasi_wal_undo(libsql_wal* wal, int (*xUndo)(void*, unsigned int), void* pUndoCtx) {
    return the_wal_methods->xUndo(wal, xUndo, pUndoCtx);
}

void libsql_wasi_wal_savepoint(libsql_wal* wal, unsigned int* aWalData) {
    return the_wal_methods->xSavepoint(wal, aWalData);
}

int libsql_wasi_wal_savepoint_undo(libsql_wal* wal, unsigned int* aWalData) {
    return the_wal_methods->xSavepointUndo(wal, aWalData);
}

int libsql_wasi_wal_frames(libsql_wal* wal, int n, libsql_pghdr* aPgHdr, unsigned int cksum, int mode, int readonly) {
    return the_wal_methods->xFrames(wal, n, aPgHdr, cksum, mode, readonly);
}

int libsql_wasi_wal_checkpoint(libsql_wal* wal, sqlite3* db, int eMode, int (*xBusy)(void*), void* pBusyArg, int sync_flags, int nBuf, unsigned char* zBuf, int* pnLog, int* pnCkpt) {
    return the_wal_methods->xCheckpoint(wal, db, eMode, xBusy, pBusyArg, sync_flags, nBuf, zBuf, pnLog, pnCkpt);
}

int libsql_wasi_wal_callback(libsql_wal* wal) {
    return the_wal_methods->xCallback(wal);
}

int libsql_wasi_wal_exclusive_mode(libsql_wal* wal, int op) {
    return the_wal_methods->xExclusiveMode(wal, op);
}

int libsql_wasi_wal_heap_memory(libsql_wal* wal) {
    return the_wal_methods->xHeapMemory(wal);
}

int libsql_wasi_wal_snapshot_get(libsql_wal* wal, sqlite3_snapshot** snapshot) {
    return the_wal_methods->xSnapshotGet(wal, snapshot);
}

void libsql_wasi_wal_snapshot_open(libsql_wal* wal, sqlite3_snapshot* snapshot) {
    return the_wal_methods->xSnapshotOpen(wal, snapshot);
}

int libsql_wasi_wal_snapshot_recover(libsql_wal* wal) {
    return the_wal_methods->xSnapshotRecover(wal);
}

int libsql_wasi_wal_snapshot_check(libsql_wal* wal, sqlite3_snapshot* snapshot) {
    return the_wal_methods->xSnapshotCheck(wal, snapshot);
}

void libsql_wasi_wal_snapshot_unlock(libsql_wal* wal) {
    return the_wal_methods->xSnapshotUnlock(wal);
}

int libsql_wasi_wal_framesize(libsql_wal* wal) {
    return the_wal_methods->xFramesize(wal);
}

sqlite3_file *libsql_wasi_wal_file(libsql_wal* wal) {
    return the_wal_methods->xFile(wal);
}

int libsql_wasi_wal_writelock(libsql_wal* wal, int bLock) {
    return the_wal_methods->xWriteLock(wal, bLock);
}

void libsql_wasi_wal_db(libsql_wal* wal, sqlite3* db) {
    return the_wal_methods->xDb(wal, db);
}

int libsql_wasi_wal_pathname_len(int orig_len) {
    return the_wal_methods->xPathnameLen(orig_len);
}

void libsql_wasi_get_wal_pathname(char *buf, const char *orig, int len) {
    return the_wal_methods->xGetWalPathname(buf, orig, len);
}

int libsql_wasi_wal_pre_main_db_open(libsql_wal_methods *methods, const char *path) {
    return 0;
}

libsql_wal_methods libsql_wasi_wal_methods = {
    .iVersion = 1,
    .xOpen = &libsql_wasi_wal_open,
    .xClose = &libsql_wasi_wal_close,
    .xLimit = &libsql_wasi_wal_limit,
    .xBeginReadTransaction = &libsql_wasi_wal_begin_read_transaction,
    .xEndReadTransaction = &libsql_wasi_wal_end_read_transaction,
    .xFindFrame = &libsql_wasi_wal_find_frame,
    .xReadFrame = &libsql_wasi_wal_read_frame,
    .xDbsize = &libsql_wasi_wal_dbsize,
    .xBeginWriteTransaction = &libsql_wasi_wal_begin_write_transaction,
    .xEndWriteTransaction = &libsql_wasi_wal_end_write_transaction,
    .xUndo = &libsql_wasi_wal_undo,
    .xSavepoint = &libsql_wasi_wal_savepoint,
    .xSavepointUndo = &libsql_wasi_wal_savepoint_undo,
    .xFrames = &libsql_wasi_wal_frames,
    .xCheckpoint = &libsql_wasi_wal_checkpoint,
    .xCallback = &libsql_wasi_wal_callback,
    .xExclusiveMode = &libsql_wasi_wal_exclusive_mode,
    .xHeapMemory = &libsql_wasi_wal_heap_memory,
    .xSnapshotGet = &libsql_wasi_wal_snapshot_get,
    .xSnapshotOpen = &libsql_wasi_wal_snapshot_open,
    .xSnapshotRecover = &libsql_wasi_wal_snapshot_recover,
    .xSnapshotCheck = &libsql_wasi_wal_snapshot_check,
    .xSnapshotUnlock = &libsql_wasi_wal_snapshot_unlock,
    .xFramesize = &libsql_wasi_wal_framesize,
    .xFile = &libsql_wasi_wal_file,
    .xWriteLock = &libsql_wasi_wal_writelock,
    .xDb = &libsql_wasi_wal_db,
    .xPathnameLen = &libsql_wasi_wal_pathname_len,
    .xGetWalPathname = &libsql_wasi_get_wal_pathname,
    .xPreMainDbOpen = &libsql_wasi_wal_pre_main_db_open,
    .bUsesShm = 0,
    .zName = "libsql_wasi",
    .pNext = NULL,
};

void libsql_wasi_init() {
    the_wal_methods = libsql_wal_methods_find(NULL);
    sqlite3_vfs_register(&libsql_wasi_vfs, 1);
    libsql_wal_methods_register(&libsql_wasi_wal_methods);
    fprintf(stderr, "WASI initialized\n");
}

sqlite3 *libsql_wasi_open_db(const char *filename) {
    sqlite3 *db;
    fprintf(stderr, "opening database %s\n", filename);
    int rc = libsql_open(filename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "libsql_wasi", "libsql_wasi");
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to open database: %s\n", sqlite3_errmsg(db));
        return NULL;
    }
    fprintf(stderr, "opened database %s\n", filename);
    rc = sqlite3_exec(db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to set journal mode: %s\n", sqlite3_errmsg(db));
        return NULL;
    }
    return db;
}

int libsql_wasi_exec(sqlite3 *db, const char *sql) {
    sqlite3_stmt *stmt;
    int rc = sqlite3_prepare_v2(db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to prepare statement: %s\n", sqlite3_errmsg(db));
        return rc;
    }
    // Step in a loop until SQLITE_DONE or error
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {}
    if (rc != SQLITE_DONE) {
        fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
        return rc;
    }
    return SQLITE_OK;
}