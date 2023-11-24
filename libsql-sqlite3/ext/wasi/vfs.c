#include "sqlite3.h"
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
LIBSQL_IMPORT("lock") int libsql_wasi_lock(sqlite3_file*, int);
LIBSQL_IMPORT("unlock") int libsql_wasi_unlock(sqlite3_file*, int);
LIBSQL_IMPORT("check_reserved_lock") int libsql_wasi_check_reserved_lock(sqlite3_file*, int *pResOut);
LIBSQL_IMPORT("file_control") int libslq_wasi_file_control(sqlite3_file*, int op, void *pArg);
LIBSQL_IMPORT("sector_size") int libsql_wasi_sector_size(sqlite3_file*);
LIBSQL_IMPORT("device_characteristics") int libsql_wasi_device_characteristics(sqlite3_file*);

typedef struct libsql_wasi_file {
    const struct sqlite3_io_methods* pMethods;
    int64_t fd;
} libsql_wasi_file;

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
    .xFileControl = &libslq_wasi_file_control,
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

void libsql_wasi_init() {
    sqlite3_vfs_register(&libsql_wasi_vfs, 1);
}

sqlite3 *libsql_wasi_open_db(const char *filename) {
    sqlite3 *db;
    int rc = sqlite3_open_v2(filename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "libsql_wasi");
    if (rc != SQLITE_OK) {
        fprintf(stderr, "Failed to open database: %s\n", sqlite3_errmsg(db));
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