#include "sqliteInt.h"
#include "wal.h"

/*
** This file contains a stub for implementing one's own WAL routines.
** Registering a new set of WAL methods can be done through
** libsql_wal_methods_register(). Later, a registered set can
** be used by passing its name as a parameter to libsql_open().
*/

extern int libsql_wal_methods_register(libsql_wal_methods*);

static int v_open(sqlite3_vfs *pVfs, sqlite3_file *pDbFd, const char *zWalName, int bNoShm, i64 mxWalSize, libsql_wal_methods *pMethods, Wal **ppWal) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_close(Wal *wal, sqlite3 *db, int sync_flags, int nBuf, u8 *zBuf) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static void v_limit(Wal *wal, i64 limit) {
  //TODO: implement
}

static int v_begin_read_transaction(Wal *wal, int *) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static void v_end_read_transaction(Wal *wal) {
  //TODO: implement
}

static int v_find_frame(Wal *wal, Pgno pgno, u32 *frame) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_read_frame(Wal *wal, u32 frame, int nOut, u8 *pOut) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static Pgno v_dbsize(Wal *wal) {
  //TODO: implement
  return 0;
}

static int v_begin_write_transaction(Wal *wal) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_end_write_transaction(Wal *wal) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_undo(Wal *wal, int (*xUndo)(void *, Pgno), void *pUndoCtx) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static void v_savepoint(Wal *wal, u32 *wal_data) {
  //TODO: implement
}

static int v_savepoint_undo(Wal *wal, u32 *wal_data) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_frames(Wal *pWal, int szPage, PgHdr *pList, Pgno nTruncate, int isCommit, int sync_flags) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_checkpoint(Wal *wal, sqlite3 *db, int eMode, int (xBusy)(void *), void *pBusyArg, int sync_flags, int nBuf, u8 *zBuf, int *pnLog, int *pnCkpt) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_callback(Wal *wal) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static int v_exclusive_mode(Wal *wal, int op) {
  //TODO: implement
  return SQLITE_MISUSE;;
}

static int v_heap_memory(Wal *wal) {
  //TODO: implement
  return SQLITE_MISUSE;
}

static sqlite3_file *v_file(Wal *wal) {
  //TODO: implement
  return NULL;
}

static void v_db(Wal *wal, sqlite3 *db) {
  //TODO: implement
}

static int v_pathname_len(int n) {
  return 0;
}

static void v_get_wal_pathname(char *buf, const char *orig, int orig_len) {
}
