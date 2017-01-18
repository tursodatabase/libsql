/*
** 2017-01-18
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used to implement key/value access interface.
*/

#include "sqliteInt.h"

#ifndef SQLITE_OMIT_KEYVALUE_ACCESSOR

/*
** An sqlite3_kv object is an accessor for key/value pairs.
**
** This is an opaque object.  The public interface sees pointers to this
** object, but not the internals.  So the internal composition of this
** object is free to change from one release to the next without breaking
** compatibility.
*/
struct sqlite3_kv {
  sqlite3 *db;            /* The database holding the table to be accessed */
  u8 iDb;                 /* Database containing the table to access */
  u32 iRoot;              /* Root page of the table */
  sqlite3_int64 iRowid;   /* Current rowid */
};

int sqlite3_kv_open(
  sqlite3 *db,
  const char *zDb,
  const char *zTable,
  unsigned int flags,   /* Must be zero.  Reserved for future expansion. */
  sqlite3_kv *pKvOut
){
  return SQLITE_MISUSE;
}

int sqlite3_kv_close(sqlite3_kv *pKv){
  return SQLITE_OK;
}

int sqlite3_kv_seek(sqlite3_kv *pKv, sqlite3_int64 rowid){
  return SQLITE_MISUSE;
}
int sqlite3_kv_bytes(sqlite3_kv *pKv){
  return -1;
}
int sqlite3_kv_read(sqlite3_kv *pKv, void *pBuf, int amt, int offset){
  return SQLITE_MISUSE;
}
int sqlite3_kv_insert(sqlite3_kv *pKv, sqlite3_int64 rid, int sz, void *pBuf){
  return SQLITE_MISUSE;
}

#endif /* #ifndef SQLITE_OMIT_KEYVALU_ACCESSOR */
