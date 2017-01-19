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
  u32 iRoot;              /* Root page of the table */
  int iGen;               /* Schema generation number */
  int iCookie;            /* Schema cookie number from the database file */
  Schema *pSchema;        /* Schema holding the table */
  sqlite3_int64 iRowid;   /* Current rowid */
};

/*
** Create a new sqlite3_kv object open on zDb.zTable and return
** a pointer to that object.
*/
int sqlite3_kv_open(
  sqlite3 *db,          /* The database connection */
  const char *zDb,      /* Schema containing zTable.  NULL for "main" */
  const char *zTable,   /* Name of table the key/value table */
  unsigned int flags,   /* Must be zero.  Reserved for future expansion. */
  sqlite3_kv **ppKvOut  /* Store the new sqlite3_kv object here */
){
  sqlite3_kv *pKv;
  Table *pTab;
  int rc = SQLITE_ERROR;

#ifdef SQLITE_ENABLE_API_ARMOR
  if( ppKvOut==0 ){
    return SQLITE_MISUSE_BKPT;
  }
#endif
  *ppKvOut = 0;
#ifdef SQLITE_ENABLE_API_ARMOR
  if( !sqlite3SafetyCheckOk(db) || zTable==0 ){
    return SQLITE_MISUSE_BKPT;
  }
#endif
  sqlite3_mutex_enter(db->mutex);
  sqlite3BtreeEnterAll(db);

  pTab = sqlite3FindTable(db, zTable, zDb);
  if( pTab==0 ){
    goto kv_open_done;
  }
  if( !((pTab->nCol==1 && pTab->iPKey<0)
        || (pTab->nCol==2 && pTab->iPKey==0)) 
  ){
    /* Must be an single-column table without an INTEGER PRIMARY KEY,
    ** or a two-column table where the first column is the INTEGER PRIMARY KEY
    */
    goto kv_open_done;
  }
  if( pTab->pIndex!=0 || pTab->pFKey!=0 || pTab->pCheck!=0 ){
    /* Do not allow secondary indexes, foreign keys, or CHECK constraints */
    goto kv_open_done;
  }
  if( pTab->tabFlags & (TF_Autoincrement|TF_Virtual|TF_WithoutRowid) ){
    /* Must not have autoincrement.  Must not be a virtual table or a
    ** without rowid table */
    goto kv_open_done;
  }
  *ppKvOut = pKv = sqlite3_malloc(sizeof(*pKv));
  if( pKv==0 ){
    rc = SQLITE_NOMEM;
    goto kv_open_done;
  }
  pKv->db = db;
  pKv->iGen = pTab->pSchema->iGeneration;
  pKv->iCookie = pTab->pSchema->schema_cookie;
  pKv->pSchema = pTab->pSchema;
  pKv->iRoot = pTab->tnum;
  rc = SQLITE_OK;

kv_open_done:
  sqlite3BtreeLeaveAll(db);
  sqlite3_mutex_leave(db->mutex);
  return rc;
}

/*
** Free the key/value accessor at pKv
*/
int sqlite3_kv_close(sqlite3_kv *pKv){
  sqlite3_free(pKv);
  return SQLITE_OK;
}

int sqlite3_kv_seek(sqlite3_kv *pKv, sqlite3_int64 rowid){
  return SQLITE_MISUSE;
}
int sqlite3_kv_reset(sqlite3_kv *pKv){
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
