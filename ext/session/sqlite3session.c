
#ifdef SQLITE_ENABLE_SESSION

#include "sqlite3session.h"
#include <assert.h>
#include <string.h>

#include "sqliteInt.h"
#include "vdbeInt.h"

typedef struct RowChange RowChange;
typedef struct SessionTable SessionTable;
typedef struct SessionChange SessionChange;

#if 0
#ifndef SQLITE_AMALGAMATION
typedef unsigned char u8;
typedef unsigned long u32;
typedef sqlite3_uint64 u64;
#endif
#endif

struct sqlite3_session {
  sqlite3 *db;                    /* Database handle session is attached to */
  char *zDb;                      /* Name of database session is attached to */
  int rc;                         /* Non-zero if an error has occurred */
  sqlite3_session *pNext;         /* Next session object on same db. */
  SessionTable *pTable;           /* List of attached tables */
};

/*
** Each session object maintains a set of the following structures, one
** for each table the session object is monitoring. The structures are
** stored in a linked list starting at sqlite3_session.pTable.
**
** The keys of the SessionTable.aChange[] hash table are all rows that have
** been modified in any way since the session object was attached to the
** table.
**
** The data associated with each hash-table entry is a structure containing
** a subset of the initial values that the modified row contained at the
** start of the session. Or no initial values if the row was inserted.
*/
struct SessionTable {
  SessionTable *pNext;
  char *zName;                    /* Local name of table */
  int nCol;                       /* Number of columns in table zName */

  /* Hash table of modified rows */
  int nEntry;                     /* NUmber of entries in hash table */
  int nChange;                    /* Size of apChange[] array */
  SessionChange **apChange;       /* Hash table buckets */
};

/* 
** RECORD FORMAT:
**
** The following record format is similar to (but not compatible with) that 
** used in SQLite database files. This format is used as part of the 
** change-set binary format, and so must be architecture independent.
**
** Unlike the SQLite database record format, each field is self-contained -
** there is no separation of header and data. Each field begins with a
** single byte describing its type, as follows:
**
**       0x00: Undefined value.
**       0x01: Integer value.
**       0x02: Real value.
**       0x03: Text value.
**       0x04: Blob value.
**       0x05: SQL NULL value.
**
** Note that the above match the definitions of SQLITE_INTEGER, SQLITE_TEXT
** and so on in sqlite3.h. For undefined and NULL values, the field consists
** only of the single type byte. For other types of values, the type byte
** is followed by:
**
**   Text values:
**     A varint containing the number of bytes in the value (encoded using
**     UTF-8). Followed by a buffer containing the UTF-8 representation
**     of the text value. There is no nul terminator.
**
**   Blob values:
**     A varint containing the number of bytes in the value, followed by
**     a buffer containing the value itself.
**
**   Integer values:
**     An 8-byte big-endian integer value.
**
**   Real values:
**     An 8-byte big-endian IEEE 754-2008 real value.
**
** Varint values are encoded in the same way as varints in the SQLite 
** record format.
**
** CHANGESET FORMAT:
**
** A changeset is a collection of DELETE, UPDATE and INSERT operations on
** one or more tables. Operations on a single table are grouped together,
** but may occur in any order (i.e. deletes, updates and inserts are all
** mixed together).
**
** Each group of changes begins with a table header:
**
**   1 byte: Constant 0x54 (capital 'T')
**   Varint: Big-endian integer set to the number of columns in the table.
**   N bytes: Unqualified table name (encoded using UTF-8). Nul-terminated.
**
** Followed by one or more changes to the table.
**
**   1 byte: Either SQLITE_INSERT, UPDATE or DELETE.
**   old.* record: (delete and update only)
**   new.* record: (insert and update only)
*/

/*
** For each row modified during a session, there exists a single instance of
** this structure stored in a SessionTable.aChange[] hash table.
*/
struct SessionChange {
  sqlite3_int64 iKey;             /* Key value */
  int nRecord;                    /* Number of bytes in buffer aRecord[] */
  u8 *aRecord;                    /* Buffer containing old.* record */
  SessionChange *pNext;           /* For hash-table collisions */
};


static int sessionVarintPut(u8 *aBuf, u32 iVal){
  if( (iVal & ~0x7F)==0 ){
    if( aBuf ){
      aBuf[0] = (u8)iVal;
    }
    return 1;
  }
  if( (iVal & ~0x3FFF)==0 ){
    if( aBuf ){
      aBuf[0] = ((iVal >> 7) & 0x7F) | 0x80;
      aBuf[1] = iVal & 0x7F;
    }
    return 2;
  }
  if( aBuf ){
    aBuf[0] = ((iVal >> 28) & 0x7F) | 0x80;
    aBuf[1] = ((iVal >> 21) & 0x7F) | 0x80;
    aBuf[2] = ((iVal >> 14) & 0x7F) | 0x80;
    aBuf[3] = ((iVal >>  7) & 0x7F) | 0x80;
    aBuf[4] = iVal & 0x7F;
  }
  return 5;
}

static int sessionVarintGet(u8 *aBuf, int *piVal){
  int ret;
  u64 v;
  ret = (int)sqlite3GetVarint(aBuf, &v);
  *piVal = (int)v;
  return ret;
}

static sqlite3_int64 sessionGetI64(u8 *aRec){
  return (((sqlite3_int64)aRec[0]) << 56)
       + (((sqlite3_int64)aRec[1]) << 48)
       + (((sqlite3_int64)aRec[2]) << 40)
       + (((sqlite3_int64)aRec[3]) << 32)
       + (((sqlite3_int64)aRec[4]) << 24)
       + (((sqlite3_int64)aRec[5]) << 16)
       + (((sqlite3_int64)aRec[6]) <<  8)
       + (((sqlite3_int64)aRec[7]) <<  0);
}

/*
** This function is used to serialize the contents of value pValue (see
** comment titled "RECORD FORMAT" above).
**
** If it is non-NULL, the serialized form of the value is written to 
** buffer aBuf. *pnWrite is set to the number of bytes written before
** returning. Or, if aBuf is NULL, the only thing this function does is
** set *pnWrite.
**
** If no error occurs, SQLITE_OK is returned. Or, if an OOM error occurs
** within a call to sqlite3_value_text() (may fail if the db is utf-16)) 
** SQLITE_NOMEM is returned.
*/
static int sessionSerializeValue(
  u8 *aBuf,                       /* If non-NULL, write serialized value here */
  sqlite3_value *pValue,          /* Value to serialize */
  int *pnWrite                    /* IN/OUT: Increment by bytes written */
){
  int eType; 
  int nByte;

  eType = sqlite3_value_type(pValue);
  if( aBuf ) aBuf[0] = eType;

  switch( eType ){
    case SQLITE_NULL: 
      nByte = 1;
      break;

    case SQLITE_INTEGER: 
    case SQLITE_FLOAT:
      if( aBuf ){
        /* TODO: SQLite does something special to deal with mixed-endian
        ** floating point values (e.g. ARM7). This code probably should
        ** too.  */
        u64 i;
        if( eType==SQLITE_INTEGER ){
          i = (u64)sqlite3_value_int64(pValue);
        }else{
          double r;
          assert( sizeof(double)==8 && sizeof(u64)==8 );
          r = sqlite3_value_double(pValue);
          memcpy(&i, &r, 8);
        }
        aBuf[1] = (i>>56) & 0xFF;
        aBuf[2] = (i>>48) & 0xFF;
        aBuf[3] = (i>>40) & 0xFF;
        aBuf[4] = (i>>32) & 0xFF;
        aBuf[5] = (i>>24) & 0xFF;
        aBuf[6] = (i>>16) & 0xFF;
        aBuf[7] = (i>> 8) & 0xFF;
        aBuf[8] = (i>> 0) & 0xFF;
      }
      nByte = 9; 
      break;

    case SQLITE_TEXT: 
    case SQLITE_BLOB: {
      int n = sqlite3_value_bytes(pValue);
      int nVarint = sessionVarintPut(0, n);
      if( aBuf ){
        sessionVarintPut(&aBuf[1], n);
        memcpy(&aBuf[nVarint + 1], eType==SQLITE_TEXT ? 
            sqlite3_value_text(pValue) : sqlite3_value_blob(pValue), n
        );
      }

      nByte = 1 + nVarint + n;
      break;
    }
  }

  *pnWrite += nByte;
  return SQLITE_OK;
}

/*
** Return the hash of iKey, assuming there are nBucket hash buckets in
** the hash table.
*/
static int sessionKeyhash(int nBucket, sqlite3_int64 iKey){
  return (iKey % nBucket);
}

/*
** If required, grow the hash table used to store changes on table pTab 
** (part of the session pSession). If a fatal OOM error occurs, set the
** session object to failed and return SQLITE_ERROR. Otherwise, return
** SQLITE_OK.
**
** It is possible that a non-fatal OOM error occurs in this function. In
** that case the hash-table does not grow, but SQLITE_OK is returned anyway.
** Growing the hash table in this case is a performance optimization only,
** it is not required for correct operation.
*/
static int sessionGrowHash(sqlite3_session *pSession, SessionTable *pTab){
  if( pTab->nChange==0 || pTab->nEntry>=(pTab->nChange/2) ){
    int i;
    SessionChange **apNew;
    int nNew = (pTab->nChange ? pTab->nChange : 128) * 2;

    apNew = (SessionChange **)sqlite3_malloc(sizeof(SessionChange *) * nNew);
    if( apNew==0 ){
      if( pTab->nChange==0 ){
        pSession->rc = SQLITE_NOMEM;
        return SQLITE_ERROR;
      }
      return SQLITE_OK;
    }
    memset(apNew, 0, sizeof(SessionChange *) * nNew);

    for(i=0; i<pTab->nChange; i++){
      SessionChange *p;
      SessionChange *pNext;
      for(p=pTab->apChange[i]; p; p=pNext){
        int iHash = sessionKeyhash(nNew, p->iKey);
        pNext = p->pNext;
        p->pNext = apNew[iHash];
        apNew[iHash] = p;
      }
    }

    sqlite3_free(pTab->apChange);
    pTab->nChange = nNew;
    pTab->apChange = apNew;
  }

  return SQLITE_OK;
}

static int sessionInitTable(sqlite3_session *pSession, SessionTable *pTab){
  if( pTab->nCol==0 ){
    pTab->nCol = sqlite3_preupdate_count(pSession->db);
  }

  if( pTab->nCol!=sqlite3_preupdate_count(pSession->db) ){
    pSession->rc = SQLITE_SCHEMA;
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

/*
** The 'pre-update' hook registered by this module with SQLite databases.
*/
static void xPreUpdate(
  void *pCtx,                     /* Copy of third arg to preupdate_hook() */
  sqlite3 *db,                    /* Database handle */
  int op,                         /* SQLITE_UPDATE, DELETE or INSERT */
  char const *zDb,                /* Database name */
  char const *zName,              /* Table name */
  sqlite3_int64 iKey1,            /* Rowid of row about to be deleted/updated */
  sqlite3_int64 iKey2             /* New rowid value (for a rowid UPDATE) */
){
  sqlite3_session *pSession;
  int nDb = strlen(zDb);
  int nName = strlen(zDb);
 
  for(pSession=(sqlite3_session *)pCtx; pSession; pSession=pSession->pNext){
    SessionTable *pTab;
    if( pSession->rc ) continue;
    if( sqlite3_strnicmp(zDb, pSession->zDb, nDb+1) ) continue;
    for(pTab=pSession->pTable; pTab; pTab=pTab->pNext){
      if( 0==sqlite3_strnicmp(pTab->zName, zName, nName+1) ){
        SessionChange *pChange;
        SessionChange *pC;
        int iHash; 
        int rc = SQLITE_OK;

        /* Load table details if required */
        if( sessionInitTable(pSession, pTab) ) return;

        /* Grow the hash table if required */
        if( sessionGrowHash(pSession, pTab) ) return;

        /* Search the hash table for an existing entry for rowid=iKey2. If
        ** one is found, store a pointer to it in pChange and unlink it from
        ** the hash table. Otherwise, set pChange to NULL.
        */
        iHash = sessionKeyhash(pTab->nChange, iKey2);
        for(pC=pTab->apChange[iHash]; pC; pC=pC->pNext){
          if( pC->iKey==iKey2 ) break;
        }
        if( pC ) continue;

        pTab->nEntry++;

        /* Create a new change object containing all the old values (if
        ** this is an SQLITE_UPDATE or SQLITE_DELETE), or no record at
        ** all (if this is an INSERT). */
        if( op==SQLITE_INSERT ){
          pChange = (SessionChange *)sqlite3_malloc(sizeof(SessionChange));
          if( pChange ){
            memset(pChange, 0, sizeof(SessionChange));
          }
        }else{
          int nByte;            /* Number of bytes to allocate */
          int i;                /* Used to iterate through columns */
          sqlite3_value *pValue;

          /* Figure out how large an allocation is required */
          nByte = sizeof(SessionChange);
          for(i=0; i<pTab->nCol && rc==SQLITE_OK; i++){
            rc = sqlite3_preupdate_old(pSession->db, i, &pValue);
            if( rc==SQLITE_OK ){
              rc = sessionSerializeValue(0, pValue, &nByte);
            }
          }

          /* Allocate the change object */
          pChange = (SessionChange *)sqlite3_malloc(nByte);
          if( !pChange ){
            rc = SQLITE_NOMEM;
          }else{
            memset(pChange, 0, sizeof(SessionChange));
            pChange->aRecord = (u8 *)&pChange[1];
          }

          /* Populate the change object */
          nByte = 0;
          for(i=0; i<pTab->nCol && rc==SQLITE_OK; i++){
            rc = sqlite3_preupdate_old(pSession->db, i, &pValue);
            if( rc==SQLITE_OK ){
              rc = sessionSerializeValue(
                  &pChange->aRecord[nByte], pValue, &nByte);
            }
          }
          pChange->nRecord = nByte;
        }

        /* If an error has occurred, mark the session object as failed. */
        if( rc!=SQLITE_OK ){
          sqlite3_free(pChange);
          pSession->rc = rc;
          return;
        }

        /* Add the change back to the hash-table */
        pChange->iKey = iKey2;
        pChange->pNext = pTab->apChange[iHash];
        pTab->apChange[iHash] = pChange;
      }
      break;
    }
  }
}

/*
** Create a session object. This session object will record changes to
** database zDb attached to connection db.
*/
int sqlite3session_create(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Name of db (e.g. "main") */
  sqlite3_session **ppSession     /* OUT: New session object */
){
  sqlite3_session *pNew;
  sqlite3_session *pOld;
  int nDb = strlen(zDb);          /* Length of zDb in bytes */

  *ppSession = 0;

  /* Allocate and populate the new session object. */
  pNew = (sqlite3_session *)sqlite3_malloc(sizeof(sqlite3_session) + nDb + 1);
  if( !pNew ) return SQLITE_NOMEM;
  memset(pNew, 0, sizeof(sqlite3_session));
  pNew->db = db;
  pNew->zDb = (char *)&pNew[1];
  memcpy(pNew->zDb, zDb, nDb+1);

  /* Add the new session object to the linked list of session objects 
  ** attached to database handle $db. Do this under the cover of the db
  ** handle mutex.  */
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  pOld = (sqlite3_session*)sqlite3_preupdate_hook(db, xPreUpdate, (void*)pNew);
  pNew->pNext = pOld;
  sqlite3_mutex_leave(sqlite3_db_mutex(db));

  *ppSession = pNew;
  return SQLITE_OK;
}

/*
** Delete a session object previously allocated using sqlite3session_create().
*/
void sqlite3session_delete(sqlite3_session *pSession){
  sqlite3 *db = pSession->db;
  sqlite3_session *pHead;
  sqlite3_session **pp;

  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  pHead = (sqlite3_session*)sqlite3_preupdate_hook(db, 0, 0);
  for(pp=&pHead; (*pp)!=pSession; pp=&((*pp)->pNext));
  *pp = (*pp)->pNext;
  if( pHead ) sqlite3_preupdate_hook(db, xPreUpdate, (void *)pHead);
  sqlite3_mutex_leave(sqlite3_db_mutex(db));

  while( pSession->pTable ){
    int i;
    SessionTable *pTab = pSession->pTable;
    pSession->pTable = pTab->pNext;
    for(i=0; i<pTab->nChange; i++){
      SessionChange *p;
      SessionChange *pNext;
      for(p=pTab->apChange[i]; p; p=pNext){
        pNext = p->pNext;
        sqlite3_free(p);
      }
    }
    sqlite3_free(pTab->apChange);
    sqlite3_free(pTab);
  }

  sqlite3_free(pSession);
}

/*
** Attach a table to a session. All subsequent changes made to the table
** while the session object is enabled will be recorded.
**
** Only tables that have a PRIMARY KEY defined may be attached. It does
** not matter if the PRIMARY KEY is an "INTEGER PRIMARY KEY" (rowid alias)
** or not.
*/
int sqlite3session_attach(
  sqlite3_session *pSession,      /* Session object */
  const char *zName               /* Table name */
){
  SessionTable *pTab;
  int nName;

  /* First search for an existing entry. If one is found, this call is
  ** a no-op. Return early. */
  nName = strlen(zName);
  for(pTab=pSession->pTable; pTab; pTab=pTab->pNext){
    if( 0==sqlite3_strnicmp(pTab->zName, zName, nName+1) ){
      return SQLITE_OK;
    }
  }

  /* Allocate new SessionTable object. */
  pTab = (SessionTable *)sqlite3_malloc(sizeof(SessionTable) + nName + 1);
  if( !pTab ) return SQLITE_NOMEM;

  /* Populate the new SessionTable object and link it into the list. */
  memset(pTab, 0, sizeof(SessionTable));
  pTab->zName = (char *)&pTab[1];
  memcpy(pTab->zName, zName, nName+1);
  pTab->pNext = pSession->pTable;
  pSession->pTable = pTab;

  return SQLITE_OK;
}

typedef struct SessionBuffer SessionBuffer;
struct SessionBuffer {
  u8 *aBuf;                       /* Pointer to changeset buffer */
  int nBuf;                       /* Size of buffer aBuf */
  int nAlloc;                     /* Size of allocation containing aBuf */
};

static int sessionBufferGrow(SessionBuffer *p, int nByte, int *pRc){
  if( p->nAlloc-p->nBuf<nByte ){
    u8 *aNew;
    int nNew = p->nAlloc ? p->nAlloc : 128;
    do {
      nNew = nNew*2;
    }while( nNew<(p->nAlloc+nByte) );

    aNew = (u8 *)sqlite3_realloc(p->aBuf, nNew);
    if( 0==aNew ){
      *pRc = SQLITE_NOMEM;
      return 1;
    }
    p->aBuf = aNew;
    p->nAlloc = nNew;
  }
  return 0;
}

static void sessionAppendByte(SessionBuffer *p, u8 v, int *pRc){
  if( *pRc==SQLITE_OK && 0==sessionBufferGrow(p, 1, pRc) ){
    p->aBuf[p->nBuf++] = v;
  }
}

static void sessionAppendVarint(SessionBuffer *p, sqlite3_int64 v, int *pRc){
  if( *pRc==SQLITE_OK && 0==sessionBufferGrow(p, 9, pRc) ){
    p->nBuf += sessionVarintPut(&p->aBuf[p->nBuf], v);
  }
}

static void sessionAppendBlob(
  SessionBuffer *p, 
  const u8 *aBlob, 
  int nBlob, 
  int *pRc
){
  if( *pRc==SQLITE_OK && 0==sessionBufferGrow(p, nBlob, pRc) ){
    memcpy(&p->aBuf[p->nBuf], aBlob, nBlob);
    p->nBuf += nBlob;
  }
}

static void sessionAppendCol(
  SessionBuffer *p, 
  sqlite3_stmt *pStmt, 
  int iCol,
  int *pRc
){
  if( *pRc==SQLITE_OK ){
    int eType = sqlite3_column_type(pStmt, iCol);
    sessionAppendByte(p, (u8)eType, pRc);
    if( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT ){
      sqlite3_int64 i;
      u8 aBuf[8];
      if( eType==SQLITE_INTEGER ){
        i = sqlite3_column_int64(pStmt, iCol);
      }else{
        double r = sqlite3_column_double(pStmt, iCol);
        memcpy(&i, &r, 8);
      }
      aBuf[0] = (i>>56) & 0xFF;
      aBuf[1] = (i>>48) & 0xFF;
      aBuf[2] = (i>>40) & 0xFF;
      aBuf[3] = (i>>32) & 0xFF;
      aBuf[4] = (i>>24) & 0xFF;
      aBuf[5] = (i>>16) & 0xFF;
      aBuf[6] = (i>> 8) & 0xFF;
      aBuf[7] = (i>> 0) & 0xFF;
      sessionAppendBlob(p, aBuf, 8, pRc);
    }
    if( eType==SQLITE_BLOB || eType==SQLITE_TEXT ){
      int nByte = sqlite3_column_bytes(pStmt, iCol);
      sessionAppendVarint(p, nByte, pRc);
      sessionAppendBlob(p, eType==SQLITE_BLOB ? 
        sqlite3_column_blob(pStmt, iCol) : sqlite3_column_text(pStmt, iCol),
        nByte, pRc
      );
    }
  }
}

static void sessionAppendUpdate(
  sqlite3_stmt *pStmt, 
  SessionBuffer *pBuf,
  SessionChange *p,
  int *pRc
){
  if( *pRc==SQLITE_OK ){
    SessionBuffer buf2 = {0, 0, 0};
    int bNoop = 1;
    int i;
    u8 *pCsr = p->aRecord;
    sessionAppendByte(pBuf, SQLITE_UPDATE, pRc);
    for(i=0; i<sqlite3_column_count(pStmt); i++){
      int nCopy = 0;
      int nAdvance;
      int eType = *pCsr;
      switch( eType ){
        case SQLITE_NULL:
          nAdvance = 1;
          if( sqlite3_column_type(pStmt, i)!=SQLITE_NULL ){
            nCopy = 1;
          }
          break;

        case SQLITE_FLOAT:
        case SQLITE_INTEGER: {
          nAdvance = 9;
          if( eType==sqlite3_column_type(pStmt, i) ){
            sqlite3_int64 iVal = sessionGetI64(&pCsr[1]);
            if( eType==SQLITE_INTEGER ){
              if( iVal==sqlite3_column_int64(pStmt, i) ) break;
            }else{
              double dVal;
              memcpy(&dVal, &iVal, 8);
              if( dVal==sqlite3_column_double(pStmt, i) ) break;
            }
          }
          nCopy = 9;
          break;
        }

        case SQLITE_TEXT:
        case SQLITE_BLOB: {
          int nByte;
          int nHdr = 1 + sessionVarintGet(&pCsr[1], &nByte);
          nAdvance = nHdr + nByte;
          if( eType==sqlite3_column_type(pStmt, i) 
           && nByte==sqlite3_column_bytes(pStmt, i) 
           && 0==memcmp(&pCsr[nHdr], sqlite3_column_blob(pStmt, i), nByte)
          ){
            break;
          }
          nCopy = nAdvance;
        }
      }

      if( nCopy==0 ){
        sessionAppendByte(pBuf, 0, pRc);
        sessionAppendByte(&buf2, 0, pRc);
      }else{
        sessionAppendBlob(pBuf, pCsr, nCopy, pRc);
        sessionAppendCol(&buf2, pStmt, i, pRc);
        bNoop = 0;
      }
      pCsr += nAdvance;
    }

    if( bNoop ){
      pBuf->nBuf -= (1 + sqlite3_column_count(pStmt));
    }else{
      sessionAppendBlob(pBuf, buf2.aBuf, buf2.nBuf, pRc);
      sqlite3_free(buf2.aBuf);
    }
  }


}

/*
** Obtain a changeset object containing all changes recorded by the 
** session object passed as the first argument.
**
** It is the responsibility of the caller to eventually free the buffer 
** using sqlite3_free().
*/
int sqlite3session_changeset(
  sqlite3_session *pSession,      /* Session object */
  int *pnChangeset,               /* OUT: Size of buffer at *ppChangeset */
  void **ppChangeset              /* OUT: Buffer containing changeset */
){
  sqlite3 *db = pSession->db;
  SessionTable *pTab;
  SessionBuffer buf = {0, 0, 0};
  int rc;

  *pnChangeset = 0;
  *ppChangeset = 0;
  rc = pSession->rc;

  for(pTab=pSession->pTable; rc==SQLITE_OK && pTab; pTab=pTab->pNext){
    if( pTab->nEntry ){
      int i;
      sqlite3_stmt *pStmt = 0;
      int bNoop = 1;
      int nRewind = buf.nBuf;

      /* Write a table header */
      sessionAppendByte(&buf, 'T', &rc);
      sessionAppendVarint(&buf, pTab->nCol, &rc);
      sessionAppendBlob(&buf, (u8 *)pTab->zName, strlen(pTab->zName)+1, &rc);

      /* Build and compile a statement to execute: */
      if( rc==SQLITE_OK ){
        char *zSql = sqlite3_mprintf("SELECT * FROM %Q.%Q WHERE _rowid_ = ?", 
            pSession->zDb, pTab->zName
        );
        if( !zSql ){
          rc = SQLITE_NOMEM;
        }else{
          rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
        }
        sqlite3_free(zSql);
      }

      if( rc==SQLITE_OK && pTab->nCol!=sqlite3_column_count(pStmt) ){
        rc = SQLITE_SCHEMA;
      }

      for(i=0; i<pTab->nChange; i++){
        SessionChange *p;
        for(p=pTab->apChange[i]; rc==SQLITE_OK && p; p=p->pNext){
          sqlite3_bind_int64(pStmt, 1, p->iKey);
          if( sqlite3_step(pStmt)==SQLITE_ROW ){
            int iCol;
            if( p->aRecord ){
              sessionAppendUpdate(pStmt, &buf, p, &rc);
            }else{
              sessionAppendByte(&buf, SQLITE_INSERT, &rc);
              for(iCol=0; iCol<pTab->nCol; iCol++){
                sessionAppendCol(&buf, pStmt, iCol, &rc);
              }
            }
            bNoop = 0;
          }else if( p->aRecord ){
            /* A DELETE change */
            sessionAppendByte(&buf, SQLITE_DELETE, &rc);
            sessionAppendBlob(&buf, p->aRecord, p->nRecord, &rc);
            bNoop = 0;
          }
          rc = sqlite3_reset(pStmt);
        }
      }

      sqlite3_finalize(pStmt);

      if( bNoop ){
        buf.nBuf = nRewind;
      }
    }
  }

  if( rc==SQLITE_OK ){
    *pnChangeset = buf.nBuf;
    *ppChangeset = buf.aBuf;
  }else{
    sqlite3_free(buf.aBuf);
  }

  return rc;
}

int sqlite3session_enable(sqlite3_session *pSession, int bEnable){
  return bEnable;
}

/************************************************************************/
/************************************************************************/
/************************************************************************/

struct sqlite3_changeset_iter {
  u8 *aChangeset;                 /* Pointer to buffer containing changeset */
  int nChangeset;                 /* Number of bytes in aChangeset */
  u8 *pNext;                      /* Pointer to next change within aChangeset */
  int rc;

  char *zTab;                     /* Current table */
  int nCol;                       /* Number of columns in zTab */
  int op;                         /* Current operation */
  sqlite3_value **apValue;        /* old.* and new.* values */
};

/*
** Create an iterator used to iterate through the contents of a changeset.
*/
int sqlite3changeset_start(
  sqlite3_changeset_iter **ppIter,
  int nChangeset, 
  void *pChangeset
){
  sqlite3_changeset_iter *pRet;   /* Iterator to return */
  int nByte;                      /* Number of bytes to allocate for iterator */

  *ppIter = 0;

  nByte = sizeof(sqlite3_changeset_iter);
  pRet = (sqlite3_changeset_iter *)sqlite3_malloc(nByte);
  if( !pRet ) return SQLITE_NOMEM;
  memset(pRet, 0, sizeof(sqlite3_changeset_iter));

  pRet->aChangeset = (u8 *)pChangeset;
  pRet->nChangeset = nChangeset;
  pRet->pNext = pRet->aChangeset;

  *ppIter = pRet;
  return SQLITE_OK;
}

static int sessionReadRecord(
  u8 **paChange,                  /* IN/OUT: Pointer to binary record */
  int nCol,                       /* Number of values in record */
  sqlite3_value **apOut           /* Write values to this array */
){
  int i;
  u8 *aRec = *paChange;

  for(i=0; i<nCol; i++){
    int eType = *aRec++;
    assert( !apOut || apOut[i]==0 );
    if( eType ){
      if( apOut ){
        apOut[i] = sqlite3ValueNew(0);
        if( !apOut[i] ) return SQLITE_NOMEM;
      }

      if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
        int nByte;
        int enc = (eType==SQLITE_TEXT ? SQLITE_UTF8 : 0);
        aRec += sessionVarintGet(aRec, &nByte);
        if( apOut ){
          sqlite3ValueSetStr(apOut[i], nByte, aRec, enc, SQLITE_STATIC);
        }
        aRec += nByte;
      }
      if( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT ){
        if( apOut ){
          sqlite3_int64 v = sessionGetI64(aRec);
          if( eType==SQLITE_INTEGER ){
            sqlite3VdbeMemSetInt64(apOut[i], v);
          }else{
            double d;
            memcpy(&d, &i, 8);
            sqlite3VdbeMemSetDouble(apOut[i], d);
          }
        }
        aRec += 8;
      }
    }
  }

  *paChange = aRec;
  return SQLITE_OK;
}

/*
** Advance an iterator created by sqlite3changeset_start() to the next
** change in the changeset. This function may return SQLITE_ROW, SQLITE_DONE
** or SQLITE_CORRUPT.
**
** This function may not be called on iterators passed to a conflict handler
** callback by changeset_apply().
*/
int sqlite3changeset_next(sqlite3_changeset_iter *p){
  u8 *aChange;
  int i;
  u8 c;

  if( p->rc!=SQLITE_OK ) return p->rc;

  if( p->apValue ){
    for(i=0; i<p->nCol*2; i++){
      sqlite3ValueFree(p->apValue[i]);
    }
    memset(p->apValue, 0, sizeof(sqlite3_value*)*p->nCol*2);
  }

  /* If the iterator is already at the end of the changeset, return DONE. */
  if( p->pNext>=&p->aChangeset[p->nChangeset] ){
    return SQLITE_DONE;
  }
  aChange = p->pNext;

  c = *(aChange++);
  if( c=='T' ){
    int nByte;                    /* Bytes to allocate for apValue */
    aChange += sessionVarintGet(aChange, &p->nCol);
    p->zTab = (char *)aChange;
    aChange += (strlen((char *)aChange) + 1);
    p->op = *(aChange++);
    sqlite3_free(p->apValue);
    nByte = sizeof(sqlite3_value *) * p->nCol * 2;
    p->apValue = (sqlite3_value **)sqlite3_malloc(nByte);
    if( !p->apValue ){
      return (p->rc = SQLITE_NOMEM);
    }
    memset(p->apValue, 0, sizeof(sqlite3_value*)*p->nCol*2);
  }else{
    p->op = c;
  }
  if( p->op!=SQLITE_UPDATE && p->op!=SQLITE_DELETE && p->op!=SQLITE_INSERT ){
    return (p->rc = SQLITE_CORRUPT);
  }

  /* If this is an UPDATE or DELETE, read the old.* record. */
  if( p->op!=SQLITE_INSERT ){
    p->rc = sessionReadRecord(&aChange, p->nCol, p->apValue);
    if( p->rc!=SQLITE_OK ) return p->rc;
  }

  /* If this is an INSERT or UPDATE, read the new.* record. */
  if( p->op!=SQLITE_DELETE ){
    p->rc = sessionReadRecord(&aChange, p->nCol, &p->apValue[p->nCol]);
    if( p->rc!=SQLITE_OK ) return p->rc;
  }

  p->pNext = aChange;
  return SQLITE_ROW;
}

/*
** The following three functions extract information on the current change
** from a changeset iterator. They may only be called after changeset_next()
** has returned SQLITE_ROW.
*/
int sqlite3changeset_op(
  sqlite3_changeset_iter *pIter,
  const char **pzTab,                 /* OUT: Pointer to table name */
  int *pnCol,                         /* OUT: Number of columns in table */
  int *pOp                            /* OUT: SQLITE_INSERT, DELETE or UPDATE */
){
  *pOp = pIter->op;
  *pnCol = pIter->nCol;
  *pzTab = pIter->zTab;
  return SQLITE_OK;
}

int sqlite3changeset_old(
  sqlite3_changeset_iter *pIter,
  int iVal,
  sqlite3_value **ppValue             /* OUT: Old value (or NULL pointer) */
){
  if( iVal<0 || iVal>=pIter->nCol ){
    return SQLITE_RANGE;
  }
  *ppValue = pIter->apValue[iVal];
  return SQLITE_OK;
}

int sqlite3changeset_new(
  sqlite3_changeset_iter *pIter,
  int iVal,
  sqlite3_value **ppValue             /* OUT: New value (or NULL pointer) */
){
  if( iVal<0 || iVal>=pIter->nCol ){
    return SQLITE_RANGE;
  }
  *ppValue = pIter->apValue[pIter->nCol+iVal];
  return SQLITE_OK;
}

/*
** Finalize an iterator allocated with sqlite3changeset_start().
**
** This function may not be called on iterators passed to a conflict handler
** callback by changeset_apply().
*/
int sqlite3changeset_finalize(sqlite3_changeset_iter *p){
  int i;
  int rc = p->rc;
  for(i=0; i<p->nCol*2; i++) sqlite3ValueFree(p->apValue[i]);
  sqlite3_free(p->apValue);
  sqlite3_free(p);
  return rc;
}

/*
** Invert a changeset object.
*/
int sqlite3changeset_invert(
  int nChangeset,                 /* Number of bytes in input */
  void *pChangeset,               /* Input changeset */
  int *pnInverted,                /* OUT: Number of bytes in output changeset */
  void **ppInverted               /* OUT: Inverse of pChangeset */
){
  u8 *aOut;
  u8 *aIn;
  int i;
  int nCol = 0;

  /* Zero the output variables in case an error occurs. */
  *ppInverted = 0;
  *pnInverted = 0;
  if( nChangeset==0 ) return SQLITE_OK;

  aOut = (u8 *)sqlite3_malloc(nChangeset);
  if( !aOut ) return SQLITE_NOMEM;
  aIn = (u8 *)pChangeset;

  i = 0;
  while( i<nChangeset ){
    u8 eType = aIn[i];
    switch( eType ){
      case 'T': {
        int nByte = 1 + sessionVarintGet(&aIn[i+1], &nCol);
        nByte += 1 + strlen((char *)&aIn[i+nByte]);
        memcpy(&aOut[i], &aIn[i], nByte);
        i += nByte;
        break;
      }

      case SQLITE_INSERT:
      case SQLITE_DELETE: {
        int nByte;
        u8 *aEnd = &aIn[i+1];

        sessionReadRecord(&aEnd, nCol, 0);
        aOut[i] = (eType==SQLITE_DELETE ? SQLITE_INSERT : SQLITE_DELETE);
        nByte = aEnd - &aIn[i+1];
        memcpy(&aOut[i+1], &aIn[i+1], nByte);
        i += 1 + nByte;
        break;
      }

      case SQLITE_UPDATE: {
        int nByte1;              /* Size of old.* record in bytes */
        int nByte2;              /* Size of new.* record in bytes */
        u8 *aEnd = &aIn[i+1];    

        sessionReadRecord(&aEnd, nCol, 0);
        nByte1 = aEnd - &aIn[i+1];
        sessionReadRecord(&aEnd, nCol, 0);
        nByte2 = aEnd - &aIn[i+1] - nByte1;

        aOut[i] = SQLITE_UPDATE;
        memcpy(&aOut[i+1], &aIn[i+1+nByte1], nByte2);
        memcpy(&aOut[i+1+nByte2], &aIn[i+1], nByte1);

        i += 1 + nByte1 + nByte2;
        break;
      }

      default:
        sqlite3_free(aOut);
        return SQLITE_CORRUPT;
    }
  }

  *pnInverted = nChangeset;
  *ppInverted = (void *)aOut;
  return SQLITE_OK;
}


#endif        /* #ifdef SQLITE_ENABLE_SESSION */
