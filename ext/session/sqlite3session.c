
#if defined(SQLITE_ENABLE_SESSION) && defined(SQLITE_ENABLE_PREUPDATE_HOOK)
#include "sqlite3session.h"
#include <assert.h>
#include <string.h>

#ifndef SQLITE_AMALGAMATION
# include "sqliteInt.h"
# include "vdbeInt.h"
#endif

typedef struct SessionTable SessionTable;
typedef struct SessionChange SessionChange;
typedef struct SessionBuffer SessionBuffer;

/*
** Session handle structure.
*/
struct sqlite3_session {
  sqlite3 *db;                    /* Database handle session is attached to */
  char *zDb;                      /* Name of database session is attached to */
  int bEnable;                    /* True if currently recording */
  int bIndirect;                  /* True if all changes are indirect */
  int bAutoAttach;                /* True to auto-attach tables */
  int rc;                         /* Non-zero if an error has occurred */
  sqlite3_session *pNext;         /* Next session object on same db. */
  SessionTable *pTable;           /* List of attached tables */
};

/*
** Structure for changeset iterators.
*/
struct sqlite3_changeset_iter {
  u8 *aChangeset;                 /* Pointer to buffer containing changeset */
  int nChangeset;                 /* Number of bytes in aChangeset */
  u8 *pNext;                      /* Pointer to next change within aChangeset */
  int rc;                         /* Iterator error code */
  sqlite3_stmt *pConflict;        /* Points to conflicting row, if any */
  char *zTab;                     /* Current table */
  int nCol;                       /* Number of columns in zTab */
  int op;                         /* Current operation */
  int bIndirect;                  /* True if current change was indirect */
  u8 *abPK;                       /* Primary key array */
  sqlite3_value **apValue;        /* old.* and new.* values */
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
  const char **azCol;             /* Column names */
  u8 *abPK;                       /* Array of primary key flags */
  int nEntry;                     /* Total number of entries in hash table */
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
**   1 byte: The "indirect-change" flag.
**   old.* record: (delete and update only)
**   new.* record: (insert and update only)
*/

/*
** For each row modified during a session, there exists a single instance of
** this structure stored in a SessionTable.aChange[] hash table.
*/
struct SessionChange {
  int op;                         /* One of UPDATE, DELETE, INSERT */
  int bIndirect;                  /* True if this change is "indirect" */
  int nRecord;                    /* Number of bytes in buffer aRecord[] */
  u8 *aRecord;                    /* Buffer containing old.* record */
  SessionChange *pNext;           /* For hash-table collisions */
};

/*
** Instances of this structure are used to build strings or binary records.
*/
struct SessionBuffer {
  u8 *aBuf;                       /* Pointer to changeset buffer */
  int nBuf;                       /* Size of buffer aBuf */
  int nAlloc;                     /* Size of allocation containing aBuf */
};

/*
** Write a varint with value iVal into the buffer at aBuf. Return the 
** number of bytes written.
*/
static int sessionVarintPut(u8 *aBuf, int iVal){
  return putVarint32(aBuf, iVal);
}

/*
** Return the number of bytes required to store value iVal as a varint.
*/
static int sessionVarintLen(int iVal){
  return sqlite3VarintLen(iVal);
}

/*
** Read a varint value from aBuf[] into *piVal. Return the number of 
** bytes read.
*/
static int sessionVarintGet(u8 *aBuf, int *piVal){
  return getVarint32(aBuf, *piVal);
}

/*
** Read a 64-bit big-endian integer value from buffer aRec[]. Return
** the value read.
*/
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
** Write a 64-bit big-endian integer value to the buffer aBuf[].
*/
static void sessionPutI64(u8 *aBuf, sqlite3_int64 i){
  aBuf[0] = (i>>56) & 0xFF;
  aBuf[1] = (i>>48) & 0xFF;
  aBuf[2] = (i>>40) & 0xFF;
  aBuf[3] = (i>>32) & 0xFF;
  aBuf[4] = (i>>24) & 0xFF;
  aBuf[5] = (i>>16) & 0xFF;
  aBuf[6] = (i>> 8) & 0xFF;
  aBuf[7] = (i>> 0) & 0xFF;
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
  int nByte;                      /* Size of serialized value in bytes */

  if( pValue ){
    int eType;                    /* Value type (SQLITE_NULL, TEXT etc.) */
  
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
          sessionPutI64(&aBuf[1], i);
        }
        nByte = 9; 
        break;
  
      default: {
        u8 *z;
        int n;
        int nVarint;
  
        assert( eType==SQLITE_TEXT || eType==SQLITE_BLOB );
        if( eType==SQLITE_TEXT ){
          z = (u8 *)sqlite3_value_text(pValue);
        }else{
          z = (u8 *)sqlite3_value_blob(pValue);
        }
        if( z==0 ) return SQLITE_NOMEM;
        n = sqlite3_value_bytes(pValue);
        nVarint = sessionVarintLen(n);
  
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
  }else{
    nByte = 1;
    if( aBuf ) aBuf[0] = '\0';
  }

  *pnWrite += nByte;
  return SQLITE_OK;
}

/*
** This macro is used to calculate hash key values for data structures. In
** order to use this macro, the entire data structure must be represented
** as a series of unsigned integers. In order to calculate a hash-key value
** for a data structure represented as three such integers, the macro may
** then be used as follows:
**
**    int hash_key_value;
**    hash_key_value = HASH_APPEND(0, <value 1>);
**    hash_key_value = HASH_APPEND(hash_key_value, <value 2>);
**    hash_key_value = HASH_APPEND(hash_key_value, <value 3>);
**
** In practice, the data structures this macro is used for are the primary
** key values of modified rows.
*/
#define HASH_APPEND(hash, add) ((hash) << 3) ^ (hash) ^ (unsigned int)(add)

/*
** Append the hash of the 64-bit integer passed as the second argument to the
** hash-key value passed as the first. Return the new hash-key value.
*/
static unsigned int sessionHashAppendI64(unsigned int h, i64 i){
  h = HASH_APPEND(h, i & 0xFFFFFFFF);
  return HASH_APPEND(h, (i>>32)&0xFFFFFFFF);
}

/*
** Append the hash of the blob passed via the second and third arguments to 
** the hash-key value passed as the first. Return the new hash-key value.
*/
static unsigned int sessionHashAppendBlob(unsigned int h, int n, const u8 *z){
  int i;
  for(i=0; i<n; i++) h = HASH_APPEND(h, z[i]);
  return h;
}

/*
** Append the hash of the data type passed as the second argument to the
** hash-key value passed as the first. Return the new hash-key value.
*/
static unsigned int sessionHashAppendType(unsigned int h, int eType){
  return HASH_APPEND(h, eType);
}

/*
** This function may only be called from within a pre-update callback.
** It calculates a hash based on the primary key values of the old.* or 
** new.* row currently available and, assuming no error occurs, writes it to
** *piHash before returning. If the primary key contains one or more NULL
** values, *pbNullPK is set to true before returning.
**
** If an error occurs, an SQLite error code is returned and the final values
** of *piHash asn *pbNullPK are undefined. Otherwise, SQLITE_OK is returned
** and the output variables are set as described above.
*/
static int sessionPreupdateHash(
  sqlite3 *db,                    /* Database handle */
  SessionTable *pTab,             /* Session table handle */
  int bNew,                       /* True to hash the new.* PK */
  int *piHash,                    /* OUT: Hash value */
  int *pbNullPK                   /* OUT: True if there are NULL values in PK */
){
  unsigned int h = 0;             /* Hash value to return */
  int i;                          /* Used to iterate through columns */

  assert( *pbNullPK==0 );
  assert( pTab->nCol==sqlite3_preupdate_count(db) );
  for(i=0; i<pTab->nCol; i++){
    if( pTab->abPK[i] ){
      int rc;
      int eType;
      sqlite3_value *pVal;

      if( bNew ){
        rc = sqlite3_preupdate_new(db, i, &pVal);
      }else{
        rc = sqlite3_preupdate_old(db, i, &pVal);
      }
      if( rc!=SQLITE_OK ) return rc;

      eType = sqlite3_value_type(pVal);
      h = sessionHashAppendType(h, eType);
      if( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT ){
        i64 iVal;
        if( eType==SQLITE_INTEGER ){
          iVal = sqlite3_value_int64(pVal);
        }else{
          double rVal = sqlite3_value_double(pVal);
          assert( sizeof(iVal)==8 && sizeof(rVal)==8 );
          memcpy(&iVal, &rVal, 8);
        }
        h = sessionHashAppendI64(h, iVal);
      }else if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
        const u8 *z;
        if( eType==SQLITE_TEXT ){
          z = (const u8 *)sqlite3_value_text(pVal);
        }else{
          z = (const u8 *)sqlite3_value_blob(pVal);
        }
        if( !z ) return SQLITE_NOMEM;
        h = sessionHashAppendBlob(h, sqlite3_value_bytes(pVal), z);
      }else{
        assert( eType==SQLITE_NULL );
        *pbNullPK = 1;
      }
    }
  }

  *piHash = (h % pTab->nChange);
  return SQLITE_OK;
}

/*
** The buffer that the argument points to contains a serialized SQL value.
** Return the number of bytes of space occupied by the value (including
** the type byte).
*/
static int sessionSerialLen(u8 *a){
  int e = *a;
  int n;
  if( e==0 ) return 1;
  if( e==SQLITE_NULL ) return 1;
  if( e==SQLITE_INTEGER || e==SQLITE_FLOAT ) return 9;
  return sessionVarintGet(&a[1], &n) + 1 + n;
}

/*
** Based on the primary key values stored in change aRecord, calculate a
** hash key. Assume the has table has nBucket buckets. The hash keys
** calculated by this function are compatible with those calculated by
** sessionPreupdateHash().
*/
static unsigned int sessionChangeHash(
  SessionTable *pTab,             /* Table handle */
  u8 *aRecord,                    /* Change record */
  int nBucket                     /* Assume this many buckets in hash table */
){
  unsigned int h = 0;             /* Value to return */
  int i;                          /* Used to iterate through columns */
  u8 *a = aRecord;                /* Used to iterate through change record */

  for(i=0; i<pTab->nCol; i++){
    int eType = *a;
    int isPK = pTab->abPK[i];

    /* It is not possible for eType to be SQLITE_NULL here. The session 
    ** module does not record changes for rows with NULL values stored in
    ** primary key columns. */
    assert( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT 
         || eType==SQLITE_TEXT || eType==SQLITE_BLOB 
         || eType==SQLITE_NULL || eType==0 
    );
    assert( !isPK || (eType!=0 && eType!=SQLITE_NULL) );

    if( isPK ){
      a++;
      h = sessionHashAppendType(h, eType);
      if( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT ){
        h = sessionHashAppendI64(h, sessionGetI64(a));
        a += 8;
      }else{
        int n; 
        a += sessionVarintGet(a, &n);
        h = sessionHashAppendBlob(h, n, a);
        a += n;
      }
    }else{
      a += sessionSerialLen(a);
    }
  }
  return (h % nBucket);
}

/*
** Arguments aLeft and aRight are pointers to change records for table pTab.
** This function returns true if the two records apply to the same row (i.e.
** have the same values stored in the primary key columns), or false 
** otherwise.
*/
static int sessionChangeEqual(
  SessionTable *pTab,             /* Table used for PK definition */
  u8 *aLeft,                      /* Change record */
  u8 *aRight                      /* Change record */
){
  u8 *a1 = aLeft;                 /* Cursor to iterate through aLeft */
  u8 *a2 = aRight;                /* Cursor to iterate through aRight */
  int iCol;                       /* Used to iterate through table columns */

  for(iCol=0; iCol<pTab->nCol; iCol++){
    int n1 = sessionSerialLen(a1);
    int n2 = sessionSerialLen(a2);

    if( pTab->abPK[iCol] && (n1!=n2 || memcmp(a1, a2, n1)) ){
      return 0;
    }
    a1 += n1;
    a2 += n2;
  }

  return 1;
}

/*
** Arguments aLeft and aRight both point to buffers containing change
** records with nCol columns. This function "merges" the two records into
** a single records which is written to the buffer at *paOut. *paOut is
** then set to point to one byte after the last byte written before 
** returning.
**
** The merging of records is done as follows: For each column, if the 
** aRight record contains a value for the column, copy the value from
** their. Otherwise, if aLeft contains a value, copy it. If neither
** record contains a value for a given column, then neither does the
** output record.
*/
static void sessionMergeRecord(
  u8 **paOut, 
  int nCol,
  u8 *aLeft,
  u8 *aRight
){
  u8 *a1 = aLeft;                 /* Cursor used to iterate through aLeft */
  u8 *a2 = aRight;                /* Cursor used to iterate through aRight */
  u8 *aOut = *paOut;              /* Output cursor */
  int iCol;                       /* Used to iterate from 0 to nCol */

  for(iCol=0; iCol<nCol; iCol++){
    int n1 = sessionSerialLen(a1);
    int n2 = sessionSerialLen(a2);
    if( *a2 ){
      memcpy(aOut, a2, n2);
      aOut += n2;
    }else{
      memcpy(aOut, a1, n1);
      aOut += n1;
    }
    a1 += n1;
    a2 += n2;
  }

  *paOut = aOut;
}

/*
** This is a helper function used by sessionMergeUpdate().
**
** When this function is called, both *paOne and *paTwo point to a value 
** within a change record. Before it returns, both have been advanced so 
** as to point to the next value in the record.
**
** If, when this function is called, *paTwo points to a valid value (i.e.
** *paTwo[0] is not 0x00 - the "no value" placeholder), a copy of the *paOne
** pointer is returned and *pnVal is set to the number of bytes in the 
** serialized value. Otherwise, a copy of *paOne is returned and *pnVal
** set to the number of bytes in the value at *paOne. If *paOne points
** to the "no value" placeholder, *pnVal is set to 1.
*/
static u8 *sessionMergeValue(
  u8 **paOne,                     /* IN/OUT: Left-hand buffer pointer */
  u8 **paTwo,                     /* IN/OUT: Right-hand buffer pointer */
  int *pnVal                      /* OUT: Bytes in returned value */
){
  u8 *a1 = *paOne;
  u8 *a2 = *paTwo;
  u8 *pRet = 0;
  int n1;

  assert( a1 );
  if( a2 ){
    int n2 = sessionSerialLen(a2);
    if( *a2 ){
      *pnVal = n2;
      pRet = a2;
    }
    *paTwo = &a2[n2];
  }

  n1 = sessionSerialLen(a1);
  if( pRet==0 ){
    *pnVal = n1;
    pRet = a1;
  }
  *paOne = &a1[n1];

  return pRet;
}

/*
** This function is used by changeset_concat() to merge two UPDATE changes
** on the same row.
*/
static int sessionMergeUpdate(
  u8 **paOut,                     /* IN/OUT: Pointer to output buffer */
  SessionTable *pTab,             /* Table change pertains to */
  u8 *aOldRecord1,                /* old.* record for first change */
  u8 *aOldRecord2,                /* old.* record for second change */
  u8 *aNewRecord1,                /* new.* record for first change */
  u8 *aNewRecord2                 /* new.* record for second change */
){
  u8 *aOld1 = aOldRecord1;
  u8 *aOld2 = aOldRecord2;
  u8 *aNew1 = aNewRecord1;
  u8 *aNew2 = aNewRecord2;

  u8 *aOut = *paOut;
  int i;
  int bRequired = 0;

  assert( aOldRecord1 && aNewRecord1 );

  /* Write the old.* vector first. */
  for(i=0; i<pTab->nCol; i++){
    int nOld;
    u8 *aOld;
    int nNew;
    u8 *aNew;

    aOld = sessionMergeValue(&aOld1, &aOld2, &nOld);
    aNew = sessionMergeValue(&aNew1, &aNew2, &nNew);
    if( pTab->abPK[i] || nOld!=nNew || memcmp(aOld, aNew, nNew) ){
      if( pTab->abPK[i]==0 ) bRequired = 1;
      memcpy(aOut, aOld, nOld);
      aOut += nOld;
    }else{
      *(aOut++) = '\0';
    }
  }

  if( !bRequired ) return 0;

  /* Write the new.* vector */
  aOld1 = aOldRecord1;
  aOld2 = aOldRecord2;
  aNew1 = aNewRecord1;
  aNew2 = aNewRecord2;
  for(i=0; i<pTab->nCol; i++){
    int nOld;
    u8 *aOld;
    int nNew;
    u8 *aNew;

    aOld = sessionMergeValue(&aOld1, &aOld2, &nOld);
    aNew = sessionMergeValue(&aNew1, &aNew2, &nNew);
    if( pTab->abPK[i] || (nOld==nNew && 0==memcmp(aOld, aNew, nNew)) ){
      *(aOut++) = '\0';
    }else{
      memcpy(aOut, aNew, nNew);
      aOut += nNew;
    }
  }

  *paOut = aOut;
  return 1;
}

/*
** This function is only called from within a pre-update-hook callback.
** It determines if the current pre-update-hook change affects the same row
** as the change stored in argument pChange. If so, it returns true. Otherwise
** if the pre-update-hook does not affect the same row as pChange, it returns
** false.
*/
static int sessionPreupdateEqual(
  sqlite3 *db,                    /* Database handle */
  SessionTable *pTab,             /* Table associated with change */
  SessionChange *pChange,         /* Change to compare to */
  int op                          /* Current pre-update operation */
){
  int iCol;                       /* Used to iterate through columns */
  u8 *a = pChange->aRecord;       /* Cursor used to scan change record */

  assert( op==SQLITE_INSERT || op==SQLITE_UPDATE || op==SQLITE_DELETE );
  for(iCol=0; iCol<pTab->nCol; iCol++){
    if( !pTab->abPK[iCol] ){
      a += sessionSerialLen(a);
    }else{
      sqlite3_value *pVal;        /* Value returned by preupdate_new/old */
      int rc;                     /* Error code from preupdate_new/old */
      int eType = *a++;           /* Type of value from change record */

      /* The following calls to preupdate_new() and preupdate_old() can not
      ** fail. This is because they cache their return values, and by the
      ** time control flows to here they have already been called once from
      ** within sessionPreupdateHash(). The first two asserts below verify
      ** this (that the method has already been called). */
      if( op==SQLITE_INSERT ){
        assert( db->pPreUpdate->pNewUnpacked || db->pPreUpdate->aNew );
        rc = sqlite3_preupdate_new(db, iCol, &pVal);
      }else{
        assert( db->pPreUpdate->pUnpacked );
        rc = sqlite3_preupdate_old(db, iCol, &pVal);
      }
      assert( rc==SQLITE_OK );
      if( sqlite3_value_type(pVal)!=eType ) return 0;

      /* A SessionChange object never has a NULL value in a PK column */
      assert( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT
           || eType==SQLITE_BLOB    || eType==SQLITE_TEXT
      );

      if( eType==SQLITE_INTEGER || eType==SQLITE_FLOAT ){
        i64 iVal = sessionGetI64(a);
        a += 8;
        if( eType==SQLITE_INTEGER ){
          if( sqlite3_value_int64(pVal)!=iVal ) return 0;
        }else{
          double rVal;
          assert( sizeof(iVal)==8 && sizeof(rVal)==8 );
          memcpy(&rVal, &iVal, 8);
          if( sqlite3_value_double(pVal)!=rVal ) return 0;
        }
      }else{
        int n;
        const u8 *z;
        a += sessionVarintGet(a, &n);
        if( sqlite3_value_bytes(pVal)!=n ) return 0;
        if( eType==SQLITE_TEXT ){
          z = sqlite3_value_text(pVal);
        }else{
          z = sqlite3_value_blob(pVal);
        }
        if( memcmp(a, z, n) ) return 0;
        a += n;
        break;
      }
    }
  }

  return 1;
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
static int sessionGrowHash(SessionTable *pTab){
  if( pTab->nChange==0 || pTab->nEntry>=(pTab->nChange/2) ){
    int i;
    SessionChange **apNew;
    int nNew = (pTab->nChange ? pTab->nChange : 128) * 2;

    apNew = (SessionChange **)sqlite3_malloc(sizeof(SessionChange *) * nNew);
    if( apNew==0 ){
      if( pTab->nChange==0 ){
        return SQLITE_ERROR;
      }
      return SQLITE_OK;
    }
    memset(apNew, 0, sizeof(SessionChange *) * nNew);

    for(i=0; i<pTab->nChange; i++){
      SessionChange *p;
      SessionChange *pNext;
      for(p=pTab->apChange[i]; p; p=pNext){
        int iHash = sessionChangeHash(pTab, p->aRecord, nNew);
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

/*
** This function queries the database for the names of the columns of table
** zThis, in schema zDb. It is expected that the table has nCol columns. If
** not, SQLITE_SCHEMA is returned and none of the output variables are
** populated.
**
** Otherwise, if they are not NULL, variable *pnCol is set to the number
** of columns in the database table and variable *pzTab is set to point to a
** nul-terminated copy of the table name. *pazCol (if not NULL) is set to
** point to an array of pointers to column names. And *pabPK (again, if not
** NULL) is set to point to an array of booleans - true if the corresponding
** column is part of the primary key.
**
** For example, if the table is declared as:
**
**     CREATE TABLE tbl1(w, x, y, z, PRIMARY KEY(w, z));
**
** Then the four output variables are populated as follows:
**
**     *pnCol  = 4
**     *pzTab  = "tbl1"
**     *pazCol = {"w", "x", "y", "z"}
**     *pabPK  = {1, 0, 0, 1}
**
** All returned buffers are part of the same single allocation, which must
** be freed using sqlite3_free() by the caller. If pazCol was not NULL, then
** pointer *pazCol should be freed to release all memory. Otherwise, pointer
** *pabPK. It is illegal for both pazCol and pabPK to be NULL.
*/
static int sessionTableInfo(
  sqlite3 *db,                    /* Database connection */
  const char *zDb,                /* Name of attached database (e.g. "main") */
  const char *zThis,              /* Table name */
  int *pnCol,                     /* OUT: number of columns */
  const char **pzTab,             /* OUT: Copy of zThis */
  const char ***pazCol,           /* OUT: Array of column names for table */
  u8 **pabPK                      /* OUT: Array of booleans - true for PK col */
){
  char *zPragma;
  sqlite3_stmt *pStmt;
  int rc;
  int nByte;
  int nDbCol = 0;
  int nThis;
  int i;
  u8 *pAlloc;
  char **azCol = 0;
  u8 *abPK;

  assert( pazCol && pabPK );

  nThis = sqlite3Strlen30(zThis);
  zPragma = sqlite3_mprintf("PRAGMA '%q'.table_info('%q')", zDb, zThis);
  if( !zPragma ) return SQLITE_NOMEM;

  rc = sqlite3_prepare_v2(db, zPragma, -1, &pStmt, 0);
  sqlite3_free(zPragma);
  if( rc!=SQLITE_OK ) return rc;

  nByte = nThis + 1;
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    nByte += sqlite3_column_bytes(pStmt, 1);
    nDbCol++;
  }
  rc = sqlite3_reset(pStmt);

  if( rc==SQLITE_OK ){
    nByte += nDbCol * (sizeof(const char *) + sizeof(u8) + 1);
    pAlloc = sqlite3_malloc(nByte);
    if( pAlloc==0 ){
      rc = SQLITE_NOMEM;
    }
  }
  if( rc==SQLITE_OK ){
    azCol = (char **)pAlloc;
    pAlloc = (u8 *)&azCol[nDbCol];
    abPK = (u8 *)pAlloc;
    pAlloc = &abPK[nDbCol];
    if( pzTab ){
      memcpy(pAlloc, zThis, nThis+1);
      *pzTab = (char *)pAlloc;
      pAlloc += nThis+1;
    }
  
    i = 0;
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      int nName = sqlite3_column_bytes(pStmt, 1);
      const unsigned char *zName = sqlite3_column_text(pStmt, 1);
      if( zName==0 ) break;
      memcpy(pAlloc, zName, nName+1);
      azCol[i] = (char *)pAlloc;
      pAlloc += nName+1;
      abPK[i] = sqlite3_column_int(pStmt, 5);
      i++;
    }
    rc = sqlite3_reset(pStmt);
  
  }

  /* If successful, populate the output variables. Otherwise, zero them and
  ** free any allocation made. An error code will be returned in this case.
  */
  if( rc==SQLITE_OK ){
    *pazCol = (const char **)azCol;
    *pabPK = abPK;
    *pnCol = nDbCol;
  }else{
    *pazCol = 0;
    *pabPK = 0;
    *pnCol = 0;
    if( pzTab ) *pzTab = 0;
    sqlite3_free(azCol);
  }
  sqlite3_finalize(pStmt);
  return rc;
}

/*
** This function is only called from within a pre-update handler for a
** write to table pTab, part of session pSession. If this is the first
** write to this table, set the SessionTable.nCol variable to the number
** of columns in the table.
**
** Otherwise, if this is not the first time this table has been written
** to, check that the number of columns in the table has not changed. If
** it has not, return zero.
**
** If the number of columns in the table has changed since the last write
** was recorded, set the session error-code to SQLITE_SCHEMA and return
** non-zero. Users are not allowed to change the number of columns in a table
** for which changes are being recorded by the session module. If they do so, 
** it is an error.
*/
static int sessionInitTable(sqlite3_session *pSession, SessionTable *pTab){
  if( pTab->nCol==0 ){
    assert( pTab->azCol==0 || pTab->abPK==0 );
    pSession->rc = sessionTableInfo(pSession->db, pSession->zDb, 
        pTab->zName, &pTab->nCol, 0, &pTab->azCol, &pTab->abPK
    );
  }
  if( pSession->rc==SQLITE_OK 
   && pTab->nCol!=sqlite3_preupdate_count(pSession->db) 
  ){
    pSession->rc = SQLITE_SCHEMA;
  }
  return pSession->rc;
}

/*
** This function is only called from with a pre-update-hook reporting a 
** change on table pTab (attached to session pSession). The type of change
** (UPDATE, INSERT, DELETE) is specified by the first argument.
**
** Unless one is already present or an error occurs, an entry is added
** to the changed-rows hash table associated with table pTab.
*/
static void sessionPreupdateOneChange(
  int op,                         /* One of SQLITE_UPDATE, INSERT, DELETE */
  sqlite3_session *pSession,      /* Session object pTab is attached to */
  SessionTable *pTab              /* Table that change applies to */
){
  sqlite3 *db = pSession->db;
  int iHash; 
  int bNullPk = 0; 
  int rc = SQLITE_OK;

  if( pSession->rc ) return;

  /* Load table details if required */
  if( sessionInitTable(pSession, pTab) ) return;

  /* Grow the hash table if required */
  if( sessionGrowHash(pTab) ){
    pSession->rc = SQLITE_NOMEM;
    return;
  }

  /* Calculate the hash-key for this change. If the primary key of the row
  ** includes a NULL value, exit early. Such changes are ignored by the
  ** session module. */
  rc = sessionPreupdateHash(db, pTab, op==SQLITE_INSERT, &iHash, &bNullPk);
  if( rc!=SQLITE_OK ) goto error_out;

  if( bNullPk==0 ){
    /* Search the hash table for an existing record for this row. */
    SessionChange *pC;
    for(pC=pTab->apChange[iHash]; pC; pC=pC->pNext){
      if( sessionPreupdateEqual(db, pTab, pC, op) ) break;
    }

    if( pC==0 ){
      /* Create a new change object containing all the old values (if
      ** this is an SQLITE_UPDATE or SQLITE_DELETE), or just the PK
      ** values (if this is an INSERT). */
      SessionChange *pChange; /* New change object */
      int nByte;              /* Number of bytes to allocate */
      int i;                  /* Used to iterate through columns */
  
      assert( rc==SQLITE_OK );
      pTab->nEntry++;
  
      /* Figure out how large an allocation is required */
      nByte = sizeof(SessionChange);
      for(i=0; i<pTab->nCol; i++){
        sqlite3_value *p = 0;
        if( op!=SQLITE_INSERT ){
          TESTONLY(int trc = ) sqlite3_preupdate_old(pSession->db, i, &p);
          assert( trc==SQLITE_OK );
        }else if( pTab->abPK[i] ){
          TESTONLY(int trc = ) sqlite3_preupdate_new(pSession->db, i, &p);
          assert( trc==SQLITE_OK );
        }

        /* This may fail if SQLite value p contains a utf-16 string that must
        ** be converted to utf-8 and an OOM error occurs while doing so. */
        rc = sessionSerializeValue(0, p, &nByte);
        if( rc!=SQLITE_OK ) goto error_out;
      }
  
      /* Allocate the change object */
      pChange = (SessionChange *)sqlite3_malloc(nByte);
      if( !pChange ){
        rc = SQLITE_NOMEM;
        goto error_out;
      }else{
        memset(pChange, 0, sizeof(SessionChange));
        pChange->aRecord = (u8 *)&pChange[1];
      }
  
      /* Populate the change object. None of the preupdate_old(),
      ** preupdate_new() or SerializeValue() calls below may fail as all
      ** required values and encodings have already been cached in memory.
      ** It is not possible for an OOM to occur in this block. */
      nByte = 0;
      for(i=0; i<pTab->nCol; i++){
        sqlite3_value *p = 0;
        if( op!=SQLITE_INSERT ){
          sqlite3_preupdate_old(pSession->db, i, &p);
        }else if( pTab->abPK[i] ){
          sqlite3_preupdate_new(pSession->db, i, &p);
        }
        sessionSerializeValue(&pChange->aRecord[nByte], p, &nByte);
      }

      /* Add the change to the hash-table */
      if( pSession->bIndirect || sqlite3_preupdate_depth(pSession->db) ){
        pChange->bIndirect = 1;
      }
      pChange->nRecord = nByte;
      pChange->op = op;
      pChange->pNext = pTab->apChange[iHash];
      pTab->apChange[iHash] = pChange;

    }else if( pC->bIndirect ){
      /* If the existing change is considered "indirect", but this current
      ** change is "direct", mark the change object as direct. */
      if( sqlite3_preupdate_depth(pSession->db)==0 && pSession->bIndirect==0 ){
        pC->bIndirect = 0;
      }
    }
  }

  /* If an error has occurred, mark the session object as failed. */
 error_out:
  if( rc!=SQLITE_OK ){
    pSession->rc = rc;
  }
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
  int nDb = sqlite3Strlen30(zDb);
  int nName = sqlite3Strlen30(zName);

  assert( sqlite3_mutex_held(db->mutex) );

  for(pSession=(sqlite3_session *)pCtx; pSession; pSession=pSession->pNext){
    SessionTable *pTab;

    /* If this session is attached to a different database ("main", "temp" 
    ** etc.), or if it is not currently enabled, there is nothing to do. Skip 
    ** to the next session object attached to this database. */
    if( pSession->bEnable==0 ) continue;
    if( pSession->rc ) continue;
    if( sqlite3_strnicmp(zDb, pSession->zDb, nDb+1) ) continue;

    for(pTab=pSession->pTable; pTab || pSession->bAutoAttach; pTab=pTab->pNext){
      if( !pTab ){
        /* This branch is taken if table zName has not yet been attached to
        ** this session and the auto-attach flag is set.  */
        pSession->rc = sqlite3session_attach(pSession,zName);
        if( pSession->rc ) break;
        pTab = pSession->pTable;
        assert( 0==sqlite3_strnicmp(pTab->zName, zName, nName+1) );
      }

      if( 0==sqlite3_strnicmp(pTab->zName, zName, nName+1) ){
        sessionPreupdateOneChange(op, pSession, pTab);
        if( op==SQLITE_UPDATE ){
          sessionPreupdateOneChange(SQLITE_INSERT, pSession, pTab);
        }
        break;
      }
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
  sqlite3_session *pNew;          /* Newly allocated session object */
  sqlite3_session *pOld;          /* Session object already attached to db */
  int nDb = sqlite3Strlen30(zDb); /* Length of zDb in bytes */

  /* Zero the output value in case an error occurs. */
  *ppSession = 0;

  /* Allocate and populate the new session object. */
  pNew = (sqlite3_session *)sqlite3_malloc(sizeof(sqlite3_session) + nDb + 1);
  if( !pNew ) return SQLITE_NOMEM;
  memset(pNew, 0, sizeof(sqlite3_session));
  pNew->db = db;
  pNew->zDb = (char *)&pNew[1];
  pNew->bEnable = 1;
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
** Free the list of table objects passed as the first argument. The contents
** of the changed-rows hash tables are also deleted.
*/
static void sessionDeleteTable(SessionTable *pList){
  SessionTable *pNext;
  SessionTable *pTab;

  for(pTab=pList; pTab; pTab=pNext){
    int i;
    pNext = pTab->pNext;
    for(i=0; i<pTab->nChange; i++){
      SessionChange *p;
      SessionChange *pNext;
      for(p=pTab->apChange[i]; p; p=pNext){
        pNext = p->pNext;
        sqlite3_free(p);
      }
    }
    sqlite3_free((char*)pTab->azCol);  /* cast works around VC++ bug */
    sqlite3_free(pTab->apChange);
    sqlite3_free(pTab);
  }
}

/*
** Delete a session object previously allocated using sqlite3session_create().
*/
void sqlite3session_delete(sqlite3_session *pSession){
  sqlite3 *db = pSession->db;
  sqlite3_session *pHead;
  sqlite3_session **pp;

  /* Unlink the session from the linked list of sessions attached to the
  ** database handle. Hold the db mutex while doing so.  */
  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  pHead = (sqlite3_session*)sqlite3_preupdate_hook(db, 0, 0);
  for(pp=&pHead; (*pp)!=pSession; pp=&((*pp)->pNext));
  *pp = (*pp)->pNext;
  if( pHead ) sqlite3_preupdate_hook(db, xPreUpdate, (void *)pHead);
  sqlite3_mutex_leave(sqlite3_db_mutex(db));

  /* Delete all attached table objects. And the contents of their 
  ** associated hash-tables. */
  sessionDeleteTable(pSession->pTable);

  /* Free the session object itself. */
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
  int rc = SQLITE_OK;
  sqlite3_mutex_enter(sqlite3_db_mutex(pSession->db));

  if( !zName ){
    pSession->bAutoAttach = 1;
  }else{
    SessionTable *pTab;           /* New table object (if required) */
    int nName;                    /* Number of bytes in string zName */

    /* First search for an existing entry. If one is found, this call is
    ** a no-op. Return early. */
    nName = sqlite3Strlen30(zName);
    for(pTab=pSession->pTable; pTab; pTab=pTab->pNext){
      if( 0==sqlite3_strnicmp(pTab->zName, zName, nName+1) ) break;
    }

    if( !pTab ){
      /* Allocate new SessionTable object. */
      pTab = (SessionTable *)sqlite3_malloc(sizeof(SessionTable) + nName + 1);
      if( !pTab ){
        rc = SQLITE_NOMEM;
      }else{
        /* Populate the new SessionTable object and link it into the list. */
        memset(pTab, 0, sizeof(SessionTable));
        pTab->zName = (char *)&pTab[1];
        memcpy(pTab->zName, zName, nName+1);
        pTab->pNext = pSession->pTable;
        pSession->pTable = pTab;
      }
    }
  }

  sqlite3_mutex_leave(sqlite3_db_mutex(pSession->db));
  return rc;
}

/*
** Ensure that there is room in the buffer to append nByte bytes of data.
** If not, use sqlite3_realloc() to grow the buffer so that there is.
**
** If successful, return zero. Otherwise, if an OOM condition is encountered,
** set *pRc to SQLITE_NOMEM and return non-zero.
*/
static int sessionBufferGrow(SessionBuffer *p, int nByte, int *pRc){
  if( *pRc==SQLITE_OK && p->nAlloc-p->nBuf<nByte ){
    u8 *aNew;
    int nNew = p->nAlloc ? p->nAlloc : 128;
    do {
      nNew = nNew*2;
    }while( nNew<(p->nAlloc+nByte) );

    aNew = (u8 *)sqlite3_realloc(p->aBuf, nNew);
    if( 0==aNew ){
      *pRc = SQLITE_NOMEM;
    }else{
      p->aBuf = aNew;
      p->nAlloc = nNew;
    }
  }
  return (*pRc!=SQLITE_OK);
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append a single byte to the buffer. 
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendByte(SessionBuffer *p, u8 v, int *pRc){
  if( 0==sessionBufferGrow(p, 1, pRc) ){
    p->aBuf[p->nBuf++] = v;
  }
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append a single varint to the buffer. 
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendVarint(SessionBuffer *p, int v, int *pRc){
  if( 0==sessionBufferGrow(p, 9, pRc) ){
    p->nBuf += sessionVarintPut(&p->aBuf[p->nBuf], v);
  }
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append a blob of data to the buffer. 
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendBlob(
  SessionBuffer *p, 
  const u8 *aBlob, 
  int nBlob, 
  int *pRc
){
  if( 0==sessionBufferGrow(p, nBlob, pRc) ){
    memcpy(&p->aBuf[p->nBuf], aBlob, nBlob);
    p->nBuf += nBlob;
  }
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append a string to the buffer. All bytes in the string
** up to (but not including) the nul-terminator are written to the buffer.
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendStr(
  SessionBuffer *p, 
  const char *zStr, 
  int *pRc
){
  int nStr = sqlite3Strlen30(zStr);
  if( 0==sessionBufferGrow(p, nStr, pRc) ){
    memcpy(&p->aBuf[p->nBuf], zStr, nStr);
    p->nBuf += nStr;
  }
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append the string representation of integer iVal
** to the buffer. No nul-terminator is written.
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendInteger(
  SessionBuffer *p,               /* Buffer to append to */
  int iVal,                       /* Value to write the string rep. of */
  int *pRc                        /* IN/OUT: Error code */
){
  char aBuf[24];
  sqlite3_snprintf(sizeof(aBuf)-1, aBuf, "%d", iVal);
  sessionAppendStr(p, aBuf, pRc);
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is 
** called. Otherwise, append the string zStr enclosed in quotes (") and
** with any embedded quote characters escaped to the buffer. No 
** nul-terminator byte is written.
**
** If an OOM condition is encountered, set *pRc to SQLITE_NOMEM before
** returning.
*/
static void sessionAppendIdent(
  SessionBuffer *p,               /* Buffer to a append to */
  const char *zStr,               /* String to quote, escape and append */
  int *pRc                        /* IN/OUT: Error code */
){
  int nStr = sqlite3Strlen30(zStr)*2 + 2 + 1;
  if( 0==sessionBufferGrow(p, nStr, pRc) ){
    char *zOut = (char *)&p->aBuf[p->nBuf];
    const char *zIn = zStr;
    *zOut++ = '"';
    while( *zIn ){
      if( *zIn=='"' ) *zOut++ = '"';
      *zOut++ = *(zIn++);
    }
    *zOut++ = '"';
    p->nBuf = (int)((u8 *)zOut - p->aBuf);
  }
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is
** called. Otherwse, it appends the serialized version of the value stored
** in column iCol of the row that SQL statement pStmt currently points
** to to the buffer.
*/
static void sessionAppendCol(
  SessionBuffer *p,               /* Buffer to append to */
  sqlite3_stmt *pStmt,            /* Handle pointing to row containing value */
  int iCol,                       /* Column to read value from */
  int *pRc                        /* IN/OUT: Error code */
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
      sessionPutI64(aBuf, i);
      sessionAppendBlob(p, aBuf, 8, pRc);
    }
    if( eType==SQLITE_BLOB || eType==SQLITE_TEXT ){
      u8 *z;
      if( eType==SQLITE_BLOB ){
        z = (u8 *)sqlite3_column_blob(pStmt, iCol);
      }else{
        z = (u8 *)sqlite3_column_text(pStmt, iCol);
      }
      if( z ){
        int nByte = sqlite3_column_bytes(pStmt, iCol);
        sessionAppendVarint(p, nByte, pRc);
        sessionAppendBlob(p, z, nByte, pRc);
      }else{
        *pRc = SQLITE_NOMEM;
      }
    }
  }
}

/*
**
** This function appends an update change to the buffer (see the comments 
** under "CHANGESET FORMAT" at the top of the file). An update change 
** consists of:
**
**   1 byte:  SQLITE_UPDATE (0x17)
**   n bytes: old.* record (see RECORD FORMAT)
**   m bytes: new.* record (see RECORD FORMAT)
**
** The SessionChange object passed as the third argument contains the
** values that were stored in the row when the session began (the old.*
** values). The statement handle passed as the second argument points
** at the current version of the row (the new.* values).
**
** If all of the old.* values are equal to their corresponding new.* value
** (i.e. nothing has changed), then no data at all is appended to the buffer.
**
** Otherwise, the old.* record contains all primary key values and the 
** original values of any fields that have been modified. The new.* record 
** contains the new values of only those fields that have been modified.
*/ 
static int sessionAppendUpdate(
  SessionBuffer *pBuf,            /* Buffer to append to */
  sqlite3_stmt *pStmt,            /* Statement handle pointing at new row */
  SessionChange *p,               /* Object containing old values */
  u8 *abPK                        /* Boolean array - true for PK columns */
){
  int rc = SQLITE_OK;
  SessionBuffer buf2 = {0,0,0}; /* Buffer to accumulate new.* record in */
  int bNoop = 1;                /* Set to zero if any values are modified */
  int nRewind = pBuf->nBuf;     /* Set to zero if any values are modified */
  int i;                        /* Used to iterate through columns */
  u8 *pCsr = p->aRecord;        /* Used to iterate through old.* values */

  sessionAppendByte(pBuf, SQLITE_UPDATE, &rc);
  sessionAppendByte(pBuf, p->bIndirect, &rc);
  for(i=0; i<sqlite3_column_count(pStmt); i++){
    int bChanged = 0;
    int nAdvance;
    int eType = *pCsr;
    switch( eType ){
      case SQLITE_NULL:
        nAdvance = 1;
        if( sqlite3_column_type(pStmt, i)!=SQLITE_NULL ){
          bChanged = 1;
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
        bChanged = 1;
        break;
      }

      default: {
        int nByte;
        int nHdr = 1 + sessionVarintGet(&pCsr[1], &nByte);
        assert( eType==SQLITE_TEXT || eType==SQLITE_BLOB );
        nAdvance = nHdr + nByte;
        if( eType==sqlite3_column_type(pStmt, i) 
         && nByte==sqlite3_column_bytes(pStmt, i) 
         && 0==memcmp(&pCsr[nHdr], sqlite3_column_blob(pStmt, i), nByte)
        ){
          break;
        }
        bChanged = 1;
      }
    }

    if( bChanged || abPK[i] ){
      sessionAppendBlob(pBuf, pCsr, nAdvance, &rc);
    }else{
      sessionAppendByte(pBuf, 0, &rc);
    }

    if( bChanged ){
      sessionAppendCol(&buf2, pStmt, i, &rc);
      bNoop = 0;
    }else{
      sessionAppendByte(&buf2, 0, &rc);
    }

    pCsr += nAdvance;
  }

  if( bNoop ){
    pBuf->nBuf = nRewind;
  }else{
    sessionAppendBlob(pBuf, buf2.aBuf, buf2.nBuf, &rc);
  }
  sqlite3_free(buf2.aBuf);

  return rc;
}

/*
** Formulate and prepare a SELECT statement to retrieve a row from table
** zTab in database zDb based on its primary key. i.e.
**
**   SELECT * FROM zDb.zTab WHERE pk1 = ? AND pk2 = ? AND ...
*/
static int sessionSelectStmt(
  sqlite3 *db,                    /* Database handle */
  const char *zDb,                /* Database name */
  const char *zTab,               /* Table name */
  int nCol,                       /* Number of columns in table */
  const char **azCol,             /* Names of table columns */
  u8 *abPK,                       /* PRIMARY KEY  array */
  sqlite3_stmt **ppStmt           /* OUT: Prepared SELECT statement */
){
  int rc = SQLITE_OK;
  int i;
  const char *zSep = "";
  SessionBuffer buf = {0, 0, 0};

  sessionAppendStr(&buf, "SELECT * FROM ", &rc);
  sessionAppendIdent(&buf, zDb, &rc);
  sessionAppendStr(&buf, ".", &rc);
  sessionAppendIdent(&buf, zTab, &rc);
  sessionAppendStr(&buf, " WHERE ", &rc);
  for(i=0; i<nCol; i++){
    if( abPK[i] ){
      sessionAppendStr(&buf, zSep, &rc);
      sessionAppendIdent(&buf, azCol[i], &rc);
      sessionAppendStr(&buf, " = ?", &rc);
      sessionAppendInteger(&buf, i+1, &rc);
      zSep = " AND ";
    }
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, (char *)buf.aBuf, buf.nBuf, ppStmt, 0);
  }
  sqlite3_free(buf.aBuf);
  return rc;
}

/*
** Bind the PRIMARY KEY values from the change passed in argument pChange
** to the SELECT statement passed as the first argument. The SELECT statement
** is as prepared by function sessionSelectStmt().
**
** Return SQLITE_OK if all PK values are successfully bound, or an SQLite
** error code (e.g. SQLITE_NOMEM) otherwise.
*/
static int sessionSelectBind(
  sqlite3_stmt *pSelect,          /* SELECT from sessionSelectStmt() */
  int nCol,                       /* Number of columns in table */
  u8 *abPK,                       /* PRIMARY KEY array */
  SessionChange *pChange          /* Change structure */
){
  int i;
  int rc = SQLITE_OK;
  u8 *a = pChange->aRecord;

  for(i=0; i<nCol && rc==SQLITE_OK; i++){
    int eType = *a++;

    switch( eType ){
      case 0:
      case SQLITE_NULL:
        assert( abPK[i]==0 );
        break;

      case SQLITE_INTEGER: {
        if( abPK[i] ){
          i64 iVal = sessionGetI64(a);
          rc = sqlite3_bind_int64(pSelect, i+1, iVal);
        }
        a += 8;
        break;
      }

      case SQLITE_FLOAT: {
        if( abPK[i] ){
          double rVal;
          i64 iVal = sessionGetI64(a);
          memcpy(&rVal, &iVal, 8);
          rc = sqlite3_bind_double(pSelect, i+1, rVal);
        }
        a += 8;
        break;
      }

      case SQLITE_TEXT: {
        int n;
        a += sessionVarintGet(a, &n);
        if( abPK[i] ){
          rc = sqlite3_bind_text(pSelect, i+1, (char *)a, n, SQLITE_TRANSIENT);
        }
        a += n;
        break;
      }

      default: {
        int n;
        assert( eType==SQLITE_BLOB );
        a += sessionVarintGet(a, &n);
        if( abPK[i] ){
          rc = sqlite3_bind_blob(pSelect, i+1, a, n, SQLITE_TRANSIENT);
        }
        a += n;
        break;
      }
    }
  }

  return rc;
}

/*
** This function is a no-op if *pRc is set to other than SQLITE_OK when it
** is called. Otherwise, append a serialized table header (part of the binary 
** changeset format) to buffer *pBuf. If an error occurs, set *pRc to an
** SQLite error code before returning.
*/
static void sessionAppendTableHdr(
  SessionBuffer *pBuf, 
  SessionTable *pTab, 
  int *pRc
){
  /* Write a table header */
  sessionAppendByte(pBuf, 'T', pRc);
  sessionAppendVarint(pBuf, pTab->nCol, pRc);
  sessionAppendBlob(pBuf, pTab->abPK, pTab->nCol, pRc);
  sessionAppendBlob(pBuf, (u8 *)pTab->zName, (int)strlen(pTab->zName)+1, pRc);
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
  sqlite3 *db = pSession->db;     /* Source database handle */
  SessionTable *pTab;             /* Used to iterate through attached tables */
  SessionBuffer buf = {0,0,0};    /* Buffer in which to accumlate changeset */
  int rc;                         /* Return code */

  /* Zero the output variables in case an error occurs. If this session
  ** object is already in the error state (sqlite3_session.rc != SQLITE_OK),
  ** this call will be a no-op.  */
  *pnChangeset = 0;
  *ppChangeset = 0;

  if( pSession->rc ) return pSession->rc;
  rc = sqlite3_exec(pSession->db, "SAVEPOINT changeset", 0, 0, 0);
  if( rc!=SQLITE_OK ) return rc;

  sqlite3_mutex_enter(sqlite3_db_mutex(db));

  for(pTab=pSession->pTable; rc==SQLITE_OK && pTab; pTab=pTab->pNext){
    if( pTab->nEntry ){
      const char *zName = pTab->zName;
      int nCol;                   /* Number of columns in table */
      u8 *abPK;                   /* Primary key array */
      const char **azCol = 0;     /* Table columns */
      int i;                      /* Used to iterate through hash buckets */
      sqlite3_stmt *pSel = 0;     /* SELECT statement to query table pTab */
      int nRewind = buf.nBuf;     /* Initial size of write buffer */
      int nNoop;                  /* Size of buffer after writing tbl header */

      /* Check the table schema is still Ok. */
      rc = sessionTableInfo(db, pSession->zDb, zName, &nCol, 0, &azCol, &abPK);
      if( !rc && (pTab->nCol!=nCol || memcmp(abPK, pTab->abPK, nCol)) ){
        rc = SQLITE_SCHEMA;
      }

      /* Write a table header */
      sessionAppendTableHdr(&buf, pTab, &rc);

      /* Build and compile a statement to execute: */
      if( rc==SQLITE_OK ){
        rc = sessionSelectStmt(
            db, pSession->zDb, zName, nCol, azCol, abPK, &pSel);
      }

      nNoop = buf.nBuf;
      for(i=0; i<pTab->nChange && rc==SQLITE_OK; i++){
        SessionChange *p;         /* Used to iterate through changes */

        for(p=pTab->apChange[i]; rc==SQLITE_OK && p; p=p->pNext){
          rc = sessionSelectBind(pSel, nCol, abPK, p);
          if( rc!=SQLITE_OK ) continue;
          if( sqlite3_step(pSel)==SQLITE_ROW ){
            if( p->op==SQLITE_INSERT ){
              int iCol;
              sessionAppendByte(&buf, SQLITE_INSERT, &rc);
              sessionAppendByte(&buf, p->bIndirect, &rc);
              for(iCol=0; iCol<nCol; iCol++){
                sessionAppendCol(&buf, pSel, iCol, &rc);
              }
            }else{
              rc = sessionAppendUpdate(&buf, pSel, p, abPK);
            }
          }else if( p->op!=SQLITE_INSERT ){
            /* A DELETE change */
            sessionAppendByte(&buf, SQLITE_DELETE, &rc);
            sessionAppendByte(&buf, p->bIndirect, &rc);
            sessionAppendBlob(&buf, p->aRecord, p->nRecord, &rc);
          }
          if( rc==SQLITE_OK ){
            rc = sqlite3_reset(pSel);
          }
        }
      }

      sqlite3_finalize(pSel);
      if( buf.nBuf==nNoop ){
        buf.nBuf = nRewind;
      }
      sqlite3_free((char*)azCol);  /* cast works around VC++ bug */
    }
  }

  if( rc==SQLITE_OK ){
    *pnChangeset = buf.nBuf;
    *ppChangeset = buf.aBuf;
  }else{
    sqlite3_free(buf.aBuf);
  }

  sqlite3_exec(db, "RELEASE changeset", 0, 0, 0);
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  return rc;
}

/*
** Enable or disable the session object passed as the first argument.
*/
int sqlite3session_enable(sqlite3_session *pSession, int bEnable){
  int ret;
  sqlite3_mutex_enter(sqlite3_db_mutex(pSession->db));
  if( bEnable>=0 ){
    pSession->bEnable = bEnable;
  }
  ret = pSession->bEnable;
  sqlite3_mutex_leave(sqlite3_db_mutex(pSession->db));
  return ret;
}

/*
** Enable or disable the session object passed as the first argument.
*/
int sqlite3session_indirect(sqlite3_session *pSession, int bIndirect){
  int ret;
  sqlite3_mutex_enter(sqlite3_db_mutex(pSession->db));
  if( bIndirect>=0 ){
    pSession->bIndirect = bIndirect;
  }
  ret = pSession->bIndirect;
  sqlite3_mutex_leave(sqlite3_db_mutex(pSession->db));
  return ret;
}

/*
** Return true if there have been no changes to monitored tables recorded
** by the session object passed as the only argument.
*/
int sqlite3session_isempty(sqlite3_session *pSession){
  int ret = 0;
  SessionTable *pTab;

  sqlite3_mutex_enter(sqlite3_db_mutex(pSession->db));
  for(pTab=pSession->pTable; pTab && ret==0; pTab=pTab->pNext){
    ret = (pTab->nEntry>0);
  }
  sqlite3_mutex_leave(sqlite3_db_mutex(pSession->db));

  return (ret==0);
}

/*
** Create an iterator used to iterate through the contents of a changeset.
*/
int sqlite3changeset_start(
  sqlite3_changeset_iter **pp,    /* OUT: Changeset iterator handle */
  int nChangeset,                 /* Size of buffer pChangeset in bytes */
  void *pChangeset                /* Pointer to buffer containing changeset */
){
  sqlite3_changeset_iter *pRet;   /* Iterator to return */
  int nByte;                      /* Number of bytes to allocate for iterator */

  /* Zero the output variable in case an error occurs. */
  *pp = 0;

  /* Allocate and initialize the iterator structure. */
  nByte = sizeof(sqlite3_changeset_iter);
  pRet = (sqlite3_changeset_iter *)sqlite3_malloc(nByte);
  if( !pRet ) return SQLITE_NOMEM;
  memset(pRet, 0, sizeof(sqlite3_changeset_iter));
  pRet->aChangeset = (u8 *)pChangeset;
  pRet->nChangeset = nChangeset;
  pRet->pNext = pRet->aChangeset;

  /* Populate the output variable and return success. */
  *pp = pRet;
  return SQLITE_OK;
}

/*
** Deserialize a single record from a buffer in memory. See "RECORD FORMAT"
** for details.
**
** When this function is called, *paChange points to the start of the record
** to deserialize. Assuming no error occurs, *paChange is set to point to
** one byte after the end of the same record before this function returns.
**
** If successful, each element of the apOut[] array (allocated by the caller)
** is set to point to an sqlite3_value object containing the value read
** from the corresponding position in the record. If that value is not
** included in the record (i.e. because the record is part of an UPDATE change
** and the field was not modified), the corresponding element of apOut[] is
** set to NULL.
**
** It is the responsibility of the caller to free all sqlite_value structures
** using sqlite3_free().
**
** If an error occurs, an SQLite error code (e.g. SQLITE_NOMEM) is returned.
** The apOut[] array may have been partially populated in this case.
*/
static int sessionReadRecord(
  u8 **paChange,                  /* IN/OUT: Pointer to binary record */
  int nCol,                       /* Number of values in record */
  sqlite3_value **apOut           /* Write values to this array */
){
  int i;                          /* Used to iterate through columns */
  u8 *aRec = *paChange;           /* Cursor for the serialized record */

  for(i=0; i<nCol; i++){
    int eType = *aRec++;          /* Type of value (SQLITE_NULL, TEXT etc.) */
    assert( !apOut || apOut[i]==0 );
    if( eType ){
      if( apOut ){
        apOut[i] = sqlite3ValueNew(0);
        if( !apOut[i] ) return SQLITE_NOMEM;
      }

      if( eType==SQLITE_TEXT || eType==SQLITE_BLOB ){
        int nByte;
        aRec += sessionVarintGet(aRec, &nByte);
        if( apOut ){
          u8 enc = (eType==SQLITE_TEXT ? SQLITE_UTF8 : 0);
          sqlite3ValueSetStr(apOut[i], nByte, (char *)aRec, enc, SQLITE_STATIC);
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
            memcpy(&d, &v, 8);
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
** Advance the changeset iterator to the next change.
**
** If both paRec and pnRec are NULL, then this function works like the public
** API sqlite3changeset_next(). If SQLITE_ROW is returned, then the
** sqlite3changeset_new() and old() APIs may be used to query for values.
**
** Otherwise, if paRec and pnRec are not NULL, then a pointer to the change
** record is written to *paRec before returning and the number of bytes in
** the record to *pnRec.
**
** Either way, this function returns SQLITE_ROW if the iterator is 
** successfully advanced to the next change in the changeset, an SQLite 
** error code if an error occurs, or SQLITE_DONE if there are no further 
** changes in the changeset.
*/
static int sessionChangesetNext(
  sqlite3_changeset_iter *p,      /* Changeset iterator */
  u8 **paRec,                     /* If non-NULL, store record pointer here */
  int *pnRec                      /* If non-NULL, store size of record here */
){
  u8 *aChange;
  int i;

  assert( (paRec==0 && pnRec==0) || (paRec && pnRec) );

  /* If the iterator is in the error-state, return immediately. */
  if( p->rc!=SQLITE_OK ) return p->rc;

  /* Free the current contents of p->apValue[], if any. */
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

  if( aChange[0]=='T' ){
    int nByte;                    /* Bytes to allocate for apValue */
    aChange++;
    aChange += sessionVarintGet(aChange, &p->nCol);
    p->abPK = (u8 *)aChange;
    aChange += p->nCol;
    p->zTab = (char *)aChange;
    aChange += (sqlite3Strlen30((char *)aChange) + 1);
    
    if( paRec==0 ){
      sqlite3_free(p->apValue);
      nByte = sizeof(sqlite3_value *) * p->nCol * 2;
      p->apValue = (sqlite3_value **)sqlite3_malloc(nByte);
      if( !p->apValue ){
        return (p->rc = SQLITE_NOMEM);
      }
      memset(p->apValue, 0, sizeof(sqlite3_value*)*p->nCol*2);
    }
  }

  p->op = *(aChange++);
  p->bIndirect = *(aChange++);
  if( p->op!=SQLITE_UPDATE && p->op!=SQLITE_DELETE && p->op!=SQLITE_INSERT ){
    return (p->rc = SQLITE_CORRUPT);
  }

  if( paRec ){ *paRec = aChange; }

  /* If this is an UPDATE or DELETE, read the old.* record. */
  if( p->op!=SQLITE_INSERT ){
    p->rc = sessionReadRecord(&aChange, p->nCol, paRec?0:p->apValue);
    if( p->rc!=SQLITE_OK ) return p->rc;
  }

  /* If this is an INSERT or UPDATE, read the new.* record. */
  if( p->op!=SQLITE_DELETE ){
    p->rc = sessionReadRecord(&aChange, p->nCol, paRec?0:&p->apValue[p->nCol]);
    if( p->rc!=SQLITE_OK ) return p->rc;
  }

  if( pnRec ){ *pnRec = (int)(aChange - *paRec); }
  p->pNext = aChange;
  return SQLITE_ROW;
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
  return sessionChangesetNext(p, 0, 0);
}

/*
** The following function extracts information on the current change
** from a changeset iterator. It may only be called after changeset_next()
** has returned SQLITE_ROW.
*/
int sqlite3changeset_op(
  sqlite3_changeset_iter *pIter,  /* Iterator handle */
  const char **pzTab,             /* OUT: Pointer to table name */
  int *pnCol,                     /* OUT: Number of columns in table */
  int *pOp,                       /* OUT: SQLITE_INSERT, DELETE or UPDATE */
  int *pbIndirect                 /* OUT: True if change is indirect */
){
  *pOp = pIter->op;
  *pnCol = pIter->nCol;
  *pzTab = pIter->zTab;
  if( pbIndirect ) *pbIndirect = pIter->bIndirect;
  return SQLITE_OK;
}

/*
** Return information regarding the PRIMARY KEY and number of columns in
** the database table affected by the change that pIter currently points
** to. This function may only be called after changeset_next() returns
** SQLITE_ROW.
*/
int sqlite3changeset_pk(
  sqlite3_changeset_iter *pIter,  /* Iterator object */
  unsigned char **pabPK,          /* OUT: Array of boolean - true for PK cols */
  int *pnCol                      /* OUT: Number of entries in output array */
){
  *pabPK = pIter->abPK;
  if( pnCol ) *pnCol = pIter->nCol;
  return SQLITE_OK;
}

/*
** This function may only be called while the iterator is pointing to an
** SQLITE_UPDATE or SQLITE_DELETE change (see sqlite3changeset_op()).
** Otherwise, SQLITE_MISUSE is returned.
**
** It sets *ppValue to point to an sqlite3_value structure containing the
** iVal'th value in the old.* record. Or, if that particular value is not
** included in the record (because the change is an UPDATE and the field
** was not modified and is not a PK column), set *ppValue to NULL.
**
** If value iVal is out-of-range, SQLITE_RANGE is returned and *ppValue is
** not modified. Otherwise, SQLITE_OK.
*/
int sqlite3changeset_old(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Index of old.* value to retrieve */
  sqlite3_value **ppValue         /* OUT: Old value (or NULL pointer) */
){
  if( pIter->op!=SQLITE_UPDATE && pIter->op!=SQLITE_DELETE ){
    return SQLITE_MISUSE;
  }
  if( iVal<0 || iVal>=pIter->nCol ){
    return SQLITE_RANGE;
  }
  *ppValue = pIter->apValue[iVal];
  return SQLITE_OK;
}

/*
** This function may only be called while the iterator is pointing to an
** SQLITE_UPDATE or SQLITE_INSERT change (see sqlite3changeset_op()).
** Otherwise, SQLITE_MISUSE is returned.
**
** It sets *ppValue to point to an sqlite3_value structure containing the
** iVal'th value in the new.* record. Or, if that particular value is not
** included in the record (because the change is an UPDATE and the field
** was not modified), set *ppValue to NULL.
**
** If value iVal is out-of-range, SQLITE_RANGE is returned and *ppValue is
** not modified. Otherwise, SQLITE_OK.
*/
int sqlite3changeset_new(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Index of new.* value to retrieve */
  sqlite3_value **ppValue         /* OUT: New value (or NULL pointer) */
){
  if( pIter->op!=SQLITE_UPDATE && pIter->op!=SQLITE_INSERT ){
    return SQLITE_MISUSE;
  }
  if( iVal<0 || iVal>=pIter->nCol ){
    return SQLITE_RANGE;
  }
  *ppValue = pIter->apValue[pIter->nCol+iVal];
  return SQLITE_OK;
}

/*
** The following two macros are used internally. They are similar to the
** sqlite3changeset_new() and sqlite3changeset_old() functions, except that
** they omit all error checking and return a pointer to the requested value.
*/
#define sessionChangesetNew(pIter, iVal) (pIter)->apValue[(pIter)->nCol+(iVal)]
#define sessionChangesetOld(pIter, iVal) (pIter)->apValue[(iVal)]

/*
** This function may only be called with a changeset iterator that has been
** passed to an SQLITE_CHANGESET_DATA or SQLITE_CHANGESET_CONFLICT 
** conflict-handler function. Otherwise, SQLITE_MISUSE is returned.
**
** If successful, *ppValue is set to point to an sqlite3_value structure
** containing the iVal'th value of the conflicting record.
**
** If value iVal is out-of-range or some other error occurs, an SQLite error
** code is returned. Otherwise, SQLITE_OK.
*/
int sqlite3changeset_conflict(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int iVal,                       /* Index of conflict record value to fetch */
  sqlite3_value **ppValue         /* OUT: Value from conflicting row */
){
  if( !pIter->pConflict ){
    return SQLITE_MISUSE;
  }
  if( iVal<0 || iVal>=sqlite3_column_count(pIter->pConflict) ){
    return SQLITE_RANGE;
  }
  *ppValue = sqlite3_column_value(pIter->pConflict, iVal);
  return SQLITE_OK;
}

/*
** This function may only be called with an iterator passed to an
** SQLITE_CHANGESET_FOREIGN_KEY conflict handler callback. In this case
** it sets the output variable to the total number of known foreign key
** violations in the destination database and returns SQLITE_OK.
**
** In all other cases this function returns SQLITE_MISUSE.
*/
int sqlite3changeset_fk_conflicts(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int *pnOut                      /* OUT: Number of FK violations */
){
  if( pIter->pConflict || pIter->apValue ){
    return SQLITE_MISUSE;
  }
  *pnOut = pIter->nCol;
  return SQLITE_OK;
}


/*
** Finalize an iterator allocated with sqlite3changeset_start().
**
** This function may not be called on iterators passed to a conflict handler
** callback by changeset_apply().
*/
int sqlite3changeset_finalize(sqlite3_changeset_iter *p){
  int i;                          /* Used to iterate through p->apValue[] */
  int rc = p->rc;                 /* Return code */
  if( p->apValue ){
    for(i=0; i<p->nCol*2; i++) sqlite3ValueFree(p->apValue[i]);
  }
  sqlite3_free(p->apValue);
  sqlite3_free(p);
  return rc;
}

/*
** Invert a changeset object.
*/
int sqlite3changeset_invert(
  int nChangeset,                 /* Number of bytes in input */
  const void *pChangeset,         /* Input changeset */
  int *pnInverted,                /* OUT: Number of bytes in output changeset */
  void **ppInverted               /* OUT: Inverse of pChangeset */
){
  int rc = SQLITE_OK;             /* Return value */
  u8 *aOut;
  u8 *aIn;
  int i;
  int nCol = 0;                   /* Number of cols in current table */
  u8 *abPK = 0;                   /* PK array for current table */
  sqlite3_value **apVal = 0;      /* Space for values for UPDATE inversion */

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
        /* A 'table' record consists of:
        **
        **   * A constant 'T' character,
        **   * Number of columns in said table (a varint),
        **   * An array of nCol bytes (abPK),
        **   * A nul-terminated table name.
        */
        int nByte = 1 + sessionVarintGet(&aIn[i+1], &nCol);
        abPK = &aIn[i+nByte];
        nByte += nCol;
        nByte += 1 + sqlite3Strlen30((char *)&aIn[i+nByte]);
        memcpy(&aOut[i], &aIn[i], nByte);
        i += nByte;
        sqlite3_free(apVal);
        apVal = 0;
        break;
      }

      case SQLITE_INSERT:
      case SQLITE_DELETE: {
        int nByte;
        u8 *aEnd = &aIn[i+2];

        sessionReadRecord(&aEnd, nCol, 0);
        aOut[i] = (eType==SQLITE_DELETE ? SQLITE_INSERT : SQLITE_DELETE);
        aOut[i+1] = aIn[i+1];
        nByte = (int)(aEnd - &aIn[i+2]);
        memcpy(&aOut[i+2], &aIn[i+2], nByte);
        i += 2 + nByte;
        break;
      }

      case SQLITE_UPDATE: {
        int iCol;
        int nWrite = 0;
        u8 *aEnd = &aIn[i+2];

        if( 0==apVal ){
          apVal = (sqlite3_value **)sqlite3_malloc(sizeof(apVal[0])*nCol*2);
          if( 0==apVal ){
            rc = SQLITE_NOMEM;
            goto finished_invert;
          }
          memset(apVal, 0, sizeof(apVal[0])*nCol*2);
        }

        /* Read the old.* and new.* records for the update change. */
        rc = sessionReadRecord(&aEnd, nCol, &apVal[0]);
        if( rc==SQLITE_OK ){
          rc = sessionReadRecord(&aEnd, nCol, &apVal[nCol]);
        }

        /* Write the header for the new UPDATE change. Same as the original. */
        aOut[i] = SQLITE_UPDATE;
        aOut[i+1] = aIn[i+1];
        nWrite = 2;

        /* Write the new old.* record. Consists of the PK columns from the
        ** original old.* record, and the other values from the original
        ** new.* record. */
        for(iCol=0; rc==SQLITE_OK && iCol<nCol; iCol++){
          sqlite3_value *pVal = apVal[iCol + (abPK[iCol] ? 0 : nCol)];
          rc = sessionSerializeValue(&aOut[i+nWrite], pVal, &nWrite);
        }

        /* Write the new new.* record. Consists of a copy of all values
        ** from the original old.* record, except for the PK columns, which
        ** are set to "undefined". */
        for(iCol=0; rc==SQLITE_OK && iCol<nCol; iCol++){
          sqlite3_value *pVal = (abPK[iCol] ? 0 : apVal[iCol]);
          rc = sessionSerializeValue(&aOut[i+nWrite], pVal, &nWrite);
        }

        for(iCol=0; iCol<nCol*2; iCol++){
          sqlite3ValueFree(apVal[iCol]);
        }
        memset(apVal, 0, sizeof(apVal[0])*nCol*2);
        if( rc!=SQLITE_OK ){
          goto finished_invert;
        }

        i += nWrite;
        assert( &aIn[i]==aEnd );
        break;
      }

      default:
        rc = SQLITE_CORRUPT;
        goto finished_invert;
    }
  }

  assert( rc==SQLITE_OK );
  *pnInverted = nChangeset;
  *ppInverted = (void *)aOut;

 finished_invert:
  if( rc!=SQLITE_OK ){
    sqlite3_free(aOut);
  }
  sqlite3_free(apVal);
  return rc;
}

typedef struct SessionApplyCtx SessionApplyCtx;
struct SessionApplyCtx {
  sqlite3 *db;
  sqlite3_stmt *pDelete;          /* DELETE statement */
  sqlite3_stmt *pUpdate;          /* UPDATE statement */
  sqlite3_stmt *pInsert;          /* INSERT statement */
  sqlite3_stmt *pSelect;          /* SELECT statement */
  int nCol;                       /* Size of azCol[] and abPK[] arrays */
  const char **azCol;             /* Array of column names */
  u8 *abPK;                       /* Boolean array - true if column is in PK */
};

/*
** Formulate a statement to DELETE a row from database db. Assuming a table
** structure like this:
**
**     CREATE TABLE x(a, b, c, d, PRIMARY KEY(a, c));
**
** The DELETE statement looks like this:
**
**     DELETE FROM x WHERE a = :1 AND c = :3 AND (:5 OR b IS :2 AND d IS :4)
**
** Variable :5 (nCol+1) is a boolean. It should be set to 0 if we require
** matching b and d values, or 1 otherwise. The second case comes up if the
** conflict handler is invoked with NOTFOUND and returns CHANGESET_REPLACE.
**
** If successful, SQLITE_OK is returned and SessionApplyCtx.pDelete is left
** pointing to the prepared version of the SQL statement.
*/
static int sessionDeleteRow(
  sqlite3 *db,                    /* Database handle */
  const char *zTab,               /* Table name */
  SessionApplyCtx *p              /* Session changeset-apply context */
){
  int i;
  const char *zSep = "";
  int rc = SQLITE_OK;
  SessionBuffer buf = {0, 0, 0};
  int nPk = 0;

  sessionAppendStr(&buf, "DELETE FROM ", &rc);
  sessionAppendIdent(&buf, zTab, &rc);
  sessionAppendStr(&buf, " WHERE ", &rc);

  for(i=0; i<p->nCol; i++){
    if( p->abPK[i] ){
      nPk++;
      sessionAppendStr(&buf, zSep, &rc);
      sessionAppendIdent(&buf, p->azCol[i], &rc);
      sessionAppendStr(&buf, " = ?", &rc);
      sessionAppendInteger(&buf, i+1, &rc);
      zSep = " AND ";
    }
  }

  if( nPk<p->nCol ){
    sessionAppendStr(&buf, " AND (?", &rc);
    sessionAppendInteger(&buf, p->nCol+1, &rc);
    sessionAppendStr(&buf, " OR ", &rc);

    zSep = "";
    for(i=0; i<p->nCol; i++){
      if( !p->abPK[i] ){
        sessionAppendStr(&buf, zSep, &rc);
        sessionAppendIdent(&buf, p->azCol[i], &rc);
        sessionAppendStr(&buf, " IS ?", &rc);
        sessionAppendInteger(&buf, i+1, &rc);
        zSep = "AND ";
      }
    }
    sessionAppendStr(&buf, ")", &rc);
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, (char *)buf.aBuf, buf.nBuf, &p->pDelete, 0);
  }
  sqlite3_free(buf.aBuf);

  return rc;
}

/*
** Formulate and prepare a statement to UPDATE a row from database db. 
** Assuming a table structure like this:
**
**     CREATE TABLE x(a, b, c, d, PRIMARY KEY(a, c));
**
** The UPDATE statement looks like this:
**
**     UPDATE x SET
**     a = CASE WHEN ?2  THEN ?3  ELSE a END,
**     b = CASE WHEN ?5  THEN ?6  ELSE b END,
**     c = CASE WHEN ?8  THEN ?9  ELSE c END,
**     d = CASE WHEN ?11 THEN ?12 ELSE d END
**     WHERE a = ?1 AND c = ?7 AND (?13 OR 
**       (?5==0 OR b IS ?4) AND (?11==0 OR d IS ?10) AND
**     )
**
** For each column in the table, there are three variables to bind:
**
**     ?(i*3+1)    The old.* value of the column, if any.
**     ?(i*3+2)    A boolean flag indicating that the value is being modified.
**     ?(i*3+3)    The new.* value of the column, if any.
**
** Also, a boolean flag that, if set to true, causes the statement to update
** a row even if the non-PK values do not match. This is required if the
** conflict-handler is invoked with CHANGESET_DATA and returns
** CHANGESET_REPLACE. This is variable "?(nCol*3+1)".
**
** If successful, SQLITE_OK is returned and SessionApplyCtx.pUpdate is left
** pointing to the prepared version of the SQL statement.
*/
static int sessionUpdateRow(
  sqlite3 *db,                    /* Database handle */
  const char *zTab,               /* Table name */
  SessionApplyCtx *p              /* Session changeset-apply context */
){
  int rc = SQLITE_OK;
  int i;
  const char *zSep = "";
  SessionBuffer buf = {0, 0, 0};

  /* Append "UPDATE tbl SET " */
  sessionAppendStr(&buf, "UPDATE ", &rc);
  sessionAppendIdent(&buf, zTab, &rc);
  sessionAppendStr(&buf, " SET ", &rc);

  /* Append the assignments */
  for(i=0; i<p->nCol; i++){
    sessionAppendStr(&buf, zSep, &rc);
    sessionAppendIdent(&buf, p->azCol[i], &rc);
    sessionAppendStr(&buf, " = CASE WHEN ?", &rc);
    sessionAppendInteger(&buf, i*3+2, &rc);
    sessionAppendStr(&buf, " THEN ?", &rc);
    sessionAppendInteger(&buf, i*3+3, &rc);
    sessionAppendStr(&buf, " ELSE ", &rc);
    sessionAppendIdent(&buf, p->azCol[i], &rc);
    sessionAppendStr(&buf, " END", &rc);
    zSep = ", ";
  }

  /* Append the PK part of the WHERE clause */
  sessionAppendStr(&buf, " WHERE ", &rc);
  for(i=0; i<p->nCol; i++){
    if( p->abPK[i] ){
      sessionAppendIdent(&buf, p->azCol[i], &rc);
      sessionAppendStr(&buf, " = ?", &rc);
      sessionAppendInteger(&buf, i*3+1, &rc);
      sessionAppendStr(&buf, " AND ", &rc);
    }
  }

  /* Append the non-PK part of the WHERE clause */
  sessionAppendStr(&buf, " (?", &rc);
  sessionAppendInteger(&buf, p->nCol*3+1, &rc);
  sessionAppendStr(&buf, " OR 1", &rc);
  for(i=0; i<p->nCol; i++){
    if( !p->abPK[i] ){
      sessionAppendStr(&buf, " AND (?", &rc);
      sessionAppendInteger(&buf, i*3+2, &rc);
      sessionAppendStr(&buf, "=0 OR ", &rc);
      sessionAppendIdent(&buf, p->azCol[i], &rc);
      sessionAppendStr(&buf, " IS ?", &rc);
      sessionAppendInteger(&buf, i*3+1, &rc);
      sessionAppendStr(&buf, ")", &rc);
    }
  }
  sessionAppendStr(&buf, ")", &rc);

  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, (char *)buf.aBuf, buf.nBuf, &p->pUpdate, 0);
  }
  sqlite3_free(buf.aBuf);

  return rc;
}

/*
** Formulate and prepare an SQL statement to query table zTab by primary
** key. Assuming the following table structure:
**
**     CREATE TABLE x(a, b, c, d, PRIMARY KEY(a, c));
**
** The SELECT statement looks like this:
**
**     SELECT * FROM x WHERE a = ?1 AND c = ?3
**
** If successful, SQLITE_OK is returned and SessionApplyCtx.pSelect is left
** pointing to the prepared version of the SQL statement.
*/
static int sessionSelectRow(
  sqlite3 *db,                    /* Database handle */
  const char *zTab,               /* Table name */
  SessionApplyCtx *p              /* Session changeset-apply context */
){
  return sessionSelectStmt(
      db, "main", zTab, p->nCol, p->azCol, p->abPK, &p->pSelect);
}

/*
** Formulate and prepare an INSERT statement to add a record to table zTab.
** For example:
**
**     INSERT INTO main."zTab" VALUES(?1, ?2, ?3 ...);
**
** If successful, SQLITE_OK is returned and SessionApplyCtx.pInsert is left
** pointing to the prepared version of the SQL statement.
*/
static int sessionInsertRow(
  sqlite3 *db,                    /* Database handle */
  const char *zTab,               /* Table name */
  SessionApplyCtx *p              /* Session changeset-apply context */
){
  int rc = SQLITE_OK;
  int i;
  SessionBuffer buf = {0, 0, 0};

  sessionAppendStr(&buf, "INSERT INTO main.", &rc);
  sessionAppendIdent(&buf, zTab, &rc);
  sessionAppendStr(&buf, " VALUES(?", &rc);
  for(i=1; i<p->nCol; i++){
    sessionAppendStr(&buf, ", ?", &rc);
  }
  sessionAppendStr(&buf, ")", &rc);

  if( rc==SQLITE_OK ){
    rc = sqlite3_prepare_v2(db, (char *)buf.aBuf, buf.nBuf, &p->pInsert, 0);
  }
  sqlite3_free(buf.aBuf);
  return rc;
}

/*
** A wrapper around sqlite3_bind_value() that detects an extra problem. 
** See comments in the body of this function for details.
*/
static int sessionBindValue(
  sqlite3_stmt *pStmt,            /* Statement to bind value to */
  int i,                          /* Parameter number to bind to */
  sqlite3_value *pVal             /* Value to bind */
){
  if( (pVal->type==SQLITE_TEXT || pVal->type==SQLITE_BLOB) && pVal->z==0 ){
    /* This condition occurs when an earlier OOM in a call to
    ** sqlite3_value_text() or sqlite3_value_blob() (perhaps from within
    ** a conflict-hanler) has zeroed the pVal->z pointer. Return NOMEM. */
    return SQLITE_NOMEM;
  }
  return sqlite3_bind_value(pStmt, i, pVal);
}

/*
** Iterator pIter must point to an SQLITE_INSERT entry. This function 
** transfers new.* values from the current iterator entry to statement
** pStmt. The table being inserted into has nCol columns.
**
** New.* value $i 0 from the iterator is bound to variable ($i+1) of 
** statement pStmt. If parameter abPK is NULL, all values from 0 to (nCol-1)
** are transfered to the statement. Otherwise, if abPK is not NULL, it points
** to an array nCol elements in size. In this case only those values for 
** which abPK[$i] is true are read from the iterator and bound to the 
** statement.
**
** An SQLite error code is returned if an error occurs. Otherwise, SQLITE_OK.
*/
static int sessionBindRow(
  sqlite3_changeset_iter *pIter,  /* Iterator to read values from */
  int(*xValue)(sqlite3_changeset_iter *, int, sqlite3_value **),
  int nCol,                       /* Number of columns */
  u8 *abPK,                       /* If not NULL, bind only if true */
  sqlite3_stmt *pStmt             /* Bind values to this statement */
){
  int i;
  int rc = SQLITE_OK;

  /* Neither sqlite3changeset_old or sqlite3changeset_new can fail if the
  ** argument iterator points to a suitable entry. Make sure that xValue 
  ** is one of these to guarantee that it is safe to ignore the return 
  ** in the code below. */
  assert( xValue==sqlite3changeset_old || xValue==sqlite3changeset_new );

  for(i=0; rc==SQLITE_OK && i<nCol; i++){
    if( !abPK || abPK[i] ){
      sqlite3_value *pVal;
      (void)xValue(pIter, i, &pVal);
      rc = sessionBindValue(pStmt, i+1, pVal);
    }
  }
  return rc;
}

/*
** SQL statement pSelect is as generated by the sessionSelectRow() function.
** This function binds the primary key values from the change that changeset
** iterator pIter points to to the SELECT and attempts to seek to the table
** entry. If a row is found, the SELECT statement left pointing at the row 
** and SQLITE_ROW is returned. Otherwise, if no row is found and no error
** has occured, the statement is reset and SQLITE_OK is returned. If an
** error occurs, the statement is reset and an SQLite error code is returned.
**
** If this function returns SQLITE_ROW, the caller must eventually reset() 
** statement pSelect. If any other value is returned, the statement does
** not require a reset().
**
** If the iterator currently points to an INSERT record, bind values from the
** new.* record to the SELECT statement. Or, if it points to a DELETE or
** UPDATE, bind values from the old.* record. 
*/
static int sessionSeekToRow(
  sqlite3 *db,                    /* Database handle */
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  u8 *abPK,                       /* Primary key flags array */
  sqlite3_stmt *pSelect           /* SELECT statement from sessionSelectRow() */
){
  int rc;                         /* Return code */
  int nCol;                       /* Number of columns in table */
  int op;                         /* Changset operation (SQLITE_UPDATE etc.) */
  const char *zDummy;             /* Unused */

  sqlite3changeset_op(pIter, &zDummy, &nCol, &op, 0);
  rc = sessionBindRow(pIter, 
      op==SQLITE_INSERT ? sqlite3changeset_new : sqlite3changeset_old,
      nCol, abPK, pSelect
  );

  if( rc==SQLITE_OK ){
    rc = sqlite3_step(pSelect);
    if( rc!=SQLITE_ROW ) rc = sqlite3_reset(pSelect);
  }

  return rc;
}

/*
** Invoke the conflict handler for the change that the changeset iterator
** currently points to.
**
** Argument eType must be either CHANGESET_DATA or CHANGESET_CONFLICT.
** If argument pbReplace is NULL, then the type of conflict handler invoked
** depends solely on eType, as follows:
**
**    eType value                 Value passed to xConflict
**    -------------------------------------------------
**    CHANGESET_DATA              CHANGESET_NOTFOUND
**    CHANGESET_CONFLICT          CHANGESET_CONSTRAINT
**
** Or, if pbReplace is not NULL, then an attempt is made to find an existing
** record with the same primary key as the record about to be deleted, updated
** or inserted. If such a record can be found, it is available to the conflict
** handler as the "conflicting" record. In this case the type of conflict
** handler invoked is as follows:
**
**    eType value         PK Record found?   Value passed to xConflict
**    ----------------------------------------------------------------
**    CHANGESET_DATA      Yes                CHANGESET_DATA
**    CHANGESET_DATA      No                 CHANGESET_NOTFOUND
**    CHANGESET_CONFLICT  Yes                CHANGESET_CONFLICT
**    CHANGESET_CONFLICT  No                 CHANGESET_CONSTRAINT
**
** If pbReplace is not NULL, and a record with a matching PK is found, and
** the conflict handler function returns SQLITE_CHANGESET_REPLACE, *pbReplace
** is set to non-zero before returning SQLITE_OK.
**
** If the conflict handler returns SQLITE_CHANGESET_ABORT, SQLITE_ABORT is
** returned. Or, if the conflict handler returns an invalid value, 
** SQLITE_MISUSE. If the conflict handler returns SQLITE_CHANGESET_OMIT,
** this function returns SQLITE_OK.
*/
static int sessionConflictHandler(
  int eType,                      /* Either CHANGESET_DATA or CONFLICT */
  SessionApplyCtx *p,             /* changeset_apply() context */
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  int(*xConflict)(void *, int, sqlite3_changeset_iter*),
  void *pCtx,                     /* First argument for conflict handler */
  int *pbReplace                  /* OUT: Set to true if PK row is found */
){
  int res;                        /* Value returned by conflict handler */
  int rc;
  int nCol;
  int op;
  const char *zDummy;

  sqlite3changeset_op(pIter, &zDummy, &nCol, &op, 0);

  assert( eType==SQLITE_CHANGESET_CONFLICT || eType==SQLITE_CHANGESET_DATA );
  assert( SQLITE_CHANGESET_CONFLICT+1==SQLITE_CHANGESET_CONSTRAINT );
  assert( SQLITE_CHANGESET_DATA+1==SQLITE_CHANGESET_NOTFOUND );

  /* Bind the new.* PRIMARY KEY values to the SELECT statement. */
  if( pbReplace ){
    rc = sessionSeekToRow(p->db, pIter, p->abPK, p->pSelect);
  }else{
    rc = SQLITE_OK;
  }

  if( rc==SQLITE_ROW ){
    /* There exists another row with the new.* primary key. */
    pIter->pConflict = p->pSelect;
    res = xConflict(pCtx, eType, pIter);
    pIter->pConflict = 0;
    rc = sqlite3_reset(p->pSelect);
  }else if( rc==SQLITE_OK ){
    /* No other row with the new.* primary key. */
    res = xConflict(pCtx, eType+1, pIter);
    if( res==SQLITE_CHANGESET_REPLACE ) rc = SQLITE_MISUSE;
  }

  if( rc==SQLITE_OK ){
    switch( res ){
      case SQLITE_CHANGESET_REPLACE:
        assert( pbReplace );
        *pbReplace = 1;
        break;

      case SQLITE_CHANGESET_OMIT:
        break;

      case SQLITE_CHANGESET_ABORT:
        rc = SQLITE_ABORT;
        break;

      default:
        rc = SQLITE_MISUSE;
        break;
    }
  }

  return rc;
}

/*
** Attempt to apply the change that the iterator passed as the first argument
** currently points to to the database. If a conflict is encountered, invoke
** the conflict handler callback.
**
** If argument pbRetry is NULL, then ignore any CHANGESET_DATA conflict. If
** one is encountered, update or delete the row with the matching primary key
** instead. Or, if pbRetry is not NULL and a CHANGESET_DATA conflict occurs,
** invoke the conflict handler. If it returns CHANGESET_REPLACE, set *pbRetry
** to true before returning. In this case the caller will invoke this function
** again, this time with pbRetry set to NULL.
**
** If argument pbReplace is NULL and a CHANGESET_CONFLICT conflict is 
** encountered invoke the conflict handler with CHANGESET_CONSTRAINT instead.
** Or, if pbReplace is not NULL, invoke it with CHANGESET_CONFLICT. If such
** an invocation returns SQLITE_CHANGESET_REPLACE, set *pbReplace to true
** before retrying. In this case the caller attempts to remove the conflicting
** row before invoking this function again, this time with pbReplace set 
** to NULL.
**
** If any conflict handler returns SQLITE_CHANGESET_ABORT, this function
** returns SQLITE_ABORT. Otherwise, if no error occurs, SQLITE_OK is 
** returned.
*/
static int sessionApplyOneOp(
  sqlite3_changeset_iter *pIter,  /* Changeset iterator */
  SessionApplyCtx *p,             /* changeset_apply() context */
  int(*xConflict)(void *, int, sqlite3_changeset_iter *),
  void *pCtx,                     /* First argument for the conflict handler */
  int *pbReplace,                 /* OUT: True to remove PK row and retry */
  int *pbRetry                    /* OUT: True to retry. */
){
  const char *zDummy;
  int op;
  int nCol;
  int rc = SQLITE_OK;

  assert( p->pDelete && p->pUpdate && p->pInsert && p->pSelect );
  assert( p->azCol && p->abPK );
  assert( !pbReplace || *pbReplace==0 );

  sqlite3changeset_op(pIter, &zDummy, &nCol, &op, 0);

  if( op==SQLITE_DELETE ){

    /* Bind values to the DELETE statement. */
    rc = sessionBindRow(pIter, sqlite3changeset_old, nCol, 0, p->pDelete);
    if( rc==SQLITE_OK && sqlite3_bind_parameter_count(p->pDelete)>nCol ){
      rc = sqlite3_bind_int(p->pDelete, nCol+1, pbRetry==0);
    }
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_step(p->pDelete);
    rc = sqlite3_reset(p->pDelete);
    if( rc==SQLITE_OK && sqlite3_changes(p->db)==0 ){
      rc = sessionConflictHandler(
          SQLITE_CHANGESET_DATA, p, pIter, xConflict, pCtx, pbRetry
      );
    }else if( (rc&0xff)==SQLITE_CONSTRAINT ){
      rc = sessionConflictHandler(
          SQLITE_CHANGESET_CONFLICT, p, pIter, xConflict, pCtx, 0
      );
    }

  }else if( op==SQLITE_UPDATE ){
    int i;

    /* Bind values to the UPDATE statement. */
    for(i=0; rc==SQLITE_OK && i<nCol; i++){
      sqlite3_value *pOld = sessionChangesetOld(pIter, i);
      sqlite3_value *pNew = sessionChangesetNew(pIter, i);

      sqlite3_bind_int(p->pUpdate, i*3+2, !!pNew);
      if( pOld ){
        rc = sessionBindValue(p->pUpdate, i*3+1, pOld);
      }
      if( rc==SQLITE_OK && pNew ){
        rc = sessionBindValue(p->pUpdate, i*3+3, pNew);
      }
    }
    if( rc==SQLITE_OK ) sqlite3_bind_int(p->pUpdate, nCol*3+1, pbRetry==0);
    if( rc!=SQLITE_OK ) return rc;

    /* Attempt the UPDATE. In the case of a NOTFOUND or DATA conflict,
    ** the result will be SQLITE_OK with 0 rows modified. */
    sqlite3_step(p->pUpdate);
    rc = sqlite3_reset(p->pUpdate);

    if( rc==SQLITE_OK && sqlite3_changes(p->db)==0 ){
      /* A NOTFOUND or DATA error. Search the table to see if it contains
      ** a row with a matching primary key. If so, this is a DATA conflict.
      ** Otherwise, if there is no primary key match, it is a NOTFOUND. */

      rc = sessionConflictHandler(
          SQLITE_CHANGESET_DATA, p, pIter, xConflict, pCtx, pbRetry
      );

    }else if( (rc&0xff)==SQLITE_CONSTRAINT ){
      /* This is always a CONSTRAINT conflict. */
      rc = sessionConflictHandler(
          SQLITE_CHANGESET_CONFLICT, p, pIter, xConflict, pCtx, 0
      );
    }

  }else{
    assert( op==SQLITE_INSERT );
    rc = sessionBindRow(pIter, sqlite3changeset_new, nCol, 0, p->pInsert);
    if( rc!=SQLITE_OK ) return rc;

    sqlite3_step(p->pInsert);
    rc = sqlite3_reset(p->pInsert);
    if( (rc&0xff)==SQLITE_CONSTRAINT ){
      rc = sessionConflictHandler(
          SQLITE_CHANGESET_CONFLICT, p, pIter, xConflict, pCtx, pbReplace
      );
    }
  }

  return rc;
}

/*
** Apply the changeset passed via pChangeset/nChangeset to the main database
** attached to handle "db". Invoke the supplied conflict handler callback
** to resolve any conflicts encountered while applying the change.
*/
int sqlite3changeset_apply(
  sqlite3 *db,                    /* Apply change to "main" db of this handle */
  int nChangeset,                 /* Size of changeset in bytes */
  void *pChangeset,               /* Changeset blob */
  int(*xFilter)(
    void *pCtx,                   /* Copy of sixth arg to _apply() */
    const char *zTab              /* Table name */
  ),
  int(*xConflict)(
    void *pCtx,                   /* Copy of fifth arg to _apply() */
    int eConflict,                /* DATA, MISSING, CONFLICT, CONSTRAINT */
    sqlite3_changeset_iter *p     /* Handle describing change and conflict */
  ),
  void *pCtx                      /* First argument passed to xConflict */
){
  int schemaMismatch = 0;
  sqlite3_changeset_iter *pIter;  /* Iterator to skip through changeset */  
  int rc;                         /* Return code */
  const char *zTab = 0;           /* Name of current table */
  int nTab = 0;                   /* Result of sqlite3Strlen30(zTab) */
  SessionApplyCtx sApply;         /* changeset_apply() context object */

  memset(&sApply, 0, sizeof(sApply));
  rc = sqlite3changeset_start(&pIter, nChangeset, pChangeset);
  if( rc!=SQLITE_OK ) return rc;

  sqlite3_mutex_enter(sqlite3_db_mutex(db));
  rc = sqlite3_exec(db, "SAVEPOINT changeset_apply", 0, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "PRAGMA defer_foreign_keys = 1", 0, 0, 0);
  }
  while( rc==SQLITE_OK && SQLITE_ROW==sqlite3changeset_next(pIter) ){
    int nCol;
    int op;
    int bReplace = 0;
    int bRetry = 0;
    const char *zNew;
    
    sqlite3changeset_op(pIter, &zNew, &nCol, &op, 0);

    if( zTab==0 || sqlite3_strnicmp(zNew, zTab, nTab+1) ){
      u8 *abPK;

      sqlite3_free((char*)sApply.azCol);  /* cast works around VC++ bug */
      sqlite3_finalize(sApply.pDelete);
      sqlite3_finalize(sApply.pUpdate); 
      sqlite3_finalize(sApply.pInsert);
      sqlite3_finalize(sApply.pSelect);
      memset(&sApply, 0, sizeof(sApply));
      sApply.db = db;

      /* If an xFilter() callback was specified, invoke it now. If the 
      ** xFilter callback returns zero, skip this table. If it returns
      ** non-zero, proceed. */
      schemaMismatch = (xFilter && (0==xFilter(pCtx, zNew)));
      if( schemaMismatch ){
        zTab = sqlite3_mprintf("%s", zNew);
        nTab = (int)strlen(zTab);
        sApply.azCol = (const char **)zTab;
      }else{
        sqlite3changeset_pk(pIter, &abPK, 0);
        rc = sessionTableInfo(
            db, "main", zNew, &sApply.nCol, &zTab, &sApply.azCol, &sApply.abPK
        );
        if( rc!=SQLITE_OK ) break;
  
        if( sApply.nCol==0 ){
          schemaMismatch = 1;
          sqlite3_log(SQLITE_SCHEMA, 
              "sqlite3changeset_apply(): no such table: %s", zTab
          );
        }
        else if( sApply.nCol!=nCol ){
          schemaMismatch = 1;
          sqlite3_log(SQLITE_SCHEMA, 
              "sqlite3changeset_apply(): table %s has %d columns, expected %d", 
              zTab, sApply.nCol, nCol
          );
        }
        else if( memcmp(sApply.abPK, abPK, nCol)!=0 ){
          schemaMismatch = 1;
          sqlite3_log(SQLITE_SCHEMA, "sqlite3changeset_apply(): "
              "primary key mismatch for table %s", zTab
          );
        }
        else if( 
            (rc = sessionSelectRow(db, zTab, &sApply))
         || (rc = sessionUpdateRow(db, zTab, &sApply))
         || (rc = sessionDeleteRow(db, zTab, &sApply))
         || (rc = sessionInsertRow(db, zTab, &sApply))
        ){
          break;
        }
        nTab = sqlite3Strlen30(zTab);
      }
    }

    /* If there is a schema mismatch on the current table, proceed to the
    ** next change. A log message has already been issued. */
    if( schemaMismatch ) continue;

    rc = sessionApplyOneOp(pIter, &sApply, xConflict, pCtx, &bReplace, &bRetry);

    if( rc==SQLITE_OK && bRetry ){
      rc = sessionApplyOneOp(pIter, &sApply, xConflict, pCtx, &bReplace, 0);
    }

    if( bReplace ){
      assert( pIter->op==SQLITE_INSERT );
      rc = sqlite3_exec(db, "SAVEPOINT replace_op", 0, 0, 0);
      if( rc==SQLITE_OK ){
        rc = sessionBindRow(pIter, 
            sqlite3changeset_new, sApply.nCol, sApply.abPK, sApply.pDelete);
        sqlite3_bind_int(sApply.pDelete, sApply.nCol+1, 1);
      }
      if( rc==SQLITE_OK ){
        sqlite3_step(sApply.pDelete);
        rc = sqlite3_reset(sApply.pDelete);
      }
      if( rc==SQLITE_OK ){
        rc = sessionApplyOneOp(pIter, &sApply, xConflict, pCtx, 0, 0);
      }
      if( rc==SQLITE_OK ){
        rc = sqlite3_exec(db, "RELEASE replace_op", 0, 0, 0);
      }
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3changeset_finalize(pIter);
  }else{
    sqlite3changeset_finalize(pIter);
  }

  if( rc==SQLITE_OK ){
    int nFk, notUsed;
    sqlite3_db_status(db, SQLITE_DBSTATUS_DEFERRED_FKS, &nFk, &notUsed, 0);
    if( nFk!=0 ){
      int res = SQLITE_CHANGESET_ABORT;
      if( xConflict ){
        sqlite3_changeset_iter sIter;
        memset(&sIter, 0, sizeof(sIter));
        sIter.nCol = nFk;
        res = xConflict(pCtx, SQLITE_CHANGESET_FOREIGN_KEY, &sIter);
      }
      if( res!=SQLITE_CHANGESET_OMIT ){
        rc = SQLITE_CONSTRAINT;
      }
    }
  }
  sqlite3_exec(db, "PRAGMA defer_foreign_keys = 0", 0, 0, 0);

  if( rc==SQLITE_OK ){
    rc = sqlite3_exec(db, "RELEASE changeset_apply", 0, 0, 0);
  }else{
    sqlite3_exec(db, "ROLLBACK TO changeset_apply", 0, 0, 0);
    sqlite3_exec(db, "RELEASE changeset_apply", 0, 0, 0);
  }

  sqlite3_finalize(sApply.pInsert);
  sqlite3_finalize(sApply.pDelete);
  sqlite3_finalize(sApply.pUpdate);
  sqlite3_finalize(sApply.pSelect);
  sqlite3_free((char*)sApply.azCol);  /* cast works around VC++ bug */
  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  return rc;
}

/*
** This function is called to merge two changes to the same row together as
** part of an sqlite3changeset_concat() operation. A new change object is
** allocated and a pointer to it stored in *ppNew.
*/
static int sessionChangeMerge(
  SessionTable *pTab,             /* Table structure */
  SessionChange *pExist,          /* Existing change */
  int op2,                        /* Second change operation */
  int bIndirect,                  /* True if second change is indirect */
  u8 *aRec,                       /* Second change record */
  int nRec,                       /* Number of bytes in aRec */
  SessionChange **ppNew           /* OUT: Merged change */
){
  SessionChange *pNew = 0;

  if( !pExist ){
    pNew = (SessionChange *)sqlite3_malloc(sizeof(SessionChange));
    if( !pNew ){
      return SQLITE_NOMEM;
    }
    memset(pNew, 0, sizeof(SessionChange));
    pNew->op = op2;
    pNew->bIndirect = bIndirect;
    pNew->nRecord = nRec;
    pNew->aRecord = aRec;
  }else{
    int op1 = pExist->op;

    /* 
    **   op1=INSERT, op2=INSERT      ->      Unsupported. Discard op2.
    **   op1=INSERT, op2=UPDATE      ->      INSERT.
    **   op1=INSERT, op2=DELETE      ->      (none)
    **
    **   op1=UPDATE, op2=INSERT      ->      Unsupported. Discard op2.
    **   op1=UPDATE, op2=UPDATE      ->      UPDATE.
    **   op1=UPDATE, op2=DELETE      ->      DELETE.
    **
    **   op1=DELETE, op2=INSERT      ->      UPDATE.
    **   op1=DELETE, op2=UPDATE      ->      Unsupported. Discard op2.
    **   op1=DELETE, op2=DELETE      ->      Unsupported. Discard op2.
    */   
    if( (op1==SQLITE_INSERT && op2==SQLITE_INSERT)
     || (op1==SQLITE_UPDATE && op2==SQLITE_INSERT)
     || (op1==SQLITE_DELETE && op2==SQLITE_UPDATE)
     || (op1==SQLITE_DELETE && op2==SQLITE_DELETE)
    ){
      pNew = pExist;
    }else if( op1==SQLITE_INSERT && op2==SQLITE_DELETE ){
      sqlite3_free(pExist);
      assert( pNew==0 );
    }else{
      int nByte;
      u8 *aCsr;

      nByte = sizeof(SessionChange) + pExist->nRecord + nRec;
      pNew = (SessionChange *)sqlite3_malloc(nByte);
      if( !pNew ){
        sqlite3_free(pExist);
        return SQLITE_NOMEM;
      }
      memset(pNew, 0, sizeof(SessionChange));
      pNew->bIndirect = (bIndirect && pExist->bIndirect);
      aCsr = pNew->aRecord = (u8 *)&pNew[1];

      if( op1==SQLITE_INSERT ){             /* INSERT + UPDATE */
        u8 *a1 = aRec;
        assert( op2==SQLITE_UPDATE );
        pNew->op = SQLITE_INSERT;
        sessionReadRecord(&a1, pTab->nCol, 0);
        sessionMergeRecord(&aCsr, pTab->nCol, pExist->aRecord, a1);
      }else if( op1==SQLITE_DELETE ){       /* DELETE + INSERT */
        assert( op2==SQLITE_INSERT );
        pNew->op = SQLITE_UPDATE;
        if( 0==sessionMergeUpdate(&aCsr, pTab, pExist->aRecord, 0, aRec, 0) ){
          sqlite3_free(pNew);
          pNew = 0;
        }
      }else if( op2==SQLITE_UPDATE ){       /* UPDATE + UPDATE */
        u8 *a1 = pExist->aRecord;
        u8 *a2 = aRec;
        assert( op1==SQLITE_UPDATE );
        sessionReadRecord(&a1, pTab->nCol, 0);
        sessionReadRecord(&a2, pTab->nCol, 0);
        pNew->op = SQLITE_UPDATE;
        if( 0==sessionMergeUpdate(&aCsr, pTab, aRec, pExist->aRecord, a1, a2) ){
          sqlite3_free(pNew);
          pNew = 0;
        }
      }else{                                /* UPDATE + DELETE */
        assert( op1==SQLITE_UPDATE && op2==SQLITE_DELETE );
        pNew->op = SQLITE_DELETE;
        sessionMergeRecord(&aCsr, pTab->nCol, aRec, pExist->aRecord);
      }

      if( pNew ){
        pNew->nRecord = (int)(aCsr - pNew->aRecord);
      }
      sqlite3_free(pExist);
    }
  }

  *ppNew = pNew;
  return SQLITE_OK;
}

/*
** Add all changes in the changeset passed via the first two arguments to
** hash tables.
*/
static int sessionConcatChangeset(
  int nChangeset,                 /* Number of bytes in pChangeset */
  void *pChangeset,               /* Changeset buffer */
  SessionTable **ppTabList        /* IN/OUT: List of table objects */
){
  u8 *aRec;
  int nRec;
  sqlite3_changeset_iter *pIter;
  int rc;
  SessionTable *pTab = 0;

  rc = sqlite3changeset_start(&pIter, nChangeset, pChangeset);
  if( rc!=SQLITE_OK ) return rc;

  while( SQLITE_ROW==sessionChangesetNext(pIter, &aRec, &nRec) ){
    const char *zNew;
    int nCol;
    int op;
    int iHash;
    int bIndirect;
    SessionChange *pChange;
    SessionChange *pExist = 0;
    SessionChange **pp;

    assert( pIter->apValue==0 );
    sqlite3changeset_op(pIter, &zNew, &nCol, &op, &bIndirect);

    assert( zNew>=(char *)pChangeset && zNew-nChangeset<((char *)pChangeset) );
    assert( !pTab || pTab->zName-nChangeset<(char *)pChangeset );
    assert( !pTab || zNew>=pTab->zName );

    if( !pTab || zNew!=pTab->zName ){
      /* Search the list for a matching table */
      int nNew = (int)strlen(zNew);
      u8 *abPK;

      sqlite3changeset_pk(pIter, &abPK, 0);
      for(pTab = *ppTabList; pTab; pTab=pTab->pNext){
        if( 0==sqlite3_strnicmp(pTab->zName, zNew, nNew+1) ) break;
      }
      if( !pTab ){
        pTab = sqlite3_malloc(sizeof(SessionTable));
        if( !pTab ){
          rc = SQLITE_NOMEM;
          break;
        }
        memset(pTab, 0, sizeof(SessionTable));
        pTab->pNext = *ppTabList;
        pTab->abPK = abPK;
        pTab->nCol = nCol;
        *ppTabList = pTab;
      }else if( pTab->nCol!=nCol || memcmp(pTab->abPK, abPK, nCol) ){
        rc = SQLITE_SCHEMA;
        break;
      }
      pTab->zName = (char *)zNew;
    }

    if( sessionGrowHash(pTab) ){
      rc = SQLITE_NOMEM;
      break;
    }
    iHash = sessionChangeHash(pTab, aRec, pTab->nChange);

    /* Search for existing entry. If found, remove it from the hash table. 
    ** Code below may link it back in.
    */
    for(pp=&pTab->apChange[iHash]; *pp; pp=&(*pp)->pNext){
      if( sessionChangeEqual(pTab, (*pp)->aRecord, aRec) ){
        pExist = *pp;
        *pp = (*pp)->pNext;
        pTab->nEntry--;
        break;
      }
    }

    rc = sessionChangeMerge(pTab, pExist, op, bIndirect, aRec, nRec, &pChange);
    if( rc ) break;
    if( pChange ){
      pChange->pNext = pTab->apChange[iHash];
      pTab->apChange[iHash] = pChange;
      pTab->nEntry++;
    }
  }

  if( rc==SQLITE_OK ){
    rc = sqlite3changeset_finalize(pIter);
  }else{
    sqlite3changeset_finalize(pIter);
  }
  return rc;
}
  

/* 
** 1. Iterate through the left-hand changeset. Add an entry to a table
**    specific hash table for each change in the changeset. The hash table
**    key is the PK of the row affected by the change.
**
** 2. Then interate through the right-hand changeset. Attempt to add an 
**    entry to a hash table for each component change. If a change already 
**    exists with the same PK values, combine the two into a single change.
**
** 3. Write an output changeset based on the contents of the hash table.
*/
int sqlite3changeset_concat(
  int nLeft,                      /* Number of bytes in lhs input */
  void *pLeft,                    /* Lhs input changeset */
  int nRight                      /* Number of bytes in rhs input */,
  void *pRight,                   /* Rhs input changeset */
  int *pnOut,                     /* OUT: Number of bytes in output changeset */
  void **ppOut                    /* OUT: changeset (left <concat> right) */
){
  SessionTable *pList = 0;        /* List of SessionTable objects */
  int rc;                         /* Return code */

  *pnOut = 0;
  *ppOut = 0;

  rc = sessionConcatChangeset(nLeft, pLeft, &pList);
  if( rc==SQLITE_OK ){
    rc = sessionConcatChangeset(nRight, pRight, &pList);
  }

  /* Create the serialized output changeset based on the contents of the
  ** hash tables attached to the SessionTable objects in list pList. 
  */
  if( rc==SQLITE_OK ){
    SessionTable *pTab;
    SessionBuffer buf = {0, 0, 0};
    for(pTab=pList; pTab; pTab=pTab->pNext){
      int i;
      if( pTab->nEntry==0 ) continue;

      sessionAppendTableHdr(&buf, pTab, &rc);
      for(i=0; i<pTab->nChange; i++){
        SessionChange *p;
        for(p=pTab->apChange[i]; p; p=p->pNext){
          sessionAppendByte(&buf, p->op, &rc);
          sessionAppendByte(&buf, p->bIndirect, &rc);
          sessionAppendBlob(&buf, p->aRecord, p->nRecord, &rc);
        }
      }
    }

    if( rc==SQLITE_OK ){
      *ppOut = buf.aBuf;
      *pnOut = buf.nBuf;
    }else{
      sqlite3_free(buf.aBuf);
    }
  }

  sessionDeleteTable(pList);
  return rc;
}

#endif /* SQLITE_ENABLE_SESSION && SQLITE_ENABLE_PREUPDATE_HOOK */
