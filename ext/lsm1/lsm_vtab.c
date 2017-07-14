/*
** 2015-11-16
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
** This file implements a simple virtual table wrapper around the LSM
** storage engine from SQLite4.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include "lsm.h"
#include <assert.h>
#include <string.h>

/* Forward declaration of subclasses of virtual table objects */
typedef struct lsm1_vtab lsm1_vtab;
typedef struct lsm1_cursor lsm1_cursor;

/* Primitive types */
typedef unsigned char u8;

/* An open connection to an LSM table */
struct lsm1_vtab {
  sqlite3_vtab base;          /* Base class - must be first */
  lsm_db *pDb;                /* Open connection to the LSM table */
};


/* lsm1_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
struct lsm1_cursor {
  sqlite3_vtab_cursor base;  /* Base class - must be first */
  lsm_cursor *pLsmCur;       /* The LSM cursor */
  u8 isDesc;                 /* 0: scan forward.  1: scan reverse */
  u8 atEof;                  /* True if the scan is complete */
  u8 bUnique;                /* True if no more than one row of output */
};

/* Dequote the string */
static void lsm1Dequote(char *z){
  int j;
  char cQuote = z[0];
  size_t i, n;

  if( cQuote!='\'' && cQuote!='"' ) return;
  n = strlen(z);
  if( n<2 || z[n-1]!=z[0] ) return;
  for(i=1, j=0; i<n-1; i++){
    if( z[i]==cQuote && z[i+1]==cQuote ) i++;
    z[j++] = z[i];
  }
  z[j] = 0;
}


/*
** The lsm1Connect() method is invoked to create a new
** lsm1_vtab that describes the virtual table.
*/
static int lsm1Connect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  lsm1_vtab *pNew;
  int rc;
  char *zFilename;

  if( argc!=4 || argv[3]==0 || argv[3][0]==0 ){
    *pzErr = sqlite3_mprintf("filename argument missing");
    return SQLITE_ERROR;
  }
  *ppVtab = sqlite3_malloc( sizeof(*pNew) );
  pNew = (lsm1_vtab*)*ppVtab;
  if( pNew==0 ){
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(*pNew));
  rc = lsm_new(0, &pNew->pDb);
  if( rc ){
    *pzErr = sqlite3_mprintf("lsm_new failed with error code %d",  rc);
    rc = SQLITE_ERROR;
    goto connect_failed;
  }
  zFilename = sqlite3_mprintf("%s", argv[3]);
  lsm1Dequote(zFilename);
  rc = lsm_open(pNew->pDb, zFilename);
  sqlite3_free(zFilename);
  if( rc ){
    *pzErr = sqlite3_mprintf("lsm_open failed with %d", rc);
    rc = SQLITE_ERROR;
    goto connect_failed;
  }

/* Column numbers */
#define LSM1_COLUMN_KEY         0
#define LSM1_COLUMN_BLOBKEY     1
#define LSM1_COLUMN_VALUE       2
#define LSM1_COLUMN_BLOBVALUE   3
#define LSM1_COLUMN_COMMAND     4

  rc = sqlite3_declare_vtab(db,
     "CREATE TABLE x("
     "  key,"              /* The primary key.  Any non-NULL */
     "  blobkey,"          /* Pure BLOB primary key */
     "  value,"            /* The value associated with key.  Any non-NULL */
     "  blobvalue,"        /* Pure BLOB value */
     "  command hidden"    /* Insert here for control operations */
     ");"
  );
connect_failed:
  if( rc!=SQLITE_OK ){
    if( pNew ){
      if( pNew->pDb ) lsm_close(pNew->pDb);
      sqlite3_free(pNew);
    }
    *ppVtab = 0;
  }
  return rc;
}

/*
** This method is the destructor for lsm1_cursor objects.
*/
static int lsm1Disconnect(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  lsm_close(p->pDb);
  sqlite3_free(p);
  return SQLITE_OK;
}

/*
** Constructor for a new lsm1_cursor object.
*/
static int lsm1Open(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCursor){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  lsm1_cursor *pCur;
  int rc;
  pCur = sqlite3_malloc( sizeof(*pCur) );
  if( pCur==0 ) return SQLITE_NOMEM;
  memset(pCur, 0, sizeof(*pCur));
  *ppCursor = &pCur->base;
  rc = lsm_csr_open(p->pDb, &pCur->pLsmCur);
  if( rc==LSM_OK ){
    rc = SQLITE_OK;
  }else{
    sqlite3_free(pCur);
    *ppCursor = 0;
    rc = SQLITE_ERROR;
  }
  return rc;
}

/*
** Destructor for a lsm1_cursor.
*/
static int lsm1Close(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  lsm_csr_close(pCur->pLsmCur);
  sqlite3_free(pCur);
  return SQLITE_OK;
}


/*
** Advance a lsm1_cursor to its next row of output.
*/
static int lsm1Next(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  int rc = LSM_OK;
  if( pCur->bUnique ){
    pCur->atEof = 1;
  }else{
    if( pCur->isDesc ){
      rc = lsm_csr_prev(pCur->pLsmCur);
    }else{
      rc = lsm_csr_next(pCur->pLsmCur);
    }
    if( rc==LSM_OK && lsm_csr_valid(pCur->pLsmCur)==0 ){
      pCur->atEof = 1;
    }
  }
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int lsm1Eof(sqlite3_vtab_cursor *cur){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  return pCur->atEof;
}

/*
** Rowids are not supported by the underlying virtual table.  So always
** return 0 for the rowid.
*/
static int lsm1Rowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  *pRowid = 0;
  return SQLITE_OK;
}

/*
** Type prefixes on LSM keys
*/
#define LSM1_TYPE_NEGATIVE   0
#define LSM1_TYPE_POSITIVE   1
#define LSM1_TYPE_TEXT       2
#define LSM1_TYPE_BLOB       3

/*
** Write a 32-bit unsigned integer as 4 big-endian bytes.
*/
static void varintWrite32(unsigned char *z, unsigned int y){
  z[0] = (unsigned char)(y>>24);
  z[1] = (unsigned char)(y>>16);
  z[2] = (unsigned char)(y>>8);
  z[3] = (unsigned char)(y);
}

/*
** Write a varint into z[].  The buffer z[] must be at least 9 characters
** long to accommodate the largest possible varint.  Return the number of
** bytes of z[] used.
*/
static int lsm1PutVarint64(unsigned char *z, sqlite3_uint64 x){
  unsigned int w, y;
  if( x<=240 ){
    z[0] = (unsigned char)x;
    return 1;
  }
  if( x<=2287 ){
    y = (unsigned int)(x - 240);
    z[0] = (unsigned char)(y/256 + 241);
    z[1] = (unsigned char)(y%256);
    return 2;
  }
  if( x<=67823 ){
    y = (unsigned int)(x - 2288);
    z[0] = 249;
    z[1] = (unsigned char)(y/256);
    z[2] = (unsigned char)(y%256);
    return 3;
  }
  y = (unsigned int)x;
  w = (unsigned int)(x>>32);
  if( w==0 ){
    if( y<=16777215 ){
      z[0] = 250;
      z[1] = (unsigned char)(y>>16);
      z[2] = (unsigned char)(y>>8);
      z[3] = (unsigned char)(y);
      return 4;
    }
    z[0] = 251;
    varintWrite32(z+1, y);
    return 5;
  }
  if( w<=255 ){
    z[0] = 252;
    z[1] = (unsigned char)w;
    varintWrite32(z+2, y);
    return 6;
  }
  if( w<=65535 ){
    z[0] = 253;
    z[1] = (unsigned char)(w>>8);
    z[2] = (unsigned char)w;
    varintWrite32(z+3, y);
    return 7;
  }
  if( w<=16777215 ){
    z[0] = 254;
    z[1] = (unsigned char)(w>>16);
    z[2] = (unsigned char)(w>>8);
    z[3] = (unsigned char)w;
    varintWrite32(z+4, y);
    return 8;
  }
  z[0] = 255;
  varintWrite32(z+1, w);
  varintWrite32(z+5, y);
  return 9;
}

/*
** Decode the varint in the first n bytes z[].  Write the integer value
** into *pResult and return the number of bytes in the varint.
**
** If the decode fails because there are not enough bytes in z[] then
** return 0;
*/
static int lsm1GetVarint64(
  const unsigned char *z,
  int n,
  sqlite3_uint64 *pResult
){
  unsigned int x;
  if( n<1 ) return 0;
  if( z[0]<=240 ){
    *pResult = z[0];
    return 1;
  }
  if( z[0]<=248 ){
    if( n<2 ) return 0;
    *pResult = (z[0]-241)*256 + z[1] + 240;
    return 2;
  }
  if( n<z[0]-246 ) return 0;
  if( z[0]==249 ){
    *pResult = 2288 + 256*z[1] + z[2];
    return 3;
  }
  if( z[0]==250 ){
    *pResult = (z[1]<<16) + (z[2]<<8) + z[3];
    return 4;
  }
  x = (z[1]<<24) + (z[2]<<16) + (z[3]<<8) + z[4];
  if( z[0]==251 ){
    *pResult = x;
    return 5;
  }
  if( z[0]==252 ){
    *pResult = (((sqlite3_uint64)x)<<8) + z[5];
    return 6;
  }
  if( z[0]==253 ){
    *pResult = (((sqlite3_uint64)x)<<16) + (z[5]<<8) + z[6];
    return 7;
  }
  if( z[0]==254 ){
    *pResult = (((sqlite3_uint64)x)<<24) + (z[5]<<16) + (z[6]<<8) + z[7];
    return 8;
  }
  *pResult = (((sqlite3_uint64)x)<<32) +
               (0xffffffff & ((z[5]<<24) + (z[6]<<16) + (z[7]<<8) + z[8]));
  return 9;
}

/*
** Generate a key encoding for pValue such that all keys compare in
** lexicographical order.  Return an SQLite error code or SQLITE_OK.
**
** The key encoding is *pnKey bytes in length written into *ppKey.
** Space to hold the key is taken from pSpace if sufficient, or else
** from sqlite3_malloc().  The caller is responsible for freeing malloced
** space.
*/
static int lsm1EncodeKey(
  sqlite3_value *pValue,     /* Value to be encoded */
  unsigned char **ppKey,     /* Write the encoding here */
  int *pnKey,                /* Write the size of the encoding here */
  unsigned char *pSpace,     /* Use this space if it is large enough */
  int nSpace                 /* Size of pSpace[] */
){
  int eType = sqlite3_value_type(pValue);
  *ppKey = 0;
  *pnKey = 0;
  assert( nSpace>=32 );
  switch( eType ){
    default: {
      return SQLITE_ERROR;  /* We cannot handle NULL keys */
    }
    case SQLITE_BLOB:
    case SQLITE_TEXT: {
      int nVal = sqlite3_value_bytes(pValue);
      const void *pVal;
      if( eType==SQLITE_BLOB ){
        eType = LSM1_TYPE_BLOB;
        pVal = sqlite3_value_blob(pValue);
      }else{
        eType = LSM1_TYPE_TEXT;
        pVal = (const void*)sqlite3_value_text(pValue);
        if( pVal==0 ) return SQLITE_NOMEM;
      }
      if( nVal+1>nSpace ){
        pSpace = sqlite3_malloc( nVal+1 );
        if( pSpace==0 ) return SQLITE_NOMEM;
      }
      pSpace[0] = (unsigned char)eType;
      memcpy(&pSpace[1], pVal, nVal);
      *ppKey = pSpace;
      *pnKey = nVal+1;
      break;
    }
    case SQLITE_INTEGER: {
      sqlite3_int64 iVal = sqlite3_value_int64(pValue);
      sqlite3_uint64 uVal;
      if( iVal<0 ){
        if( iVal==0xffffffffffffffffLL ) return SQLITE_ERROR;
        uVal = *(sqlite3_uint64*)&iVal;
        eType = LSM1_TYPE_NEGATIVE;
      }else{
        uVal = iVal;
        eType = LSM1_TYPE_POSITIVE;
      }
      pSpace[0] = (unsigned char)eType;
      *ppKey = pSpace;
      *pnKey = 1 + lsm1PutVarint64(&pSpace[1], uVal);
    }
  }
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the lsm1_cursor
** is currently pointing.
*/
static int lsm1Column(
  sqlite3_vtab_cursor *cur,   /* The cursor */
  sqlite3_context *ctx,       /* First argument to sqlite3_result_...() */
  int i                       /* Which column to return */
){
  lsm1_cursor *pCur = (lsm1_cursor*)cur;
  switch( i ){
    case LSM1_COLUMN_BLOBKEY: {
      const void *pVal;
      int nVal;
      if( lsm_csr_key(pCur->pLsmCur, &pVal, &nVal)==LSM_OK ){
        sqlite3_result_blob(ctx, pVal, nVal, SQLITE_TRANSIENT);
      }
      break;
    }
    case LSM1_COLUMN_KEY: {
      const unsigned char *pVal;
      int nVal;
      if( lsm_csr_key(pCur->pLsmCur, (const void**)&pVal, &nVal)==LSM_OK
       && nVal>=1
      ){
        if( pVal[0]==LSM1_TYPE_BLOB ){
          sqlite3_result_blob(ctx, (const void*)&pVal[1],nVal-1,
                              SQLITE_TRANSIENT);
        }else if( pVal[0]==LSM1_TYPE_TEXT ){
          sqlite3_result_text(ctx, (const char*)&pVal[1],nVal-1,
                              SQLITE_TRANSIENT);
        }else if( nVal>=2 && nVal<=10 &&
           (pVal[0]==LSM1_TYPE_POSITIVE || pVal[0]==LSM1_TYPE_NEGATIVE)
        ){
          sqlite3_int64 iVal;
          lsm1GetVarint64(pVal+1, nVal-1, (sqlite3_uint64*)&iVal);
          sqlite3_result_int64(ctx, iVal);
        }         
      }
      break;
    }
    case LSM1_COLUMN_BLOBVALUE: {
      const void *pVal;
      int nVal;
      if( lsm_csr_value(pCur->pLsmCur, (const void**)&pVal, &nVal)==LSM_OK ){
        sqlite3_result_blob(ctx, pVal, nVal, SQLITE_TRANSIENT);
      }
      break;
    }
    case LSM1_COLUMN_VALUE: {
      const unsigned char *aVal;
      int nVal;
      if( lsm_csr_value(pCur->pLsmCur, (const void**)&aVal, &nVal)==LSM_OK
          && nVal>=1
      ){
        switch( aVal[0] ){
          case SQLITE_FLOAT:
          case SQLITE_INTEGER: {
            sqlite3_uint64 x = 0;
            int j;
            for(j=1; j<nVal; j++){
              x = (x<<8) | aVal[j];
            }
            if( aVal[0]==SQLITE_INTEGER ){
              sqlite3_result_int64(ctx, *(sqlite3_int64*)&x);
            }else{
              double r;
              assert( sizeof(r)==sizeof(x) );
              memcpy(&r, &x, sizeof(r));
              sqlite3_result_double(ctx, r);
            }
            break;
          }
          case SQLITE_TEXT: {
            sqlite3_result_text(ctx, (char*)&aVal[1], nVal-1, SQLITE_TRANSIENT);
            break;
          }
          case SQLITE_BLOB: {
            sqlite3_result_blob(ctx, &aVal[1], nVal-1, SQLITE_TRANSIENT);
            break;
          }
        }
      }
      break;
    }
    default: {
      break;
    }
  }
  return SQLITE_OK;
}

/* Move to the first row to return.
*/
static int lsm1Filter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  lsm1_cursor *pCur = (lsm1_cursor *)pVtabCursor;
  int rc = LSM_OK;
  pCur->atEof = 1;
  if( idxNum==1 ){
    assert( argc==1 );
    pCur->isDesc = 0;
    pCur->bUnique = 1;
    if( sqlite3_value_type(argv[0])==SQLITE_BLOB ){
      const void *pVal = sqlite3_value_blob(argv[0]);
      int nVal = sqlite3_value_bytes(argv[0]);
      rc = lsm_csr_seek(pCur->pLsmCur, pVal, nVal, LSM_SEEK_EQ);
    }
  }else{
    rc = lsm_csr_first(pCur->pLsmCur);
    pCur->isDesc = 0;
    pCur->bUnique = 0;
  }
  if( rc==LSM_OK && lsm_csr_valid(pCur->pLsmCur)!=0 ){
    pCur->atEof = 0;
  }
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** Only comparisons against the key are allowed.  The idxNum defines
** which comparisons are available:
**
**     0        Full table scan only
**   bit 1      key==?1  single argument for ?1
**   bit 2      key>?1
**   bit 3      key>=?1
**   bit 4      key<?N   (N==1 if bits 2,3 clear, or 2 if bits2,3 set)
**   bit 5      key<=?N  (N==1 if bits 2,3 clear, or 2 if bits2,3 set)
**   bit 6      Use blobkey instead of key
**
** To put it another way:
**
**     0        Full table scan.
**     1        key==?1
**     2        key>?1
**     4        key>=?1
**     8        key<?1
**     10       key>?1 AND key<?2
**     12       key>=?1 AND key<?2
**     16       key<=?1
**     18       key>?1 AND key<=?2
**     20       key>=?1 AND key<=?2
**     33..52   Use blobkey in place of key...
*/
static int lsm1BestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  int i;                 /* Loop over constraints */
  int idxNum = 0;        /* The query plan bitmask */
  int nArg = 0;          /* Number of arguments to xFilter */
  int eqIdx = -1;        /* Index of the key== constraint, or -1 if none */

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint && idxNum<16; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( pConstraint->iColumn!=LSM1_COLUMN_KEY ) continue;
    if( pConstraint->op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    switch( pConstraint->op ){
      case SQLITE_INDEX_CONSTRAINT_EQ: {
        eqIdx = i;
        idxNum = 1;
        break;
      }
    }
  }
  if( eqIdx>=0 ){
    pIdxInfo->aConstraintUsage[eqIdx].argvIndex = ++nArg;
    pIdxInfo->aConstraintUsage[eqIdx].omit = 1;
  }
  if( idxNum==1 ){
    pIdxInfo->estimatedCost = (double)1;
    pIdxInfo->estimatedRows = 1;
    pIdxInfo->orderByConsumed = 1;
  }else{
    /* Full table scan */
    pIdxInfo->estimatedCost = (double)2147483647;
    pIdxInfo->estimatedRows = 2147483647;
  }
  pIdxInfo->idxNum = idxNum;
  return SQLITE_OK;
}

/*
** The xUpdate method is normally used for INSERT, REPLACE, UPDATE, and
** DELETE.  But this virtual table only supports INSERT and REPLACE.
** DELETE is accomplished by inserting a record with a value of NULL.
** UPDATE is achieved by using REPLACE.
*/
int lsm1Update(
  sqlite3_vtab *pVTab,
  int argc,
  sqlite3_value **argv,
  sqlite_int64 *pRowid
){
  lsm1_vtab *p = (lsm1_vtab*)pVTab;
  const void *pKey;
  void *pFree = 0;
  int nKey;
  int eType;
  int rc = LSM_OK;
  sqlite3_value *pValue;
  const unsigned char *pVal;
  unsigned char *pData;
  int nVal;
  unsigned char pSpace[100];

  if( argc==1 ){
    pVTab->zErrMsg = sqlite3_mprintf("cannot DELETE");
    return SQLITE_ERROR;
  }
  if( sqlite3_value_type(argv[0])!=SQLITE_NULL ){
    pVTab->zErrMsg = sqlite3_mprintf("cannot UPDATE");
    return SQLITE_ERROR;
  }

  /* "INSERT INTO tab(command) VALUES('....')" is used to implement
  ** special commands.
  */
  if( sqlite3_value_type(argv[2+LSM1_COLUMN_COMMAND])!=SQLITE_NULL ){
    return SQLITE_OK;
  }
  if( sqlite3_value_type(argv[2+LSM1_COLUMN_BLOBKEY])==SQLITE_BLOB ){
    /* Use the blob key exactly as supplied */
    pKey = sqlite3_value_blob(argv[2+LSM1_COLUMN_BLOBKEY]);
    nKey = sqlite3_value_bytes(argv[2+LSM1_COLUMN_BLOBKEY]);
  }else{
    /* Use a key encoding that sorts in lexicographical order */
    rc = lsm1EncodeKey(argv[2+LSM1_COLUMN_KEY],
                       (unsigned char**)&pKey,&nKey,
                       pSpace,sizeof(pSpace));
    if( rc ) return rc;
    if( pKey!=(const void*)pSpace ) pFree = (void*)pKey;
  }
  if( sqlite3_value_type(argv[2+LSM1_COLUMN_BLOBVALUE])==SQLITE_BLOB ){
    pVal = sqlite3_value_blob(argv[2+LSM1_COLUMN_BLOBVALUE]);
    nVal = sqlite3_value_bytes(argv[2+LSM1_COLUMN_BLOBVALUE]);
    rc = lsm_insert(p->pDb, pKey, nKey, pVal, nVal);
  }else{
    pValue = argv[2+LSM1_COLUMN_VALUE];
    eType = sqlite3_value_type(pValue);
    switch( eType ){
      case SQLITE_NULL: {
        rc = lsm_delete(p->pDb, pKey, nKey);
        break;
      }
      case SQLITE_BLOB:
      case SQLITE_TEXT: {
        if( eType==SQLITE_TEXT ){
          pVal = sqlite3_value_text(pValue);
        }else{
          pVal = (unsigned char*)sqlite3_value_blob(pValue);
        }
        nVal = sqlite3_value_bytes(pValue);
        pData = sqlite3_malloc( nVal+1 );
        if( pData==0 ){
          rc = SQLITE_NOMEM;
        }else{
          pData[0] = (unsigned char)eType;
          memcpy(&pData[1], pVal, nVal);
          rc = lsm_insert(p->pDb, pKey, nKey, pData, nVal+1);
          sqlite3_free(pData);
        }
        break;
      }
      case SQLITE_INTEGER:
      case SQLITE_FLOAT: {
        sqlite3_uint64 x;
        unsigned char aVal[9];
        int i;
        if( eType==SQLITE_INTEGER ){
          *(sqlite3_int64*)&x = sqlite3_value_int64(pValue);
        }else{
          double r = sqlite3_value_double(pValue);
          assert( sizeof(r)==sizeof(x) );
          memcpy(&x, &r, sizeof(r));
        }
        for(i=8; x>0 && i>=1; i--){
          aVal[i] = x & 0xff;
          x >>= 8;
        }
        aVal[i] = (unsigned char)eType;
        rc = lsm_insert(p->pDb, pKey, nKey, &aVal[i], 9-i);
        break;
      }
    }
  }
  sqlite3_free(pFree);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}      

/* Begin a transaction
*/
static int lsm1Begin(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_begin(p->pDb, 1);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/* Phase 1 of a transaction commit.
*/
static int lsm1Sync(sqlite3_vtab *pVtab){
  return SQLITE_OK;
}

/* Commit a transaction
*/
static int lsm1Commit(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_commit(p->pDb, 0);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/* Rollback a transaction
*/
static int lsm1Rollback(sqlite3_vtab *pVtab){
  lsm1_vtab *p = (lsm1_vtab*)pVtab;
  int rc = lsm_rollback(p->pDb, 0);
  return rc==LSM_OK ? SQLITE_OK : SQLITE_ERROR;
}

/*
** This following structure defines all the methods for the 
** generate_lsm1 virtual table.
*/
static sqlite3_module lsm1Module = {
  0,                       /* iVersion */
  lsm1Connect,             /* xCreate */
  lsm1Connect,             /* xConnect */
  lsm1BestIndex,           /* xBestIndex */
  lsm1Disconnect,          /* xDisconnect */
  lsm1Disconnect,          /* xDestroy */
  lsm1Open,                /* xOpen - open a cursor */
  lsm1Close,               /* xClose - close a cursor */
  lsm1Filter,              /* xFilter - configure scan constraints */
  lsm1Next,                /* xNext - advance a cursor */
  lsm1Eof,                 /* xEof - check for end of scan */
  lsm1Column,              /* xColumn - read data */
  lsm1Rowid,               /* xRowid - read data */
  lsm1Update,              /* xUpdate */
  lsm1Begin,               /* xBegin */
  lsm1Sync,                /* xSync */
  lsm1Commit,              /* xCommit */
  lsm1Rollback,            /* xRollback */
  0,                       /* xFindMethod */
  0,                       /* xRename */
};


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_lsm_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "lsm1", &lsm1Module, 0);
  return rc;
}
