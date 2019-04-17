/*
** 2019-04-17
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file contains an implementation of the eponymous "sqlite_dbdata"
** virtual table. sqlite_dbdata is used to extract data directly from a
** database b-tree page and its associated overflow pages, bypassing the b-tree
** layer. The table schema is equivalent to:
**
**     CREATE TABLE sqlite_dbdata(
**       pgno INTEGER,
**       cell INTEGER,
**       field INTEGER,
**       value ANY,
**       schema TEXT HIDDEN
**     );
**
** Each page of the database is inspected. If it cannot be interpreted as a
** b-tree page, or if it is a b-tree page containing 0 entries, the
** sqlite_dbdata table contains no rows for that page.  Otherwise, the table
** contains one row for each field in the record associated with each
** cell on the page. For intkey b-trees, the key value is stored in field -1.
**
** For example, for the database:
**
**     CREATE TABLE t1(a, b);     -- root page is page 2
**     INSERT INTO t1(rowid, a, b) VALUES(5, 'v', 'five');
**     INSERT INTO t1(rowid, a, b) VALUES(10, 'x', 'ten');
**
** the sqlite_dbdata table contains, as well as from entries related to 
** page 1, content equivalent to:
**
**     INSERT INTO sqlite_dbdata(pgno, cell, field, value) VALUES
**         (2, 0, -1, 5     ),
**         (2, 0,  0, 'v'   ),
**         (2, 0,  1, 'five'),
**         (2, 1, -1, 10    ),
**         (2, 1,  0, 'x'   ),
**         (2, 1,  1, 'ten' );
**
** If database corruption is encountered, this module does not report an
** error. Instead, it attempts to extract as much data as possible and
** ignores the corruption.
**
** This module requires that the "sqlite_dbpage" eponymous virtual table be
** available.
*/
#if !defined(SQLITEINT_H)
#include "sqlite3ext.h"

typedef unsigned char u8;
typedef unsigned int u32;

#endif
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <assert.h>

typedef struct DbdataTable DbdataTable;
typedef struct DbdataCursor DbdataCursor;

/* A cursor for the sqlite_dbdata table */
struct DbdataCursor {
  sqlite3_vtab_cursor base;       /* Base class.  Must be first */
  sqlite3_stmt *pStmt;            /* For fetching database pages */

  int iPgno;                      /* Current page number */
  u8 *aPage;                      /* Buffer containing page */
  int nPage;                      /* Size of aPage[] in bytes */
  int nCell;                      /* Number of cells on aPage[] */
  int iCell;                      /* Current cell number */
  u8 *pRec;                       /* Buffer containing current record */
  int nRec;                       /* Size of pRec[] in bytes */
  int nField;                     /* Number of fields in pRec */
  int iField;                     /* Current field number */
  sqlite3_int64 iIntkey;          /* Integer key value */

  sqlite3_int64 iRowid;
};

/* The sqlite_dbdata table */
struct DbdataTable {
  sqlite3_vtab base;              /* Base class.  Must be first */
  sqlite3 *db;                    /* The database connection */
};

#define DBDATA_COLUMN_PGNO        0
#define DBDATA_COLUMN_CELL        1
#define DBDATA_COLUMN_FIELD       2
#define DBDATA_COLUMN_VALUE       3
#define DBDATA_COLUMN_SCHEMA      4

#define DBDATA_SCHEMA             \
      "CREATE TABLE x("           \
      "  pgno INTEGER,"           \
      "  cell INTEGER,"           \
      "  field INTEGER,"          \
      "  value ANY,"              \
      "  schema TEXT HIDDEN"      \
      ")"

/*
** Connect to the sqlite_dbdata virtual table.
*/
static int dbdataConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  DbdataTable *pTab = 0;
  int rc = sqlite3_declare_vtab(db, DBDATA_SCHEMA);

  if( rc==SQLITE_OK ){
    pTab = (DbdataTable*)sqlite3_malloc64(sizeof(DbdataTable));
    if( pTab==0 ){
      rc = SQLITE_NOMEM;
    }else{
      memset(pTab, 0, sizeof(DbdataTable));
      pTab->db = db;
    }
  }

  *ppVtab = (sqlite3_vtab*)pTab;
  return rc;
}

/*
** Disconnect from or destroy a dbdata virtual table.
*/
static int dbdataDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

/*
**
** This function interprets two types of constraints:
**
**       schema=?
**       pgno=?
**
** If neither are present, idxNum is set to 0. If schema=? is present,
** the 0x01 bit in idxNum is set. If pgno=? is present, the 0x02 bit
** in idxNum is set.
**
** If both parameters are present, schema is in position 0 and pgno in
** position 1.
*/
static int dbdataBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  int i;
  int iSchema = -1;
  int iPgno = -1;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      if( p->iColumn==DBDATA_COLUMN_SCHEMA ){
        if( p->usable==0 ) return SQLITE_CONSTRAINT;
        iSchema = i;
      }
      if( p->iColumn==DBDATA_COLUMN_PGNO && p->usable ){
        iPgno = i;
      }
    }
  }

  if( iSchema>=0 ){
    pIdxInfo->aConstraintUsage[iSchema].argvIndex = 1;
    pIdxInfo->aConstraintUsage[iSchema].omit = 1;
  }
  if( iPgno>=0 ){
    pIdxInfo->aConstraintUsage[iPgno].argvIndex = 1 + (iSchema>=0);
    pIdxInfo->aConstraintUsage[iPgno].omit = 1;
  }
  pIdxInfo->idxNum = (iSchema>=0 ? 0x01 : 0x00) | (iPgno>=0 ? 0x02 : 0x00);
  return SQLITE_OK;
}

/*
** Open a new dbdata cursor.
*/
static int dbdataOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  DbdataCursor *pCsr;

  pCsr = (DbdataCursor*)sqlite3_malloc64(sizeof(DbdataCursor));
  if( pCsr==0 ){
    return SQLITE_NOMEM;
  }else{
    memset(pCsr, 0, sizeof(DbdataCursor));
    pCsr->base.pVtab = pVTab;
  }

  *ppCursor = (sqlite3_vtab_cursor *)pCsr;
  return SQLITE_OK;
}

static void dbdataResetCursor(DbdataCursor *pCsr){
  sqlite3_finalize(pCsr->pStmt);
  pCsr->pStmt = 0;
  pCsr->iPgno = 1;
  pCsr->iCell = 0;
  pCsr->iField = 0;
}

/*
** Close a dbdata cursor.
*/
static int dbdataClose(sqlite3_vtab_cursor *pCursor){
  DbdataCursor *pCsr = (DbdataCursor*)pCursor;
  dbdataResetCursor(pCsr);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}


/* Decode big-endian integers */
static unsigned int get_uint16(unsigned char *a){
  return (a[0]<<8)|a[1];
}
static unsigned int get_uint32(unsigned char *a){
  return (a[0]<<24)|(a[1]<<16)|(a[2]<<8)|a[3];
}

static int dbdataLoadPage(
  DbdataCursor *pCsr, 
  u32 pgno,
  u8 **ppPage,
  int *pnPage
){
  int rc2;
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt = pCsr->pStmt;

  *ppPage = 0;
  *pnPage = 0;
  sqlite3_bind_int64(pStmt, 2, pgno);
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    int nCopy = sqlite3_column_bytes(pStmt, 0);
    u8 *pPage = (u8*)sqlite3_malloc64(nCopy);
    if( pPage==0 ){
      rc = SQLITE_NOMEM;
    }else{
      const u8 *pCopy = sqlite3_column_blob(pStmt, 0);
      memcpy(pPage, pCopy, nCopy);
      *ppPage = pPage;
      *pnPage = nCopy;
    }
  }
  rc2 = sqlite3_reset(pStmt);
  if( *ppPage==0 ) rc = rc2;

  return rc;
}

/*
** Read a varint.  Put the value in *pVal and return the number of bytes.
*/
static int dbdataGetVarint(const u8 *z, sqlite3_int64 *pVal){
  sqlite3_int64 v = 0;
  int i;
  for(i=0; i<8; i++){
    v = (v<<7) + (z[i]&0x7f);
    if( (z[i]&0x80)==0 ){ *pVal = v; return i+1; }
  }
  v = (v<<8) + (z[i]&0xff);
  *pVal = v;
  return 9;
}

/*
** Move a dbdata cursor to the next entry in the file.
*/
static int dbdataNext(sqlite3_vtab_cursor *pCursor){
  DbdataCursor *pCsr = (DbdataCursor *)pCursor;

  pCsr->iRowid++;
  while( 1 ){
    int rc;

    if( pCsr->aPage==0 ){
      rc = dbdataLoadPage(pCsr, pCsr->iPgno, &pCsr->aPage, &pCsr->nPage);
      if( rc!=SQLITE_OK ) return rc;
      pCsr->iCell = 0;
      pCsr->nCell = get_uint16(&pCsr->aPage[pCsr->iPgno==1 ? 103 : 3]);
    }

    /* If there is no record loaded, load it now. */
    if( pCsr->pRec==0 ){
      int iOff = (pCsr->iPgno==1 ? 100 : 0);
      int bHasRowid = 0;
      int nPointer = 0;
      sqlite3_int64 nPayload = 0;
      sqlite3_int64 nHdr = 0;
      int iHdr;
      int U, X;
      int nLocal;

      switch( pCsr->aPage[iOff] ){
        case 0x02:
          nPointer = 4;
          break;
        case 0x0a:
          break;
        case 0x0d:
          bHasRowid = 1;
          break;
        default:
          pCsr->iCell = pCsr->nCell;
          break;
      }
      if( pCsr->iCell>=pCsr->nCell ){
        sqlite3_free(pCsr->aPage);
        pCsr->aPage = 0;
        return SQLITE_OK;
      }

      iOff += 8 + nPointer + pCsr->iCell*2;
      iOff = get_uint16(&pCsr->aPage[iOff]);

      /* For an interior node cell, skip past the child-page number */
      iOff += nPointer;

      /* Load the "byte of payload including overflow" field */
      iOff += dbdataGetVarint(&pCsr->aPage[iOff], &nPayload);

      /* If this is a leaf intkey cell, load the rowid */
      if( bHasRowid ){
        iOff += dbdataGetVarint(&pCsr->aPage[iOff], &pCsr->iIntkey);
      }

      /* Allocate space for payload */
      pCsr->pRec = (u8*)sqlite3_malloc64(nPayload);
      if( pCsr->pRec==0 ) return SQLITE_NOMEM;
      pCsr->nRec = nPayload;

      U = pCsr->nPage;
      if( bHasRowid ){
        X = U-35;
      }else{
        X = ((U-12)*64/255)-23;
      }
      if( nPayload<=X ){
        nLocal = nPayload;
      }else{
        int M, K;
        M = ((U-12)*32/255)-23;
        K = M+((nPayload-M)%(U-4));
        if( K<=X ){
          nLocal = K;
        }else{
          nLocal = M;
        }
      }

      /* Load the nLocal bytes of payload */
      memcpy(pCsr->pRec, &pCsr->aPage[iOff], nLocal);
      iOff += nLocal;

      /* Load content from overflow pages */
      if( nPayload>nLocal ){
        sqlite3_int64 nRem = nPayload - nLocal;
        u32 pgnoOvfl = get_uint32(&pCsr->aPage[iOff]);
        while( nRem>0 ){
          u8 *aOvfl = 0;
          int nOvfl = 0;
          int nCopy;
          rc = dbdataLoadPage(pCsr, pgnoOvfl, &aOvfl, &nOvfl);
          assert( rc!=SQLITE_OK || nOvfl==pCsr->nPage );
          if( rc!=SQLITE_OK ) return rc;

          nCopy = U-4;
          if( nCopy>nRem ) nCopy = nRem;
          memcpy(&pCsr->pRec[nPayload-nRem], &aOvfl[4], nCopy);
          nRem -= nCopy;

          sqlite3_free(aOvfl);
        }
      }

      /* Figure out how many fields in the record */
      pCsr->nField = 0;
      iHdr = dbdataGetVarint(pCsr->pRec, &nHdr);
      while( iHdr<nHdr ){
        sqlite3_int64 iDummy;
        iHdr += dbdataGetVarint(&pCsr->pRec[iHdr], &iDummy);
        pCsr->nField++;
      }

      pCsr->iField = (bHasRowid ? -2 : -1);
    }

    pCsr->iField++;
    if( pCsr->iField<pCsr->nField ) return SQLITE_OK;

    /* Advance to the next cell. The next iteration of the loop will load
    ** the record and so on. */
    sqlite3_free(pCsr->pRec);
    pCsr->pRec = 0;
    pCsr->iCell++;
  }

  assert( !"can't get here" );
  return SQLITE_OK;
}

/* We have reached EOF if previous sqlite3_step() returned
** anything other than SQLITE_ROW;
*/
static int dbdataEof(sqlite3_vtab_cursor *pCursor){
  DbdataCursor *pCsr = (DbdataCursor*)pCursor;
  return pCsr->aPage==0;
}

/* Position a cursor back to the beginning.
*/
static int dbdataFilter(
  sqlite3_vtab_cursor *pCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  DbdataCursor *pCsr = (DbdataCursor*)pCursor;
  DbdataTable *pTab = (DbdataTable*)pCursor->pVtab;
  int rc;
  const char *zSchema = "main";

  dbdataResetCursor(pCsr);
  assert( pCsr->iPgno==1 );
  if( idxNum & 0x01 ){
    zSchema = sqlite3_value_text(argv[0]);
  }
  if( idxNum & 0x02 ){
    pCsr->iPgno = sqlite3_value_int(argv[(idxNum & 0x01)]);
  }

  rc = sqlite3_prepare_v2(pTab->db, 
      "SELECT data FROM sqlite_dbpage(?) WHERE pgno=?", -1,
      &pCsr->pStmt, 0
  );
  if( rc==SQLITE_OK ){
    rc = sqlite3_bind_text(pCsr->pStmt, 1, zSchema, -1, SQLITE_TRANSIENT);
  }
  if( rc==SQLITE_OK ){
    rc = dbdataNext(pCursor);
  }
  return rc;
}

static int dbdataValueBytes(int eType){
  switch( eType ){
    case 0: case 8: case 9:
    case 10: case 11:
      return 0;
    case 1:
      return 1;
    case 2:
      return 2;
    case 3:
      return 3;
    case 4:
      return 4;
    case 5:
      return 6;
    case 6:
    case 7:
      return 8;
    default:
      return ((eType-12) / 2);
  }
}

static void dbdataValue(sqlite3_context *pCtx, int eType, u8 *pData){
  switch( eType ){
    case 0: 
    case 10: 
    case 11: 
      sqlite3_result_null(pCtx);
      break;
    
    case 8: 
      sqlite3_result_int(pCtx, 0);
      break;
    case 9:
      sqlite3_result_int(pCtx, 1);
      break;

    case 1: case 2: case 3: case 4: case 5: case 6: case 7: {
      sqlite3_uint64 v = (signed char)pData[0];
      pData++;
      switch( eType ){
        case 7:
        case 6:  v = (v<<16) + (pData[0]<<8) + pData[1];  pData += 2;
        case 5:  v = (v<<16) + (pData[0]<<8) + pData[1];  pData += 2;
        case 4:  v = (v<<8) + pData[0];  pData++;
        case 3:  v = (v<<8) + pData[0];  pData++;
        case 2:  v = (v<<8) + pData[0];  pData++;
      }

      if( eType==7 ){
        double r;
        memcpy(&r, &v, sizeof(r));
        sqlite3_result_double(pCtx, r);
      }else{
        sqlite3_result_int64(pCtx, (sqlite3_int64)v);
      }
      break;
    }

    default: {
      int n = ((eType-12) / 2);
      if( eType % 2 ){
        sqlite3_result_text(pCtx, pData, n, SQLITE_TRANSIENT);
      }else{
        sqlite3_result_blob(pCtx, pData, n, SQLITE_TRANSIENT);
      }
    }
  }
}

/* Return a column for the sqlite_dbdata table */
static int dbdataColumn(
  sqlite3_vtab_cursor *pCursor, 
  sqlite3_context *ctx, 
  int i
){
  DbdataCursor *pCsr = (DbdataCursor*)pCursor;
  switch( i ){
    case DBDATA_COLUMN_PGNO:
      sqlite3_result_int64(ctx, pCsr->iPgno);
      break;
    case DBDATA_COLUMN_CELL:
      sqlite3_result_int(ctx, pCsr->iCell);
      break;
    case DBDATA_COLUMN_FIELD:
      sqlite3_result_int(ctx, pCsr->iField);
      break;
    case DBDATA_COLUMN_VALUE: {
      if( pCsr->iField<0 ){
        sqlite3_result_int64(ctx, pCsr->iIntkey);
      }else{
        int iHdr;
        sqlite3_int64 iType;
        sqlite3_int64 iOff;
        int i;
        iHdr = dbdataGetVarint(pCsr->pRec, &iOff);
        for(i=0; i<pCsr->iField; i++){
          iHdr += dbdataGetVarint(&pCsr->pRec[iHdr], &iType);
          iOff += dbdataValueBytes(iType);
        }
        dbdataGetVarint(&pCsr->pRec[iHdr], &iType);

        dbdataValue(ctx, iType, &pCsr->pRec[iOff]);
      }
      break;
    }
  }
  return SQLITE_OK;
}

/* Return the ROWID for the sqlite_dbdata table */
static int dbdataRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  DbdataCursor *pCsr = (DbdataCursor*)pCursor;
  *pRowid = pCsr->iRowid;
  return SQLITE_OK;
}


/*
** Invoke this routine to register the "sqlite_dbdata" virtual table module
*/
static int sqlite3DbdataRegister(sqlite3 *db){
  static sqlite3_module dbdata_module = {
    0,                            /* iVersion */
    0,                            /* xCreate */
    dbdataConnect,                /* xConnect */
    dbdataBestIndex,              /* xBestIndex */
    dbdataDisconnect,             /* xDisconnect */
    0,                            /* xDestroy */
    dbdataOpen,                   /* xOpen - open a cursor */
    dbdataClose,                  /* xClose - close a cursor */
    dbdataFilter,                 /* xFilter - configure scan constraints */
    dbdataNext,                   /* xNext - advance a cursor */
    dbdataEof,                    /* xEof - check for end of scan */
    dbdataColumn,                 /* xColumn - read data */
    dbdataRowid,                  /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0,                            /* xRollbackTo */
    0                             /* xShadowName */
  };
  return sqlite3_create_module(db, "sqlite_dbdata", &dbdata_module, 0);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_dbdata_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi);
  return sqlite3DbdataRegister(db);
}
