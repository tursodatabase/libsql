/*
** 2017 July 15
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
** This file contains the implementation of the "unionvtab" virtual
** table. This module provides read-only access to multiple tables, 
** possibly in multiple database files, via a single database object.
** The source tables must have the following characteristics:
**
**   * They must all be rowid tables (not VIRTUAL or WITHOUT ROWID
**     tables or views).
**
**   * Each table must have the same set of columns, declared in
**     the same order and with the same declared types.
**
**   * The tables must not feature a user-defined column named "_rowid_".
**
**   * Each table must contain a distinct range of rowid values.
**
** A "unionvtab" virtual table is created as follows:
**
**   CREATE VIRTUAL TABLE <name> USING unionvtab(<sql statement>);
**
** The implementation evalutes <sql statement> whenever a unionvtab virtual
** table is created or opened. It should return one row for each source
** database table. The four columns required of each row are:
**
**   1. The name of the database containing the table ("main" or "temp" or
**      the name of an attached database). Or NULL to indicate that all
**      databases should be searched for the table in the usual fashion.
**
**   2. The name of the database table.
**
**   3. The smallest rowid in the range of rowids that may be stored in the
**      database table (an integer).
**
**   4. The largest rowid in the range of rowids that may be stored in the
**      database table (an integer).
**
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

/*
** Largest and smallest possible 64-bit signed integers. These macros
** copied from sqliteInt.h.
*/
#ifndef LARGEST_INT64
# define LARGEST_INT64  (0xffffffff|(((sqlite3_int64)0x7fffffff)<<32))
#endif
#ifndef SMALLEST_INT64
# define SMALLEST_INT64 (((sqlite3_int64)-1) - LARGEST_INT64)
#endif

#define SWARMVTAB_MAX_ATTACHED 9

typedef struct UnionCsr UnionCsr;
typedef struct UnionTab UnionTab;
typedef struct UnionSrc UnionSrc;

/*
** Each source table (row returned by the initialization query) is 
** represented by an instance of the following structure stored in the
** UnionTab.aSrc[] array.
*/
struct UnionSrc {
  char *zDb;                      /* Database containing source table */
  char *zTab;                     /* Source table name */
  sqlite3_int64 iMin;             /* Minimum rowid */
  sqlite3_int64 iMax;             /* Maximum rowid */

  /* Fields used by swarmvtab only */
  char *zFile;                    /* File to ATTACH */
  int bAttached;                  /* True if currently attached */
  UnionSrc *pNextAttached;        /* Next in list of all attached sources */
};

/*
** Virtual table  type for union vtab.
*/
struct UnionTab {
  sqlite3_vtab base;              /* Base class - must be first */
  sqlite3 *db;                    /* Database handle */
  int bSwarm;                     /* 1 for "swarmvtab", 0 for "unionvtab" */
  int iPK;                        /* INTEGER PRIMARY KEY column, or -1 */
  int nSrc;                       /* Number of elements in the aSrc[] array */
  sqlite3_stmt *pSourceStr;       /* Used by unionSourceToStr() */
  UnionSrc *aSrc;                 /* Array of source tables, sorted by rowid */

  /* Used by swarmvtab only */
  char *zSourceStr;               /* Expected unionSourceToStr() value */
  UnionSrc *pAttached;            /* First in list of attached sources */
  int nAttach;                    /* Current number of attached sources */
  int nMaxAttach;                 /* Maximum number of attached sources */
};

/*
** Virtual table cursor type for union vtab.
*/
struct UnionCsr {
  sqlite3_vtab_cursor base;       /* Base class - must be first */
  sqlite3_stmt *pStmt;            /* SQL statement to run */

  /* Used by swarmvtab only */
  sqlite3_int64 iMaxRowid;        /* Last rowid to visit */
  int iTab;                       /* Index of table read by pStmt */
};

/*
** If *pRc is other than SQLITE_OK when this function is called, it
** always returns NULL. Otherwise, it attempts to allocate and return
** a pointer to nByte bytes of zeroed memory. If the memory allocation
** is attempted but fails, NULL is returned and *pRc is set to 
** SQLITE_NOMEM.
*/
static void *unionMalloc(int *pRc, int nByte){
  void *pRet;
  assert( nByte>0 );
  if( *pRc==SQLITE_OK ){
    pRet = sqlite3_malloc(nByte);
    if( pRet ){
      memset(pRet, 0, nByte);
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }else{
    pRet = 0;
  }
  return pRet;
}

/*
** If *pRc is other than SQLITE_OK when this function is called, it
** always returns NULL. Otherwise, it attempts to allocate and return
** a copy of the nul-terminated string passed as the second argument.
** If the allocation is attempted but fails, NULL is returned and *pRc is 
** set to SQLITE_NOMEM.
*/
static char *unionStrdup(int *pRc, const char *zIn){
  char *zRet = 0;
  if( zIn ){
    int nByte = (int)strlen(zIn) + 1;
    zRet = unionMalloc(pRc, nByte);
    if( zRet ){
      memcpy(zRet, zIn, nByte);
    }
  }
  return zRet;
}

/*
** If the first character of the string passed as the only argument to this
** function is one of the 4 that may be used as an open quote character
** in SQL, this function assumes that the input is a well-formed quoted SQL 
** string. In this case the string is dequoted in place.
**
** If the first character of the input is not an open quote, then this
** function is a no-op.
*/
static void unionDequote(char *z){
  if( z ){
    char q = z[0];

    /* Set stack variable q to the close-quote character */
    if( q=='[' || q=='\'' || q=='"' || q=='`' ){
      int iIn = 1;
      int iOut = 0;
      if( q=='[' ) q = ']';  
      while( z[iIn] ){
        if( z[iIn]==q ){
          if( z[iIn+1]!=q ){
            /* Character iIn was the close quote. */
            iIn++;
            break;
          }else{
            /* Character iIn and iIn+1 form an escaped quote character. Skip
            ** the input cursor past both and copy a single quote character 
            ** to the output buffer. */
            iIn += 2;
            z[iOut++] = q;
          }
        }else{
          z[iOut++] = z[iIn++];
        }
      }
      z[iOut] = '\0';
    }
  }
}

/*
** This function is a no-op if *pRc is set to other than SQLITE_OK when it
** is called. NULL is returned in this case.
**
** Otherwise, the SQL statement passed as the third argument is prepared
** against the database handle passed as the second. If the statement is
** successfully prepared, a pointer to the new statement handle is 
** returned. It is the responsibility of the caller to eventually free the
** statement by calling sqlite3_finalize(). Alternatively, if statement
** compilation fails, NULL is returned, *pRc is set to an SQLite error
** code and *pzErr may be set to an error message buffer allocated by
** sqlite3_malloc().
*/
static sqlite3_stmt *unionPrepare(
  int *pRc,                       /* IN/OUT: Error code */
  sqlite3 *db,                    /* Database handle */
  const char *zSql,               /* SQL statement to prepare */
  char **pzErr                    /* OUT: Error message */
){
  sqlite3_stmt *pRet = 0;
  if( *pRc==SQLITE_OK ){
    int rc = sqlite3_prepare_v2(db, zSql, -1, &pRet, 0);
    if( rc!=SQLITE_OK && pzErr ){
      *pzErr = sqlite3_mprintf("sql error: %s", sqlite3_errmsg(db));
      *pRc = rc;
    }
  }
  return pRet;
}

/*
** Like unionPrepare(), except prepare the results of vprintf(zFmt, ...)
** instead of a constant SQL string.
*/
static sqlite3_stmt *unionPreparePrintf(
  int *pRc,                       /* IN/OUT: Error code */
  char **pzErr,                   /* OUT: Error message */
  sqlite3 *db,                    /* Database handle */
  const char *zFmt,               /* printf() format string */
  ...                             /* Trailing printf args */
){
  sqlite3_stmt *pRet = 0;
  char *zSql;
  va_list ap;
  va_start(ap, zFmt);

  zSql = sqlite3_vmprintf(zFmt, ap);
  if( *pRc==SQLITE_OK ){
    if( zSql==0 ){
      *pRc = SQLITE_NOMEM;
    }else{
      pRet = unionPrepare(pRc, db, zSql, pzErr);
    }
  }
  sqlite3_free(zSql);

  va_end(ap);
  return pRet;
}


/*
** Call sqlite3_reset() on SQL statement pStmt. If *pRc is set to 
** SQLITE_OK when this function is called, then it is set to the
** value returned by sqlite3_reset() before this function exits.
** In this case, *pzErr may be set to point to an error message
** buffer allocated by sqlite3_malloc().
*/
static void unionReset(int *pRc, sqlite3_stmt *pStmt, char **pzErr){
  int rc = sqlite3_reset(pStmt);
  if( *pRc==SQLITE_OK ){
    *pRc = rc;
    if( rc ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(sqlite3_db_handle(pStmt)));
    }
  }
}

/*
** Call sqlite3_finalize() on SQL statement pStmt. If *pRc is set to 
** SQLITE_OK when this function is called, then it is set to the
** value returned by sqlite3_finalize() before this function exits.
*/
static void unionFinalize(int *pRc, sqlite3_stmt *pStmt){
  int rc = sqlite3_finalize(pStmt);
  if( *pRc==SQLITE_OK ) *pRc = rc;
}

static int unionDetachDatabase(UnionTab *pTab, char **pzErr){
  sqlite3_stmt *pStmt = 0;
  int rc = SQLITE_OK;
  UnionSrc **pp;
  assert( pTab->pAttached );

  pp = &pTab->pAttached;
  while( (*pp)->pNextAttached ){
    pp = &(*pp)->pNextAttached;
  }

  pStmt = unionPreparePrintf(&rc, pzErr, pTab->db, "DETACH %s", (*pp)->zDb);
  if( rc==SQLITE_OK ) sqlite3_step(pStmt);
  unionFinalize(&rc, pStmt);
  assert( rc==SQLITE_OK );
  if( rc==SQLITE_OK ){
    assert( (*pp)->bAttached && (*pp)->pNextAttached==0 );
    (*pp)->bAttached = 0;
    *pp = 0;
    pTab->nAttach--;
  }

  return rc;
}

/*
** xDisconnect method.
*/
static int unionDisconnect(sqlite3_vtab *pVtab){
  if( pVtab ){
    UnionTab *pTab = (UnionTab*)pVtab;
    int i;
    for(i=0; i<pTab->nSrc; i++){
      sqlite3_free(pTab->aSrc[i].zDb);
      sqlite3_free(pTab->aSrc[i].zTab);
      sqlite3_free(pTab->aSrc[i].zFile);
    }
    while( pTab->pAttached ){
      if( unionDetachDatabase(pTab, 0)!=SQLITE_OK ) break;
    }
    sqlite3_finalize(pTab->pSourceStr);
    sqlite3_free(pTab->zSourceStr);
    sqlite3_free(pTab->aSrc);
    sqlite3_free(pTab);
  }
  return SQLITE_OK;
}

/*
** Check that the table identified by pSrc is a rowid table. If not,
** return SQLITE_ERROR and set (*pzErr) to point to an English language
** error message. If the table is a rowid table and no error occurs,
** return SQLITE_OK and leave (*pzErr) unmodified.
*/
static int unionIsIntkeyTable(
  sqlite3 *db,                    /* Database handle */
  UnionSrc *pSrc,                 /* Source table to test */
  char **pzErr                    /* OUT: Error message */
){
  int bPk = 0;
  const char *zType = 0;
  int rc;

  sqlite3_table_column_metadata(
      db, pSrc->zDb, pSrc->zTab, "_rowid_", &zType, 0, 0, &bPk, 0
  );
  rc = sqlite3_errcode(db);
  if( rc==SQLITE_ERROR 
   || (rc==SQLITE_OK && (!bPk || sqlite3_stricmp("integer", zType)))
  ){
    rc = SQLITE_ERROR;
    *pzErr = sqlite3_mprintf("no such rowid table: %s%s%s",
        (pSrc->zDb ? pSrc->zDb : ""),
        (pSrc->zDb ? "." : ""),
        pSrc->zTab
    );
  }
  return rc;
}

/*
** This function is a no-op if *pRc is other than SQLITE_OK when it is
** called. In this case it returns NULL.
**
** Otherwise, this function checks that the source table passed as the
** second argument (a) exists, (b) is not a view and (c) has a column 
** named "_rowid_" of type "integer" that is the primary key.
** If this is not the case, *pRc is set to SQLITE_ERROR and NULL is
** returned.
**
** Finally, if the source table passes the checks above, a nul-terminated
** string describing the column names and types belonging to the source
** table is returned. Tables with the same set of column names and types 
** cause this function to return identical strings. Is is the responsibility
** of the caller to free the returned string using sqlite3_free() when
** it is no longer required.
*/
static char *unionSourceToStr(
  int *pRc,                       /* IN/OUT: Error code */
  UnionTab *pTab,                 /* Virtual table object */
  UnionSrc *pSrc,                 /* Source table to test */
  char **pzErr                    /* OUT: Error message */
){
  char *zRet = 0;
  if( *pRc==SQLITE_OK ){
    int rc = unionIsIntkeyTable(pTab->db, pSrc, pzErr);
    if( rc==SQLITE_OK && pTab->pSourceStr==0 ){
      pTab->pSourceStr = unionPrepare(&rc, pTab->db, 
        "SELECT group_concat(quote(name) || '.' || quote(type)) "
        "FROM pragma_table_info(?, ?)", pzErr
      );
    }
    if( rc==SQLITE_OK ){
      sqlite3_bind_text(pTab->pSourceStr, 1, pSrc->zTab, -1, SQLITE_STATIC);
      sqlite3_bind_text(pTab->pSourceStr, 2, pSrc->zDb, -1, SQLITE_STATIC);
      if( SQLITE_ROW==sqlite3_step(pTab->pSourceStr) ){
        const char *z = (const char*)sqlite3_column_text(pTab->pSourceStr, 0);
        zRet = unionStrdup(&rc, z);
      }
      unionReset(&rc, pTab->pSourceStr, pzErr);
    }
    *pRc = rc;
  }

  return zRet;
}

/*
** Check that all configured source tables exist and have the same column
** names and datatypes. If this is not the case, or if some other error
** occurs, return an SQLite error code. In this case *pzErr may be set
** to point to an error message buffer allocated by sqlite3_mprintf().
** Or, if no problems regarding the source tables are detected and no
** other error occurs, SQLITE_OK is returned.
*/
static int unionSourceCheck(UnionTab *pTab, char **pzErr){
  int rc = SQLITE_OK;

  assert( *pzErr==0 );
  if( pTab->nSrc==0 ){
    *pzErr = sqlite3_mprintf("no source tables configured");
    rc = SQLITE_ERROR;
  }else{
    char *z0 = 0;
    int i;

    z0 = unionSourceToStr(&rc, pTab, &pTab->aSrc[0], pzErr);
    for(i=1; i<pTab->nSrc; i++){
      char *z = unionSourceToStr(&rc, pTab, &pTab->aSrc[i], pzErr);
      if( rc==SQLITE_OK && sqlite3_stricmp(z, z0) ){
        *pzErr = sqlite3_mprintf("source table schema mismatch");
        rc = SQLITE_ERROR;
      }
      sqlite3_free(z);
    }

    unionFinalize(&rc, pTab->pSourceStr);
    pTab->pSourceStr = 0;
    sqlite3_free(z0);
  }
  return rc;
}

static int unionAttachDatabase(UnionTab *pTab, int iSrc, char **pzErr){
  int rc = SQLITE_OK;
  UnionSrc *pSrc = &pTab->aSrc[iSrc];

  assert( pTab->bSwarm && iSrc<pTab->nSrc );
  if( pSrc->bAttached==0 ){
    sqlite3_stmt *pStmt;

    if( pTab->nAttach>=pTab->nMaxAttach ){
      rc = unionDetachDatabase(pTab, pzErr);
    }

    pStmt = unionPreparePrintf(
        &rc, pzErr, pTab->db, "ATTACH %Q AS %s", pSrc->zFile, pSrc->zDb
    );
    if( rc==SQLITE_OK ){
      sqlite3_step(pStmt);
      rc = sqlite3_finalize(pStmt);
    }
    if( rc!=SQLITE_OK ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(pTab->db));
    }else{
      char *z = unionSourceToStr(&rc, pTab, pSrc, pzErr);
      if( rc==SQLITE_OK ){
        if( pTab->zSourceStr==0 ){
          pTab->zSourceStr = z;
        }else{
          if( sqlite3_stricmp(z, pTab->zSourceStr) ){
            *pzErr = sqlite3_mprintf("source table schema mismatch");
            rc = SQLITE_ERROR;
          }
          sqlite3_free(z);
        }
      }
    }

    pSrc->pNextAttached = pTab->pAttached;
    pSrc->bAttached = 1;
    pTab->pAttached = pSrc;
    pTab->nAttach++;
  }

  return rc;
}

/* 
** xConnect/xCreate method.
**
** The argv[] array contains the following:
**
**   argv[0]   -> module name  ("unionvtab")
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[3]   -> SQL statement
*/
static int unionConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  UnionTab *pTab = 0;
  int rc = SQLITE_OK;
  int bSwarm = (pAux==0 ? 0 : 1);
  const char *zVtab = (bSwarm ? "swarmvtab" : "unionvtab");
  const char *zName = argv[2];

  if( sqlite3_stricmp("temp", argv[1]) ){
    /* unionvtab tables may only be created in the temp schema */
    *pzErr = sqlite3_mprintf("%s tables must be created in TEMP schema", zVtab);
    rc = SQLITE_ERROR;
  }else if( argc!=4 ){
    *pzErr = sqlite3_mprintf("wrong number of arguments for %s", zVtab);
    rc = SQLITE_ERROR;
  }else{
    int nAlloc = 0;               /* Allocated size of pTab->aSrc[] */
    sqlite3_stmt *pStmt = 0;      /* Argument statement */
    char *zArg = unionStrdup(&rc, argv[3]);      /* Copy of argument to CVT */

    /* Prepare the SQL statement. Instead of executing it directly, sort
    ** the results by the "minimum rowid" field. This makes it easier to
    ** check that there are no rowid range overlaps between source tables 
    ** and that the UnionTab.aSrc[] array is always sorted by rowid.  */
    unionDequote(zArg);
    pStmt = unionPreparePrintf(&rc, pzErr, db, 
        "SELECT * FROM (%z) ORDER BY 3", zArg
    );

    /* Allocate the UnionTab structure */
    pTab = unionMalloc(&rc, sizeof(UnionTab));

    /* Iterate through the rows returned by the SQL statement specified
    ** as an argument to the CREATE VIRTUAL TABLE statement. */
    while( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      const char *zDb = (const char*)sqlite3_column_text(pStmt, 0);
      const char *zTab = (const char*)sqlite3_column_text(pStmt, 1);
      sqlite3_int64 iMin = sqlite3_column_int64(pStmt, 2);
      sqlite3_int64 iMax = sqlite3_column_int64(pStmt, 3);
      UnionSrc *pSrc;

      /* Grow the pTab->aSrc[] array if required. */
      if( nAlloc<=pTab->nSrc ){
        int nNew = nAlloc ? nAlloc*2 : 8;
        UnionSrc *aNew = (UnionSrc*)sqlite3_realloc(
            pTab->aSrc, nNew*sizeof(UnionSrc)
        );
        if( aNew==0 ){
          rc = SQLITE_NOMEM;
          break;
        }else{
          memset(&aNew[pTab->nSrc], 0, (nNew-pTab->nSrc)*sizeof(UnionSrc));
          pTab->aSrc = aNew;
          nAlloc = nNew;
        }
      }

      /* Check for problems with the specified range of rowids */
      if( iMax<iMin || (pTab->nSrc>0 && iMin<=pTab->aSrc[pTab->nSrc-1].iMax) ){
        *pzErr = sqlite3_mprintf("rowid range mismatch error");
        rc = SQLITE_ERROR;
      }

      if( rc==SQLITE_OK ){
        pSrc = &pTab->aSrc[pTab->nSrc++];
        pSrc->zTab = unionStrdup(&rc, zTab);
        pSrc->iMin = iMin;
        pSrc->iMax = iMax;
        if( bSwarm ){
          pSrc->zFile = unionStrdup(&rc, zDb);
          pSrc->zDb = sqlite3_mprintf("swm_%s_%d", zName, pTab->nSrc);
          if( pSrc->zDb==0 ) rc = SQLITE_NOMEM;
        }else{
          pSrc->zDb = unionStrdup(&rc, zDb);
          pSrc->bAttached = 1;
        }
      }
    }
    unionFinalize(&rc, pStmt);
    pStmt = 0;

    /* It is an error if the SELECT statement returned zero rows. If only
    ** because there is no way to determine the schema of the virtual 
    ** table in this case.  */
    if( rc==SQLITE_OK && pTab->nSrc==0 ){
      *pzErr = sqlite3_mprintf("no source tables configured");
      rc = SQLITE_ERROR;
    }

    /* For unionvtab, verify that all source tables exist and have 
    ** compatible schemas. For swarmvtab, attach the first database and
    ** check that the first table is a rowid table only.  */
    if( rc==SQLITE_OK ){
      pTab->db = db;
      pTab->bSwarm = bSwarm;
      pTab->nMaxAttach = SWARMVTAB_MAX_ATTACHED;
      if( bSwarm ){
        rc = unionAttachDatabase(pTab, 0, pzErr);
      }else{
        rc = unionSourceCheck(pTab, pzErr);
      }
    }

    /* Compose a CREATE TABLE statement and pass it to declare_vtab() */
    if( rc==SQLITE_OK ){
      pStmt = unionPreparePrintf(&rc, pzErr, db, "SELECT "
          "'CREATE TABLE xyz('"
          "    || group_concat(quote(name) || ' ' || type, ', ')"
          "    || ')',"
          "max((cid+1) * (type='INTEGER' COLLATE nocase AND pk=1))-1 "
          "FROM pragma_table_info(%Q, ?)", 
          pTab->aSrc[0].zTab, pTab->aSrc[0].zDb
      );
    }
    if( rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
      const char *zDecl = (const char*)sqlite3_column_text(pStmt, 0);
      rc = sqlite3_declare_vtab(db, zDecl);
      pTab->iPK = sqlite3_column_int(pStmt, 1);
    }

    unionFinalize(&rc, pStmt);
  }

  if( rc!=SQLITE_OK ){
    unionDisconnect((sqlite3_vtab*)pTab);
    pTab = 0;
  }

  *ppVtab = (sqlite3_vtab*)pTab;
  return rc;
}


/*
** xOpen
*/
static int unionOpen(sqlite3_vtab *p, sqlite3_vtab_cursor **ppCursor){
  UnionCsr *pCsr;
  int rc = SQLITE_OK;
  (void)p;  /* Suppress harmless warning */
  pCsr = (UnionCsr*)unionMalloc(&rc, sizeof(UnionCsr));
  *ppCursor = &pCsr->base;
  return rc;
}

/*
** xClose
*/
static int unionClose(sqlite3_vtab_cursor *cur){
  UnionCsr *pCsr = (UnionCsr*)cur;
  sqlite3_finalize(pCsr->pStmt);
  sqlite3_free(pCsr);
  return SQLITE_OK;
}


/*
** xNext
*/
static int doUnionNext(UnionCsr *pCsr){
  int rc = SQLITE_OK;
  assert( pCsr->pStmt );
  if( sqlite3_step(pCsr->pStmt)!=SQLITE_ROW ){
    UnionTab *pTab = (UnionTab*)pCsr->base.pVtab;
    rc = sqlite3_finalize(pCsr->pStmt);
    pCsr->pStmt = 0;
    if( rc==SQLITE_OK && pTab->bSwarm ){
      pCsr->iTab++;
      if( pCsr->iTab<pTab->nSrc ){
        UnionSrc *pSrc = &pTab->aSrc[pCsr->iTab];
        if( pCsr->iMaxRowid>=pSrc->iMin ){
          /* It is necessary to scan the next table. */
          rc = unionAttachDatabase(pTab, pCsr->iTab, &pTab->base.zErrMsg);
          pCsr->pStmt = unionPreparePrintf(&rc, &pTab->base.zErrMsg, pTab->db,
              "SELECT rowid, * FROM %Q.%Q %s %lld",
              pSrc->zDb, pSrc->zTab,
              (pSrc->iMax>pCsr->iMaxRowid ? "WHERE _rowid_ <=" : "-- "),
              pCsr->iMaxRowid
          );
          if( rc==SQLITE_OK ) rc = SQLITE_ROW;
        }
      }
    }
  }

  return rc;
}

static int unionNext(sqlite3_vtab_cursor *cur){
  int rc;
  do {
    rc = doUnionNext((UnionCsr*)cur);
  }while( rc==SQLITE_ROW );
  return rc;
}

/*
** xColumn
*/
static int unionColumn(
  sqlite3_vtab_cursor *cur,
  sqlite3_context *ctx,
  int i
){
  UnionCsr *pCsr = (UnionCsr*)cur;
  sqlite3_result_value(ctx, sqlite3_column_value(pCsr->pStmt, i+1));
  return SQLITE_OK;
}

/*
** xRowid
*/
static int unionRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  UnionCsr *pCsr = (UnionCsr*)cur;
  *pRowid = sqlite3_column_int64(pCsr->pStmt, 0);
  return SQLITE_OK;
}

/*
** xEof
*/
static int unionEof(sqlite3_vtab_cursor *cur){
  UnionCsr *pCsr = (UnionCsr*)cur;
  return pCsr->pStmt==0;
}

/*
** xFilter
*/
static int unionFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  UnionTab *pTab = (UnionTab*)(pVtabCursor->pVtab);
  UnionCsr *pCsr = (UnionCsr*)pVtabCursor;
  int rc = SQLITE_OK;
  int i;
  char *zSql = 0;
  int bZero = 0;

  sqlite3_int64 iMin = SMALLEST_INT64;
  sqlite3_int64 iMax = LARGEST_INT64;

  assert( idxNum==0 
       || idxNum==SQLITE_INDEX_CONSTRAINT_EQ
       || idxNum==SQLITE_INDEX_CONSTRAINT_LE
       || idxNum==SQLITE_INDEX_CONSTRAINT_GE
       || idxNum==SQLITE_INDEX_CONSTRAINT_LT
       || idxNum==SQLITE_INDEX_CONSTRAINT_GT
       || idxNum==(SQLITE_INDEX_CONSTRAINT_GE|SQLITE_INDEX_CONSTRAINT_LE)
  );

  (void)idxStr;  /* Suppress harmless warning */
  
  if( idxNum==SQLITE_INDEX_CONSTRAINT_EQ ){
    assert( argc==1 );
    iMin = iMax = sqlite3_value_int64(argv[0]);
  }else{

    if( idxNum & (SQLITE_INDEX_CONSTRAINT_LE|SQLITE_INDEX_CONSTRAINT_LT) ){
      assert( argc>=1 );
      iMax = sqlite3_value_int64(argv[0]);
      if( idxNum & SQLITE_INDEX_CONSTRAINT_LT ){
        if( iMax==SMALLEST_INT64 ){
          bZero = 1;
        }else{
          iMax--;
        }
      }
    }

    if( idxNum & (SQLITE_INDEX_CONSTRAINT_GE|SQLITE_INDEX_CONSTRAINT_GT) ){
      assert( argc>=1 );
      iMin = sqlite3_value_int64(argv[argc-1]);
      if( idxNum & SQLITE_INDEX_CONSTRAINT_GT ){
        if( iMin==LARGEST_INT64 ){
          bZero = 1;
        }else{
          iMin++;
        }
      }
    }
  }

  sqlite3_finalize(pCsr->pStmt);
  pCsr->pStmt = 0;
  if( bZero ){
    return SQLITE_OK;
  }

  for(i=0; i<pTab->nSrc; i++){
    UnionSrc *pSrc = &pTab->aSrc[i];
    if( iMin>pSrc->iMax || iMax<pSrc->iMin ){
      continue;
    }

    zSql = sqlite3_mprintf("%z%sSELECT rowid, * FROM %s%q%s%Q"
        , zSql
        , (zSql ? " UNION ALL " : "")
        , (pSrc->zDb ? "'" : "")
        , (pSrc->zDb ? pSrc->zDb : "")
        , (pSrc->zDb ? "'." : "")
        , pSrc->zTab
    );
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
      break;
    }

    if( iMin==iMax ){
      zSql = sqlite3_mprintf("%z WHERE rowid=%lld", zSql, iMin);
    }else{
      const char *zWhere = "WHERE";
      if( iMin!=SMALLEST_INT64 && iMin>pSrc->iMin ){
        zSql = sqlite3_mprintf("%z WHERE rowid>=%lld", zSql, iMin);
        zWhere = "AND";
      }
      if( iMax!=LARGEST_INT64 && iMax<pSrc->iMax ){
        zSql = sqlite3_mprintf("%z %s rowid<=%lld", zSql, zWhere, iMax);
      }
    }

    if( pTab->bSwarm ){
      pCsr->iTab = i;
      pCsr->iMaxRowid = iMax;
      rc = unionAttachDatabase(pTab, i, &pTab->base.zErrMsg);
      break;
    }
  }


  if( zSql==0 ) return rc;
  pCsr->pStmt = unionPrepare(&rc, pTab->db, zSql, &pTab->base.zErrMsg);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) return rc;
  return unionNext(pVtabCursor);
}

/*
** xBestIndex.
**
** This implementation searches for constraints on the rowid field. EQ, 
** LE, LT, GE and GT are handled.
**
** If there is an EQ comparison, then idxNum is set to INDEX_CONSTRAINT_EQ.
** In this case the only argument passed to xFilter is the rhs of the ==
** operator.
**
** Otherwise, if an LE or LT constraint is found, then the INDEX_CONSTRAINT_LE
** or INDEX_CONSTRAINT_LT (but not both) bit is set in idxNum. The first
** argument to xFilter is the rhs of the <= or < operator.  Similarly, if 
** an GE or GT constraint is found, then the INDEX_CONSTRAINT_GE or
** INDEX_CONSTRAINT_GT bit is set in idxNum. The rhs of the >= or > operator
** is passed as either the first or second argument to xFilter, depending
** on whether or not there is also a LT|LE constraint.
*/
static int unionBestIndex(
  sqlite3_vtab *tab,
  sqlite3_index_info *pIdxInfo
){
  UnionTab *pTab = (UnionTab*)tab;
  int iEq = -1;
  int iLt = -1;
  int iGt = -1;
  int i;

  for(i=0; i<pIdxInfo->nConstraint; i++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[i];
    if( p->usable && (p->iColumn<0 || p->iColumn==pTab->iPK) ){
      switch( p->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ:
          iEq = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_LE:
        case SQLITE_INDEX_CONSTRAINT_LT:
          iLt = i;
          break;
        case SQLITE_INDEX_CONSTRAINT_GE:
        case SQLITE_INDEX_CONSTRAINT_GT:
          iGt = i;
          break;
      }
    }
  }

  if( iEq>=0 ){
    pIdxInfo->estimatedRows = 1;
    pIdxInfo->idxFlags = SQLITE_INDEX_SCAN_UNIQUE;
    pIdxInfo->estimatedCost = 3.0;
    pIdxInfo->idxNum = SQLITE_INDEX_CONSTRAINT_EQ;
    pIdxInfo->aConstraintUsage[iEq].argvIndex = 1;
    pIdxInfo->aConstraintUsage[iEq].omit = 1;
  }else{
    int iCons = 1;
    int idxNum = 0;
    sqlite3_int64 nRow = 1000000;
    if( iLt>=0 ){
      nRow = nRow / 2;
      pIdxInfo->aConstraintUsage[iLt].argvIndex = iCons++;
      pIdxInfo->aConstraintUsage[iLt].omit = 1;
      idxNum |= pIdxInfo->aConstraint[iLt].op;
    }
    if( iGt>=0 ){
      nRow = nRow / 2;
      pIdxInfo->aConstraintUsage[iGt].argvIndex = iCons++;
      pIdxInfo->aConstraintUsage[iGt].omit = 1;
      idxNum |= pIdxInfo->aConstraint[iGt].op;
    }
    pIdxInfo->estimatedRows = nRow;
    pIdxInfo->estimatedCost = 3.0 * (double)nRow;
    pIdxInfo->idxNum = idxNum;
  }

  return SQLITE_OK;
}

/*
** Register the unionvtab virtual table module with database handle db.
*/
static int createUnionVtab(sqlite3 *db){
  static sqlite3_module unionModule = {
    0,                            /* iVersion */
    unionConnect,
    unionConnect,
    unionBestIndex,               /* xBestIndex - query planner */
    unionDisconnect, 
    unionDisconnect,
    unionOpen,                    /* xOpen - open a cursor */
    unionClose,                   /* xClose - close a cursor */
    unionFilter,                  /* xFilter - configure scan constraints */
    unionNext,                    /* xNext - advance a cursor */
    unionEof,                     /* xEof - check for end of scan */
    unionColumn,                  /* xColumn - read data */
    unionRowid,                   /* xRowid - read data */
    0,                            /* xUpdate */
    0,                            /* xBegin */
    0,                            /* xSync */
    0,                            /* xCommit */
    0,                            /* xRollback */
    0,                            /* xFindMethod */
    0,                            /* xRename */
    0,                            /* xSavepoint */
    0,                            /* xRelease */
    0                             /* xRollbackTo */
  };
  int rc;

  rc = sqlite3_create_module(db, "unionvtab", &unionModule, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_module(db, "swarmvtab", &unionModule, (void*)db);
  }
  return rc;
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_unionvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Suppress harmless warning */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  rc = createUnionVtab(db);
#endif
  return rc;
}
