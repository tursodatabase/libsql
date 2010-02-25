/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement the "sqlite" command line
** utility for accessing SQLite databases.
*/
#if defined(_WIN32) || defined(WIN32)
/* This needs to come before any includes for MSVC compiler */
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include "sqlite3.h"
#include <ctype.h>
#include <stdarg.h>

#if !defined(_WIN32) && !defined(WIN32) && !defined(__OS2__)
# include <signal.h>
# if !defined(__RTP__) && !defined(_WRS_KERNEL)
#  include <pwd.h>
# endif
# include <unistd.h>
# include <sys/types.h>
#endif

#ifdef __OS2__
# include <unistd.h>
#endif

#if defined(HAVE_READLINE) && HAVE_READLINE==1
# include <readline/readline.h>
# include <readline/history.h>
#else
# define readline(p) local_getline(p,stdin)
# define add_history(X)
# define read_history(X)
# define write_history(X)
# define stifle_history(X)
#endif

#if defined(_WIN32) || defined(WIN32)
# include <io.h>
#define isatty(h) _isatty(h)
#define access(f,m) _access((f),(m))
#else
/* Make sure isatty() has a prototype.
*/
extern int isatty();
#endif

#if defined(_WIN32_WCE)
/* Windows CE (arm-wince-mingw32ce-gcc) does not provide isatty()
 * thus we always assume that we have a console. That can be
 * overridden with the -batch command line option.
 */
#define isatty(x) 1
#endif

#if !defined(_WIN32) && !defined(WIN32) && !defined(__OS2__) && !defined(__RTP__) && !defined(_WRS_KERNEL)
#include <sys/time.h>
#include <sys/resource.h>

/* Saved resource information for the beginning of an operation */
static struct rusage sBegin;

/* True if the timer is enabled */
static int enableTimer = 0;

/*
** Begin timing an operation
*/
static void beginTimer(void){
  if( enableTimer ){
    getrusage(RUSAGE_SELF, &sBegin);
  }
}

/* Return the difference of two time_structs in seconds */
static double timeDiff(struct timeval *pStart, struct timeval *pEnd){
  return (pEnd->tv_usec - pStart->tv_usec)*0.000001 + 
         (double)(pEnd->tv_sec - pStart->tv_sec);
}

/*
** Print the timing results.
*/
static void endTimer(void){
  if( enableTimer ){
    struct rusage sEnd;
    getrusage(RUSAGE_SELF, &sEnd);
    printf("CPU Time: user %f sys %f\n",
       timeDiff(&sBegin.ru_utime, &sEnd.ru_utime),
       timeDiff(&sBegin.ru_stime, &sEnd.ru_stime));
  }
}

#define BEGIN_TIMER beginTimer()
#define END_TIMER endTimer()
#define HAS_TIMER 1

#elif (defined(_WIN32) || defined(WIN32))

#include <windows.h>

/* Saved resource information for the beginning of an operation */
static HANDLE hProcess;
static FILETIME ftKernelBegin;
static FILETIME ftUserBegin;
typedef BOOL (WINAPI *GETPROCTIMES)(HANDLE, LPFILETIME, LPFILETIME, LPFILETIME, LPFILETIME);
static GETPROCTIMES getProcessTimesAddr = NULL;

/* True if the timer is enabled */
static int enableTimer = 0;

/*
** Check to see if we have timer support.  Return 1 if necessary
** support found (or found previously).
*/
static int hasTimer(void){
  if( getProcessTimesAddr ){
    return 1;
  } else {
    /* GetProcessTimes() isn't supported in WIN95 and some other Windows versions.
    ** See if the version we are running on has it, and if it does, save off
    ** a pointer to it and the current process handle.
    */
    hProcess = GetCurrentProcess();
    if( hProcess ){
      HINSTANCE hinstLib = LoadLibrary(TEXT("Kernel32.dll"));
      if( NULL != hinstLib ){
        getProcessTimesAddr = (GETPROCTIMES) GetProcAddress(hinstLib, "GetProcessTimes");
        if( NULL != getProcessTimesAddr ){
          return 1;
        }
        FreeLibrary(hinstLib); 
      }
    }
  }
  return 0;
}

/*
** Begin timing an operation
*/
static void beginTimer(void){
  if( enableTimer && getProcessTimesAddr ){
    FILETIME ftCreation, ftExit;
    getProcessTimesAddr(hProcess, &ftCreation, &ftExit, &ftKernelBegin, &ftUserBegin);
  }
}

/* Return the difference of two FILETIME structs in seconds */
static double timeDiff(FILETIME *pStart, FILETIME *pEnd){
  sqlite_int64 i64Start = *((sqlite_int64 *) pStart);
  sqlite_int64 i64End = *((sqlite_int64 *) pEnd);
  return (double) ((i64End - i64Start) / 10000000.0);
}

/*
** Print the timing results.
*/
static void endTimer(void){
  if( enableTimer && getProcessTimesAddr){
    FILETIME ftCreation, ftExit, ftKernelEnd, ftUserEnd;
    getProcessTimesAddr(hProcess, &ftCreation, &ftExit, &ftKernelEnd, &ftUserEnd);
    printf("CPU Time: user %f sys %f\n",
       timeDiff(&ftUserBegin, &ftUserEnd),
       timeDiff(&ftKernelBegin, &ftKernelEnd));
  }
}

#define BEGIN_TIMER beginTimer()
#define END_TIMER endTimer()
#define HAS_TIMER hasTimer()

#else
#define BEGIN_TIMER 
#define END_TIMER
#define HAS_TIMER 0
#endif

/*
** Used to prevent warnings about unused parameters
*/
#define UNUSED_PARAMETER(x) (void)(x)


/**************************************************************************
***************************************************************************
** Begin genfkey logic.
*/
#if !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined SQLITE_OMIT_SUBQUERY

#define GENFKEY_ERROR         1
#define GENFKEY_DROPTRIGGER   2
#define GENFKEY_CREATETRIGGER 3
static int genfkey_create_triggers(sqlite3 *, const char *, void *,
  int (*)(void *, int, const char *)
);

struct GenfkeyCb {
  void *pCtx;
  int eType;
  int (*xData)(void *, int, const char *);
};
typedef struct GenfkeyCb GenfkeyCb;

/* The code in this file defines a sqlite3 virtual-table module that
** provides a read-only view of the current database schema. There is one
** row in the schema table for each column in the database schema.
*/
#define SCHEMA \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "tablename,"         /* Name of table */                                   \
  "cid,"               /* Column number (from left-to-right, 0 upward) */    \
  "name,"              /* Column name */                                     \
  "type,"              /* Specified type (i.e. VARCHAR(32)) */               \
  "not_null,"          /* Boolean. True if NOT NULL was specified */         \
  "dflt_value,"        /* Default value for this column */                   \
  "pk"                 /* True if this column is part of the primary key */  \
")"

#define SCHEMA2 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "from_tbl,"          /* Name of table */                                   \
  "fkid,"                                                                    \
  "seq,"                                                                     \
  "to_tbl,"                                                                  \
  "from_col,"                                                                \
  "to_col,"                                                                  \
  "on_update,"                                                               \
  "on_delete,"                                                               \
  "match"                                                                    \
")"

#define SCHEMA3 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "tablename,"         /* Name of table */                                   \
  "seq,"                                                                     \
  "name,"                                                                    \
  "isunique"                                                                 \
")"

#define SCHEMA4 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "indexname,"         /* Name of table */                                   \
  "seqno,"                                                                   \
  "cid,"                                                                     \
  "name"                                                                     \
")"

#define SCHEMA5 \
"CREATE TABLE x("                                                            \
  "database,"          /* Name of database (i.e. main, temp etc.) */         \
  "triggername,"       /* Name of trigger */                                 \
  "dummy"              /* Unused */                                          \
")"

typedef struct SchemaTable SchemaTable;
static struct SchemaTable {
  const char *zName;
  const char *zObject;
  const char *zPragma;
  const char *zSchema;
} aSchemaTable[] = {
  { "table_info",       "table", "PRAGMA %Q.table_info(%Q)",       SCHEMA },
  { "foreign_key_list", "table", "PRAGMA %Q.foreign_key_list(%Q)", SCHEMA2 },
  { "index_list",       "table", "PRAGMA %Q.index_list(%Q)",       SCHEMA3 },
  { "index_info",       "index", "PRAGMA %Q.index_info(%Q)",       SCHEMA4 },
  { "trigger_list",     "trigger", "SELECT 1",                     SCHEMA5 },
  { 0, 0, 0, 0 }
};

typedef struct schema_vtab schema_vtab;
typedef struct schema_cursor schema_cursor;

/* A schema table object */
struct schema_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  SchemaTable *pType;
};

/* A schema table cursor object */
struct schema_cursor {
  sqlite3_vtab_cursor base;
  sqlite3_stmt *pDbList;
  sqlite3_stmt *pTableList;
  sqlite3_stmt *pColumnList;
  int rowid;
};

/*
** Table destructor for the schema module.
*/
static int schemaDestroy(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return 0;
}

/*
** Table constructor for the schema module.
*/
static int schemaCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  int rc = SQLITE_NOMEM;
  schema_vtab *pVtab;
  SchemaTable *pType = &aSchemaTable[0];

  UNUSED_PARAMETER(pzErr);
  if( argc>3 ){
    int i;
    pType = 0;
    for(i=0; aSchemaTable[i].zName; i++){ 
      if( 0==strcmp(argv[3], aSchemaTable[i].zName) ){
        pType = &aSchemaTable[i];
      }
    }
    if( !pType ){
      return SQLITE_ERROR;
    }
  }

  pVtab = sqlite3_malloc(sizeof(schema_vtab));
  if( pVtab ){
    memset(pVtab, 0, sizeof(schema_vtab));
    pVtab->db = (sqlite3 *)pAux;
    pVtab->pType = pType;
    rc = sqlite3_declare_vtab(db, pType->zSchema);
  }
  *ppVtab = (sqlite3_vtab *)pVtab;
  return rc;
}

/*
** Open a new cursor on the schema table.
*/
static int schemaOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  schema_cursor *pCur;
  UNUSED_PARAMETER(pVTab);
  pCur = sqlite3_malloc(sizeof(schema_cursor));
  if( pCur ){
    memset(pCur, 0, sizeof(schema_cursor));
    *ppCursor = (sqlite3_vtab_cursor *)pCur;
    rc = SQLITE_OK;
  }
  return rc;
}

/*
** Close a schema table cursor.
*/
static int schemaClose(sqlite3_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  sqlite3_finalize(pCur->pDbList);
  sqlite3_finalize(pCur->pTableList);
  sqlite3_finalize(pCur->pColumnList);
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static void columnToResult(sqlite3_context *ctx, sqlite3_stmt *pStmt, int iCol){
  switch( sqlite3_column_type(pStmt, iCol) ){
    case SQLITE_NULL:
      sqlite3_result_null(ctx);
      break;
    case SQLITE_INTEGER:
      sqlite3_result_int64(ctx, sqlite3_column_int64(pStmt, iCol));
      break;
    case SQLITE_FLOAT:
      sqlite3_result_double(ctx, sqlite3_column_double(pStmt, iCol));
      break;
    case SQLITE_TEXT: {
      const char *z = (const char *)sqlite3_column_text(pStmt, iCol);
      sqlite3_result_text(ctx, z, -1, SQLITE_TRANSIENT);
      break;
    }
  }
}

/*
** Retrieve a column of data.
*/
static int schemaColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  schema_cursor *pCur = (schema_cursor *)cur;
  switch( i ){
    case 0:
      columnToResult(ctx, pCur->pDbList, 1);
      break;
    case 1:
      columnToResult(ctx, pCur->pTableList, 0);
      break;
    default:
      columnToResult(ctx, pCur->pColumnList, i-2);
      break;
  }
  return SQLITE_OK;
}

/*
** Retrieve the current rowid.
*/
static int schemaRowid(sqlite3_vtab_cursor *cur, sqlite_int64 *pRowid){
  schema_cursor *pCur = (schema_cursor *)cur;
  *pRowid = pCur->rowid;
  return SQLITE_OK;
}

static int finalize(sqlite3_stmt **ppStmt){
  int rc = sqlite3_finalize(*ppStmt);
  *ppStmt = 0;
  return rc;
}

static int schemaEof(sqlite3_vtab_cursor *cur){
  schema_cursor *pCur = (schema_cursor *)cur;
  return (pCur->pDbList ? 0 : 1);
}

/*
** Advance the cursor to the next row.
*/
static int schemaNext(sqlite3_vtab_cursor *cur){
  int rc = SQLITE_OK;
  schema_cursor *pCur = (schema_cursor *)cur;
  schema_vtab *pVtab = (schema_vtab *)(cur->pVtab);
  char *zSql = 0;

  while( !pCur->pColumnList || SQLITE_ROW!=sqlite3_step(pCur->pColumnList) ){
    if( SQLITE_OK!=(rc = finalize(&pCur->pColumnList)) ) goto next_exit;

    while( !pCur->pTableList || SQLITE_ROW!=sqlite3_step(pCur->pTableList) ){
      if( SQLITE_OK!=(rc = finalize(&pCur->pTableList)) ) goto next_exit;

      assert(pCur->pDbList);
      while( SQLITE_ROW!=sqlite3_step(pCur->pDbList) ){
        rc = finalize(&pCur->pDbList);
        goto next_exit;
      }

      /* Set zSql to the SQL to pull the list of tables from the 
      ** sqlite_master (or sqlite_temp_master) table of the database
      ** identfied by the row pointed to by the SQL statement pCur->pDbList
      ** (iterating through a "PRAGMA database_list;" statement).
      */
      if( sqlite3_column_int(pCur->pDbList, 0)==1 ){
        zSql = sqlite3_mprintf(
            "SELECT name FROM sqlite_temp_master WHERE type=%Q",
            pVtab->pType->zObject
        );
      }else{
        sqlite3_stmt *pDbList = pCur->pDbList;
        zSql = sqlite3_mprintf(
            "SELECT name FROM %Q.sqlite_master WHERE type=%Q",
             sqlite3_column_text(pDbList, 1), pVtab->pType->zObject
        );
      }
      if( !zSql ){
        rc = SQLITE_NOMEM;
        goto next_exit;
      }

      rc = sqlite3_prepare(pVtab->db, zSql, -1, &pCur->pTableList, 0);
      sqlite3_free(zSql);
      if( rc!=SQLITE_OK ) goto next_exit;
    }

    /* Set zSql to the SQL to the table_info pragma for the table currently
    ** identified by the rows pointed to by statements pCur->pDbList and
    ** pCur->pTableList.
    */
    zSql = sqlite3_mprintf(pVtab->pType->zPragma,
        sqlite3_column_text(pCur->pDbList, 1),
        sqlite3_column_text(pCur->pTableList, 0)
    );

    if( !zSql ){
      rc = SQLITE_NOMEM;
      goto next_exit;
    }
    rc = sqlite3_prepare(pVtab->db, zSql, -1, &pCur->pColumnList, 0);
    sqlite3_free(zSql);
    if( rc!=SQLITE_OK ) goto next_exit;
  }
  pCur->rowid++;

next_exit:
  /* TODO: Handle rc */
  return rc;
}

/*
** Reset a schema table cursor.
*/
static int schemaFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  int rc;
  schema_vtab *pVtab = (schema_vtab *)(pVtabCursor->pVtab);
  schema_cursor *pCur = (schema_cursor *)pVtabCursor;
  UNUSED_PARAMETER(idxNum);
  UNUSED_PARAMETER(idxStr);
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(argv);
  pCur->rowid = 0;
  finalize(&pCur->pTableList);
  finalize(&pCur->pColumnList);
  finalize(&pCur->pDbList);
  rc = sqlite3_prepare(pVtab->db,"SELECT 0, 'main'", -1, &pCur->pDbList, 0);
  return (rc==SQLITE_OK ? schemaNext(pVtabCursor) : rc);
}

/*
** Analyse the WHERE condition.
*/
static int schemaBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  UNUSED_PARAMETER(tab);
  UNUSED_PARAMETER(pIdxInfo);
  return SQLITE_OK;
}

/*
** A virtual table module that merely echos method calls into TCL
** variables.
*/
static sqlite3_module schemaModule = {
  0,                           /* iVersion */
  schemaCreate,
  schemaCreate,
  schemaBestIndex,
  schemaDestroy,
  schemaDestroy,
  schemaOpen,                  /* xOpen - open a cursor */
  schemaClose,                 /* xClose - close a cursor */
  schemaFilter,                /* xFilter - configure scan constraints */
  schemaNext,                  /* xNext - advance a cursor */
  schemaEof,                   /* xEof */
  schemaColumn,                /* xColumn - read data */
  schemaRowid,                 /* xRowid - read data */
  0,                           /* xUpdate */
  0,                           /* xBegin */
  0,                           /* xSync */
  0,                           /* xCommit */
  0,                           /* xRollback */
  0,                           /* xFindMethod */
  0,                           /* xRename */
};

/*
** Extension load function.
*/
static int installSchemaModule(sqlite3 *db, sqlite3 *sdb){
  sqlite3_create_module(db, "schema", &schemaModule, (void *)sdb);
  return 0;
}

/*
**   sj(zValue, zJoin)
**
** The following block contains the implementation of an aggregate 
** function that returns a string. Each time the function is stepped, 
** it appends data to an internal buffer. When the aggregate is finalized,
** the contents of the buffer are returned.
**
** The first time the aggregate is stepped the buffer is set to a copy
** of the first argument. The second time and subsequent times it is
** stepped a copy of the second argument is appended to the buffer, then
** a copy of the first.
**
** Example:
**
**   INSERT INTO t1(a) VALUES('1');
**   INSERT INTO t1(a) VALUES('2');
**   INSERT INTO t1(a) VALUES('3');
**   SELECT sj(a, ', ') FROM t1;
**
**     =>  "1, 2, 3"
**
*/
struct StrBuffer {
  char *zBuf;
};
typedef struct StrBuffer StrBuffer;
static void joinFinalize(sqlite3_context *context){
  StrBuffer *p;
  p = (StrBuffer *)sqlite3_aggregate_context(context, sizeof(StrBuffer));
  sqlite3_result_text(context, p->zBuf, -1, SQLITE_TRANSIENT);
  sqlite3_free(p->zBuf);
}
static void joinStep(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  StrBuffer *p;
  UNUSED_PARAMETER(argc);
  p = (StrBuffer *)sqlite3_aggregate_context(context, sizeof(StrBuffer));
  if( p->zBuf==0 ){
    p->zBuf = sqlite3_mprintf("%s", sqlite3_value_text(argv[0]));
  }else{
    char *zTmp = p->zBuf;
    p->zBuf = sqlite3_mprintf("%s%s%s", 
        zTmp, sqlite3_value_text(argv[1]), sqlite3_value_text(argv[0])
    );
    sqlite3_free(zTmp);
  }
}

/*
**   dq(zString)
**
** This scalar function accepts a single argument and interprets it as
** a text value. The return value is the argument enclosed in double
** quotes. If any double quote characters are present in the argument, 
** these are escaped.
**
**   dq('the raven "Nevermore."') == '"the raven ""Nevermore."""'
*/
static void doublequote(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  int ii;
  char *zOut;
  char *zCsr;
  const char *zIn = (const char *)sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);

  UNUSED_PARAMETER(argc);
  zOut = sqlite3_malloc(nIn*2+3);
  zCsr = zOut;
  *zCsr++ = '"';
  for(ii=0; ii<nIn; ii++){
    *zCsr++ = zIn[ii];
    if( zIn[ii]=='"' ){
      *zCsr++ = '"';
    }
  }
  *zCsr++ = '"';
  *zCsr++ = '\0';

  sqlite3_result_text(context, zOut, -1, SQLITE_TRANSIENT);
  sqlite3_free(zOut);
}

/*
**   multireplace(zString, zSearch1, zReplace1, ...)
*/
static void multireplace(
  sqlite3_context *context, 
  int argc, 
  sqlite3_value **argv
){
  int i = 0;
  char *zOut = 0;
  int nOut = 0;
  int nMalloc = 0;
  const char *zIn = (const char *)sqlite3_value_text(argv[0]);
  int nIn = sqlite3_value_bytes(argv[0]);

  while( i<nIn ){
    const char *zCopy = &zIn[i];
    int nCopy = 1;
    int nReplace = 1;
    int j;
    for(j=1; j<(argc-1); j+=2){
      const char *z = (const char *)sqlite3_value_text(argv[j]);
      int n = sqlite3_value_bytes(argv[j]);
      if( n<=(nIn-i) && 0==strncmp(z, zCopy, n) ){
        zCopy = (const char *)sqlite3_value_text(argv[j+1]);
        nCopy = sqlite3_value_bytes(argv[j+1]);
        nReplace = n;
        break;
      }
    }
    if( (nOut+nCopy)>nMalloc ){
      char *zNew;
      nMalloc = 16 + (nOut+nCopy)*2;
      zNew = (char*)sqlite3_realloc(zOut, nMalloc);
      if( zNew==0 ){
        sqlite3_result_error_nomem(context);
        return;
      }else{
        zOut = zNew;
      }
    }
    assert( nMalloc>=(nOut+nCopy) );
    memcpy(&zOut[nOut], zCopy, nCopy);
    i += nReplace;
    nOut += nCopy;
  }

  sqlite3_result_text(context, zOut, nOut, SQLITE_TRANSIENT);
  sqlite3_free(zOut);
}

/*
** A callback for sqlite3_exec() invokes the callback specified by the
** GenfkeyCb structure pointed to by the void* passed as the first argument.
*/
static int invokeCallback(void *p, int nArg, char **azArg, char **azCol){
  GenfkeyCb *pCb = (GenfkeyCb *)p;
  UNUSED_PARAMETER(nArg);
  UNUSED_PARAMETER(azCol);
  return pCb->xData(pCb->pCtx, pCb->eType, azArg[0]);
}

static int detectSchemaProblem(
  sqlite3 *db,                   /* Database connection */
  const char *zMessage,          /* English language error message */
  const char *zSql,              /* SQL statement to run */
  GenfkeyCb *pCb
){
  sqlite3_stmt *pStmt;
  int rc;
  rc = sqlite3_prepare(db, zSql, -1, &pStmt, 0);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  while( SQLITE_ROW==sqlite3_step(pStmt) ){
    char *zDel;
    int iFk = sqlite3_column_int(pStmt, 0);
    const char *zTab = (const char *)sqlite3_column_text(pStmt, 1);
    zDel = sqlite3_mprintf("Error in table %s: %s", zTab, zMessage);
    rc = pCb->xData(pCb->pCtx, pCb->eType, zDel);
    sqlite3_free(zDel);
    if( rc!=SQLITE_OK ) return rc;
    zDel = sqlite3_mprintf(
        "DELETE FROM temp.fkey WHERE from_tbl = %Q AND fkid = %d"
        , zTab, iFk
    );
    sqlite3_exec(db, zDel, 0, 0, 0);
    sqlite3_free(zDel);
  }
  sqlite3_finalize(pStmt);
  return SQLITE_OK;
}

/*
** Create and populate temporary table "fkey".
*/
static int populateTempTable(sqlite3 *db, GenfkeyCb *pCallback){
  int rc;
  
  rc = sqlite3_exec(db, 
      "CREATE VIRTUAL TABLE temp.v_fkey USING schema(foreign_key_list);"
      "CREATE VIRTUAL TABLE temp.v_col USING schema(table_info);"
      "CREATE VIRTUAL TABLE temp.v_idxlist USING schema(index_list);"
      "CREATE VIRTUAL TABLE temp.v_idxinfo USING schema(index_info);"
      "CREATE VIRTUAL TABLE temp.v_triggers USING schema(trigger_list);"
      "CREATE TABLE temp.fkey AS "
        "SELECT from_tbl, to_tbl, fkid, from_col, to_col, on_update, on_delete "
        "FROM temp.v_fkey WHERE database = 'main';"
      , 0, 0, 0
  );
  if( rc!=SQLITE_OK ) return rc;

  rc = detectSchemaProblem(db, "foreign key columns do not exist",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NOT NULL AND NOT EXISTS (SELECT 1 "
        "FROM temp.v_col WHERE tablename=to_tbl AND name==to_col"
    ")", pCallback
  );
  if( rc!=SQLITE_OK ) return rc;

  /* At this point the temp.fkey table is mostly populated. If any foreign
  ** keys were specified so that they implicitly refer to they primary
  ** key of the parent table, the "to_col" values of the temp.fkey rows
  ** are still set to NULL.
  **
  ** This is easily fixed for single column primary keys, but not for
  ** composites. With a composite primary key, there is no way to reliably
  ** query sqlite for the order in which the columns that make up the
  ** composite key were declared i.e. there is no way to tell if the
  ** schema actually contains "PRIMARY KEY(a, b)" or "PRIMARY KEY(b, a)".
  ** Therefore, this case is not handled. The following function call
  ** detects instances of this case.
  */
  rc = detectSchemaProblem(db, "implicit mapping to composite primary key",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NULL "
    "GROUP BY fkid, from_tbl HAVING count(*) > 1", pCallback
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Detect attempts to implicitly map to the primary key of a table 
  ** that has no primary key column.
  */
  rc = detectSchemaProblem(db, "implicit mapping to non-existant primary key",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey "
    "WHERE to_col IS NULL AND NOT EXISTS "
      "(SELECT 1 FROM temp.v_col WHERE pk AND tablename = temp.fkey.to_tbl)"
    , pCallback
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Fix all the implicit primary key mappings in the temp.fkey table. */
  rc = sqlite3_exec(db, 
    "UPDATE temp.fkey SET to_col = "
      "(SELECT name FROM temp.v_col WHERE pk AND tablename=temp.fkey.to_tbl)"
    " WHERE to_col IS NULL;"
    , 0, 0, 0
  );
  if( rc!=SQLITE_OK ) return rc;

  /* Now check that all all parent keys are either primary keys or 
  ** subject to a unique constraint.
  */
  rc = sqlite3_exec(db, 
    "CREATE TABLE temp.idx2 AS SELECT "
      "il.tablename AS tablename,"
      "ii.indexname AS indexname,"
      "ii.name AS col "
      "FROM temp.v_idxlist AS il, temp.v_idxinfo AS ii "
      "WHERE il.isunique AND il.database='main' AND ii.indexname = il.name;"
    "INSERT INTO temp.idx2 "
      "SELECT tablename, 'pk', name FROM temp.v_col WHERE pk;"

    "CREATE TABLE temp.idx AS SELECT "
      "tablename, indexname, sj(dq(col),',') AS cols "
      "FROM (SELECT * FROM temp.idx2 ORDER BY col) " 
      "GROUP BY tablename, indexname;"

    "CREATE TABLE temp.fkey2 AS SELECT "
        "fkid, from_tbl, to_tbl, sj(dq(to_col),',') AS cols "
        "FROM (SELECT * FROM temp.fkey ORDER BY to_col) " 
        "GROUP BY fkid, from_tbl;"

    "CREATE TABLE temp.triggers AS SELECT "
        "triggername FROM temp.v_triggers WHERE database='main' AND "
        "triggername LIKE 'genfkey%';"
    , 0, 0, 0
  );
  if( rc!=SQLITE_OK ) return rc;
  rc = detectSchemaProblem(db, "foreign key is not unique",
    "SELECT fkid, from_tbl "
    "FROM temp.fkey2 "
    "WHERE NOT EXISTS (SELECT 1 "
        "FROM temp.idx WHERE tablename=to_tbl AND fkey2.cols==idx.cols"
    ")", pCallback
  );
  if( rc!=SQLITE_OK ) return rc;

  return rc;
}

#define GENFKEY_ERROR         1
#define GENFKEY_DROPTRIGGER   2
#define GENFKEY_CREATETRIGGER 3
static int genfkey_create_triggers(
  sqlite3 *sdb,                        /* Connection to read schema from */
  const char *zDb,                     /* Name of db to read ("main", "temp") */
  void *pCtx,                          /* Context pointer to pass to xData */
  int (*xData)(void *, int, const char *)
){
  const char *zSql =
    "SELECT multireplace('"

      "-- Triggers for foreign key mapping:\n"
      "--\n"
      "--     /from_readable/ REFERENCES /to_readable/\n"
      "--     on delete /on_delete/\n"
      "--     on update /on_update/\n"
      "--\n"

      /* The "BEFORE INSERT ON <referencing>" trigger. This trigger's job is to
      ** throw an exception if the user tries to insert a row into the
      ** referencing table for which there is no corresponding row in
      ** the referenced table.
      */
      "CREATE TRIGGER /name/_insert_referencing BEFORE INSERT ON /tbl/ WHEN \n"
      "    /key_notnull/ AND NOT EXISTS (SELECT 1 FROM /ref/ WHERE /cond1/)\n" 
      "BEGIN\n"
        "  SELECT RAISE(ABORT, ''constraint failed'');\n"
      "END;\n"

      /* The "BEFORE UPDATE ON <referencing>" trigger. This trigger's job 
      ** is to throw an exception if the user tries to update a row in the
      ** referencing table causing it to correspond to no row in the
      ** referenced table.
      */
      "CREATE TRIGGER /name/_update_referencing BEFORE\n"
      "    UPDATE OF /rkey_list/ ON /tbl/ WHEN \n"
      "    /key_notnull/ AND \n"
      "    NOT EXISTS (SELECT 1 FROM /ref/ WHERE /cond1/)\n" 
      "BEGIN\n"
        "  SELECT RAISE(ABORT, ''constraint failed'');\n"
      "END;\n"


      /* The "BEFORE DELETE ON <referenced>" trigger. This trigger's job 
      ** is to detect when a row is deleted from the referenced table to 
      ** which rows in the referencing table correspond. The action taken
      ** depends on the value of the 'ON DELETE' clause.
      */
      "CREATE TRIGGER /name/_delete_referenced BEFORE DELETE ON /ref/ WHEN\n"
      "    EXISTS (SELECT 1 FROM /tbl/ WHERE /cond2/)\n"
      "BEGIN\n"
      "  /delete_action/\n"
      "END;\n"

      /* The "AFTER UPDATE ON <referenced>" trigger. This trigger's job 
      ** is to detect when the key columns of a row in the referenced table 
      ** to which one or more rows in the referencing table correspond are
      ** updated. The action taken depends on the value of the 'ON UPDATE' 
      ** clause.
      */
      "CREATE TRIGGER /name/_update_referenced AFTER\n"
      "    UPDATE OF /fkey_list/ ON /ref/ WHEN \n"
      "    EXISTS (SELECT 1 FROM /tbl/ WHERE /cond2/)\n"
      "BEGIN\n"
      "  /update_action/\n"
      "END;\n"
    "'"

    /* These are used in the SQL comment written above each set of triggers */
    ", '/from_readable/',  from_tbl || '(' || sj(from_col, ', ') || ')'"
    ", '/to_readable/',    to_tbl || '(' || sj(to_col, ', ') || ')'"
    ", '/on_delete/', on_delete"
    ", '/on_update/', on_update"

    ", '/name/',   'genfkey' || min(rowid)"
    ", '/tbl/',    dq(from_tbl)"
    ", '/ref/',    dq(to_tbl)"
    ", '/key_notnull/', sj('new.' || dq(from_col) || ' IS NOT NULL', ' AND ')"

    ", '/fkey_list/', sj(dq(to_col), ', ')"
    ", '/rkey_list/', sj(dq(from_col), ', ')"

    ", '/cond1/',  sj(multireplace('new./from/ == /to/'"
                   ", '/from/', dq(from_col)"
                   ", '/to/',   dq(to_col)"
                   "), ' AND ')"
    ", '/cond2/',  sj(multireplace('old./to/ == /from/'"
                   ", '/from/', dq(from_col)"
                   ", '/to/',   dq(to_col)"
                   "), ' AND ')"

    ", '/update_action/', CASE on_update "
      "WHEN 'SET NULL' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(dq(from_col)||' = NULL',', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "WHEN 'CASCADE' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(dq(from_col)||' = new.'||dq(to_col),', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "ELSE "
      "  'SELECT RAISE(ABORT, ''constraint failed'');'"
      "END "

    ", '/delete_action/', CASE on_delete "
      "WHEN 'SET NULL' THEN "
        "multireplace('UPDATE /tbl/ SET /setlist/ WHERE /where/;' "
        ", '/setlist/', sj(dq(from_col)||' = NULL',', ')"
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "WHEN 'CASCADE' THEN "
        "multireplace('DELETE FROM /tbl/ WHERE /where/;' "
        ", '/tbl/',     dq(from_tbl)"
        ", '/where/',   sj(dq(from_col)||' = old.'||dq(to_col),' AND ')"
        ")"
      "ELSE "
      "  'SELECT RAISE(ABORT, ''constraint failed'');'"
      "END "

    ") FROM temp.fkey "
    "GROUP BY from_tbl, fkid"
  ;

  int rc;
  const int enc = SQLITE_UTF8;
  sqlite3 *db = 0;

  GenfkeyCb cb;
  cb.xData = xData;
  cb.pCtx = pCtx;

  UNUSED_PARAMETER(zDb);

  /* Open the working database handle. */
  rc = sqlite3_open(":memory:", &db);
  if( rc!=SQLITE_OK ) goto genfkey_exit;

  /* Create the special scalar and aggregate functions used by this program. */
  sqlite3_create_function(db, "dq", 1, enc, 0, doublequote, 0, 0);
  sqlite3_create_function(db, "multireplace", -1, enc, db, multireplace, 0, 0);
  sqlite3_create_function(db, "sj", 2, enc, 0, 0, joinStep, joinFinalize);

  /* Install the "schema" virtual table module */
  installSchemaModule(db, sdb);

  /* Create and populate a temp table with the information required to
  ** build the foreign key triggers. See function populateTempTable()
  ** for details.
  */
  cb.eType = GENFKEY_ERROR;
  rc = populateTempTable(db, &cb);
  if( rc!=SQLITE_OK ) goto genfkey_exit;

  /* Unless the --no-drop option was specified, generate DROP TRIGGER
  ** statements to drop any triggers in the database generated by a
  ** previous run of this program.
  */
  cb.eType = GENFKEY_DROPTRIGGER;
  rc = sqlite3_exec(db, 
    "SELECT 'DROP TRIGGER main.' || dq(triggername) || ';' FROM triggers"
    ,invokeCallback, (void *)&cb, 0
  );
  if( rc!=SQLITE_OK ) goto genfkey_exit;

  /* Run the main query to create the trigger definitions. */
  cb.eType = GENFKEY_CREATETRIGGER;
  rc = sqlite3_exec(db, zSql, invokeCallback, (void *)&cb, 0);
  if( rc!=SQLITE_OK ) goto genfkey_exit;

genfkey_exit:
  sqlite3_close(db);
  return rc;
}


#endif
/* End genfkey logic. */
/*************************************************************************/
/*************************************************************************/

/*
** If the following flag is set, then command execution stops
** at an error if we are not interactive.
*/
static int bail_on_error = 0;

/*
** Threat stdin as an interactive input if the following variable
** is true.  Otherwise, assume stdin is connected to a file or pipe.
*/
static int stdin_is_interactive = 1;

/*
** The following is the open SQLite database.  We make a pointer
** to this database a static variable so that it can be accessed
** by the SIGINT handler to interrupt database processing.
*/
static sqlite3 *db = 0;

/*
** True if an interrupt (Control-C) has been received.
*/
static volatile int seenInterrupt = 0;

/*
** This is the name of our program. It is set in main(), used
** in a number of other places, mostly for error messages.
*/
static char *Argv0;

/*
** Prompt strings. Initialized in main. Settable with
**   .prompt main continue
*/
static char mainPrompt[20];     /* First line prompt. default: "sqlite> "*/
static char continuePrompt[20]; /* Continuation prompt. default: "   ...> " */

/*
** Write I/O traces to the following stream.
*/
#ifdef SQLITE_ENABLE_IOTRACE
static FILE *iotrace = 0;
#endif

/*
** This routine works like printf in that its first argument is a
** format string and subsequent arguments are values to be substituted
** in place of % fields.  The result of formatting this string
** is written to iotrace.
*/
#ifdef SQLITE_ENABLE_IOTRACE
static void iotracePrintf(const char *zFormat, ...){
  va_list ap;
  char *z;
  if( iotrace==0 ) return;
  va_start(ap, zFormat);
  z = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  fprintf(iotrace, "%s", z);
  sqlite3_free(z);
}
#endif


/*
** Determines if a string is a number of not.
*/
static int isNumber(const char *z, int *realnum){
  if( *z=='-' || *z=='+' ) z++;
  if( !isdigit(*z) ){
    return 0;
  }
  z++;
  if( realnum ) *realnum = 0;
  while( isdigit(*z) ){ z++; }
  if( *z=='.' ){
    z++;
    if( !isdigit(*z) ) return 0;
    while( isdigit(*z) ){ z++; }
    if( realnum ) *realnum = 1;
  }
  if( *z=='e' || *z=='E' ){
    z++;
    if( *z=='+' || *z=='-' ) z++;
    if( !isdigit(*z) ) return 0;
    while( isdigit(*z) ){ z++; }
    if( realnum ) *realnum = 1;
  }
  return *z==0;
}

/*
** A global char* and an SQL function to access its current value 
** from within an SQL statement. This program used to use the 
** sqlite_exec_printf() API to substitue a string into an SQL statement.
** The correct way to do this with sqlite3 is to use the bind API, but
** since the shell is built around the callback paradigm it would be a lot
** of work. Instead just use this hack, which is quite harmless.
*/
static const char *zShellStatic = 0;
static void shellstaticFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( 0==argc );
  assert( zShellStatic );
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(argv);
  sqlite3_result_text(context, zShellStatic, -1, SQLITE_STATIC);
}


/*
** This routine reads a line of text from FILE in, stores
** the text in memory obtained from malloc() and returns a pointer
** to the text.  NULL is returned at end of file, or if malloc()
** fails.
**
** The interface is like "readline" but no command-line editing
** is done.
*/
static char *local_getline(char *zPrompt, FILE *in){
  char *zLine;
  int nLine;
  int n;
  int eol;

  if( zPrompt && *zPrompt ){
    printf("%s",zPrompt);
    fflush(stdout);
  }
  nLine = 100;
  zLine = malloc( nLine );
  if( zLine==0 ) return 0;
  n = 0;
  eol = 0;
  while( !eol ){
    if( n+100>nLine ){
      nLine = nLine*2 + 100;
      zLine = realloc(zLine, nLine);
      if( zLine==0 ) return 0;
    }
    if( fgets(&zLine[n], nLine - n, in)==0 ){
      if( n==0 ){
        free(zLine);
        return 0;
      }
      zLine[n] = 0;
      eol = 1;
      break;
    }
    while( zLine[n] ){ n++; }
    if( n>0 && zLine[n-1]=='\n' ){
      n--;
      if( n>0 && zLine[n-1]=='\r' ) n--;
      zLine[n] = 0;
      eol = 1;
    }
  }
  zLine = realloc( zLine, n+1 );
  return zLine;
}

/*
** Retrieve a single line of input text.
**
** zPrior is a string of prior text retrieved.  If not the empty
** string, then issue a continuation prompt.
*/
static char *one_input_line(const char *zPrior, FILE *in){
  char *zPrompt;
  char *zResult;
  if( in!=0 ){
    return local_getline(0, in);
  }
  if( zPrior && zPrior[0] ){
    zPrompt = continuePrompt;
  }else{
    zPrompt = mainPrompt;
  }
  zResult = readline(zPrompt);
#if defined(HAVE_READLINE) && HAVE_READLINE==1
  if( zResult && *zResult ) add_history(zResult);
#endif
  return zResult;
}

struct previous_mode_data {
  int valid;        /* Is there legit data in here? */
  int mode;
  int showHeader;
  int colWidth[100];
};

/*
** An pointer to an instance of this structure is passed from
** the main program to the callback.  This is used to communicate
** state and mode information.
*/
struct callback_data {
  sqlite3 *db;           /* The database */
  int echoOn;            /* True to echo input commands */
  int cnt;               /* Number of records displayed so far */
  FILE *out;             /* Write results here */
  int mode;              /* An output mode setting */
  int writableSchema;    /* True if PRAGMA writable_schema=ON */
  int showHeader;        /* True to show column names in List or Column mode */
  char *zDestTable;      /* Name of destination table when MODE_Insert */
  char separator[20];    /* Separator character for MODE_List */
  int colWidth[100];     /* Requested width of each column when in column mode*/
  int actualWidth[100];  /* Actual width of each column */
  char nullvalue[20];    /* The text to print when a NULL comes back from
                         ** the database */
  struct previous_mode_data explainPrev;
                         /* Holds the mode information just before
                         ** .explain ON */
  char outfile[FILENAME_MAX]; /* Filename for *out */
  const char *zDbFilename;    /* name of the database file */
  sqlite3_stmt *pStmt;   /* Current statement if any. */
  FILE *pLog;            /* Write log output here */
};

/*
** These are the allowed modes.
*/
#define MODE_Line     0  /* One column per line.  Blank line between records */
#define MODE_Column   1  /* One record per line in neat columns */
#define MODE_List     2  /* One record per line with a separator */
#define MODE_Semi     3  /* Same as MODE_List but append ";" to each line */
#define MODE_Html     4  /* Generate an XHTML table */
#define MODE_Insert   5  /* Generate SQL "insert" statements */
#define MODE_Tcl      6  /* Generate ANSI-C or TCL quoted elements */
#define MODE_Csv      7  /* Quote strings, numbers are plain */
#define MODE_Explain  8  /* Like MODE_Column, but do not truncate data */

static const char *modeDescr[] = {
  "line",
  "column",
  "list",
  "semi",
  "html",
  "insert",
  "tcl",
  "csv",
  "explain",
};

/*
** Number of elements in an array
*/
#define ArraySize(X)  (int)(sizeof(X)/sizeof(X[0]))

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
*/
static int strlen30(const char *z){
  const char *z2 = z;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}

/*
** A callback for the sqlite3_log() interface.
*/
static void shellLog(void *pArg, int iErrCode, const char *zMsg){
  struct callback_data *p = (struct callback_data*)pArg;
  if( p->pLog==0 ) return;
  fprintf(p->pLog, "(%d) %s\n", iErrCode, zMsg);
  fflush(p->pLog);
}

/*
** Output the given string as a hex-encoded blob (eg. X'1234' )
*/
static void output_hex_blob(FILE *out, const void *pBlob, int nBlob){
  int i;
  char *zBlob = (char *)pBlob;
  fprintf(out,"X'");
  for(i=0; i<nBlob; i++){ fprintf(out,"%02x",zBlob[i]); }
  fprintf(out,"'");
}

/*
** Output the given string as a quoted string using SQL quoting conventions.
*/
static void output_quoted_string(FILE *out, const char *z){
  int i;
  int nSingle = 0;
  for(i=0; z[i]; i++){
    if( z[i]=='\'' ) nSingle++;
  }
  if( nSingle==0 ){
    fprintf(out,"'%s'",z);
  }else{
    fprintf(out,"'");
    while( *z ){
      for(i=0; z[i] && z[i]!='\''; i++){}
      if( i==0 ){
        fprintf(out,"''");
        z++;
      }else if( z[i]=='\'' ){
        fprintf(out,"%.*s''",i,z);
        z += i+1;
      }else{
        fprintf(out,"%s",z);
        break;
      }
    }
    fprintf(out,"'");
  }
}

/*
** Output the given string as a quoted according to C or TCL quoting rules.
*/
static void output_c_string(FILE *out, const char *z){
  unsigned int c;
  fputc('"', out);
  while( (c = *(z++))!=0 ){
    if( c=='\\' ){
      fputc(c, out);
      fputc(c, out);
    }else if( c=='\t' ){
      fputc('\\', out);
      fputc('t', out);
    }else if( c=='\n' ){
      fputc('\\', out);
      fputc('n', out);
    }else if( c=='\r' ){
      fputc('\\', out);
      fputc('r', out);
    }else if( !isprint(c) ){
      fprintf(out, "\\%03o", c&0xff);
    }else{
      fputc(c, out);
    }
  }
  fputc('"', out);
}

/*
** Output the given string with characters that are special to
** HTML escaped.
*/
static void output_html_string(FILE *out, const char *z){
  int i;
  while( *z ){
    for(i=0;   z[i] 
            && z[i]!='<' 
            && z[i]!='&' 
            && z[i]!='>' 
            && z[i]!='\"' 
            && z[i]!='\'';
        i++){}
    if( i>0 ){
      fprintf(out,"%.*s",i,z);
    }
    if( z[i]=='<' ){
      fprintf(out,"&lt;");
    }else if( z[i]=='&' ){
      fprintf(out,"&amp;");
    }else if( z[i]=='>' ){
      fprintf(out,"&gt;");
    }else if( z[i]=='\"' ){
      fprintf(out,"&quot;");
    }else if( z[i]=='\'' ){
      fprintf(out,"&#39;");
    }else{
      break;
    }
    z += i + 1;
  }
}

/*
** If a field contains any character identified by a 1 in the following
** array, then the string must be quoted for CSV.
*/
static const char needCsvQuote[] = {
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 0, 1, 0, 0, 0, 0, 1,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 1, 
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
};

/*
** Output a single term of CSV.  Actually, p->separator is used for
** the separator, which may or may not be a comma.  p->nullvalue is
** the null value.  Strings are quoted using ANSI-C rules.  Numbers
** appear outside of quotes.
*/
static void output_csv(struct callback_data *p, const char *z, int bSep){
  FILE *out = p->out;
  if( z==0 ){
    fprintf(out,"%s",p->nullvalue);
  }else{
    int i;
    int nSep = strlen30(p->separator);
    for(i=0; z[i]; i++){
      if( needCsvQuote[((unsigned char*)z)[i]] 
         || (z[i]==p->separator[0] && 
             (nSep==1 || memcmp(z, p->separator, nSep)==0)) ){
        i = 0;
        break;
      }
    }
    if( i==0 ){
      putc('"', out);
      for(i=0; z[i]; i++){
        if( z[i]=='"' ) putc('"', out);
        putc(z[i], out);
      }
      putc('"', out);
    }else{
      fprintf(out, "%s", z);
    }
  }
  if( bSep ){
    fprintf(p->out, "%s", p->separator);
  }
}

#ifdef SIGINT
/*
** This routine runs when the user presses Ctrl-C
*/
static void interrupt_handler(int NotUsed){
  UNUSED_PARAMETER(NotUsed);
  seenInterrupt = 1;
  if( db ) sqlite3_interrupt(db);
}
#endif

/*
** This is the callback routine that the shell
** invokes for each row of a query result.
*/
static int shell_callback(void *pArg, int nArg, char **azArg, char **azCol, int *aiType){
  int i;
  struct callback_data *p = (struct callback_data*)pArg;

  if( p->echoOn && p->cnt==0  && p->pStmt){
    printf("%s\n", sqlite3_sql(p->pStmt));
  }

  switch( p->mode ){
    case MODE_Line: {
      int w = 5;
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int len = strlen30(azCol[i] ? azCol[i] : "");
        if( len>w ) w = len;
      }
      if( p->cnt++>0 ) fprintf(p->out,"\n");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"%*s = %s\n", w, azCol[i],
                azArg[i] ? azArg[i] : p->nullvalue);
      }
      break;
    }
    case MODE_Explain:
    case MODE_Column: {
      if( p->cnt++==0 ){
        for(i=0; i<nArg; i++){
          int w, n;
          if( i<ArraySize(p->colWidth) ){
            w = p->colWidth[i];
          }else{
            w = 0;
          }
          if( w<=0 ){
            w = strlen30(azCol[i] ? azCol[i] : "");
            if( w<10 ) w = 10;
            n = strlen30(azArg && azArg[i] ? azArg[i] : p->nullvalue);
            if( w<n ) w = n;
          }
          if( i<ArraySize(p->actualWidth) ){
            p->actualWidth[i] = w;
          }
          if( p->showHeader ){
            fprintf(p->out,"%-*.*s%s",w,w,azCol[i], i==nArg-1 ? "\n": "  ");
          }
        }
        if( p->showHeader ){
          for(i=0; i<nArg; i++){
            int w;
            if( i<ArraySize(p->actualWidth) ){
               w = p->actualWidth[i];
            }else{
               w = 10;
            }
            fprintf(p->out,"%-*.*s%s",w,w,"-----------------------------------"
                   "----------------------------------------------------------",
                    i==nArg-1 ? "\n": "  ");
          }
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int w;
        if( i<ArraySize(p->actualWidth) ){
           w = p->actualWidth[i];
        }else{
           w = 10;
        }
        if( p->mode==MODE_Explain && azArg[i] && 
           strlen30(azArg[i])>w ){
          w = strlen30(azArg[i]);
        }
        fprintf(p->out,"%-*.*s%s",w,w,
            azArg[i] ? azArg[i] : p->nullvalue, i==nArg-1 ? "\n": "  ");
      }
      break;
    }
    case MODE_Semi:
    case MODE_List: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          fprintf(p->out,"%s%s",azCol[i], i==nArg-1 ? "\n" : p->separator);
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        char *z = azArg[i];
        if( z==0 ) z = p->nullvalue;
        fprintf(p->out, "%s", z);
        if( i<nArg-1 ){
          fprintf(p->out, "%s", p->separator);
        }else if( p->mode==MODE_Semi ){
          fprintf(p->out, ";\n");
        }else{
          fprintf(p->out, "\n");
        }
      }
      break;
    }
    case MODE_Html: {
      if( p->cnt++==0 && p->showHeader ){
        fprintf(p->out,"<TR>");
        for(i=0; i<nArg; i++){
          fprintf(p->out,"<TH>");
          output_html_string(p->out, azCol[i]);
          fprintf(p->out,"</TH>\n");
        }
        fprintf(p->out,"</TR>\n");
      }
      if( azArg==0 ) break;
      fprintf(p->out,"<TR>");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"<TD>");
        output_html_string(p->out, azArg[i] ? azArg[i] : p->nullvalue);
        fprintf(p->out,"</TD>\n");
      }
      fprintf(p->out,"</TR>\n");
      break;
    }
    case MODE_Tcl: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          output_c_string(p->out,azCol[i] ? azCol[i] : "");
          fprintf(p->out, "%s", p->separator);
        }
        fprintf(p->out,"\n");
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        output_c_string(p->out, azArg[i] ? azArg[i] : p->nullvalue);
        fprintf(p->out, "%s", p->separator);
      }
      fprintf(p->out,"\n");
      break;
    }
    case MODE_Csv: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          output_csv(p, azCol[i] ? azCol[i] : "", i<nArg-1);
        }
        fprintf(p->out,"\n");
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        output_csv(p, azArg[i], i<nArg-1);
      }
      fprintf(p->out,"\n");
      break;
    }
    case MODE_Insert: {
      p->cnt++;
      if( azArg==0 ) break;
      fprintf(p->out,"INSERT INTO %s VALUES(",p->zDestTable);
      for(i=0; i<nArg; i++){
        char *zSep = i>0 ? ",": "";
        if( (azArg[i]==0) || (aiType && aiType[i]==SQLITE_NULL) ){
          fprintf(p->out,"%sNULL",zSep);
        }else if( aiType && aiType[i]==SQLITE_TEXT ){
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_quoted_string(p->out, azArg[i]);
        }else if( aiType && (aiType[i]==SQLITE_INTEGER || aiType[i]==SQLITE_FLOAT) ){
          fprintf(p->out,"%s%s",zSep, azArg[i]);
        }else if( aiType && aiType[i]==SQLITE_BLOB && p->pStmt ){
          const void *pBlob = sqlite3_column_blob(p->pStmt, i);
          int nBlob = sqlite3_column_bytes(p->pStmt, i);
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_hex_blob(p->out, pBlob, nBlob);
        }else if( isNumber(azArg[i], 0) ){
          fprintf(p->out,"%s%s",zSep, azArg[i]);
        }else{
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_quoted_string(p->out, azArg[i]);
        }
      }
      fprintf(p->out,");\n");
      break;
    }
  }
  return 0;
}

/*
** This is the callback routine that the SQLite library
** invokes for each row of a query result.
*/
static int callback(void *pArg, int nArg, char **azArg, char **azCol){
  /* since we don't have type info, call the shell_callback with a NULL value */
  return shell_callback(pArg, nArg, azArg, azCol, NULL);
}

/*
** Set the destination table field of the callback_data structure to
** the name of the table given.  Escape any quote characters in the
** table name.
*/
static void set_table_name(struct callback_data *p, const char *zName){
  int i, n;
  int needQuote;
  char *z;

  if( p->zDestTable ){
    free(p->zDestTable);
    p->zDestTable = 0;
  }
  if( zName==0 ) return;
  needQuote = !isalpha((unsigned char)*zName) && *zName!='_';
  for(i=n=0; zName[i]; i++, n++){
    if( !isalnum((unsigned char)zName[i]) && zName[i]!='_' ){
      needQuote = 1;
      if( zName[i]=='\'' ) n++;
    }
  }
  if( needQuote ) n += 2;
  z = p->zDestTable = malloc( n+1 );
  if( z==0 ){
    fprintf(stderr,"Error: out of memory\n");
    exit(1);
  }
  n = 0;
  if( needQuote ) z[n++] = '\'';
  for(i=0; zName[i]; i++){
    z[n++] = zName[i];
    if( zName[i]=='\'' ) z[n++] = '\'';
  }
  if( needQuote ) z[n++] = '\'';
  z[n] = 0;
}

/* zIn is either a pointer to a NULL-terminated string in memory obtained
** from malloc(), or a NULL pointer. The string pointed to by zAppend is
** added to zIn, and the result returned in memory obtained from malloc().
** zIn, if it was not NULL, is freed.
**
** If the third argument, quote, is not '\0', then it is used as a 
** quote character for zAppend.
*/
static char *appendText(char *zIn, char const *zAppend, char quote){
  int len;
  int i;
  int nAppend = strlen30(zAppend);
  int nIn = (zIn?strlen30(zIn):0);

  len = nAppend+nIn+1;
  if( quote ){
    len += 2;
    for(i=0; i<nAppend; i++){
      if( zAppend[i]==quote ) len++;
    }
  }

  zIn = (char *)realloc(zIn, len);
  if( !zIn ){
    return 0;
  }

  if( quote ){
    char *zCsr = &zIn[nIn];
    *zCsr++ = quote;
    for(i=0; i<nAppend; i++){
      *zCsr++ = zAppend[i];
      if( zAppend[i]==quote ) *zCsr++ = quote;
    }
    *zCsr++ = quote;
    *zCsr++ = '\0';
    assert( (zCsr-zIn)==len );
  }else{
    memcpy(&zIn[nIn], zAppend, nAppend);
    zIn[len-1] = '\0';
  }

  return zIn;
}


/*
** Execute a query statement that has a single result column.  Print
** that result column on a line by itself with a semicolon terminator.
**
** This is used, for example, to show the schema of the database by
** querying the SQLITE_MASTER table.
*/
static int run_table_dump_query(
  FILE *out,              /* Send output here */
  sqlite3 *db,            /* Database to query */
  const char *zSelect,    /* SELECT statement to extract content */
  const char *zFirstRow   /* Print before first row, if not NULL */
){
  sqlite3_stmt *pSelect;
  int rc;
  rc = sqlite3_prepare(db, zSelect, -1, &pSelect, 0);
  if( rc!=SQLITE_OK || !pSelect ){
    return rc;
  }
  rc = sqlite3_step(pSelect);
  while( rc==SQLITE_ROW ){
    if( zFirstRow ){
      fprintf(out, "%s", zFirstRow);
      zFirstRow = 0;
    }
    fprintf(out, "%s;\n", sqlite3_column_text(pSelect, 0));
    rc = sqlite3_step(pSelect);
  }
  return sqlite3_finalize(pSelect);
}

/*
** Allocate space and save off current error string.
*/
static char *save_err_msg(
  sqlite3 *db            /* Database to query */
){
  int nErrMsg = 1+strlen30(sqlite3_errmsg(db));
  char *zErrMsg = sqlite3_malloc(nErrMsg);
  if( zErrMsg ){
    memcpy(zErrMsg, sqlite3_errmsg(db), nErrMsg);
  }
  return zErrMsg;
}

/*
** Execute a statement or set of statements.  Print 
** any result rows/columns depending on the current mode 
** set via the supplied callback.
**
** This is very similar to SQLite's built-in sqlite3_exec() 
** function except it takes a slightly different callback 
** and callback data argument.
*/
static int shell_exec(
  sqlite3 *db,                                /* An open database */
  const char *zSql,                           /* SQL to be evaluated */
  int (*xCallback)(void*,int,char**,char**,int*),   /* Callback function */
                                              /* (not the same as sqlite3_exec) */
  struct callback_data *pArg,                 /* Pointer to struct callback_data */
  char **pzErrMsg                             /* Error msg written here */
){
  sqlite3_stmt *pStmt = NULL;     /* Statement to execute. */
  int rc = SQLITE_OK;             /* Return Code */
  const char *zLeftover;          /* Tail of unprocessed SQL */

  if( pzErrMsg ){
    *pzErrMsg = NULL;
  }

  while( zSql[0] && (SQLITE_OK == rc) ){
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zLeftover);
    if( SQLITE_OK != rc ){
      if( pzErrMsg ){
        *pzErrMsg = save_err_msg(db);
      }
    }else{
      if( !pStmt ){
        /* this happens for a comment or white-space */
        zSql = zLeftover;
        while( isspace(zSql[0]) ) zSql++;
        continue;
      }

      /* perform the first step.  this will tell us if we
      ** have a result set or not and how wide it is.
      */
      rc = sqlite3_step(pStmt);
      /* if we have a result set... */
      if( SQLITE_ROW == rc ){
        /* if we have a callback... */
        if( xCallback ){
          /* allocate space for col name ptr, value ptr, and type */
          int nCol = sqlite3_column_count(pStmt);
          void *pData = sqlite3_malloc(3*nCol*sizeof(const char*) + 1);
          if( !pData ){
            rc = SQLITE_NOMEM;
          }else{
            char **azCols = (char **)pData;      /* Names of result columns */
            char **azVals = &azCols[nCol];       /* Results */
            int *aiTypes = (int *)&azVals[nCol]; /* Result types */
            int i;
            assert(sizeof(int) <= sizeof(char *)); 
            /* save off ptrs to column names */
            for(i=0; i<nCol; i++){
              azCols[i] = (char *)sqlite3_column_name(pStmt, i);
            }
            /* save off the prepared statment handle and reset row count */
            if( pArg ){
              pArg->pStmt = pStmt;
              pArg->cnt = 0;
            }
            do{
              /* extract the data and data types */
              for(i=0; i<nCol; i++){
                azVals[i] = (char *)sqlite3_column_text(pStmt, i);
                aiTypes[i] = sqlite3_column_type(pStmt, i);
                if( !azVals[i] && (aiTypes[i]!=SQLITE_NULL) ){
                  rc = SQLITE_NOMEM;
                  break; /* from for */
                }
              } /* end for */

              /* if data and types extracted successfully... */
              if( SQLITE_ROW == rc ){ 
                /* call the supplied callback with the result row data */
                if( xCallback(pArg, nCol, azVals, azCols, aiTypes) ){
                  rc = SQLITE_ABORT;
                }else{
                  rc = sqlite3_step(pStmt);
                }
              }
            } while( SQLITE_ROW == rc );
            sqlite3_free(pData);
            if( pArg ){
              pArg->pStmt = NULL;
            }
          }
        }else{
          do{
            rc = sqlite3_step(pStmt);
          } while( rc == SQLITE_ROW );
        }
      }

      /* Finalize the statement just executed. If this fails, save a 
      ** copy of the error message. Otherwise, set zSql to point to the
      ** next statement to execute. */
      rc = sqlite3_finalize(pStmt);
      if( rc==SQLITE_OK ){
        zSql = zLeftover;
        while( isspace(zSql[0]) ) zSql++;
      }else if( pzErrMsg ){
        *pzErrMsg = save_err_msg(db);
      }
    }
  } /* end while */

  return rc;
}


/*
** This is a different callback routine used for dumping the database.
** Each row received by this callback consists of a table name,
** the table type ("index" or "table") and SQL to create the table.
** This routine should print text sufficient to recreate the table.
*/
static int dump_callback(void *pArg, int nArg, char **azArg, char **azCol){
  int rc;
  const char *zTable;
  const char *zType;
  const char *zSql;
  const char *zPrepStmt = 0;
  struct callback_data *p = (struct callback_data *)pArg;

  UNUSED_PARAMETER(azCol);
  if( nArg!=3 ) return 1;
  zTable = azArg[0];
  zType = azArg[1];
  zSql = azArg[2];
  
  if( strcmp(zTable, "sqlite_sequence")==0 ){
    zPrepStmt = "DELETE FROM sqlite_sequence;\n";
  }else if( strcmp(zTable, "sqlite_stat1")==0 ){
    fprintf(p->out, "ANALYZE sqlite_master;\n");
  }else if( strncmp(zTable, "sqlite_", 7)==0 ){
    return 0;
  }else if( strncmp(zSql, "CREATE VIRTUAL TABLE", 20)==0 ){
    char *zIns;
    if( !p->writableSchema ){
      fprintf(p->out, "PRAGMA writable_schema=ON;\n");
      p->writableSchema = 1;
    }
    zIns = sqlite3_mprintf(
       "INSERT INTO sqlite_master(type,name,tbl_name,rootpage,sql)"
       "VALUES('table','%q','%q',0,'%q');",
       zTable, zTable, zSql);
    fprintf(p->out, "%s\n", zIns);
    sqlite3_free(zIns);
    return 0;
  }else{
    fprintf(p->out, "%s;\n", zSql);
  }

  if( strcmp(zType, "table")==0 ){
    sqlite3_stmt *pTableInfo = 0;
    char *zSelect = 0;
    char *zTableInfo = 0;
    char *zTmp = 0;
    int nRow = 0;
   
    zTableInfo = appendText(zTableInfo, "PRAGMA table_info(", 0);
    zTableInfo = appendText(zTableInfo, zTable, '"');
    zTableInfo = appendText(zTableInfo, ");", 0);

    rc = sqlite3_prepare(p->db, zTableInfo, -1, &pTableInfo, 0);
    free(zTableInfo);
    if( rc!=SQLITE_OK || !pTableInfo ){
      return 1;
    }

    zSelect = appendText(zSelect, "SELECT 'INSERT INTO ' || ", 0);
    zTmp = appendText(zTmp, zTable, '"');
    if( zTmp ){
      zSelect = appendText(zSelect, zTmp, '\'');
    }
    zSelect = appendText(zSelect, " || ' VALUES(' || ", 0);
    rc = sqlite3_step(pTableInfo);
    while( rc==SQLITE_ROW ){
      const char *zText = (const char *)sqlite3_column_text(pTableInfo, 1);
      zSelect = appendText(zSelect, "quote(", 0);
      zSelect = appendText(zSelect, zText, '"');
      rc = sqlite3_step(pTableInfo);
      if( rc==SQLITE_ROW ){
        zSelect = appendText(zSelect, ") || ',' || ", 0);
      }else{
        zSelect = appendText(zSelect, ") ", 0);
      }
      nRow++;
    }
    rc = sqlite3_finalize(pTableInfo);
    if( rc!=SQLITE_OK || nRow==0 ){
      free(zSelect);
      return 1;
    }
    zSelect = appendText(zSelect, "|| ')' FROM  ", 0);
    zSelect = appendText(zSelect, zTable, '"');

    rc = run_table_dump_query(p->out, p->db, zSelect, zPrepStmt);
    if( rc==SQLITE_CORRUPT ){
      zSelect = appendText(zSelect, " ORDER BY rowid DESC", 0);
      rc = run_table_dump_query(p->out, p->db, zSelect, 0);
    }
    if( zSelect ) free(zSelect);
  }
  return 0;
}

/*
** Run zQuery.  Use dump_callback() as the callback routine so that
** the contents of the query are output as SQL statements.
**
** If we get a SQLITE_CORRUPT error, rerun the query after appending
** "ORDER BY rowid DESC" to the end.
*/
static int run_schema_dump_query(
  struct callback_data *p, 
  const char *zQuery,
  char **pzErrMsg
){
  int rc;
  rc = sqlite3_exec(p->db, zQuery, dump_callback, p, pzErrMsg);
  if( rc==SQLITE_CORRUPT ){
    char *zQ2;
    int len = strlen30(zQuery);
    if( pzErrMsg ) sqlite3_free(*pzErrMsg);
    zQ2 = malloc( len+100 );
    if( zQ2==0 ) return rc;
    sqlite3_snprintf(sizeof(zQ2), zQ2, "%s ORDER BY rowid DESC", zQuery);
    rc = sqlite3_exec(p->db, zQ2, dump_callback, p, pzErrMsg);
    free(zQ2);
  }
  return rc;
}

#if !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined(SQLITE_OMIT_SUBQUERY)
struct GenfkeyCmd {
  sqlite3 *db;                   /* Database handle */
  struct callback_data *pCb;     /* Callback data */
  int isIgnoreErrors;            /* True for --ignore-errors */
  int isExec;                    /* True for --exec */
  int isNoDrop;                  /* True for --no-drop */
  int nErr;                      /* Number of errors seen so far */
};
typedef struct GenfkeyCmd GenfkeyCmd;

static int genfkeyParseArgs(GenfkeyCmd *p, char **azArg, int nArg){
  int ii;
  memset(p, 0, sizeof(GenfkeyCmd));

  for(ii=0; ii<nArg; ii++){
    int n = strlen30(azArg[ii]);

    if( n>2 && n<10 && 0==strncmp(azArg[ii], "--no-drop", n) ){
      p->isNoDrop = 1;
    }else if( n>2 && n<16 && 0==strncmp(azArg[ii], "--ignore-errors", n) ){
      p->isIgnoreErrors = 1;
    }else if( n>2 && n<7 && 0==strncmp(azArg[ii], "--exec", n) ){
      p->isExec = 1;
    }else{
      fprintf(stderr, "unknown option: %s\n", azArg[ii]);
      return -1;
    }
  }

  return SQLITE_OK;
}

static int genfkeyCmdCb(void *pCtx, int eType, const char *z){
  GenfkeyCmd *p = (GenfkeyCmd *)pCtx;
  if( eType==GENFKEY_ERROR && !p->isIgnoreErrors ){
    p->nErr++;
    fprintf(stderr, "%s\n", z);
  } 

  if( p->nErr==0 && (
        (eType==GENFKEY_CREATETRIGGER)
     || (eType==GENFKEY_DROPTRIGGER && !p->isNoDrop)
  )){
    if( p->isExec ){
      sqlite3_exec(p->db, z, 0, 0, 0);
    }else{
      char *zCol = "sql";
      callback((void *)p->pCb, 1, (char **)&z, (char **)&zCol);
    }
  }

  return SQLITE_OK;
}
#endif

/*
** Text of a help message
*/
static char zHelp[] =
  ".backup ?DB? FILE      Backup DB (default \"main\") to FILE\n"
  ".bail ON|OFF           Stop after hitting an error.  Default OFF\n"
  ".databases             List names and files of attached databases\n"
  ".dump ?TABLE? ...      Dump the database in an SQL text format\n"
  "                         If TABLE specified, only dump tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".echo ON|OFF           Turn command echo on or off\n"
  ".exit                  Exit this program\n"
  ".explain ?ON|OFF?      Turn output mode suitable for EXPLAIN on or off.\n"
  "                         With no args, it turns EXPLAIN on.\n"
#if !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined(SQLITE_OMIT_SUBQUERY)
  ".genfkey ?OPTIONS?     Options are:\n"
  "                         --no-drop: Do not drop old fkey triggers.\n"
  "                         --ignore-errors: Ignore tables with fkey errors\n"
  "                         --exec: Execute generated SQL immediately\n"
  "                       See file tool/genfkey.README in the source \n"
  "                       distribution for further information.\n"
#endif
  ".header(s) ON|OFF      Turn display of headers on or off\n"
  ".help                  Show this message\n"
  ".import FILE TABLE     Import data from FILE into TABLE\n"
  ".indices ?TABLE?       Show names of all indices\n"
  "                         If TABLE specified, only show indices for tables\n"
  "                         matching LIKE pattern TABLE.\n"
#ifdef SQLITE_ENABLE_IOTRACE
  ".iotrace FILE          Enable I/O diagnostic logging to FILE\n"
#endif
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  ".load FILE ?ENTRY?     Load an extension library\n"
#endif
  ".log FILE|off          Turn logging on or off.  FILE can be stderr/stdout\n"
  ".mode MODE ?TABLE?     Set output mode where MODE is one of:\n"
  "                         csv      Comma-separated values\n"
  "                         column   Left-aligned columns.  (See .width)\n"
  "                         html     HTML <table> code\n"
  "                         insert   SQL insert statements for TABLE\n"
  "                         line     One value per line\n"
  "                         list     Values delimited by .separator string\n"
  "                         tabs     Tab-separated values\n"
  "                         tcl      TCL list elements\n"
  ".nullvalue STRING      Print STRING in place of NULL values\n"
  ".output FILENAME       Send output to FILENAME\n"
  ".output stdout         Send output to the screen\n"
  ".prompt MAIN CONTINUE  Replace the standard prompts\n"
  ".quit                  Exit this program\n"
  ".read FILENAME         Execute SQL in FILENAME\n"
  ".restore ?DB? FILE     Restore content of DB (default \"main\") from FILE\n"
  ".schema ?TABLE?        Show the CREATE statements\n"
  "                         If TABLE specified, only show tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".separator STRING      Change separator used by output mode and .import\n"
  ".show                  Show the current values for various settings\n"
  ".tables ?TABLE?        List names of tables\n"
  "                         If TABLE specified, only list tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".timeout MS            Try opening locked tables for MS milliseconds\n"
  ".width NUM1 NUM2 ...   Set column widths for \"column\" mode\n"
;

static char zTimerHelp[] =
  ".timer ON|OFF          Turn the CPU timer measurement on or off\n"
;

/* Forward reference */
static int process_input(struct callback_data *p, FILE *in);

/*
** Make sure the database is open.  If it is not, then open it.  If
** the database fails to open, print an error message and exit.
*/
static void open_db(struct callback_data *p){
  if( p->db==0 ){
    sqlite3_open(p->zDbFilename, &p->db);
    db = p->db;
    if( db && sqlite3_errcode(db)==SQLITE_OK ){
      sqlite3_create_function(db, "shellstatic", 0, SQLITE_UTF8, 0,
          shellstaticFunc, 0, 0);
    }
    if( db==0 || SQLITE_OK!=sqlite3_errcode(db) ){
      fprintf(stderr,"Error: unable to open database \"%s\": %s\n", 
          p->zDbFilename, sqlite3_errmsg(db));
      exit(1);
    }
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    sqlite3_enable_load_extension(p->db, 1);
#endif
  }
}

/*
** Do C-language style dequoting.
**
**    \t    -> tab
**    \n    -> newline
**    \r    -> carriage return
**    \NNN  -> ascii character NNN in octal
**    \\    -> backslash
*/
static void resolve_backslashes(char *z){
  int i, j;
  char c;
  for(i=j=0; (c = z[i])!=0; i++, j++){
    if( c=='\\' ){
      c = z[++i];
      if( c=='n' ){
        c = '\n';
      }else if( c=='t' ){
        c = '\t';
      }else if( c=='r' ){
        c = '\r';
      }else if( c>='0' && c<='7' ){
        c -= '0';
        if( z[i+1]>='0' && z[i+1]<='7' ){
          i++;
          c = (c<<3) + z[i] - '0';
          if( z[i+1]>='0' && z[i+1]<='7' ){
            i++;
            c = (c<<3) + z[i] - '0';
          }
        }
      }
    }
    z[j] = c;
  }
  z[j] = 0;
}

/*
** Interpret zArg as a boolean value.  Return either 0 or 1.
*/
static int booleanValue(char *zArg){
  int val = atoi(zArg);
  int j;
  for(j=0; zArg[j]; j++){
    zArg[j] = (char)tolower(zArg[j]);
  }
  if( strcmp(zArg,"on")==0 ){
    val = 1;
  }else if( strcmp(zArg,"yes")==0 ){
    val = 1;
  }
  return val;
}

/*
** If an input line begins with "." then invoke this routine to
** process that line.
**
** Return 1 on error, 2 to exit, and 0 otherwise.
*/
static int do_meta_command(char *zLine, struct callback_data *p){
  int i = 1;
  int nArg = 0;
  int n, c;
  int rc = 0;
  char *azArg[50];

  /* Parse the input line into tokens.
  */
  while( zLine[i] && nArg<ArraySize(azArg) ){
    while( isspace((unsigned char)zLine[i]) ){ i++; }
    if( zLine[i]==0 ) break;
    if( zLine[i]=='\'' || zLine[i]=='"' ){
      int delim = zLine[i++];
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && zLine[i]!=delim ){ i++; }
      if( zLine[i]==delim ){
        zLine[i++] = 0;
      }
      if( delim=='"' ) resolve_backslashes(azArg[nArg-1]);
    }else{
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && !isspace((unsigned char)zLine[i]) ){ i++; }
      if( zLine[i] ) zLine[i++] = 0;
      resolve_backslashes(azArg[nArg-1]);
    }
  }

  /* Process the input line.
  */
  if( nArg==0 ) return 0; /* no tokens, no error */
  n = strlen30(azArg[0]);
  c = azArg[0][0];
  if( c=='b' && n>=3 && strncmp(azArg[0], "backup", n)==0 && nArg>1 && nArg<4){
    const char *zDestFile;
    const char *zDb;
    sqlite3 *pDest;
    sqlite3_backup *pBackup;
    if( nArg==2 ){
      zDestFile = azArg[1];
      zDb = "main";
    }else{
      zDestFile = azArg[2];
      zDb = azArg[1];
    }
    rc = sqlite3_open(zDestFile, &pDest);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zDestFile);
      sqlite3_close(pDest);
      return 1;
    }
    open_db(p);
    pBackup = sqlite3_backup_init(pDest, "main", p->db, zDb);
    if( pBackup==0 ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(pDest));
      sqlite3_close(pDest);
      return 1;
    }
    while(  (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK ){}
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = 0;
    }else{
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(pDest));
      rc = 1;
    }
    sqlite3_close(pDest);
  }else

  if( c=='b' && n>=3 && strncmp(azArg[0], "bail", n)==0 && nArg>1 && nArg<3 ){
    bail_on_error = booleanValue(azArg[1]);
  }else

  if( c=='d' && n>1 && strncmp(azArg[0], "databases", n)==0 && nArg==1 ){
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 1;
    data.mode = MODE_Column;
    data.colWidth[0] = 3;
    data.colWidth[1] = 15;
    data.colWidth[2] = 58;
    data.cnt = 0;
    sqlite3_exec(p->db, "PRAGMA database_list; ", callback, &data, &zErrMsg);
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }
  }else

  if( c=='d' && strncmp(azArg[0], "dump", n)==0 && nArg<3 ){
    char *zErrMsg = 0;
    open_db(p);
    /* When playing back a "dump", the content might appear in an order
    ** which causes immediate foreign key constraints to be violated.
    ** So disable foreign-key constraint enforcement to prevent problems. */
    fprintf(p->out, "PRAGMA foreign_keys=OFF;\n");
    fprintf(p->out, "BEGIN TRANSACTION;\n");
    p->writableSchema = 0;
    sqlite3_exec(p->db, "PRAGMA writable_schema=ON", 0, 0, 0);
    if( nArg==1 ){
      run_schema_dump_query(p, 
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE sql NOT NULL AND type=='table' AND name!='sqlite_sequence'", 0
      );
      run_schema_dump_query(p, 
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE name=='sqlite_sequence'", 0
      );
      run_table_dump_query(p->out, p->db,
        "SELECT sql FROM sqlite_master "
        "WHERE sql NOT NULL AND type IN ('index','trigger','view')", 0
      );
    }else{
      int i;
      for(i=1; i<nArg; i++){
        zShellStatic = azArg[i];
        run_schema_dump_query(p,
          "SELECT name, type, sql FROM sqlite_master "
          "WHERE tbl_name LIKE shellstatic() AND type=='table'"
          "  AND sql NOT NULL", 0);
        run_table_dump_query(p->out, p->db,
          "SELECT sql FROM sqlite_master "
          "WHERE sql NOT NULL"
          "  AND type IN ('index','trigger','view')"
          "  AND tbl_name LIKE shellstatic()", 0
        );
        zShellStatic = 0;
      }
    }
    if( p->writableSchema ){
      fprintf(p->out, "PRAGMA writable_schema=OFF;\n");
      p->writableSchema = 0;
    }
    sqlite3_exec(p->db, "PRAGMA writable_schema=OFF", 0, 0, 0);
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
    }else{
      fprintf(p->out, "COMMIT;\n");
    }
  }else

  if( c=='e' && strncmp(azArg[0], "echo", n)==0 && nArg>1 && nArg<3 ){
    p->echoOn = booleanValue(azArg[1]);
  }else

  if( c=='e' && strncmp(azArg[0], "exit", n)==0  && nArg==1 ){
    rc = 2;
  }else

  if( c=='e' && strncmp(azArg[0], "explain", n)==0 && nArg<3 ){
    int val = nArg>=2 ? booleanValue(azArg[1]) : 1;
    if(val == 1) {
      if(!p->explainPrev.valid) {
        p->explainPrev.valid = 1;
        p->explainPrev.mode = p->mode;
        p->explainPrev.showHeader = p->showHeader;
        memcpy(p->explainPrev.colWidth,p->colWidth,sizeof(p->colWidth));
      }
      /* We could put this code under the !p->explainValid
      ** condition so that it does not execute if we are already in
      ** explain mode. However, always executing it allows us an easy
      ** was to reset to explain mode in case the user previously
      ** did an .explain followed by a .width, .mode or .header
      ** command.
      */
      p->mode = MODE_Explain;
      p->showHeader = 1;
      memset(p->colWidth,0,ArraySize(p->colWidth));
      p->colWidth[0] = 4;                  /* addr */
      p->colWidth[1] = 13;                 /* opcode */
      p->colWidth[2] = 4;                  /* P1 */
      p->colWidth[3] = 4;                  /* P2 */
      p->colWidth[4] = 4;                  /* P3 */
      p->colWidth[5] = 13;                 /* P4 */
      p->colWidth[6] = 2;                  /* P5 */
      p->colWidth[7] = 13;                  /* Comment */
    }else if (p->explainPrev.valid) {
      p->explainPrev.valid = 0;
      p->mode = p->explainPrev.mode;
      p->showHeader = p->explainPrev.showHeader;
      memcpy(p->colWidth,p->explainPrev.colWidth,sizeof(p->colWidth));
    }
  }else

#if !defined(SQLITE_OMIT_VIRTUALTABLE) && !defined(SQLITE_OMIT_SUBQUERY)
  if( c=='g' && strncmp(azArg[0], "genfkey", n)==0 ){
    GenfkeyCmd cmd;
    if( 0==genfkeyParseArgs(&cmd, &azArg[1], nArg-1) ){
      cmd.db = p->db;
      cmd.pCb = p;
      genfkey_create_triggers(p->db, "main", (void *)&cmd, genfkeyCmdCb);
    }
  }else
#endif

  if( c=='h' && (strncmp(azArg[0], "header", n)==0 ||
                 strncmp(azArg[0], "headers", n)==0) && nArg>1 && nArg<3 ){
    p->showHeader = booleanValue(azArg[1]);
  }else

  if( c=='h' && strncmp(azArg[0], "help", n)==0 ){
    fprintf(stderr,"%s",zHelp);
    if( HAS_TIMER ){
      fprintf(stderr,"%s",zTimerHelp);
    }
  }else

  if( c=='i' && strncmp(azArg[0], "import", n)==0 && nArg==3 ){
    char *zTable = azArg[2];    /* Insert data into this table */
    char *zFile = azArg[1];     /* The file from which to extract data */
    sqlite3_stmt *pStmt = NULL; /* A statement */
    int nCol;                   /* Number of columns in the table */
    int nByte;                  /* Number of bytes in an SQL string */
    int i, j;                   /* Loop counters */
    int nSep;                   /* Number of bytes in p->separator[] */
    char *zSql;                 /* An SQL statement */
    char *zLine;                /* A single line of input from the file */
    char **azCol;               /* zLine[] broken up into columns */
    char *zCommit;              /* How to commit changes */   
    FILE *in;                   /* The input file */
    int lineno = 0;             /* Line number of input file */

    open_db(p);
    nSep = strlen30(p->separator);
    if( nSep==0 ){
      fprintf(stderr, "Error: non-null separator required for import\n");
      return 1;
    }
    zSql = sqlite3_mprintf("SELECT * FROM '%q'", zTable);
    if( zSql==0 ){
      fprintf(stderr, "Error: out of memory\n");
      return 1;
    }
    nByte = strlen30(zSql);
    rc = sqlite3_prepare(p->db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if( rc ){
      if (pStmt) sqlite3_finalize(pStmt);
      fprintf(stderr,"Error: %s\n", sqlite3_errmsg(db));
      return 1;
    }
    nCol = sqlite3_column_count(pStmt);
    sqlite3_finalize(pStmt);
    pStmt = 0;
    if( nCol==0 ) return 0; /* no columns, no error */
    zSql = malloc( nByte + 20 + nCol*2 );
    if( zSql==0 ){
      fprintf(stderr, "Error: out of memory\n");
      return 1;
    }
    sqlite3_snprintf(nByte+20, zSql, "INSERT INTO '%q' VALUES(?", zTable);
    j = strlen30(zSql);
    for(i=1; i<nCol; i++){
      zSql[j++] = ',';
      zSql[j++] = '?';
    }
    zSql[j++] = ')';
    zSql[j] = 0;
    rc = sqlite3_prepare(p->db, zSql, -1, &pStmt, 0);
    free(zSql);
    if( rc ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(db));
      if (pStmt) sqlite3_finalize(pStmt);
      return 1;
    }
    in = fopen(zFile, "rb");
    if( in==0 ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zFile);
      sqlite3_finalize(pStmt);
      return 1;
    }
    azCol = malloc( sizeof(azCol[0])*(nCol+1) );
    if( azCol==0 ){
      fprintf(stderr, "Error: out of memory\n");
      fclose(in);
      sqlite3_finalize(pStmt);
      return 1;
    }
    sqlite3_exec(p->db, "BEGIN", 0, 0, 0);
    zCommit = "COMMIT";
    while( (zLine = local_getline(0, in))!=0 ){
      char *z;
      i = 0;
      lineno++;
      azCol[0] = zLine;
      for(i=0, z=zLine; *z && *z!='\n' && *z!='\r'; z++){
        if( *z==p->separator[0] && strncmp(z, p->separator, nSep)==0 ){
          *z = 0;
          i++;
          if( i<nCol ){
            azCol[i] = &z[nSep];
            z += nSep-1;
          }
        }
      } /* end for */
      *z = 0;
      if( i+1!=nCol ){
        fprintf(stderr,
                "Error: %s line %d: expected %d columns of data but found %d\n",
                zFile, lineno, nCol, i+1);
        zCommit = "ROLLBACK";
        free(zLine);
        rc = 1;
        break; /* from while */
      }
      for(i=0; i<nCol; i++){
        sqlite3_bind_text(pStmt, i+1, azCol[i], -1, SQLITE_STATIC);
      }
      sqlite3_step(pStmt);
      rc = sqlite3_reset(pStmt);
      free(zLine);
      if( rc!=SQLITE_OK ){
        fprintf(stderr,"Error: %s\n", sqlite3_errmsg(db));
        zCommit = "ROLLBACK";
        rc = 1;
        break; /* from while */
      }
    } /* end while */
    free(azCol);
    fclose(in);
    sqlite3_finalize(pStmt);
    sqlite3_exec(p->db, zCommit, 0, 0, 0);
  }else

  if( c=='i' && strncmp(azArg[0], "indices", n)==0 && nArg<3 ){
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_List;
    if( nArg==1 ){
      rc = sqlite3_exec(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name NOT LIKE 'sqlite_%' "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type='index' "
        "ORDER BY 1",
        callback, &data, &zErrMsg
      );
    }else{
      zShellStatic = azArg[1];
      rc = sqlite3_exec(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND tbl_name LIKE shellstatic() "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type='index' AND tbl_name LIKE shellstatic() "
        "ORDER BY 1",
        callback, &data, &zErrMsg
      );
      zShellStatic = 0;
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }else if( rc != SQLITE_OK ){
      fprintf(stderr,"Error: querying sqlite_master and sqlite_temp_master\n");
      rc = 1;
    }
  }else

#ifdef SQLITE_ENABLE_IOTRACE
  if( c=='i' && strncmp(azArg[0], "iotrace", n)==0 ){
    extern void (*sqlite3IoTrace)(const char*, ...);
    if( iotrace && iotrace!=stdout ) fclose(iotrace);
    iotrace = 0;
    if( nArg<2 ){
      sqlite3IoTrace = 0;
    }else if( strcmp(azArg[1], "-")==0 ){
      sqlite3IoTrace = iotracePrintf;
      iotrace = stdout;
    }else{
      iotrace = fopen(azArg[1], "w");
      if( iotrace==0 ){
        fprintf(stderr, "Error: cannot open \"%s\"\n", azArg[1]);
        sqlite3IoTrace = 0;
        rc = 1;
      }else{
        sqlite3IoTrace = iotracePrintf;
      }
    }
  }else
#endif

#ifndef SQLITE_OMIT_LOAD_EXTENSION
  if( c=='l' && strncmp(azArg[0], "load", n)==0 && nArg>=2 ){
    const char *zFile, *zProc;
    char *zErrMsg = 0;
    zFile = azArg[1];
    zProc = nArg>=3 ? azArg[2] : 0;
    open_db(p);
    rc = sqlite3_load_extension(p->db, zFile, zProc, &zErrMsg);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }
  }else
#endif

  if( c=='l' && strncmp(azArg[0], "log", n)==0 && nArg>=1 ){
    const char *zFile = azArg[1];
    if( p->pLog && p->pLog!=stdout && p->pLog!=stderr ){
      fclose(p->pLog);
      p->pLog = 0;
    }
    if( strcmp(zFile,"stdout")==0 ){
      p->pLog = stdout;
    }else if( strcmp(zFile, "stderr")==0 ){
      p->pLog = stderr;
    }else if( strcmp(zFile, "off")==0 ){
      p->pLog = 0;
    }else{
      p->pLog = fopen(zFile, "w");
      if( p->pLog==0 ){
        fprintf(stderr, "Error: cannot open \"%s\"\n", zFile);
      }
    }
  }else

  if( c=='m' && strncmp(azArg[0], "mode", n)==0 && nArg==2 ){
    int n2 = strlen30(azArg[1]);
    if( (n2==4 && strncmp(azArg[1],"line",n2)==0)
        ||
        (n2==5 && strncmp(azArg[1],"lines",n2)==0) ){
      p->mode = MODE_Line;
    }else if( (n2==6 && strncmp(azArg[1],"column",n2)==0)
              ||
              (n2==7 && strncmp(azArg[1],"columns",n2)==0) ){
      p->mode = MODE_Column;
    }else if( n2==4 && strncmp(azArg[1],"list",n2)==0 ){
      p->mode = MODE_List;
    }else if( n2==4 && strncmp(azArg[1],"html",n2)==0 ){
      p->mode = MODE_Html;
    }else if( n2==3 && strncmp(azArg[1],"tcl",n2)==0 ){
      p->mode = MODE_Tcl;
    }else if( n2==3 && strncmp(azArg[1],"csv",n2)==0 ){
      p->mode = MODE_Csv;
      sqlite3_snprintf(sizeof(p->separator), p->separator, ",");
    }else if( n2==4 && strncmp(azArg[1],"tabs",n2)==0 ){
      p->mode = MODE_List;
      sqlite3_snprintf(sizeof(p->separator), p->separator, "\t");
    }else if( n2==6 && strncmp(azArg[1],"insert",n2)==0 ){
      p->mode = MODE_Insert;
      set_table_name(p, "table");
    }else {
      fprintf(stderr,"Error: mode should be one of: "
         "column csv html insert line list tabs tcl\n");
      rc = 1;
    }
  }else

  if( c=='m' && strncmp(azArg[0], "mode", n)==0 && nArg==3 ){
    int n2 = strlen30(azArg[1]);
    if( n2==6 && strncmp(azArg[1],"insert",n2)==0 ){
      p->mode = MODE_Insert;
      set_table_name(p, azArg[2]);
    }else {
      fprintf(stderr, "Error: invalid arguments: "
        " \"%s\". Enter \".help\" for help\n", azArg[2]);
      rc = 1;
    }
  }else

  if( c=='n' && strncmp(azArg[0], "nullvalue", n)==0 && nArg==2 ) {
    sqlite3_snprintf(sizeof(p->nullvalue), p->nullvalue,
                     "%.*s", (int)ArraySize(p->nullvalue)-1, azArg[1]);
  }else

  if( c=='o' && strncmp(azArg[0], "output", n)==0 && nArg==2 ){
    if( p->out!=stdout ){
      fclose(p->out);
    }
    if( strcmp(azArg[1],"stdout")==0 ){
      p->out = stdout;
      sqlite3_snprintf(sizeof(p->outfile), p->outfile, "stdout");
    }else{
      p->out = fopen(azArg[1], "wb");
      if( p->out==0 ){
        fprintf(stderr,"Error: cannot write to \"%s\"\n", azArg[1]);
        p->out = stdout;
        rc = 1;
      } else {
         sqlite3_snprintf(sizeof(p->outfile), p->outfile, "%s", azArg[1]);
      }
    }
  }else

  if( c=='p' && strncmp(azArg[0], "prompt", n)==0 && (nArg==2 || nArg==3)){
    if( nArg >= 2) {
      strncpy(mainPrompt,azArg[1],(int)ArraySize(mainPrompt)-1);
    }
    if( nArg >= 3) {
      strncpy(continuePrompt,azArg[2],(int)ArraySize(continuePrompt)-1);
    }
  }else

  if( c=='q' && strncmp(azArg[0], "quit", n)==0 && nArg==1 ){
    rc = 2;
  }else

  if( c=='r' && n>=3 && strncmp(azArg[0], "read", n)==0 && nArg==2 ){
    FILE *alt = fopen(azArg[1], "rb");
    if( alt==0 ){
      fprintf(stderr,"Error: cannot open \"%s\"\n", azArg[1]);
      rc = 1;
    }else{
      rc = process_input(p, alt);
      fclose(alt);
    }
  }else

  if( c=='r' && n>=3 && strncmp(azArg[0], "restore", n)==0 && nArg>1 && nArg<4){
    const char *zSrcFile;
    const char *zDb;
    sqlite3 *pSrc;
    sqlite3_backup *pBackup;
    int nTimeout = 0;

    if( nArg==2 ){
      zSrcFile = azArg[1];
      zDb = "main";
    }else{
      zSrcFile = azArg[2];
      zDb = azArg[1];
    }
    rc = sqlite3_open(zSrcFile, &pSrc);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zSrcFile);
      sqlite3_close(pSrc);
      return 1;
    }
    open_db(p);
    pBackup = sqlite3_backup_init(p->db, zDb, pSrc, "main");
    if( pBackup==0 ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(p->db));
      sqlite3_close(pSrc);
      return 1;
    }
    while( (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK
          || rc==SQLITE_BUSY  ){
      if( rc==SQLITE_BUSY ){
        if( nTimeout++ >= 3 ) break;
        sqlite3_sleep(100);
      }
    }
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = 0;
    }else if( rc==SQLITE_BUSY || rc==SQLITE_LOCKED ){
      fprintf(stderr, "Error: source database is busy\n");
      rc = 1;
    }else{
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(p->db));
      rc = 1;
    }
    sqlite3_close(pSrc);
  }else

  if( c=='s' && strncmp(azArg[0], "schema", n)==0 && nArg<3 ){
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_Semi;
    if( nArg>1 ){
      int i;
      for(i=0; azArg[1][i]; i++) azArg[1][i] = (char)tolower(azArg[1][i]);
      if( strcmp(azArg[1],"sqlite_master")==0 ){
        char *new_argv[2], *new_colv[2];
        new_argv[0] = "CREATE TABLE sqlite_master (\n"
                      "  type text,\n"
                      "  name text,\n"
                      "  tbl_name text,\n"
                      "  rootpage integer,\n"
                      "  sql text\n"
                      ")";
        new_argv[1] = 0;
        new_colv[0] = "sql";
        new_colv[1] = 0;
        callback(&data, 1, new_argv, new_colv);
        rc = SQLITE_OK;
      }else if( strcmp(azArg[1],"sqlite_temp_master")==0 ){
        char *new_argv[2], *new_colv[2];
        new_argv[0] = "CREATE TEMP TABLE sqlite_temp_master (\n"
                      "  type text,\n"
                      "  name text,\n"
                      "  tbl_name text,\n"
                      "  rootpage integer,\n"
                      "  sql text\n"
                      ")";
        new_argv[1] = 0;
        new_colv[0] = "sql";
        new_colv[1] = 0;
        callback(&data, 1, new_argv, new_colv);
        rc = SQLITE_OK;
      }else{
        zShellStatic = azArg[1];
        rc = sqlite3_exec(p->db,
          "SELECT sql FROM "
          "  (SELECT sql sql, type type, tbl_name tbl_name, name name"
          "     FROM sqlite_master UNION ALL"
          "   SELECT sql, type, tbl_name, name FROM sqlite_temp_master) "
          "WHERE tbl_name LIKE shellstatic() AND type!='meta' AND sql NOTNULL "
          "ORDER BY substr(type,2,1), name",
          callback, &data, &zErrMsg);
        zShellStatic = 0;
      }
    }else{
      rc = sqlite3_exec(p->db,
         "SELECT sql FROM "
         "  (SELECT sql sql, type type, tbl_name tbl_name, name name"
         "     FROM sqlite_master UNION ALL"
         "   SELECT sql, type, tbl_name, name FROM sqlite_temp_master) "
         "WHERE type!='meta' AND sql NOTNULL AND name NOT LIKE 'sqlite_%'"
         "ORDER BY substr(type,2,1), name",
         callback, &data, &zErrMsg
      );
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }else if( rc != SQLITE_OK ){
      fprintf(stderr,"Error: querying schema information\n");
      rc = 1;
    }else{
      rc = 0;
    }
  }else

  if( c=='s' && strncmp(azArg[0], "separator", n)==0 && nArg==2 ){
    sqlite3_snprintf(sizeof(p->separator), p->separator,
                     "%.*s", (int)sizeof(p->separator)-1, azArg[1]);
  }else

  if( c=='s' && strncmp(azArg[0], "show", n)==0 && nArg==1 ){
    int i;
    fprintf(p->out,"%9.9s: %s\n","echo", p->echoOn ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","explain", p->explainPrev.valid ? "on" :"off");
    fprintf(p->out,"%9.9s: %s\n","headers", p->showHeader ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","mode", modeDescr[p->mode]);
    fprintf(p->out,"%9.9s: ", "nullvalue");
      output_c_string(p->out, p->nullvalue);
      fprintf(p->out, "\n");
    fprintf(p->out,"%9.9s: %s\n","output",
            strlen30(p->outfile) ? p->outfile : "stdout");
    fprintf(p->out,"%9.9s: ", "separator");
      output_c_string(p->out, p->separator);
      fprintf(p->out, "\n");
    fprintf(p->out,"%9.9s: ","width");
    for (i=0;i<(int)ArraySize(p->colWidth) && p->colWidth[i] != 0;i++) {
      fprintf(p->out,"%d ",p->colWidth[i]);
    }
    fprintf(p->out,"\n");
  }else

  if( c=='t' && n>1 && strncmp(azArg[0], "tables", n)==0 && nArg<3 ){
    char **azResult;
    int nRow;
    char *zErrMsg;
    open_db(p);
    if( nArg==1 ){
      rc = sqlite3_get_table(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type IN ('table','view') "
        "ORDER BY 1",
        &azResult, &nRow, 0, &zErrMsg
      );
    }else{
      zShellStatic = azArg[1];
      rc = sqlite3_get_table(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type IN ('table','view') AND name LIKE shellstatic() "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type IN ('table','view') AND name LIKE shellstatic() "
        "ORDER BY 1",
        &azResult, &nRow, 0, &zErrMsg
      );
      zShellStatic = 0;
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }else if( rc != SQLITE_OK ){
      fprintf(stderr,"Error: querying sqlite_master and sqlite_temp_master\n");
      rc = 1;
    }else{
      int len, maxlen = 0;
      int i, j;
      int nPrintCol, nPrintRow;
      for(i=1; i<=nRow; i++){
        if( azResult[i]==0 ) continue;
        len = strlen30(azResult[i]);
        if( len>maxlen ) maxlen = len;
      }
      nPrintCol = 80/(maxlen+2);
      if( nPrintCol<1 ) nPrintCol = 1;
      nPrintRow = (nRow + nPrintCol - 1)/nPrintCol;
      for(i=0; i<nPrintRow; i++){
        for(j=i+1; j<=nRow; j+=nPrintRow){
          char *zSp = j<=nPrintRow ? "" : "  ";
          printf("%s%-*s", zSp, maxlen, azResult[j] ? azResult[j] : "");
        }
        printf("\n");
      }
    }
    sqlite3_free_table(azResult);
  }else

  if( c=='t' && n>4 && strncmp(azArg[0], "timeout", n)==0 && nArg==2 ){
    open_db(p);
    sqlite3_busy_timeout(p->db, atoi(azArg[1]));
  }else
    
  if( HAS_TIMER && c=='t' && n>=5 && strncmp(azArg[0], "timer", n)==0 && nArg==2 ){
    enableTimer = booleanValue(azArg[1]);
  }else
  
  if( c=='w' && strncmp(azArg[0], "width", n)==0 && nArg>1 ){
    int j;
    assert( nArg<=ArraySize(azArg) );
    for(j=1; j<nArg && j<ArraySize(p->colWidth); j++){
      p->colWidth[j-1] = atoi(azArg[j]);
    }
  }else

  {
    fprintf(stderr, "Error: unknown command or invalid arguments: "
      " \"%s\". Enter \".help\" for help\n", azArg[0]);
    rc = 1;
  }

  return rc;
}

/*
** Return TRUE if a semicolon occurs anywhere in the first N characters
** of string z[].
*/
static int _contains_semicolon(const char *z, int N){
  int i;
  for(i=0; i<N; i++){  if( z[i]==';' ) return 1; }
  return 0;
}

/*
** Test to see if a line consists entirely of whitespace.
*/
static int _all_whitespace(const char *z){
  for(; *z; z++){
    if( isspace(*(unsigned char*)z) ) continue;
    if( *z=='/' && z[1]=='*' ){
      z += 2;
      while( *z && (*z!='*' || z[1]!='/') ){ z++; }
      if( *z==0 ) return 0;
      z++;
      continue;
    }
    if( *z=='-' && z[1]=='-' ){
      z += 2;
      while( *z && *z!='\n' ){ z++; }
      if( *z==0 ) return 1;
      continue;
    }
    return 0;
  }
  return 1;
}

/*
** Return TRUE if the line typed in is an SQL command terminator other
** than a semi-colon.  The SQL Server style "go" command is understood
** as is the Oracle "/".
*/
static int _is_command_terminator(const char *zLine){
  while( isspace(*(unsigned char*)zLine) ){ zLine++; };
  if( zLine[0]=='/' && _all_whitespace(&zLine[1]) ){
    return 1;  /* Oracle */
  }
  if( tolower(zLine[0])=='g' && tolower(zLine[1])=='o'
         && _all_whitespace(&zLine[2]) ){
    return 1;  /* SQL Server */
  }
  return 0;
}

/*
** Return true if zSql is a complete SQL statement.  Return false if it
** ends in the middle of a string literal or C-style comment.
*/
static int _is_complete(char *zSql, int nSql){
  int rc;
  if( zSql==0 ) return 1;
  zSql[nSql] = ';';
  zSql[nSql+1] = 0;
  rc = sqlite3_complete(zSql);
  zSql[nSql] = 0;
  return rc;
}

/*
** Read input from *in and process it.  If *in==0 then input
** is interactive - the user is typing it it.  Otherwise, input
** is coming from a file or device.  A prompt is issued and history
** is saved only if input is interactive.  An interrupt signal will
** cause this routine to exit immediately, unless input is interactive.
**
** Return the number of errors.
*/
static int process_input(struct callback_data *p, FILE *in){
  char *zLine = 0;
  char *zSql = 0;
  int nSql = 0;
  int nSqlPrior = 0;
  char *zErrMsg;
  int rc;
  int errCnt = 0;
  int lineno = 0;
  int startline = 0;

  while( errCnt==0 || !bail_on_error || (in==0 && stdin_is_interactive) ){
    fflush(p->out);
    free(zLine);
    zLine = one_input_line(zSql, in);
    if( zLine==0 ){
      break;  /* We have reached EOF */
    }
    if( seenInterrupt ){
      if( in!=0 ) break;
      seenInterrupt = 0;
    }
    lineno++;
    if( (zSql==0 || zSql[0]==0) && _all_whitespace(zLine) ) continue;
    if( zLine && zLine[0]=='.' && nSql==0 ){
      if( p->echoOn ) printf("%s\n", zLine);
      rc = do_meta_command(zLine, p);
      if( rc==2 ){ /* exit requested */
        break;
      }else if( rc ){
        errCnt++;
      }
      continue;
    }
    if( _is_command_terminator(zLine) && _is_complete(zSql, nSql) ){
      memcpy(zLine,";",2);
    }
    nSqlPrior = nSql;
    if( zSql==0 ){
      int i;
      for(i=0; zLine[i] && isspace((unsigned char)zLine[i]); i++){}
      if( zLine[i]!=0 ){
        nSql = strlen30(zLine);
        zSql = malloc( nSql+3 );
        if( zSql==0 ){
          fprintf(stderr, "Error: out of memory\n");
          exit(1);
        }
        memcpy(zSql, zLine, nSql+1);
        startline = lineno;
      }
    }else{
      int len = strlen30(zLine);
      zSql = realloc( zSql, nSql + len + 4 );
      if( zSql==0 ){
        fprintf(stderr,"Error: out of memory\n");
        exit(1);
      }
      zSql[nSql++] = '\n';
      memcpy(&zSql[nSql], zLine, len+1);
      nSql += len;
    }
    if( zSql && _contains_semicolon(&zSql[nSqlPrior], nSql-nSqlPrior)
                && sqlite3_complete(zSql) ){
      p->cnt = 0;
      open_db(p);
      BEGIN_TIMER;
      rc = shell_exec(p->db, zSql, shell_callback, p, &zErrMsg);
      END_TIMER;
      if( rc || zErrMsg ){
        char zPrefix[100];
        if( in!=0 || !stdin_is_interactive ){
          sqlite3_snprintf(sizeof(zPrefix), zPrefix, 
                           "Error: near line %d:", startline);
        }else{
          sqlite3_snprintf(sizeof(zPrefix), zPrefix, "Error:");
        }
        if( zErrMsg!=0 ){
          fprintf(stderr, "%s %s\n", zPrefix, zErrMsg);
          sqlite3_free(zErrMsg);
          zErrMsg = 0;
        }else{
          fprintf(stderr, "%s %s\n", zPrefix, sqlite3_errmsg(p->db));
        }
        errCnt++;
      }
      free(zSql);
      zSql = 0;
      nSql = 0;
    }
  }
  if( zSql ){
    if( !_all_whitespace(zSql) ) fprintf(stderr, "Error: incomplete SQL: %s\n", zSql);
    free(zSql);
  }
  free(zLine);
  return errCnt;
}

/*
** Return a pathname which is the user's home directory.  A
** 0 return indicates an error of some kind.  Space to hold the
** resulting string is obtained from malloc().  The calling
** function should free the result.
*/
static char *find_home_dir(void){
  char *home_dir = NULL;

#if !defined(_WIN32) && !defined(WIN32) && !defined(__OS2__) && !defined(_WIN32_WCE) && !defined(__RTP__) && !defined(_WRS_KERNEL)
  struct passwd *pwent;
  uid_t uid = getuid();
  if( (pwent=getpwuid(uid)) != NULL) {
    home_dir = pwent->pw_dir;
  }
#endif

#if defined(_WIN32_WCE)
  /* Windows CE (arm-wince-mingw32ce-gcc) does not provide getenv()
   */
  home_dir = strdup("/");
#else

#if defined(_WIN32) || defined(WIN32) || defined(__OS2__)
  if (!home_dir) {
    home_dir = getenv("USERPROFILE");
  }
#endif

  if (!home_dir) {
    home_dir = getenv("HOME");
  }

#if defined(_WIN32) || defined(WIN32) || defined(__OS2__)
  if (!home_dir) {
    char *zDrive, *zPath;
    int n;
    zDrive = getenv("HOMEDRIVE");
    zPath = getenv("HOMEPATH");
    if( zDrive && zPath ){
      n = strlen30(zDrive) + strlen30(zPath) + 1;
      home_dir = malloc( n );
      if( home_dir==0 ) return 0;
      sqlite3_snprintf(n, home_dir, "%s%s", zDrive, zPath);
      return home_dir;
    }
    home_dir = "c:\\";
  }
#endif

#endif /* !_WIN32_WCE */

  if( home_dir ){
    int n = strlen30(home_dir) + 1;
    char *z = malloc( n );
    if( z ) memcpy(z, home_dir, n);
    home_dir = z;
  }

  return home_dir;
}

/*
** Read input from the file given by sqliterc_override.  Or if that
** parameter is NULL, take input from ~/.sqliterc
**
** Returns the number of errors.
*/
static int process_sqliterc(
  struct callback_data *p,        /* Configuration data */
  const char *sqliterc_override   /* Name of config file. NULL to use default */
){
  char *home_dir = NULL;
  const char *sqliterc = sqliterc_override;
  char *zBuf = 0;
  FILE *in = NULL;
  int nBuf;
  int rc = 0;

  if (sqliterc == NULL) {
    home_dir = find_home_dir();
    if( home_dir==0 ){
#if !defined(__RTP__) && !defined(_WRS_KERNEL)
      fprintf(stderr,"%s: Error: cannot locate your home directory\n", Argv0);
#endif
      return 1;
    }
    nBuf = strlen30(home_dir) + 16;
    zBuf = malloc( nBuf );
    if( zBuf==0 ){
      fprintf(stderr,"%s: Error: out of memory\n",Argv0);
      return 1;
    }
    sqlite3_snprintf(nBuf, zBuf,"%s/.sqliterc",home_dir);
    free(home_dir);
    sqliterc = (const char*)zBuf;
  }
  in = fopen(sqliterc,"rb");
  if( in ){
    if( stdin_is_interactive ){
      fprintf(stderr,"-- Loading resources from %s\n",sqliterc);
    }
    rc = process_input(p,in);
    fclose(in);
  }
  free(zBuf);
  return rc;
}

/*
** Show available command line options
*/
static const char zOptions[] = 
  "   -help                show this message\n"
  "   -init filename       read/process named file\n"
  "   -echo                print commands before execution\n"
  "   -[no]header          turn headers on or off\n"
  "   -bail                stop after hitting an error\n"
  "   -interactive         force interactive I/O\n"
  "   -batch               force batch I/O\n"
  "   -column              set output mode to 'column'\n"
  "   -csv                 set output mode to 'csv'\n"
  "   -html                set output mode to HTML\n"
  "   -line                set output mode to 'line'\n"
  "   -list                set output mode to 'list'\n"
  "   -separator 'x'       set output field separator (|)\n"
  "   -nullvalue 'text'    set text string for NULL values\n"
  "   -version             show SQLite version\n"
;
static void usage(int showDetail){
  fprintf(stderr,
      "Usage: %s [OPTIONS] FILENAME [SQL]\n"  
      "FILENAME is the name of an SQLite database. A new database is created\n"
      "if the file does not previously exist.\n", Argv0);
  if( showDetail ){
    fprintf(stderr, "OPTIONS include:\n%s", zOptions);
  }else{
    fprintf(stderr, "Use the -help option for additional information\n");
  }
  exit(1);
}

/*
** Initialize the state information in data
*/
static void main_init(struct callback_data *data) {
  memset(data, 0, sizeof(*data));
  data->mode = MODE_List;
  memcpy(data->separator,"|", 2);
  data->showHeader = 0;
  sqlite3_config(SQLITE_CONFIG_LOG, shellLog, data);
  sqlite3_snprintf(sizeof(mainPrompt), mainPrompt,"sqlite> ");
  sqlite3_snprintf(sizeof(continuePrompt), continuePrompt,"   ...> ");
}

int main(int argc, char **argv){
  char *zErrMsg = 0;
  struct callback_data data;
  const char *zInitFile = 0;
  char *zFirstCmd = 0;
  int i;
  int rc = 0;

  Argv0 = argv[0];
  main_init(&data);
  stdin_is_interactive = isatty(0);

  /* Make sure we have a valid signal handler early, before anything
  ** else is done.
  */
#ifdef SIGINT
  signal(SIGINT, interrupt_handler);
#endif

  /* Do an initial pass through the command-line argument to locate
  ** the name of the database file, the name of the initialization file,
  ** and the first command to execute.
  */
  for(i=1; i<argc-1; i++){
    char *z;
    if( argv[i][0]!='-' ) break;
    z = argv[i];
    if( z[0]=='-' && z[1]=='-' ) z++;
    if( strcmp(argv[i],"-separator")==0 || strcmp(argv[i],"-nullvalue")==0 ){
      i++;
    }else if( strcmp(argv[i],"-init")==0 ){
      i++;
      zInitFile = argv[i];
    /* Need to check for batch mode here to so we can avoid printing
    ** informational messages (like from process_sqliterc) before 
    ** we do the actual processing of arguments later in a second pass.
    */
    }else if( strcmp(argv[i],"-batch")==0 ){
      stdin_is_interactive = 0;
    }
  }
  if( i<argc ){
#if defined(SQLITE_OS_OS2) && SQLITE_OS_OS2
    data.zDbFilename = (const char *)convertCpPathToUtf8( argv[i++] );
#else
    data.zDbFilename = argv[i++];
#endif
  }else{
#ifndef SQLITE_OMIT_MEMORYDB
    data.zDbFilename = ":memory:";
#else
    data.zDbFilename = 0;
#endif
  }
  if( i<argc ){
    zFirstCmd = argv[i++];
  }
  if( i<argc ){
    fprintf(stderr,"%s: Error: too many options: \"%s\"\n", Argv0, argv[i]);
    fprintf(stderr,"Use -help for a list of options.\n");
    return 1;
  }
  data.out = stdout;

#ifdef SQLITE_OMIT_MEMORYDB
  if( data.zDbFilename==0 ){
    fprintf(stderr,"%s: Error: no database filename specified\n", Argv0);
    return 1;
  }
#endif

  /* Go ahead and open the database file if it already exists.  If the
  ** file does not exist, delay opening it.  This prevents empty database
  ** files from being created if a user mistypes the database name argument
  ** to the sqlite command-line tool.
  */
  if( access(data.zDbFilename, 0)==0 ){
    open_db(&data);
  }

  /* Process the initialization file if there is one.  If no -init option
  ** is given on the command line, look for a file named ~/.sqliterc and
  ** try to process it.
  */
  rc = process_sqliterc(&data,zInitFile);
  if( rc>0 ){
    return rc;
  }

  /* Make a second pass through the command-line argument and set
  ** options.  This second pass is delayed until after the initialization
  ** file is processed so that the command-line arguments will override
  ** settings in the initialization file.
  */
  for(i=1; i<argc && argv[i][0]=='-'; i++){
    char *z = argv[i];
    if( z[1]=='-' ){ z++; }
    if( strcmp(z,"-init")==0 ){
      i++;
    }else if( strcmp(z,"-html")==0 ){
      data.mode = MODE_Html;
    }else if( strcmp(z,"-list")==0 ){
      data.mode = MODE_List;
    }else if( strcmp(z,"-line")==0 ){
      data.mode = MODE_Line;
    }else if( strcmp(z,"-column")==0 ){
      data.mode = MODE_Column;
    }else if( strcmp(z,"-csv")==0 ){
      data.mode = MODE_Csv;
      memcpy(data.separator,",",2);
    }else if( strcmp(z,"-separator")==0 ){
      i++;
      if(i>=argc){
        fprintf(stderr,"%s: Error: missing argument for option: %s\n", Argv0, z);
        fprintf(stderr,"Use -help for a list of options.\n");
        return 1;
      }
      sqlite3_snprintf(sizeof(data.separator), data.separator,
                       "%.*s",(int)sizeof(data.separator)-1,argv[i]);
    }else if( strcmp(z,"-nullvalue")==0 ){
      i++;
      if(i>=argc){
        fprintf(stderr,"%s: Error: missing argument for option: %s\n", Argv0, z);
        fprintf(stderr,"Use -help for a list of options.\n");
        return 1;
      }
      sqlite3_snprintf(sizeof(data.nullvalue), data.nullvalue,
                       "%.*s",(int)sizeof(data.nullvalue)-1,argv[i]);
    }else if( strcmp(z,"-header")==0 ){
      data.showHeader = 1;
    }else if( strcmp(z,"-noheader")==0 ){
      data.showHeader = 0;
    }else if( strcmp(z,"-echo")==0 ){
      data.echoOn = 1;
    }else if( strcmp(z,"-bail")==0 ){
      bail_on_error = 1;
    }else if( strcmp(z,"-version")==0 ){
      printf("%s\n", sqlite3_libversion());
      return 0;
    }else if( strcmp(z,"-interactive")==0 ){
      stdin_is_interactive = 1;
    }else if( strcmp(z,"-batch")==0 ){
      stdin_is_interactive = 0;
    }else if( strcmp(z,"-help")==0 || strcmp(z, "--help")==0 ){
      usage(1);
    }else{
      fprintf(stderr,"%s: Error: unknown option: %s\n", Argv0, z);
      fprintf(stderr,"Use -help for a list of options.\n");
      return 1;
    }
  }

  if( zFirstCmd ){
    /* Run just the command that follows the database name
    */
    if( zFirstCmd[0]=='.' ){
      rc = do_meta_command(zFirstCmd, &data);
      return rc;
    }else{
      open_db(&data);
      rc = shell_exec(data.db, zFirstCmd, shell_callback, &data, &zErrMsg);
      if( zErrMsg!=0 ){
        fprintf(stderr,"Error: %s\n", zErrMsg);
        return rc!=0 ? rc : 1;
      }else if( rc!=0 ){
        fprintf(stderr,"Error: unable to process SQL \"%s\"\n", zFirstCmd);
        return rc;
      }
    }
  }else{
    /* Run commands received from standard input
    */
    if( stdin_is_interactive ){
      char *zHome;
      char *zHistory = 0;
      int nHistory;
      printf(
        "SQLite version %s\n"
        "Enter \".help\" for instructions\n"
        "Enter SQL statements terminated with a \";\"\n",
        sqlite3_libversion()
      );
      zHome = find_home_dir();
      if( zHome ){
        nHistory = strlen30(zHome) + 20;
        if( (zHistory = malloc(nHistory))!=0 ){
          sqlite3_snprintf(nHistory, zHistory,"%s/.sqlite_history", zHome);
        }
      }
#if defined(HAVE_READLINE) && HAVE_READLINE==1
      if( zHistory ) read_history(zHistory);
#endif
      rc = process_input(&data, 0);
      if( zHistory ){
        stifle_history(100);
        write_history(zHistory);
        free(zHistory);
      }
      free(zHome);
    }else{
      rc = process_input(&data, stdin);
    }
  }
  set_table_name(&data, 0);
  if( db ){
    if( sqlite3_close(db)!=SQLITE_OK ){
      fprintf(stderr,"Error: cannot close database \"%s\"\n", sqlite3_errmsg(db));
      rc++;
    }
  }
  return rc;
}
