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
** Main file for the SQLite library.  The routines in this file
** implement the programmer interface to the library.  Routines in
** other files are for internal use by SQLite and should not be
** accessed by users of the library.
**
** $Id: main.c,v 1.190 2004/05/26 02:04:57 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "os.h"
#include <ctype.h>

/*
** A pointer to this structure is used to communicate information
** from sqlite3Init into the sqlite3InitCallback.
*/
typedef struct {
  sqlite *db;         /* The database being initialized */
  char **pzErrMsg;    /* Error message stored here */
} InitData;

/*
** The following constant value is used by the SQLITE3_BIGENDIAN and
** SQLITE3_LITTLEENDIAN macros.
*/
const int sqlite3one = 1;

/*
** Fill the InitData structure with an error message that indicates
** that the database is corrupt.
*/
static void corruptSchema(InitData *pData, const char *zExtra){
  sqlite3SetString(pData->pzErrMsg, "malformed database schema",
     zExtra!=0 && zExtra[0]!=0 ? " - " : (char*)0, zExtra, (char*)0);
}

/*
** This is the callback routine for the code that initializes the
** database.  See sqlite3Init() below for additional information.
**
** Each callback contains the following information:
**
**     argv[0] = "file-format" or "schema-cookie" or "table" or "index"
**     argv[1] = table or index name or meta statement type.
**     argv[2] = root page number for table or index.  NULL for meta.
**     argv[3] = SQL text for a CREATE TABLE or CREATE INDEX statement.
**     argv[4] = "1" for temporary files, "0" for main database, "2" or more
**               for auxiliary database files.
**
*/
static
int sqlite3InitCallback(void *pInit, int argc, char **argv, char **azColName){
  InitData *pData = (InitData*)pInit;
  int nErr = 0;

  assert( argc==5 );
  if( argv==0 ) return 0;   /* Might happen if EMPTY_RESULT_CALLBACKS are on */
  if( argv[0]==0 ){
    corruptSchema(pData, 0);
    return 1;
  }
  switch( argv[0][0] ){
    case 'v':
    case 'i':
    case 't': {  /* CREATE TABLE, CREATE INDEX, or CREATE VIEW statements */
      sqlite *db = pData->db;
      if( argv[2]==0 || argv[4]==0 ){
        corruptSchema(pData, 0);
        return 1;
      }
      if( argv[3] && argv[3][0] ){
        /* Call the parser to process a CREATE TABLE, INDEX or VIEW.
        ** But because db->init.busy is set to 1, no VDBE code is generated
        ** or executed.  All the parser does is build the internal data
        ** structures that describe the table, index, or view.
        */
        char *zErr;
        assert( db->init.busy );
        db->init.iDb = atoi(argv[4]);
        assert( db->init.iDb>=0 && db->init.iDb<db->nDb );
        db->init.newTnum = atoi(argv[2]);
        if( sqlite3_exec(db, argv[3], 0, 0, &zErr) ){
          corruptSchema(pData, zErr);
          sqlite3_freemem(zErr);
        }
        db->init.iDb = 0;
      }else{
        /* If the SQL column is blank it means this is an index that
        ** was created to be the PRIMARY KEY or to fulfill a UNIQUE
        ** constraint for a CREATE TABLE.  The index should have already
        ** been created when we processed the CREATE TABLE.  All we have
        ** to do here is record the root page number for that index.
        */
        int iDb;
        Index *pIndex;

        iDb = atoi(argv[4]);
        assert( iDb>=0 && iDb<db->nDb );
        pIndex = sqlite3FindIndex(db, argv[1], db->aDb[iDb].zName);
        if( pIndex==0 || pIndex->tnum!=0 ){
          /* This can occur if there exists an index on a TEMP table which
          ** has the same name as another index on a permanent index.  Since
          ** the permanent table is hidden by the TEMP table, we can also
          ** safely ignore the index on the permanent table.
          */
          /* Do Nothing */;
        }else{
          pIndex->tnum = atoi(argv[2]);
        }
      }
      break;
    }
    default: {
      /* This can not happen! */
      nErr = 1;
      assert( nErr==0 );
    }
  }
  return nErr;
}

/*
** Attempt to read the database schema and initialize internal
** data structures for a single database file.  The index of the
** database file is given by iDb.  iDb==0 is used for the main
** database.  iDb==1 should never be used.  iDb>=2 is used for
** auxiliary databases.  Return one of the SQLITE_ error codes to
** indicate success or failure.
*/
static int sqlite3InitOne(sqlite *db, int iDb, char **pzErrMsg){
  int rc;
  BtCursor *curMain;
  int size;
  Table *pTab;
  char *azArg[6];
  char zDbNum[30];
  int meta[10];
  InitData initData;

  /*
  ** The master database table has a structure like this
  */
  static char master_schema[] = 
     "CREATE TABLE sqlite_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;
  static char temp_master_schema[] = 
     "CREATE TEMP TABLE sqlite_temp_master(\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;

  /* The following SQL will read the schema from the master tables.
  */
  static char init_script1[] = 
     "SELECT type, name, rootpage, sql, 1 FROM sqlite_temp_master";
  static char init_script2[] = 
     "SELECT type, name, rootpage, sql, 0 FROM sqlite_master";

  assert( iDb>=0 && iDb!=1 && iDb<db->nDb );

  /* Construct the schema tables: sqlite_master and sqlite_temp_master
  */
  sqlite3SafetyOff(db);
  azArg[0] = "table";
  azArg[1] = MASTER_NAME;
  azArg[2] = "1";
  azArg[3] = master_schema;
  sprintf(zDbNum, "%d", iDb);
  azArg[4] = zDbNum;
  azArg[5] = 0;
  initData.db = db;
  initData.pzErrMsg = pzErrMsg;
  sqlite3InitCallback(&initData, 5, azArg, 0);
  pTab = sqlite3FindTable(db, MASTER_NAME, "main");
  if( pTab ){
    pTab->readOnly = 1;
  }
  if( iDb==0 ){
    azArg[1] = TEMP_MASTER_NAME;
    azArg[3] = temp_master_schema;
    azArg[4] = "1";
    sqlite3InitCallback(&initData, 5, azArg, 0);
    pTab = sqlite3FindTable(db, TEMP_MASTER_NAME, "temp");
    if( pTab ){
      pTab->readOnly = 1;
    }
  }
  sqlite3SafetyOn(db);

  /* Create a cursor to hold the database open
  */
  if( db->aDb[iDb].pBt==0 ) return SQLITE_OK;
  rc = sqlite3BtreeCursor(db->aDb[iDb].pBt, MASTER_ROOT, 0, 0, 0, &curMain);
  if( rc!=SQLITE_OK && rc!=SQLITE_EMPTY ){
    sqlite3SetString(pzErrMsg, sqlite3_error_string(rc), (char*)0);
    return rc;
  }

  /* Get the database meta information.
  **
  ** Meta values are as follows:
  **    meta[0]   Schema cookie.  Changes with each schema change.
  **    meta[1]   File format of schema layer.
  **    meta[2]   Size of the page cache.
  **    meta[3]   Synchronous setting.  1:off, 2:normal, 3:full
  **    meta[4]   Db text encoding. 1:UTF-8 2:UTF-16 LE 3:UTF-16 BE
  **    meta[5]   Pragma temp_store value.  See comments on BtreeFactory
  **    meta[6]
  **    meta[7]
  **    meta[8]
  **    meta[9]
  **
  ** Note: The hash defined TEXT_Utf* symbols in sqliteInt.h correspond to
  ** the possible values of meta[4].
  */
  if( rc==SQLITE_OK ){
    int i;
    for(i=0; rc==SQLITE_OK && i<sizeof(meta)/sizeof(meta[0]); i++){
      rc = sqlite3BtreeGetMeta(db->aDb[iDb].pBt, i+1, &meta[i]);
    }
    if( rc ){
      sqlite3SetString(pzErrMsg, sqlite3_error_string(rc), (char*)0);
      sqlite3BtreeCloseCursor(curMain);
      return rc;
    }
  }else{
    memset(meta, 0, sizeof(meta));
  }
  db->aDb[iDb].schema_cookie = meta[0];
  if( iDb==0 ){
    db->next_cookie = meta[0];
    db->file_format = meta[1];
    if( meta[4] ){
      /* If meta[4] is still zero, then we are opening a previously empty
      ** file. Leave db->enc to the default value set by the sqlite3_open()
      ** call in this case.
      */
      db->enc = (u8)meta[4];
    }
    size = meta[2];
    if( size==0 ){ size = MAX_PAGES; }
    db->cache_size = size;
    db->safety_level = meta[3];
    if( meta[5]>0 && meta[5]<=2 && db->temp_store==0 ){
      db->temp_store = meta[5];
    }
    if( db->safety_level==0 ) db->safety_level = 2;

    /*
    **  file_format==1    Version 3.0.0.
    */
    if( db->file_format==0 ){
      /* This happens if the database was initially empty */
      db->file_format = 1;
    }else if( db->file_format>1 ){
      sqlite3BtreeCloseCursor(curMain);
      sqlite3SetString(pzErrMsg, "unsupported file format", (char*)0);
      return SQLITE_ERROR;
    }
  }else if( db->file_format!=meta[1] ){
    if( meta[1]==0 ){
      sqlite3SetString(pzErrMsg, "cannot attach empty database: ",
         db->aDb[iDb].zName, (char*)0);
    }else{
      sqlite3SetString(pzErrMsg, "incompatible file format in auxiliary "
         "database: ", db->aDb[iDb].zName, (char*)0);
    }
    sqlite3BtreeClose(db->aDb[iDb].pBt);
    db->aDb[iDb].pBt = 0;
    return SQLITE_FORMAT;
  }
  sqlite3BtreeSetCacheSize(db->aDb[iDb].pBt, db->cache_size);
  sqlite3BtreeSetSafetyLevel(db->aDb[iDb].pBt, meta[3]==0 ? 2 : meta[3]);

  /* Read the schema information out of the schema tables
  */
  assert( db->init.busy );
  sqlite3SafetyOff(db);
  if( rc==SQLITE_EMPTY ){
    /* For an empty database, there is nothing to read */
    rc = SQLITE_OK;
  }else{
    if( iDb==0 ){
      /* This SQL statement tries to read the temp.* schema from the
      ** sqlite_temp_master table. It might return SQLITE_EMPTY. 
      */
      rc = sqlite3_exec(db, init_script1, sqlite3InitCallback, &initData, 0);
      if( rc==SQLITE_OK || rc==SQLITE_EMPTY ){
        rc = sqlite3_exec(db, init_script2, sqlite3InitCallback, &initData, 0);
      }
    }else{
      char *zSql = 0;
      sqlite3SetString(&zSql, 
         "SELECT type, name, rootpage, sql, ", zDbNum, " FROM \"",
         db->aDb[iDb].zName, "\".sqlite_master", (char*)0);
      rc = sqlite3_exec(db, zSql, sqlite3InitCallback, &initData, 0);
      sqliteFree(zSql);
    }
    sqlite3SafetyOn(db);
    sqlite3BtreeCloseCursor(curMain);
  }
  if( sqlite3_malloc_failed ){
    sqlite3SetString(pzErrMsg, "out of memory", (char*)0);
    rc = SQLITE_NOMEM;
    sqlite3ResetInternalSchema(db, 0);
  }
  if( rc==SQLITE_OK ){
    DbSetProperty(db, iDb, DB_SchemaLoaded);
    if( iDb==0 ){
      DbSetProperty(db, 1, DB_SchemaLoaded);
    }
  }else{
    sqlite3ResetInternalSchema(db, iDb);
  }
  return rc;
}

/*
** Initialize all database files - the main database file, the file
** used to store temporary tables, and any additional database files
** created using ATTACH statements.  Return a success code.  If an
** error occurs, write an error message into *pzErrMsg.
**
** After the database is initialized, the SQLITE_Initialized
** bit is set in the flags field of the sqlite structure.  An
** attempt is made to initialize the database as soon as it
** is opened.  If that fails (perhaps because another process
** has the sqlite_master table locked) than another attempt
** is made the first time the database is accessed.
*/
int sqlite3Init(sqlite *db, char **pzErrMsg){
  int i, rc;
  
  if( db->init.busy ) return SQLITE_OK;
  assert( (db->flags & SQLITE_Initialized)==0 );
  rc = SQLITE_OK;
  db->init.busy = 1;
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
    if( DbHasProperty(db, i, DB_SchemaLoaded) ) continue;
    assert( i!=1 );  /* Should have been initialized together with 0 */
    rc = sqlite3InitOne(db, i, pzErrMsg);
    if( rc ){
      sqlite3ResetInternalSchema(db, i);
    }
  }
  db->init.busy = 0;
  if( rc==SQLITE_OK ){
    db->flags |= SQLITE_Initialized;
    sqlite3CommitInternalChanges(db);
  }

  if( rc!=SQLITE_OK ){
    db->flags &= ~SQLITE_Initialized;
  }
  return rc;
}

/*
** The version of the library
*/
const char rcsid[] = "@(#) \044Id: SQLite version " SQLITE_VERSION " $";
const char sqlite3_version[] = SQLITE_VERSION;

/*
** Does the library expect data to be encoded as UTF-8 or iso8859?  The
** following global constant always lets us know.
*/
#ifdef SQLITE_UTF8
const char sqlite3_encoding[] = "UTF-8";
#else
const char sqlite3_encoding[] = "iso8859";
#endif

/*
** This is the default collating function named "BINARY" which is always
** available.
*/
static int binaryCollatingFunc(
  void *NotUsed,
  int nKey1, const void *pKey1,
  int nKey2, const void *pKey2
){
  int rc, n;
  n = nKey1<nKey2 ? nKey1 : nKey2;
  rc = memcmp(pKey1, pKey2, n);
  if( rc==0 ){
    rc = nKey1 - nKey2;
  }
  return rc;
}

/*
** Return the ROWID of the most recent insert
*/
int sqlite3_last_insert_rowid(sqlite *db){
  return db->lastRowid;
}

/*
** Return the number of changes in the most recent call to sqlite3_exec().
*/
int sqlite3_changes(sqlite *db){
  return db->nChange;
}

/*
** Return the number of changes produced by the last INSERT, UPDATE, or
** DELETE statement to complete execution. The count does not include
** changes due to SQL statements executed in trigger programs that were
** triggered by that statement
*/
int sqlite3_last_statement_changes(sqlite *db){
  return db->lsChange;
}

/*
** Close an existing SQLite database
*/
void sqlite3_close(sqlite *db){
  HashElem *i;
  int j;
  db->want_to_close = 1;
  if( sqlite3SafetyCheck(db) || sqlite3SafetyOn(db) ){
    /* printf("DID NOT CLOSE\n"); fflush(stdout); */
    return;
  }
  db->magic = SQLITE_MAGIC_CLOSED;
  for(j=0; j<db->nDb; j++){
    struct Db *pDb = &db->aDb[j];
    if( pDb->pBt ){
      sqlite3BtreeClose(pDb->pBt);
      pDb->pBt = 0;
    }
  }
  sqlite3ResetInternalSchema(db, 0);
  assert( db->nDb<=2 );
  assert( db->aDb==db->aDbStatic );
  for(i=sqliteHashFirst(&db->aFunc); i; i=sqliteHashNext(i)){
    FuncDef *pFunc, *pNext;
    for(pFunc = (FuncDef*)sqliteHashData(i); pFunc; pFunc=pNext){
      pNext = pFunc->pNext;
      sqliteFree(pFunc);
    }
  }
  sqlite3HashClear(&db->aFunc);
  sqlite3Error(db, SQLITE_OK, 0); /* Deallocates any cached error strings. */
  sqliteFree(db);
}

/*
** Rollback all database files.
*/
void sqlite3RollbackAll(sqlite *db){
  int i;
  for(i=0; i<db->nDb; i++){
    if( db->aDb[i].pBt ){
      sqlite3BtreeRollback(db->aDb[i].pBt);
      db->aDb[i].inTrans = 0;
    }
  }
  sqlite3ResetInternalSchema(db, 0);
  /* sqlite3RollbackInternalChanges(db); */
}

/*
** Return a static string that describes the kind of error specified in the
** argument.
*/
const char *sqlite3_error_string(int rc){
  const char *z;
  switch( rc ){
    case SQLITE_OK:         z = "not an error";                          break;
    case SQLITE_ERROR:      z = "SQL logic error or missing database";   break;
    case SQLITE_INTERNAL:   z = "internal SQLite implementation flaw";   break;
    case SQLITE_PERM:       z = "access permission denied";              break;
    case SQLITE_ABORT:      z = "callback requested query abort";        break;
    case SQLITE_BUSY:       z = "database is locked";                    break;
    case SQLITE_LOCKED:     z = "database table is locked";              break;
    case SQLITE_NOMEM:      z = "out of memory";                         break;
    case SQLITE_READONLY:   z = "attempt to write a readonly database";  break;
    case SQLITE_INTERRUPT:  z = "interrupted";                           break;
    case SQLITE_IOERR:      z = "disk I/O error";                        break;
    case SQLITE_CORRUPT:    z = "database disk image is malformed";      break;
    case SQLITE_NOTFOUND:   z = "table or record not found";             break;
    case SQLITE_FULL:       z = "database is full";                      break;
    case SQLITE_CANTOPEN:   z = "unable to open database file";          break;
    case SQLITE_PROTOCOL:   z = "database locking protocol failure";     break;
    case SQLITE_EMPTY:      z = "table contains no data";                break;
    case SQLITE_SCHEMA:     z = "database schema has changed";           break;
    case SQLITE_TOOBIG:     z = "too much data for one table row";       break;
    case SQLITE_CONSTRAINT: z = "constraint failed";                     break;
    case SQLITE_MISMATCH:   z = "datatype mismatch";                     break;
    case SQLITE_MISUSE:     z = "library routine called out of sequence";break;
    case SQLITE_NOLFS:      z = "kernel lacks large file support";       break;
    case SQLITE_AUTH:       z = "authorization denied";                  break;
    case SQLITE_FORMAT:     z = "auxiliary database format error";       break;
    case SQLITE_RANGE:      z = "bind index out of range";               break;
    case SQLITE_NOTADB:     z = "file is encrypted or is not a database";break;
    default:                z = "unknown error";                         break;
  }
  return z;
}

/*
** This routine implements a busy callback that sleeps and tries
** again until a timeout value is reached.  The timeout value is
** an integer number of milliseconds passed in as the first
** argument.
*/
static int sqliteDefaultBusyCallback(
 void *Timeout,           /* Maximum amount of time to wait */
 const char *NotUsed,     /* The name of the table that is busy */
 int count                /* Number of times table has been busy */
){
#if SQLITE_MIN_SLEEP_MS==1
  static const char delays[] =
     { 1, 2, 5, 10, 15, 20, 25, 25,  25,  50,  50,  50, 100};
  static const short int totals[] =
     { 0, 1, 3,  8, 18, 33, 53, 78, 103, 128, 178, 228, 287};
# define NDELAY (sizeof(delays)/sizeof(delays[0]))
  int timeout = (int)Timeout;
  int delay, prior;

  if( count <= NDELAY ){
    delay = delays[count-1];
    prior = totals[count-1];
  }else{
    delay = delays[NDELAY-1];
    prior = totals[NDELAY-1] + delay*(count-NDELAY-1);
  }
  if( prior + delay > timeout ){
    delay = timeout - prior;
    if( delay<=0 ) return 0;
  }
  sqlite3OsSleep(delay);
  return 1;
#else
  int timeout = (int)Timeout;
  if( (count+1)*1000 > timeout ){
    return 0;
  }
  sqlite3OsSleep(1000);
  return 1;
#endif
}

/*
** This routine sets the busy callback for an Sqlite database to the
** given callback function with the given argument.
*/
void sqlite3_busy_handler(
  sqlite *db,
  int (*xBusy)(void*,const char*,int),
  void *pArg
){
  db->xBusyCallback = xBusy;
  db->pBusyArg = pArg;
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** This routine sets the progress callback for an Sqlite database to the
** given callback function with the given argument. The progress callback will
** be invoked every nOps opcodes.
*/
void sqlite3_progress_handler(
  sqlite *db, 
  int nOps,
  int (*xProgress)(void*), 
  void *pArg
){
  if( nOps>0 ){
    db->xProgress = xProgress;
    db->nProgressOps = nOps;
    db->pProgressArg = pArg;
  }else{
    db->xProgress = 0;
    db->nProgressOps = 0;
    db->pProgressArg = 0;
  }
}
#endif


/*
** This routine installs a default busy handler that waits for the
** specified number of milliseconds before returning 0.
*/
void sqlite3_busy_timeout(sqlite *db, int ms){
  if( ms>0 ){
    sqlite3_busy_handler(db, sqliteDefaultBusyCallback, (void*)ms);
  }else{
    sqlite3_busy_handler(db, 0, 0);
  }
}

/*
** Cause any pending operation to stop at its earliest opportunity.
*/
void sqlite3_interrupt(sqlite *db){
  db->flags |= SQLITE_Interrupt;
}

/*
** Windows systems should call this routine to free memory that
** is returned in the in the errmsg parameter of sqlite3_open() when
** SQLite is a DLL.  For some reason, it does not work to call free()
** directly.
**
** Note that we need to call free() not sqliteFree() here, since every
** string that is exported from SQLite should have already passed through
** sqlite3StrRealloc().
*/
void sqlite3_freemem(void *p){ free(p); }

/*
** Windows systems need functions to call to return the sqlite3_version
** and sqlite3_encoding strings since they are unable to access constants
** within DLLs.
*/
const char *sqlite3_libversion(void){ return sqlite3_version; }
const char *sqlite3_libencoding(void){ return sqlite3_encoding; }

/*
** Create new user-defined functions.  The sqlite3_create_function()
** routine creates a regular function and sqlite3_create_aggregate()
** creates an aggregate function.
**
** Passing a NULL xFunc argument or NULL xStep and xFinalize arguments
** disables the function.  Calling sqlite3_create_function() with the
** same name and number of arguments as a prior call to
** sqlite3_create_aggregate() disables the prior call to
** sqlite3_create_aggregate(), and vice versa.
**
** If nArg is -1 it means that this function will accept any number
** of arguments, including 0.  The maximum allowed value of nArg is 127.
*/
int sqlite3_create_function(
  sqlite *db,          /* Add the function to this database connection */
  const char *zName,   /* Name of the function to add */
  int nArg,            /* Number of arguments */
  void (*xFunc)(sqlite3_context*,int,sqlite3_value **), /* The implementation */
  void *pUserData      /* User data */
){
  FuncDef *p;
  int nName;
  if( db==0 || zName==0 || sqlite3SafetyCheck(db) ) return 1;
  if( nArg<-1 || nArg>127 ) return 1;
  nName = strlen(zName);
  if( nName>255 ) return 1;
  p = sqlite3FindFunction(db, zName, nName, nArg, 1);
  if( p==0 ) return 1;
  p->xFunc = xFunc;
  p->xStep = 0;
  p->xFinalize = 0;
  p->pUserData = pUserData;
  return 0;
}
int sqlite3_create_aggregate(
  sqlite *db,          /* Add the function to this database connection */
  const char *zName,   /* Name of the function to add */
  int nArg,            /* Number of arguments */
  void (*xStep)(sqlite3_context*,int,sqlite3_value**), /* The step function */
  void (*xFinalize)(sqlite3_context*),              /* The finalizer */
  void *pUserData      /* User data */
){
  FuncDef *p;
  int nName;
  if( db==0 || zName==0 || sqlite3SafetyCheck(db) ) return 1;
  if( nArg<-1 || nArg>127 ) return 1;
  nName = strlen(zName);
  if( nName>255 ) return 1;
  p = sqlite3FindFunction(db, zName, nName, nArg, 1);
  if( p==0 ) return 1;
  p->xFunc = 0;
  p->xStep = xStep;
  p->xFinalize = xFinalize;
  p->pUserData = pUserData;
  return 0;
}

/*
** Change the datatype for all functions with a given name.  See the
** header comment for the prototype of this function in sqlite.h for
** additional information.
*/
int sqlite3_function_type(sqlite *db, const char *zName, int dataType){
  FuncDef *p = (FuncDef*)sqlite3HashFind(&db->aFunc, zName, strlen(zName));
  while( p ){
    p->dataType = dataType; 
    p = p->pNext;
  }
  return SQLITE_OK;
}

/*
** Register a trace function.  The pArg from the previously registered trace
** is returned.  
**
** A NULL trace function means that no tracing is executes.  A non-NULL
** trace is a pointer to a function that is invoked at the start of each
** sqlite3_exec().
*/
void *sqlite3_trace(sqlite *db, void (*xTrace)(void*,const char*), void *pArg){
  void *pOld = db->pTraceArg;
  db->xTrace = xTrace;
  db->pTraceArg = pArg;
  return pOld;
}

/*** EXPERIMENTAL ***
**
** Register a function to be invoked when a transaction comments.
** If either function returns non-zero, then the commit becomes a
** rollback.
*/
void *sqlite3_commit_hook(
  sqlite *db,               /* Attach the hook to this database */
  int (*xCallback)(void*),  /* Function to invoke on each commit */
  void *pArg                /* Argument to the function */
){
  void *pOld = db->pCommitArg;
  db->xCommitCallback = xCallback;
  db->pCommitArg = pArg;
  return pOld;
}


/*
** This routine is called to create a connection to a database BTree
** driver.  If zFilename is the name of a file, then that file is
** opened and used.  If zFilename is the magic name ":memory:" then
** the database is stored in memory (and is thus forgotten as soon as
** the connection is closed.)  If zFilename is NULL then the database
** is for temporary use only and is deleted as soon as the connection
** is closed.
**
** A temporary database can be either a disk file (that is automatically
** deleted when the file is closed) or a set of red-black trees held in memory,
** depending on the values of the TEMP_STORE compile-time macro and the
** db->temp_store variable, according to the following chart:
**
**       TEMP_STORE     db->temp_store     Location of temporary database
**       ----------     --------------     ------------------------------
**           0               any             file
**           1                1              file
**           1                2              memory
**           1                0              file
**           2                1              file
**           2                2              memory
**           2                0              memory
**           3               any             memory
*/
int sqlite3BtreeFactory(
  const sqlite *db,	    /* Main database when opening aux otherwise 0 */
  const char *zFilename,    /* Name of the file containing the BTree database */
  int omitJournal,          /* if TRUE then do not journal this file */
  int nCache,               /* How many pages in the page cache */
  Btree **ppBtree           /* Pointer to new Btree object written here */
){
  int btree_flags = 0;
  
  assert( ppBtree != 0);
  if( omitJournal ){
    btree_flags |= BTREE_OMIT_JOURNAL;
  }
  if( !zFilename ){
    btree_flags |= BTREE_MEMORY;
  }

  return sqlite3BtreeOpen(zFilename, ppBtree, nCache, btree_flags);
}

/*
** Return UTF-8 encoded English language explanation of the most recent
** error.
*/
const char *sqlite3_errmsg(sqlite3 *db){
  if( !db ){
    /* If db is NULL, then assume that a malloc() failed during an
    ** sqlite3_open() call.
    */
    return sqlite3_error_string(SQLITE_NOMEM);
  }
  if( db->zErrMsg ){
    return db->zErrMsg;
  }
  return sqlite3_error_string(db->errCode);
}

/*
** Return UTF-16 encoded English language explanation of the most recent
** error.
*/
const void *sqlite3_errmsg16(sqlite3 *db){
  if( !db ){
    /* If db is NULL, then assume that a malloc() failed during an
    ** sqlite3_open() call. We have a static version of the string 
    ** "out of memory" encoded using UTF-16 just for this purpose.
    **
    ** Because all the characters in the string are in the unicode
    ** range 0x00-0xFF, if we pad the big-endian string with a 
    ** zero byte, we can obtain the little-endian string with
    ** &big_endian[1].
    */
    static char outOfMemBe[] = {
      0, 'o', 0, 'u', 0, 't', 0, ' ', 
      0, 'o', 0, 'f', 0, ' ', 
      0, 'm', 0, 'e', 0, 'm', 0, 'o', 0, 'r', 0, 'y', 0, 0, 0
    };
    static char *outOfMemLe = &outOfMemBe[1];

    if( SQLITE3_BIGENDIAN ){
      return (void *)outOfMemBe;
    }else{
      return (void *)outOfMemLe;
    }
  }
  if( !db->zErrMsg16 ){
    char const *zErr8 = sqlite3_errmsg(db);
    if( SQLITE3_BIGENDIAN ){
      db->zErrMsg16 = sqlite3utf8to16be(zErr8, -1);
    }else{
      db->zErrMsg16 = sqlite3utf8to16le(zErr8, -1);
    }
  }
  return db->zErrMsg16;
}

int sqlite3_errcode(sqlite3 *db){
  return db->errCode;
}

/*
** Compile the UTF-8 encoded SQL statement zSql into a statement handle.
*/
int sqlite3_prepare(
  sqlite3 *db,              /* Database handle. */
  const char *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite3_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const char** pzTail       /* OUT: End of parsed string */
){
  Parse sParse;
  char *zErrMsg = 0;
  int rc = SQLITE_OK;

  if( sqlite3SafetyOn(db) ){
    rc = SQLITE_MISUSE;
    goto prepare_out;
  }

  if( !db->init.busy ){
    if( (db->flags & SQLITE_Initialized)==0 ){
      int cnt = 1;
      while( (rc = sqlite3Init(db, &zErrMsg))==SQLITE_BUSY
         && db->xBusyCallback
         && db->xBusyCallback(db->pBusyArg, "", cnt++)!=0 ){}
      if( rc!=SQLITE_OK ){
        goto prepare_out;
      }
      if( zErrMsg ){
        sqliteFree(zErrMsg);
        zErrMsg = 0;
      }
    }
  }
  assert( (db->flags & SQLITE_Initialized)!=0 || db->init.busy );

  if( db->pVdbe==0 ){ db->nChange = 0; }
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sqlite3RunParser(&sParse, zSql, &zErrMsg);

  if( db->xTrace && !db->init.busy ){
    /* Trace only the statment that was compiled.
    ** Make a copy of that part of the SQL string since zSQL is const
    ** and we must pass a zero terminated string to the trace function
    ** The copy is unnecessary if the tail pointer is pointing at the
    ** beginnig or end of the SQL string.
    */
    if( sParse.zTail && sParse.zTail!=zSql && *sParse.zTail ){
      char *tmpSql = sqliteStrNDup(zSql, sParse.zTail - zSql);
      if( tmpSql ){
        db->xTrace(db->pTraceArg, tmpSql);
        free(tmpSql);
      }else{
        /* If a memory error occurred during the copy,
        ** trace entire SQL string and fall through to the
        ** sqlite3_malloc_failed test to report the error.
        */
        db->xTrace(db->pTraceArg, zSql); 
      }
    }else{
      db->xTrace(db->pTraceArg, zSql); 
    }
  }

  if( sqlite3_malloc_failed ){
    rc = SQLITE_NOMEM;
    sqlite3RollbackAll(db);
    sqlite3ResetInternalSchema(db, 0);
    db->flags &= ~SQLITE_InTrans;
    goto prepare_out;
  }
  if( sParse.rc==SQLITE_DONE ) sParse.rc = SQLITE_OK;
  if( sParse.rc==SQLITE_SCHEMA ){
    sqlite3ResetInternalSchema(db, 0);
  }
  assert( ppStmt );
  *ppStmt = (sqlite3_stmt*)sParse.pVdbe;
  if( pzTail ) *pzTail = sParse.zTail;
  rc = sParse.rc;

  if( rc==SQLITE_OK && sParse.pVdbe && sParse.explain ){
    sqlite3VdbeSetNumCols(sParse.pVdbe, 5);
  } 

prepare_out:
  if( sqlite3SafetyOff(db) ){
    rc = SQLITE_MISUSE;
  }
  if( zErrMsg ){
    sqlite3Error(db, rc, "%s", zErrMsg);
  }else{
    sqlite3Error(db, rc, 0);
  }
  return rc;
}

/*
** Compile the UTF-16 encoded SQL statement zSql into a statement handle.
*/
int sqlite3_prepare16(
  sqlite3 *db,              /* Database handle. */ 
  const void *zSql,         /* UTF-8 encoded SQL statement. */
  int nBytes,               /* Length of zSql in bytes. */
  sqlite3_stmt **ppStmt,    /* OUT: A pointer to the prepared statement */
  const void **pzTail       /* OUT: End of parsed string */
){
  /* This function currently works by first transforming the UTF-16
  ** encoded string to UTF-8, then invoking sqlite3_prepare(). The
  ** tricky bit is figuring out the pointer to return in *pzTail.
  */
  char *zSql8 = 0;
  char const *zTail8 = 0;
  int rc;

  zSql8 = sqlite3utf16to8(zSql, nBytes, SQLITE3_BIGENDIAN);
  if( !zSql8 ){
    sqlite3Error(db, SQLITE_NOMEM, 0);
    return SQLITE_NOMEM;
  }
  rc = sqlite3_prepare(db, zSql8, -1, ppStmt, &zTail8);

  if( zTail8 && pzTail ){
    /* If sqlite3_prepare returns a tail pointer, we calculate the
    ** equivalent pointer into the UTF-16 string by counting the unicode
    ** characters between zSql8 and zTail8, and then returning a pointer
    ** the same number of characters into the UTF-16 string.
    */
    int chars_parsed = sqlite3utf8CharLen(zSql8, zTail8-zSql8);
    *pzTail = (u8 *)zSql + sqlite3utf16ByteLen(zSql, chars_parsed);
  }
 
  return rc;
}

/*
** This routine does the work of opening a database on behalf of
** sqlite3_open() and sqlite3_open16(). The database filename "zFilename"  
** is UTF-8 encoded. The fourth argument, "def_enc" is one of the TEXT_*
** macros from sqliteInt.h. If we end up creating a new database file
** (not opening an existing one), the text encoding of the database
** will be set to this value.
*/
static int openDatabase(
  const char *zFilename, /* Database filename UTF-8 encoded */
  sqlite3 **ppDb,        /* OUT: Returned database handle */
  const char **options,  /* Null terminated list of db options, or null */
  u8 def_enc             /* One of TEXT_Utf8, TEXT_Utf16le or TEXT_Utf16be */
){
  sqlite3 *db;
  int rc, i;
  char *zErrMsg = 0;

#ifdef SQLITE_TEST
  for(i=0; options && options[i]; i++){
    char const *zOpt = options[i];
    if( 0==sqlite3StrICmp(zOpt, "-utf8") ){
      def_enc = TEXT_Utf8;
    }else if( 0==sqlite3StrICmp(zOpt, "-utf16le") ){
      def_enc = TEXT_Utf16le;
    }else if( 0==sqlite3StrICmp(zOpt, "-utf16be") ){
      def_enc = TEXT_Utf16be;
    }
  }
#endif

  /* Allocate the sqlite data structure */
  db = sqliteMalloc( sizeof(sqlite) );
  if( db==0 ) goto opendb_out;
  db->onError = OE_Default;
  db->priorNewRowid = 0;
  db->magic = SQLITE_MAGIC_BUSY;
  db->nDb = 2;
  db->aDb = db->aDbStatic;
  db->enc = def_enc;
  /* db->flags |= SQLITE_ShortColNames; */
  sqlite3HashInit(&db->aFunc, SQLITE_HASH_STRING, 1);
  sqlite3HashInit(&db->aCollSeq, SQLITE_HASH_STRING, 0);
  for(i=0; i<db->nDb; i++){
    sqlite3HashInit(&db->aDb[i].tblHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].idxHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].trigHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].aFKey, SQLITE_HASH_STRING, 1);
  }
  db->pDfltColl =
     sqlite3ChangeCollatingFunction(db, "BINARY", 6, 0, binaryCollatingFunc);
  
  /* Open the backend database driver */
  if( zFilename[0]==':' && strcmp(zFilename,":memory:")==0 ){
    db->temp_store = 2;
  }
  rc = sqlite3BtreeFactory(db, zFilename, 0, MAX_PAGES, &db->aDb[0].pBt);
  if( rc!=SQLITE_OK ){
    /* FIX ME: sqlite3BtreeFactory() should call sqlite3Error(). */
    sqlite3Error(db, rc, 0);
    db->magic = SQLITE_MAGIC_CLOSED;
    goto opendb_out;
  }
  db->aDb[0].zName = "main";
  db->aDb[1].zName = "temp";

  /* Attempt to read the schema */
  sqlite3RegisterBuiltinFunctions(db);
  rc = sqlite3Init(db, &zErrMsg);
  if( sqlite3_malloc_failed ){
    sqlite3_close(db);
    db = 0;
    goto opendb_out;
  }else if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
    sqlite3Error(db, rc, "%s", zErrMsg, 0);
    db->magic = SQLITE_MAGIC_CLOSED;
  }else{
    db->magic = SQLITE_MAGIC_OPEN;
  }
  if( zErrMsg ) sqliteFree(zErrMsg);

opendb_out:
  *ppDb = db;
  return sqlite3_errcode(db);
}

/*
** Open a new database handle.
*/
int sqlite3_open(
  const char *zFilename, 
  sqlite3 **ppDb, 
  const char **options
){
  return openDatabase(zFilename, ppDb, options, TEXT_Utf8);
}

/*
** Open a new database handle.
*/
int sqlite3_open16(
  const void *zFilename, 
  sqlite3 **ppDb, 
  const char **options
){
  char *zFilename8;   /* zFilename encoded in UTF-8 instead of UTF-16 */
  int rc;

  assert( ppDb );

  zFilename8 = sqlite3utf16to8(zFilename, -1, SQLITE3_BIGENDIAN);
  if( !zFilename8 ){
    *ppDb = 0;
    return SQLITE_NOMEM;
  }

  /* FIX ME: Also need to translate the option strings */

  if( SQLITE3_BIGENDIAN ){
    rc = openDatabase(zFilename8, ppDb, options, TEXT_Utf16be);
  }else{
    rc = openDatabase(zFilename8, ppDb, options, TEXT_Utf16le);
  }

  sqliteFree(zFilename8);
  return rc;
}

/*
** The following routine destroys a virtual machine that is created by
** the sqlite3_compile() routine. The integer returned is an SQLITE_
** success/failure code that describes the result of executing the virtual
** machine.
**
** This routine sets the error code and string returned by
** sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16().
*/
int sqlite3_finalize(sqlite3_stmt *pStmt){
  return sqlite3VdbeFinalize((Vdbe*)pStmt, 0);
}

/*
** Terminate the current execution of an SQL statement and reset it
** back to its starting state so that it can be reused. A success code from
** the prior execution is returned.
**
** This routine sets the error code and string returned by
** sqlite3_errcode(), sqlite3_errmsg() and sqlite3_errmsg16().
*/
int sqlite3_reset(sqlite3_stmt *pStmt){
  int rc = sqlite3VdbeReset((Vdbe*)pStmt, 0);
  sqlite3VdbeMakeReady((Vdbe*)pStmt, -1, 0);
  return rc;
}
