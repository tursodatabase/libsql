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
** $Id: main.c,v 1.173 2004/05/11 09:31:32 drh Exp $
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
  static char init_script[] = 
     "SELECT type, name, rootpage, sql, 1 FROM sqlite_temp_master "
     "UNION ALL "
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
  **    meta[4]
  **    meta[5]   Pragma temp_store value.  See comments on BtreeFactory
  **    meta[6]
  **    meta[7]
  **    meta[8]
  **    meta[9]
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
      rc = sqlite3_exec(db, init_script, sqlite3InitCallback, &initData, 0);
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
** Open a new SQLite database.  Construct an "sqlite" structure to define
** the state of this database and return a pointer to that structure.
**
** An attempt is made to initialize the in-memory data structures that
** hold the database schema.  But if this fails (because the schema file
** is locked) then that step is deferred until the first call to
** sqlite3_exec().
*/
sqlite *sqlite3_open(const char *zFilename, int mode, char **pzErrMsg){
  sqlite *db;
  int rc, i;

  /* Allocate the sqlite data structure */
  db = sqliteMalloc( sizeof(sqlite) );
  if( pzErrMsg ) *pzErrMsg = 0;
  if( db==0 ) goto no_mem_on_open;
  db->onError = OE_Default;
  db->priorNewRowid = 0;
  db->magic = SQLITE_MAGIC_BUSY;
  db->nDb = 2;
  db->aDb = db->aDbStatic;
  /* db->flags |= SQLITE_ShortColNames; */
  sqlite3HashInit(&db->aFunc, SQLITE_HASH_STRING, 1);
  for(i=0; i<db->nDb; i++){
    sqlite3HashInit(&db->aDb[i].tblHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].idxHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].trigHash, SQLITE_HASH_STRING, 0);
    sqlite3HashInit(&db->aDb[i].aFKey, SQLITE_HASH_STRING, 1);
  }
  
  /* Open the backend database driver */
  if( zFilename[0]==':' && strcmp(zFilename,":memory:")==0 ){
    db->temp_store = 2;
  }
  rc = sqlite3BtreeFactory(db, zFilename, 0, MAX_PAGES, &db->aDb[0].pBt);
  if( rc!=SQLITE_OK ){
    switch( rc ){
      default: {
        sqlite3SetString(pzErrMsg, "unable to open database: ",
           zFilename, (char*)0);
      }
    }
    sqliteFree(db);
    sqlite3StrRealloc(pzErrMsg);
    return 0;
  }
  db->aDb[0].zName = "main";
  db->aDb[1].zName = "temp";

  /* Attempt to read the schema */
  sqlite3RegisterBuiltinFunctions(db);
  rc = sqlite3Init(db, pzErrMsg);
  db->magic = SQLITE_MAGIC_OPEN;
  if( sqlite3_malloc_failed ){
    sqlite3_close(db);
    goto no_mem_on_open;
  }else if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
    sqlite3_close(db);
    sqlite3StrRealloc(pzErrMsg);
    return 0;
  }else if( pzErrMsg ){
    sqliteFree(*pzErrMsg);
    *pzErrMsg = 0;
  }

  /* Return a pointer to the newly opened database structure */
  return db;

no_mem_on_open:
  sqlite3SetString(pzErrMsg, "out of memory", (char*)0);
  sqlite3StrRealloc(pzErrMsg);
  return 0;
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
** Execute SQL code.  Return one of the SQLITE_ success/failure
** codes.  Also write an error message into memory obtained from
** malloc() and make *pzErrMsg point to that message.
**
** If the SQL is a query, then for each row in the query result
** the xCallback() function is called.  pArg becomes the first
** argument to xCallback().  If xCallback=NULL then no callback
** is invoked, even for queries.
*/
int sqlite3_exec(
  sqlite *db,                 /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  sqlite_callback xCallback,  /* Invoke this callback routine */
  void *pArg,                 /* First argument to xCallback() */
  char **pzErrMsg             /* Write error messages here */
){
  int rc = SQLITE_OK;
  const char *zLeftover;
  sqlite_vm *pVm;
  int nRetry = 0;
  int nChange = 0;
  int nCallback;

  if( zSql==0 ) return SQLITE_OK;
  while( rc==SQLITE_OK && zSql[0] ){
    pVm = 0;
    rc = sqlite3_compile(db, zSql, &zLeftover, &pVm, pzErrMsg);
    if( rc!=SQLITE_OK ){
      assert( pVm==0 || sqlite3_malloc_failed );
      return rc;
    }
    if( pVm==0 ){
      /* This happens if the zSql input contained only whitespace */
      break;
    }
    db->nChange += nChange;
    nCallback = 0;
    while(1){
      int nArg;
      char **azArg, **azCol;
      rc = sqlite3_step(pVm, &nArg, (const char***)&azArg,(const char***)&azCol);
      if( rc==SQLITE_ROW ){
        if( xCallback!=0 && xCallback(pArg, nArg, azArg, azCol) ){
          sqlite3_finalize(pVm, 0);
          return SQLITE_ABORT;
        }
        nCallback++;
      }else{
        if( rc==SQLITE_DONE && nCallback==0
          && (db->flags & SQLITE_NullCallback)!=0 && xCallback!=0 ){
          xCallback(pArg, nArg, azArg, azCol);
        }
        rc = sqlite3_finalize(pVm, pzErrMsg);
        if( rc==SQLITE_SCHEMA && nRetry<2 ){
          nRetry++;
          rc = SQLITE_OK;
          break;
        }
        if( db->pVdbe==0 ){
          nChange = db->nChange;
        }
        nRetry = 0;
        zSql = zLeftover;
        while( isspace(zSql[0]) ) zSql++;
        break;
      }
    }
  }
  return rc;
}


/*
** Compile a single statement of SQL into a virtual machine.  Return one
** of the SQLITE_ success/failure codes.  Also write an error message into
** memory obtained from malloc() and make *pzErrMsg point to that message.
*/
int sqlite3_compile(
  sqlite *db,                 /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  const char **pzTail,        /* OUT: Next statement after the first */
  sqlite_vm **ppVm,           /* OUT: The virtual machine */
  char **pzErrMsg             /* OUT: Write error messages here */
){
  Parse sParse;

  if( pzErrMsg ) *pzErrMsg = 0;
  if( sqlite3SafetyOn(db) ) goto exec_misuse;
  if( !db->init.busy ){
    if( (db->flags & SQLITE_Initialized)==0 ){
      int rc, cnt = 1;
      while( (rc = sqlite3Init(db, pzErrMsg))==SQLITE_BUSY
         && db->xBusyCallback
         && db->xBusyCallback(db->pBusyArg, "", cnt++)!=0 ){}
      if( rc!=SQLITE_OK ){
        sqlite3StrRealloc(pzErrMsg);
        sqlite3SafetyOff(db);
        return rc;
      }
      if( pzErrMsg ){
        sqliteFree(*pzErrMsg);
        *pzErrMsg = 0;
      }
    }
  }
  assert( (db->flags & SQLITE_Initialized)!=0 || db->init.busy );
  if( db->pVdbe==0 ){ db->nChange = 0; }
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sqlite3RunParser(&sParse, zSql, pzErrMsg);
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
    sqlite3SetString(pzErrMsg, "out of memory", (char*)0);
    sParse.rc = SQLITE_NOMEM;
    sqlite3RollbackAll(db);
    sqlite3ResetInternalSchema(db, 0);
    db->flags &= ~SQLITE_InTrans;
  }
  if( sParse.rc==SQLITE_DONE ) sParse.rc = SQLITE_OK;
  if( sParse.rc!=SQLITE_OK && pzErrMsg && *pzErrMsg==0 ){
    sqlite3SetString(pzErrMsg, sqlite3_error_string(sParse.rc), (char*)0);
  }
  sqlite3StrRealloc(pzErrMsg);
  if( sParse.rc==SQLITE_SCHEMA ){
    sqlite3ResetInternalSchema(db, 0);
  }
  assert( ppVm );
  *ppVm = (sqlite_vm*)sParse.pVdbe;
  if( pzTail ) *pzTail = sParse.zTail;
  if( sqlite3SafetyOff(db) ) goto exec_misuse;
  return sParse.rc;

exec_misuse:
  if( pzErrMsg ){
    *pzErrMsg = 0;
    sqlite3SetString(pzErrMsg, sqlite3_error_string(SQLITE_MISUSE), (char*)0);
    sqlite3StrRealloc(pzErrMsg);
  }
  return SQLITE_MISUSE;
}


/*
** The following routine destroys a virtual machine that is created by
** the sqlite3_compile() routine.
**
** The integer returned is an SQLITE_ success/failure code that describes
** the result of executing the virtual machine.  An error message is
** written into memory obtained from malloc and *pzErrMsg is made to
** point to that error if pzErrMsg is not NULL.  The calling routine
** should use sqlite3_freemem() to delete the message when it has finished
** with it.
*/
int sqlite3_finalize(
  sqlite_vm *pVm,            /* The virtual machine to be destroyed */
  char **pzErrMsg            /* OUT: Write error messages here */
){
  int rc = sqlite3VdbeFinalize((Vdbe*)pVm, pzErrMsg);
  sqlite3StrRealloc(pzErrMsg);
  return rc;
}

/*
** Terminate the current execution of a virtual machine then
** reset the virtual machine back to its starting state so that it
** can be reused.  Any error message resulting from the prior execution
** is written into *pzErrMsg.  A success code from the prior execution
** is returned.
*/
int sqlite3_reset(
  sqlite_vm *pVm,            /* The virtual machine to be destroyed */
  char **pzErrMsg            /* OUT: Write error messages here */
){
  int rc = sqlite3VdbeReset((Vdbe*)pVm, pzErrMsg);
  sqlite3VdbeMakeReady((Vdbe*)pVm, -1, 0);
  sqlite3StrRealloc(pzErrMsg);
  return rc;
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
  void (*xFunc)(sqlite_func*,int,const char**),  /* The implementation */
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
  void (*xStep)(sqlite_func*,int,const char**), /* The step function */
  void (*xFinalize)(sqlite_func*),              /* The finalizer */
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

#if 0

/*
** sqlite3_open
**
*/
int sqlite3_open(const char *filename, sqlite3 **pDb, const char **options){
  *pDb = sqlite3_open(filename, 0, &errmsg);
  return (*pDb?SQLITE_OK:SQLITE_ERROR);
}
int sqlite3_open16(const void *filename, sqlite3 **pDb, const char **options){
  int rc;
  char * filename8;

  filename8 = sqlite3utf16to8(filename, -1);
  if( !filename8 ){
    return SQLITE_NOMEM;
  }

  rc = sqlite3_open(filename8, pDb, options);
  sqliteFree(filename8);

  return rc;
}

/*
** sqlite3_close
**
*/
int sqlite3_close(sqlite3 *db){
  return sqlite3_close(db);
}

/*
** sqlite3_errmsg
**
** TODO: !
*/
const char *sqlite3_errmsg(sqlite3 *db){
  assert(!"TODO");
}
const void *sqlite3_errmsg16(sqlite3 *db){
  assert(!"TODO");
}

/*
** sqlite3_errcode
**
** TODO: !
*/
int sqlite3_errcode(sqlite3 *db){
  assert(!"TODO");
}

struct sqlite_stmt {
};

/*
** sqlite3_prepare
**
** TODO: error message handling
*/
int sqlite3_prepare(
  sqlite3 *db, 
  const char *zSql, 
  sqlite3_stmt **ppStmt, 
  const char** pzTail
){
  int rc;
  rc = sqlite3_compile(db, zSql, pzTail, ppStmt, 0); 
  return rc;
}
int sqlite3_prepare16(
  sqlite3 *db, 
  const void *zSql, 
  sqlite3_stmt **ppStmt, 
  const void **pzTail
){
  int rc;
  char *sql8;

  sql8 = sqlite3utf16to8(zSql, -1);
  if( !sql8 ){
    return SQLITE_NOMEM;
  }

  /* TODO: Have to set *pzTail to point into the original UTF-16 string
  ** somehow.
  */
  rc = sqlite3_prepare(db, sql8, ppStmt, 0);
  sqliteFree(filename8);

  return rc;
}

/*
** sqlite3_finalize
*/
int sqlite3_finalize(sqlite3_stmt *stmt){
  return sqlite3_finalize(stmt, 0);
}

/*
** sqlite3_reset
*/
int sqlite3_reset(sqlite3_stmt*){
  return sqlite3_reset(stmt, 0);
}

/*
** sqlite3_step
*/
int sqlite3_step(sqlite3_stmt *pStmt){
  return sqlite3_step(pStmt);
}

/*
** sqlite3_bind_text
*/
int sqlite3_bind_text(
  sqlite3_stmt *pStmt, 
  int i, 
  const char *zVal, 
  int n, 
  int eCopy
){
  return sqlite3_bind(pStmt, i, zVal, n, eCopy);
}

int sqlite3_bind_text16(
  sqlite3_stmt *pStmt, 
  int i, 
  void *zVal, 
  int n, 
  int eCopy
){
  int rc;
  char * zVal8;

  /* convert the first n bytes of the UTF-16 string to UTF-8 */
  zVal8 = sqlite3utf16to8(zVal, n);
  if( !zVal8 ){
    return SQLITE_NOMEM;
  }

  /* Pass -1 as the length of the UTF-8 string. It is guaranteed to be
  ** NULL-terminated by sqlite3utf16to8().
  */
  rc = sqlite3_bind_text(pStmt, i, zVal8, -1, eCopy);
  sqliteFree(filename8);

  return rc;
}

/*
** sqlite3_bind_null
*/
int sqlite3_bind_null(sqlite3_stmt*, int iParm){
  return sqlite3_bind(pStmt, i, 0, 0, 0);
}


int sqlite3_bind_int32(sqlite3_stmt*, int iParm, int iValue){
  assert(!"TODO");
}
int sqlite3_bind_int64(sqlite3_stmt*, int iParm, long long int iValue){
  assert(!"TODO");
}
int sqlite3_bind_double(sqlite3_stmt*, int iParm, double iValue){
  assert(!"TODO");
}
int sqlite3_bind_blob(sqlite3_stmt*, int i, const void*, int n, int eCopy){
  assert(!"TODO");
}


int sqlite3_column_count(sqlite3_stmt*){
}

int sqlite3_column_type(sqlite3_stmt*,int){
}

const char *sqlite3_column_decltype(sqlite3_stmt*,int){
}

const void *sqlite3_column_decltype16(sqlite3_stmt*,int){
}

const char *sqlite3_column_name(sqlite3_stmt*,int){
}

const void *sqlite3_column_name16(sqlite3_stmt*,int){
}

const unsigned char *sqlite3_column_data(sqlite3_stmt*,int){
}

const void *sqlite3_column_data16(sqlite3_stmt*,int){
}

int sqlite3_column_bytes(sqlite3_stmt*,int){
}

long long int sqlite3_column_int(sqlite3_stmt*,int){
}

double sqlite3_column_float(sqlite3_stmt*,int){
}

#endif
