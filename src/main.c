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
** $Id: main.c,v 1.49 2001/11/07 16:48:27 drh Exp $
*/
#include "sqliteInt.h"
#include "os.h"

/*
** This is the callback routine for the code that initializes the
** database.  See sqliteInit() below for additional information.
**
** Each callback contains the following information:
**
**     argv[0] = "meta" or "table" or "index"
**     argv[1] = table or index name or meta statement type.
**     argv[2] = root page number for table or index.  NULL for meta.
**     argv[3] = SQL create statement for the table or index
**
*/
static int sqliteOpenCb(void *pDb, int argc, char **argv, char **azColName){
  sqlite *db = (sqlite*)pDb;
  Parse sParse;
  int nErr = 0;

  /* TODO: Do some validity checks on all fields.  In particular,
  ** make sure fields do not contain NULLs. Otherwise we might core
  ** when attempting to initialize from a corrupt database file. */

  assert( argc==4 );
  switch( argv[0][0] ){
    case 'm': {  /* Meta information */
      if( strcmp(argv[1],"file-format")==0 ){
        db->file_format = atoi(argv[3]);
      }else if( strcmp(argv[1],"schema-cookie")==0 ){
        db->schema_cookie = atoi(argv[3]);
        db->next_cookie = db->schema_cookie;
      }
      break;
    }
    case 'i':
    case 't': {  /* CREATE TABLE  and CREATE INDEX statements */
      if( argv[3] && argv[3][0] ){
        /* Call the parser to process a CREATE TABLE or CREATE INDEX statement.
        ** But because sParse.initFlag is set to 1, no VDBE code is generated
        ** or executed.  All the parser does is build the internal data
        ** structures that describe the table or index.
        */
        memset(&sParse, 0, sizeof(sParse));
        sParse.db = db;
        sParse.initFlag = 1;
        sParse.newTnum = atoi(argv[2]);
        nErr = sqliteRunParser(&sParse, argv[3], 0);
      }else{
        /* If the SQL column is blank it means this is an index that
        ** was created to be the PRIMARY KEY or to fulfill a UNIQUE
        ** constraint or a CREATE TABLE.  The index should have already
        ** been created when we processed the CREATE TABLE.  All we have
        ** to do here is record the root page.
        */
        Index *pIndex = sqliteFindIndex(db, argv[1]);
        if( pIndex==0 || pIndex->tnum!=0 ){
          nErr++;
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
** data structures.  Return one of the SQLITE_ error codes to
** indicate success or failure.
**
** After the database is initialized, the SQLITE_Initialized
** bit is set in the flags field of the sqlite structure.  An
** attempt is made to initialize the database as soon as it
** is opened.  If that fails (perhaps because another process
** has the sqlite_master table locked) than another attempt
** is made the first time the database is accessed.
*/
static int sqliteInit(sqlite *db, char **pzErrMsg){
  Vdbe *vdbe;
  int rc;

  /*
  ** The master database table has a structure like this
  */
  static char master_schema[] = 
     "CREATE TABLE " MASTER_NAME " (\n"
     "  type text,\n"
     "  name text,\n"
     "  tbl_name text,\n"
     "  rootpage integer,\n"
     "  sql text\n"
     ")"
  ;

  /* The following program is used to initialize the internal
  ** structure holding the tables and indexes of the database.
  ** The database contains a special table named "sqlite_master"
  ** defined as follows:
  **
  **    CREATE TABLE sqlite_master (
  **        type       text,    --  Either "table" or "index" or "meta"
  **        name       text,    --  Name of table or index
  **        tbl_name   text,    --  Associated table 
  **        rootpage   integer, --  The integer page number of root page
  **        sql        text     --  The CREATE statement for this object
  **    );
  **
  ** The sqlite_master table contains a single entry for each table
  ** and each index.  The "type" column tells whether the entry is
  ** a table or index.  The "name" column is the name of the object.
  ** The "tbl_name" is the name of the associated table.  For tables,
  ** the tbl_name column is always the same as name.  For indices, the
  ** tbl_name column contains the name of the table that the index
  ** indexes.  The "rootpage" column holds the number of the root page
  ** for the b-tree for the table or index.  Finally, the "sql" column
  ** contains the complete text of the CREATE TABLE or CREATE INDEX
  ** statement that originally created the table or index.  If an index
  ** was created to fulfill a PRIMARY KEY or UNIQUE constraint on a table,
  ** then the "sql" column is NULL.
  **
  ** If the "type" column has the value "meta", then the "sql" column
  ** contains extra information about the database, such as the
  ** file format version number.  All meta information must be processed
  ** before any tables or indices are constructed.
  **
  ** The following program invokes its callback on the SQL for each
  ** table then goes back and invokes the callback on the
  ** SQL for each index.  The callback will invoke the
  ** parser to build the internal representation of the
  ** database scheme.
  */
  static VdbeOp initProg[] = {
    { OP_Open,     0, 2,  0},
    { OP_Rewind,   0, 31, 0},
    { OP_Column,   0, 0,  0},           /* 2 */
    { OP_String,   0, 0,  "meta"},
    { OP_Ne,       0, 10, 0},
    { OP_Column,   0, 0,  0},
    { OP_Column,   0, 1,  0},
    { OP_Column,   0, 3,  0},
    { OP_Column,   0, 4,  0},
    { OP_Callback, 4, 0,  0},
    { OP_Next,     0, 2,  0},           /* 10 */
    { OP_Rewind,   0, 31, 0},           /* 11 */
    { OP_Column,   0, 0,  0},           /* 12 */
    { OP_String,   0, 0,  "table"},
    { OP_Ne,       0, 20, 0},
    { OP_Column,   0, 0,  0},
    { OP_Column,   0, 1,  0},
    { OP_Column,   0, 3,  0},
    { OP_Column,   0, 4,  0},
    { OP_Callback, 4, 0,  0},
    { OP_Next,     0, 12, 0},           /* 20 */
    { OP_Rewind,   0, 31, 0},           /* 21 */
    { OP_Column,   0, 0,  0},           /* 22 */
    { OP_String,   0, 0,  "index"},
    { OP_Ne,       0, 30, 0},
    { OP_Column,   0, 0,  0},
    { OP_Column,   0, 1,  0},
    { OP_Column,   0, 3,  0},
    { OP_Column,   0, 4,  0},
    { OP_Callback, 4, 0,  0},
    { OP_Next,     0, 22, 0},           /* 30 */
    { OP_String,   0, 0,  "meta"},      /* 31 */
    { OP_String,   0, 0,  "schema-cookie"},
    { OP_String,   0, 0,  0},
    { OP_ReadCookie,0,0,  0},
    { OP_Callback, 4, 0,  0},
    { OP_Close,    0, 0,  0},
    { OP_Halt,     0, 0,  0},
  };

  /* Create a virtual machine to run the initialization program.  Run
  ** the program.  Then delete the virtual machine.
  */
  vdbe = sqliteVdbeCreate(db);
  if( vdbe==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return SQLITE_NOMEM;
  }
  sqliteVdbeAddOpList(vdbe, sizeof(initProg)/sizeof(initProg[0]), initProg);
  rc = sqliteVdbeExec(vdbe, sqliteOpenCb, db, pzErrMsg, 
                      db->pBusyArg, db->xBusyCallback);
  sqliteVdbeDelete(vdbe);
  if( rc==SQLITE_OK && db->file_format>1 && db->nTable>0 ){
    sqliteSetString(pzErrMsg, "unsupported file format", 0);
    rc = SQLITE_ERROR;
  }
  if( rc==SQLITE_OK ){
    Table *pTab;
    char *azArg[6];
    azArg[0] = "table";
    azArg[1] = MASTER_NAME;
    azArg[2] = "2";
    azArg[3] = master_schema;
    azArg[4] = 0;
    sqliteOpenCb(db, 4, azArg, 0);
    pTab = sqliteFindTable(db, MASTER_NAME);
    if( pTab ){
      pTab->readOnly = 1;
    }
    db->flags |= SQLITE_Initialized;
    sqliteCommitInternalChanges(db);
  }
  return rc;
}

/*
** The version of the library
*/
const char sqlite_version[] = SQLITE_VERSION;

/*
** Does the library expect data to be encoded as UTF-8 or iso8859?  The
** following global constant always lets us know.
*/
#ifdef SQLITE_UTF8
const char sqlite_encoding[] = "UTF-8";
#else
const char sqlite_encoding[] = "iso8859";
#endif

/*
** Open a new SQLite database.  Construct an "sqlite" structure to define
** the state of this database and return a pointer to that structure.
**
** An attempt is made to initialize the in-memory data structures that
** hold the database schema.  But if this fails (because the schema file
** is locked) then that step is deferred until the first call to
** sqlite_exec().
*/
sqlite *sqlite_open(const char *zFilename, int mode, char **pzErrMsg){
  sqlite *db;
  int rc;

  /* Allocate the sqlite data structure */
  db = sqliteMalloc( sizeof(sqlite) );
  if( pzErrMsg ) *pzErrMsg = 0;
  if( db==0 ) goto no_mem_on_open;
  sqliteHashInit(&db->tblHash, SQLITE_HASH_STRING, 0);
  sqliteHashInit(&db->idxHash, SQLITE_HASH_STRING, 0);
  db->nextRowid = sqliteRandomInteger(db);
  
  /* Open the backend database driver */
  rc = sqliteBtreeOpen(zFilename, mode, MAX_PAGES, &db->pBe);
  if( rc!=SQLITE_OK ){
    switch( rc ){
      default: {
        if( pzErrMsg ){
          sqliteSetString(pzErrMsg, "unable to open database: ", zFilename, 0);
        }
      }
    }
    sqliteFree(db);
    sqliteStrRealloc(pzErrMsg);
    return 0;
  }

  /* Assume file format 1 unless the database says otherwise */
  db->file_format = 1;

  /* Attempt to read the schema */
  rc = sqliteInit(db, pzErrMsg);
  if( sqlite_malloc_failed ){
    sqlite_close(db);
    goto no_mem_on_open;
  }else if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
    sqlite_close(db);
    sqliteStrRealloc(pzErrMsg);
    return 0;
  }else /* if( pzErrMsg ) */{
    sqliteFree(*pzErrMsg);
    *pzErrMsg = 0;
  }
  return db;

no_mem_on_open:
  sqliteSetString(pzErrMsg, "out of memory", 0);
  sqliteStrRealloc(pzErrMsg);
  return 0;
}

/*
** Erase all schema information from the schema hash table.  Except
** tables that are created using CREATE TEMPORARY TABLE are preserved
** if the preserverTemps flag is true.
**
** The database schema is normally read in once when the database
** is first opened and stored in a hash table in the sqlite structure.
** This routine erases the stored schema.  This erasure occurs because
** either the database is being closed or because some other process
** changed the schema and this process needs to reread it.
*/
static void clearHashTable(sqlite *db, int preserveTemps){
  HashElem *pElem;
  Hash temp1;
  temp1 = db->tblHash;
  sqliteHashInit(&db->tblHash, SQLITE_HASH_STRING, 0);
  sqliteHashClear(&db->idxHash);
  for(pElem=sqliteHashFirst(&temp1); pElem; pElem=sqliteHashNext(pElem)){
    Table *pTab = sqliteHashData(pElem);
    if( preserveTemps && pTab->isTemp ){
      Index *pIdx;
      int nName = strlen(pTab->zName);
      Table *pOld = sqliteHashInsert(&db->tblHash, pTab->zName, nName+1, pTab);
      if( pOld!=0 ){
        assert( pOld==pTab );   /* Malloc failed on the HashInsert */
        sqliteDeleteTable(db, pOld);
        continue;
      }
      for(pIdx=pTab->pIndex; pIdx; pIdx=pIdx->pNext){
        int n = strlen(pIdx->zName)+1;
        Index *pOldIdx;
        pOldIdx = sqliteHashInsert(&db->idxHash, pIdx->zName, n, pIdx);
        if( pOld ){
          assert( pOldIdx==pIdx );
          sqliteUnlinkAndDeleteIndex(db, pOldIdx);
        }
      }
    }else{
      sqliteDeleteTable(db, pTab);
    }
  }
  sqliteHashClear(&temp1);
  db->flags &= ~SQLITE_Initialized;
}

/*
** Close an existing SQLite database
*/
void sqlite_close(sqlite *db){
  sqliteBtreeClose(db->pBe);
  clearHashTable(db, 0);
  if( db->pBeTemp ){
    sqliteBtreeClose(db->pBeTemp);
  }
  sqliteFree(db);
}

/*
** Return TRUE if the given SQL string ends in a semicolon.
*/
int sqlite_complete(const char *zSql){
  int isComplete = 0;
  while( *zSql ){
    switch( *zSql ){
      case ';': {
        isComplete = 1;
        break;
      }
      case ' ':
      case '\t':
      case '\n':
      case '\f': {
        break;
      }
      case '\'': {
        isComplete = 0;
        zSql++;
        while( *zSql && *zSql!='\'' ){ zSql++; }
        if( *zSql==0 ) return 0;
        break;
      }
      case '"': {
        isComplete = 0;
        zSql++;
        while( *zSql && *zSql!='"' ){ zSql++; }
        if( *zSql==0 ) return 0;
        break;
      }
      case '-': {
        if( zSql[1]!='-' ){
          isComplete = 0;
          break;
        }
        while( *zSql && *zSql!='\n' ){ zSql++; }
        if( *zSql==0 ) return isComplete;
        break;
      } 
      default: {
        isComplete = 0;
        break;
      }
    }
    zSql++;
  }
  return isComplete;
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
int sqlite_exec(
  sqlite *db,                 /* The database on which the SQL executes */
  const char *zSql,           /* The SQL to be executed */
  sqlite_callback xCallback,  /* Invoke this callback routine */
  void *pArg,                 /* First argument to xCallback() */
  char **pzErrMsg             /* Write error messages here */
){
  Parse sParse;

  if( pzErrMsg ) *pzErrMsg = 0;
  if( (db->flags & SQLITE_Initialized)==0 ){
    int rc = sqliteInit(db, pzErrMsg);
    if( rc!=SQLITE_OK ){
      sqliteStrRealloc(pzErrMsg);
      return rc;
    }
  }
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sParse.pBe = db->pBe;
  sParse.xCallback = xCallback;
  sParse.pArg = pArg;
  sqliteRunParser(&sParse, zSql, pzErrMsg);
  if( sqlite_malloc_failed ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    sParse.rc = SQLITE_NOMEM;
    sqliteBtreeRollback(db->pBe);
    if( db->pBeTemp ) sqliteBtreeRollback(db->pBeTemp);
    db->flags &= ~SQLITE_InTrans;
    clearHashTable(db, 0);
  }
  sqliteStrRealloc(pzErrMsg);
  if( sParse.rc==SQLITE_SCHEMA ){
    clearHashTable(db, 1);
  }
  return sParse.rc;
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
  int delay = 10;
  int prior_delay = 0;
  int timeout = (int)Timeout;
  int i;

  for(i=1; i<count; i++){ 
    prior_delay += delay;
    delay = delay*2;
    if( delay>=1000 ){
      delay = 1000;
      prior_delay += 1000*(count - i - 1);
      break;
    }
  }
  if( prior_delay + delay > timeout ){
    delay = timeout - prior_delay;
    if( delay<=0 ) return 0;
  }
  sqliteOsSleep(delay);
  return 1;
#else
  int timeout = (int)Timeout;
  if( (count+1)*1000 > timeout ){
    return 0;
  }
  sqliteOsSleep(1000);
  return 1;
#endif
}

/*
** This routine sets the busy callback for an Sqlite database to the
** given callback function with the given argument.
*/
void sqlite_busy_handler(
  sqlite *db,
  int (*xBusy)(void*,const char*,int),
  void *pArg
){
  db->xBusyCallback = xBusy;
  db->pBusyArg = pArg;
}

/*
** This routine installs a default busy handler that waits for the
** specified number of milliseconds before returning 0.
*/
void sqlite_busy_timeout(sqlite *db, int ms){
  if( ms>0 ){
    sqlite_busy_handler(db, sqliteDefaultBusyCallback, (void*)ms);
  }else{
    sqlite_busy_handler(db, 0, 0);
  }
}

/*
** Cause any pending operation to stop at its earliest opportunity.
*/
void sqlite_interrupt(sqlite *db){
  db->flags |= SQLITE_Interrupt;
}
