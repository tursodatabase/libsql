/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** Main file for the SQLite library.  The routines in this file
** implement the programmer interface to the library.  Routines in
** other files are for internal use by SQLite and should not be
** accessed by users of the library.
**
** $Id: main.c,v 1.16 2000/08/02 13:47:42 drh Exp $
*/
#include "sqliteInt.h"

/*
** This is the callback routine for the code that initializes the
** database.  Each callback contains text of a CREATE TABLE or
** CREATE INDEX statement that must be parsed to yield the internal
** structures that describe the tables.
**
** This callback is also called with argc==2 when there is meta
** information in the sqlite_master file.  The meta information is
** contained in argv[1].  Typical meta information is the file format
** version.
*/
static int sqliteOpenCb(void *pDb, int argc, char **argv, char **azColName){
  sqlite *db = (sqlite*)pDb;
  Parse sParse;
  int nErr;

  if( argc==2 ){
    if( sscanf(argv[1],"file format %d",&db->file_format)==1 ){
      return 0;
    }
    /* Unknown meta information.  Ignore it. */
    return 0;
  }
  if( argc!=1 ) return 0;
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sParse.initFlag = 1;
  nErr = sqliteRunParser(&sParse, argv[0], 0);
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
  **        sql        text     --  The CREATE statement for this object
  **    );
  **
  ** The sqlite_master table contains a single entry for each table
  ** and each index.  The "type" column tells whether the entry is
  ** a table or index.  The "name" column is the name of the object.
  ** The "tbl_name" is the name of the associated table.  For tables,
  ** the tbl_name column is always the same as name.  For indices, the
  ** tbl_name column contains the name of the table that the index
  ** indexes.  Finally, the "sql" column contains the complete text of
  ** the CREATE TABLE or CREATE INDEX statement that originally created
  ** the table or index.
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
    { OP_Open,     0, 0,  MASTER_NAME},
    { OP_Next,     0, 9,  0},           /* 1 */
    { OP_Field,    0, 0,  0},
    { OP_String,   0, 0,  "meta"},
    { OP_Ne,       0, 1,  0},
    { OP_Field,    0, 0,  0},
    { OP_Field,    0, 3,  0},
    { OP_Callback, 2, 0,  0},
    { OP_Goto,     0, 1,  0},
    { OP_Rewind,   0, 0,  0},           /* 9 */
    { OP_Next,     0, 17, 0},           /* 10 */
    { OP_Field,    0, 0,  0},
    { OP_String,   0, 0,  "table"},
    { OP_Ne,       0, 10, 0},
    { OP_Field,    0, 3,  0},
    { OP_Callback, 1, 0,  0},
    { OP_Goto,     0, 10, 0},
    { OP_Rewind,   0, 0,  0},           /* 17 */
    { OP_Next,     0, 25, 0},           /* 18 */
    { OP_Field,    0, 0,  0},
    { OP_String,   0, 0,  "index"},
    { OP_Ne,       0, 18, 0},
    { OP_Field,    0, 3,  0},
    { OP_Callback, 1, 0,  0},
    { OP_Goto,     0, 18, 0},
    { OP_Halt,     0, 0,  0},           /* 25 */
  };

  /* Create a virtual machine to run the initialization program.  Run
  ** the program.  The delete the virtual machine.
  */
  vdbe = sqliteVdbeCreate(db->pBe);
  if( vdbe==0 ){
    sqliteSetString(pzErrMsg, "out of memory",0); 
    return 1;
  }
  sqliteVdbeAddOpList(vdbe, sizeof(initProg)/sizeof(initProg[0]), initProg);
  rc = sqliteVdbeExec(vdbe, sqliteOpenCb, db, pzErrMsg, 
                      db->pBusyArg, db->xBusyCallback);
  sqliteVdbeDelete(vdbe);
  if( rc==SQLITE_OK && db->file_format<2 && db->nTable>0 ){
    sqliteSetString(pzErrMsg, "obsolete file format", 0);
    rc = SQLITE_ERROR;
  }
  if( rc==SQLITE_OK ){
    Table *pTab;
    char *azArg[2];
    azArg[0] = master_schema;
    azArg[1] = 0;
    sqliteOpenCb(db, 1, azArg, 0);
    pTab = sqliteFindTable(db, MASTER_NAME);
    if( pTab ){
      pTab->readOnly = 1;
    }
    db->flags |= SQLITE_Initialized;
  }else{
    sqliteStrRealloc(pzErrMsg);
  }
  return rc;
}

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
  if( db==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    sqliteStrRealloc(pzErrMsg);
    return 0;
  }
  
  /* Open the backend database driver */
  db->pBe = sqliteDbbeOpen(zFilename, (mode&0222)!=0, mode!=0, pzErrMsg);
  if( db->pBe==0 ){
    sqliteStrRealloc(pzErrMsg);
    sqliteFree(db);
    return 0;
  }

  /* Assume file format 1 unless the database says otherwise */
  db->file_format = 1;

  /* Attempt to read the schema */
  rc = sqliteInit(db, pzErrMsg);
  if( rc!=SQLITE_OK && rc!=SQLITE_BUSY ){
    sqlite_close(db);
    return 0;
  }else{
    free(*pzErrMsg);
    *pzErrMsg = 0;
  }
  return db;
}

/*
** Close an existing SQLite database
*/
void sqlite_close(sqlite *db){
  int i;
  sqliteDbbeClose(db->pBe);
  for(i=0; i<N_HASH; i++){
    Table *pNext, *pList = db->apTblHash[i];
    db->apTblHash[i] = 0;
    while( pList ){
      pNext = pList->pHash;
      pList->pHash = 0;
      sqliteDeleteTable(db, pList);
      pList = pNext;
    }
  }
  sqliteFree(db);
}

/*
** Return TRUE if the given SQL string ends in a semicolon.
*/
int sqlite_complete(const char *zSql){
  int i;
  int lastWasSemi = 0;

  i = 0;
  while( i>=0 && zSql[i]!=0 ){
    int tokenType;
    int n;

    n = sqliteGetToken(&zSql[i], &tokenType);
    switch( tokenType ){
      case TK_SPACE:
      case TK_COMMENT:
        break;
      case TK_SEMI:
        lastWasSemi = 1;
        break;
      default:
        lastWasSemi = 0;
        break;
    }
    i += n;
  }
  return lastWasSemi;
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
  char *zSql,                 /* The SQL to be executed */
  sqlite_callback xCallback,  /* Invoke this callback routine */
  void *pArg,                 /* First argument to xCallback() */
  char **pzErrMsg             /* Write error messages here */
){
  Parse sParse;
  int rc;

  if( pzErrMsg ) *pzErrMsg = 0;
  if( (db->flags & SQLITE_Initialized)==0 ){
    int rc = sqliteInit(db, pzErrMsg);
    if( rc!=SQLITE_OK ) return rc;
  }
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sParse.xCallback = xCallback;
  sParse.pArg = pArg;
  rc = sqliteRunParser(&sParse, zSql, pzErrMsg);
  sqliteStrRealloc(pzErrMsg);
  return rc;
}

/*
** This routine implements a busy callback that sleeps and tries
** again until a timeout value is reached.  The timeout value is
** an integer number of milliseconds passed in as the first
** argument.
*/
static int sqlite_default_busy_callback(
 void *Timeout,           /* Maximum amount of time to wait */
 const char *NotUsed,     /* The name of the table that is busy */
 int count                /* Number of times table has been busy */
){
  int rc;
#if defined(HAVE_USLEEP) && HAVE_USLEEP
  int delay = 10000;
  int prior_delay = 0;
  int timeout = (int)Timeout;
  int i;

  for(i=1; i<count; i++){ 
    prior_delay += delay;
    delay = delay*2;
    if( delay>=1000000 ){
      delay = 1000000;
      prior_delay += 1000000*(count - i - 1);
      break;
    }
  }
  if( prior_delay + delay > timeout*1000 ){
    delay = timeout*1000 - prior_delay;
    if( delay<=0 ) return 0;
  }
  usleep(delay);
  return 1;
#else
  int timeout = (int)Timeout;
  if( (count+1)*1000 > timeout ){
    return 0;
  }
  sleep(1);
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
    sqlite_busy_handler(db, sqlite_default_busy_callback, (void*)ms);
  }else{
    sqlite_busy_handler(db, 0, 0);
  }
}
