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
** $Id: main.c,v 1.1 2000/05/29 14:26:01 drh Exp $
*/
#include "sqliteInt.h"

/*
** This is the callback routine for the code that initializes the
** database.  Each callback contains text of a CREATE TABLE or
** CREATE INDEX statement that must be parsed to yield the internal
** structures that describe the tables.
*/
static int sqliteOpenCb(void *pDb, int argc, char **argv, char **azColName){
  sqlite *db = (sqlite*)pDb;
  Parse sParse;
  int nErr;
  char *zErrMsg = 0;

  if( argc!=1 ) return 0;
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sParse.initFlag = 1;
  nErr = sqliteRunParser(&sParse, argv[0], &zErrMsg);
  return nErr;
}

/*
** Open a new SQLite database.  Construct an "sqlite" structure to define
** the state of this database and return a pointer to that structure.
*/
sqlite *sqlite_open(const char *zFilename, int mode, char **pzErrMsg){
  sqlite *db;
  Vdbe *vdbe;
  Table *pTab;
  char *azArg[2];
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
  **        type       text,    --  Either "table" or "index"
  **        name       text,    --  Name of table or index
  **        tbl_name   text,    --  Associated table 
  **        sql        text     --  The CREATE statement for this object
  **    );
  **
  ** The sqlite_master table contains a single entry for each table
  ** and each index.  The "type" field tells whether the entry is
  ** a table or index.  The "name" field is the name of the object.
  ** The "tbl_name" is the name of the associated table.  For tables,
  ** the tbl_name field is always the same as name.  For indices, the
  ** tbl_name field contains the name of the table that the index
  ** indexes.  Finally, the sql field contains the complete text of
  ** the CREATE TABLE or CREATE INDEX statement that originally created
  ** the table or index.
  **
  ** The following program invokes its callback on the SQL for each
  ** table then goes back and invokes the callback on the
  ** SQL for each index.  The callback will invoke the
  ** parser to build the internal representation of the
  ** database scheme.
  */
  static VdbeOp initProg[] = {
    { OP_Open,     0, 0,  MASTER_NAME},
    { OP_Next,     0, 8,  0},           /* 1 */
    { OP_Field,    0, 0,  0},
    { OP_String,   0, 0,  "table"},
    { OP_Ne,       0, 1,  0},
    { OP_Field,    0, 3,  0},
    { OP_Callback, 1, 0,  0},
    { OP_Goto,     0, 1,  0},
    { OP_Rewind,   0, 0,  0},           /* 8 */
    { OP_Next,     0, 16, 0},           /* 9 */
    { OP_Field,    0, 0,  0},
    { OP_String,   0, 0,  "index"},
    { OP_Ne,       0, 9,  0},
    { OP_Field,    0, 3,  0},
    { OP_Callback, 1, 0,  0},
    { OP_Goto,     0, 9,  0},
    { OP_Halt,     0, 0,  0},           /* 16 */
  };

  /* Allocate space to hold the main database structure */
  db = sqliteMalloc( sizeof(sqlite) );
  if( pzErrMsg ) *pzErrMsg = 0;
  if( db==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  
  /* Open the backend database driver */
  db->pBe = sqliteDbbeOpen(zFilename, (mode&0222)!=0, mode!=0, pzErrMsg);
  if( db->pBe==0 ){
    sqliteFree(db);
    return 0;
  }

  /* Create a virtual machine to run the initialization program.  Run
  ** the program.  The delete the virtual machine.
  */
  azArg[0] = master_schema;
  azArg[1] = 0;
  sqliteOpenCb(db, 1, azArg, 0);
  pTab = sqliteFindTable(db, MASTER_NAME);
  if( pTab ){
    pTab->readOnly = 1;
  }
  vdbe = sqliteVdbeCreate(db->pBe);
  sqliteVdbeAddOpList(vdbe, sizeof(initProg)/sizeof(initProg[0]), initProg);
  sqliteVdbeExec(vdbe, sqliteOpenCb, db, pzErrMsg);
  sqliteVdbeDelete(vdbe);
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
** Execute SQL code 
*/
int sqlite_exec(
  sqlite *db,                 /* The database on which the SQL executes */
  char *zSql,                 /* The SQL to be executed */
  sqlite_callback xCallback,  /* Invoke this callback routine */
  void *pArg,                 /* First argument to xCallback() */
  char **pzErrMsg             /* Write error messages here */
){
  Parse sParse;
  int nErr;

  if( pzErrMsg ) *pzErrMsg = 0;
  memset(&sParse, 0, sizeof(sParse));
  sParse.db = db;
  sParse.xCallback = xCallback;
  sParse.pArg = pArg;
  nErr = sqliteRunParser(&sParse, zSql, pzErrMsg);
  return nErr;
}
