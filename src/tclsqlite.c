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
** A TCL Interface to SQLite
**
** $Id: tclsqlite.c,v 1.33 2002/06/25 19:31:18 drh Exp $
*/
#ifndef NO_TCL     /* Omit this whole file if TCL is unavailable */

#include "sqlite.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** If TCL uses UTF-8 and SQLite is configured to use iso8859, then we
** have to do a translation when going between the two.  Set the 
** UTF_TRANSLATION_NEEDED macro to indicate that we need to do
** this translation.  
*/
#if defined(TCL_UTF_MAX) && !defined(SQLITE_UTF8)
# define UTF_TRANSLATION_NEEDED 1
#endif

/*
** There is one instance of this structure for each SQLite database
** that has been opened by the SQLite TCL interface.
*/
typedef struct SqliteDb SqliteDb;
struct SqliteDb {
  sqlite *db;           /* The "real" database structure */
  Tcl_Interp *interp;   /* The interpreter used for this database */
  char *zBusy;          /* The busy callback routine */
};

/*
** An instance of this structure passes information thru the sqlite
** logic from the original TCL command into the callback routine.
*/
typedef struct CallbackData CallbackData;
struct CallbackData {
  Tcl_Interp *interp;       /* The TCL interpreter */
  char *zArray;             /* The array into which data is written */
  Tcl_Obj *pCode;           /* The code to execute for each row */
  int once;                 /* Set for first callback only */
  int tcl_rc;               /* Return code from TCL script */
  int nColName;             /* Number of entries in the azColName[] array */
  char **azColName;         /* Column names translated to UTF-8 */
};

#ifdef UTF_TRANSLATION_NEEDED
/*
** Called for each row of the result.
**
** This version is used when TCL expects UTF-8 data but the database
** uses the ISO8859 format.  A translation must occur from ISO8859 into
** UTF-8.
*/
static int DbEvalCallback(
  void *clientData,      /* An instance of CallbackData */
  int nCol,              /* Number of columns in the result */
  char ** azCol,         /* Data for each column */
  char ** azN            /* Name for each column */
){
  CallbackData *cbData = (CallbackData*)clientData;
  int i, rc;
  Tcl_DString dCol;
  Tcl_DStringInit(&dCol);
  if( cbData->azColName==0 ){
    assert( cbData->once );
    cbData->once = 0;
    if( cbData->zArray[0] ){
      Tcl_SetVar2(cbData->interp, cbData->zArray, "*", "", 0);
    }
    cbData->azColName = malloc( nCol*sizeof(char*) );
    if( cbData->azColName==0 ){ return 1; }
    cbData->nColName = nCol;
    for(i=0; i<nCol; i++){
      Tcl_ExternalToUtfDString(NULL, azN[i], -1, &dCol);
      cbData->azColName[i] = malloc( Tcl_DStringLength(&dCol) + 1 );
      if( cbData->azColName[i] ){
        strcpy(cbData->azColName[i], Tcl_DStringValue(&dCol));
      }else{
        return 1;
      }
      if( cbData->zArray[0] ){
        Tcl_SetVar2(cbData->interp, cbData->zArray, "*",
             Tcl_DStringValue(&dCol), TCL_LIST_ELEMENT|TCL_APPEND_VALUE);
      }
      Tcl_DStringFree(&dCol);
    }
  }
  if( azCol!=0 ){
    if( cbData->zArray[0] ){
      for(i=0; i<nCol; i++){
        char *z = azCol[i];
        if( z==0 ) z = "";
        Tcl_DStringInit(&dCol);
        Tcl_ExternalToUtfDString(NULL, z, -1, &dCol);
        Tcl_SetVar2(cbData->interp, cbData->zArray, cbData->azColName[i], 
              Tcl_DStringValue(&dCol), 0);
        Tcl_DStringFree(&dCol);
      }
    }else{
      for(i=0; i<nCol; i++){
        char *z = azCol[i];
        if( z==0 ) z = "";
        Tcl_DStringInit(&dCol);
        Tcl_ExternalToUtfDString(NULL, z, -1, &dCol);
        Tcl_SetVar(cbData->interp, cbData->azColName[i],
                   Tcl_DStringValue(&dCol), 0);
        Tcl_DStringFree(&dCol);
      }
    }
  }
  rc = Tcl_EvalObj(cbData->interp, cbData->pCode);
  if( rc==TCL_CONTINUE ) rc = TCL_OK;
  cbData->tcl_rc = rc;
  return rc!=TCL_OK;
}
#endif /* UTF_TRANSLATION_NEEDED */

#ifndef UTF_TRANSLATION_NEEDED
/*
** Called for each row of the result.
**
** This version is used when either of the following is true:
**
**    (1) This version of TCL uses UTF-8 and the data in the
**        SQLite database is already in the UTF-8 format.
**
**    (2) This version of TCL uses ISO8859 and the data in the
**        SQLite database is already in the ISO8859 format.
*/
static int DbEvalCallback(
  void *clientData,      /* An instance of CallbackData */
  int nCol,              /* Number of columns in the result */
  char ** azCol,         /* Data for each column */
  char ** azN            /* Name for each column */
){
  CallbackData *cbData = (CallbackData*)clientData;
  int i, rc;
  if( azCol==0 || (cbData->once && cbData->zArray[0]) ){
    Tcl_SetVar2(cbData->interp, cbData->zArray, "*", "", 0);
    for(i=0; i<nCol; i++){
      Tcl_SetVar2(cbData->interp, cbData->zArray, "*", azN[i],
         TCL_LIST_ELEMENT|TCL_APPEND_VALUE);
    }
    cbData->once = 0;
  }
  if( azCol!=0 ){
    if( cbData->zArray[0] ){
      for(i=0; i<nCol; i++){
        char *z = azCol[i];
        if( z==0 ) z = "";
        Tcl_SetVar2(cbData->interp, cbData->zArray, azN[i], z, 0);
      }
    }else{
      for(i=0; i<nCol; i++){
        char *z = azCol[i];
        if( z==0 ) z = "";
        Tcl_SetVar(cbData->interp, azN[i], z, 0);
      }
    }
  }
  rc = Tcl_EvalObj(cbData->interp, cbData->pCode);
  if( rc==TCL_CONTINUE ) rc = TCL_OK;
  cbData->tcl_rc = rc;
  return rc!=TCL_OK;
}
#endif

/*
** This is an alternative callback for database queries.  Instead
** of invoking a TCL script to handle the result, this callback just
** appends each column of the result to a list.  After the query
** is complete, the list is returned.
*/
static int DbEvalCallback2(
  void *clientData,      /* An instance of CallbackData */
  int nCol,              /* Number of columns in the result */
  char ** azCol,         /* Data for each column */
  char ** azN            /* Name for each column */
){
  Tcl_Obj *pList = (Tcl_Obj*)clientData;
  int i;
  if( azCol==0 ) return 0;
  for(i=0; i<nCol; i++){
    Tcl_Obj *pElem;
    if( azCol[i] && *azCol[i] ){
#ifdef UTF_TRANSLATION_NEEDED
      Tcl_DString dCol;
      Tcl_DStringInit(&dCol);
      Tcl_ExternalToUtfDString(NULL, azCol[i], -1, &dCol);
      pElem = Tcl_NewStringObj(Tcl_DStringValue(&dCol), -1);
      Tcl_DStringFree(&dCol);
#else
      pElem = Tcl_NewStringObj(azCol[i], -1);
#endif
    }else{
      pElem = Tcl_NewObj();
    }
    Tcl_ListObjAppendElement(0, pList, pElem);
  }
  return 0;
}

/*
** Called when the command is deleted.
*/
static void DbDeleteCmd(void *db){
  SqliteDb *pDb = (SqliteDb*)db;
  sqlite_close(pDb->db);
  if( pDb->zBusy ){
    Tcl_Free(pDb->zBusy);
  }
  Tcl_Free((char*)pDb);
}

/*
** This routine is called when a database file is locked while trying
** to execute SQL.
*/
static int DbBusyHandler(void *cd, const char *zTable, int nTries){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;
  char zVal[30];
  char *zCmd;
  Tcl_DString cmd;

  Tcl_DStringInit(&cmd);
  Tcl_DStringAppend(&cmd, pDb->zBusy, -1);
  Tcl_DStringAppendElement(&cmd, zTable);
  sprintf(zVal, " %d", nTries);
  Tcl_DStringAppend(&cmd, zVal, -1);
  zCmd = Tcl_DStringValue(&cmd);
  rc = Tcl_Eval(pDb->interp, zCmd);
  Tcl_DStringFree(&cmd);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 0;
  }
  return 1;
}

/*
** The "sqlite" command below creates a new Tcl command for each
** connection it opens to an SQLite database.  This routine is invoked
** whenever one of those connection-specific commands is executed
** in Tcl.  For example, if you run Tcl code like this:
**
**       sqlite db1  "my_database"
**       db1 close
**
** The first command opens a connection to the "my_database" database
** and calls that connection "db1".  The second command causes this
** subroutine to be invoked.
*/
static int DbObjCmd(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  SqliteDb *pDb = (SqliteDb*)cd;
  int choice;
  static char *DB_strs[] = {
    "busy",               "changes",           "close",
    "complete",           "eval",              "last_insert_rowid",
    "open_aux_file",      "timeout",           0
  };
  enum DB_enum {
    DB_BUSY,              DB_CHANGES,          DB_CLOSE,
    DB_COMPLETE,          DB_EVAL,             DB_LAST_INSERT_ROWID,
    DB_OPEN_AUX_FILE,     DB_TIMEOUT,          
  };

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], DB_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  switch( (enum DB_enum)choice ){

  /*    $db busy ?CALLBACK?
  **
  ** Invoke the given callback if an SQL statement attempts to open
  ** a locked database file.
  */
  case DB_BUSY: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "CALLBACK");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zBusy ){
        Tcl_AppendResult(interp, pDb->zBusy, 0);
      }
    }else{
      char *zBusy;
      int len;
      if( pDb->zBusy ){
        Tcl_Free(pDb->zBusy);
      }
      zBusy = Tcl_GetStringFromObj(objv[2], &len);
      if( zBusy && len>0 ){
        pDb->zBusy = Tcl_Alloc( len + 1 );
        strcpy(pDb->zBusy, zBusy);
      }else{
        pDb->zBusy = 0;
      }
      if( pDb->zBusy ){
        pDb->interp = interp;
        sqlite_busy_handler(pDb->db, DbBusyHandler, pDb);
      }else{
        sqlite_busy_handler(pDb->db, 0, 0);
      }
    }
    break;
  }

  /*
  **     $db changes
  **
  ** Return the number of rows that were modified, inserted, or deleted by
  ** the most recent "eval".
  */
  case DB_CHANGES: {
    Tcl_Obj *pResult;
    int nChange;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    nChange = sqlite_changes(pDb->db);
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetIntObj(pResult, nChange);
    break;
  }

  /*    $db close
  **
  ** Shutdown the database
  */
  case DB_CLOSE: {
    Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
    break;
  }

  /*    $db complete SQL
  **
  ** Return TRUE if SQL is a complete SQL statement.  Return FALSE if
  ** additional lines of input are needed.  This is similar to the
  ** built-in "info complete" command of Tcl.
  */
  case DB_COMPLETE: {
    Tcl_Obj *pResult;
    int isComplete;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SQL");
      return TCL_ERROR;
    }
    isComplete = sqlite_complete( Tcl_GetStringFromObj(objv[2], 0) );
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetBooleanObj(pResult, isComplete);
    break;
  }
   
  /*
  **    $db eval $sql ?array {  ...code... }?
  **
  ** The SQL statement in $sql is evaluated.  For each row, the values are
  ** placed in elements of the array named "array" and ...code... is executed.
  ** If "array" and "code" are omitted, then no callback is every invoked.
  ** If "array" is an empty string, then the values are placed in variables
  ** that have the same name as the fields extracted by the query.
  */
  case DB_EVAL: {
    CallbackData cbData;
    char *zErrMsg;
    char *zSql;
    int rc;
#ifdef UTF_TRANSLATION_NEEDED
    Tcl_DString dSql;
    int i;
#endif

    if( objc!=5 && objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SQL ?ARRAY-NAME CODE?");
      return TCL_ERROR;
    }
    pDb->interp = interp;
    zSql = Tcl_GetStringFromObj(objv[2], 0);
#ifdef UTF_TRANSLATION_NEEDED
    Tcl_DStringInit(&dSql);
    Tcl_UtfToExternalDString(NULL, zSql, -1, &dSql);
    zSql = Tcl_DStringValue(&dSql);
#endif
    Tcl_IncrRefCount(objv[2]);
    if( objc==5 ){
      cbData.interp = interp;
      cbData.once = 1;
      cbData.zArray = Tcl_GetStringFromObj(objv[3], 0);
      cbData.pCode = objv[4];
      cbData.tcl_rc = TCL_OK;
      cbData.nColName = 0;
      cbData.azColName = 0;
      zErrMsg = 0;
      Tcl_IncrRefCount(objv[3]);
      Tcl_IncrRefCount(objv[4]);
      rc = sqlite_exec(pDb->db, zSql, DbEvalCallback, &cbData, &zErrMsg);
      Tcl_DecrRefCount(objv[4]);
      Tcl_DecrRefCount(objv[3]);
      if( cbData.tcl_rc==TCL_BREAK ){ cbData.tcl_rc = TCL_OK; }
    }else{
      Tcl_Obj *pList = Tcl_NewObj();
      cbData.tcl_rc = TCL_OK;
      rc = sqlite_exec(pDb->db, zSql, DbEvalCallback2, pList, &zErrMsg);
      Tcl_SetObjResult(interp, pList);
    }
    if( zErrMsg ){
      Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
      free(zErrMsg);
      rc = TCL_ERROR;
    }else if( rc!=SQLITE_OK && rc!=SQLITE_ABORT ){
      Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
      rc = TCL_ERROR;
    }else{
      rc = cbData.tcl_rc;
    }
    Tcl_DecrRefCount(objv[2]);
#ifdef UTF_TRANSLATION_NEEDED
    Tcl_DStringFree(&dSql);
    if( objc==5 && cbData.azColName ){
      for(i=0; i<cbData.nColName; i++){
        if( cbData.azColName[i] ) free(cbData.azColName[i]);
      }
      free(cbData.azColName);
      cbData.azColName = 0;
    }
#endif
    return rc;
  }

  /*
  **     $db last_insert_rowid 
  **
  ** Return an integer which is the ROWID for the most recent insert.
  */
  case DB_LAST_INSERT_ROWID: {
    Tcl_Obj *pResult;
    int rowid;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    rowid = sqlite_last_insert_rowid(pDb->db);
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetIntObj(pResult, rowid);
    break;
  }

  /*
  **     $db open_aux_file  FILENAME
  **
  ** Begin using FILENAME as the database file used to store temporary
  ** tables.
  */
  case DB_OPEN_AUX_FILE: {
    const char *zFilename;
    char *zErrMsg = 0;
    int rc;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "FILENAME");
      return TCL_ERROR;
    }
    zFilename = Tcl_GetStringFromObj(objv[2], 0);
    rc = sqlite_open_aux_file(pDb->db, zFilename, &zErrMsg);
    if( rc!=0 ){
      if( zErrMsg ){
        Tcl_AppendResult(interp, zErrMsg, 0);
        free(zErrMsg);
      }else{
        Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
      }
      return TCL_ERROR;
    }
    break;
  }

  /*
  **     $db timeout MILLESECONDS
  **
  ** Delay for the number of milliseconds specified when a file is locked.
  */
  case DB_TIMEOUT: {
    int ms;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "MILLISECONDS");
      return TCL_ERROR;
    }
    if( Tcl_GetIntFromObj(interp, objv[2], &ms) ) return TCL_ERROR;
    sqlite_busy_timeout(pDb->db, ms);
    break;
  }
  } /* End of the SWITCH statement */
  return TCL_OK;
}

/*
**   sqlite DBNAME FILENAME ?MODE?
**
** This is the main Tcl command.  When the "sqlite" Tcl command is
** invoked, this routine runs to process that command.
**
** The first argument, DBNAME, is an arbitrary name for a new
** database connection.  This command creates a new command named
** DBNAME that is used to control that connection.  The database
** connection is deleted when the DBNAME command is deleted.
**
** The second argument is the name of the directory that contains
** the sqlite database that is to be accessed.
**
** For testing purposes, we also support the following:
**
**  sqlite -encoding
**
**       Return the encoding used by LIKE and GLOB operators.  Choices
**       are UTF-8 and iso8859.
**
**  sqlite -tcl-uses-utf
**
**       Return "1" if compiled with a Tcl uses UTF-8.  Return "0" if
**       not.  Used by tests to make sure the library was compiled 
**       correctly.
*/
static int DbMain(void *cd, Tcl_Interp *interp, int argc, char **argv){
  int mode;
  SqliteDb *p;
  char *zErrMsg;
  if( argc==2 ){
    if( strcmp(argv[1],"-encoding")==0 ){
      Tcl_AppendResult(interp,sqlite_encoding,0);
      return TCL_OK;
    }
    if( strcmp(argv[1],"-tcl-uses-utf")==0 ){
#ifdef TCL_UTF_MAX
      Tcl_AppendResult(interp,"1",0);
#else
      Tcl_AppendResult(interp,"0",0);
#endif
      return TCL_OK;
    }
  }
  if( argc!=3 && argc!=4 ){
    Tcl_AppendResult(interp,"wrong # args: should be \"", argv[0],
       " HANDLE FILENAME ?MODE?\"", 0);
    return TCL_ERROR;
  }
  if( argc==3 ){
    mode = 0666;
  }else if( Tcl_GetInt(interp, argv[3], &mode)!=TCL_OK ){
    return TCL_ERROR;
  }
  zErrMsg = 0;
  p = (SqliteDb*)Tcl_Alloc( sizeof(*p) );
  if( p==0 ){
    Tcl_SetResult(interp, "malloc failed", TCL_STATIC);
    return TCL_ERROR;
  }
  memset(p, 0, sizeof(*p));
  p->db = sqlite_open(argv[2], mode, &zErrMsg);
  if( p->db==0 ){
    Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
    Tcl_Free((char*)p);
    free(zErrMsg);
    return TCL_ERROR;
  }
  Tcl_CreateObjCommand(interp, argv[1], DbObjCmd, (char*)p, DbDeleteCmd);

  /* If compiled with SQLITE_TEST turned on, then register the "md5sum"
  ** SQL function and return an integer which is the memory address of
  ** the underlying sqlite* pointer.
  */
#ifdef SQLITE_TEST
  {
    char zBuf[40];
    extern void Md5_Register(sqlite*);
    Md5_Register(p->db);
    sprintf(zBuf, "%d", (int)p->db);
    Tcl_AppendResult(interp, zBuf, 0);
  }
#endif  
  return TCL_OK;
}

/*
** Provide a dummy Tcl_InitStubs if we are using this as a static
** library.
*/
#ifndef USE_TCL_STUBS
# undef  Tcl_InitStubs
# define Tcl_InitStubs(a,b,c)
#endif

/*
** Initialize this module.
**
** This Tcl module contains only a single new Tcl command named "sqlite".
** (Hence there is no namespace.  There is no point in using a namespace
** if the extension only supplies one new name!)  The "sqlite" command is
** used to open a new SQLite database.  See the DbMain() routine above
** for additional information.
*/
int Sqlite_Init(Tcl_Interp *interp){
  Tcl_InitStubs(interp, "8.0", 0);
  Tcl_CreateCommand(interp, "sqlite", DbMain, 0, 0);
  Tcl_PkgProvide(interp, "sqlite", "2.0");
  return TCL_OK;
}
int Tclsqlite_Init(Tcl_Interp *interp){
  Tcl_InitStubs(interp, "8.0", 0);
  Tcl_CreateCommand(interp, "sqlite", DbMain, 0, 0);
  Tcl_PkgProvide(interp, "sqlite", "2.0");
  return TCL_OK;
}
int Sqlite_SafeInit(Tcl_Interp *interp){
  return TCL_OK;
}
int Tclsqlite_SafeInit(Tcl_Interp *interp){
  return TCL_OK;
}

#if 0
/*
** If compiled using mktclapp, this routine runs to initialize
** everything.
*/
int Et_AppInit(Tcl_Interp *interp){
  return Sqlite_Init(interp);
}
#endif

/*
** If the macro TCLSH is defined and is one, then put in code for the
** "main" routine that will initialize Tcl.
*/
#if defined(TCLSH) && TCLSH==1
static char zMainloop[] =
  "set line {}\n"
  "while {![eof stdin]} {\n"
    "if {$line!=\"\"} {\n"
      "puts -nonewline \"> \"\n"
    "} else {\n"
      "puts -nonewline \"% \"\n"
    "}\n"
    "flush stdout\n"
    "append line [gets stdin]\n"
    "if {[info complete $line]} {\n"
      "if {[catch {uplevel #0 $line} result]} {\n"
        "puts stderr \"Error: $result\"\n"
      "} elseif {$result!=\"\"} {\n"
        "puts $result\n"
      "}\n"
      "set line {}\n"
    "} else {\n"
      "append line \\n\n"
    "}\n"
  "}\n"
;

#define TCLSH_MAIN main   /* Needed to fake out mktclapp */
int TCLSH_MAIN(int argc, char **argv){
  Tcl_Interp *interp;
  Tcl_FindExecutable(argv[0]);
  interp = Tcl_CreateInterp();
  Sqlite_Init(interp);
#ifdef SQLITE_TEST
  {
    extern int Sqlitetest1_Init(Tcl_Interp*);
    extern int Sqlitetest2_Init(Tcl_Interp*);
    extern int Sqlitetest3_Init(Tcl_Interp*);
    extern int Md5_Init(Tcl_Interp*);
    Sqlitetest1_Init(interp);
    Sqlitetest2_Init(interp);
    Sqlitetest3_Init(interp);
    Md5_Init(interp);
  }
#endif
  if( argc>=2 ){
    int i;
    Tcl_SetVar(interp,"argv0",argv[1],TCL_GLOBAL_ONLY);
    Tcl_SetVar(interp,"argv", "", TCL_GLOBAL_ONLY);
    for(i=2; i<argc; i++){
      Tcl_SetVar(interp, "argv", argv[i],
          TCL_GLOBAL_ONLY | TCL_LIST_ELEMENT | TCL_APPEND_VALUE);
    }
    if( Tcl_EvalFile(interp, argv[1])!=TCL_OK ){
      char *zInfo = Tcl_GetVar(interp, "errorInfo", TCL_GLOBAL_ONLY);
      if( zInfo==0 ) zInfo = interp->result;
      fprintf(stderr,"%s: %s\n", *argv, zInfo);
      return 1;
    }
  }else{
    Tcl_GlobalEval(interp, zMainloop);
  }
  return 0;
}
#endif /* TCLSH */

#endif /* !defined(NO_TCL) */
