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
** $Id: tclsqlite.c,v 1.59.2.1 2004/06/19 11:57:40 drh Exp $
*/
#ifndef NO_TCL     /* Omit this whole file if TCL is unavailable */

#include "sqliteInt.h"
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
** New SQL functions can be created as TCL scripts.  Each such function
** is described by an instance of the following structure.
*/
typedef struct SqlFunc SqlFunc;
struct SqlFunc {
  Tcl_Interp *interp;   /* The TCL interpret to execute the function */
  char *zScript;        /* The script to be run */
  SqlFunc *pNext;       /* Next function on the list of them all */
};

/*
** There is one instance of this structure for each SQLite database
** that has been opened by the SQLite TCL interface.
*/
typedef struct SqliteDb SqliteDb;
struct SqliteDb {
  sqlite *db;           /* The "real" database structure */
  Tcl_Interp *interp;   /* The interpreter used for this database */
  char *zBusy;          /* The busy callback routine */
  char *zCommit;        /* The commit hook callback routine */
  char *zTrace;         /* The trace callback routine */
  char *zProgress;      /* The progress callback routine */
  char *zAuth;          /* The authorization callback routine */
  SqlFunc *pFunc;       /* List of SQL functions */
  int rc;               /* Return code of most recent sqlite_exec() */
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
        if( azN[nCol]!=0 ){
          Tcl_DString dType;
          Tcl_DStringInit(&dType);
          Tcl_DStringAppend(&dType, "typeof:", -1);
          Tcl_DStringAppend(&dType, Tcl_DStringValue(&dCol), -1);
          Tcl_DStringFree(&dCol);
          Tcl_ExternalToUtfDString(NULL, azN[i+nCol], -1, &dCol);
          Tcl_SetVar2(cbData->interp, cbData->zArray, 
               Tcl_DStringValue(&dType), Tcl_DStringValue(&dCol),
               TCL_LIST_ELEMENT|TCL_APPEND_VALUE);
          Tcl_DStringFree(&dType);
        }
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
      if( azN[nCol] ){
        char *z = sqlite_mprintf("typeof:%s", azN[i]);
        Tcl_SetVar2(cbData->interp, cbData->zArray, z, azN[i+nCol],
           TCL_LIST_ELEMENT|TCL_APPEND_VALUE);
        sqlite_freemem(z);
      }
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
** This is a second alternative callback for database queries.  A the
** first column of the first row of the result is made the TCL result.
*/
static int DbEvalCallback3(
  void *clientData,      /* An instance of CallbackData */
  int nCol,              /* Number of columns in the result */
  char ** azCol,         /* Data for each column */
  char ** azN            /* Name for each column */
){
  Tcl_Interp *interp = (Tcl_Interp*)clientData;
  Tcl_Obj *pElem;
  if( azCol==0 ) return 1;
  if( nCol==0 ) return 1;
#ifdef UTF_TRANSLATION_NEEDED
  {
    Tcl_DString dCol;
    Tcl_DStringInit(&dCol);
    Tcl_ExternalToUtfDString(NULL, azCol[0], -1, &dCol);
    pElem = Tcl_NewStringObj(Tcl_DStringValue(&dCol), -1);
    Tcl_DStringFree(&dCol);
  }
#else
  pElem = Tcl_NewStringObj(azCol[0], -1);
#endif
  Tcl_SetObjResult(interp, pElem);
  return 1;
}

/*
** Called when the command is deleted.
*/
static void DbDeleteCmd(void *db){
  SqliteDb *pDb = (SqliteDb*)db;
  sqlite_close(pDb->db);
  while( pDb->pFunc ){
    SqlFunc *pFunc = pDb->pFunc;
    pDb->pFunc = pFunc->pNext;
    Tcl_Free((char*)pFunc);
  }
  if( pDb->zBusy ){
    Tcl_Free(pDb->zBusy);
  }
  if( pDb->zTrace ){
    Tcl_Free(pDb->zTrace);
  }
  if( pDb->zAuth ){
    Tcl_Free(pDb->zAuth);
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
** This routine is invoked as the 'progress callback' for the database.
*/
static int DbProgressHandler(void *cd){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;

  assert( pDb->zProgress );
  rc = Tcl_Eval(pDb->interp, pDb->zProgress);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 1;
  }
  return 0;
}

/*
** This routine is called by the SQLite trace handler whenever a new
** block of SQL is executed.  The TCL script in pDb->zTrace is executed.
*/
static void DbTraceHandler(void *cd, const char *zSql){
  SqliteDb *pDb = (SqliteDb*)cd;
  Tcl_DString str;

  Tcl_DStringInit(&str);
  Tcl_DStringAppend(&str, pDb->zTrace, -1);
  Tcl_DStringAppendElement(&str, zSql);
  Tcl_Eval(pDb->interp, Tcl_DStringValue(&str));
  Tcl_DStringFree(&str);
  Tcl_ResetResult(pDb->interp);
}

/*
** This routine is called when a transaction is committed.  The
** TCL script in pDb->zCommit is executed.  If it returns non-zero or
** if it throws an exception, the transaction is rolled back instead
** of being committed.
*/
static int DbCommitHandler(void *cd){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;

  rc = Tcl_Eval(pDb->interp, pDb->zCommit);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 1;
  }
  return 0;
}

/*
** This routine is called to evaluate an SQL function implemented
** using TCL script.
*/
static void tclSqlFunc(sqlite_func *context, int argc, const char **argv){
  SqlFunc *p = sqlite_user_data(context);
  Tcl_DString cmd;
  int i;
  int rc;

  Tcl_DStringInit(&cmd);
  Tcl_DStringAppend(&cmd, p->zScript, -1);
  for(i=0; i<argc; i++){
    Tcl_DStringAppendElement(&cmd, argv[i] ? argv[i] : "");
  }
  rc = Tcl_Eval(p->interp, Tcl_DStringValue(&cmd));
  if( rc ){
    sqlite_set_result_error(context, Tcl_GetStringResult(p->interp), -1); 
  }else{
    sqlite_set_result_string(context, Tcl_GetStringResult(p->interp), -1);
  }
}
#ifndef SQLITE_OMIT_AUTHORIZATION
/*
** This is the authentication function.  It appends the authentication
** type code and the two arguments to zCmd[] then invokes the result
** on the interpreter.  The reply is examined to determine if the
** authentication fails or succeeds.
*/
static int auth_callback(
  void *pArg,
  int code,
  const char *zArg1,
  const char *zArg2,
  const char *zArg3,
  const char *zArg4
){
  char *zCode;
  Tcl_DString str;
  int rc;
  const char *zReply;
  SqliteDb *pDb = (SqliteDb*)pArg;

  switch( code ){
    case SQLITE_COPY              : zCode="SQLITE_COPY"; break;
    case SQLITE_CREATE_INDEX      : zCode="SQLITE_CREATE_INDEX"; break;
    case SQLITE_CREATE_TABLE      : zCode="SQLITE_CREATE_TABLE"; break;
    case SQLITE_CREATE_TEMP_INDEX : zCode="SQLITE_CREATE_TEMP_INDEX"; break;
    case SQLITE_CREATE_TEMP_TABLE : zCode="SQLITE_CREATE_TEMP_TABLE"; break;
    case SQLITE_CREATE_TEMP_TRIGGER: zCode="SQLITE_CREATE_TEMP_TRIGGER"; break;
    case SQLITE_CREATE_TEMP_VIEW  : zCode="SQLITE_CREATE_TEMP_VIEW"; break;
    case SQLITE_CREATE_TRIGGER    : zCode="SQLITE_CREATE_TRIGGER"; break;
    case SQLITE_CREATE_VIEW       : zCode="SQLITE_CREATE_VIEW"; break;
    case SQLITE_DELETE            : zCode="SQLITE_DELETE"; break;
    case SQLITE_DROP_INDEX        : zCode="SQLITE_DROP_INDEX"; break;
    case SQLITE_DROP_TABLE        : zCode="SQLITE_DROP_TABLE"; break;
    case SQLITE_DROP_TEMP_INDEX   : zCode="SQLITE_DROP_TEMP_INDEX"; break;
    case SQLITE_DROP_TEMP_TABLE   : zCode="SQLITE_DROP_TEMP_TABLE"; break;
    case SQLITE_DROP_TEMP_TRIGGER : zCode="SQLITE_DROP_TEMP_TRIGGER"; break;
    case SQLITE_DROP_TEMP_VIEW    : zCode="SQLITE_DROP_TEMP_VIEW"; break;
    case SQLITE_DROP_TRIGGER      : zCode="SQLITE_DROP_TRIGGER"; break;
    case SQLITE_DROP_VIEW         : zCode="SQLITE_DROP_VIEW"; break;
    case SQLITE_INSERT            : zCode="SQLITE_INSERT"; break;
    case SQLITE_PRAGMA            : zCode="SQLITE_PRAGMA"; break;
    case SQLITE_READ              : zCode="SQLITE_READ"; break;
    case SQLITE_SELECT            : zCode="SQLITE_SELECT"; break;
    case SQLITE_TRANSACTION       : zCode="SQLITE_TRANSACTION"; break;
    case SQLITE_UPDATE            : zCode="SQLITE_UPDATE"; break;
    case SQLITE_ATTACH            : zCode="SQLITE_ATTACH"; break;
    case SQLITE_DETACH            : zCode="SQLITE_DETACH"; break;
    default                       : zCode="????"; break;
  }
  Tcl_DStringInit(&str);
  Tcl_DStringAppend(&str, pDb->zAuth, -1);
  Tcl_DStringAppendElement(&str, zCode);
  Tcl_DStringAppendElement(&str, zArg1 ? zArg1 : "");
  Tcl_DStringAppendElement(&str, zArg2 ? zArg2 : "");
  Tcl_DStringAppendElement(&str, zArg3 ? zArg3 : "");
  Tcl_DStringAppendElement(&str, zArg4 ? zArg4 : "");
  rc = Tcl_GlobalEval(pDb->interp, Tcl_DStringValue(&str));
  Tcl_DStringFree(&str);
  zReply = Tcl_GetStringResult(pDb->interp);
  if( strcmp(zReply,"SQLITE_OK")==0 ){
    rc = SQLITE_OK;
  }else if( strcmp(zReply,"SQLITE_DENY")==0 ){
    rc = SQLITE_DENY;
  }else if( strcmp(zReply,"SQLITE_IGNORE")==0 ){
    rc = SQLITE_IGNORE;
  }else{
    rc = 999;
  }
  return rc;
}
#endif /* SQLITE_OMIT_AUTHORIZATION */

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
  int rc = TCL_OK;
  static const char *DB_strs[] = {
    "authorizer",         "busy",                   "changes",
    "close",              "commit_hook",            "complete",
    "errorcode",          "eval",                   "function",
    "last_insert_rowid",  "last_statement_changes", "onecolumn",
    "progress",           "rekey",                  "timeout",
    "trace",
    0                    
  };
  enum DB_enum {
    DB_AUTHORIZER,        DB_BUSY,                   DB_CHANGES,
    DB_CLOSE,             DB_COMMIT_HOOK,            DB_COMPLETE,
    DB_ERRORCODE,         DB_EVAL,                   DB_FUNCTION,
    DB_LAST_INSERT_ROWID, DB_LAST_STATEMENT_CHANGES, DB_ONECOLUMN,        
    DB_PROGRESS,          DB_REKEY,                  DB_TIMEOUT,
    DB_TRACE
  };

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], DB_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  switch( (enum DB_enum)choice ){

  /*    $db authorizer ?CALLBACK?
  **
  ** Invoke the given callback to authorize each SQL operation as it is
  ** compiled.  5 arguments are appended to the callback before it is
  ** invoked:
  **
  **   (1) The authorization type (ex: SQLITE_CREATE_TABLE, SQLITE_INSERT, ...)
  **   (2) First descriptive name (depends on authorization type)
  **   (3) Second descriptive name
  **   (4) Name of the database (ex: "main", "temp")
  **   (5) Name of trigger that is doing the access
  **
  ** The callback should return on of the following strings: SQLITE_OK,
  ** SQLITE_IGNORE, or SQLITE_DENY.  Any other return value is an error.
  **
  ** If this method is invoked with no arguments, the current authorization
  ** callback string is returned.
  */
  case DB_AUTHORIZER: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
    }else if( objc==2 ){
      if( pDb->zAuth ){
        Tcl_AppendResult(interp, pDb->zAuth, 0);
      }
    }else{
      char *zAuth;
      int len;
      if( pDb->zAuth ){
        Tcl_Free(pDb->zAuth);
      }
      zAuth = Tcl_GetStringFromObj(objv[2], &len);
      if( zAuth && len>0 ){
        pDb->zAuth = Tcl_Alloc( len + 1 );
        strcpy(pDb->zAuth, zAuth);
      }else{
        pDb->zAuth = 0;
      }
#ifndef SQLITE_OMIT_AUTHORIZATION
      if( pDb->zAuth ){
        pDb->interp = interp;
        sqlite_set_authorizer(pDb->db, auth_callback, pDb);
      }else{
        sqlite_set_authorizer(pDb->db, 0, 0);
      }
#endif
    }
    break;
  }

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

  /*    $db progress ?N CALLBACK?
  ** 
  ** Invoke the given callback every N virtual machine opcodes while executing
  ** queries.
  */
  case DB_PROGRESS: {
    if( objc==2 ){
      if( pDb->zProgress ){
        Tcl_AppendResult(interp, pDb->zProgress, 0);
      }
    }else if( objc==4 ){
      char *zProgress;
      int len;
      int N;
      if( TCL_OK!=Tcl_GetIntFromObj(interp, objv[2], &N) ){
	return TCL_ERROR;
      };
      if( pDb->zProgress ){
        Tcl_Free(pDb->zProgress);
      }
      zProgress = Tcl_GetStringFromObj(objv[3], &len);
      if( zProgress && len>0 ){
        pDb->zProgress = Tcl_Alloc( len + 1 );
        strcpy(pDb->zProgress, zProgress);
      }else{
        pDb->zProgress = 0;
      }
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
      if( pDb->zProgress ){
        pDb->interp = interp;
        sqlite_progress_handler(pDb->db, N, DbProgressHandler, pDb);
      }else{
        sqlite_progress_handler(pDb->db, 0, 0, 0);
      }
#endif
    }else{
      Tcl_WrongNumArgs(interp, 2, objv, "N CALLBACK");
      return TCL_ERROR;
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

  /*
  **     $db last_statement_changes
  **
  ** Return the number of rows that were modified, inserted, or deleted by
  ** the last statment to complete execution (excluding changes due to
  ** triggers)
  */
  case DB_LAST_STATEMENT_CHANGES: {
    Tcl_Obj *pResult;
    int lsChange;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    lsChange = sqlite_last_statement_changes(pDb->db);
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetIntObj(pResult, lsChange);
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

  /*    $db commit_hook ?CALLBACK?
  **
  ** Invoke the given callback just before committing every SQL transaction.
  ** If the callback throws an exception or returns non-zero, then the
  ** transaction is aborted.  If CALLBACK is an empty string, the callback
  ** is disabled.
  */
  case DB_COMMIT_HOOK: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
    }else if( objc==2 ){
      if( pDb->zCommit ){
        Tcl_AppendResult(interp, pDb->zCommit, 0);
      }
    }else{
      char *zCommit;
      int len;
      if( pDb->zCommit ){
        Tcl_Free(pDb->zCommit);
      }
      zCommit = Tcl_GetStringFromObj(objv[2], &len);
      if( zCommit && len>0 ){
        pDb->zCommit = Tcl_Alloc( len + 1 );
        strcpy(pDb->zCommit, zCommit);
      }else{
        pDb->zCommit = 0;
      }
      if( pDb->zCommit ){
        pDb->interp = interp;
        sqlite_commit_hook(pDb->db, DbCommitHandler, pDb);
      }else{
        sqlite_commit_hook(pDb->db, 0, 0);
      }
    }
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
  **    $db errorcode
  **
  ** Return the numeric error code that was returned by the most recent
  ** call to sqlite_exec().
  */
  case DB_ERRORCODE: {
    Tcl_SetObjResult(interp, Tcl_NewIntObj(pDb->rc));
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
    pDb->rc = rc;
    if( rc==SQLITE_ABORT ){
      if( zErrMsg ) free(zErrMsg);
      rc = cbData.tcl_rc;
    }else if( zErrMsg ){
      Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
      free(zErrMsg);
      rc = TCL_ERROR;
    }else if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
      rc = TCL_ERROR;
    }else{
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
  **     $db function NAME SCRIPT
  **
  ** Create a new SQL function called NAME.  Whenever that function is
  ** called, invoke SCRIPT to evaluate the function.
  */
  case DB_FUNCTION: {
    SqlFunc *pFunc;
    char *zName;
    char *zScript;
    int nScript;
    if( objc!=4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "NAME SCRIPT");
      return TCL_ERROR;
    }
    zName = Tcl_GetStringFromObj(objv[2], 0);
    zScript = Tcl_GetStringFromObj(objv[3], &nScript);
    pFunc = (SqlFunc*)Tcl_Alloc( sizeof(*pFunc) + nScript + 1 );
    if( pFunc==0 ) return TCL_ERROR;
    pFunc->interp = interp;
    pFunc->pNext = pDb->pFunc;
    pFunc->zScript = (char*)&pFunc[1];
    strcpy(pFunc->zScript, zScript);
    sqlite_create_function(pDb->db, zName, -1, tclSqlFunc, pFunc);
    sqlite_function_type(pDb->db, zName, SQLITE_NUMERIC);
    break;
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
  **     $db onecolumn SQL
  **
  ** Return a single column from a single row of the given SQL query.
  */
  case DB_ONECOLUMN: {
    char *zSql;
    char *zErrMsg = 0;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SQL");
      return TCL_ERROR;
    }
    zSql = Tcl_GetStringFromObj(objv[2], 0);
    rc = sqlite_exec(pDb->db, zSql, DbEvalCallback3, interp, &zErrMsg);
    if( rc==SQLITE_ABORT ){
      rc = SQLITE_OK;
    }else if( zErrMsg ){
      Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
      free(zErrMsg);
      rc = TCL_ERROR;
    }else if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
      rc = TCL_ERROR;
    }
    break;
  }

  /*
  **     $db rekey KEY
  **
  ** Change the encryption key on the currently open database.
  */
  case DB_REKEY: {
    int nKey;
    void *pKey;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "KEY");
      return TCL_ERROR;
    }
    pKey = Tcl_GetByteArrayFromObj(objv[2], &nKey);
#ifdef SQLITE_HAS_CODEC
    rc = sqlite_rekey(pDb->db, pKey, nKey);
    if( rc ){
      Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
      rc = TCL_ERROR;
    }
#endif
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

  /*    $db trace ?CALLBACK?
  **
  ** Make arrangements to invoke the CALLBACK routine for each SQL statement
  ** that is executed.  The text of the SQL is appended to CALLBACK before
  ** it is executed.
  */
  case DB_TRACE: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
    }else if( objc==2 ){
      if( pDb->zTrace ){
        Tcl_AppendResult(interp, pDb->zTrace, 0);
      }
    }else{
      char *zTrace;
      int len;
      if( pDb->zTrace ){
        Tcl_Free(pDb->zTrace);
      }
      zTrace = Tcl_GetStringFromObj(objv[2], &len);
      if( zTrace && len>0 ){
        pDb->zTrace = Tcl_Alloc( len + 1 );
        strcpy(pDb->zTrace, zTrace);
      }else{
        pDb->zTrace = 0;
      }
      if( pDb->zTrace ){
        pDb->interp = interp;
        sqlite_trace(pDb->db, DbTraceHandler, pDb);
      }else{
        sqlite_trace(pDb->db, 0, 0);
      }
    }
    break;
  }

  } /* End of the SWITCH statement */
  return rc;
}

/*
**   sqlite DBNAME FILENAME ?MODE? ?-key KEY?
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
**  sqlite -version
**
**       Return the version number of the SQLite library.
**
**  sqlite -tcl-uses-utf
**
**       Return "1" if compiled with a Tcl uses UTF-8.  Return "0" if
**       not.  Used by tests to make sure the library was compiled 
**       correctly.
*/
static int DbMain(void *cd, Tcl_Interp *interp, int objc,Tcl_Obj *const*objv){
  int mode;
  SqliteDb *p;
  void *pKey = 0;
  int nKey = 0;
  const char *zArg;
  char *zErrMsg;
  const char *zFile;
  char zBuf[80];
  if( objc==2 ){
    zArg = Tcl_GetStringFromObj(objv[1], 0);
    if( strcmp(zArg,"-encoding")==0 ){
      Tcl_AppendResult(interp,sqlite_encoding,0);
      return TCL_OK;
    }
    if( strcmp(zArg,"-version")==0 ){
      Tcl_AppendResult(interp,sqlite_version,0);
      return TCL_OK;
    }
    if( strcmp(zArg,"-has-codec")==0 ){
#ifdef SQLITE_HAS_CODEC
      Tcl_AppendResult(interp,"1",0);
#else
      Tcl_AppendResult(interp,"0",0);
#endif
      return TCL_OK;
    }
    if( strcmp(zArg,"-tcl-uses-utf")==0 ){
#ifdef TCL_UTF_MAX
      Tcl_AppendResult(interp,"1",0);
#else
      Tcl_AppendResult(interp,"0",0);
#endif
      return TCL_OK;
    }
  }
  if( objc==5 || objc==6 ){
    zArg = Tcl_GetStringFromObj(objv[objc-2], 0);
    if( strcmp(zArg,"-key")==0 ){
      pKey = Tcl_GetByteArrayFromObj(objv[objc-1], &nKey);
      objc -= 2;
    }
  }
  if( objc!=3 && objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, 
#ifdef SQLITE_HAS_CODEC
      "HANDLE FILENAME ?-key CODEC-KEY?"
#else
      "HANDLE FILENAME ?MODE?"
#endif
    );
    return TCL_ERROR;
  }
  if( objc==3 ){
    mode = 0666;
  }else if( Tcl_GetIntFromObj(interp, objv[3], &mode)!=TCL_OK ){
    return TCL_ERROR;
  }
  zErrMsg = 0;
  p = (SqliteDb*)Tcl_Alloc( sizeof(*p) );
  if( p==0 ){
    Tcl_SetResult(interp, "malloc failed", TCL_STATIC);
    return TCL_ERROR;
  }
  memset(p, 0, sizeof(*p));
  zFile = Tcl_GetStringFromObj(objv[2], 0);
#ifdef SQLITE_HAS_CODEC
  p->db = sqlite_open_encrypted(zFile, pKey, nKey, 0, &zErrMsg);
#else
  p->db = sqlite_open(zFile, mode, &zErrMsg);
#endif
  if( p->db==0 ){
    Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
    Tcl_Free((char*)p);
    free(zErrMsg);
    return TCL_ERROR;
  }
  zArg = Tcl_GetStringFromObj(objv[1], 0);
  Tcl_CreateObjCommand(interp, zArg, DbObjCmd, (char*)p, DbDeleteCmd);

  /* The return value is the value of the sqlite* pointer
  */
  sprintf(zBuf, "%p", p->db);
  if( strncmp(zBuf,"0x",2) ){
    sprintf(zBuf, "0x%p", p->db);
  }
  Tcl_AppendResult(interp, zBuf, 0);

  /* If compiled with SQLITE_TEST turned on, then register the "md5sum"
  ** SQL function.
  */
#ifdef SQLITE_TEST
  {
    extern void Md5_Register(sqlite*);
    Md5_Register(p->db);
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
  Tcl_CreateObjCommand(interp, "sqlite", (Tcl_ObjCmdProc*)DbMain, 0, 0);
  Tcl_PkgProvide(interp, "sqlite", "2.0");
  return TCL_OK;
}
int Tclsqlite_Init(Tcl_Interp *interp){
  Tcl_InitStubs(interp, "8.0", 0);
  Tcl_CreateObjCommand(interp, "sqlite", (Tcl_ObjCmdProc*)DbMain, 0, 0);
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
/***************************************************************************
** The remaining code is only included if the TCLSH macro is defined to
** be an integer greater than 0
*/
#if defined(TCLSH) && TCLSH>0

/*
** If the macro TCLSH is defined and is one, then put in code for the
** "main" routine that implement a interactive shell into which the user
** can type TCL commands.
*/
#if TCLSH==1
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
#endif /* TCLSH==1 */

int Libsqlite_Init( Tcl_Interp *interp) {
#ifdef TCL_THREADS
  if (Thread_Init(interp) == TCL_ERROR) {
    return TCL_ERROR;
  }
#endif
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
    Tcl_StaticPackage(interp, "sqlite", Libsqlite_Init, Libsqlite_Init);
  }
#endif
  return TCL_OK;
}

#define TCLSH_MAIN main   /* Needed to fake out mktclapp */
#if TCLSH==1
int TCLSH_MAIN(int argc, char **argv){
#ifndef TCL_THREADS
  Tcl_Interp *interp;
  Tcl_FindExecutable(argv[0]);
  interp = Tcl_CreateInterp();
  Libsqlite_Init(interp);
  if( argc>=2 ){
    int i;
    Tcl_SetVar(interp,"argv0",argv[1],TCL_GLOBAL_ONLY);
    Tcl_SetVar(interp,"argv", "", TCL_GLOBAL_ONLY);
    for(i=2; i<argc; i++){
      Tcl_SetVar(interp, "argv", argv[i],
          TCL_GLOBAL_ONLY | TCL_LIST_ELEMENT | TCL_APPEND_VALUE);
    }
    if( Tcl_EvalFile(interp, argv[1])!=TCL_OK ){
      const char *zInfo = Tcl_GetVar(interp, "errorInfo", TCL_GLOBAL_ONLY);
      if( zInfo==0 ) zInfo = interp->result;
      fprintf(stderr,"%s: %s\n", *argv, zInfo);
      return TCL_ERROR;
    }
  }else{
    Tcl_GlobalEval(interp, zMainloop);
  }
  return 0;
#else
  Tcl_Main(argc, argv, Libsqlite_Init);
#endif /* TCL_THREADS */
  return 0;
}
#endif /* TCLSH==1 */


/*
** If the macro TCLSH is set to 2, then implement a space analysis tool.
*/
#if TCLSH==2
static char zAnalysis[] = 
#include "spaceanal_tcl.h"
;

int main(int argc, char **argv){
  Tcl_Interp *interp;
  int i;
  Tcl_FindExecutable(argv[0]);
  interp = Tcl_CreateInterp();
  Libsqlite_Init(interp);
  Tcl_SetVar(interp,"argv0",argv[0],TCL_GLOBAL_ONLY);
  Tcl_SetVar(interp,"argv", "", TCL_GLOBAL_ONLY);
  for(i=1; i<argc; i++){
    Tcl_SetVar(interp, "argv", argv[i],
        TCL_GLOBAL_ONLY | TCL_LIST_ELEMENT | TCL_APPEND_VALUE);
  }
  if( Tcl_GlobalEval(interp, zAnalysis)!=TCL_OK ){
    const char *zInfo = Tcl_GetVar(interp, "errorInfo", TCL_GLOBAL_ONLY);
    if( zInfo==0 ) zInfo = interp->result;
    fprintf(stderr,"%s: %s\n", *argv, zInfo);
    return TCL_ERROR;
  }
  return 0;
}
#endif /* TCLSH==2 */

#endif /* TCLSH */

#endif /* NO_TCL */
