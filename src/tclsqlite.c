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
** A TCL Interface to SQLite
**
** $Id: tclsqlite.c,v 1.1 2000/05/29 14:26:01 drh Exp $
*/
#include "sqlite.h"
#include <tcl.h>
#include <stdlib.h>
#include <string.h>

/*
** An instance of this structure passes information thru the sqlite
** logic from the original TCL command into the callback routine.
*/
typedef struct CallbackData CallbackData;
struct CallbackData {
  Tcl_Interp *interp;       /* The TCL interpreter */
  char *zArray;             /* The array into which data is written */
  char *zCode;              /* The code to execute for each row */
  int once;                 /* Set only for the first invocation of callback */
};

/*
** Called for each row of the result.
*/
static int DbEvalCallback(
  void *clientData,      /* An instance of CallbackData */
  int nCol,              /* Number of columns in the result */
  char ** azCol,         /* Data for each column */
  char ** azN            /* Name for each column */
){
  CallbackData *cbData = (CallbackData*)clientData;
  int i, rc;
  if( cbData->zArray[0] ){
    if( cbData->once ){
      for(i=0; i<nCol; i++){
        Tcl_SetVar2(cbData->interp, cbData->zArray, "*", azN[i],
           TCL_LIST_ELEMENT|TCL_APPEND_VALUE);
      }
    }
    for(i=0; i<nCol; i++){
      Tcl_SetVar2(cbData->interp, cbData->zArray, azN[i], azCol[i], 0);
    }
  }else{
    for(i=0; i<nCol; i++){
      Tcl_SetVar(cbData->interp, azN[i], azCol[i], 0);
    }
  }
  cbData->once = 0;
  rc = Tcl_Eval(cbData->interp, cbData->zCode);
  return rc;
}

/*
** Called when the command is deleted.
*/
static void DbDeleteCmd(void *db){
  sqlite_close((sqlite*)db);
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
static int DbCmd(void *cd, Tcl_Interp *interp, int argc, char **argv){
  char *z;
  int n, c;
  sqlite *db = cd;
  if( argc<2 ){
    Tcl_AppendResult(interp,"wrong # args: should be \"", argv[0],
        " SUBCOMMAND ...\"", 0);
    return TCL_ERROR;
  }
  z = argv[1];
  n = strlen(z);
  c = z[0];

  /*    $db close
  **
  ** Shutdown the database
  */
  if( c=='c' && n>=2 && strncmp(z,"close",n)==0 ){
    Tcl_DeleteCommand(interp, argv[0]);
  }else

  /*    $db complete SQL
  **
  ** Return TRUE if SQL is a complete SQL statement.  Return FALSE if
  ** additional lines of input are needed.  This is similar to the
  ** built-in "info complete" command of Tcl.
  */
  if( c=='c' && n>=2 && strncmp(z,"complete",n)==0 ){
    char *zRes;
    if( argc!=3 ){
      Tcl_AppendResult(interp,"wrong # args: should be \"", argv[0],
          " complete SQL\"", 0);
      return TCL_ERROR;
    }
    zRes = sqlite_complete(argv[2]) ? "1" : "0";
    Tcl_SetResult(interp, zRes, TCL_VOLATILE);
  }else
   
  /*
  **    $db eval $sql ?array {  ...code... }?
  **
  ** The SQL statement in $sql is evaluated.  For each row, the values are
  ** placed in elements of the array named "array" and ...code.. is executed.
  ** If "array" and "code" are omitted, then no callback is every invoked.
  ** If "array" is an empty string, then the values are placed in variables
  ** that have the same name as the fields extracted by the query.
  */
  if( c=='e' && strncmp(z,"eval",n)==0 ){
    CallbackData cbData;
    char *zErrMsg;
    int rc;

    if( argc!=5 && argc!=3 ){
      Tcl_AppendResult(interp,"wrong # args: should be \"", argv[0],
         " eval SQL ?ARRAY-NAME CODE?", 0);
      return TCL_ERROR;
    }
    if( argc==5 ){
      cbData.interp = interp;
      cbData.zArray = argv[3];
      cbData.zCode = argv[4];
      zErrMsg = 0;
      rc = sqlite_exec(db, argv[2], DbEvalCallback, &cbData, &zErrMsg);
    }else{
      rc = sqlite_exec(db, argv[2], 0, 0, &zErrMsg);
    }
    if( zErrMsg ){
      Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
      free(zErrMsg);
    }
    return rc;
  }

  /* The default
  */
  else{
    Tcl_AppendResult(interp,"unknown subcommand \"", z, 
        "\" - should be one of: close complete eval", 0);
    return TCL_ERROR;
  }
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
*/
static int DbMain(void *cd, Tcl_Interp *interp, int argc, char **argv){
  int mode;
  sqlite *p;
  char *zErrMsg;
  if( argc!=3 && argc!=4 ){
    Tcl_AppendResult(interp,"wrong # args: should be \"", argv[0],
       " HANDLE FILENAME ?MODE?\"", 0);
    return TCL_ERROR;
  }
  if( argc==3 ){
    mode = 0;
  }else if( Tcl_GetInt(interp, argv[3], &mode)!=TCL_OK ){
    return TCL_ERROR;
  }
  zErrMsg = 0;
  p = sqlite_open(argv[2], mode, &zErrMsg);
  if( p==0 ){
    Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
    free(zErrMsg);
    return TCL_ERROR;
  }
  Tcl_CreateCommand(interp, argv[1], DbCmd, p, DbDeleteCmd);
  return TCL_OK;
}

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
  Tcl_CreateCommand(interp, "sqlite", DbMain, 0, 0);
  return TCL_OK;
}
int Sqlite_SafeInit(Tcl_Interp *interp){
  return TCL_OK;
}

/*
** If compiled using mktclapp, this routine runs to initialize
** everything.
*/
int Et_AppInit(Tcl_Interp *interp){
  return Sqlite_Init(interp);
}
