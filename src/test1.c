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
** Code for testing the printf() interface to SQLite.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
**
** $Id: test1.c,v 1.4 2001/09/24 03:12:40 drh Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>

/*
** Usage:   sqlite_open filename
**
** Returns:  The name of an open database.
*/
static int sqlite_test_open(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  char *zErr = 0;
  char zBuf[100];
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME\"", 0);
    return TCL_ERROR;
  }
  db = sqlite_open(argv[1], 0666, &zErr);
  if( db==0 ){
    Tcl_AppendResult(interp, zErr, 0);
    free(zErr);
    return TCL_ERROR;
  }
  sprintf(zBuf,"%d",(int)db);
  Tcl_AppendResult(interp, zBuf, 0);
  return TCL_OK;
}

/*
** The callback routine for sqlite_exec_printf().
*/
static int exec_printf_cb(void *pArg, int argc, char **argv, char **name){
  Tcl_DString *str = (Tcl_DString*)pArg;
  int i;

  if( Tcl_DStringLength(str)==0 ){
    for(i=0; i<argc; i++){
      Tcl_DStringAppendElement(str, name[i] ? name[i] : "NULL");
    }
  }
  for(i=0; i<argc; i++){
    Tcl_DStringAppendElement(str, argv[i] ? argv[i] : "NULL");
  }
  return 0;
}

/*
** Usage:  sqlite_exec_printf  DB  FORMAT  STRING
**
** Invoke the sqlite_exec_printf() interface using the open database
** DB.  The SQL is the string FORMAT.  The format string should contain
** one %s or %q.  STRING is the value inserted into %s or %q.
*/
static int test_exec_printf(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  Tcl_DString str;
  int rc;
  char *zErr = 0;
  char zBuf[30];
  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " DB FORMAT STRING", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)atoi(argv[1]);
  Tcl_DStringInit(&str);
  rc = sqlite_exec_printf(db, argv[2], exec_printf_cb, &str, &zErr, argv[3]);
  sprintf(zBuf, "%d", rc);
  Tcl_AppendElement(interp, zBuf);
  Tcl_AppendElement(interp, rc==SQLITE_OK ? Tcl_DStringValue(&str) : zErr);
  Tcl_DStringFree(&str);
  if( zErr ) free(zErr);
  return TCL_OK;
}

/*
** Usage:  sqlite_get_table_printf  DB  FORMAT  STRING
**
** Invoke the sqlite_get_table_printf() interface using the open database
** DB.  The SQL is the string FORMAT.  The format string should contain
** one %s or %q.  STRING is the value inserted into %s or %q.
*/
static int test_get_table_printf(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  Tcl_DString str;
  int rc;
  char *zErr = 0;
  int nRow, nCol;
  char **aResult;
  int i;
  char zBuf[30];
  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " DB FORMAT STRING", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)atoi(argv[1]);
  Tcl_DStringInit(&str);
  rc = sqlite_get_table_printf(db, argv[2], &aResult, &nRow, &nCol, 
               &zErr, argv[3]);
  sprintf(zBuf, "%d", rc);
  Tcl_AppendElement(interp, zBuf);
  if( rc==SQLITE_OK ){
    sprintf(zBuf, "%d", nRow);
    Tcl_AppendElement(interp, zBuf);
    sprintf(zBuf, "%d", nCol);
    Tcl_AppendElement(interp, zBuf);
    for(i=0; i<(nRow+1)*nCol; i++){
      Tcl_AppendElement(interp, aResult[i] ? aResult[i] : "NULL");
    }
  }else{
    Tcl_AppendElement(interp, zErr);
  }
  sqlite_free_table(aResult);
  if( zErr ) free(zErr);
  return TCL_OK;
}

/*
** Usage:  sqlite_close DB
**
** Closes the database opened by sqlite_open.
*/
static int sqlite_test_close(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME\"", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)atoi(argv[1]);
  sqlite_close(db);
  return TCL_OK;
}

/*
** Usage:  sqlite_mprintf_int FORMAT INTEGER INTEGER INTEGER
**
** Call mprintf with three integer arguments
*/
static int sqlite_mprintf_int(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int a[3], i;
  char *z;
  if( argc!=5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FORMAT INT INT INT\"", 0);
    return TCL_ERROR;
  }
  for(i=2; i<5; i++){
    if( Tcl_GetInt(interp, argv[i], &a[i-2]) ) return TCL_ERROR;
  }
  z = sqlite_mprintf(argv[1], a[0], a[1], a[2]);
  Tcl_AppendResult(interp, z, 0);
  sqliteFree(z);
  return TCL_OK;
}

/*
** Usage:  sqlite_mprintf_str FORMAT INTEGER INTEGER STRING
**
** Call mprintf with two integer arguments and one string argument
*/
static int sqlite_mprintf_str(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int a[3], i;
  char *z;
  if( argc!=5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FORMAT INT INT STRING\"", 0);
    return TCL_ERROR;
  }
  for(i=2; i<4; i++){
    if( Tcl_GetInt(interp, argv[i], &a[i-2]) ) return TCL_ERROR;
  }
  z = sqlite_mprintf(argv[1], a[0], a[1], argv[4]);
  Tcl_AppendResult(interp, z, 0);
  sqliteFree(z);
  return TCL_OK;
}

/*
** Usage:  sqlite_mprintf_str FORMAT INTEGER INTEGER DOUBLE
**
** Call mprintf with two integer arguments and one double argument
*/
static int sqlite_mprintf_double(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int a[3], i;
  double r;
  char *z;
  if( argc!=5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FORMAT INT INT STRING\"", 0);
    return TCL_ERROR;
  }
  for(i=2; i<4; i++){
    if( Tcl_GetInt(interp, argv[i], &a[i-2]) ) return TCL_ERROR;
  }
  if( Tcl_GetDouble(interp, argv[4], &r) ) return TCL_ERROR;
  z = sqlite_mprintf(argv[1], a[0], a[1], r);
  Tcl_AppendResult(interp, z, 0);
  sqliteFree(z);
  return TCL_OK;
}

/*
** Usage: sqlite_malloc_fail N
**
** Rig sqliteMalloc() to fail on the N-th call.  Turn off this mechanism
** and reset the sqlite_malloc_failed variable is N==0.
*/
#ifdef MEMORY_DEBUG
static int sqlite_malloc_fail(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int n;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], " N\"", 0);
    return TCL_ERROR;
  }
  if( Tcl_GetInt(interp, argv[1], &n) ) return TCL_ERROR;
  sqlite_iMallocFail = n;
  sqlite_malloc_failed = 0;
  return TCL_OK;
}
#endif

/*
** Usage: sqlite_malloc_stat
**
** Return the number of prior calls to sqliteMalloc() and sqliteFree().
*/
#ifdef MEMORY_DEBUG
static int sqlite_malloc_stat(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  char zBuf[200];
  sprintf(zBuf, "%d %d %d", sqlite_nMalloc, sqlite_nFree, sqlite_iMallocFail);
  Tcl_AppendResult(interp, zBuf, 0);
  return TCL_OK;
}
#endif

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest1_Init(Tcl_Interp *interp){
  Tcl_CreateCommand(interp, "sqlite_mprintf_int", sqlite_mprintf_int, 0, 0);
  Tcl_CreateCommand(interp, "sqlite_mprintf_str", sqlite_mprintf_str, 0, 0);
  Tcl_CreateCommand(interp, "sqlite_mprintf_double", sqlite_mprintf_double,0,0);
  Tcl_CreateCommand(interp, "sqlite_open", sqlite_test_open, 0, 0);
  Tcl_CreateCommand(interp, "sqlite_exec_printf", test_exec_printf, 0, 0);
  Tcl_CreateCommand(interp, "sqlite_get_table_printf", test_get_table_printf,
      0, 0);
  Tcl_CreateCommand(interp, "sqlite_close", sqlite_test_close, 0, 0);
#ifdef MEMORY_DEBUG
  Tcl_CreateCommand(interp, "sqlite_malloc_fail", sqlite_malloc_fail, 0, 0);
  Tcl_CreateCommand(interp, "sqlite_malloc_stat", sqlite_malloc_stat, 0, 0);
#endif
  return TCL_OK;
}
