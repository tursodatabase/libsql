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
** $Id: test1.c,v 1.13 2002/08/31 18:53:08 drh Exp $
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
  db = (sqlite*)strtol(argv[1], 0, 0);
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
  db = (sqlite*)strtol(argv[1], 0, 0);
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
** Usage:  sqlite_last_insert_rowid DB
**
** Returns the integer ROWID of the most recent insert.
*/
static int test_last_rowid(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  char zBuf[30];

  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], " DB\"", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)strtol(argv[1], 0, 0);
  sprintf(zBuf, "%d", sqlite_last_insert_rowid(db));
  Tcl_AppendResult(interp, zBuf, 0);
  return SQLITE_OK;
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
  db = (sqlite*)strtol(argv[1], 0, 0);
  sqlite_close(db);
  return TCL_OK;
}

/*
** Implementation of the x_coalesce() function.
** Return the first argument non-NULL argument.
*/
static void ifnullFunc(sqlite_func *context, int argc, const char **argv){
  int i;
  for(i=0; i<argc; i++){
    if( argv[i] ){
      sqlite_set_result_string(context, argv[i], -1);
      break;
    }
  }
}

/*
** Implementation of the x_sqlite_exec() function.  This function takes
** a single argument and attempts to execute that argument as SQL code.
** This is illegal and should set the SQLITE_MISUSE flag on the database.
** 
** This routine simulates the effect of having two threads attempt to
** use the same database at the same time.
*/
static void sqliteExecFunc(sqlite_func *context, int argc, const char **argv){
  sqlite_exec((sqlite*)sqlite_user_data(context), argv[0], 0, 0, 0);
}

/*
** Usage:  sqlite_test_create_function DB
**
** Call the sqlite_create_function API on the given database in order
** to create a function named "x_coalesce".  This function does the same thing
** as the "coalesce" function.  This function also registers an SQL function
** named "x_sqlite_exec" that invokes sqlite_exec().  Invoking sqlite_exec()
** in this way is illegal recursion and should raise an SQLITE_MISUSE error.
** The effect is similar to trying to use the same database connection from
** two threads at the same time.
**
** The original motivation for this routine was to be able to call the
** sqlite_create_function function while a query is in progress in order
** to test the SQLITE_MISUSE detection logic.
*/
static int test_create_function(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  extern void Md5_Register(sqlite*);
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FILENAME\"", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)strtol(argv[1], 0, 0);
  sqlite_create_function(db, "x_coalesce", -1, ifnullFunc, 0);
  sqlite_create_function(db, "x_sqlite_exec", 1, sqliteExecFunc, db);
  return TCL_OK;
}

/*
** Routines to implement the x_count() aggregate function.
*/
typedef struct CountCtx CountCtx;
struct CountCtx {
  int n;
};
static void countStep(sqlite_func *context, int argc, const char **argv){
  CountCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  if( (argc==0 || argv[0]) && p ){
    p->n++;
  }
}   
static void countFinalize(sqlite_func *context){
  CountCtx *p;
  p = sqlite_aggregate_context(context, sizeof(*p));
  sqlite_set_result_int(context, p ? p->n : 0);
}

/*
** Usage:  sqlite_test_create_aggregate DB
**
** Call the sqlite_create_function API on the given database in order
** to create a function named "x_count".  This function does the same thing
** as the "md5sum" function.
**
** The original motivation for this routine was to be able to call the
** sqlite_create_aggregate function while a query is in progress in order
** to test the SQLITE_MISUSE detection logic.
*/
static int test_create_aggregate(
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
  db = (sqlite*)strtol(argv[1], 0, 0);
  sqlite_create_aggregate(db, "x_count", 0, countStep, countFinalize, 0);
  sqlite_create_aggregate(db, "x_count", 1, countStep, countFinalize, 0);
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
  sqlite_freemem(z);
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
  if( argc<4 || argc>5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FORMAT INT INT ?STRING?\"", 0);
    return TCL_ERROR;
  }
  for(i=2; i<4; i++){
    if( Tcl_GetInt(interp, argv[i], &a[i-2]) ) return TCL_ERROR;
  }
  z = sqlite_mprintf(argv[1], a[0], a[1], argc>4 ? argv[4] : NULL);
  Tcl_AppendResult(interp, z, 0);
  sqlite_freemem(z);
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
  sqlite_freemem(z);
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
** Usage:  sqlite_abort
**
** Shutdown the process immediately.  This is not a clean shutdown.
** This command is used to test the recoverability of a database in
** the event of a program crash.
*/
static int sqlite_abort(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  assert( interp==0 );   /* This will always fail */
  return TCL_OK;
}

/*
** The following routine is a user-defined SQL function whose purpose
** is to test the sqlite_set_result() API.
*/
static void testFunc(sqlite_func *context, int argc, const char **argv){
  while( argc>=2 ){
    if( argv[0]==0 ){
      sqlite_set_result_error(context, "first argument to test function "
         "may not be NULL", -1);
    }else if( sqliteStrICmp(argv[0],"string")==0 ){
      sqlite_set_result_string(context, argv[1], -1);
    }else if( argv[1]==0 ){
      sqlite_set_result_error(context, "2nd argument may not be NULL if the "
         "first argument is not \"string\"", -1);
    }else if( sqliteStrICmp(argv[0],"int")==0 ){
      sqlite_set_result_int(context, atoi(argv[1]));
    }else if( sqliteStrICmp(argv[0],"double")==0 ){
      sqlite_set_result_double(context, atof(argv[1]));
    }else{
      sqlite_set_result_error(context,"first argument should be one of: "
          "string int double", -1);
    }
    argc -= 2;
    argv += 2;
  }
}

/*
** Usage:   sqlite_register_test_function  DB  NAME
**
** Register the test SQL function on the database DB under the name NAME.
*/
static int test_register_func(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  int rc;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " DB FUNCTION-NAME", 0);
    return TCL_ERROR;
  }
  db = (sqlite*)strtol(argv[1], 0, 0);
  rc = sqlite_create_function(db, argv[2], -1, testFunc, 0);
  if( rc!=0 ){
    Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest1_Init(Tcl_Interp *interp){
  extern int sqlite_search_count;
  static struct {
     char *zName;
     Tcl_CmdProc *xProc;
  } aCmd[] = {
     { "sqlite_mprintf_int",             (Tcl_CmdProc*)sqlite_mprintf_int    },
     { "sqlite_mprintf_str",             (Tcl_CmdProc*)sqlite_mprintf_str    },
     { "sqlite_mprintf_double",          (Tcl_CmdProc*)sqlite_mprintf_double },
     { "sqlite_open",                    (Tcl_CmdProc*)sqlite_test_open      },
     { "sqlite_last_insert_rowid",       (Tcl_CmdProc*)test_last_rowid       },
     { "sqlite_exec_printf",             (Tcl_CmdProc*)test_exec_printf      },
     { "sqlite_get_table_printf",        (Tcl_CmdProc*)test_get_table_printf },
     { "sqlite_close",                   (Tcl_CmdProc*)sqlite_test_close     },
     { "sqlite_create_function",         (Tcl_CmdProc*)test_create_function  },
     { "sqlite_create_aggregate",        (Tcl_CmdProc*)test_create_aggregate },
     { "sqlite_register_test_function",  (Tcl_CmdProc*)test_register_func    },
     { "sqlite_abort",                   (Tcl_CmdProc*)sqlite_abort          },
#ifdef MEMORY_DEBUG
     { "sqlite_malloc_fail",             (Tcl_CmdProc*)sqlite_malloc_fail    },
     { "sqlite_malloc_stat",             (Tcl_CmdProc*)sqlite_malloc_stat    },
#endif
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }
  Tcl_LinkVar(interp, "sqlite_search_count", 
      (char*)&sqlite_search_count, TCL_LINK_INT);
  return TCL_OK;
}
