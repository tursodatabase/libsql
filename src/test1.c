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
** $Id: test1.c,v 1.36.2.1 2004/05/07 00:57:06 drh Exp $
*/
#include "sqliteInt.h"
#include "tcl.h"
#include "os.h"
#include <stdlib.h>
#include <string.h>

#if OS_WIN
# define PTR_FMT "%x"
#else
# define PTR_FMT "%p"
#endif

/*
** Decode a pointer to an sqlite object.
*/
static int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite **ppDb){
  if( sscanf(zA, PTR_FMT, (void**)ppDb)!=1 && 
      (zA[0]!='0' || zA[1]!='x' || sscanf(&zA[2], PTR_FMT, (void**)ppDb)!=1)
  ){
    Tcl_AppendResult(interp, "\"", zA, "\" is not a valid pointer value", 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Decode a pointer to an sqlite_vm object.
*/
static int getVmPointer(Tcl_Interp *interp, const char *zArg, sqlite_vm **ppVm){
  if( sscanf(zArg, PTR_FMT, (void**)ppVm)!=1 ){
    Tcl_AppendResult(interp, "\"", zArg, "\" is not a valid pointer value", 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Generate a text representation of a pointer that can be understood
** by the getDbPointer and getVmPointer routines above.
**
** The problem is, on some machines (Solaris) if you do a printf with
** "%p" you cannot turn around and do a scanf with the same "%p" and
** get your pointer back.  You have to prepend a "0x" before it will
** work.  Or at least that is what is reported to me (drh).  But this
** behavior varies from machine to machine.  The solution used her is
** to test the string right after it is generated to see if it can be
** understood by scanf, and if not, try prepending an "0x" to see if
** that helps.  If nothing works, a fatal error is generated.
*/
static int makePointerStr(Tcl_Interp *interp, char *zPtr, void *p){
  void *p2;
  sprintf(zPtr, PTR_FMT, p);
  if( sscanf(zPtr, PTR_FMT, &p2)!=1 || p2!=p ){
    sprintf(zPtr, "0x" PTR_FMT, p);
    if( sscanf(zPtr, PTR_FMT, &p2)!=1 || p2!=p ){
      Tcl_AppendResult(interp, "unable to convert a pointer to a string "
         "in the file " __FILE__ " in function makePointerStr().  Please "
         "report this problem to the SQLite mailing list or as a new but "
         "report.  Please provide detailed information about how you compiled "
         "SQLite and what computer you are running on.", 0);
      return TCL_ERROR;
    }
  }
  return TCL_OK;
}

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
  if( makePointerStr(interp, zBuf, db) ) return TCL_ERROR;
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
** Usage:  sqlite_mprintf_z_test  SEPARATOR  ARG0  ARG1 ...
**
** Test the %z format of mprintf().  Use multiple mprintf() calls to 
** concatenate arg0 through argn using separator as the separator.
** Return the result.
*/
static int test_mprintf_z(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  char *zResult = 0;
  int i;

  for(i=2; i<argc; i++){
    zResult = sqliteMPrintf("%z%s%s", zResult, argv[1], argv[i]);
  }
  Tcl_AppendResult(interp, zResult, 0);
  sqliteFree(zResult);
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
** A structure into which to accumulate text.
*/
struct dstr {
  int nAlloc;  /* Space allocated */
  int nUsed;   /* Space used */
  char *z;     /* The space */
};

/*
** Append text to a dstr
*/
static void dstrAppend(struct dstr *p, const char *z, int divider){
  int n = strlen(z);
  if( p->nUsed + n + 2 > p->nAlloc ){
    char *zNew;
    p->nAlloc = p->nAlloc*2 + n + 200;
    zNew = sqliteRealloc(p->z, p->nAlloc);
    if( zNew==0 ){
      sqliteFree(p->z);
      memset(p, 0, sizeof(*p));
      return;
    }
    p->z = zNew;
  }
  if( divider && p->nUsed>0 ){
    p->z[p->nUsed++] = divider;
  }
  memcpy(&p->z[p->nUsed], z, n+1);
  p->nUsed += n;
}

/*
** Invoked for each callback from sqliteExecFunc
*/
static int execFuncCallback(void *pData, int argc, char **argv, char **NotUsed){
  struct dstr *p = (struct dstr*)pData;
  int i;
  for(i=0; i<argc; i++){
    if( argv[i]==0 ){
      dstrAppend(p, "NULL", ' ');
    }else{
      dstrAppend(p, argv[i], ' ');
    }
  }
  return 0;
}

/*
** Implementation of the x_sqlite_exec() function.  This function takes
** a single argument and attempts to execute that argument as SQL code.
** This is illegal and should set the SQLITE_MISUSE flag on the database.
**
** 2004-Jan-07:  We have changed this to make it legal to call sqlite_exec()
** from within a function call.  
** 
** This routine simulates the effect of having two threads attempt to
** use the same database at the same time.
*/
static void sqliteExecFunc(sqlite_func *context, int argc, const char **argv){
  struct dstr x;
  memset(&x, 0, sizeof(x));
  sqlite_exec((sqlite*)sqlite_user_data(context), argv[0], 
      execFuncCallback, &x, 0);
  sqlite_set_result_string(context, x.z, x.nUsed);
  sqliteFree(x.z);
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
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
** Usage:  sqlite_mprintf_str FORMAT DOUBLE DOUBLE
**
** Call mprintf with a single double argument which is the product of the
** two arguments given above.  This is used to generate overflow and underflow
** doubles to test that they are converted properly.
*/
static int sqlite_mprintf_scaled(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  int i;
  double r[2];
  char *z;
  if( argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0],
       " FORMAT DOUBLE DOUBLE\"", 0);
    return TCL_ERROR;
  }
  for(i=2; i<4; i++){
    if( Tcl_GetDouble(interp, argv[i], &r[i-2]) ) return TCL_ERROR;
  }
  z = sqlite_mprintf(argv[1], r[0]*r[1]);
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
      sqlite_set_result_double(context, sqliteAtoF(argv[1], 0));
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
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
  rc = sqlite_create_function(db, argv[2], -1, testFunc, 0);
  if( rc!=0 ){
    Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** This SQLite callback records the datatype of all columns.
**
** The pArg argument is really a pointer to a TCL interpreter.  The
** column names are inserted as the result of this interpreter.
**
** This routine returns non-zero which causes the query to abort.
*/
static int rememberDataTypes(void *pArg, int nCol, char **argv, char **colv){
  int i;
  Tcl_Interp *interp = (Tcl_Interp*)pArg;
  Tcl_Obj *pList, *pElem;
  if( colv[nCol+1]==0 ){
    return 1;
  }
  pList = Tcl_NewObj();
  for(i=0; i<nCol; i++){
    pElem = Tcl_NewStringObj(colv[i+nCol] ? colv[i+nCol] : "NULL", -1);
    Tcl_ListObjAppendElement(interp, pList, pElem);
  }
  Tcl_SetObjResult(interp, pList);
  return 1;
}

/*
** Invoke an SQL statement but ignore all the data in the result.  Instead,
** return a list that consists of the datatypes of the various columns.
**
** This only works if "PRAGMA show_datatypes=on" has been executed against
** the database connection.
*/
static int sqlite_datatypes(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  int rc;
  if( argc!=3 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " DB SQL", 0);
    return TCL_ERROR;
  }
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
  rc = sqlite_exec(db, argv[2], rememberDataTypes, interp, 0);
  if( rc!=0 && rc!=SQLITE_ABORT ){
    Tcl_AppendResult(interp, sqlite_error_string(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:  sqlite_compile  DB  SQL  ?TAILVAR?
**
** Attempt to compile an SQL statement.  Return a pointer to the virtual
** machine used to execute that statement.  Unprocessed SQL is written
** into TAILVAR.
*/
static int test_compile(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite *db;
  sqlite_vm *vm;
  int rc;
  char *zErr = 0;
  const char *zTail;
  char zBuf[50];
  if( argc!=3 && argc!=4 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " DB SQL TAILVAR", 0);
    return TCL_ERROR;
  }
  if( getDbPointer(interp, argv[1], &db) ) return TCL_ERROR;
  rc = sqlite_compile(db, argv[2], argc==4 ? &zTail : 0, &vm, &zErr);
  if( argc==4 ) Tcl_SetVar(interp, argv[3], zTail, 0);
  if( rc ){
    assert( vm==0 );
    sprintf(zBuf, "(%d) ", rc);
    Tcl_AppendResult(interp, zBuf, zErr, 0);
    sqlite_freemem(zErr);
    return TCL_ERROR;
  }
  if( vm ){
    if( makePointerStr(interp, zBuf, vm) ) return TCL_ERROR;
    Tcl_AppendResult(interp, zBuf, 0);
  }
  return TCL_OK;
}

/*
** Usage:  sqlite_step  VM  ?NVAR?  ?VALUEVAR?  ?COLNAMEVAR?
**
** Step a virtual machine.  Return a the result code as a string.
** Column results are written into three variables.
*/
static int test_step(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite_vm *vm;
  int rc, i;
  const char **azValue = 0;
  const char **azColName = 0;
  int N = 0;
  char *zRc;
  char zBuf[50];
  if( argc<2 || argc>5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " VM NVAR VALUEVAR COLNAMEVAR", 0);
    return TCL_ERROR;
  }
  if( getVmPointer(interp, argv[1], &vm) ) return TCL_ERROR;
  rc = sqlite_step(vm, argc>=3?&N:0, argc>=4?&azValue:0, argc==5?&azColName:0);
  if( argc>=3 ){
    sprintf(zBuf, "%d", N);
    Tcl_SetVar(interp, argv[2], zBuf, 0);
  }
  if( argc>=4 ){
    Tcl_SetVar(interp, argv[3], "", 0);
    if( azValue ){
      for(i=0; i<N; i++){
        Tcl_SetVar(interp, argv[3], azValue[i] ? azValue[i] : "",
            TCL_APPEND_VALUE | TCL_LIST_ELEMENT);
      }
    }
  }
  if( argc==5 ){
    Tcl_SetVar(interp, argv[4], "", 0);
    if( azColName ){
      for(i=0; i<N*2; i++){
        Tcl_SetVar(interp, argv[4], azColName[i] ? azColName[i] : "",
            TCL_APPEND_VALUE | TCL_LIST_ELEMENT);
      }
    }
  }
  switch( rc ){
    case SQLITE_DONE:   zRc = "SQLITE_DONE";    break;
    case SQLITE_BUSY:   zRc = "SQLITE_BUSY";    break;
    case SQLITE_ROW:    zRc = "SQLITE_ROW";     break;
    case SQLITE_ERROR:  zRc = "SQLITE_ERROR";   break;
    case SQLITE_MISUSE: zRc = "SQLITE_MISUSE";  break;
    default:            zRc = "unknown";        break;
  }
  Tcl_AppendResult(interp, zRc, 0);
  return TCL_OK;
}

/*
** Usage:  sqlite_finalize  VM 
**
** Shutdown a virtual machine.
*/
static int test_finalize(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite_vm *vm;
  int rc;
  char *zErrMsg = 0;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " VM\"", 0);
    return TCL_ERROR;
  }
  if( getVmPointer(interp, argv[1], &vm) ) return TCL_ERROR;
  rc = sqlite_finalize(vm, &zErrMsg);
  if( rc ){
    char zBuf[50];
    sprintf(zBuf, "(%d) ", rc);
    Tcl_AppendResult(interp, zBuf, zErrMsg, 0);
    sqlite_freemem(zErrMsg);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:  sqlite_reset   VM 
**
** Reset a virtual machine and prepare it to be run again.
*/
static int test_reset(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite_vm *vm;
  int rc;
  char *zErrMsg = 0;
  if( argc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " VM\"", 0);
    return TCL_ERROR;
  }
  if( getVmPointer(interp, argv[1], &vm) ) return TCL_ERROR;
  rc = sqlite_reset(vm, &zErrMsg);
  if( rc ){
    char zBuf[50];
    sprintf(zBuf, "(%d) ", rc);
    Tcl_AppendResult(interp, zBuf, zErrMsg, 0);
    sqlite_freemem(zErrMsg);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** This is the "static_bind_value" that variables are bound to when
** the FLAG option of sqlite_bind is "static"
*/
static char *sqlite_static_bind_value = 0;

/*
** Usage:  sqlite_bind  VM  IDX  VALUE  FLAGS
**
** Sets the value of the IDX-th occurance of "?" in the original SQL
** string.  VALUE is the new value.  If FLAGS=="null" then VALUE is
** ignored and the value is set to NULL.  If FLAGS=="static" then
** the value is set to the value of a static variable named
** "sqlite_static_bind_value".  If FLAGS=="normal" then a copy
** of the VALUE is made.
*/
static int test_bind(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  sqlite_vm *vm;
  int rc;
  int idx;
  if( argc!=5 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"", argv[0], 
       " VM IDX VALUE (null|static|normal)\"", 0);
    return TCL_ERROR;
  }
  if( getVmPointer(interp, argv[1], &vm) ) return TCL_ERROR;
  if( Tcl_GetInt(interp, argv[2], &idx) ) return TCL_ERROR;
  if( strcmp(argv[4],"null")==0 ){
    rc = sqlite_bind(vm, idx, 0, 0, 0);
  }else if( strcmp(argv[4],"static")==0 ){
    rc = sqlite_bind(vm, idx, sqlite_static_bind_value, -1, 0);
  }else if( strcmp(argv[4],"normal")==0 ){
    rc = sqlite_bind(vm, idx, argv[3], -1, 1);
  }else{
    Tcl_AppendResult(interp, "4th argument should be "
        "\"null\" or \"static\" or \"normal\"", 0);
    return TCL_ERROR;
  }
  if( rc ){
    char zBuf[50];
    sprintf(zBuf, "(%d) ", rc);
    Tcl_AppendResult(interp, zBuf, sqlite_error_string(rc), 0);
    return TCL_ERROR;
  }
  return TCL_OK;
}

/*
** Usage:    breakpoint
**
** This routine exists for one purpose - to provide a place to put a
** breakpoint with GDB that can be triggered using TCL code.  The use
** for this is when a particular test fails on (say) the 1485th iteration.
** In the TCL test script, we can add code like this:
**
**     if {$i==1485} breakpoint
**
** Then run testfixture in the debugger and wait for the breakpoint to
** fire.  Then additional breakpoints can be set to trace down the bug.
*/
static int test_breakpoint(
  void *NotUsed,
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int argc,              /* Number of arguments */
  char **argv            /* Text of each argument */
){
  return TCL_OK;         /* Do nothing */
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest1_Init(Tcl_Interp *interp){
  extern int sqlite_search_count;
  extern int sqlite_interrupt_count;
  extern int sqlite_open_file_count;
  extern int sqlite_current_time;
  extern int sqlite_temp_directory;
  static struct {
     char *zName;
     Tcl_CmdProc *xProc;
  } aCmd[] = {
     { "sqlite_mprintf_int",             (Tcl_CmdProc*)sqlite_mprintf_int    },
     { "sqlite_mprintf_str",             (Tcl_CmdProc*)sqlite_mprintf_str    },
     { "sqlite_mprintf_double",          (Tcl_CmdProc*)sqlite_mprintf_double },
     { "sqlite_mprintf_scaled",          (Tcl_CmdProc*)sqlite_mprintf_scaled },
     { "sqlite_mprintf_z_test",          (Tcl_CmdProc*)test_mprintf_z        },
     { "sqlite_open",                    (Tcl_CmdProc*)sqlite_test_open      },
     { "sqlite_last_insert_rowid",       (Tcl_CmdProc*)test_last_rowid       },
     { "sqlite_exec_printf",             (Tcl_CmdProc*)test_exec_printf      },
     { "sqlite_get_table_printf",        (Tcl_CmdProc*)test_get_table_printf },
     { "sqlite_close",                   (Tcl_CmdProc*)sqlite_test_close     },
     { "sqlite_create_function",         (Tcl_CmdProc*)test_create_function  },
     { "sqlite_create_aggregate",        (Tcl_CmdProc*)test_create_aggregate },
     { "sqlite_register_test_function",  (Tcl_CmdProc*)test_register_func    },
     { "sqlite_abort",                   (Tcl_CmdProc*)sqlite_abort          },
     { "sqlite_datatypes",               (Tcl_CmdProc*)sqlite_datatypes      },
#ifdef MEMORY_DEBUG
     { "sqlite_malloc_fail",             (Tcl_CmdProc*)sqlite_malloc_fail    },
     { "sqlite_malloc_stat",             (Tcl_CmdProc*)sqlite_malloc_stat    },
#endif
     { "sqlite_compile",                 (Tcl_CmdProc*)test_compile          },
     { "sqlite_step",                    (Tcl_CmdProc*)test_step             },
     { "sqlite_finalize",                (Tcl_CmdProc*)test_finalize         },
     { "sqlite_bind",                    (Tcl_CmdProc*)test_bind             },
     { "sqlite_reset",                   (Tcl_CmdProc*)test_reset            },
     { "breakpoint",                     (Tcl_CmdProc*)test_breakpoint       },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }
  Tcl_LinkVar(interp, "sqlite_search_count", 
      (char*)&sqlite_search_count, TCL_LINK_INT);
  Tcl_LinkVar(interp, "sqlite_interrupt_count", 
      (char*)&sqlite_interrupt_count, TCL_LINK_INT);
  Tcl_LinkVar(interp, "sqlite_open_file_count", 
      (char*)&sqlite_open_file_count, TCL_LINK_INT);
  Tcl_LinkVar(interp, "sqlite_current_time", 
      (char*)&sqlite_current_time, TCL_LINK_INT);
  Tcl_LinkVar(interp, "sqlite_static_bind_value",
      (char*)&sqlite_static_bind_value, TCL_LINK_STRING);
  Tcl_LinkVar(interp, "sqlite_temp_directory",
      (char*)&sqlite_temp_directory, TCL_LINK_STRING);
  return TCL_OK;
}
