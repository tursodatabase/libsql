/*
** 2022-08-27
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
*/

#include "sqlite3recover.h"
#include "sqliteInt.h"
#include "tclsqlite.h"
#include <assert.h>

#ifndef SQLITE_OMIT_VIRTUALTABLE

typedef struct TestRecover TestRecover;
struct TestRecover {
  sqlite3_recover *p;
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};

static int xSqlCallback(void *pSqlArg, const char *zSql){
  TestRecover *p = (TestRecover*)pSqlArg;
  Tcl_Obj *pEval = 0;
  int res = 0;

  pEval = Tcl_DuplicateObj(p->pScript);
  Tcl_IncrRefCount(pEval);

  res = Tcl_ListObjAppendElement(p->interp, pEval, Tcl_NewStringObj(zSql, -1));
  if( res==TCL_OK ){
    res = Tcl_EvalObjEx(p->interp, pEval, 0);
  }

  Tcl_DecrRefCount(pEval);
  if( res ){
    Tcl_BackgroundError(p->interp);
    return TCL_ERROR;
  }else{
    Tcl_Obj *pObj = Tcl_GetObjResult(p->interp);
    if( Tcl_GetCharLength(pObj)==0 ){
      res = 0;
    }else if( Tcl_GetIntFromObj(p->interp, pObj, &res) ){
      Tcl_BackgroundError(p->interp);
      return TCL_ERROR;
    }
  }
  return res;
}

static int getDbPointer(Tcl_Interp *interp, Tcl_Obj *pObj, sqlite3 **pDb){
  Tcl_CmdInfo info;
  if( 0==Tcl_GetCommandInfo(interp, Tcl_GetString(pObj), &info) ){
    Tcl_AppendResult(interp, "no such handle: ", Tcl_GetString(pObj), 0);
    return TCL_ERROR;
  }
  *pDb = *(sqlite3 **)info.objClientData;
  return TCL_OK;
}

/*
** Implementation of the command created by [sqlite3_recover_init]:
**
**     $cmd config OP ARG
**     $cmd run
**     $cmd errmsg
**     $cmd errcode
**     $cmd finalize
*/
static int testRecoverCmd(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static struct RecoverSub {
    const char *zSub;
    int nArg;
    const char *zMsg;
  } aSub[] = {
    { "config",    2, "ARG"         }, /* 0 */
    { "run",      0, ""             }, /* 1 */
    { "errmsg",    0, ""            }, /* 2 */
    { "errcode",   0, ""            }, /* 3 */
    { "finish",  0, ""              }, /* 4 */
    { "step",  0, ""                }, /* 5 */
    { 0 }
  };
  int rc = TCL_OK;
  int iSub = 0;
  TestRecover *pTest = (TestRecover*)clientData;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(interp, 
      objv[1], aSub, sizeof(aSub[0]), "sub-command", 0, &iSub
  );
  if( rc!=TCL_OK ) return rc;
  if( (objc-2)!=aSub[iSub].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aSub[iSub].zMsg);
    return TCL_ERROR;
  }

  switch( iSub ){
    case 0:  assert( sqlite3_stricmp("config", aSub[iSub].zSub)==0 ); {
      const char *aOp[] = {
        "testdb",          /* 0 */
        "lostandfound",    /* 1 */
        "freelistcorrupt", /* 2 */
        "rowids",          /* 3 */
        "slowindexes",     /* 4 */
        "invalid",         /* 5 */
        0
      };
      int iOp = 0;
      int res = 0;
      if( Tcl_GetIndexFromObj(interp, objv[2], aOp, "option", 0, &iOp) ){
        return TCL_ERROR;
      }
      switch( iOp ){
        case 0:
          res = sqlite3_recover_config(pTest->p, 
              789, (void*)Tcl_GetString(objv[3]) /* MAGIC NUMBER! */
          );
          break;
        case 1: {
          const char *zStr = Tcl_GetString(objv[3]);
          res = sqlite3_recover_config(pTest->p, 
              SQLITE_RECOVER_LOST_AND_FOUND, (void*)(zStr[0] ? zStr : 0)
          );
          break;
        }
        case 2: {
          int iVal = 0;
          if( Tcl_GetBooleanFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          res = sqlite3_recover_config(pTest->p, 
              SQLITE_RECOVER_FREELIST_CORRUPT, (void*)&iVal
          );
          break;
        }
        case 3: {
          int iVal = 0;
          if( Tcl_GetBooleanFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          res = sqlite3_recover_config(pTest->p, 
              SQLITE_RECOVER_ROWIDS, (void*)&iVal
          );
          break;
        }
        case 4: {
          int iVal = 0;
          if( Tcl_GetBooleanFromObj(interp, objv[3], &iVal) ) return TCL_ERROR;
          res = sqlite3_recover_config(pTest->p, 
              SQLITE_RECOVER_SLOWINDEXES, (void*)&iVal
          );
          break;
        }
        case 5: {
          res = sqlite3_recover_config(pTest->p, 12345, 0);
          break;
        }
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(res));
      break;
    }
    case 1:  assert( sqlite3_stricmp("run", aSub[iSub].zSub)==0 ); {
      int res = sqlite3_recover_run(pTest->p);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(res));
      break;
    }
    case 2:  assert( sqlite3_stricmp("errmsg", aSub[iSub].zSub)==0 ); {
      const char *zErr = sqlite3_recover_errmsg(pTest->p);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
      break;
    }
    case 3:  assert( sqlite3_stricmp("errcode", aSub[iSub].zSub)==0 ); {
      int errCode = sqlite3_recover_errcode(pTest->p);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(errCode));
      break;
    }
    case 4:  assert( sqlite3_stricmp("finish", aSub[iSub].zSub)==0 ); {
      int res = sqlite3_recover_errcode(pTest->p);
      int res2;
      if( res!=SQLITE_OK ){
        const char *zErr = sqlite3_recover_errmsg(pTest->p);
        Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
      }
      res2 = sqlite3_recover_finish(pTest->p);
      assert( res2==res );
      if( res ) return TCL_ERROR;
      break;
    }
    case 5:  assert( sqlite3_stricmp("step", aSub[iSub].zSub)==0 ); {
      int res = sqlite3_recover_step(pTest->p);
      Tcl_SetObjResult(interp, Tcl_NewIntObj(res));
      break;
    }
  }

  return TCL_OK;
}

/*
** sqlite3_recover_init DB DBNAME URI
*/
static int test_sqlite3_recover_init(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  static int iTestRecoverCmd = 1;

  TestRecover *pNew = 0;
  sqlite3 *db = 0;
  const char *zDb = 0;
  const char *zUri = 0;
  char zCmd[128];
  int bSql = clientData ? 1 : 0;

  if( objc!=4 ){
    const char *zErr = (bSql ? "DB DBNAME SCRIPT" : "DB DBNAME URI");
    Tcl_WrongNumArgs(interp, 1, objv, zErr);
    return TCL_ERROR;
  }
  if( getDbPointer(interp, objv[1], &db) ) return TCL_ERROR;
  zDb = Tcl_GetString(objv[2]);
  if( zDb[0]=='\0' ) zDb = 0;

  pNew = (TestRecover*)ckalloc(sizeof(TestRecover));
  if( bSql==0 ){
    zUri = Tcl_GetString(objv[3]);
    pNew->p = sqlite3_recover_init(db, zDb, zUri);
  }else{
    pNew->interp = interp;
    pNew->pScript = objv[3];
    Tcl_IncrRefCount(pNew->pScript);
    pNew->p = sqlite3_recover_init_sql(db, zDb, xSqlCallback, (void*)pNew);
  }

  sprintf(zCmd, "sqlite_recover%d", iTestRecoverCmd++);
  Tcl_CreateObjCommand(interp, zCmd, testRecoverCmd, (void*)pNew, 0);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(zCmd, -1));
  return TCL_OK;
}

/*
** Declaration for public API function in file dbdata.c. This may be called
** with NULL as the final two arguments to register the sqlite_dbptr and
** sqlite_dbdata virtual tables with a database handle.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_dbdata_init(sqlite3*, char**, const sqlite3_api_routines*);

/*
** sqlite3_recover_init DB DBNAME URI
*/
static int test_sqlite3_dbdata_init(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db = 0;

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, objv[1], &db) ) return TCL_ERROR;
  sqlite3_dbdata_init(db, 0, 0);

  Tcl_ResetResult(interp);
  return TCL_OK;
}

#endif /* SQLITE_OMIT_VIRTUALTABLE */

int TestRecover_Init(Tcl_Interp *interp){
#ifndef SQLITE_OMIT_VIRTUALTABLE
  struct Cmd {
    const char *zCmd;
    Tcl_ObjCmdProc *xProc;
    void *pArg;
  } aCmd[] = {
    { "sqlite3_recover_init", test_sqlite3_recover_init, 0 },
    { "sqlite3_recover_init_sql", test_sqlite3_recover_init, (void*)1 },
    { "sqlite3_dbdata_init", test_sqlite3_dbdata_init, (void*)1 },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(struct Cmd); i++){
    struct Cmd *p = &aCmd[i];
    Tcl_CreateObjCommand(interp, p->zCmd, p->xProc, p->pArg, 0);
  }
#endif
  return TCL_OK;
}
