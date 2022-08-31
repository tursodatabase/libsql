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

#include <tcl.h>
#include <assert.h>

typedef struct TestRecover TestRecover;
struct TestRecover {
  sqlite3_recover *p;
};

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
**     $cmd step
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
    { "config",    2, "REBASE-BLOB" }, /* 0 */
    { "step",      0, ""            }, /* 1 */
    { "errmsg",    0, ""            }, /* 2 */
    { "errcode",   0, ""            }, /* 3 */
    { "finish",  0, ""              }, /* 4 */
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
        "testdb",    /* 0 */
        0
      };
      int iOp = 0;
      int res = 0;
      if( Tcl_GetIndexFromObj(interp, objv[2], aOp, "option", 0, &iOp) ){
        return TCL_ERROR;
      }
      switch( iOp ){
        case 0:
          res = sqlite3_recover_config(
              pTest->p, SQLITE_RECOVER_TESTDB, (void*)Tcl_GetString(objv[3])
          );
          break;
      }
      Tcl_SetObjResult(interp, Tcl_NewIntObj(res));
      break;
    }
    case 1:  assert( sqlite3_stricmp("step", aSub[iSub].zSub)==0 ); {
      int res = sqlite3_recover_step(pTest->p);
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
        char *zRes = sqlite3_mprintf("(%d) - %s", res, zErr);
        Tcl_SetObjResult(interp, Tcl_NewStringObj(zRes, -1));
        sqlite3_free(zRes);
      }
      res2 = sqlite3_recover_finish(pTest->p);
      assert( res2==res );
      if( res ) return TCL_ERROR;
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

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME URI");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, objv[1], &db) ) return TCL_ERROR;
  zDb = Tcl_GetString(objv[2]);
  zUri = Tcl_GetString(objv[3]);

  pNew = ckalloc(sizeof(TestRecover));
  pNew->p = sqlite3_recover_init(db, zDb, zUri);

  sprintf(zCmd, "sqlite_recover%d", iTestRecoverCmd++);
  Tcl_CreateObjCommand(interp, zCmd, testRecoverCmd, (void*)pNew, 0);

  Tcl_SetObjResult(interp, Tcl_NewStringObj(zCmd, -1));
  return TCL_OK;
}

int TestRecover_Init(Tcl_Interp *interp){
  struct Cmd {
    const char *zCmd;
    Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite3_recover_init", test_sqlite3_recover_init },
  };
  int i;

  for(i=0; i<sizeof(aCmd)/sizeof(struct Cmd); i++){
    struct Cmd *p = &aCmd[i];
    Tcl_CreateObjCommand(interp, p->zCmd, p->xProc, 0, 0);
  }

  return TCL_OK;
}

