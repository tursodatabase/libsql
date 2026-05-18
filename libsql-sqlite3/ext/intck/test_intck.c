/*
** 2010 August 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing all sorts of SQLite interfaces. This code
** is not included in the SQLite library. 
*/

#include "sqlite3.h"
#include "sqlite3intck.h"
#include "tclsqlite.h"
#include <string.h>
#include <assert.h>

/* In test1.c */
int getDbPointer(Tcl_Interp *interp, const char *zA, sqlite3 **ppDb);
const char *sqlite3ErrName(int);

typedef struct TestIntck TestIntck;
struct TestIntck {
  sqlite3_intck *intck;
};

static int testIntckCmd(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  struct Subcmd {
    const char *zName;
    int nArg;
    const char *zExpect;
  } aCmd[] = {
    {"close", 0, ""},        /* 0 */
    {"step", 0, ""},         /* 1 */
    {"message", 0, ""},      /* 2 */
    {"error", 0, ""},        /* 3 */
    {"unlock", 0, ""},      /* 4 */
    {"test_sql", 1, ""},     /* 5 */
    {0 , 0}
  };
  int rc = TCL_OK;
  int iIdx = -1;
  TestIntck *p = (TestIntck*)clientData;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUB-COMMAND ...");
    return TCL_ERROR;
  }

  rc = Tcl_GetIndexFromObjStruct(
      interp, objv[1], aCmd, sizeof(aCmd[0]), "SUB-COMMAND", 0, &iIdx
  );
  if( rc ) return rc;

  if( objc!=2+aCmd[iIdx].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aCmd[iIdx].zExpect);
    return TCL_ERROR;
  }

  switch( iIdx ){
    case 0: assert( 0==strcmp("close", aCmd[iIdx].zName) ); {
      Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
      break;
    }

    case 1: assert( 0==strcmp("step", aCmd[iIdx].zName) ); {
      rc = sqlite3_intck_step(p->intck);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
      break;
    }

    case 2: assert( 0==strcmp("message", aCmd[iIdx].zName) ); {
      const char *z = sqlite3_intck_message(p->intck);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(z ? z : "", -1));
      break;
    }

    case 3: assert( 0==strcmp("error", aCmd[iIdx].zName) ); {
      const char *zErr = 0;
      Tcl_Obj *pRes;
      rc = sqlite3_intck_error(p->intck, 0);
      pRes = Tcl_NewObj();
      Tcl_ListObjAppendElement(
          interp, pRes, Tcl_NewStringObj(sqlite3ErrName(rc), -1)
      );
      sqlite3_intck_error(p->intck, &zErr);
      Tcl_ListObjAppendElement(
          interp, pRes, Tcl_NewStringObj(zErr ? zErr : 0, -1)
      );
      Tcl_SetObjResult(interp, pRes);
      break;
    }

    case 4: assert( 0==strcmp("unlock", aCmd[iIdx].zName) ); {
      rc = sqlite3_intck_unlock(p->intck);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
      break;
    }

    case 5: assert( 0==strcmp("test_sql", aCmd[iIdx].zName) ); {
      const char *zObj = Tcl_GetString(objv[2]);
      const char *zSql = sqlite3_intck_test_sql(p->intck, zObj[0] ? zObj : 0);
      Tcl_SetObjResult(interp, Tcl_NewStringObj(zSql, -1));
      break;
    }
  }

  return TCL_OK;
}

/*
** Destructor for commands created by test_sqlite3_intck().
*/
static void testIntckFree(void *clientData){
  TestIntck *p = (TestIntck*)clientData;
  sqlite3_intck_close(p->intck);
  ckfree(p);
}

/*
** tclcmd: sqlite3_intck DB DBNAME
*/
static int test_sqlite3_intck(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  char zName[64];
  int iName = 0;
  Tcl_CmdInfo info;
  TestIntck *p = 0;
  sqlite3 *db = 0;
  const char *zDb = 0;
  int rc = SQLITE_OK;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME");
    return TCL_ERROR;
  }

  p = (TestIntck*)ckalloc(sizeof(TestIntck));
  memset(p, 0, sizeof(TestIntck));

  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[2]);
  if( zDb[0]=='\0' ) zDb = 0;

  rc = sqlite3_intck_open(db, zDb, &p->intck);
  if( rc!=SQLITE_OK ){
    ckfree(p);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3_errstr(rc), -1));
    return TCL_ERROR;
  }

  do {
    sprintf(zName, "intck%d", iName++);
  }while( Tcl_GetCommandInfo(interp, zName, &info)!=0 );
  Tcl_CreateObjCommand(interp, zName, testIntckCmd, (void*)p, testIntckFree);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(zName, -1));

  return TCL_OK;
}

/*
** tclcmd: test_do_intck DB DBNAME
*/
static int test_do_intck(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db = 0;
  const char *zDb = 0;
  int rc = SQLITE_OK;
  sqlite3_intck *pCk = 0;
  Tcl_Obj *pRet = 0;
  const char *zErr = 0;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ){
    return TCL_ERROR;
  }
  zDb = Tcl_GetString(objv[2]);

  pRet = Tcl_NewObj();
  Tcl_IncrRefCount(pRet);

  rc = sqlite3_intck_open(db, zDb, &pCk);
  if( rc==SQLITE_OK ){
    while( sqlite3_intck_step(pCk)==SQLITE_OK ){
      const char *zMsg = sqlite3_intck_message(pCk);
      if( zMsg ){
        Tcl_ListObjAppendElement(interp, pRet, Tcl_NewStringObj(zMsg, -1));
      }
    }
    rc = sqlite3_intck_error(pCk, &zErr);
  }
  if( rc!=SQLITE_OK ){
    if( zErr ){
      Tcl_SetObjResult(interp, Tcl_NewStringObj(zErr, -1));
    }else{
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3ErrName(rc), -1));
    }
  }else{
    Tcl_SetObjResult(interp, pRet);
  }
  Tcl_DecrRefCount(pRet);
  sqlite3_intck_close(pCk);
  sqlite3_intck_close(0);
  return rc ? TCL_ERROR : TCL_OK;
}

int Sqlitetestintck_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite3_intck", test_sqlite3_intck, 0, 0);
  Tcl_CreateObjCommand(interp, "test_do_intck", test_do_intck, 0, 0);
  return TCL_OK;
}
