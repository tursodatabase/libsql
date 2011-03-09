
#if defined(SQLITE_TEST) && defined(SQLITE_ENABLE_SESSION)

#include "sqlite3session.h"
#include <assert.h>
#include <string.h>
#include <tcl.h>

static int test_session_error(Tcl_Interp *interp, int rc){
  extern const char *sqlite3TestErrorName(int);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3TestErrorName(rc), -1));
  return TCL_ERROR;
}

/*
** Tclcmd:  $session attach TABLE
**          $session changeset
**          $session delete
**          $session enable BOOL
*/
static int test_session_cmd(
  void *clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3_session *pSession = (sqlite3_session *)clientData;
  struct SessionSubcmd {
    const char *zSub;
    int nArg;
    const char *zMsg;
    int iSub;
  } aSub[] = {
    { "attach",    1, "TABLE", }, /* 0 */
    { "changeset", 0, "",      }, /* 1 */
    { "delete",    0, "",      }, /* 2 */
    { "enable",    1, "",      }, /* 3 */
    { 0 }
  };
  int iSub;
  int rc;

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  rc = Tcl_GetIndexFromObjStruct(interp, 
      objv[1], aSub, sizeof(aSub[0]), "sub-command", 0, &iSub
  );
  if( rc!=TCL_OK ) return rc;
  if( objc!=2+aSub[iSub].nArg ){
    Tcl_WrongNumArgs(interp, 2, objv, aSub[iSub].zMsg);
    return TCL_ERROR;
  }

  switch( iSub ){
    case 0:        /* attach */
      rc = sqlite3session_attach(pSession, Tcl_GetString(objv[2]));
      if( rc!=SQLITE_OK ){
        return test_session_error(interp, rc);
      }
      break;

    case 1: {      /* changeset */
      int nChange;
      void *pChange;
      rc = sqlite3session_changeset(pSession, &nChange, &pChange);
      if( rc==SQLITE_OK ){
        Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(pChange, nChange)); 
        sqlite3_free(pChange);
      }else{
        return test_session_error(interp, rc);
      }
      break;
    }

    case 2:        /* delete */
      Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
      break;

    case 3: {      /* enable */
      int val;
      if( Tcl_GetBooleanFromObj(interp, objv[2], &val) ) return TCL_ERROR;
      val = sqlite3session_enable(pSession, val);
      Tcl_SetObjResult(interp, Tcl_NewBooleanObj(val));
      break;
    }
  }

  return TCL_OK;
}

static void test_session_del(void *clientData){
  sqlite3_session *pSession = (sqlite3_session *)clientData;
  sqlite3session_delete(pSession);
}

/*
** Tclcmd:  sqlite3session CMD DB-HANDLE DB-NAME
*/
static int test_sqlite3session(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db;
  Tcl_CmdInfo info;
  int rc;                         /* sqlite3session_create() return code */
  sqlite3_session *pSession;      /* New session object */

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "CMD DB-HANDLE DB-NAME");
    return TCL_ERROR;
  }

  if( 0==Tcl_GetCommandInfo(interp, Tcl_GetString(objv[2]), &info) ){
    Tcl_AppendResult(interp, "no such handle: ", Tcl_GetString(objv[2]), 0);
    return TCL_ERROR;
  }
  db = *(sqlite3 **)info.objClientData;

  rc = sqlite3session_create(db, Tcl_GetString(objv[3]), &pSession);
  if( rc!=SQLITE_OK ){
    return test_session_error(interp, rc);
  }

  Tcl_CreateObjCommand(
      interp, Tcl_GetString(objv[1]), test_session_cmd, (ClientData)pSession,
      test_session_del
  );
  Tcl_SetObjResult(interp, objv[1]);
  return TCL_OK;
}

static void test_append_value(Tcl_Obj *pList, sqlite3_value *pVal){
  if( pVal==0 ){
    Tcl_ListObjAppendElement(0, pList, Tcl_NewObj());
    Tcl_ListObjAppendElement(0, pList, Tcl_NewObj());
  }else{
    Tcl_Obj *pObj;
    switch( sqlite3_value_type(pVal) ){
      case SQLITE_NULL:
        Tcl_ListObjAppendElement(0, pList, Tcl_NewStringObj("n", 1));
        pObj = Tcl_NewObj();
        break;
      case SQLITE_INTEGER:
        Tcl_ListObjAppendElement(0, pList, Tcl_NewStringObj("i", 1));
        pObj = Tcl_NewWideIntObj(sqlite3_value_int64(pVal));
        break;
      case SQLITE_FLOAT:
        Tcl_ListObjAppendElement(0, pList, Tcl_NewStringObj("f", 1));
        pObj = Tcl_NewDoubleObj(sqlite3_value_double(pVal));
        break;
      case SQLITE_TEXT:
        Tcl_ListObjAppendElement(0, pList, Tcl_NewStringObj("t", 1));
        pObj = Tcl_NewStringObj((char *)sqlite3_value_text(pVal), -1);
        break;
      case SQLITE_BLOB:
        Tcl_ListObjAppendElement(0, pList, Tcl_NewStringObj("b", 1));
        pObj = Tcl_NewByteArrayObj(
            sqlite3_value_blob(pVal),
            sqlite3_value_bytes(pVal)
        );
        break;
    }
    Tcl_ListObjAppendElement(0, pList, pObj);
  }
}

/*
** sqlite3changeset_invert CHANGESET
*/
static int test_sqlite3changeset_invert(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int rc;                         /* Return code from changeset_invert() */
  void *aChangeset;               /* Input changeset */
  int nChangeSet;                 /* Size of buffer aChangeset in bytes */
  void *aOut;                     /* Output changeset */
  int nOut;                       /* Size of buffer aOut in bytes */

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "CHANGESET");
  }
  aChangeset = (void *)Tcl_GetByteArrayFromObj(objv[1], &nChangeSet);

  rc = sqlite3changeset_invert(nChangeSet, aChangeset, &nOut, &aOut);
  if( rc!=SQLITE_OK ){
    return test_session_error(interp, rc);
  }
  Tcl_SetObjResult(interp, Tcl_NewByteArrayObj((unsigned char *)aOut, nOut));
  sqlite3_free(aOut);
  return TCL_OK;
}

/*
** sqlite3session_foreach VARNAME CHANGESET SCRIPT
*/
static int test_sqlite3session_foreach(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  void *pChangeSet;
  int nChangeSet;
  sqlite3_changeset_iter *pIter;
  int rc;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "VARNAME CHANGESET SCRIPT");
    return TCL_ERROR;
  }

  pChangeSet = (void *)Tcl_GetByteArrayFromObj(objv[2], &nChangeSet);
  rc = sqlite3changeset_start(&pIter, nChangeSet, pChangeSet);
  if( rc!=SQLITE_OK ){
    return test_session_error(interp, rc);
  }

  while( SQLITE_ROW==sqlite3changeset_next(pIter) ){
    int nCol;                     /* Number of columns in table */
    int op;                       /* SQLITE_INSERT, UPDATE or DELETE */
    const char *zTab;             /* Name of table change applies to */
    Tcl_Obj *pVar;                /* Tcl value to set $VARNAME to */
    Tcl_Obj *pOld;                /* Vector of old.* values */
    Tcl_Obj *pNew;                /* Vector of new.* values */

    sqlite3changeset_op(pIter, &zTab, &nCol, &op);
    pVar = Tcl_NewObj();
    Tcl_ListObjAppendElement(0, pVar, Tcl_NewStringObj(
          op==SQLITE_INSERT ? "INSERT" :
          op==SQLITE_UPDATE ? "UPDATE" : 
          "DELETE", -1
    ));
    Tcl_ListObjAppendElement(0, pVar, Tcl_NewStringObj(zTab, -1));

    pOld = Tcl_NewObj();
    if( op!=SQLITE_INSERT ){
      int i;
      for(i=0; i<nCol; i++){
        sqlite3_value *pVal;
        sqlite3changeset_old(pIter, i, &pVal);
        test_append_value(pOld, pVal);
      }
    }
    pNew = Tcl_NewObj();
    if( op!=SQLITE_DELETE ){
      int i;
      for(i=0; i<nCol; i++){
        sqlite3_value *pVal;
        sqlite3changeset_new(pIter, i, &pVal);
        test_append_value(pNew, pVal);
      }
    }
    Tcl_ListObjAppendElement(0, pVar, pOld);
    Tcl_ListObjAppendElement(0, pVar, pNew);

    Tcl_ObjSetVar2(interp, objv[1], 0, pVar, 0);
    rc = Tcl_EvalObjEx(interp, objv[3], 0);
    if( rc!=TCL_OK && rc!=TCL_CONTINUE ){
      sqlite3changeset_finalize(pIter);
      return rc==TCL_BREAK ? TCL_OK : rc;
    }
  }
  rc = sqlite3changeset_finalize(pIter);
  if( rc!=SQLITE_OK ){
    return test_session_error(interp, rc);
  }

  return TCL_OK;
}

int TestSession_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "sqlite3session", test_sqlite3session, 0, 0);
  Tcl_CreateObjCommand(
      interp, "sqlite3session_foreach", test_sqlite3session_foreach, 0, 0
  );
  Tcl_CreateObjCommand(
      interp, "sqlite3changeset_invert", test_sqlite3changeset_invert, 0, 0
  );
  return TCL_OK;
}

#endif

