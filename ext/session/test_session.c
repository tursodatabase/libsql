
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
**          $session indirect BOOL
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
    { "enable",    1, "BOOL",  }, /* 3 */
    { "indirect",  1, "BOOL",  }, /* 4 */
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
    case 0: {      /* attach */
      char *zArg = Tcl_GetString(objv[2]);
      if( zArg[0]=='*' && zArg[1]=='\0' ) zArg = 0;
      rc = sqlite3session_attach(pSession, zArg);
      if( rc!=SQLITE_OK ){
        return test_session_error(interp, rc);
      }
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

    case 4: {      /* indirect */
      int val;
      if( Tcl_GetBooleanFromObj(interp, objv[2], &val) ) return TCL_ERROR;
      val = sqlite3session_indirect(pSession, val);
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

typedef struct TestConflictHandler TestConflictHandler;
struct TestConflictHandler {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};

static int test_obj_eq_string(Tcl_Obj *p, const char *z){
  int n;
  int nObj;
  char *zObj;

  n = strlen(z);
  zObj = Tcl_GetStringFromObj(p, &nObj);

  return (nObj==n && (n==0 || 0==memcmp(zObj, z, n)));
}

static int test_conflict_handler(
  void *pCtx,                     /* Pointer to TestConflictHandler structure */
  int eConf,                      /* DATA, MISSING, CONFLICT, CONSTRAINT */
  sqlite3_changeset_iter *pIter   /* Handle describing change and conflict */
){
  TestConflictHandler *p = (TestConflictHandler *)pCtx;
  Tcl_Obj *pEval;
  Tcl_Interp *interp = p->interp;
  int ret = 0;                    /* Return value */

  int op;                         /* SQLITE_UPDATE, DELETE or INSERT */
  const char *zTab;               /* Name of table conflict is on */
  int nCol;                       /* Number of columns in table zTab */

  pEval = Tcl_DuplicateObj(p->pScript);
  Tcl_IncrRefCount(pEval);

  sqlite3changeset_op(pIter, &zTab, &nCol, &op, 0);

  /* Append the operation type. */
  Tcl_ListObjAppendElement(0, pEval, Tcl_NewStringObj(
      op==SQLITE_INSERT ? "INSERT" :
      op==SQLITE_UPDATE ? "UPDATE" : 
      "DELETE", -1
  ));

  /* Append the table name. */
  Tcl_ListObjAppendElement(0, pEval, Tcl_NewStringObj(zTab, -1));

  /* Append the conflict type. */
  switch( eConf ){
    case SQLITE_CHANGESET_DATA:
      Tcl_ListObjAppendElement(interp, pEval,Tcl_NewStringObj("DATA",-1));
      break;
    case SQLITE_CHANGESET_NOTFOUND:
      Tcl_ListObjAppendElement(interp, pEval,Tcl_NewStringObj("NOTFOUND",-1));
      break;
    case SQLITE_CHANGESET_CONFLICT:
      Tcl_ListObjAppendElement(interp, pEval,Tcl_NewStringObj("CONFLICT",-1));
      break;
    case SQLITE_CHANGESET_CONSTRAINT:
      Tcl_ListObjAppendElement(interp, pEval,Tcl_NewStringObj("CONSTRAINT",-1));
      break;
  }

  /* If this is not an INSERT, append the old row */
  if( op!=SQLITE_INSERT ){
    int i;
    Tcl_Obj *pOld = Tcl_NewObj();
    for(i=0; i<nCol; i++){
      sqlite3_value *pVal;
      sqlite3changeset_old(pIter, i, &pVal);
      test_append_value(pOld, pVal);
    }
    Tcl_ListObjAppendElement(0, pEval, pOld);
  }

  /* If this is not a DELETE, append the new row */
  if( op!=SQLITE_DELETE ){
    int i;
    Tcl_Obj *pNew = Tcl_NewObj();
    for(i=0; i<nCol; i++){
      sqlite3_value *pVal;
      sqlite3changeset_new(pIter, i, &pVal);
      test_append_value(pNew, pVal);
    }
    Tcl_ListObjAppendElement(0, pEval, pNew);
  }

  /* If this is a CHANGESET_DATA or CHANGESET_CONFLICT conflict, append
  ** the conflicting row. */
  if( eConf==SQLITE_CHANGESET_DATA || eConf==SQLITE_CHANGESET_CONFLICT ){
    int i;
    Tcl_Obj *pConflict = Tcl_NewObj();
    for(i=0; i<nCol; i++){
      sqlite3_value *pVal;
      sqlite3changeset_conflict(pIter, i, &pVal);
      test_append_value(pConflict, pVal);
    }
    Tcl_ListObjAppendElement(0, pEval, pConflict);
  }

  if( TCL_OK!=Tcl_EvalObjEx(interp, pEval, TCL_EVAL_GLOBAL) ){
    Tcl_BackgroundError(interp);
  }else{
    Tcl_Obj *pRes = Tcl_GetObjResult(interp);
    if( test_obj_eq_string(pRes, "OMIT") || test_obj_eq_string(pRes, "") ){
      ret = SQLITE_CHANGESET_OMIT;
    }else if( test_obj_eq_string(pRes, "REPLACE") ){
      ret = SQLITE_CHANGESET_REPLACE;
    }else if( test_obj_eq_string(pRes, "ABORT") ){
      ret = SQLITE_CHANGESET_ABORT;
    }else{
      Tcl_IncrRefCount(pRes);
      Tcl_ResetResult(interp);
      Tcl_AppendResult(interp, "unrecognized conflict handler return: \"", 
          Tcl_GetString(pRes), "\"", 0
      );
      Tcl_DecrRefCount(pRes);
      Tcl_BackgroundError(interp);
    }
  }

  Tcl_DecrRefCount(pEval);
  return ret;
}

/*
** sqlite3changeset_apply DB CHANGESET SCRIPT
*/
static int test_sqlite3changeset_apply(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  sqlite3 *db;                    /* Database handle */
  Tcl_CmdInfo info;               /* Database Tcl command (objv[1]) info */
  int rc;                         /* Return code from changeset_invert() */
  void *pChangeset;               /* Buffer containing changeset */
  int nChangeset;                 /* Size of buffer aChangeset in bytes */
  TestConflictHandler ctx;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB CHANGESET SCRIPT");
    return TCL_ERROR;
  }
  if( 0==Tcl_GetCommandInfo(interp, Tcl_GetString(objv[1]), &info) ){
    Tcl_AppendResult(interp, "no such handle: ", Tcl_GetString(objv[2]), 0);
    return TCL_ERROR;
  }
  db = *(sqlite3 **)info.objClientData;
  pChangeset = (void *)Tcl_GetByteArrayFromObj(objv[2], &nChangeset);
  ctx.pScript = objv[3];
  ctx.interp = interp;

  rc = sqlite3changeset_apply(
      db, nChangeset, pChangeset, test_conflict_handler, (void *)&ctx
  );
  if( rc!=SQLITE_OK ){
    return test_session_error(interp, rc);
  }
  Tcl_ResetResult(interp);
  return TCL_OK;
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
    return TCL_ERROR;
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
    int bIndirect;

    sqlite3changeset_op(pIter, &zTab, &nCol, &op, &bIndirect);
    pVar = Tcl_NewObj();
    Tcl_ListObjAppendElement(0, pVar, Tcl_NewStringObj(
          op==SQLITE_INSERT ? "INSERT" :
          op==SQLITE_UPDATE ? "UPDATE" : 
          "DELETE", -1
    ));
    Tcl_ListObjAppendElement(0, pVar, Tcl_NewStringObj(zTab, -1));
    Tcl_ListObjAppendElement(0, pVar, Tcl_NewBooleanObj(bIndirect));

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
  Tcl_CreateObjCommand(
      interp, "sqlite3changeset_apply", test_sqlite3changeset_apply, 0, 0
  );
  return TCL_OK;
}

#endif

