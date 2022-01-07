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
#if defined(INCLUDE_SQLITE_TCL_H)
#  include "sqlite_tcl.h"
#else
#  include "tcl.h"
#endif

/* Solely for the UNUSED_PARAMETER() macro. */
#include "sqliteInt.h"

#ifdef SQLITE_ENABLE_RTREE

typedef struct BoxGeomCtx BoxGeomCtx;
struct BoxGeomCtx {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};

typedef struct BoxQueryCtx BoxQueryCtx;
struct BoxQueryCtx {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
};

static void testDelUser(void *pCtx){
  BoxGeomCtx *p = (BoxGeomCtx*)pCtx;
  Tcl_EvalObjEx(p->interp, p->pScript, 0);
  Tcl_DecrRefCount(p->pScript);
  sqlite3_free(p);
}

static int invokeTclGeomCb(
  const char *zName, 
  sqlite3_rtree_geometry *p, 
  int nCoord,
  sqlite3_rtree_dbl *aCoord
){
  int rc = SQLITE_OK;
  if( p->pContext ){
    char aPtr[64];
    BoxGeomCtx *pCtx = (BoxGeomCtx*)p->pContext;
    Tcl_Interp *interp = pCtx->interp;
    Tcl_Obj *pScript = 0;
    Tcl_Obj *pParam = 0;
    Tcl_Obj *pCoord = 0;
    int ii;
    Tcl_Obj *pRes;


    pScript = Tcl_DuplicateObj(pCtx->pScript);
    Tcl_IncrRefCount(pScript);
    Tcl_ListObjAppendElement(interp, pScript, Tcl_NewStringObj(zName,-1));

    sqlite3_snprintf(sizeof(aPtr)-1, aPtr, "%p", (void*)p->pContext);
    Tcl_ListObjAppendElement(interp, pScript, Tcl_NewStringObj(aPtr,-1));

    pParam = Tcl_NewObj();
    for(ii=0; ii<p->nParam; ii++){
      Tcl_ListObjAppendElement(
          interp, pParam, Tcl_NewDoubleObj(p->aParam[ii])
      );
    }
    Tcl_ListObjAppendElement(interp, pScript, pParam);

    pCoord = Tcl_NewObj();
    for(ii=0; ii<nCoord; ii++){
      Tcl_ListObjAppendElement(interp, pCoord, Tcl_NewDoubleObj(aCoord[ii]));
    }
    Tcl_ListObjAppendElement(interp, pScript, pCoord);

    sqlite3_snprintf(sizeof(aPtr)-1, aPtr, "%p", (void*)p);
    Tcl_ListObjAppendElement(interp, pScript, Tcl_NewStringObj(aPtr,-1));

    rc = Tcl_EvalObjEx(interp, pScript, 0);
    if( rc!=TCL_OK ){
      rc = SQLITE_ERROR;
    }else{
      int nObj = 0;
      Tcl_Obj **aObj = 0;

      pRes = Tcl_GetObjResult(interp);
      if( Tcl_ListObjGetElements(interp, pRes, &nObj, &aObj) ) return TCL_ERROR;
      if( nObj>0 ){
        const char *zCmd = Tcl_GetString(aObj[0]);
        if( 0==sqlite3_stricmp(zCmd, "zero") ){
          p->aParam[0] = 0.0;
          p->nParam = 1;
        }
        else if( 0==sqlite3_stricmp(zCmd, "user") ){
          if( p->pUser || p->xDelUser ){
            rc = SQLITE_ERROR;
          }else{
            BoxGeomCtx *pBGCtx = sqlite3_malloc(sizeof(BoxGeomCtx));
            if( pBGCtx==0 ){
              rc = SQLITE_NOMEM;
            }else{
              pBGCtx->interp = interp;
              pBGCtx->pScript = Tcl_DuplicateObj(pRes);
              Tcl_IncrRefCount(pBGCtx->pScript);
              Tcl_ListObjReplace(interp, pBGCtx->pScript, 0, 1, 0, 0);
              p->pUser = (void*)pBGCtx;
              p->xDelUser = testDelUser;
            }
          }
        }
        else if( 0==sqlite3_stricmp(zCmd, "user_is_zero") ){
          if( p->pUser || p->xDelUser ) rc = SQLITE_ERROR;
        }
      }
    }
  }
  return rc;
}

/*
# EVIDENCE-OF: R-00693-36727 The legacy xGeom callback is invoked with
# four arguments.

# EVIDENCE-OF: R-50437-53270 The first argument is a pointer to an
# sqlite3_rtree_geometry structure which provides information about how
# the SQL function was invoked.

# EVIDENCE-OF: R-00090-24248 The third argument, aCoord[], is an array
# of nCoord coordinates that defines a bounding box to be tested.

# EVIDENCE-OF: R-28207-40885 The last argument is a pointer into which
# the callback result should be written.

*/
static int box_geom(
  sqlite3_rtree_geometry *p,      /* R-50437-53270 */
  int nCoord,                     /* R-02424-24769 */
  sqlite3_rtree_dbl *aCoord,      /* R-00090-24248 */
  int *pRes                       /* R-28207-40885 */
){
  int ii;

  if( p->nParam!=nCoord ){
    invokeTclGeomCb("box", p, nCoord, aCoord);
    return SQLITE_ERROR;
  }
  if( invokeTclGeomCb("box", p, nCoord, aCoord) ) return SQLITE_ERROR;

  for(ii=0; ii<nCoord; ii+=2){
    if( aCoord[ii]>p->aParam[ii+1] || aCoord[ii+1]<p->aParam[ii] ){
      /* R-28207-40885 */
      *pRes = 0;
      return SQLITE_OK;
    }
  }

  /* R-28207-40885 */
  *pRes = 1;

  return SQLITE_OK;
}

static int SQLITE_TCLAPI register_box_geom(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  extern int getDbPointer(Tcl_Interp*, const char*, sqlite3**);
  extern const char *sqlite3ErrName(int);
  sqlite3 *db;
  BoxGeomCtx *pCtx;
  char aPtr[64];

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SCRIPT");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;

  pCtx = (BoxGeomCtx*)ckalloc(sizeof(BoxGeomCtx*));
  pCtx->interp = interp;
  pCtx->pScript = Tcl_DuplicateObj(objv[2]);
  Tcl_IncrRefCount(pCtx->pScript);

  sqlite3_rtree_geometry_callback(db, "box", box_geom, (void*)pCtx);

  sqlite3_snprintf(64, aPtr, "%p", (void*)pCtx);
  Tcl_SetObjResult(interp, Tcl_NewStringObj(aPtr, -1));
  return TCL_OK;
}

static int box_query(sqlite3_rtree_query_info *pInfo){
  const char *azParentWithin[] = {"not", "partly", "fully", 0};
  BoxQueryCtx *pCtx = (BoxQueryCtx*)pInfo->pContext;
  Tcl_Interp *interp = pCtx->interp;
  Tcl_Obj *pEval;
  Tcl_Obj *pArg;
  Tcl_Obj *pTmp = 0;
  int rc;
  int ii;

  pEval = Tcl_DuplicateObj(pCtx->pScript);
  Tcl_IncrRefCount(pEval);
  pArg = Tcl_NewObj();
  Tcl_IncrRefCount(pArg);

  /* aParam[] */
  pTmp = Tcl_NewObj();
  Tcl_IncrRefCount(pTmp);
  for(ii=0; ii<pInfo->nParam; ii++){
    Tcl_Obj *p = Tcl_NewDoubleObj(pInfo->aParam[ii]);
    Tcl_ListObjAppendElement(interp, pTmp, p);
  }
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("aParam", -1));
  Tcl_ListObjAppendElement(interp, pArg, pTmp);
  Tcl_DecrRefCount(pTmp);

  /* aCoord[] */
  pTmp = Tcl_NewObj();
  Tcl_IncrRefCount(pTmp);
  for(ii=0; ii<pInfo->nCoord; ii++){
    Tcl_Obj *p = Tcl_NewDoubleObj(pInfo->aCoord[ii]);
    Tcl_ListObjAppendElement(interp, pTmp, p);
  }
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("aCoord", -1));
  Tcl_ListObjAppendElement(interp, pArg, pTmp);
  Tcl_DecrRefCount(pTmp);

  /* anQueue[] */
  pTmp = Tcl_NewObj();
  Tcl_IncrRefCount(pTmp);
  for(ii=0; ii<=pInfo->mxLevel; ii++){
    Tcl_Obj *p = Tcl_NewIntObj((int)pInfo->anQueue[ii]);
    Tcl_ListObjAppendElement(interp, pTmp, p);
  }
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("anQueue", -1));
  Tcl_ListObjAppendElement(interp, pArg, pTmp);
  Tcl_DecrRefCount(pTmp);
  
  /* iLevel */
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("iLevel", -1));
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewIntObj(pInfo->iLevel));

  /* mxLevel */
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("mxLevel", -1));
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewIntObj(pInfo->mxLevel));

  /* iRowid */
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("iRowid", -1));
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewWideIntObj(pInfo->iRowid));

  /* rParentScore */
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("rParentScore", -1));
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewDoubleObj(pInfo->rParentScore));

  /* eParentWithin */
  assert( pInfo->eParentWithin==0 
       || pInfo->eParentWithin==1 
       || pInfo->eParentWithin==2 
  );
  Tcl_ListObjAppendElement(interp, pArg, Tcl_NewStringObj("eParentWithin", -1));
  Tcl_ListObjAppendElement(interp, pArg, 
      Tcl_NewStringObj(azParentWithin[pInfo->eParentWithin], -1)
  );

  Tcl_ListObjAppendElement(interp, pEval, pArg);
  rc = Tcl_EvalObjEx(interp, pEval, 0) ? SQLITE_ERROR : SQLITE_OK;

  if( rc==SQLITE_OK ){
    double rScore = 0.0;
    int nObj = 0;
    int eP = 0;
    Tcl_Obj **aObj = 0;
    Tcl_Obj *pRes = Tcl_GetObjResult(interp);

    if( Tcl_ListObjGetElements(interp, pRes, &nObj, &aObj) 
     || nObj!=2 
     || Tcl_GetDoubleFromObj(interp, aObj[1], &rScore)
     || Tcl_GetIndexFromObj(interp, aObj[0], azParentWithin, "value", 0, &eP)
    ){
      rc = SQLITE_ERROR;
    }else{
      pInfo->rScore = rScore;
      pInfo->eParentWithin = eP;
    }
  }

  Tcl_DecrRefCount(pArg);
  Tcl_DecrRefCount(pEval);
  return rc;
}

static void box_query_destroy(void *p){
  BoxQueryCtx *pCtx = (BoxQueryCtx*)p;
  Tcl_DecrRefCount(pCtx->pScript);
  ckfree((char*)pCtx);
}

static int SQLITE_TCLAPI register_box_query(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  extern int getDbPointer(Tcl_Interp*, const char*, sqlite3**);
  extern const char *sqlite3ErrName(int);
  sqlite3 *db;
  BoxQueryCtx *pCtx;

  if( objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DB SCRIPT");
    return TCL_ERROR;
  }
  if( getDbPointer(interp, Tcl_GetString(objv[1]), &db) ) return TCL_ERROR;

  pCtx = (BoxQueryCtx*)ckalloc(sizeof(BoxQueryCtx*));
  pCtx->interp = interp;
  pCtx->pScript = Tcl_DuplicateObj(objv[2]);
  Tcl_IncrRefCount(pCtx->pScript);

  sqlite3_rtree_query_callback(
      db, "qbox", box_query, (void*)pCtx, box_query_destroy
  );

  Tcl_ResetResult(interp);
  return TCL_OK;
}
#endif /* SQLITE_ENABLE_RTREE */


int Sqlitetestrtreedoc_Init(Tcl_Interp *interp){
#ifdef SQLITE_ENABLE_RTREE
  Tcl_CreateObjCommand(interp, "register_box_geom", register_box_geom, 0, 0);
  Tcl_CreateObjCommand(interp, "register_box_query", register_box_query, 0, 0);
#endif /* SQLITE_ENABLE_RTREE */
  return TCL_OK;
}
