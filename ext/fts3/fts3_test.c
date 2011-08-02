/*
** 2011 Jun 13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This file is not part of the production FTS code. It is only used for
** testing. It contains a Tcl command that can be used to test if a document
** matches an FTS NEAR expression.
*/

#include <tcl.h>
#include <string.h>
#include <assert.h>

#ifdef SQLITE_TEST

/* Required so that the "ifdef SQLITE_ENABLE_FTS3" below works */
#include "fts3Int.h"

#define NM_MAX_TOKEN 12

typedef struct NearPhrase NearPhrase;
typedef struct NearDocument NearDocument;
typedef struct NearToken NearToken;

struct NearDocument {
  int nToken;                     /* Length of token in bytes */
  NearToken *aToken;              /* Token array */
};

struct NearToken {
  int n;                          /* Length of token in bytes */
  const char *z;                  /* Pointer to token string */
};

struct NearPhrase {
  int nNear;                      /* Preceding NEAR value */
  int nToken;                     /* Number of tokens in this phrase */
  NearToken aToken[NM_MAX_TOKEN]; /* Array of tokens in this phrase */
};

static int nm_phrase_match(
  NearPhrase *p,
  NearToken *aToken
){
  int ii;

  for(ii=0; ii<p->nToken; ii++){
    NearToken *pToken = &p->aToken[ii];
    if( pToken->n>0 && pToken->z[pToken->n-1]=='*' ){
      if( aToken[ii].n<(pToken->n-1) ) return 0;
      if( memcmp(aToken[ii].z, pToken->z, pToken->n-1) ) return 0;
    }else{
      if( aToken[ii].n!=pToken->n ) return 0;
      if( memcmp(aToken[ii].z, pToken->z, pToken->n) ) return 0;
    }
  }

  return 1;
}

static int nm_near_chain(
  int iDir,                       /* Direction to iterate through aPhrase[] */
  NearDocument *pDoc,             /* Document to match against */
  int iPos,                       /* Position at which iPhrase was found */
  int nPhrase,                    /* Size of phrase array */
  NearPhrase *aPhrase,            /* Phrase array */
  int iPhrase                     /* Index of phrase found */
){
  int iStart;
  int iStop;
  int ii;
  int nNear;
  int iPhrase2;
  NearPhrase *p;
  NearPhrase *pPrev;

  assert( iDir==1 || iDir==-1 );

  if( iDir==1 ){
    if( (iPhrase+1)==nPhrase ) return 1;
    nNear = aPhrase[iPhrase+1].nNear;
  }else{
    if( iPhrase==0 ) return 1;
    nNear = aPhrase[iPhrase].nNear;
  }
  pPrev = &aPhrase[iPhrase];
  iPhrase2 = iPhrase+iDir;
  p = &aPhrase[iPhrase2];

  iStart = iPos - nNear - p->nToken;
  iStop = iPos + nNear + pPrev->nToken;

  if( iStart<0 ) iStart = 0;
  if( iStop > pDoc->nToken - p->nToken ) iStop = pDoc->nToken - p->nToken;

  for(ii=iStart; ii<=iStop; ii++){
    if( nm_phrase_match(p, &pDoc->aToken[ii]) ){
      if( nm_near_chain(iDir, pDoc, ii, nPhrase, aPhrase, iPhrase2) ) return 1;
    }
  }

  return 0;
}

static int nm_match_count(
  NearDocument *pDoc,             /* Document to match against */
  int nPhrase,                    /* Size of phrase array */
  NearPhrase *aPhrase,            /* Phrase array */
  int iPhrase                     /* Index of phrase to count matches for */
){
  int nOcc = 0;
  int ii;
  NearPhrase *p = &aPhrase[iPhrase];

  for(ii=0; ii<(pDoc->nToken + 1 - p->nToken); ii++){
    if( nm_phrase_match(p, &pDoc->aToken[ii]) ){
      /* Test forward NEAR chain (i>iPhrase) */
      if( 0==nm_near_chain(1, pDoc, ii, nPhrase, aPhrase, iPhrase) ) continue;

      /* Test reverse NEAR chain (i<iPhrase) */
      if( 0==nm_near_chain(-1, pDoc, ii, nPhrase, aPhrase, iPhrase) ) continue;

      /* This is a real match. Increment the counter. */
      nOcc++;
    }
  } 

  return nOcc;
}

/*
** Tclcmd: fts3_near_match DOCUMENT EXPR ?OPTIONS?
*/
static int fts3_near_match_cmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int nTotal = 0;
  int rc;
  int ii;
  int nPhrase;
  NearPhrase *aPhrase = 0;
  NearDocument doc = {0, 0};
  Tcl_Obj **apDocToken;
  Tcl_Obj *pRet;
  Tcl_Obj *pPhrasecount = 0;
  
  Tcl_Obj **apExprToken;
  int nExprToken;

  /* Must have 3 or more arguments. */
  if( objc<3 || (objc%2)==0 ){
    Tcl_WrongNumArgs(interp, 1, objv, "DOCUMENT EXPR ?OPTION VALUE?...");
    rc = TCL_ERROR;
    goto near_match_out;
  }

  for(ii=3; ii<objc; ii+=2){
    enum NM_enum { NM_PHRASECOUNTS };
    struct TestnmSubcmd {
      char *zName;
      enum NM_enum eOpt;
    } aOpt[] = {
      { "-phrasecountvar", NM_PHRASECOUNTS },
      { 0, 0 }
    };
    int iOpt;
    if( Tcl_GetIndexFromObjStruct(
        interp, objv[ii], aOpt, sizeof(aOpt[0]), "option", 0, &iOpt) 
    ){
      return TCL_ERROR;
    }

    switch( aOpt[iOpt].eOpt ){
      case NM_PHRASECOUNTS:
        pPhrasecount = objv[ii+1];
        break;
    }
  }

  rc = Tcl_ListObjGetElements(interp, objv[1], &doc.nToken, &apDocToken);
  if( rc!=TCL_OK ) goto near_match_out;
  doc.aToken = (NearToken *)ckalloc(doc.nToken*sizeof(NearToken));
  for(ii=0; ii<doc.nToken; ii++){
    doc.aToken[ii].z = Tcl_GetStringFromObj(apDocToken[ii], &doc.aToken[ii].n);
  }

  rc = Tcl_ListObjGetElements(interp, objv[2], &nExprToken, &apExprToken);
  if( rc!=TCL_OK ) goto near_match_out;

  nPhrase = (nExprToken + 1) / 2;
  aPhrase = (NearPhrase *)ckalloc(nPhrase * sizeof(NearPhrase));
  memset(aPhrase, 0, nPhrase * sizeof(NearPhrase));
  for(ii=0; ii<nPhrase; ii++){
    Tcl_Obj *pPhrase = apExprToken[ii*2];
    Tcl_Obj **apToken;
    int nToken;
    int jj;

    rc = Tcl_ListObjGetElements(interp, pPhrase, &nToken, &apToken);
    if( rc!=TCL_OK ) goto near_match_out;
    if( nToken>NM_MAX_TOKEN ){
      Tcl_AppendResult(interp, "Too many tokens in phrase", 0);
      rc = TCL_ERROR;
      goto near_match_out;
    }
    for(jj=0; jj<nToken; jj++){
      NearToken *pT = &aPhrase[ii].aToken[jj];
      pT->z = Tcl_GetStringFromObj(apToken[jj], &pT->n);
    }
    aPhrase[ii].nToken = nToken;
  }
  for(ii=1; ii<nPhrase; ii++){
    Tcl_Obj *pNear = apExprToken[2*ii-1];
    int nNear;
    rc = Tcl_GetIntFromObj(interp, pNear, &nNear);
    if( rc!=TCL_OK ) goto near_match_out;
    aPhrase[ii].nNear = nNear;
  }

  pRet = Tcl_NewObj();
  Tcl_IncrRefCount(pRet);
  for(ii=0; ii<nPhrase; ii++){
    int nOcc = nm_match_count(&doc, nPhrase, aPhrase, ii);
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(nOcc));
    nTotal += nOcc;
  }
  if( pPhrasecount ){
    Tcl_ObjSetVar2(interp, pPhrasecount, 0, pRet, 0);
  }
  Tcl_DecrRefCount(pRet);
  Tcl_SetObjResult(interp, Tcl_NewBooleanObj(nTotal>0));

 near_match_out: 
  ckfree((char *)aPhrase);
  ckfree((char *)doc.aToken);
  return rc;
}

/*
**   Tclcmd: fts3_configure_incr_load ?CHUNKSIZE THRESHOLD?
**
** Normally, FTS uses hard-coded values to determine the minimum doclist
** size eligible for incremental loading, and the size of the chunks loaded
** when a doclist is incrementally loaded. This command allows the built-in
** values to be overridden for testing purposes.
**
** If present, the first argument is the chunksize in bytes to load doclists
** in. The second argument is the minimum doclist size in bytes to use
** incremental loading with.
**
** Whether or not the arguments are present, this command returns a list of
** two integers - the initial chunksize and threshold when the command is
** invoked. This can be used to restore the default behaviour after running
** tests. For example:
**
**    # Override incr-load settings for testing:
**    set cfg [fts3_configure_incr_load $new_chunksize $new_threshold]
**
**    .... run tests ....
**
**    # Restore initial incr-load settings:
**    eval fts3_configure_incr_load $cfg
*/
static int fts3_configure_incr_load_cmd(
  ClientData clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
#ifdef SQLITE_ENABLE_FTS3
  extern int test_fts3_node_chunksize;
  extern int test_fts3_node_chunk_threshold;
  Tcl_Obj *pRet;

  if( objc!=1 && objc!=3 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?CHUNKSIZE THRESHOLD?");
    return TCL_ERROR;
  }

  pRet = Tcl_NewObj();
  Tcl_IncrRefCount(pRet);
  Tcl_ListObjAppendElement(
      interp, pRet, Tcl_NewIntObj(test_fts3_node_chunksize));
  Tcl_ListObjAppendElement(
      interp, pRet, Tcl_NewIntObj(test_fts3_node_chunk_threshold));

  if( objc==3 ){
    int iArg1;
    int iArg2;
    if( Tcl_GetIntFromObj(interp, objv[1], &iArg1)
     || Tcl_GetIntFromObj(interp, objv[2], &iArg2)
    ){
      Tcl_DecrRefCount(pRet);
      return TCL_ERROR;
    }
    test_fts3_node_chunksize = iArg1;
    test_fts3_node_chunk_threshold = iArg2;
  }

  Tcl_SetObjResult(interp, pRet);
  Tcl_DecrRefCount(pRet);
#endif
  return TCL_OK;
}

int Sqlitetestfts3_Init(Tcl_Interp *interp){
  Tcl_CreateObjCommand(interp, "fts3_near_match", fts3_near_match_cmd, 0, 0);
  Tcl_CreateObjCommand(interp, 
      "fts3_configure_incr_load", fts3_configure_incr_load_cmd, 0, 0
  );
  return TCL_OK;
}
#endif                  /* ifdef SQLITE_TEST */
