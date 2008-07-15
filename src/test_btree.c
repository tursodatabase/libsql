/*
** 2007 May 05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing the btree.c module in SQLite.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library.
**
** $Id: test_btree.c,v 1.6 2008/07/15 00:27:35 drh Exp $
*/
#include "btreeInt.h"
#include <tcl.h>

/*
** Usage: sqlite3_shared_cache_report
**
** Return a list of file that are shared and the number of
** references to each file.
*/
int sqlite3BtreeSharedCacheReport(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
#ifndef SQLITE_OMIT_SHARED_CACHE
  extern BtShared *sqlite3SharedCacheList;
  BtShared *pBt;
  Tcl_Obj *pRet = Tcl_NewObj();
  for(pBt=sqlite3SharedCacheList; pBt; pBt=pBt->pNext){
    const char *zFile = sqlite3PagerFilename(pBt->pPager);
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewStringObj(zFile, -1));
    Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(pBt->nRef));
  }
  Tcl_SetObjResult(interp, pRet);
#endif
  return TCL_OK;
}

/*
** Print debugging information about all cursors to standard output.
*/
void sqlite3BtreeCursorList(Btree *p){
#ifdef SQLITE_DEBUG
  BtCursor *pCur;
  BtShared *pBt = p->pBt;
  for(pCur=pBt->pCursor; pCur; pCur=pCur->pNext){
    MemPage *pPage = pCur->pPage;
    char *zMode = pCur->wrFlag ? "rw" : "ro";
    sqlite3DebugPrintf("CURSOR %p rooted at %4d(%s) currently at %d.%d%s\n",
       pCur, pCur->pgnoRoot, zMode,
       pPage ? pPage->pgno : 0, pCur->idx,
       (pCur->eState==CURSOR_VALID) ? "" : " eof"
    );
  }
#endif
}


/*
** Fill aResult[] with information about the entry and page that the
** cursor is pointing to.
** 
**   aResult[0] =  The page number
**   aResult[1] =  The entry number
**   aResult[2] =  Total number of entries on this page
**   aResult[3] =  Cell size (local payload + header)
**   aResult[4] =  Number of free bytes on this page
**   aResult[5] =  Number of free blocks on the page
**   aResult[6] =  Total payload size (local + overflow)
**   aResult[7] =  Header size in bytes
**   aResult[8] =  Local payload size
**   aResult[9] =  Parent page number
**   aResult[10]=  Page number of the first overflow page
**
** This routine is used for testing and debugging only.
*/
int sqlite3BtreeCursorInfo(BtCursor *pCur, int *aResult, int upCnt){
  int cnt, idx;
  MemPage *pPage = pCur->pPage;
  BtCursor tmpCur;
  int rc;

  if( pCur->eState==CURSOR_REQUIRESEEK ){
    rc = sqlite3BtreeRestoreCursorPosition(pCur);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }

  assert( pPage->isInit );
  sqlite3BtreeGetTempCursor(pCur, &tmpCur);
  while( upCnt-- ){
    sqlite3BtreeMoveToParent(&tmpCur);
  }
  pPage = tmpCur.pPage;
  aResult[0] = sqlite3PagerPagenumber(pPage->pDbPage);
  assert( aResult[0]==pPage->pgno );
  aResult[1] = tmpCur.idx;
  aResult[2] = pPage->nCell;
  if( tmpCur.idx>=0 && tmpCur.idx<pPage->nCell ){
    sqlite3BtreeParseCell(tmpCur.pPage, tmpCur.idx, &tmpCur.info);
    aResult[3] = tmpCur.info.nSize;
    aResult[6] = tmpCur.info.nData;
    aResult[7] = tmpCur.info.nHeader;
    aResult[8] = tmpCur.info.nLocal;
  }else{
    aResult[3] = 0;
    aResult[6] = 0;
    aResult[7] = 0;
    aResult[8] = 0;
  }
  aResult[4] = pPage->nFree;
  cnt = 0;
  idx = get2byte(&pPage->aData[pPage->hdrOffset+1]);
  while( idx>0 && idx<pPage->pBt->usableSize ){
    cnt++;
    idx = get2byte(&pPage->aData[idx]);
  }
  aResult[5] = cnt;
  if( pPage->pParent==0 || sqlite3BtreeIsRootPage(pPage) ){
    aResult[9] = 0;
  }else{
    aResult[9] = pPage->pParent->pgno;
  }
  if( tmpCur.info.iOverflow ){
    aResult[10] = get4byte(&tmpCur.info.pCell[tmpCur.info.iOverflow]);
  }else{
    aResult[10] = 0;
  }
  sqlite3BtreeReleaseTempCursor(&tmpCur);
  return SQLITE_OK;
}
