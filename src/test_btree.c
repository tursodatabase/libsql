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
** $Id: test_btree.c,v 1.1 2007/05/05 18:39:25 drh Exp $
*/
#include "btreeInt.h"
#include <tcl.h>

/*
** Print a disassembly of the given page on standard output.  This routine
** is used for debugging and testing only.
*/
static int btreePageDump(
  BtShared *pBt,         /* The Btree to be dumped */
  int pgno,              /* The page to be dumped */
  int recursive,         /* True to decend into child pages */
  MemPage *pParent       /* Parent page */
){
  int rc;
  MemPage *pPage;
  int i, j, c;
  int nFree;
  u16 idx;
  int hdr;
  int nCell;
  int isInit;
  unsigned char *data;
  char range[20];
  unsigned char payload[20];

  rc = sqlite3BtreeGetPage(pBt, (Pgno)pgno, &pPage, 0);
  isInit = pPage->isInit;
  if( pPage->isInit==0 ){
    sqlite3BtreeInitPage(pPage, pParent);
  }
  if( rc ){
    return rc;
  }
  hdr = pPage->hdrOffset;
  data = pPage->aData;
  c = data[hdr];
  pPage->intKey = (c & (PTF_INTKEY|PTF_LEAFDATA))!=0;
  pPage->zeroData = (c & PTF_ZERODATA)!=0;
  pPage->leafData = (c & PTF_LEAFDATA)!=0;
  pPage->leaf = (c & PTF_LEAF)!=0;
  pPage->hasData = !(pPage->zeroData || (!pPage->leaf && pPage->leafData));
  nCell = get2byte(&data[hdr+3]);
  sqlite3DebugPrintf("PAGE %d:  flags=0x%02x  frag=%d   parent=%d\n", pgno,
    data[hdr], data[hdr+7], 
    (pPage->isInit && pPage->pParent) ? pPage->pParent->pgno : 0);
  assert( hdr == (pgno==1 ? 100 : 0) );
  idx = hdr + 12 - pPage->leaf*4;
  for(i=0; i<nCell; i++){
    CellInfo info;
    Pgno child;
    unsigned char *pCell;
    int sz;
    int addr;

    addr = get2byte(&data[idx + 2*i]);
    pCell = &data[addr];
    sqlite3BtreeParseCellPtr(pPage, pCell, &info);
    sz = info.nSize;
    sqlite3_snprintf(sizeof(range),range,"%d..%d", addr, addr+sz-1);
    if( pPage->leaf ){
      child = 0;
    }else{
      child = get4byte(pCell);
    }
    sz = info.nData;
    if( !pPage->intKey ) sz += info.nKey;
    if( sz>sizeof(payload)-1 ) sz = sizeof(payload)-1;
    memcpy(payload, &pCell[info.nHeader], sz);
    for(j=0; j<sz; j++){
      if( payload[j]<0x20 || payload[j]>0x7f ) payload[j] = '.';
    }
    payload[sz] = 0;
    sqlite3DebugPrintf(
      "cell %2d: i=%-10s chld=%-4d nk=%-4lld nd=%-4d payload=%s\n",
      i, range, child, info.nKey, info.nData, payload
    );
  }
  if( !pPage->leaf ){
    sqlite3DebugPrintf("right_child: %d\n", get4byte(&data[hdr+8]));
  }
  nFree = 0;
  i = 0;
  idx = get2byte(&data[hdr+1]);
  while( idx>0 && idx<pPage->pBt->usableSize ){
    int sz = get2byte(&data[idx+2]);
    sqlite3_snprintf(sizeof(range),range,"%d..%d", idx, idx+sz-1);
    nFree += sz;
    sqlite3DebugPrintf("freeblock %2d: i=%-10s size=%-4d total=%d\n",
       i, range, sz, nFree);
    idx = get2byte(&data[idx]);
    i++;
  }
  if( idx!=0 ){
    sqlite3DebugPrintf("ERROR: next freeblock index out of range: %d\n", idx);
  }
  if( recursive && !pPage->leaf ){
    for(i=0; i<nCell; i++){
      unsigned char *pCell = sqlite3BtreeFindCell(pPage, i);
      btreePageDump(pBt, get4byte(pCell), 1, pPage);
      idx = get2byte(pCell);
    }
    btreePageDump(pBt, get4byte(&data[hdr+8]), 1, pPage);
  }
  pPage->isInit = isInit;
  sqlite3PagerUnref(pPage->pDbPage);
  fflush(stdout);
  return SQLITE_OK;
}
int sqlite3BtreePageDump(Btree *p, int pgno, int recursive){
  return btreePageDump(p->pBt, pgno, recursive, 0);
}

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
  const ThreadData *pTd = sqlite3ThreadDataReadOnly();
  if( pTd->useSharedData ){
    BtShared *pBt;
    Tcl_Obj *pRet = Tcl_NewObj();
    for(pBt=pTd->pBtree; pBt; pBt=pBt->pNext){
      const char *zFile = sqlite3PagerFilename(pBt->pPager);
      Tcl_ListObjAppendElement(interp, pRet, Tcl_NewStringObj(zFile, -1));
      Tcl_ListObjAppendElement(interp, pRet, Tcl_NewIntObj(pBt->nRef));
    }
    Tcl_SetObjResult(interp, pRet);
  }
#endif
  return TCL_OK;
}

/*
** Print debugging information about all cursors to standard output.
*/
void sqlite3BtreeCursorList(Btree *p){
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

  int rc = sqlite3BtreeRestoreOrClearCursorPosition(pCur);
  if( rc!=SQLITE_OK ){
    return rc;
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
