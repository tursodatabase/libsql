/*
** Copyright (c) 2001 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** $Id: btree.c,v 1.1 2001/04/17 20:09:11 drh Exp $
*/
#include "sqliteInt.h"
#include "pager.h"
#include "btree.h"
#include <assert.h>

typedef unsigned int u32;

/*
** Everything we need to know about an open database
*/
struct Btree {
  Pager *pPager;        /* The page cache */
  BtCursor *pCursor;    /* All open cursors */
  u32 *page1;           /* First page of the database */
  int inTrans;          /* True if a transaction is current */
};
typedef Btree Bt;

/*
** The maximum depth of a cursor
*/
#define MX_LEVEL 20

/*
** Within a cursor, each level off the search tree is an instance of
** this structure.
*/
typedef struct BtIdxpt BtIdxpt;
struct BtIdxpt {
  Pgno pgno;        /* The page number */
  u32 *aPage;       /* The page data */
  int idx;          /* Index into pPage[] */
};

/*
** Everything we need to know about a cursor
*/
struct BtCursor {
  Btree *pBt;                   /* The whole database */
  BtCursor *pPrev, *pNext;      /* Linked list of all cursors */
  int valid;                    /* True if the cursor points to something */
  int nLevel;                   /* Number of levels of indexing used */
  BtIdxpt aLevel[MX_LEVEL];     /* The index levels */
};

/*
** The first page contains the following additional information:
**
**      MAGIC-1
**      MAGIC-2
**      First free block
*/
#define EXTRA_PAGE_1_CELLS  3
#define MAGIC_1  0x7264dc61
#define MAGIC_2  0x54e55d9e

/*
** Open a new database
*/
int sqliteBtreeOpen(const char *zFilename, int mode, Btree **ppBtree){
  Btree *pBt;

  pBt = sqliteMalloc( sizeof(*pBt) );
  if( pBt==0 ){
    **ppBtree = 0;
    return SQLITE_NOMEM;
  }
  rc = sqlitepager_open(&pBt->pPager, zFilename, 100);
  if( rc!=SQLITE_OK ){
    if( pBt->pPager ) sqlitepager_close(pBt->pPager);
    sqliteFree(pBt);
    *ppBtree = 0;
    return rc;
  }
  pBt->pCursor = 0;
  pBt->page1 = 0;
  *ppBtree = pBt;
  return SQLITE_OK;
}

/*
** Close an open database and invalidate all cursors.
*/
int sqliteBtreeClose(Btree *pBt){
  while( pBt->pCursor ){
    sqliteBtreeCloseCursor(pBt->pCursor);
  }
  sqlitepager_close(pBt->pPager);
  sqliteFree(pBt);
  return SQLITE_OK;
}

/*
** Start a new transaction
*/
int sqliteBtreeBeginTrans(Btree *pBt){
  int rc;
  if( pBt->inTrans ) return SQLITE_ERROR;
  if( pBt->page1==0 ){
    rc = sqlitepager_get(pBt->pPager, 1, &pBt->page1);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = sqlitepager_write(pBt->page1);
  if( rc==SQLITE_OK ){
    pBt->inTrans = 1;
  }
  return rc;
}

/*
** Get a reference to page1 of the database file.  This will
** also acquire a readlock on that file.
*/
static int lockBtree(Btree *pBt){
  int rc;
  if( pBt->page1 ) return SQLITE_OK;
  rc = sqlitepager_get(pBt->pPager, 1, &pBt->page1);
  if( rc!=SQLITE_OK ) return rc;
  /* Sanity checking on the database file format */
  return rc;
}

/*
** Remove the last reference to the database file.  This will
** remove the read lock.
*/
static void unlockBtree(Btree *pBt){
  if( pBt->pCursor==0 && pBt->page1!=0 ){
    sqlitepager_unref(pBt->page1);
    pBt->page1 = 0;
    pBt->inTrans = 0;
  }
}

/*
** Commit the transaction currently in progress.  All cursors
** must be closed before this routine is called.
*/
int sqliteBtreeCommit(Btree *pBt){
  int rc;
  assert( pBt->pCursor==0 );
  rc = sqlitepager_commit(pBt->pPager);
  unlockBtree(pBt);
  return rc;
}

/*
** Rollback the transaction in progress.  All cursors must be
** closed before this routine is called.
*/
int sqliteBtreeRollback(Btree *pBt){
  int rc;
  assert( pBt->pCursor==0 );
  rc = sqlitepager_rollback(pBt->pPager);
  unlockBtree(pBt);
  return rc;
}

/*
** Create a new cursor.  The act of acquiring a cursor
** gets a read lock on the database file.
*/
int sqliteBtreeCursor(Btree *pBt, BtCursor **ppCur){
  int rc;
  BtCursor *pCur;
  if( pBt->page1==0 ){
    rc = lockBtree(pBt);
    if( rc!=SQLITE_OK ){
      *ppCur = 0;
      return rc;
    }
  }
  pCur = sqliteMalloc( sizeof(*pCur) );
  if( pCur==0 ){
    *ppCur = 0;
    unlockBtree(pBt);
    return SQLITE_NOMEM;
  }
  pCur->pPrev = 0;
  pCur->pNext = pBt->pCursor;
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur;
  }
  pBt->pCursor = pCur;
  pCur->pBt = pBt;
  pCur->nLevel = 1;
  pCur->aLevel[0].pgno = 1;
  pCur->aLevel[0].aPage = pBt->page1;
  pCur->aLevel[0].idx = 0;
}

/*
** Close a cursor. 
*/
int sqliteBtreeCloseCursor(BtCursor *pCur){
  Btree *pBt = pCur->pBt;
  int i;
  if( pCur->pPrev ){
    pCur->pPrev->pNext = pCur->pNext;
  }else{
    pBt->pCursor = pCur->pNext;
  }
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur->pPrev;
  }
  for(i=pCur->nLevel-1; i>0; i--){
    sqlitepager_unref(pCur->aLevel[i].aPage);
  }
  if( pBt->pCursor==0 && pBt->inTrans==0 ){
    unlockBtree(pBt);
  }
  sqliteFree(pCur);
}

int sqliteBtreeKeySize(BtCursor *pCur){
  int nEntry;
  u32 *aPage;
  BtIdxpt *pIdx;
  int offset;
  if( !pCur->valid ) return 0;
  pIdx = &pCur->aLevel[pCur->nLevel-1];
  aPage = pIdx->aPage;
  offset = (pIdx->pgno==1)*EXTRA_PAGE_1_CELLS;
  nEntry = aPage[offset];
  if( pIdx->idx<nEntry ){
    
}
int sqliteBtreeKey(BtCursor*, int offset, int amt, char *zBuf);
int sqliteBtreeDataSize(BtCursor*);
int sqliteBtreeData(BtCursor*, int offset, int amt, char *zBuf);


/* Move the cursor so that it points to an entry near pKey.
** Return 0 if the cursor is left pointing exactly at pKey.
** Return -1 if the cursor points to the largest entry less than pKey.
** Return 1 if the cursor points to the smallest entry greater than pKey.
*/
int sqliteBtreeMoveto(BtCursor*, void *pKey, int nKey);
int sqliteBtreeDelete(BtCursor*);
int sqliteBtreeInsert(BtCursor*, void *pKey, int nKey, void *pData, int nData);
int sqliteBtreeNext(BtCursor*);
