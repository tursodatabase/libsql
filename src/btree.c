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
** $Id: btree.c,v 1.4 2001/05/11 11:02:47 drh Exp $
*/
#include "sqliteInt.h"
#include "pager.h"
#include "btree.h"
#include <assert.h>

typedef unsigned int u32;
typedef unsigned short int u16;

/*
** Forward declarations of structures used only in this file.
*/
typedef struct Page1Header Page1Header;
typedef struct PageHdr PageHdr;
typedef struct Cell Cell;
typedef struct FreeBlk FreeBlk;


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

struct Page1Header {
  u32 magic1;
  u32 magic2;
  Pgno firstList;
};

/*
** Each database page has a header as follows:
**
**      page1_header          Extra numbers found on page 1 only.
**      leftmost_pgno         Page number of the leftmost child
**      first_cell            Index into MemPage.aPage of first cell
**      first_free            Index of first free block
**
** MemPage.pStart always points to the leftmost_pgno.  First_free is
** 0 if there is no free space on this page.  Otherwise it points to
** an area like this:
**
**      nByte                 Number of free bytes in this block
**      next_free             Next free block or 0 if this is the end
*/
struct PageHdr {
  Pgno pgno;      /* Child page that comes after all cells on this page */
  u16 firstCell;  /* Index in MemPage.aPage[] of the first cell */
  u16 firstFree;  /* Index in MemPage.aPage[] of the first free block */
};
struct Cell {
  Pgno pgno;      /* Child page that comes before this cell */
  u16 nKey;       /* Number of bytes in the key */
  u16 iNext;      /* Index in MemPage.aPage[] of next cell in sorted order */
  u32 nData;      /* Number of bytes of data */
  char aData[4];  /* Key and data */
};
struct FreeBlk {
  u16 iSize;      /* Number of u32-sized slots in the block of free space */
  u16 iNext;      /* Index in MemPage.aPage[] of the next free block */
};

/*
** The maximum number of database entries that can be held in a single
** page of the database.  Each entry has a 16-byte header consisting of
** 4 unsigned 32-bit numbers, as follows:
**
**       nKey       Number of byte in the key
**       nData      Number of byte in the data
**       pgno       Page number of the right child block 
**       next       index in MemPage.aPage[] of the next entry in sorted order
**
** The key and data follow this header.  The key and data are packed together
** and the total rounded up to the next multiple of 4 bytes.  There must
** be at least 4 bytes in the key/data packet, so each entry consumes at
** least 20 bytes of space on the page.
*/
#define MX_CELL ((SQLITE_PAGE_SIZE-sizeof(PageHdr))/sizeof(Cell))

/*
** The maximum amount of data (in bytes) that can be stored locally for a
** database entry.  If the entry contains more data than this, the
** extra goes onto overflow pages.
*/
#define MX_LOCAL_PAYLOAD ((SQLITE_PAGE_SIZE-20-4*24)/4)

/*
** On a single disk page, there are sections of the page that are used
** to hold data and sections that are unused and available for holding
** new data.  A single instance of this structure describes a contiguous
** block of free space on a disk page.
*/
struct FreeBlk {
  int idx;          /* Index into MemPage.aPage[] of the start of freeblock */
  int size;         /* Number of MemPage.aPage[] slots used by this block */
};
typedef struct FreeBlk;

/*
** For every page in the database file, an instance of the following structure
** is stored in memory.  The aPage[] array contains the data obtained from
** the disk.  The rest is auxiliary data that held in memory only.
*/
struct MemPage {
  u32 aPage[SQLITE_PAGE_SIZE/sizeof(u32)];  /* Page data stored on disk */
  unsigned char isInit;                     /* True if sequel is initialized */
  unsigned char validUp;                    /* True if MemPage.up is valid */
  unsigned char validLeft;                  /* True if MemPage.left is valid */
  unsigned char validRight;                 /* True if MemPage.right is valid */
  Pgno up;                     /* The parent page.  0 means this is the root */
  Pgno left;                   /* Left sibling page.  0==none */
  Pgno right;                  /* Right sibling page.  0==none */
  int idxStart;                /* Index in aPage[] of real data */
  int nFree;                   /* Number of free slots of aPage[] */
  int nCell;                   /* Number of entries on this page */
  u32 *aCell[MX_CELL];         /* All entires in sorted order */
}
typedef struct MemPage;

/*
** The in-memory image of a disk page has the auxiliary information appended
** to the end.  EXTRA_SIZE is the number of bytes of space needed to hold
** that extra information.
*/
#define EXTRA_SIZE (sizeof(MemPage)-SQLITE_PAGE_SIZE)

/*
** Everything we need to know about an open database
*/
struct Btree {
  Pager *pPager;        /* The page cache */
  BtCursor *pCursor;    /* All open cursors */
  MemPage *page1;       /* First page of the database */
  int inTrans;          /* True if a transaction is current */
};
typedef Btree Bt;

/*
** A cursor is a pointer to a particular entry in the BTree.
** The entry is identified by its MemPage and the index in
** MemPage.aCell[] of the entry.
*/
struct Cursor {
  Btree *pBt;           /* The pointer back to the BTree */
  MemPage *pPage;       /* Page that contains the entry */
  int idx;              /* Index of the entry in pPage->aCell[] */
  int skip_incr;        /* */
};

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
  u32 *aIdx;        /* Pointer to pPage[idx] */
};

/*
** Everything we need to know about a cursor
*/
struct BtCursor {
  Btree *pBt;                   /* The whole database */
  BtCursor *pPrev, *pNext;      /* Linked list of all cursors */
  int valid;                    /* True if the cursor points to something */
  int nLevel;                   /* Number of levels of indexing used */
  BtIdxpt *pLevel;              /* Pointer to aLevel[nLevel] */
  BtIdxpt aLevel[MX_LEVEL];     /* The index levels */
};


/*
** Defragment the page given.  All of the free space
** is collected into one big block at the end of the
** page.
*/
static void defragmentPage(MemPage *pPage){
}

/*
** Mark a section of the memory block as in-use.
*/
static void useSpace(MemPage *pPage, int start, int size){
}

/*
** Return a section of the MemPage.aPage[] to the freelist.
*/
static void freeSpace(MemPage *pPage, int start, int size){
}

/*
** Initialize the auxiliary information for a disk block.
*/
static int initPage(MemPage *pPage, Pgno pgnoThis, Pgno pgnoParent){
  u32 idx;
  pPage->isInit = 1;
  pPage->validUp = 1;
  pPage->up = pgnoParent;
  pPage->nFreeSlot = SQLITE_PAGE_SIZE/sizeof(pPage->aPage[0]) - 2;
  pPage->nFree = 1;
  if( pgnoThis==1 ){
    pPage->idxStart = EXTRA_PAGE_1_CELLS;
    pPage->nFreeByte -= EXTRA_PAGE_1_CELLS;
  }
  pPage->aFree[0].idx = pPage->idxStart + 2;
  pPage->aFree[0].size = pPage->nFreeByte;
  pPage->nCell = 0;
  idx = pPage->aPage[pPage->idxStart+1];
  while( idx!=0 ){
    int size;
    pPage->aCell[pPage->nCell++] = idx;
    size = pPage->aPage[idx] + pPage->aPage[idx+1];
    if( size>MX_LOCAL_PAYLOAD ){
      if( size>MX_DIRECT_PAYLOAD ){
        size = MX_LOCAL_PAYLOAD + 2*sizeof(u32);
      }else{
        size = MX_LOCAL_PAYLOAD + sizeof(u32);
      }
    }
    size = (size + sizeof(u32) - 1)/sizeof(u32) + 4;
    useSpace(pPage, idx, size);
    idx = pPage->aPage[idx+3];
  }
  return SQLITE_OK;
}

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
  rc = sqlitepager_open(&pBt->pPager, zFilename, 100, EXTRA_SPACE);
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
    rc = lockBtree(pBt);
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
  rc = initPage(pBt->page1);
  if( rc!=SQLITE_OK ){
    sqlitepager_unref(pBt->page1);
    pBt->page1 = 0;
    return rc;
  }
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

/*
** Return the number of bytes in the key of the entry to which
** the cursor is currently point.  If the cursor has not been
** initialized or is pointed to a deleted entry, then return 0.
*/
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
