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
** $Id: btree.c,v 1.10 2001/06/02 02:40:57 drh Exp $
*/
#include "sqliteInt.h"
#include "pager.h"
#include "btree.h"
#include <assert.h>


/*
** Primitive data types.  u32 must be 4 bytes and u16 must be 2 bytes.
** Change these typedefs when porting to new architectures.
*/
typedef unsigned int u32;
typedef unsigned short int u16;
typedef unsigned char u8;

/*
** Forward declarations of structures used only in this file.
*/
typedef struct PageOne PageOne;
typedef struct MemPage MemPage;
typedef struct PageHdr PageHdr;
typedef struct Cell Cell;
typedef struct CellHdr CellHdr;
typedef struct FreeBlk FreeBlk;
typedef struct OverflowPage OverflowPage;

/*
** All structures on a database page are aligned to 4-byte boundries.
** This routine rounds up a number of bytes to the next multiple of 4.
**
** This might need to change for computer architectures that require
** and 8-byte alignment boundry for structures.
*/
#define ROUNDUP(X)  ((X+3) & ~3)

/*
** This is a magic string that appears at the beginning of every
** SQLite database in order to identify the fail as a real database.
*/
static const char zMagicHeader[] = 
   "** This file contains an SQLite 2.0 database **"
#define MAGIC_SIZE (sizeof(zMagicHeader))

/*
** The first page of the database file contains a magic header string
** to identify the file as an SQLite database file.  It also contains
** a pointer to the first free page of the file.  Page 2 contains the
** root of the BTree.
**
** Remember that pages are numbered beginning with 1.  (See pager.c
** for additional information.)  Page 0 does not exist and a page
** number of 0 is used to mean "no such page".
*/
struct PageOne {
  char zMagic[MAGIC_SIZE]; /* String that identifies the file as a database */
  Pgno firstList;          /* First free page in a list of all free pages */
};

/*
** Each database page has a header that is an instance of this
** structure.
**
** MemPage.pHdr always points to the rightmost_pgno.  First_free is
** 0 if there is no free space on this page.  Otherwise, first_free is
** the index in MemPage.aDisk[] of a FreeBlk structure that describes
** the first block of free space.  All free space is defined by a linked
** list of FreeBlk structures.
**
** Data is stored in a linked list of Cell structures.  First_cell is
** the index into MemPage.aDisk[] of the first cell on the page.  The
** Cells are kept in sorted order.
*/
struct PageHdr {
  Pgno rightChild;  /* Child page that comes after all cells on this page */
  u16 firstCell;    /* Index in MemPage.aDisk[] of the first cell */
  u16 firstFree;    /* Index in MemPage.aDisk[] of the first free block */
};

/*
** Entries on a page of the database are called "Cells".  Each Cell
** has a header and data.  This structure defines the header.  The
** key and data (collectively the "payload") follow this header on
** the database page.
**
** A definition of the complete Cell structure is given below.  The
** header for the cell must be defined separately in order to do some
** of the sizing #defines that follow.
*/
struct CellHdr {
  Pgno leftChild; /* Child page that comes before this cell */
  u16 nKey;       /* Number of bytes in the key */
  u16 iNext;      /* Index in MemPage.aDisk[] of next cell in sorted order */
  u32 nData;      /* Number of bytes of data */
}

/*
** The minimum size of a complete Cell.  The Cell must contain a header
** and at least 4 bytes of payload.
*/
#define MIN_CELL_SIZE  (sizeof(CellHdr)+4)

/*
** The maximum number of database entries that can be held in a single
** page of the database. 
*/
#define MX_CELL ((SQLITE_PAGE_SIZE-sizeof(PageHdr))/MIN_CELL_SIZE)

/*
** The maximum amount of data (in bytes) that can be stored locally for a
** database entry.  If the entry contains more data than this, the
** extra goes onto overflow pages.
**
** This number is chosen so that at least 4 cells will fit on every page.
*/
#define MX_LOCAL_PAYLOAD \
  ((SQLITE_PAGE_SIZE-sizeof(PageHdr))/4-(sizeof(CellHdr)+sizeof(Pgno)))

/*
** Data on a database page is stored as a linked list of Cell structures.
** Both the key and the data are stored in aPayload[].  The key always comes
** first.  The aPayload[] field grows as necessary to hold the key and data,
** up to a maximum of MX_LOCAL_PAYLOAD bytes.  If the size of the key and
** data combined exceeds MX_LOCAL_PAYLOAD bytes, then Cell.ovfl is the
** page number of the first overflow page.
**
** Though this structure is fixed in size, the Cell on the database
** page varies in size.  Every cell has a CellHdr and at least 4 bytes
** of payload space.  Additional payload bytes (up to the maximum of
** MX_LOCAL_PAYLOAD) and the Cell.ovfl value are allocated only as
** needed.
*/
struct Cell {
  CellHdr h;                        /* The cell header */
  char aPayload[MX_LOCAL_PAYLOAD];  /* Key and data */
  Pgno ovfl;                        /* The first overflow page */
};

/*
** Free space on a page is remembered using a linked list of the FreeBlk
** structures.  Space on a database page is allocated in increments of
** at least 4 bytes and is always aligned to a 4-byte boundry.  The
** linked list of freeblocks is always kept in order by address.
*/
struct FreeBlk {
  u16 iSize;      /* Number of bytes in this block of free space */
  u16 iNext;      /* Index in MemPage.aDisk[] of the next free block */
};

/*
** Number of bytes on a single overflow page.
*/
#define OVERFLOW_SIZE (SQLITE_PAGE_SIZE-sizeof(Pgno))

/*
** When the key and data for a single entry in the BTree will not fit in
** the MX_LOACAL_PAYLOAD bytes of space available on the database page,
** then all extra data is written to a linked list of overflow pages.
** Each overflow page is an instance of the following structure.
**
** Unused pages in the database are also represented by instances of
** the OverflowPage structure.  The PageOne.freeList field is the
** page number of the first page in a linked list of unused database
** pages.
*/
struct OverflowPage {
  Pgno next;
  char aPayload[OVERFLOW_SIZE];
};

/*
** For every page in the database file, an instance of the following structure
** is stored in memory.  The aDisk[] array contains the raw bits read from
** the disk.  The rest is auxiliary information that held in memory only. The
** auxiliary info is only valid for regular database pages - it is not
** used for overflow pages and pages on the freelist.
**
** Of particular interest in the auxiliary info is the apCell[] entry.  Each
** apCell[] entry is a pointer to a Cell structure in aDisk[].  The cells are
** put in this array so that they can be accessed in constant time, rather
** than in linear time which would be needed if we had to walk the linked 
** list on every access.
**
** The pParent field points back to the parent page.  This allows us to
** walk up the BTree from any leaf to the root.  Care must be taken to
** unref() the parent page pointer when this page is no longer referenced.
** The pageDestructor() routine handles that chore.
*/
struct MemPage {
  char aDisk[SQLITE_PAGE_SIZE];  /* Page data stored on disk */
  int isInit;                    /* True if auxiliary data is initialized */
  MemPage *pParent;              /* The parent of this page.  NULL for root */
  int nFree;                     /* Number of free bytes in aDisk[] */
  int nCell;                     /* Number of entries on this page */
  Cell *apCell[MX_CELL];         /* All data entires in sorted order */
}

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
  BtCursor *pCursor;    /* A list of all open cursors */
  PageOne *page1;       /* First page of the database */
  int inTrans;          /* True if a transaction is in progress */
};
typedef Btree Bt;

/*
** A cursor is a pointer to a particular entry in the BTree.
** The entry is identified by its MemPage and the index in
** MemPage.apCell[] of the entry.
*/
struct BtCursor {
  Btree *pBt;               /* The Btree to which this cursor belongs */
  BtCursor *pPrev, *pNext;  /* List of all cursors */
  MemPage *pPage;           /* Page that contains the entry */
  u16 idx;                  /* Index of the entry in pPage->apCell[] */
  u8 bSkipNext;             /* sqliteBtreeNext() is no-op if true */
  u8 iMatch;                /* compare result from last sqliteBtreeMoveto() */
};

/*
** Compute the total number of bytes that a Cell needs on the main
** database page.  The number returned includes the Cell header,
** local payload storage, and the pointer to overflow pages (if
** applicable).  Additional spaced allocated on overflow pages
** is NOT included in the value returned from this routine.
*/
static int cellSize(Cell *pCell){
  int n = pCell->h.nKey + pCell->h.nData;
  if( n>MX_LOCAL_PAYLOAD ){
    n = MX_LOCAL_PAYLOAD + sizeof(Pgno);
  }else{
    n = ROUNDUP(n);
  }
  n += sizeof(CellHdr);
  return n;
}

/*
** Defragment the page given.  All Cells are moved to the
** beginning of the page and all free space is collected 
** into one big FreeBlk at the end of the page.
*/
static void defragmentPage(MemPage *pPage){
  int pc;
  int i, n;
  FreeBlk *pFBlk;
  char newPage[SQLITE_PAGE_SIZE];

  pc = sizeof(PageHdr);
  ((PageHdr*)pPage)->firstCell = pc;
  memcpy(newPage, pPage->aDisk, pc);
  for(i=0; i<pPage->nCell; i++){
    Cell *pCell = &pPage->apCell[i];
    n = cellSize(pCell);
    pCell->h.iNext = i<pPage->nCell ? pc + n : 0;
    memcpy(&newPage[pc], pCell, n);
    pPage->apCell[i] = (Cell*)&pPage->aDisk[pc];
    pc += n;
  }
  assert( pPage->nFree==SQLITE_PAGE_SIZE-pc );
  memcpy(pPage->aDisk, newPage, pc);
  pFBlk = &pPage->aDisk[pc];
  pFBlk->iSize = SQLITE_PAGE_SIZE - pc;
  pFBlk->iNext = 0;
  ((PageHdr*)pPage)->firstFree = pc;
  memset(&pFBlk[1], 0, SQLITE_PAGE_SIZE - pc - sizeof(FreeBlk));
}

/*
** Allocate space on a page.  The space needs to be at least
** nByte bytes in size.  nByte must be a multiple of 4.
**
** Return the index into pPage->aDisk[] of the first byte of
** the new allocation. Or return 0 if there is not enough free
** space on the page to satisfy the allocation request.
**
** If the page contains nBytes of free space but does not contain
** nBytes of contiguous free space, then defragementPage() is
** called to consolidate all free space before allocating the
** new chunk.
*/
static int allocateSpace(MemPage *pPage, int nByte){
  FreeBlk *p;
  u16 *pIdx;
  int start;

  assert( nByte==ROUNDUP(nByte) );
  if( pPage->nFree<nByte ) return 0;
  pIdx = &((PageHdr*)pPage)->firstFree;
  p = (FreeBlk*)&pPage->aDisk[*pIdx];
  while( p->iSize<nByte ){
    if( p->iNext==0 ){
      defragmentPage(pPage);
      pIdx = &((PageHdr*)pPage)->firstFree;
    }else{
      pIdx = &p->iNext;
    }
    p = (FreeBlk*)&pPage->aDisk[*pIdx];
  }
  if( p->iSize==nByte ){
    start = *pIdx;
    *pIdx = p->iNext;
  }else{
    start = *pIdx;
    FreeBlk *pNew = (FreeBlk*)&pPage->aDisk[start + nByte];
    pNew->iNext = p->iNext;
    pNew->iSize = p->iSize - nByte;
    *pIdx = start + nByte;
  }
  pPage->nFree -= nByte;
  return start;
}

/*
** Return a section of the MemPage.aDisk[] to the freelist.
** The first byte of the new free block is pPage->aDisk[start]
** and the size of the block is "size".
**
** Most of the effort here is involved in coalesing adjacent
** free blocks into a single big free block.
*/
static void freeSpace(MemPage *pPage, int start, int size){
  int end = start + size;
  u16 *pIdx, idx;
  FreeBlk *pFBlk;
  FreeBlk *pNew;
  FreeBlk *pNext;

  assert( size == ROUNDUP(size) );
  assert( start == ROUNDUP(start) );
  pIdx = &((PageHdr*)pPage)->firstFree;
  idx = *pIdx;
  while( idx!=0 && idx<start ){
    pFBlk = (FreeBlk*)&pPage->aDisk[idx];
    if( idx + pFBlk->iSize == start ){
      pFBlk->iSize += size;
      if( idx + pFBlk->iSize == pFBlk->iNext ){
        pNext = (FreeBlk*)&pPage->aDisk[pFblk->iNext];
        pFBlk->iSize += pNext->iSize;
        pFBlk->iNext = pNext->iNext;
      }
      pPage->nFree += size;
      return;
    }
    pIdx = &pFBlk->iNext;
    idx = *pIdx;
  }
  pNew = (FreeBlk*)&pPage->aDisk[start];
  if( idx != end ){
    pNew->iSize = size;
    pNew->iNext = idx;
  }else{
    pNext = (FreeBlk*)&pPage->aDisk[idx];
    pNew->iSize = size + pNext->iSize;
    pNew->iNext = pNext->iNext;
  }
  *pIdx = start;
  pPage->nFree += size;
}

/*
** Initialize the auxiliary information for a disk block.
**
** The pParent parameter must be a pointer to the MemPage which
** is the parent of the page being initialized.  The root of the
** BTree (page 2) has no parent and so for that page, pParent==NULL.
**
** Return SQLITE_OK on success.  If we see that the page does
** not contained a well-formed database page, then return 
** SQLITE_CORRUPT.  Note that a return of SQLITE_OK does not
** guarantee that the page is well-formed.  It only shows that
** we failed to detect any corruption.
*/
static int initPage(MemPage *pPage, Pgno pgnoThis, MemPage *pParent){
  int idx;           /* An index into pPage->aDisk[] */
  Cell *pCell;       /* A pointer to a Cell in pPage->aDisk[] */
  FreeBlk *pFBlk;    /* A pointer to a free block in pPage->aDisk[] */
  int sz;            /* The size of a Cell in bytes */
  int freeSpace;     /* Amount of free space on the page */

  if( pPage->pParent ){
    assert( pPage->pParent==pParent );
    return SQLITE_OK;
  }
  if( pParent ){
    pPage->pParent = pParent;
    sqlitepager_ref(pParent);
  }
  if( pPage->isInit ) return SQLITE_OK;
  pPage->isInit = 1;
  pPage->nCell = 0;
  freeSpace = SQLITE_PAGE_SIZE - sizeof(PageHdr);
  idx = ((PageHdr*)pPage)->firstCell;
  while( idx!=0 ){
    if( idx>SQLITE_PAGE_SIZE-MN_CELL_SIZE ) goto page_format_error;
    if( idx<sizeof(PageHdr) ) goto page_format_error;
    pCell = (Cell*)&pPage->aDisk[idx];
    sz = cellSize(pCell);
    if( idx+sz > SQLITE_PAGE_SIZE ) goto page_format_error;
    freeSpace -= sz;
    pPage->apCell[pPage->nCell++] = pCell;
    idx = pCell->h.iNext;
  }
  pPage->nFree = 0;
  idx = ((PageHdr*)pPage)->firstFree;
  while( idx!=0 ){
    if( idx>SQLITE_PAGE_SIZE-sizeof(FreeBlk) ) goto page_format_error;
    if( idx<sizeof(PageHdr) ) goto page_format_error;
    pFBlk = (FreeBlk*)&pPage->aDisk[idx];
    pPage->nFree += pFBlk->iSize;
    if( pFBlk->iNext <= idx ) goto page_format_error;
    idx = pFBlk->iNext;
  }
  if( pPage->nFree!=freeSpace ) goto page_format_error;
  return SQLITE_OK;

page_format_error:
  return SQLITE_CORRUPT;
}

/*
** Recompute the MemPage.apCell[], MemPage.nCell, and MemPage.nFree parameters
** for a cell after the content has be changed significantly.
**
** The computation here is similar to initPage() except that in this case
** the MemPage.aDisk[] field has been set up internally (instead of 
** having been read from disk) so we do not need to do as much error
** checking.
*/
static void reinitPage(MemPage *pPage){
  Cell *pCell;

  pPage->nCell = 0;
  idx = ((PageHdr*)pPage)->firstCell;
  while( idx!=0 ){
    pCell = (Cell*)&pPage->aDisk[idx];
    sz = cellSize(pCell);
    pPage->apCell[pPage->nCell++] = pCell;
    idx = pCell->h.iNext;
  }
  pPage->nFree = 0;
  idx = ((PageHdr*)pPage)->firstFree;
  while( idx!=0 ){
    pFBlk = (FreeBlk*)&pPage->aDisk[idx];
    pPage->nFree += pFBlk->iSize;
    idx = pFBlk->iNext;
  }
  return SQLITE_OK;
}

/*
** Initialize a database page so that it holds no entries at all.
*/
static void zeroPage(MemPage *pPage){
  PageHdr *pHdr;
  FreeBlk *pFBlk;
  memset(pPage, 0, SQLITE_PAGE_SIZE);
  pHdr = (PageHdr*)pPage;
  pHdr->firstCell = 0;
  pHdr->firstFree = sizeof(*pHdr);
  pFBlk = (FreeBlk*)&pHdr[1];
  pFBlk->iNext = 0;
  pFBlk->iSize = SQLITE_PAGE_SIZE - sizeof(*pHdr);
}

/*
** This routine is called when the reference count for a page
** reaches zero.  We need to unref the pParent pointer when that
** happens.
*/
static void pageDestructor(void *pData){
  MemPage *pPage = (MemPage*)pData;
  if( pPage->pParent ){
    MemPage *pParent = pPage->pParent;
    pPage->pParent = 0;
    sqlitepager_unref(pParent);
  }
}

/*
** Open a new database.
**
** Actually, this routine just sets up the internal data structures
** for accessing the database.  We do not open the database file 
** until the first page is loaded.
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
  sqlitepager_set_destructor(pBt->pPager, pageDestructor);
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
** Get a reference to page1 of the database file.  This will
** also acquire a readlock on that file.
**
** SQLITE_OK is returned on success.  If the file is not a
** well-formed database file, then SQLITE_CORRUPT is returned.
** SQLITE_BUSY is returned if the database is locked.  SQLITE_NOMEM
** is returned if we run out of memory.  SQLITE_PROTOCOL is returned
** if there is a locking protocol violation.
*/
static int lockBtree(Btree *pBt){
  int rc;
  if( pBt->page1 ) return SQLITE_OK;
  rc = sqlitepager_get(pBt->pPager, 1, &pBt->page1);
  if( rc!=SQLITE_OK ) return rc;

  /* Do some checking to help insure the file we opened really is
  ** a valid database file. 
  */
  if( sqlitepager_pagecount(pBt->pPager)>0 ){
    PageOne *pP1 = pBt->page1;
    if( strcmp(pP1->zMagic1,zMagicHeader)!=0 ){
      rc = SQLITE_CORRUPT;
      goto page1_init_failed;
    }
  }
  return rc;

page1_init_failed:
  sqlitepager_unref(pBt->page1);
  pBt->page1 = 0;
  return rc;
}

/*
** Attempt to start a new transaction.
*/
int sqliteBtreeBeginTrans(Btree *pBt){
  int rc;
  Page1Header *pP1;
  if( pBt->inTrans ) return SQLITE_ERROR;
  if( pBt->page1==0 ){
    rc = lockBtree(pBt);
    if( rc!=SQLITE_OK ) return rc;
  }
  rc = sqlitepager_write(pBt->page1);
  if( rc==SQLITE_OK ){
    pBt->inTrans = 1;
  }
  pP1 = (Page1Header*)pBt->page1;
  if( pP1->magic1==0 ){
    pP1->magic1 = MAGIC_1;
    pP1->magic2 = MAGIC_2;
  }
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
  if( pBt->pCursor!=0 ) return SQLITE_ERROR;
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
  if( pBt->pCursor!=0 ) return SQLITE_ERROR;
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
    rc = SQLITE_NOMEM;
    goto create_cursor_exception;
  }
  rc = sqlitepager_get(pBt->pPager, 2, &pCur->pPage);
  if( rc!=SQLITE_OK ){
    goto create_cursor_exception;
  }
  rc = initPage(pCur->pPage, 2, 0);
  if( rc!=SQLITE_OK ){
    goto create_cursor_exception;
  }
  pCur->pPrev = 0;
  pCur->pNext = pBt->pCursor;
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur;
  }
  pBt->pCursor = pCur;
  pCur->pBt = pBt;
  pCur->idx = 0;
  *ppCur = pCur;
  return SQLITE_OK;

create_cursor_exception:
  *ppCur = 0;
  if( pCur ){
    if( pCur->pPage ) sqlitepager_unref(pCur->pPage);
    sqliteFree(pCur);
  }
  unlinkBtree(pBt);
  return rc;
}

/*
** Close a cursor.  The lock on the database file is released
** when the last cursor is closed.
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
  sqlitepager_unref(pCur->pPage);
  unlockBtree(pBt);
  sqliteFree(pCur);
}

/*
** Make a temporary cursor by filling in the fields of pTempCur.
** The temporary cursor is not on the cursor list for the Btree.
*/
static void CreateTemporaryCursor(BtCursor *pCur, BtCursor *pTempCur){
  memcpy(pTempCur, pCur, sizeof(*pCur));
  pTempCur->pNext = 0;
  pTempCur->pPrev = 0;
  sqlitepager_ref(pTempCur->pPage);
}

/*
** Delete a temporary cursor such as was made by the CreateTemporaryCursor()
** function above.
*/
static void DestroyTemporaryCursor(BeCursor *pCur){
  sqlitepager_unref(pCur->pPage);
}

/*
** Set *pSize to the number of bytes of key in the entry the
** cursor currently points to.  Always return SQLITE_OK.
** Failure is not possible.  If the cursor is not currently
** pointing to an entry (which can happen, for example, if
** the database is empty) then *pSize is set to 0.
*/
int sqliteBtreeKeySize(BtCursor *pCur, int *pSize){
  Cell *pCell;
  MemPage *pPage;

  pPage = pCur->pPage;
  assert( pPage!=0 );
  if( pCur->idx >= pPage->nCell ){
    *pSize = 0;
  }else{
    pCell = pPage->apCell[pCur->idx];
    *psize = pCell->h.nKey;
  }
  return SQLITE_OK;
}

/*
** Read payload information from the entry that the pCur cursor is
** pointing to.  Begin reading the payload at "offset" and read
** a total of "amt" bytes.  Put the result in zBuf.
**
** This routine does not make a distinction between key and data.
** It just reads bytes from the payload area.
*/
static int getPayload(BtCursor *pCur, int offset, int amt, char *zBuf){
  char *aPayload;
  Pgno nextPage;
  assert( pCur!=0 && pCur->pPage!=0 );
  assert( pCur->idx>=0 && pCur->idx<pCur->nCell );
  aPayload = pCur->pPage->apCell[pCur->idx].aPayload;
  if( offset<MX_LOCAL_PAYLOAD ){
    int a = amt;
    if( a+offset>MX_LOCAL_PAYLOAD ){
      a = MX_LOCAL_PAYLOAD - offset;
    }
    memcpy(zBuf, &aPayload[offset], a);
    if( a==amt ){
      return SQLITE_OK;
    }
    offset += a;
    zBuf += a;
    amt -= a;
  }
  if( amt>0 ){
    nextPage = pCur->pPage->apCell[pCur->idx].ovfl;
  }
  while( amt>0 && nextPage ){
    OverflowPage *pOvfl;
    rc = sqlitepager_get(pCur->pBt->pPager, nextPage, &pOvfl);
    if( rc!=0 ){
      return rc;
    }
    nextPage = pOvfl->next;
    if( offset<OVERFLOW_SIZE ){
      int a = amt;
      if( a + offset > OVERFLOW_SIZE ){
        a = OVERFLOW_SIZE - offset;
      }
      memcpy(zBuf, &pOvfl->aPayload[offset], a);
      amt -= a;
      zBuf += a;
    }
    offset -= OVERFLOW_SIZE;
    sqlitepager_unref(pOvfl);
  }
  return amt==0 ? SQLITE_OK : SQLITE_CORRUPT;
}

/*
** Read part of the key associated with cursor pCur.  A total
** of "amt" bytes will be transfered into zBuf[].  The transfer
** begins at "offset".  If the key does not contain enough data
** to satisfy the request, no data is fetched and this routine
** returns SQLITE_ERROR.
*/
int sqliteBtreeKey(BtCursor *pCur, int offset, int amt, char *zBuf){
  Cell *pCell;
  MemPage *pPage;

  if( amt<0 ) return SQLITE_ERROR;
  if( offset<0 ) return SQLITE_ERROR;
  if( amt==0 ) return SQLITE_OK;
  pPage = pCur->pPage;
  assert( pPage!=0 );
  if( pCur->idx >= pPage->nCell ){
    return SQLITE_ERROR;
  }
  pCell = pPage->apCell[pCur->idx];
  if( amt+offset > pCell->h.nKey ){
    return SQLITE_ERROR;
  }
  return getPayload(pCur, offset, amt, zBuf);
}

/*
** Set *pSize to the number of bytes of data in the entry the
** cursor currently points to.  Always return SQLITE_OK.
** Failure is not possible.  If the cursor is not currently
** pointing to an entry (which can happen, for example, if
** the database is empty) then *pSize is set to 0.
*/
int sqliteBtreeDataSize(BtCursor *pCur, int *pSize){
  Cell *pCell;
  MemPage *pPage;

  pPage = pCur->pPage;
  assert( pPage!=0 );
  if( pCur->idx >= pPage->nCell ){
    *pSize = 0;
  }else{
    pCell = pPage->apCell[pCur->idx];
    *pSize = pCell->h.nData;
  }
  return SQLITE_OK;
}

/*
** Read part of the data associated with cursor pCur.  A total
** of "amt" bytes will be transfered into zBuf[].  The transfer
** begins at "offset".  If the size of the data in the record
** is insufficent to satisfy this request then no data is read
** and this routine returns SQLITE_ERROR.
*/
int sqliteBtreeData(BtCursor *pCur, int offset, int amt, char *zBuf){
  Cell *pCell;
  MemPage *pPage;

  if( amt<0 ) return SQLITE_ERROR;
  if( offset<0 ) return SQLITE_ERROR;
  if( amt==0 ) return SQLITE_OK;
  pPage = pCur->pPage;
  assert( pPage!=0 );
  if( pCur->idx >= pPage->nCell ){
    return SQLITE_ERROR;
  }
  pCell = pPage->apCell[pCur->idx];
  if( amt+offset > pCell->h.nData ){
    return SQLITE_ERROR;
  }
  return getPayload(pCur, offset + pCell->h.nKey, amt, zBuf);
}

/*
** Compare the key for the entry that pCur points to against the 
** given key (pKey,nKeyOrig).  Put the comparison result in *pResult.
** The result is negative if pCur<pKey, zero if they are equal and
** positive if pCur>pKey.
**
** SQLITE_OK is returned on success.  If part of the cursor key
** is on overflow pages and we are unable to access those overflow
** pages, then some other value might be returned to indicate the
** reason for the error.
*/
static int compareKey(BtCursor *pCur, char *pKey, int nKeyOrig, int *pResult){
  Pgno nextPage;
  int nKey = nKeyOrig;
  int n;
  Cell *pCell;

  assert( pCur->pPage );
  assert( pCur->idx>=0 && pCur->idx<pCur->pPage->nCell );
  pCell = pCur->pPage->apCell[pCur->idx];
  if( nKey > pCell->h.nKey ){
    nKey = pCell->h.nKey;
  }
  n = nKey;
  if( n>MX_LOCAL_PAYLOAD ){
    n = MX_LOCAL_PAYLOAD;
  }
  c = memcmp(pCell->aPayload, pKey, n);
  if( c!=0 ){
    *pResult = c;
    return SQLITE_OK;
  }
  pKey += n;
  nKey -= n;
  nextPage = pCell->ovfl;
  while( nKey>0 ){
    OverflowPage *pOvfl;
    if( nextPage==0 ){
      return SQLITE_CORRUPT;
    }
    rc = sqlitepager_get(pCur->pBt->pPager, nextPage, &pOvfl);
    if( rc ){
      return rc;
    }
    nextPage = pOvfl->next;
    n = nKey;
    if( n>OVERFLOW_SIZE ){
      n = OVERFLOW_SIZE;
    }
    c = memcmp(pOvfl->aPayload, pKey, n);
    sqlitepager_unref(pOvfl);
    if( c!=0 ){
      *pResult = c;
      return SQLITE_OK;
    }
    nKey -= n;
    pKey += n;
  }
  c = pCell->h.nKey - nKeyOrig;
  *pResult = c;
  return SQLITE_OK;
}

/*
** Move the cursor down to a new child page.
*/
static int moveToChild(BtCursor *pCur, int newPgno){
  int rc;
  MemPage *pNewPage;

  rc = sqlitepager_get(pCur->pBt->pPager, newPgno, &pNewPage);
  if( rc ){
    return rc;
  }
  initPage(pNewPage, newPgno, pCur->pPage);
  sqlitepager_unref(pCur->pPage);
  pCur->pPage = pNewPage;
  pCur->idx = 0;
  return SQLITE_OK;
}

/*
** Move the cursor up to the parent page.
**
** pCur->idx is set to the cell index that contains the pointer
** to the page we are coming from.  If we are coming from the
** right-most child page then pCur->idx is set to one more than
** the largest cell index.
*/
static int moveToParent(BtCursor *pCur){
  Pgno oldPgno;
  MemPage *pParent;

  pParent = pCur->pPage->pParent;
  if( pParent==0 ) return SQLITE_INTERNAL;
  oldPgno = sqlitepager_pagenumber(pCur->pPage);
  sqlitepager_ref(pParent);
  sqlitepager_unref(pCur->pPage);
  pCur->pPage = pParent;
  pCur->idx = pPage->nCell;
  for(i=0; i<pPage->nCell; i++){
    if( pPage->apCell[i].h.leftChild==oldPgno ){
      pCur->idx = i;
      break;
    }
  }
  return SQLITE_OK;
}

/*
** Move the cursor to the root page
*/
static int moveToRoot(BtCursor *pCur){
  MemPage *pNew;
  int rc;

  rc = sqlitepager_get(pCur->pBt->pPager, 2, &pNew);
  if( rc ) return rc;
  sqlitepager_unref(pCur->pPage);
  pCur->pPage = pNew;
  pCur->idx = 0;
  return SQLITE_OK;
}

/*
** Move the cursor down to the left-most leaf entry beneath the
** entry to which it is currently pointing.
*/
static int moveToLeftmost(BtCursor *pCur){
  Pgno pgno;
  int rc;

  while( (pgno = pCur->pPage->apCell[pCur->idx]->h.leftChild)!=0 ){
    rc = moveToChild(pCur, pgno);
    if( rc ) return rc;
  }
  return SQLITE_OK;
}


/* Move the cursor so that it points to an entry near pKey.
** Return a success code.
**
** If an exact match is not found, then the cursor is always
** left pointing at a leaf page which would hold the entry if it
** were present.  The cursor might point to an entry that comes
** before or after the key.
**
** The result of comparing the key with the entry to which the
** cursor is left pointing is stored in pCur->iMatch.  The same
** value is also written to *pRes if pRes!=NULL.  The meaning of
** this value is as follows:
**
**     *pRes<0      The cursor is left pointing at an entry that
**                  is larger than pKey.
**
**     *pRes==0     The cursor is left pointing at an entry that
**                  exactly matches pKey.
**
**     *pRes>0      The cursor is left pointing at an entry that
**                  is smaller than pKey.
*/
int sqliteBtreeMoveto(BtCursor *pCur, void *pKey, int nKey, int *pRes){
  int rc;
  rc = moveToRoot(pCur);
  if( rc ) return rc;
  for(;;){
    int lwr, upr;
    Pgno chldPg;
    MemPage *pPage = pCur->pPage;
    lwr = 0;
    upr = pPage->nCell-1;
    while( lwr<=upr ){
      int c;
      pCur->idx = (lwr+upr)/2;
      rc = compareKey(pCur, pKey, nKey, &c);
      if( rc ) return rc;
      if( c==0 ){
        pCur->iMatch = c;
        if( pRes ) *pRes = 0;
        return SQLITE_OK;
      }
      if( c<0 ){
        lwr = pCur->idx+1;
      }else{
        upr = pCur->idx-1;
      }
    }
    assert( lwr==upr+1 );
    if( lwr>=pPage->nCell ){
      chldPg = ((PageHdr*)pPage)->rightChild;
    }else{
      chldPg = pPage->apCell[lwr]->h.leftChild;
    }
    if( chldPg==0 ){
      pCur->iMatch = c;
      if( pRes ) *pRes = c;
      return SQLITE_OK;
    }
    rc = moveToChild(pCur, chldPg);
    if( rc ) return rc;
  }
  /* NOT REACHED */
}

/*
** Advance the cursor to the next entry in the database.  If
** successful and pRes!=NULL then set *pRes=0.  If the cursor
** was already pointing to the last entry in the database before
** this routine was called, then set *pRes=1 if pRes!=NULL.
*/
int sqliteBtreeNext(BtCursor *pCur, int *pRes){
  int rc;
  if( pCur->bSkipNext ){
    pCur->bSkipNext = 0;
    if( pRes ) *pRes = 0;
    return SQLITE_OK;
  }
  pCur->idx++;
  if( pCur->idx>=pCur->pPage->nCell ){
    if( ((PageHdr*)pPage)->rightChild ){
      rc = moveToChild(pCur, ((PageHdr*)pPage)->rightChild);
      if( rc ) return rc;
      rc = moveToLeftmost(pCur);
      if( rc ) return rc;
      if( pRes ) *pRes = 0;
      return SQLITE_OK;
    }
    do{
      if( pCur->pParent==0 ){
        if( pRes ) *pRes = 1;
        return SQLITE_OK;
      }
      rc = moveToParent(pCur);
      if( rc ) return rc;
    }while( pCur->idx>=pCur->pPage->nCell );
    if( pRes ) *pRes = 0;
    return SQLITE_OK;
  }
  rc = moveToLeftmost(pCur);
  if( rc ) return rc;
  if( pRes ) *pRes = 0;
  return SQLITE_OK;
}

/*
** Allocate a new page from the database file.
**
** The new page is marked as dirty.  (In other words, sqlitepager_write()
** has already been called on the new page.)  The new page has also
** been referenced and the calling routine is responsible for calling
** sqlitepager_unref() on the new page when it is done.
**
** SQLITE_OK is returned on success.  Any other return value indicates
** an error.  *ppPage and *pPgno are undefined in the event of an error.
** Do not invoke sqlitepager_unref() on *ppPage if an error is returned.
*/
static int allocatePage(Btree *pBt, MemPage **ppPage, Pgno *pPgno){
  PageOne *pPage1 = pBt->page1;
  if( pPage1->freeList ){
    OverflowPage *pOvfl;
    rc = sqlitepager_write(pPage1);
    if( rc ) return rc;
    *pPgno = pPage1->freeList;
    rc = sqlitepager_get(pBt->pPager, pPage1->freeList, &pOvfl);
    if( rc ) return rc;
    rc = sqlitepager_write(pOvfl);
    if( rc ){
      sqlitepager_unref(pOvfl);
      return rc;
    }
    pPage1->freeList = pOvfl->next;
    *ppPage = (MemPage*)pOvfl;
  }else{
    *pPgno = sqlitepager_pagecount(pBt->pPager);
    rc = sqlitepager_get(pBt->pPager, *pPgno, ppPage);
    if( rc ) return rc;
    rc = sqlitepager_write(*ppPage);
  }
  return rc;
}

/*
** Add a page of the database file to the freelist.  Either pgno or
** pPage but not both may be 0. 
**
** sqlitepager_unref() is NOT called for pPage.  The calling routine
** needs to do that.
*/
static int freePage(Btree *pBt, void *pPage, Pgno pgno){
  PageOne *pPage1 = pBt->page1;
  OverflowPage *pOvfl = (OverflowPage*)pPage;
  int rc;
  int needOvflUnref = 0;
  if( pgno==0 ){
    assert( pOvfl!=0 );
    pgno = sqlitepager_pagenumber(pOvfl);
  }
  rc = sqlitepager_write(pPage1);
  if( rc ){
    return rc;
  }
  if( pOvfl==0 ){
    assert( pgno>0 );
    rc = sqlitepager_get(pBt->pPager, pgno, &pOvfl);
    if( rc ) return rc;
    needOvflUnref = 1;
  }
  rc = sqlitepager_write(pOvfl);
  if( rc ){
    if( needOvflUnref ) sqlitepager_unref(pOvfl);
    return rc;
  }
  pOvfl->next = pPage1->freeList;
  pPage1->freeList = pgno;
  memset(pOvfl->aPayload, 0, OVERFLOW_SIZE);
  pPage->isInit = 0;
  assert( pPage->pParent==0 );
  rc = sqlitepager_unref(pOvfl);
  return rc;
}

/*
** Erase all the data out of a cell.  This involves returning overflow
** pages back the freelist.
*/
static int clearCell(Btree *pBt, Cell *pCell){
  Pager *pPager = pBt->pPager;
  OverflowPage *pOvfl;
  Pgno ovfl, nextOvfl;
  int rc;

  if( pCell->h.nKey + pCell->h.nData <= MX_LOCAL_PAYLOAD ){
    return SQLITE_OK;
  }
  ovfl = pCell->ovfl;
  pCell->ovfl = 0;
  while( ovfl ){
    rc = sqlitepager_get(pPager, ovfl, &pOvfl);
    if( rc ) return rc;
    nextOvfl = pOvfl->next;
    rc = freePage(pBt, pOvfl, ovfl);
    if( rc ) return rc;
    ovfl = nextOvfl;
    sqlitepager_unref(pOvfl);
  }
  return SQLITE_OK;
}

/*
** Create a new cell from key and data.  Overflow pages are allocated as
** necessary and linked to this cell.  
*/
static int fillInCell(
  Btree *pBt,              /* The whole Btree.  Needed to allocate pages */
  Cell *pCell,             /* Populate this Cell structure */
  void *pKey, int nKey,    /* The key */
  void *pData,int nData    /* The data */
){
  int OverflowPage *pOvfl;
  Pgno *pNext;
  int spaceLeft;
  int n;
  int nPayload;
  char *pPayload;
  char *pSpace;

  pCell->h.leftChild = 0;
  pCell->h.nKey = nKey;
  pCell->h.nData = nData;
  pCell->h.iNext = 0;

  pNext = &pCell->ovfl;
  pSpace = pCell->aPayload;
  spaceLeft = MX_LOCAL_PAYLOAD;
  pPayload = pKey;
  pKey = 0;
  nPayload = nKey;
  while( nPayload>0 ){
    if( spaceLeft==0 ){
      rc = allocatePage(pBt, &pOvfl, pNext);
      if( rc ){
        *pNext = 0;
        clearCell(pBt, pCell);
        return rc;
      }
      spaceLeft = OVERFLOW_SIZE;
      pSpace = pOvfl->aPayload;
      pNextPg = &pOvfl->next;
    }
    n = nPayload;
    if( n>spaceLeft ) n = spaceLeft;
    memcpy(pSpace, pPayload, n);
    nPayload -= n;
    if( nPayload==0 && pData ){
      pPayload = pData;
      nPayload = nData;
      pData = 0;
    }else{
      pPayload += n;
    }
    spaceLeft -= n;
    pSpace += n;
  }
  return SQLITE_OK;
}

/*
** Change the MemPage.pParent pointer on the page whose number is
** given in the second argument sot that MemPage.pParent holds the
** pointer in the third argument.
*/
static void reparentPage(Pager *pPager, Pgno pgno, MemPage *pNewParent){
  MemPage *pThis;

  assert( pPager!=0 && pgno!=0 );
  pThis = sqlitepager_lookup(pPager, pgno);
  if( pThis && pThis->pParent!=pNewParent ){
    if( pThis->pParent ) sqlitepager_unref(pThis->pParent);
    pThis->pParent = pNewParent;
    if( pNewParent ) sqlitepager_ref(pNewParent);
  }
}

/*
** Reparent all children of the given page to be the given page.
** In other words, for every child of pPage, invoke reparentPage()
** to make sure that child knows that pPage is its parent.
**
** This routine gets called after you memcpy() one page into
** another.
*/
static void reparentChildPages(Pager *pPager, Page *pPage){
  int i;
  for(i=0; i<pPage->nCell; i++){
    reparentPage(pPager, pPage->apCell[i]->leftChild, pPage);
  }
  reparentPage(pPager, ((PageHdr*)pPage)->rightChild, pPage);
}

/*
** Attempt to move N or more bytes out of the page that the cursor
** points to into the left sibling page.  (The left sibling page
** contains cells that are less than the cells on this page.)  The
** entry that the cursor is pointing to cannot be moved.  Return
** TRUE if successful and FALSE if not.
**
** Reasons for not being successful include: 
**
**    (1) there is no left sibling,
**    (2) we could only move N-1 bytes or less,
**    (3) some kind of file I/O error occurred
**
** Note that a partial rotation may have occurred even if this routine
** returns FALSE.  Failure means we could not rotation a full N bytes.
** If it is possible to rotation some smaller number M, then the 
** rotation occurs but we still return false.
**
** Example:  Consider a segment of the Btree that looks like the
** figure below prior to rotation.  The cursor is pointing to the
** entry *.  The sort order of the entries is A B C D E * F Y.
**
**
**            -------------------------
**                ... | C | Y | ...
**            -------------------------
**                     /     \
**            ---------       -----------------
**            | A | B |       | D | E | * | F |
**            ---------       -----------------
**
** After rotation of two cells (D and E), the same Btree segment 
** looks like this:
**
**            -------------------------
**                ... | E | Y | ...
**            -------------------------
**                     /     \
**    -----------------       ---------
**    | A | B | C | D |       | * | F |
**    -----------------       ---------
**
** The size of this rotation is the size by which the page containing
** the cursor was reduced.  In this case, the size of D and E.
**
*/
static int rotateLeft(BtCursor *pCur, int N){
  return 0;
}

/*
** This routine is the same as rotateLeft() except that it move data
** to the right instead of to the left.  See comments on the rotateLeft()
** routine for additional information.
*/
static int rotateRight(BtCursor *pCur, int N){
  return 0;
}

/*
** Append a cell onto the end of a page.
**
** The child page of the cell is reparented if pPager!=NULL.
*/
static void appendCell(
  Pager *pPager,      /* The page cache.  Needed for reparenting */
  Cell *pSrc,         /* The Cell to be copied onto a new page */
  MemPage *pPage      /* The page into which the cell is copied */
){
  int pc;
  int sz;
  Cell *pDest;

  sz = cellSize(pSrc);
  pc = allocateSpace(pPage, sz);
  assert( pc>0 ){
  pDest = pPage->apCell[pPage->nCell] = &pPage->aDisk[pc];
  memcpy(pDest, pSrc, sz);
  pDest->h.iNext = 0;
  if( pPage->nCell>0 ){
    pPage->apCell[pPage->nCell-1]->h.iNext = pc;
  }else{
    ((PageHdr*)pPage)->firstCell = pc;
  }
  if( pPager && pDest->h.leftChild ){
    reparentPage(pPager, pDest->h.leftChild, pPage);
  }
}

/*
** Split a single database page into two roughly equal-sized pages.
**
** The input is an existing page and a new Cell.  The Cell might contain
** a valid Cell.h.leftChild field pointing to a child page.
**
** The output is the Cell that divides the two new pages.  The content
** of this divider Cell is written into *pCenter.  pCenter->h.leftChild
** holds the page number of the new page that was created to hold the 
** smaller of the cells from the divided page.  The larger cells from
** the divided page are written to a newly allocated page and *ppOut 
** is made to point to that page.  Or if ppOut==NULL then the larger cells
** remain on pIn.
**
** Upon return, pCur should be pointing to the same cell, even if that
** cell has moved to a new page.  The cell that pCur points to cannot
** be the pCenter cell.
*/
static int split(
  BtCursor *pCur,     /* A cursor pointing at a cell on the page to be split */
  Cell *pNewCell,     /* A new cell to add to pIn before dividing it up */
  Cell *pCenter,      /* Write the cell that divides the two pages here */
  MemPage **ppOut     /* If not NULL, put larger cells in new page at *ppOut */
){
  MemPage *pLeft, *pRight;
  Pgno pgnoLeft, pgnoRight;
  PageHdr *pHdr;
  int rc;
  Pager *pPager = pCur->pBt->pPager;
  MemPage tempPage;

  /* Allocate pages to hold cells after the split and make pRight and 
  ** pLeft point to the newly allocated pages.
  */
  rc = allocatePage(pCur->pBt, &pLeft, &pgnoLeft);
  if( rc ) return rc;
  if( ppOut ){
    rc = allocatePage(pCur->pBt, &pRight, &pgnoRight);
    if( rc ){
      freePage(pCur->pBt, pLeft, pgnoLeft);
      return rc;
    }
    *ppOut = pRight;
  }else{
    *ppOut = tempPage;
  }

  /* Copy the smaller cells from the original page into the left page
  ** of the split.
  */
  zeroPage(pLeft);
  if( pCur->idx==0 && pCur->match>0 ){
    appendCell(pPager, pNewCell, pLeft);
  }
  do{
    assert( i<pPage->nCell );
    appendCell(pPager, pPage->apCell[i++], pLeft);
    if( pCur->idx==i && pCur->iMatch>0 ){
      appendCell(pPager, pNewCell, Left);
    }
  }while( pc < SQLITE_PAGE_SIZE/2 );

  /* Copy the middle entry into *pCenter
  */
  assert( i<pPage->nCell );
  memcpy(pCenter, pPage->aCell[i], cellSize(pPage->aCell[i]));
  i++;
  pHdr = (PageHdr*)pLeft;
  pHdr->rightChild = pCenter->h.leftChild;
  if( pHdr->rightChild ){
    reparentPage(pPager, pHdr->rightChild, pLeft);
  }
  pCenter->h.leftChild = pgnoLeft;
 
  /* Copy the larger cells from the original page into the right
  ** page of the split
  */
  zeroPage(pRight);
  while( i<pPage->nCell ){
    appendCell(0, pPage->apCell[i++], pRight);
  }

  /* If ppOut==NULL then copy the temporary right page over top of
  ** the original input page.
  */
  if( ppOut==0 ){
    pRight->pParent = pPage->pParent;
    pRight->isInit = 1;
    memcpy(pPage, pRight, sizeof(*pPage));
  }
  reparentChildPages(pPager, pPage);
}

/*
** Unlink a cell from a database page.  Add the space used by the cell
** back to the freelist for the database page on which the cell used to
** reside.
**
** This operation overwrites the cell header and content.
*/
static void unlinkCell(BtCursor *pCur){
  MemPage *pPage;    /* Page containing cell to be unlinked */
  int idx;           /* The index of the cell to be unlinked */
  Cell *pCell;       /* Pointer to the cell to be unlinked */
  u16 *piCell;       /* iNext pointer from prior cell */
  int iCell;         /* Index in pPage->aDisk[] of cell to be unlinked */
  int i;             /* Loop counter */

  pPage = pCur->pPage;
  sqlitepager_write(pPage);
  idx = pCur->idx;
  pCell = pPage->apCell[idx];
  if( idx==0 ){
    piCell = &pPage->pHdr->firstCell;
  }else{
    piCell = &pPage->apCell[idx-1]->h.iNext;
  }
  iCell = *piCell;
  *piCell = pCell->h.iNext;
  freeSpace(pPage, iCell, cellSize(pCell));
  pPage->nCell--;
  for(i=idx; i<pPage->nCell; i++){
    pPage->apCell[i] = pPage->apCell[i+1];
  }
}

/*
** Add a Cell to a database page at the spot indicated by the cursor.
**
** With this routine, we know that the Cell pNewCell will fit into the
** database page that pCur points to.  The calling routine has made
** sure it will fit.  All this routine needs to do is add the Cell
** to the page.  The addToPage() routine should be used for cases
** were it is not known if the new cell will fit.
**
** The new cell is added to the page either before or after the cell
** to which the cursor is pointing.  The new cell is added before
** the cursor cell if pCur->iMatch>0 and the new cell is added after
** the cursor cell if pCur->iMatch<0.  pCur->iMatch should have been set
** by a prior call to sqliteBtreeMoveto() where the key was the key
** of the cell being inserted.  If sqliteBtreeMoveto() ended up on a
** cell that is larger than the key, then pCur->iMatch was set to a
** positive number, hence we insert the new record before the pointer
** if pCur->iMatch is positive.  If sqliteBtreeMaveto() ended up on a
** cell that is smaller than the key then pCur->iMatch was set to a
** negative number, hence we insert the new record after then pointer
** if pCur->iMatch is negative.
*/
static int insertCell(BtCursor *pCur, Cell *pNewCell){
  int sz;
  int idx;
  int i;
  Cell *pCell, *pIdx;
  MemPage *pPage;

  pPage = pCur->pPage;
  sz = cellSize(pNewCell);
  idx = allocateSpace(pPage, sz);
  assert( idx>0 && idx<=SQLITE_PAGE_SIZE - sz );
  pCell = (Cell*)&pPage->aDisk[idx];
  memcpy(pCell, pNewCell, sz);
  pIdx = pPage->aDisk[pCur->idx];
  if( pCur->iMatch<0 ){
    /* Insert the new cell after the cell pCur points to */
    pCell->h.iNext = pIdx->h.iNext;
    pIdx->h.iNext = idx;
    for(i=pPage->nCell-1; i>pCur->idx; i--){
      pPage->apCell[i+1] = pPage->apCell[i];
    }
    pPage->apCell[pCur->idx+1] = pCell;
  }else{
    /* Insert the new cell before the cell pCur points to */
    pCell->h.iNext = pPage->pHdr->firstCell;
    pPage->pHdr->firstCell = idx;
    for(i=pPage->nCell; i>0; i++){
      pPage->apCell[i] = pPage->apCell[i-1];
    }
    pPage->apCell[0] = pCell;
  }
  pPage->nCell++;
  if( pCell->h.leftChild ){
    MemPage *pChild = sqlitepager_lookup(pCur->pBt, pCell->h.leftChild);
    if( pChild && pChild->pParent ){
      sqlitepager_unref(pChild->pParent);
      pChild->pParent = pPage;
      sqlitepager_ref(pChild->pParent);
    }
  }
  return SQLITE_OK;
}

/*
** Insert pNewCell into the database page that pCur is pointing to at
** the place where pCur is pointing.  
**
** This routine works just like insertCell() except that the cell
** to be inserted need not fit on the page.  If the new cell does 
** not fit, then the page sheds data to its siblings to try to get 
** down to a size where the new cell will fit.  If that effort fails,
** then the page is split.
*/
static int addToPage(BtCursor *pCur, Cell *pNewCell){
  Cell tempCell;
  Cell centerCell;

  for(;;){
    MemPage *pPage = pCur->pPage;
    rc = sqlitepager_write(pPage);
    if( rc ) return rc;
    int sz = cellSize(pNewCell);
    if( sz<=pPage->nFree ){
      insertCell(pCur, pNewCell);
      return SQLITE_OK;
    }
    if( pPage->pParent==0 ){
      MemPage *pRight;
      PageHdr *pHdr;
      FreeBlk *pFBlk;
      int pc;
      rc = split(pCur, pNewCell, &centerCell, &pRight);
      if( rc ) return rc;
      pHdr = pPage->pHdr;
      pHdr->right = sqlitepager_pagenumber(pRight);
      sqlitepager_unref(pRight);
      pHdr->firstCell = pc = sizeof(*pHdr);
      sz = cellSize(&centerCell);
      memcpy(&pPage->aDisk[pc], &centerCell, sz);
      pc += sz;
      pHdr->firstFree = pc;
      pFBlk = (FreeBlk*)&pPage->aDisk[pc];
      pFBlk->iSize = SQLITE_PAGE_SIZE - pc;
      pFBlk->iNext = 0;
      memset(&pFBlk[1], 0, pFBlk->iSize-sizeof(*pFBlk));
      return SQLITE_OK;
    }
    if( rotateLeft(pCur, sz - pPage->nFree) 
           || rotateRight(pCur, sz - pPage->nFree) ){
      insertCell(pCur, pNewCell);
      return SQLITE_OK;
    }
    rc = split(pCur, pNewCell, &centerCell, 0);
    if( rc ) return rc;
    moveToParent(pCur);
    tempCell = centerCell;
    pNewPage = &tempCell;
  }
  /* NOT REACHED */
}

/*
** Insert a new record into the BTree.  The key is given by (pKey,nKey)
** and the data is given by (pData,nData).  The cursor is used only to
** define what database the record should be inserted into.  The cursor
** is NOT left pointing at the new record.
*/
int sqliteBtreeInsert(
  BtCursor *pCur,            /* Insert data into the table of this cursor */
  void *pKey,  int nKey,     /* The key of the new record */
  void *pData, int nData     /* The data of the new record */
){
  Cell newCell;
  int rc;
  int loc;
  MemPage *pPage;
  Btree *pBt = pCur->pBt;

  rc = sqliteBtreeMoveTo(pCur, pKey, nKey, &loc);
  if( rc ) return rc;
  rc = sqlitepager_write(pCur->pPage);
  if( rc ) return rc;
  rc = fillInCell(pBt, &newCell, pKey, nKey, pData, nData);
  if( rc ) return rc;
  if( loc==0 ){
    newCell.h.leftChild = pCur->pPage->apCell[pCur->idx]->h.leftChild;
    rc = clearCell(pBt, pCur->pPage->apCell[pCur->idx]);
    if( rc ) return rc;
    unlinkCell(pCur);
  }
  return addToPage(pCur, &newCell);
}

/*
** Check the page at which the cursor points to see if it is less than
** half full.  If it is less than half full, then try to increase
** its fill factor by grabbing cells from siblings or by merging
** the page with siblings.
*/
static int refillPage(BtCursor *pCur){
  MemPage *pPage;
  BtCursor tempCur;
  int rc;
  Pager *pPager;

  pPage = pCur->pPage;
  if( pPage->nFree < SQLITE_PAGE_SIZE/2 ){
    return SQLITE_OK;
  }
  rc = sqlitepager_write(pPage);
  if( rc ) return rc;
  pPager = pCur->pBt->pPager;

  if( pPage->nCell==0 ){
    /* The page being refilled is the root of the BTree and it has
    ** no entries of its own.  If there is a child page, then make the
    ** child become the new root.
    */
    MemPage *pChild;
    Pgno pgnoChild;
    assert( pPage->pParent==0 );
    assert( sqlitepager_pagenumber(pPage)==2 );
    pgnoChild = ((PageHdr*)pPage)->rightChild;
    if( pgnoChild==0 ){
      return SQLITE_OK;
    }
    rc = sqlitepager_get(pPager, pgno, &pChild);
    if( rc ) return rc;
    memcpy(pPage, pChild, SQLITE_PAGE_SIZE);
    memset(&pPage->aDisk[SQLITE_PAGE_SIZE], 0, EXTRA_SIZE);
    freePage(pCur->pBt, pChild, pgnoChild);
    sqlitepager_unref(pChild);
    rc = initPage(pPage, 2, 0);
    reparentChildPages(pPager, pPage);
    return SQLITE_OK;
  }

  /** merge with siblings **/

  /** borrow from siblings **/
}

/*
** Replace the content of the cell that pCur is pointing to with the content
** in pNewContent.  The pCur cell is not unlinked or moved in the Btree,
** its content is just replaced.
**
** If the size of pNewContent is greater than the current size of the
** cursor cell then the page that cursor points to might have to split.
*/
static int ReplaceContentOfCell(BtCursor *pCur, Cell *pNewContent){
  Cell *pCell;       /* The cell whose content will be changed */
  Pgno pgno;         /* Temporary storage for a page number */

  pCell = pCur->pPage->apCell[pCur->idx];
  rc = clearCell(pCur->pBt, pCell);
  if( rc ) return rc;
  pgno = pNewCell->h.leftChild;
  pNewCell->h.leftChild = pCell->h.leftChild;
  unlinkCell(pCur);
  rc = addToPage(pCur, pNewCell);
  pNewCell->h.leftChild = pgno;
  return rc;
}

/*
** Delete the entry that the cursor is pointing to.
**
** The cursor is left pointing at either the next or the previous
** entry.  If the cursor is left pointing to the next entry, then 
** the pCur->bSkipNext flag is set which forces the next call to 
** sqliteBtreeNext() to be a no-op.  That way, you can always call
** sqliteBtreeNext() after a delete and the cursor will be left
** pointing to the first entry after the deleted entry.
*/
int sqliteBtreeDelete(BtCursor *pCur){
  MemPage *pPage = pCur->pPage;
  Cell *pCell;
  int rc;
  if( pCur->idx >= pPage->nCell ){
    return SQLITE_ERROR;  /* The cursor is not pointing to anything */
  }
  rc = sqlitepager_write(pPage);
  if( rc ) return rc;
  pCell = pPage->apCell[pCur->idx];
  if( pPage->pHdr->rightChild ){
    /* The entry to be deleted is not on a leaf page.  Non-leaf entries 
    ** cannot be deleted directly because they have to be present to
    ** hold pointers to subpages.  So what we do is look at the next 
    ** entry in sequence.  The next entry is guaranteed to exist and 
    ** be a leaf.  We copy the payload from the next entry into this
    ** entry, then delete the next entry.
    */
    BtCursor origCur;
    CreateTemporaryCursor(pCur, &origCur);
    rc = sqliteBtreeNext(pCur, 0);
    if( rc==SQLITE_OK ){
      pPage = pCur->pPage;
      pCell = pPage->apCell[pCur->idx];
      rc = ReplaceContentOfCell(&origCur, pCell);
    }
    DestroyTemporaryCursor(&origCur);
    if( rc ) return rc;
  }
  rc = clearCell(pCell);
  if( rc ) return rc;
  unlinkCell(pCur->pBt, pCell);
  if( pCur->idx == 0 ){
    pCur->bSkipNext = 1;
  }else{
    pCur->idx--;
  }
  rc = refillPage(pCur);
  return rc;
}
