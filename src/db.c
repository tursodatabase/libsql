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
** $Id: db.c,v 1.4 2001/01/25 01:45:40 drh Exp $
*/
#include "sqliteInt.h"
#include "pg.h"

/*
** Everything we need to know about an open database
*/
struct Db {
  Pgr *pPgr;            /* The pager for the database */
  DbCursor *pCursor;    /* All open cursors */
  int inTransaction;    /* True if a transaction is in progress */
  int nContents;        /* Number of slots in aContents[] */
  int nAlloc;           /* Space allocated for aContents[] */
  u32 *aContents;       /* Contents table for the database */
};

/*
** The maximum depth of a cursor
*/
#define MX_LEVEL 10

/*
** Within a cursor, each level off the search tree is an instance of
** this structure.
*/
typedef struct DbIdxpt DbIdxpt;
struct DbIdxpt {
  int pgno;         /* The page number */
  u32 *aPage;       /* The page data */
  int idx;          /* Index into pPage[] */
  int hashLB;       /* Lower bound on hash at this level */
  int hashUB;       /* Upper bound on hash at this level */
};

/*
** Everything we need to know about a cursor
*/
struct DbCursor {
  Db *pDb;                      /* The whole database */
  DbCursor *pPrev, *pNext;      /* Linked list of all cursors */
  u32 rootPgno;                 /* Root page of table for this cursor */
  int onEntry;                  /* True if pointing to a table entry */
  int nLevel;                   /* Number of levels of indexing used */
  DbIdxpt aLevel[MX_LEVEL];     /* The index levels */
};

/*
** The first word of every page is some combination of these values
** used to indicate its function.
*/
#define BLOCK_MAGIC            0x24e47190
#define BLOCK_INDEX            0x00000001
#define BLOCK_LEAF             0x00000002
#define BLOCK_FREE             0x00000003
#define BLOCK_OVERFLOW         0x00000004
#define BLOCK_CONTENTS         0x00000005
#define BLOCK_MAGIC_MASK       0xfffffff8
#define BLOCK_TYPE_MASK        0x00000007

/*
** Free blocks:
**
**     0.   BLOCK_MAGIC | BLOCK_FREE
**     1.   address of next block on freelist
**
** Leaf blocks:
**
**     0.   BLOCK_MAGIC | BLOCK_LEAF 
**     1.   number of table entries  (only used if a table root block)
**     entries....
**         0.  size of this entry (measured in u32's)
**         1.  hash
**         2.  keysize  (in bytes)
**         3.  datasize (in bytes)
**         4.  payload
**
** Payload area:
**
**     *   up to LOCAL_PAYLOAD bytes of data
**     *   10 page number of direct blocks
**     *   1 indirect block
**     *   1 double-indirect block
**
** Index block:
**
**     0.   BLOCK_MAGIC | BLOCK_INDEX
**     1.   number of table entries  (only used if a table root block)
**     2.   entries in this index block
**     entries...
**         0.  largest hash value for pgno
**         1.  pgno of subblock
**
** Contents block:  (The first page in the file)
**     0.   BLOCK_MAGIC | BLOCK_CONTENTS
**     1.   zero
**     2.   number of bytes of payload
**     3.   freelist
**     4... root pages numbers of tables
*/

/*
** The number of u32-sized objects that will fit on one page.
*/
#define U32_PER_PAGE  (SQLITE_PAGE_SIZE/sizeof(u32))

/*
** Number of direct overflow pages per database entry
*/
#define N_DIRECT  10

/*
** The maximum amount of payload that will fit on on the same
** page as a leaf, assuming the leaf contains only a single
** database entry and the entry uses no overflow pages.
*/
#define LOCAL_PAYLOAD  (SQLITE_PAGE_SIZE - (8+N_DIRECT)*sizeof(u32))

/*
** Allocate a new page.  Return both the page number and a pointer
** to the page data.  The calling function is responsible for unref-ing
** the page when it is no longer needed.
*/
int allocPage(Db *pDb, u32 *pPgno, u32 **ppPage){
  u32 pgno;
  int rc;

  if( pDb->aContent==0 ) return SQLITE_NOMEM;

  /* Try to reuse a page from the freelist
  */
  pgno = pDb->aContent[0];
  if( pgno!=0 ){
    rc = sqlitePgGet(pDb->pPgr, pgno, (void**)ppPage);
    if( rc==SQLITE_OK ){
      pDb->aContent[0] = pFree[1];
      *pPgno = pgno;
      memset(*ppPage, 0, SQLITE_PAGE_SIZE);
      return SQLITE_OK;
    }
  }

  /* If the freelist is empty, or we cannot access it,
  ** then allocate a new page from the end of the file.
  */
  if( (rc = sqlitePgCount(pDb->pPgr, &pgno))==SQLITE_OK &&
      (rc = sqlitePgGet(pDb->pPgr, pgno, (void**)ppPage))==SQLITE_OK ){
    *pPgno = pgno;
    memset(*ppPage, 0, SQLITE_PAGE_SIZE);
    return SQLITE_OK;
  }
  return rc;
}

/*
** Return a page to the freelist and dereference the page.
*/
static void freePage(DB *pDb, u32 pgno, u32 *aPage){
  if( pDb->aContent==0 ) return;
  if( pgno==0 ) return
  if( aPage==0 ){
    int rc;
    rc = sqlitePgGet(pDb->pPgr, pgno, &aPage);
    if( rc!=SQLITE_OK ) return;
  }
  aPage[0] = BLOCK_MAGIC | BLOCK_FREE;
  aPage[1] = pDb->aContent[0];
  memset(&aPage[2], 0, SQLITE_PAGE_SIZE - 2*sizeof(u32));
  pDb->aContent[0] = pgno;
  sqlitePgTouch(aPage);
  sqlitePgUnref(aPage);
}

/*
** Return the number of bytes of payload storage required on the leaf
** node to hold the amount of payload specified by the argument.
** Overflow pages do not count, only memory on the leaf page.
**
** Return -1 if nTotal is more than sqlite is able to store.
*/
static int payloadLocalSize(int nTotal){
  int nLocal, i;
  if( nTotal<0 ) nTotal = 0;
  if( nTotal <= LOCAL_PAYLOAD ){
    /* All the data fits on the leaf page */
    return (nTotal + 3)/4;
  }
  nLocal = LOCAL_PAYLOAD;
  nTotal -= LOCAL_PAYLOAD;
  if( nTotal < 10*SQLITE_PAGE_SIZE ){
    return nLocal + ((nTotal+SQLITE_PAGE_SIZE-1)/SQLITE_PAGE_SIZE)*sizeof(u32);
  }
  nLocal += N_DIRECT*sizeof(u32);
  nTotal -= N_DIRECT*SQLITE_PAGE_SIZE;
  if( nTotal < U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    return nLocal + sizeof(u32);
  }
  nLocal += sizeof(u32);
  nTotal -= U32_PER_PAGE*SQLITE_PAGE_SIZE;
  if( nTotal < U32_PER_PAGE*U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    return nLocal + sizeof(u32);
  }
  return -1;  /* This payload will not fit. */
}

/*
** Read data from the payload area.
**
** aPage points directly at the beginning of the payload.  No bounds 
** checking is done on offset or amt -- it is assumed that the payload
** area is big enough to accomodate.
*/
static int payloadRead(Db *pDb, u32 *aPage, int offset, int amt, void *pBuf){
  int rc;
  int tomove;
  int i;

  /* First read local data off of the leaf page itself.
  ** This is all that ever happens in 99% of accesses.
  */
  assert( offset>=0 && amt>=0 );
  if( offset < LOCAL_PAYLOAD ){
    /* Data stored directly in the leaf block of the BTree */
    if( amt+offset>LOCAL_PAYLOAD ){
      tomove = LOCAL_PAYLOAD - offset;
    }else{
      tomove = amt;
    }
    memcpy(pBuf, &((char*)aPage)[offset], tomove);
    pBuf = &((char*)pBuf)[tomove];
    offset += tomove;
    amt -= tomove;
    if( amt<=0 ) return SQLITE_OK;
  }
  offset -= LOCAL_PAYLOAD;
  aPage += LOCAL_PAYLOAD/sizeof(aPage[0]);

  /* If not all of the data fits locally, read from the first
  ** ten direct-access overflow pages.
  */
  if( offset < N_DIRECT*SQLITE_PAGE_SIZE ){
    for(i=offset/SQLITE_PAGE_SIZE; i<N_DIRECT && amt>0; i++){
      char *aData;
      base = offset - i*SQLITE_PAGE_SIZE;
      rc = sqlitePgGet(pDb->pPgr, aPage[i], &aData);
      if( rc!=SQLITE_OK ) return rc;
      if( amt+base > SQLITE_PAGE_SIZE ){
        tomove = SQLITE_PAGE_SIZE - base;
      }else{
        tomove = amt;
      }
      memcpy(pBuf, &aData[base], tomove);
      sqlitePgUnref(aData);
      pBuf = &((char*)pBuf)[tomove];
      amt -= tomove;
    }
  }
  offset -= N_DIRECT*SQLITE_PAGE_SIZE;
  aPage += N_DIRECT;

  /* If the first N_DIRECT overflow pages do not contain everything, then
  ** read from an overflow page that is filled with pointer to
  ** U32_PER_PAGE more overflow pages.
  */
  if( offset < U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    u32 *indirPage;
    rc = sqlitePgGet(pDb->pPgr, aPage[0], &indirPage);
    if( rc!=SQLITE_OK ) return rc;
    for(i=offset/SQLITE_PAGE_SIZE; i<U32_PER_PAGE && amt>0; i++){
      int base;
      char *aData;
      base = offset - i*SQLITE_PAGE_SIZE;
      rc = sqlitePgGet(pDb->pPgr, indirPage[idx], &aData);
      if( rc!=SQLITE_OK ) break;
      if( amt+base > SQLITE_PAGE_SIZE ){
        tomove = SQLITE_PAGE_SIZE - base;
      }else{
        tomove = amt;
      }
      memcpy(pBuf, &aData[base], tomove);
      sqlitePgUnref(aData);
      pBuf = &((char*)pBuf)[tomove];
      amt -= tomove;
    }
    sqlitePgUnref(indirPage);
    if( rc!=SQLITE_OK ) return rc;
    if( amt<=0 ) return SQLITE_OK;
  }
  offset -= U32_PER_PAGE*SQLITE_PAGE_SIZE;
  aPage++;

  /* If there is still more data, then read using a double-indirect
  ** overflow.  The overflow page points to U32_PER_PAGE additional
  ** overflow pages, each of which pointer to U32_PER_PAGE more overflow
  ** pages which contain data.
  **
  ** This is hard to test.  To exercise this code, you have to make
  ** a database entry of more than 273336 bytes in side, assuming a
  ** pagesize of 1024 bytes and 10 direct overflow pages.  By the 
  ** time this code runs, you have already used 267 overflow pages.
  */
  if( offset < U32_PER_PAGE*U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    u32 *dblIndirPage;
    rc = sqlitePgGet(pDb->pPgr, aPage[0], &dblIndirPage);
    if( rc!=SQLITE_OK ) return rc;
    i = offset/(U32_PER_PAGE*SQLITE_PAGE_SIZE);
    for(; i<U32_PER_PAGE && amt>0; i++){
      u32 *indirPage;
      int basis;
      int j;
      rc = sqlitePgGet(pDb->pPgr, dblIndirPage[i], &indirPage);
      if( rc!=SQLITE_OK ) break;
      basis = i*U32_PER_PAGE*SQLITE_PAGE_SIZE;
      j = (offset - basis)/SQLITE_PAGE_SIZE;
      for(; j<U32_PER_PAGE && amt>0; j++){
        char *aData;
        base = (offset - basis) - ij*SQLITE_PAGE_SIZE;
        rc = sqlitePgGet(pDb->pPgr, indirPage[j], &aData);
        if( rc!=SQLITE_OK ) break;
        if( amt+base > SQLITE_PAGE_SIZE ){
          tomove = SQLITE_PAGE_SIZE - base;
        }else{
          tomove = amt;
        }
        memcpy(pBuf, &aData[base], tomove);
        sqlitePgUnref(aData);
        pBuf = &((char*)pBuf)[tomove];
        amt -= tomove;
      }
      sqlitePgUnref(indirPage);
      if( rc!=SQLITE_OK ) break;
    }
    sqlitePgUnref(dblIndirPage);
  }

  /* Anything beyond the double-indirect pages, just fill in with
  ** zeros.  You have to write 67382200 bytes to go past the
  ** double-indirect pages, assuming a 1024 byte page size.
  */
  if( amt>0 ) memset(pBuf, 0, amt);
  return SQLITE_OK;
}

/*
** Write data into the payload area.
**
** If pages have already been allocated for the payload, they are
** simply overwritten.  New pages are allocated as necessary to
** fill in gaps.  sqlitePgTouch() is called on all overflow pages,
** but the calling function must invoke sqlitePgTouch() for aPage
** itself.
*/
static int payloadWrite(Db *pDb, u32 *aPage, int offset, int amt, void *pBuf){
  assert( offset>=0 && amt>=0 );

  /* Local data
  */
  if( offset < LOCAL_PAYLOAD ){
    if( amt+offset>LOCAL_PAYLOAD ){
      tomove = LOCAL_PAYLOAD - offset;
    }else{
      tomove = amt;
    }
    memcpy(&((char*)aPage)[offset], pBuf, tomove);
    pBuf = &((char*)pBuf)[tomove];
    offset += tomove;
    amt -= tomove;
    if( amt<=0 ) return SQLITE_OK;
  }
  offset -= LOCAL_PAYLOAD;
  aPage += LOCAL_PAYLOAD/sizeof(aPage[0]);

  /* Direct overflow pages
  */
  if( offset < N_DIRECT*SQLITE_PAGE_SIZE ){
    for(i=offset/SQLITE_PAGE_SIZE; i<N_DIRECT && amt>0; i++){
      base = offset - i*SQLITE_PAGE_SIZE;
      if( aPage[i] ){
        rc = sqlitePgGet(pDb->pPgr, aPage[i], &aData);
      }else{
        rc = allocPage(pDb, &aPage[i], &aData);
      }
      if( rc!=SQLITE_OK ) return rc;
      if( amt+base > SQLITE_PAGE_SIZE ){
        tomove = SQLITE_PAGE_SIZE - base;
      }else{
        tomove = amt;
      }
      memcpy(&aData[base], pBuf, tomove);
      sqlitePgTouch(aData);
      sqlitePgUnref(aData);
      pBuf = &((char*)pBuf)[tomove];
      amt -= tomove;
    }
    if( amt<=0 ) return SQLITE_OK;
  }
  offset -= N_DIRECT*SQLITE_PAGE_SIZE;
  aPage += N_DIRECT;

  /* Indirect overflow pages
  */
  if( offset < U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    u32 *indirPage;
    if( aPage[0] ){
      rc = sqlitePgGet(pDb->pPgr, aPage[0], &indirPage);
    }else{
      rc = allocPage(pDb, &aPage[0], &indirPage);
    }
    if( rc!=SQLITE_OK ) return rc;
    for(i=offset/SQLITE_PAGE_SIZE; i<U32_PER_PAGE && amt>0; i++){
      int base;
      char *aData;
      base = offset - i*SQLITE_PAGE_SIZE;
      if( indirPage[i] ){
        rc = sqlitePgGet(pDb->pPgr, indirPage[i], &aData);
      }else{
        rc = allocPage(pDb, &indirPage[i], &aData);
        sqlitePgTouch(indirPage);
      }
      if( rc!=SQLITE_OK ) break;
      if( amt+base > SQLITE_PAGE_SIZE ){
        tomove = SQLITE_PAGE_SIZE - base;
      }else{
        tomove = amt;
      }
      memcpy(&aData[base], pBuf, tomove);
      sqlitePgUnref(aData);
      pBuf = &((char*)pBuf)[tomove];
      amt -= tomove;
    }
    sqlitePgUnref(indirPage);
    if( rc!=SQLITE_OK ) return rc;
    if( amt<=0 ) return SQLITE_OK;
  }
  offset -= U32_PER_PAGE*SQLITE_PAGE_SIZE;
  aPage++;

  /* Double-indirect overflow pages
  */
  if( offset < U32_PER_PAGE*U32_PER_PAGE*SQLITE_PAGE_SIZE ){
    u32 *dblIndirPage;
    if( aPage[0] ){
      rc = sqlitePgGet(pDb->pPgr, aPage[0], &dblIndirPage);
    }else{
      rc = allocPage(pDb, &aPage[0], &dblIndirPage);
    }
    if( rc!=SQLITE_OK ) return rc;
    i = offset/(U32_PER_PAGE*SQLITE_PAGE_SIZE);
    for(; i<U32_PER_PAGE && amt>0; i++){
      u32 *indirPage;
      int basis;
      int j;
      if( aPage[0] ){
        rc = sqlitePgGet(pDb->pPgr, aPage[0], &dblIndirPage);
      }else{
        rc = allocPage(pDb, &aPage[0], &dblIndirPage);
        sqlitePgTouch(dblIndirPage);
      }
      rc = sqlitePgGet(pDb->pPgr, dblIndirPage[i], &indirPage);
      if( rc!=SQLITE_OK ) break;
      basis = i*U32_PER_PAGE*SQLITE_PAGE_SIZE;
      j = (offset - basis)/SQLITE_PAGE_SIZE;
      for(; j<U32_PER_PAGE && amt>0; j++){
        char *aData;
        base = (offset - basis) - ij*SQLITE_PAGE_SIZE;
        if( indirPage[j] ){
          rc = sqlitePgGet(pDb->pPgr, indirPage[j], &aData);
        }else{
          rc = allocPage(pDb, &indirPage[j], &aData);
          sqlitePgTouch(indirPage);
        }
        if( rc!=SQLITE_OK ) break;
        if( amt+base > SQLITE_PAGE_SIZE ){
          tomove = SQLITE_PAGE_SIZE - base;
        }else{
          tomove = amt;
        }
        memcpy(&aData[base], pBuf, tomove);
        sqlitePgTouch(aData);
        sqlitePgUnref(aData);
        pBuf = &((char*)pBuf)[tomove];
        amt -= tomove;
      }
      sqlitePgUnref(indirPage);
      if( rc!=SQLITE_OK ) break;
    }
    sqlitePgUnref(dblIndirPage);
  }

  return SQLITE_OK;
}

/*
** Release any and all overflow pages associated with data starting
** with byte "newSize".  oldSize is the amount of payload before doing
** the free operation.
*/
static int payloadFree(Db *pDb, u32 *aPage, int newSize, int oldSize){
  int i, j;          /* Loop counters */
  int first, last;   /* Indices of first and last pages to be freed */
  int rc;            /* Return code from sqlitePgGet() */

  /* Skip over the local data.  We do not need to free it.
  */
  if( newSize>=oldSize ) return SQLITE_OK;
  oldSize -= LOCAL_PAYLOAD;
  if( oldSize<=0 ) return SQLITE_OK;
  newSize -= LOCAL_PAYLOAD;
  aPage += LOCAL_PAYLOAD/sizeof(u32);

  /* Compute the indices of the first and last overflow pages to
  ** be freed.
  */
  first = (newSize - 1)/SQLITE_PAGE_SIZE + 1;
  last = (oldSize - 1)/SQLITE_PAGE_SIZE;

  /* Free the direct overflow pages
  */
  if( first < N_DIRECT ){
    for(i=first; i<N_DIRECT && i<=last; i++){
      freePage(pDb, aPage[i], 0);
      aPage[i] = 0;
    }
  }
  aPage += N_DIRECT;
  first -= N_DIRECT;
  last -= N_DIRECT;
  if( last<0 ) return SQLITE_OK;
  if( first<0 ) first = 0;
  
  /* Free indirect overflow pages
  */
  if( first < U32_PER_PAGE ){
    u32 *indirPage;
    rc = sqlitePgGet(pDb->pPgr, aPage[0], &indirPage);
    if( rc!=SQLITE_OK ) return rc;
    for(i=first; i<U32_PER_PAGE && i<=last; i++){
      freePage(pDb, indirPage[i], 0);
      indirPage[i] = 0;
      touch = 1;
    }
    if( first<=0 ){
      freepage(pDb, aPage[0], indirPage);
      aPage[0] = 0;
    }else{
      sqlitePgTouch(indirPage);
      sqlitePgUnref(indirPage);
    }
  }
  aPage++;
  first -= U32_PER_PAGE;
  last -= U32_PER_PAGE;
  if( last<0 ) return SQLITE_OK;
  if( first<0 ) first = 0;

  /* Free double-indirect overflow pages
  */
  if( first < U32_PER_PAGE*U32_PER_PAGE ){
    u32 *dblIndirPage;
    rc = sqlitePgGet(pDb->pPgr, aPage[0], &dblIndirPage);
    if( rc!=SQLITE_OK ) return rc;
    for(i=first/U32_PER_PAGE; i<U32_PER_PAGE; i++){
      u32 *indirPage;
      basis = i*U32_PER_PAGE;
      if( last < basis ) break;
      rc = sqlitePgGet(pDb->pPgr, dblIndirPage[i], &indirPage);
      if( rc!=SQLITE_OK ) return rc;
      for(j=first>basis?first-basis:0 ; j<U32_PER_PAGE; j++){
        if( j + basis > last ) break;
        freePage(pDb, indirPage[j], 0);
        indirPage[j] = 0;
      }
      if( first<=basis ){
        freepage(pDb, dblIndirPage[i], 0);
        dblIndirPage[i] = 0;
      }else{
        sqlitePgTouch(indirPage);
        sqlitePgUnref(indirPage);
      }
    }
    if( first<=0 ){
      freepage(pDb, aPage[0], dblIndirPage);
      aPage[0] = 0;
    }else{
      sqlitePgTouch(dblIndirPage);
      sqlitePgUnref(dblIndirPage);
    }
  }

  return SQLITE_OK;    
}

/*
** Allocate space for the content table in the given Db structure.
** return SQLITE_OK on success and SQLITE_NOMEM if it fails.
*/
static int sqliteDbExpandContent(Db *pDb, int newSize){
  if( pDb->nAlloc>=newSize ) return SQLITE_OK;
  pDb->nAlloc = newSize;
  pDb->aContent = sqliteRealloc( pDb->aContent, pDb->nAlloc*sizeof(u32));
  if( pDb->aContent==0 ){
    pDb->nContent = 0;
    pDb->nAlloc = 0;
    pDb->inTranaction = 0;
    return SQLITE_NOMEM;
  }
  return SQLITE_OK;
}

/*
** Open a database.
*/
int sqliteDbOpen(const char *filename, Db **ppDb){
  Db *pDb = 0;
  Pgr *pPgr = 0;
  u32 *aPage1;
  int rc;
  u32 nPage;

  rc = sqlitePgOpen(filename, &pPgr);
  if( rc!=SQLITE_OK ) goto open_err;
  pDb = sqliteMalloc( sizeof(*pDb) );
  if( pDb==0 ){
    rc = SQLITE_NOMEM;
    goto open_err;
  }
  pDb->pPgr = pPgr;
  pDb->pCursor = 0;
  pDb->inTransaction = 0;
  sqlitePgCount(pDb->pPgr, &nPage);
  rc = sqlitePgGet(pDb->pPgr, 1, &aPage1);
  if( rc!=0 ) goto open_err;
  if( nPage==0 ){
    sqlitePgBeginTransaction(pDb->pPgr);
    aPage1[0] = BLOCK_MAGIC|BLOCK_CONTENT;
    aPage1[2] = sizeof(u32)*10;
    sqlitePgTouch(aPage1);
    sqlitePgCommit(pDb->pPgr);
  }
  pDb->nContent = aPage1[2]/sizeof(u32);
  pDb->nAlloc = 0;
  rc = sqliteDbExpandContent(pDb, pDb->nContent);
  if( rc!=SQLITE_OK ) goto open_err;
  rc = payloadRead(pDb, &aPage1[3], 0, aPage[2], pDb->aContent);
  sqlitePgUnref(aPage1);
  if( rc!=SQLITE_OK ) goto open_err;
  *ppDb = pDb;
  return SQLITE_OK;

open_err:
  *ppDb = 0;
  if( pPgr ) sqlitePgClose(pPgr);
  if( pDb && pDb->aContent ) sqliteFree(pDb->aContent);
  if( pDb ) sqliteFree(pDb);
  return rc;
}

/*
** Close a database
*/
int sqliteDbClose(Db *pDb){
  while( pDb->pCursor ){
    sqliteDbCursorClose(pDb->pCursor);
  }
  sqlitePgClose(pDb->pPgr);
  sqliteFree(pDb->aContent);
  sqliteFree(pDb);
  return SQLITE_OK;
}

/*
** Begin a transaction
*/
int sqliteDbBeginTransaction(Db *pDb){
  int rc;
  if( pDb->aContent==0 ){
    return SQLITE_NOMEM;
  }
  if( pDb->inTransaction ){
    return SQLITE_INTERNAL;
  }
  rc = sqlitePgBeginTransaction(pDb->pPgr);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pDb->inTransaction = 1;
  return SQLITE_OK;
}

/*
** Commit changes to the database
*/ 
int sqliteDbCommit(Db *pDb){
  u32 *aPage1;
  int rc;
  if( !pDb->inTransaction ){
    return SQLITE_OK;
  }
  rc = sqlitePgGet(pDb->pPgr, 1, &aPage1);
  if( rc!=SQLITE_OK ) return rc;
  aPage1[2] = pDb->nContent*sizeof(u32);
  payloadWrite(pDb, 0, aPage1[2], pDb->aContent);
  sqlitePgUnref(aPage1);
  rc = sqlitePgCommit(pDb->pPgr);
  if( rc!=SQLITE_OK ) return rc;
  pDb->inTransaction = 0;
  return SQLITE_OK;
}

/*
** Rollback the database to its state prior to the beginning of
** the transaction
*/
int sqliteDbRollback(Db *pDb){
  u32 *aPage1;
  if( !pDb->inTransaction ) return SQLITE_OK;
  rc = sqlitePgRollback(pDb->pPgr);
  if( rc!=SQLITE_OK ) return rc;
  rc = sqlitePgGet(pDb->pPgr, 1, &aPage1);
  if( rc!=SQLITE_OK ) return rc;
  pDb->nContent = SWB(aPage1[3]) + 2;
  if( sqliteDbExpandContent(pDb, pDb->nContent)!=SQLITE_OK ){
    return SQLITE_NOMEM;
  }
  sqliteDbReadOvfl(pDb, 1, aPage1, 0, pDb->nContent*sizeof(u32), pDb->aContent);
  pDb->inTransaction = 0;
  return SQLITE_OK;
}

/*
** Create a new table in the database.  Write the table number
** that is used to open a cursor into that table into *pTblno.
*/
int sqliteDbCreateTable(Db *pDb, int *pTblno){
  u32 *pPage;
  u32 pgno;
  int rc;
  int swTblno;
  int i;

  rc = allocPage(pDb, &pgno, &pPage);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  tblno = -1;
  for(i=2; i<pDb->nContent; i++){
    if( pDb->aContent[i]==0 ){
      tblno = i - 2;
      break;
    }
  }
  if( tblno<0 ){
    tblno = SWB(pDb->aContent[1]);
  }
  if( tblno+2 >= pDb->nContent ){
    sqliteDbExpandContent(pDb, tblno+2);
  }
  if( pDb->aContent==0 ){
    return SQLITE_NOMEM;
  }
  pDb->aContent[tblno+2] = SWB(pgno);
  pPage[0] = SWB(BLOCK_MAGIC | BLOCK_LEAF);
  memset(&pPage[1], 0, SQLITE_PAGE_SIZE - sizeof(u32));
  sqlitePgTouch(pPage);
  sqlitePgUnref(pPage);
  return SQLITE_OK;
}

/* forward reference */
static int sqliteDbClearEntry(Db *pDb, u32 *pEntry);

/*
** Recursively add a page to the free list
*/
static int sqliteDbDropPage(Db *pDb, u32 pgno){
  u32 *aPage;
  int rc;

  rc = sqlitePgGet(pDb->pPgr, pgno, (void**)&aPage);
  if( rc!=SQLITE_OK ) return rc;
  switch(  aPage[0] ){
    case BLOCK_MAGIC | BLOCK_INDEX: {
      int n, i;
      n = aPage[2];
      for(i=0; i<n; i++){
        u32 subpgno = aPage[3+i*2];
        sqliteDbDropPage(pDb, subpgno);
      }
      freePage(pDb, pgno, aPage);
      break;
    }
    case BLOCK_MAGIC | BLOCK_LEAF: {
      int i = 1;
      while( i<SQLITE_PAGE_SIZE/sizeof(u32) ){
        int entrySize = SWB(aPage[i]);
        if( entrySize==0 ) break;
        sqliteDbClearEntry(pDb, &aPage[i]);
        i += entrySize;
      }
      freePage(pDb, pgno, aPage);
      break;
    }
    default: {
      /* Do nothing */
      break;
    }
  }
}

/*
** aEntry points directly at a database entry on a leaf page.
** Free any overflow pages associated with the key or data of
** this entry.
*/
static int sqliteDbClearEntry(Db *pDb, u32 *aEntry){
  return SQLITE_OK;
}

/*
** Delete the current associate of a cursor and release all the
** pages it holds.  Except, do not release pages at levels less
** than N.
*/
static void sqliteDbResetCursor(DbCursor *pCur, int N){
  int i;
  for(i=pCur->nLevel-1; i>=N; i--){
    sqlitePgUnref(pCur->aLevel[i].aPage);
  }
  pCur->nLevel = N;
  pCur->onEntry = 0;
}

/*
** Delete an entire table.
*/
static int sqliteDbDropTable(Db *pDb, int tblno){
  DbCursor *pCur;
  u32 pgno;

  /* Find the root page for the table to be dropped.
  */
  if( pDb->aContent==0 ){
    return SQLITE_NOMEM;
  }
  if( tblno<0 || tblno+2>=pDb->nContent || pDb->aContent[tblno+2]==0 ){
    return SQLITE_NOTFOUND;
  }
  pgno = SWB(pDb->aContent[tblno+2]);

  /* Reset any cursors point to the table that is about to
  ** be dropped */
  for(pCur=pDb->pCursor; pCur; pCur=pCur->pNext){
    if( pCur->rootPgno==pgno ){
      sqliteDbResetCursor(pCur, 0);
    }
  }

  /* Move all pages associated with this table to the freelist
  */
  sqliteDbDropPage(pDb, pgno);
  return SQLITE_OK;
}

/*
** Create a new cursor
*/
int sqliteDbCursorOpen(Db *pDb, int tblno, DbCursor **ppCur){
  u32 pgno;
  DbCursor *pCur;

  /* Translate the table number into a page number
  */
  if( pDb->aContent==0 ){
    *ppCur = 0;
    return SQLITE_NOMEM;
  }
  if( tblno<0 || tblno+2>=pDb->nContent || pDb->aContent[tblno+2]==0 ){
    *ppCur = 0;
    return SQLITE_NOTFOUND;
  }
  pgno = SWB(pDb->aContent[tblno+2]);
  
  /* Allocate the cursor
  */
  pCur = sqliteMalloc( sizeof(*pCur) );
  pCur->pgno = pgno;
  pCur->pDb = pDb;
  pCur->pNext = pDb->pCursor;
  pCur->pPrev = 0;
  if( pDb->pCursor ){
     pDb->pCursor->pPrev = pCur;
  }
  pDb->pCursor = pCur;
  *ppCur = pCur;
  return SQLITE_OK;
}

/*
** Delete a cursor
*/
int sqliteDbCursorClose(DbCursor *pCur){
  int i;
  if( pCur->pPrev ){
    pCur->pPrev->pNext = pCur->pNext;
  }else if( pCur->pDb->pCursor==pCur ){
    pCur->pDb->pCursor = pCur->pNext;
  }
  if( pCur->pNext ){
    pCur->pNext->pPrev = pCur->pPrev;
  }
  sqliteDbResetCursor(pCur, 0);
  sqliteFree(pCur);
  return SQLITE_OK; 
}

/*
** Beginning at index level "i" (the outer most index is 0), move down 
** to the first entry of the table.  Levels above i (less than i) are 
** unchanged.
*/
static int sqliteDbGotoFirst(DbCursor *pCur, int i){
  int rc = -1;

  assert( i>=0 && i<MAX_LEVEL );
  if( pCur->nLevel > i+1 ){
    sqliteDbResetCursor(pCur, i+1);
  }
  assert( pCur->nLevel==i+1 );
  while( rc < 0 ){
    u32 *aPage = pCur->aLevel[i].aPage;
    assert( aPage!=0 );
    switch( SWB(aPage[0]) ){
      case BLOCK_LEAF | BLOCK_MAGIC: {
        if( aPage[1]!=0 ){
          pCur->aLevel[i].idx = 1;
          pCur->onEntry = 1;
        }else{
          sqliteDbResetCursor(pCur, 1);
        }
        rc = SQLITE_OK;
        break;
      }
      case BLOCK_INDEX | BLOCK_MAGIC: {
        int n = SWB(aPage[2]);
        if( n<2 || n>=((SQLITE_PAGE_SIZE/sizeof(u32))-3)/2 ){
          sqliteDbResetCur(pCur, 1);
          rc = SQLITE_CORRUPT;
          break;
        }
        pCur->nLevel++;
        i++;
        pCur->aLevel[i].pgno = SWB(aPage[4]);
        rc = sqlitePgGet(pCur->pDb->pPgr, pCur->aLevel[i].pgno,
                    &pCur->aLevel[i].aPage);
        if( rc != SQLITE_OK ){
          sqliteDbResetCursor(pCur, 1);
        }else{
          rc = -1;
        }
        break;
      }
      default: {
        sqliteDbResetCursor(pCur, 1);
        rc = SQLITE_CORRUPT;
      }
    }
  }
  return rc;
}

/*
** Move the cursor to the first entry in the table.
*/
int sqliteDbCursorFirst(DbCursor *pCur){
  if( pCur->nLevel==0 ){
    int rc;
    pCur->aLevel[0].pgno = pCur->rootPgno;
    rc = sqlitePgGet(pCur->pDb->pPgr, pCur->rootPgno, pCur->aLevel[0].aPage);
    if( rc!=SQLITE_OK ){
      sqliteDbResetCursor(pCur, 0);
      return rc;
    }
    pCur->nLevel = 1;
  }
  return sqliteDbGotoFirst(pCur, 0);
}

/*
** Advance the cursor to the next entry in the table.
*/
int sqliteDbCursorNext(DbCursor *pCur){
  int i, idx, n, rc;
  u32 pgno, *aPage;
  if( !pCur->onEntry ){
     return sqliteDbCursorFirst(pCur);
  }
  i = pCur->nLevel-1;
  aPage = pCur->aLevel[i].aPage;
  idx = pCur->aLevel[i].idx;
  idx += SWB(aPage[idx]);
  if( idx >= SQLITE_PAGE_SIZE/sizeof(u32) ){
    sqliteDbResetCursor(pCur, 1);
    return SQLITE_CORRUPT;
  }
  if( aPage[idx]!=0 ){
    pCur->aLabel[i].idx = idx;
    return SQLITE_OK;
  }
  rc = SQLITE_OK;
  while( pCur->nLevel>1 ){
    pCur->nLevel--;
    i = pCur->nLevel-1;
    sqlitePgUnref(pCur->aLevel[pCur->nLevel].aPage);
    aPage = pCur->aLevel[i].aPage;
    idx = pCur->aLevel[i].idx;
    assert( SWB(aPage[0])==BLOCK_MAGIC|BLOCK_INDEX );
    n = SWB(aPage[2]);
    idx += 2;
    if( (idx-3)/2 < n ){
      pCur->aLevel[i].idx = idx;
      pCur->nLevel++;
      i++;
      pgno = pCur->aLevel[i].pgno = SWB(aPage[idx+1]);
      rc = sqlitePgGet(pDb->pPgr, pgno, &pCur->aLevel[i].aPage);
      if( rc!=SQLITE_OK ) break;
      rc = sqliteDbGotoFirst(pCur, i);
      break;
    }
  }
  sqliteDbResetCursor(pCur, 0);
  return SQLITE_OK;
}

/*
** Return the amount of data on the entry that the cursor points
** to.
*/
int sqliteDbCursorDatasize(DbCursor *pCur){
  u32 *aPage;
  int idx, i;
  if( !pCur->onEntry ) return 0;
  i = pCur->nLevel-1;
  idx = pCur->aLevel[i].idx;
  aPage = pCur->aLevel[i].aPage;
  assert( aPage );
  assert( idx>=2 && idx+4<(SQLITE_PAGE_SIZE/sizeof(u32))
  return SWB(aPage[idx+3]) & 0x80000000;
}

/*
** Return the number of bytes of key on the entry that the cursor points
** to.
*/
int sqliteDbCursorKeysize(DbCursor *pCur){
  u32 *aPage;
  int idx, i;
  if( !pCur->onEntry ) return 0;
  i = pCur->nLevel-1;
  idx = pCur->aLevel[i].idx;
  aPage = pCur->aLevel[i].aPage;
  assert( aPage );
  assert( idx>=2 && idx+4<(SQLITE_PAGE_SIZE/sizeof(u32))
  return SWB(aPage[idx+2]) & 0x80000000;
}

/*
** Read data from the cursor.
*/
int sqliteDbCursorRead(DbCursor *pCur, int amt, int offset, void *buf){
  u32 *aPage;
  int idx, i, dstart;
  int nData;
  int nKey;
  char *cbuf = buf;
  char *cfrom;
  if( !pCur->onEntry ){
    memset(cbuf, 0, amt);
    return SQLITE_OK;
  }
  if( amt<=0 || offset<0 ){
    return SQLITE_ERR;
  }
  i = pCur->nLevel-1;
  idx = pCur->aLevel[i].idx;
  aPage = pCur->aLevel[i].aPage;
  assert( aPage );
  assert( idx>=2 && idx+4<(SQLITE_PAGE_SIZE/sizeof(u32))
  nData = SWB(aPage[idx+3]);
  nKey = SWB(aPage[idx+2]);
  dstart = idx + 4;
  if( nKey!=4 ) dstart++;
  if( nData & 0x80000000 ){
    return sqliteDbReadOvfl(pCur->pDb, SWB(aPage[dstart]), 0, amt, offset, buf);
  }
  cfrom = (char*)&aPage[dstart];
  cfrom += offset;
  nData -= offset;
  if( nData<0 ) nData = 0;
  if( amt>nData ){
    memset(&cbuf[nData], 0, amt-nData);
  }
  if( amt<nData ){
    nData = amt;
  }
  memcpy(cbuf, cfrom, nData);
}

/*
** Read the current key from the cursor.
*/
int sqliteDbCursorReadKey(DbCursor *pCur, int amt, int offset, void *buf){
  u32 *aPage;
  int idx, i, kstart;
  int nData;
  int nKey;
  char *cbuf = buf;
  char *cfrom;
  if( !pCur->onEntry ){
    memset(cbuf, 0, amt);
    return SQLITE_OK;
  }
  if( amt<=0 || offset<0 ){
    return SQLITE_ERR;
  }
  i = pCur->nLevel-1;
  idx = pCur->aLevel[i].idx;
  aPage = pCur->aLevel[i].aPage;
  assert( aPage );
  assert( idx>=2 && idx+4<(SQLITE_PAGE_SIZE/sizeof(u32))
  nKey = SWB(aPage[idx+2]);
  if( nKey & 0x80000000 ){
    return sqliteDbReadOvfl(pCur->pDb, SWB(aPage[idx+4]), 0, amt, offset, buf);
  }
  if( nKey==4 ){
    kstart = idx + 1;
  }else{
    kstart = idx + 4;
  }
  cfrom = (char*)&aPage[kstart];
  cfrom += offset;
  nKey -= offset;
  if( nKey<0 ) nKey = 0;
  if( amt>nKey ){
    memset(&cbuf[nKey], 0, amt-nKey);
  }
  if( amt<nKey ){
    nData = amt;
  }
  memcpy(cbuf, cfrom, nKey);
}

/*
** Generate a 32-bit hash from the given key.
*/
static u32 sqliteDbHash(int nKey, void *pKey){
  u32 h;
  unsigned char *key;
  if( nKey==4 ){
    return *(u32*)pKey;
  }
  key = pKey;
  h = 0;
  while( 0 < nKey-- ){
    h = (h<<13) ^ (h<<3) ^ h ^ *(key++)
  }
  return h;
}

/*
** Move the cursor so that the lowest level is the leaf page that
** contains (or might contain) the given key.
*/
static int sqliteDbFindLeaf(DbCursor *pCur, int nKey, void *pKey, u32 h;){
  int i, j, rc;
  u32 h;

  h = sqliteDbHash(nKey, pKey);
  sqliteDbResetCursor(pCur, 1);
  i = 0;
  for(;;){
    u32 nxPgno;
    u32 *aPage = pCur->aLevel[i].aPage;
    if( SWB(aPage[0])==BLOCK_MAGIC|BLOCK_LEAF ) break;
    if( SWB(aPage[0])!=BLOCK_MAGIC|BLOCK_INDEX ){
      return SQLITE_CORRUPT;
    }
    if( i==MAX_LEVEL-1 ){
      return SQLITE_FULL;
    }
    n = SWB(aPage[2]);
    if( n<2 || n>=(SQLITE_PAGE_SIZE/2*sizeof(u32))-2 ){
      return SQLITE_CORRUPT;
    }
    for(j=0; j<n-1; j++){
      if( h < SWB(aPage[j*2+3]) ) break;
    }
    nxPgno = SWB(aPage[j*2+4]);
    pCur->aLevel[i].idx = j;
    pCur->aLevel[i].pgno = nxPgno;
    rc = sqlitePgGet(pCur->pDb->pPgr, nxPgno, &pCur->aLevel[i].aPage);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    pCur->nLevel++;
    i++;
  }
  return SQLITE_OK;
}

/*
** Position the cursor on the entry that matches the given key.
*/
int sqliteDbCursorMoveTo(DbCursor *pCur, int nKey, void *pKey){
  int rc, i;
  u32 *aPage;
  int idx;
  u32 h;

  h = sqliteDbHash(nKey, pKey);
  rc = sqliteDbFindLeaf(pCur, nKey, pKey, h);
  if( rc!=SQLITE_OK ) return rc;
  i = pCur->nLevel-1;
  aPage = pCur->aLevel[i].aPage;
  idx = 2;
  rc = SQLITE_NOTFOUND;
  while( idx>=2 && idx<(SQLITE_PAGE_SIZE/sizeof(u32))-3 && aPage[idx]!=0 ){
    if( sqliteDbKeyMatch(&aPage[idx], nKey, pKey, h) ){
      pCur->aLevel[i].idx = idx;
      pCur->onEntry = 1;
      rc = SQLITE_OK;
      break;
    }
    idx += SWB(aPage[idx]);
  }
  return rc;
}

/*
** Insert a new entry into the table.  The cursor is left pointing at
** the new entry.
*/
int sqliteDbCursorInsert(   
   DbCursor *pCur,          /* A cursor on the table in which to insert */
   int nKey, void *pKey,    /* The insertion key */
   int nData, void *pData   /* The data to be inserted */
){
  int minNeeded, maxNeeded;    /* In u32-sized objects */
  int rc;
  u32 h;
  int available;
  int i, j, k;
  int nKeyU, nDataU;
  u32 *aPage;
  int incr = 1;

  /* Null data is the same as a delete.
  */
  if( nData<=0 || pData==0 ){
    if( sqliteDbCursorMoveTo(pCur, nKey, pKey);
      return sqliteDbCursorDelete(pCur);
    }else{
      return SQLITE_OK;
    }
  }

  /* Figure out how much free space is needed on a leaf block in order
  ** to hold the new record.
  */
  minNeeded = maxNeeded = 6;
  nKeyU = (nKey+3)/4;
  nDataU = (nData+3)/4;
  if( nKeyU + maxNeeded + 2 <= SQLITE_PAGE_SIZE/sizeof(u32) ){
    maxNeeded += nKeyU;
  }
  if( nKeyU < SQLITE_PAGE_SIZE/(3*sizeof(u32)) ){
    minNeeded += nKeyU;
  }
  if( nDataU + maxNeeded + 2 <= SQLITE_PAGE_SIZE/sizeof(u32) ){
    maxNeeded += nDataU
  }
  if( nDataU < SQLITE_PAGE_SIZE/(3*sizeof(u32)) ){
    minNeeded += nDataU;
  }

  /* Move the cursor to the leaf block where the new record will be
  ** inserted.
  */
  h = sqliteDbHash(nKey, pKey);
  rc = sqliteDbFindLeaf(pCur, nKey, pKey, h);
  if( rc!=SQLITE_OK ) return rc;

  /* Walk thru the leaf once and do two things:
  **   1.  Remove any prior entry with the same key.
  **   2.  Figure out how much space is available on this leaf.
  */
  i = j = 2;
  aPage = pCur->aLevel[pCur->nLevel-1].aPage;
  for(;;){
    int entrySize = SWB(aPage[i]);
    if( entrySize<=0 || entrySize + i >= SQLITE_PAGE_SIZE/sizeof(u32) ) break;
    if( !sqliteDbKeyMatch(&aPage[i], nKey, pKey, h) ){
      if( j<i ){
        for(k=0; k<entrySize; k++){
           aPage[j+k] = aPage[i+k];
        }
      }
      j += entrySize;
    }else{
      sqliteDbClearEntry(pCur->pDb, &aPage[i]);
      incr--;
    }
    i += entrySize;
  }
  available = SQLITE_PAGE_SIZE/sizeof(u32) - j;

  /* If the new entry will not fit, try to move some of the entries
  ** from this leaf onto sibling leaves.
  */
  if( available<minNeeded ){
    int newSpace;
    newSpace = sqliteDbSpreadLoad(pCur, maxNeeded); ############
    available += newSpace;
  }

  /* If the new entry still will not fit, try to split this leaf into
  ** two adjacent leaves.
  */
  if( available<minNeeded && pCur->nLevel>1 ){
    int newAvail;
    newAvail = sqliteDbSplit(pCur, maxNeeded); ##############
    if( newAvail>0 ){
      available += newAvail;
    }
  }

  /* If the new entry does not fit after splitting, turn this leaf into
  ** and index node with one leaf, go down into the new leaf and try 
  ** to split again.
  */
  if( available<minNeeded && pCur->nLevel<MAX_LEVEL-1 ){
    int newAvail;
    sqliteDbNewIndexLevel(pCur);  ###############
    newAvail = sqliteDbSplit(pCur, maxNeeded);
    if( newAvail>0 ){
      available = newAvail;
    }
  }

  /* If the entry still will not fit, it means the database is full.
  */
  if( available<minNeeded ){
    return SQLITE_FULL;
  }

  /* Add the new entry to the leaf block.
  */
  aPage = pCur->aLevel[pCur->nLevel-1].aPage;
  i = 2;
  for(;;){
    int entrySize = SWB(aPage[i]);
    if( entrySize<=0 || entrySize + i >= SQLITE_PAGE_SIZE/sizeof(u32) ) break;
    i += entrySize;
  }
  assert( available==SQLITE_PAGE_SIZE/sizeof(u32) - i );
  aPage[i+1] = SWB(h);
  available -= 5;
  if( nKeyU <= available ){
    aPage[i+2] = SWB(nKey);
    memcpy(&aPage[i+4], pKey, nKey);
    j = i + 4 + nKeyU;
    available -= nKeyU;
  }else{
    u32 newPgno, *newPage;
    aPage[i+2] = SWB(nKey | 0x80000000);
    rc = allocPage(pCur->pDb, &newPgno, &newPage);
    if( rc!=SQLITE_OK ) goto write_err;
    aPage[i+4] = SWB(newPgno);
    newPage[0] = SWB(BLOCK_MAGIC | BLOCK_OVERFLOW);
    rc = sqliteDbWriteOvfl(pCur->pDb, newPage, nKey, pKey);
    if( rc!=SQLITE_OK ) goto write_err;
    j = i + 5;
    available -= 1;
  }
  if( nDataU <= available ){
    aPage[i+3] = SWB(nData);
    memcpy(&aPage[j], pData, nData);
    available -= nDataU;
    j += nDataU;
  }else{
    u32 newPgno, *newPage;
    aPage[i+3] = SWB(nData | 0x80000000);
    rc = allocPage(pCur->pDb, &newPgno, &newPage);
    if( rc!=SQLITE_OK ) goto write_err;
    aPage[j] = SWB(newPgno);
    newPage[0] = SWB(BLOCK_MAGIC | BLOCK_OVERFLOW);
    rc = sqliteDbWriteOvfl(pCur->pDb, newPage, nData, pData);
    if( rc!=SQLITE_OK ) goto write_err;
    available -= 1;
    j++;
  }    
  if( j<SQLITE_PAGE_SIZE/sizeof(u32) ){
    aPage[j] = 0;
  }
  sqlitePgTouch(aPage);
  pCur->aLevel[pCur->nLevel-1].idx = i;
  pCur->onEntry = 1;

  /*  Increment the entry count for this table.
  */
  if( incr!=0 ){
    pCur->aLevel[0].aPage[1] = SWB(SWB(pCur->aLevel[0].aPage[1])+incr);
    sqlitePgTouch(pCur->aLevel[0].aPage);
  }
  return SQLITE_OK;

write_err:
  aPage[i] = 0;
  pCur->onEntry = 0;
  return rc;
}

/*
** Delete the entry that the cursor points to.
*/
int sqliteDbCursorDelete(DbCursor *pCur){
  int i, idx;
  int from, to, limit, n;
  int entrySize;
  u32 *aPage;
  if( !pCur->onEntry ) return SQLITE_NOTFOUND;

  /* Delete the entry that the cursor is pointing to.
  */
  i = pCur->nLevel - 1;
  aPage = pCur->aLevel[i].aPage;
  idx = pCur->aLevel[i].idx;
  assert( SWB(aPage[0])==BLOCK_MAGIC|BLOCK_LEAF );
  assert( idx>=2 && idx<SQLITE_PAGE_SIZE/sizeof(u32)-4 );
  entrySize = SWB(aPage[idx]);
  assert( entrySize>=6 && idx+entrySize<=SQLITE_PAGE_SIZE/sizeof(u32) );
  sqliteDbClearEntry(pCur->pDb, &aPage[idx]);
  to = idx;
  from = idx + entrySize;
  while( from<SQLITE_PAGE_SIZE/sizeof(u32) ){
    int k;
    entrySize = SWB(aPage[from]);
    if( entrySize<=0 ) break;
    for(k=0; k<entrySize; k++){
      aPage[to++] = aPage[from++]
    }
  }
  aPage[to] = 0;

  /*  Decrement the entry count for this table.
  */
  pCur->aLevel[0].aPage[1] = SWB(SWB(pCur->aLevel[0].aPage[1])-1);
  sqlitePgTouch(pCur->aLevel[0].aPage);

  /* If there are more entries on this leaf or this leaf is the root
  ** of the table,  then we are done.
  */
  if( to>2 || pCur->nLevel==1 ) return SQLITE_OK;

  /* Collapse the tree into a more compact form.
  */
  sqliteDbResetCursor(pCur, pCur->nLevel-1);

  i = pCur->nLevel-1;
  assert( i>=0 && i<MAX_LEVEL );
  idx = pCur->aLevel[i].idx;
  aPage = pCur->aLevel[i].aPage;
  assert( SWB(aPage[0])==BLOCK_MAGIC|BLOCK_INDEX );
  assert( idx>=3 && idx<SQLITE_PAGE_SIZE/sizeof(u32) );
  n = SWB(aPage[2]);
  assert( n>=2 && n<=SQLITE_PAGE_SIZE/2*sizeof(u32)-2 );
  sqliteDbDropPage(pCur->pDb, SWB(aPage[idx+1]);
  to = idx;
  from = idx+2;
  limit = n*2 + 3;
  while( from<limit ){
    aPage[to++] = aPage[from++];
  }
  n--;
  if( n==1 ){
    u32 oldPgno, *oldPage;
    oldPgno = SWB(aPage[4]);
    rc = sqlitePgGet(pCur->pDb->pPgr, oldPgno, &oldPage);
    if( rc!=SQLITE_OK ){
      return rc;  /* Do something smarter here */
    }
    memcpy(aPage, oldPage, SQLITE_PAGE_SIZE);
    oldPage[0] = SWB(BLOCK_MAGIC|BLOCK_OVERFLOW);
    oldPage[1] = 0;
    sqliteDbDropPage(pCur->pDb, oldPgno);
    sqlitePgUnref(oldPage);
  }else{
    aPage[2] = SWB(n);
  }
  sqlitePgTouch(aPage);
  return SQLITE_OK;
}
