/*
** 2008 December 3
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This module implements an object we call a "Row Set".
**
** The RowSet object is a bag of rowids.  Rowids
** are inserted into the bag in an arbitrary order.  Then they are
** pulled from the bag in sorted order.  Rowids only appear in the
** bag once.  If the same rowid is inserted multiple times, the
** second and subsequent inserts make no difference on the output.
**
** This implementation accumulates rowids in a linked list.  For
** output, it first sorts the linked list (removing duplicates during
** the sort) then returns elements one by one by walking the list.
**
** Big chunks of rowid/next-ptr pairs are allocated at a time, to
** reduce the malloc overhead.
**
** $Id: rowset.c,v 1.3 2009/01/13 20:14:16 drh Exp $
*/
#include "sqliteInt.h"

/*
** The number of rowset entries per allocation chunk.
*/
#define ROWSET_ENTRY_PER_CHUNK  63

/*
** Each entry in a RowSet is an instance of the following
** structure:
*/
struct RowSetEntry {            
  i64 v;                        /* ROWID value for this entry */
  struct RowSetEntry *pNext;    /* Next entry on a list of all entries */
};

/*
** Index entries are allocated in large chunks (instances of the
** following structure) to reduce memory allocation overhead.  The
** chunks are kept on a linked list so that they can be deallocated
** when the RowSet is destroyed.
*/
struct RowSetChunk {
  struct RowSetChunk *pNext;             /* Next chunk on list of them all */
  struct RowSetEntry aEntry[ROWSET_ENTRY_PER_CHUNK]; /* Allocated entries */
};

/*
** A RowSet in an instance of the following structure.
**
** A typedef of this structure if found in sqliteInt.h.
*/
struct RowSet {
  struct RowSetChunk *pChunk;    /* List of all chunk allocations */
  sqlite3 *db;                   /* The database connection */
  struct RowSetEntry *pEntry;    /* List of entries in the rowset */
  struct RowSetEntry *pLast;     /* Last entry on the pEntry list */
  struct RowSetEntry *pFresh;    /* Source of new entry objects */
  u16 nFresh;                    /* Number of objects on pFresh */
  u8 isSorted;                   /* True if content is sorted */
};

/*
** Turn bulk memory into a RowSet object.  N bytes of memory
** are available at pSpace.  The db pointer is used as a memory context
** for any subsequent allocations that need to occur.
** Return a pointer to the new RowSet object.
**
** It must be the case that N is sufficient to make a Rowset.  If not
** an assertion fault occurs.
** 
** If N is larger than the minimum, use the surplus as an initial
** allocation of entries available to be filled.
*/
RowSet *sqlite3RowSetInit(sqlite3 *db, void *pSpace, unsigned int N){
  RowSet *p;
  assert( N >= sizeof(*p) );
  p = pSpace;
  p->pChunk = 0;
  p->db = db;
  p->pEntry = 0;
  p->pLast = 0;
  p->pFresh = (struct RowSetEntry*)&p[1];
  p->nFresh = (u16)((N - sizeof(*p))/sizeof(struct RowSetEntry));
  p->isSorted = 1;
  return p;
}

/*
** Deallocate all chunks from a RowSet.
*/
void sqlite3RowSetClear(RowSet *p){
  struct RowSetChunk *pChunk, *pNextChunk;
  for(pChunk=p->pChunk; pChunk; pChunk = pNextChunk){
    pNextChunk = pChunk->pNext;
    sqlite3DbFree(p->db, pChunk);
  }
  p->pChunk = 0;
  p->nFresh = 0;
  p->pEntry = 0;
  p->pLast = 0;
  p->isSorted = 1;
}

/*
** Insert a new value into a RowSet.
**
** The mallocFailed flag of the database connection is set if a
** memory allocation fails.
*/
void sqlite3RowSetInsert(RowSet *p, i64 rowid){
  struct RowSetEntry *pEntry;
  struct RowSetEntry *pLast;
  if( p==0 ) return;  /* Must have been a malloc failure */
  if( p->nFresh==0 ){
    struct RowSetChunk *pNew;
    pNew = sqlite3DbMallocRaw(p->db, sizeof(*pNew));
    if( pNew==0 ){
      return;
    }
    pNew->pNext = p->pChunk;
    p->pChunk = pNew;
    p->pFresh = pNew->aEntry;
    p->nFresh = ROWSET_ENTRY_PER_CHUNK;
  }
  pEntry = p->pFresh++;
  p->nFresh--;
  pEntry->v = rowid;
  pEntry->pNext = 0;
  pLast = p->pLast;
  if( pLast ){
    if( p->isSorted && rowid<=pLast->v ){
      p->isSorted = 0;
    }
    pLast->pNext = pEntry;
  }else{
    assert( p->pEntry==0 );
    p->pEntry = pEntry;
  }
  p->pLast = pEntry;
}

/*
** Merge two lists of RowSet entries.  Remove duplicates.
**
** The input lists are assumed to be in sorted order.
*/
static struct RowSetEntry *boolidxMerge(
  struct RowSetEntry *pA,    /* First sorted list to be merged */
  struct RowSetEntry *pB     /* Second sorted list to be merged */
){
  struct RowSetEntry head;
  struct RowSetEntry *pTail;

  pTail = &head;
  while( pA && pB ){
    assert( pA->pNext==0 || pA->v<=pA->pNext->v );
    assert( pB->pNext==0 || pB->v<=pB->pNext->v );
    if( pA->v<pB->v ){
      pTail->pNext = pA;
      pA = pA->pNext;
      pTail = pTail->pNext;
    }else if( pB->v<pA->v ){
      pTail->pNext = pB;
      pB = pB->pNext;
      pTail = pTail->pNext;
    }else{
      pA = pA->pNext;
    }
  }
  if( pA ){
    assert( pA->pNext==0 || pA->v<=pA->pNext->v );
    pTail->pNext = pA;
  }else{
    assert( pB==0 || pB->pNext==0 || pB->v<=pB->pNext->v );
    pTail->pNext = pB;
  }
  return head.pNext;
}

/*
** Sort all elements of the RowSet into ascending order.
*/ 
static void sqlite3RowSetSort(RowSet *p){
  unsigned int i;
  struct RowSetEntry *pEntry;
  struct RowSetEntry *aBucket[40];

  assert( p->isSorted==0 );
  memset(aBucket, 0, sizeof(aBucket));
  while( p->pEntry ){
    pEntry = p->pEntry;
    p->pEntry = pEntry->pNext;
    pEntry->pNext = 0;
    for(i=0; aBucket[i]; i++){
      pEntry = boolidxMerge(aBucket[i],pEntry);
      aBucket[i] = 0;
    }
    aBucket[i] = pEntry;
  }
  pEntry = 0;
  for(i=0; i<sizeof(aBucket)/sizeof(aBucket[0]); i++){
    pEntry = boolidxMerge(pEntry,aBucket[i]);
  }
  p->pEntry = pEntry;
  p->pLast = 0;
  p->isSorted = 1;
}

/*
** Extract the next (smallest) element from the RowSet.
** Write the element into *pRowid.  Return 1 on success.  Return
** 0 if the RowSet is already empty.
*/
int sqlite3RowSetNext(RowSet *p, i64 *pRowid){
  if( !p->isSorted ){
    sqlite3RowSetSort(p);
  }
  if( p->pEntry ){
    *pRowid = p->pEntry->v;
    p->pEntry = p->pEntry->pNext;
    if( p->pEntry==0 ){
      sqlite3RowSetClear(p);
    }
    return 1;
  }else{
    return 0;
  }
}
