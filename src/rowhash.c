/*
** 2009 April 15
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
** This file contains the implementation of the "row-hash" data structure.
**
** $Id: rowhash.c,v 1.1 2009/04/21 09:02:47 danielk1977 Exp $
*/
#include "sqliteInt.h"

typedef struct RowHashElem RowHashElem;
typedef struct RowHashBlock RowHashBlock;

/*
** Size of heap allocations made by this module. This limit is 
** never exceeded.
*/
#define ROWHASH_ALLOCATION 1024

/*
** Number of elements in the RowHashBlock.aElem[] array. This array is
** sized to make RowHashBlock very close to (without exceeding)
** ROWHASH_ALLOCATION bytes in size.
*/
#define ROWHASH_ELEM_PER_BLOCK (                                            \
    (ROWHASH_ALLOCATION - ROUND8(sizeof(struct RowHashBlockData))) /        \
    sizeof(RowHashElem)                                                     \
)

/*
** Number of pointers that fit into a single allocation of 
** ROWHASH_ALLOCATION bytes.
*/
#define ROWHASH_POINTER_PER_PAGE (ROWHASH_ALLOCATION/sizeof(void *))

/*
** If there are less than this number of elements in the block-list, do not
** bother building a hash-table. Just do a linear search of the list when
** querying.
*/
#define ROWHASH_LINEAR_SEARCH_LIMIT 10

/*
** Element stored in the hash-table.
*/
struct RowHashElem {
  i64 iVal;
  RowHashElem *pNext;
};

/*
** The following structure is either exactly ROWHASH_ALLOCATION bytes in
** size or just slightly less. It stores up to ROWHASH_ELEM_PER_BLOCK 
** RowHashElem structures.
*/
struct RowHashBlock {
  struct RowHashBlockData {
    int nElem;
    RowHashBlock *pNext;
  } data;
  RowHashElem aElem[ROWHASH_ELEM_PER_BLOCK];
};

/*
** RowHash structure. References to a structure of this type are passed
** around and used as opaque handles by code in other modules.
*/
struct RowHash {
  /* Variables populated by sqlite3RowhashInsert() */
  int nEntry;               /* Total number of entries in block-list */
  RowHashBlock *pBlock;     /* Linked list of entries */

  /* Variables populated by makeHashTable() */
  int iSet;                 /* Most recent iSet parameter passed to Test() */
  int iMod;                 /* Number of buckets in hash table */
  int nLeaf;                /* Number of leaf pages in hash table */
  int nHeight;              /* Height of tree containing leaf pages */
  void *pHash;              /* Pointer to root of tree */
  int nLinearLimit;         /* Linear search limit (used if pHash==0) */
};


/*
** Allocate a tree of height nHeight with *pnLeaf leaf pages. Set *pp to
** point to the root of the tree. If the maximum number of leaf pages in a
** tree of height nHeight is less than *pnLeaf, allocate a tree with the 
** maximum possible number of leaves for height nHeight. 
**
** Before returning, subtract the number of leaves in the tree allocated
** from *pnLeaf.
**
** This routine returns SQLITE_NOMEM if a malloc() fails, or SQLITE_OK
** otherwise.
*/
static int allocTable(void **pp, int nHeight, int *pnLeaf){
  void **ap = (void **)sqlite3MallocZero(ROWHASH_ALLOCATION);
  if( !ap ){
    return SQLITE_NOMEM;
  }
  *pp = (void *)ap;
  if( nHeight==0 ){
    (*pnLeaf)--;
  }else{
    int ii;
    for(ii=0; ii<ROWHASH_POINTER_PER_PAGE && *pnLeaf>0; ii++){
      if( allocTable(&ap[ii], nHeight-1, pnLeaf) ){
        return SQLITE_NOMEM;
      }
    }
  }
  return SQLITE_OK;
}

/*
** Delete the tree of height nHeight passed as the first argument.
*/
static void deleteTable(void **ap, int nHeight){
  if( ap ){
    if( nHeight>0 ){
      int ii;
      for(ii=0; ii<ROWHASH_POINTER_PER_PAGE; ii++){
        deleteTable((void **)ap[ii], nHeight-1);
      }
    }
    sqlite3_free(ap);
  }
}

/*
** Delete the hash-table stored in p->pHash. The p->pHash pointer is
** set to zero before returning. This function is the inverse of 
** allocHashTable()
*/
static void deleteHashTable(RowHash *p){
  deleteTable(p->pHash, p->nHeight);
  p->pHash = 0;
}

/*
** Allocate the hash table structure based on the current values of
** p->nLeaf and p->nHeight.
*/
static int allocHashTable(RowHash *p){
  int nLeaf = p->nLeaf;
  assert( p->pHash==0 );
  assert( p->nLeaf>0 );
  return allocTable(&p->pHash, p->nHeight, &nLeaf);
}

/*
** Find the hash-bucket associated with value iVal. Return a pointer to it.
*/
static void **findHashBucket(RowHash *p, i64 iVal){
  int aOffset[16];
  int n = p->nHeight;
  void **ap = p->pHash;
  int h = (((u64)iVal) % p->iMod);
  for(n=0; n<p->nHeight; n++){
    int h1 = h / ROWHASH_POINTER_PER_PAGE;
    aOffset[n] = h - (h1 * ROWHASH_POINTER_PER_PAGE);
    h = h1;
  }
  aOffset[n] = h;
  for(n=p->nHeight; n>0; n--){
    ap = (void **)ap[aOffset[n]];
  }
  return &ap[aOffset[0]];
}

/*
** Build a hash table to query with sqlite3RowhashTest() based on the
** set of values stored in the linked list of RowHashBlock structures.
*/
static int makeHashTable(RowHash *p, int iSet){
  RowHashBlock *pBlock;
  int iMod;
  int nLeaf;
  
  /* Delete the old hash table. */
  deleteHashTable(p);
  assert( p->iSet!=iSet );
  p->iSet = iSet;

  if( p->nEntry<ROWHASH_LINEAR_SEARCH_LIMIT ){
    p->nLinearLimit = p->nEntry;
    return SQLITE_OK;
  }

  /* Determine how many leaves the hash-table will comprise. */
  nLeaf = 1 + (p->nEntry / ROWHASH_POINTER_PER_PAGE);
  iMod = nLeaf*ROWHASH_POINTER_PER_PAGE;
  p->nLeaf = nLeaf;
  p->iMod = iMod;

  /* Set nHeight to the height of the tree that contains the leaf pages. If
  ** RowHash.nHeight is zero, then the whole hash-table fits on a single
  ** leaf. If RowHash.nHeight is 1, then RowHash.pHash points to an array
  ** of pointers to leaf pages. If 2, pHash points to an array of pointers
  ** to arrays of pointers to leaf pages. And so on.
  */
  p->nHeight = 0;
  while( nLeaf>1 ){
    nLeaf = (nLeaf+ROWHASH_POINTER_PER_PAGE-1) / ROWHASH_POINTER_PER_PAGE;
    p->nHeight++;
  }

  /* Allocate the hash-table. */
  if( allocHashTable(p) ){
    return SQLITE_NOMEM;
  }

  /* Insert all values into the hash-table. */
  for(pBlock=p->pBlock; pBlock; pBlock=pBlock->data.pNext){
    RowHashElem * const pEnd = &pBlock->aElem[pBlock->data.nElem];
    RowHashElem *pIter;
    for(pIter=pBlock->aElem; pIter<pEnd; pIter++){
      RowHashElem **ppElem = (RowHashElem **)findHashBucket(p, pIter->iVal);
      pIter->pNext = *ppElem;
      *ppElem = pIter;
    }
  }

  return SQLITE_OK;
}

/*
** Test if value iVal is in the hash table. If so, set *pExists to 1
** before returning. If iVal is not in the hash table, set *pExists to 0.
**
** Return SQLITE_OK if all goes as planned. If a malloc() fails, return
** SQLITE_NOMEM.
*/
int sqlite3RowhashTest(RowHash *p, int iSet, i64 iVal, int *pExists){
  *pExists = 0;
  if( p ){
    assert( p->pBlock );
    if( iSet!=p->iSet && makeHashTable(p, iSet) ){
      return SQLITE_NOMEM;
    }
    if( p->pHash ){
      RowHashElem *pElem = *(RowHashElem **)findHashBucket(p, iVal);
      for(; pElem; pElem=pElem->pNext){
        if( pElem->iVal==iVal ){
          *pExists = 1;
          break;
        }
      }
    }else{
      int ii;
      RowHashElem *aElem = p->pBlock->aElem;
      for(ii=0; ii<p->nLinearLimit; ii++){
        if( aElem[ii].iVal==iVal ){
          *pExists = 1;
          break;
        }
      }
    }
  }
  return SQLITE_OK;
}

/*
** Insert value iVal into the RowHash object.
**
** Return SQLITE_OK if all goes as planned. If a malloc() fails, return
** SQLITE_NOMEM.
*/
int sqlite3RowhashInsert(RowHash **pp, i64 iVal){
  RowHash *p = *pp;
  
  /* If the RowHash structure has not been allocated, allocate it now. */
  if( !p ){
    p = (RowHash*)sqlite3MallocZero(sizeof(RowHash));
    if( !p ){
      return SQLITE_NOMEM;
    }
    *pp = p;
  }

  /* If the current RowHashBlock is full, or if the first RowHashBlock has
  ** not yet been allocated, allocate one now. */ 
  if( !p->pBlock || p->pBlock->data.nElem==ROWHASH_ELEM_PER_BLOCK ){
    RowHashBlock *pBlock = (RowHashBlock*)sqlite3Malloc(sizeof(RowHashBlock));
    if( !pBlock ){
      return SQLITE_NOMEM;
    }
    pBlock->data.nElem = 0;
    pBlock->data.pNext = p->pBlock;
    p->pBlock = pBlock;
  }

  /* Add iVal to the current RowHashBlock. */
  p->pBlock->aElem[p->pBlock->data.nElem].iVal = iVal;
  p->pBlock->data.nElem++;
  p->nEntry++;
  return SQLITE_OK;
}

/*
** Destroy the RowHash object passed as the first argument.
*/
void sqlite3RowhashDestroy(RowHash *p){
  if( p ){
    RowHashBlock *pBlock, *pNext;
    deleteHashTable(p);
    for(pBlock=p->pBlock; pBlock; pBlock=pNext){
      pNext = pBlock->data.pNext;
      sqlite3_free(pBlock);
    }
    sqlite3_free(p);
  }
}

