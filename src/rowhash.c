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
** This file contains the implementation of the RowHash data structure.
** A RowHash has the following properties:
**
**   *  A RowHash stores an unordered "bag" of 64-bit integer rowids.
**      There is no other content.
**
**   *  Primative operations are CREATE, INSERT, TEST, and DESTROY.
**      There is no way to remove individual elements from the RowHash
**      once they are inserted.
**
**   *  INSERT operations are batched.  TEST operation will ignore
**      elements in the current INSERT batch.  Only elements inserted
**      in prior batches will be seen by a TEST.
**
** The insert batch number is a parameter to the TEST primitive.  The
** hash table is rebuilt whenever the batch number increases.  TEST
** operations only look for INSERTs that occurred in prior batches.
**
** The caller is responsible for insuring that there are no duplicate
** INSERTs.
**
** $Id: rowhash.c,v 1.4 2009/04/21 18:20:45 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** An upper bound on the size of heap allocations made by this module.
** Limiting the size of allocations helps to avoid memory fragmentation.
*/
#define ROWHASH_ALLOCATION 1024

/*
** If there are less than this number of elements in the RowHash, do not
** bother building a hash-table. Just do a linear search.
*/
#define ROWHASH_LINEAR_SEARCH_LIMIT 10

/*
** This value is what we want the average length of the collision hash
** chain to be.
*/
#define ROWHASH_COLLISION_LENGTH 3


/* Forward references to data structures. */
typedef struct RowHashElem RowHashElem;
typedef struct RowHashBlock RowHashBlock;
typedef union RowHashPtr RowHashPtr;
typedef struct RowHashPage RowHashPage;

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
#define ROWHASH_POINTER_PER_PAGE (ROWHASH_ALLOCATION/sizeof(RowHashPtr))

/*
** A page of pointers used to construct a hash table.
**
** The hash table is actually a tree composed of instances of this
** object.  Leaves of the tree use the a[].pElem pointer to point
** to RowHashElem entries.  Interior nodes of the tree use the
** a[].pPage element to point to subpages.
**
** The hash table is split into a tree in order to avoid having
** to make large memory allocations, since large allocations can
** result in unwanted memory fragmentation.
*/
struct RowHashPage {
  union RowHashPtr {
    RowHashPage *pPage;   /* Used by interior nodes.  Pointer to subtree. */
    RowHashElem *pElem;   /* Used by leaves.  Pointer to hash entry. */
  } a[ROWHASH_POINTER_PER_PAGE];
};

/*
** Each 64-bit integer in a RowHash is stored as an instance of
** this object.  
**
** Instances of this object are not allocated separately.  They are
** allocated in large blocks using the RowHashBlock object as a container.
*/
struct RowHashElem {
  i64 iVal;              /* The value being stored.  A rowid. */
  RowHashElem *pNext;    /* Next element with the same hash */
};

/*
** In order to avoid many small allocations of RowHashElem objects,
** multiple RowHashElem objects are allocated at once, as an instance
** of this object, and then used as needed.
**
** A single RowHash object will allocate one or more of these RowHashBlock
** objects.  As many are allocated as are needed to store all of the
** content.  All RowHashBlocks are kept on a linked list formed using
** RowHashBlock.data.pNext so that they can be freed when the RowHash
** is destroyed.
**
** The linked list of RowHashBlock objects also provides a way to sequentially
** scan all elements in the RowHash.  This sequential scan is used when
** rebuilding the hash table.  The hash table is rebuilt after every 
** batch of inserts.
*/
struct RowHashBlock {
  struct RowHashBlockData {
    RowHashBlock *pNext;      /* Next RowHashBlock object in list of them all */
  } data;
  RowHashElem aElem[ROWHASH_ELEM_PER_BLOCK]; /* Available RowHashElem objects */
};

/*
** RowHash structure. References to a structure of this type are passed
** around and used as opaque handles by code in other modules.
*/
struct RowHash {
  int nUsed;              /* Number of used entries in first RowHashBlock */
  int nEntry;             /* Number of used entries over all RowHashBlocks */
  int iBatch;             /* The current insert batch number */
  u8 nHeight;             /* Height of tree of hash pages */
  u8 nLinearLimit;        /* Linear search limit (used if pHash==0) */
  int nBucket;            /* Number of buckets in hash table */
  RowHashPage *pHash;     /* Pointer to root of hash table tree */
  RowHashBlock *pBlock;   /* Linked list of RowHashBlocks */
  sqlite3 *db;            /* Associated database connection */
};


/*
** Allocate a hash table tree of height nHeight with *pnLeaf leaf pages. 
** Set *pp to point to the root of the tree.  If the maximum number of leaf 
** pages in a tree of height nHeight is less than *pnLeaf, allocate only
** that part of the tree that is necessary to account for all leaves.
**
** Before returning, subtract the number of leaves in the tree allocated
** from *pnLeaf.
**
** This routine returns SQLITE_NOMEM if a malloc() fails, or SQLITE_OK
** otherwise.
*/
static int allocHashTable(RowHashPage **pp, int nHeight, int *pnLeaf){
  RowHashPage *p = (RowHashPage *)sqlite3MallocZero(sizeof(*p));
  if( !p ){
    return SQLITE_NOMEM;
  }
  *pp = p;
  if( nHeight==0 ){
    (*pnLeaf)--;
  }else{
    int ii;
    for(ii=0; ii<ROWHASH_POINTER_PER_PAGE && *pnLeaf>0; ii++){
      if( allocHashTable(&p->a[ii].pPage, nHeight-1, pnLeaf) ){
        return SQLITE_NOMEM;
      }
    }
  }
  return SQLITE_OK;
}

/*
** Delete the hash table tree of height nHeight passed as the first argument.
*/
static void deleteHashTable(RowHashPage *p, int nHeight){
  if( p ){
    if( nHeight>0 ){
      int ii;
      for(ii=0; ii<ROWHASH_POINTER_PER_PAGE; ii++){
        deleteHashTable(p->a[ii].pPage, nHeight-1);
      }
    }
    sqlite3_free(p);
  }
}

/*
** Find the hash-bucket associated with value iVal. Return a pointer to it.
**
** By "hash-bucket", we mean the RowHashPage.a[].pElem pointer that
** corresponds to a particular hash entry.
*/
static RowHashElem **findHashBucket(RowHash *pRowHash, i64 iVal){
  int aOffset[16];
  int n;
  RowHashPage *pPage = pRowHash->pHash;
  int h = (((u64)iVal) % pRowHash->nBucket);

  assert( pRowHash->nHeight < sizeof(aOffset)/sizeof(aOffset[0]) );
  for(n=0; n<pRowHash->nHeight; n++){
    int h1 = h / ROWHASH_POINTER_PER_PAGE;
    aOffset[n] = h - (h1 * ROWHASH_POINTER_PER_PAGE);
    h = h1;
  }
  aOffset[n] = h;
  for(n=pRowHash->nHeight; n>0; n--){
    pPage = pPage->a[aOffset[n]].pPage;
  }
  return &pPage->a[aOffset[0]].pElem;
}

/*
** Build a new hash table tree in p->pHash.  The new hash table should
** contain all p->nEntry entries in the p->pBlock list.  If there
** existed a prior tree, delete the old tree first before constructing
** the new one.
**
** If the number of entries (p->nEntry) is less than
** ROWHASH_LINEAR_SEARCH_LIMIT, then we are guessing that a linear
** search is going to be faster than a lookup, so do not bother
** building the hash table.
*/
static int makeHashTable(RowHash *p, int iBatch){
  RowHashBlock *pBlock;
  int nBucket;
  int nLeaf, n;
  
  /* Delete the old hash table. */
  deleteHashTable(p->pHash, p->nHeight);
  assert( p->iBatch!=iBatch );
  p->iBatch = iBatch;

  /* Skip building the hash table if the number of elements is small */
  if( p->nEntry<ROWHASH_LINEAR_SEARCH_LIMIT ){
    p->nLinearLimit = p->nEntry;
    p->pHash = 0;
    return SQLITE_OK;
  }

  /* Determine how many leaves the hash-table will comprise. */
  nLeaf = 1 + (p->nEntry / (ROWHASH_POINTER_PER_PAGE*ROWHASH_COLLISION_LENGTH));
  p->nBucket = nBucket = nLeaf*ROWHASH_POINTER_PER_PAGE;

  /* Set nHeight to the height of the tree that contains the leaf pages. If
  ** RowHash.nHeight is zero, then the whole hash-table fits on a single
  ** leaf. If RowHash.nHeight is 1, then RowHash.pHash points to an array
  ** of pointers to leaf pages. If 2, pHash points to an array of pointers
  ** to arrays of pointers to leaf pages. And so on.
  */
  p->nHeight = 0;
  n = nLeaf;
  while( n>1 ){
    n = (n+ROWHASH_POINTER_PER_PAGE-1) / ROWHASH_POINTER_PER_PAGE;
    p->nHeight++;
  }

  /* Allocate the hash-table. */
  if( allocHashTable(&p->pHash, p->nHeight, &nLeaf) ){
    return SQLITE_NOMEM;
  }

  /* Insert all values into the hash-table. */
  for(pBlock=p->pBlock; pBlock; pBlock=pBlock->data.pNext){
    RowHashElem * const pEnd = &pBlock->aElem[
      pBlock==p->pBlock?p->nUsed:ROWHASH_ELEM_PER_BLOCK
    ];
    RowHashElem *pIter;
    for(pIter=pBlock->aElem; pIter<pEnd; pIter++){
      RowHashElem **ppElem = findHashBucket(p, pIter->iVal);
      pIter->pNext = *ppElem;
      *ppElem = pIter;
    }
  }

  return SQLITE_OK;
}

/*
** Check to see if iVal has been inserted into the hash table "p"
** in some batch prior to iBatch.  If so, set *pExists to 1.
** If not, set *pExists to 0.
**
** The hash table is rebuilt whenever iBatch changes.  A hash table
** rebuild might encounter an out-of-memory condition.  If that happens,
** return SQLITE_NOMEM.  If there are no problems, return SQLITE_OK.
**
** The initial "batch" is 0.  So, if there were prior calls to
** sqlite3RowhashInsert() and then this routine is invoked with iBatch==0,
** because all prior inserts where in the same batch, none of the prior
** inserts will be visible and this routine will indicate not found.
** Hence, the first invocation of this routine should probably use
** a batch number of 1.
*/
int sqlite3RowhashTest(
  RowHash *p,     /* The RowHash to search in */
  int iBatch,     /* Look for values inserted in batches prior to this batch */
  i64 iVal,       /* The rowid value we are looking for */
  int *pExists    /* Store 0 or 1 hear to indicate not-found or found */
){
  *pExists = 0;
  if( p ){
    assert( p->pBlock );
    if( iBatch!=p->iBatch && makeHashTable(p, iBatch) ){
      return SQLITE_NOMEM;
    }
    if( p->pHash ){
      RowHashElem *pElem = *findHashBucket(p, iVal);
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
** Insert value iVal into the RowHash object.  Allocate a new RowHash
** object if necessary.
**
** Return SQLITE_OK if all goes as planned. If a malloc() fails, return
** SQLITE_NOMEM.
*/
int sqlite3RowhashInsert(sqlite3 *db, RowHash **pp, i64 iVal){
  RowHash *p = *pp;
  
  /* If the RowHash structure has not been allocated, allocate it now. */
  if( !p ){
    p = (RowHash*)sqlite3DbMallocZero(db, sizeof(RowHash));
    if( !p ){
      return SQLITE_NOMEM;
    }
    p->db = db;
    *pp = p;
  }

  /* If the current RowHashBlock is full, or if the first RowHashBlock has
  ** not yet been allocated, allocate one now. */ 
  if( !p->pBlock || p->nUsed==ROWHASH_ELEM_PER_BLOCK ){
    RowHashBlock *pBlock = (RowHashBlock*)sqlite3Malloc(sizeof(RowHashBlock));
    if( !pBlock ){
      return SQLITE_NOMEM;
    }
    pBlock->data.pNext = p->pBlock;
    p->pBlock = pBlock;
    p->nUsed = 0;
  }
  assert( p->nUsed==(p->nEntry % ROWHASH_ELEM_PER_BLOCK) );

  /* Add iVal to the current RowHashBlock. */
  p->pBlock->aElem[p->nUsed].iVal = iVal;
  p->nUsed++;
  p->nEntry++;
  return SQLITE_OK;
}

/*
** Destroy the RowHash object passed as the first argument.
*/
void sqlite3RowhashDestroy(RowHash *p){
  if( p ){
    RowHashBlock *pBlock, *pNext;
    deleteHashTable(p->pHash, p->nHeight);
    for(pBlock=p->pBlock; pBlock; pBlock=pNext){
      pNext = pBlock->data.pNext;
      sqlite3_free(pBlock);
    }
    sqlite3DbFree(p->db, p);
  }
}
