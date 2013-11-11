/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code for implementations of the r-tree and r*-tree
** algorithms packaged as an SQLite virtual table module.
*/

/*
** Database Format of R-Tree Tables
** --------------------------------
**
** The data structure for a single virtual r-tree table is stored in three 
** native SQLite tables declared as follows. In each case, the '%' character
** in the table name is replaced with the user-supplied name of the r-tree
** table.
**
**   CREATE TABLE %_node(nodeno INTEGER PRIMARY KEY, data BLOB)
**   CREATE TABLE %_parent(nodeno INTEGER PRIMARY KEY, parentnode INTEGER)
**   CREATE TABLE %_rowid(rowid INTEGER PRIMARY KEY, nodeno INTEGER)
**
** The data for each node of the r-tree structure is stored in the %_node
** table. For each node that is not the root node of the r-tree, there is
** an entry in the %_parent table associating the node with its parent.
** And for each row of data in the table, there is an entry in the %_rowid
** table that maps from the entries rowid to the id of the node that it
** is stored on.
**
** The root node of an r-tree always exists, even if the r-tree table is
** empty. The nodeno of the root node is always 1. All other nodes in the
** table must be the same size as the root node. The content of each node
** is formatted as follows:
**
**   1. If the node is the root node (node 1), then the first 2 bytes
**      of the node contain the tree depth as a big-endian integer.
**      For non-root nodes, the first 2 bytes are left unused.
**
**   2. The next 2 bytes contain the number of entries currently 
**      stored in the node.
**
**   3. The remainder of the node contains the node entries. Each entry
**      consists of a single 8-byte integer followed by an even number
**      of 4-byte coordinates. For leaf nodes the integer is the rowid
**      of a record. For internal nodes it is the node number of a
**      child page.
*/

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_RTREE)

/*
** This file contains an implementation of a couple of different variants
** of the r-tree algorithm. See the README file for further details. The 
** same data-structure is used for all, but the algorithms for insert and
** delete operations vary. The variants used are selected at compile time 
** by defining the following symbols:
*/

/* Either, both or none of the following may be set to activate 
** r*tree variant algorithms.
*/
#define VARIANT_RSTARTREE_CHOOSESUBTREE 0
#define VARIANT_RSTARTREE_REINSERT      1

/* 
** Exactly one of the following must be set to 1.
*/
#define VARIANT_GUTTMAN_QUADRATIC_SPLIT 0
#define VARIANT_GUTTMAN_LINEAR_SPLIT    0
#define VARIANT_RSTARTREE_SPLIT         1

#define VARIANT_GUTTMAN_SPLIT \
        (VARIANT_GUTTMAN_LINEAR_SPLIT||VARIANT_GUTTMAN_QUADRATIC_SPLIT)

#if VARIANT_GUTTMAN_QUADRATIC_SPLIT
  #define PickNext QuadraticPickNext
  #define PickSeeds QuadraticPickSeeds
  #define AssignCells splitNodeGuttman
#endif
#if VARIANT_GUTTMAN_LINEAR_SPLIT
  #define PickNext LinearPickNext
  #define PickSeeds LinearPickSeeds
  #define AssignCells splitNodeGuttman
#endif
#if VARIANT_RSTARTREE_SPLIT
  #define AssignCells splitNodeStartree
#endif

#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif

#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT1
#else
  #include "sqlite3.h"
#endif

#include <string.h>
#include <assert.h>

#ifndef SQLITE_AMALGAMATION
#include "sqlite3rtree.h"
typedef sqlite3_int64 i64;
typedef unsigned char u8;
typedef unsigned int u32;
#endif

/*  The following macro is used to suppress compiler warnings.
*/
#ifndef UNUSED_PARAMETER
# define UNUSED_PARAMETER(x) (void)(x)
#endif

typedef struct Rtree Rtree;
typedef struct RtreeCursor RtreeCursor;
typedef struct RtreeNode RtreeNode;
typedef struct RtreeCell RtreeCell;
typedef struct RtreeConstraint RtreeConstraint;
typedef struct RtreeMatchArg RtreeMatchArg;
typedef struct RtreeGeomCallback RtreeGeomCallback;
typedef union RtreeCoord RtreeCoord;

/* The rtree may have between 1 and RTREE_MAX_DIMENSIONS dimensions. */
#define RTREE_MAX_DIMENSIONS 5

/* Size of hash table Rtree.aHash. This hash table is not expected to
** ever contain very many entries, so a fixed number of buckets is 
** used.
*/
#define HASHSIZE 128

/* The xBestIndex method of this virtual table requires an estimate of
** the number of rows in the virtual table to calculate the costs of
** various strategies. If possible, this estimate is loaded from the
** sqlite_stat1 table (with RTREE_MIN_ROWEST as a hard-coded minimum).
** Otherwise, if no sqlite_stat1 entry is available, use 
** RTREE_DEFAULT_ROWEST.
*/
#define RTREE_DEFAULT_ROWEST 1048576
#define RTREE_MIN_ROWEST         100

/* 
** An rtree virtual-table object.
*/
struct Rtree {
  sqlite3_vtab base;
  sqlite3 *db;                /* Host database connection */
  int iNodeSize;              /* Size in bytes of each node in the node table */
  int nDim;                   /* Number of dimensions */
  int nBytesPerCell;          /* Bytes consumed per cell */
  int iDepth;                 /* Current depth of the r-tree structure */
  char *zDb;                  /* Name of database containing r-tree table */
  char *zName;                /* Name of r-tree table */ 
  RtreeNode *aHash[HASHSIZE]; /* Hash table of in-memory nodes. */ 
  int nBusy;                  /* Current number of users of this structure */
  i64 nRowEst;                /* Estimated number of rows in this table */

  /* List of nodes removed during a CondenseTree operation. List is
  ** linked together via the pointer normally used for hash chains -
  ** RtreeNode.pNext. RtreeNode.iNode stores the depth of the sub-tree 
  ** headed by the node (leaf nodes have RtreeNode.iNode==0).
  */
  RtreeNode *pDeleted;
  int iReinsertHeight;        /* Height of sub-trees Reinsert() has run on */

  /* Statements to read/write/delete a record from xxx_node */
  sqlite3_stmt *pReadNode;
  sqlite3_stmt *pWriteNode;
  sqlite3_stmt *pDeleteNode;

  /* Statements to read/write/delete a record from xxx_rowid */
  sqlite3_stmt *pReadRowid;
  sqlite3_stmt *pWriteRowid;
  sqlite3_stmt *pDeleteRowid;

  /* Statements to read/write/delete a record from xxx_parent */
  sqlite3_stmt *pReadParent;
  sqlite3_stmt *pWriteParent;
  sqlite3_stmt *pDeleteParent;

  int eCoordType;
};

/* Possible values for eCoordType: */
#define RTREE_COORD_REAL32 0
#define RTREE_COORD_INT32  1

/*
** If SQLITE_RTREE_INT_ONLY is defined, then this virtual table will
** only deal with integer coordinates.  No floating point operations
** will be done.
*/
#ifdef SQLITE_RTREE_INT_ONLY
  typedef sqlite3_int64 RtreeDValue;       /* High accuracy coordinate */
  typedef int RtreeValue;                  /* Low accuracy coordinate */
#else
  typedef double RtreeDValue;              /* High accuracy coordinate */
  typedef float RtreeValue;                /* Low accuracy coordinate */
#endif

/*
** The minimum number of cells allowed for a node is a third of the 
** maximum. In Gutman's notation:
**
**     m = M/3
**
** If an R*-tree "Reinsert" operation is required, the same number of
** cells are removed from the overfull node and reinserted into the tree.
*/
#define RTREE_MINCELLS(p) ((((p)->iNodeSize-4)/(p)->nBytesPerCell)/3)
#define RTREE_REINSERT(p) RTREE_MINCELLS(p)
#define RTREE_MAXCELLS 51

/*
** The smallest possible node-size is (512-64)==448 bytes. And the largest
** supported cell size is 48 bytes (8 byte rowid + ten 4 byte coordinates).
** Therefore all non-root nodes must contain at least 3 entries. Since 
** 2^40 is greater than 2^64, an r-tree structure always has a depth of
** 40 or less.
*/
#define RTREE_MAX_DEPTH 40

/* 
** An rtree cursor object.
*/
struct RtreeCursor {
  sqlite3_vtab_cursor base;
  RtreeNode *pNode;                 /* Node cursor is currently pointing at */
  int iCell;                        /* Index of current cell in pNode */
  int iStrategy;                    /* Copy of idxNum search parameter */
  int nConstraint;                  /* Number of entries in aConstraint */
  RtreeConstraint *aConstraint;     /* Search constraints. */
};

union RtreeCoord {
  RtreeValue f;
  int i;
};

/*
** The argument is an RtreeCoord. Return the value stored within the RtreeCoord
** formatted as a RtreeDValue (double or int64). This macro assumes that local
** variable pRtree points to the Rtree structure associated with the
** RtreeCoord.
*/
#ifdef SQLITE_RTREE_INT_ONLY
# define DCOORD(coord) ((RtreeDValue)coord.i)
#else
# define DCOORD(coord) (                           \
    (pRtree->eCoordType==RTREE_COORD_REAL32) ?      \
      ((double)coord.f) :                           \
      ((double)coord.i)                             \
  )
#endif

/*
** A search constraint.
*/
struct RtreeConstraint {
  int iCoord;                     /* Index of constrained coordinate */
  int op;                         /* Constraining operation */
  RtreeDValue rValue;             /* Constraint value. */
  int (*xGeom)(sqlite3_rtree_geometry*, int, RtreeDValue*, int*);
  sqlite3_rtree_geometry *pGeom;  /* Constraint callback argument for a MATCH */
};

/* Possible values for RtreeConstraint.op */
#define RTREE_EQ    0x41
#define RTREE_LE    0x42
#define RTREE_LT    0x43
#define RTREE_GE    0x44
#define RTREE_GT    0x45
#define RTREE_MATCH 0x46

/* 
** An rtree structure node.
*/
struct RtreeNode {
  RtreeNode *pParent;               /* Parent node */
  i64 iNode;
  int nRef;
  int isDirty;
  u8 *zData;
  RtreeNode *pNext;                 /* Next node in this hash chain */
};
#define NCELL(pNode) readInt16(&(pNode)->zData[2])

/* 
** Structure to store a deserialized rtree record.
*/
struct RtreeCell {
  i64 iRowid;
  RtreeCoord aCoord[RTREE_MAX_DIMENSIONS*2];
};


/*
** Value for the first field of every RtreeMatchArg object. The MATCH
** operator tests that the first field of a blob operand matches this
** value to avoid operating on invalid blobs (which could cause a segfault).
*/
#define RTREE_GEOMETRY_MAGIC 0x891245AB

/*
** An instance of this structure must be supplied as a blob argument to
** the right-hand-side of an SQL MATCH operator used to constrain an
** r-tree query.
*/
struct RtreeMatchArg {
  u32 magic;                      /* Always RTREE_GEOMETRY_MAGIC */
  int (*xGeom)(sqlite3_rtree_geometry *, int, RtreeDValue*, int *);
  void *pContext;
  int nParam;
  RtreeDValue aParam[1];
};

/*
** When a geometry callback is created (see sqlite3_rtree_geometry_callback),
** a single instance of the following structure is allocated. It is used
** as the context for the user-function created by by s_r_g_c(). The object
** is eventually deleted by the destructor mechanism provided by
** sqlite3_create_function_v2() (which is called by s_r_g_c() to create
** the geometry callback function).
*/
struct RtreeGeomCallback {
  int (*xGeom)(sqlite3_rtree_geometry*, int, RtreeDValue*, int*);
  void *pContext;
};

#ifndef MAX
# define MAX(x,y) ((x) < (y) ? (y) : (x))
#endif
#ifndef MIN
# define MIN(x,y) ((x) > (y) ? (y) : (x))
#endif

/*
** Functions to deserialize a 16 bit integer, 32 bit real number and
** 64 bit integer. The deserialized value is returned.
*/
static int readInt16(u8 *p){
  return (p[0]<<8) + p[1];
}
static void readCoord(u8 *p, RtreeCoord *pCoord){
  u32 i = (
    (((u32)p[0]) << 24) + 
    (((u32)p[1]) << 16) + 
    (((u32)p[2]) <<  8) + 
    (((u32)p[3]) <<  0)
  );
  *(u32 *)pCoord = i;
}
static i64 readInt64(u8 *p){
  return (
    (((i64)p[0]) << 56) + 
    (((i64)p[1]) << 48) + 
    (((i64)p[2]) << 40) + 
    (((i64)p[3]) << 32) + 
    (((i64)p[4]) << 24) + 
    (((i64)p[5]) << 16) + 
    (((i64)p[6]) <<  8) + 
    (((i64)p[7]) <<  0)
  );
}

/*
** Functions to serialize a 16 bit integer, 32 bit real number and
** 64 bit integer. The value returned is the number of bytes written
** to the argument buffer (always 2, 4 and 8 respectively).
*/
static int writeInt16(u8 *p, int i){
  p[0] = (i>> 8)&0xFF;
  p[1] = (i>> 0)&0xFF;
  return 2;
}
static int writeCoord(u8 *p, RtreeCoord *pCoord){
  u32 i;
  assert( sizeof(RtreeCoord)==4 );
  assert( sizeof(u32)==4 );
  i = *(u32 *)pCoord;
  p[0] = (i>>24)&0xFF;
  p[1] = (i>>16)&0xFF;
  p[2] = (i>> 8)&0xFF;
  p[3] = (i>> 0)&0xFF;
  return 4;
}
static int writeInt64(u8 *p, i64 i){
  p[0] = (i>>56)&0xFF;
  p[1] = (i>>48)&0xFF;
  p[2] = (i>>40)&0xFF;
  p[3] = (i>>32)&0xFF;
  p[4] = (i>>24)&0xFF;
  p[5] = (i>>16)&0xFF;
  p[6] = (i>> 8)&0xFF;
  p[7] = (i>> 0)&0xFF;
  return 8;
}

/*
** Increment the reference count of node p.
*/
static void nodeReference(RtreeNode *p){
  if( p ){
    p->nRef++;
  }
}

/*
** Clear the content of node p (set all bytes to 0x00).
*/
static void nodeZero(Rtree *pRtree, RtreeNode *p){
  memset(&p->zData[2], 0, pRtree->iNodeSize-2);
  p->isDirty = 1;
}

/*
** Given a node number iNode, return the corresponding key to use
** in the Rtree.aHash table.
*/
static int nodeHash(i64 iNode){
  return (
    (iNode>>56) ^ (iNode>>48) ^ (iNode>>40) ^ (iNode>>32) ^ 
    (iNode>>24) ^ (iNode>>16) ^ (iNode>> 8) ^ (iNode>> 0)
  ) % HASHSIZE;
}

/*
** Search the node hash table for node iNode. If found, return a pointer
** to it. Otherwise, return 0.
*/
static RtreeNode *nodeHashLookup(Rtree *pRtree, i64 iNode){
  RtreeNode *p;
  for(p=pRtree->aHash[nodeHash(iNode)]; p && p->iNode!=iNode; p=p->pNext);
  return p;
}

/*
** Add node pNode to the node hash table.
*/
static void nodeHashInsert(Rtree *pRtree, RtreeNode *pNode){
  int iHash;
  assert( pNode->pNext==0 );
  iHash = nodeHash(pNode->iNode);
  pNode->pNext = pRtree->aHash[iHash];
  pRtree->aHash[iHash] = pNode;
}

/*
** Remove node pNode from the node hash table.
*/
static void nodeHashDelete(Rtree *pRtree, RtreeNode *pNode){
  RtreeNode **pp;
  if( pNode->iNode!=0 ){
    pp = &pRtree->aHash[nodeHash(pNode->iNode)];
    for( ; (*pp)!=pNode; pp = &(*pp)->pNext){ assert(*pp); }
    *pp = pNode->pNext;
    pNode->pNext = 0;
  }
}

/*
** Allocate and return new r-tree node. Initially, (RtreeNode.iNode==0),
** indicating that node has not yet been assigned a node number. It is
** assigned a node number when nodeWrite() is called to write the
** node contents out to the database.
*/
static RtreeNode *nodeNew(Rtree *pRtree, RtreeNode *pParent){
  RtreeNode *pNode;
  pNode = (RtreeNode *)sqlite3_malloc(sizeof(RtreeNode) + pRtree->iNodeSize);
  if( pNode ){
    memset(pNode, 0, sizeof(RtreeNode) + pRtree->iNodeSize);
    pNode->zData = (u8 *)&pNode[1];
    pNode->nRef = 1;
    pNode->pParent = pParent;
    pNode->isDirty = 1;
    nodeReference(pParent);
  }
  return pNode;
}

/*
** Obtain a reference to an r-tree node.
*/
static int
nodeAcquire(
  Rtree *pRtree,             /* R-tree structure */
  i64 iNode,                 /* Node number to load */
  RtreeNode *pParent,        /* Either the parent node or NULL */
  RtreeNode **ppNode         /* OUT: Acquired node */
){
  int rc;
  int rc2 = SQLITE_OK;
  RtreeNode *pNode;

  /* Check if the requested node is already in the hash table. If so,
  ** increase its reference count and return it.
  */
  if( (pNode = nodeHashLookup(pRtree, iNode)) ){
    assert( !pParent || !pNode->pParent || pNode->pParent==pParent );
    if( pParent && !pNode->pParent ){
      nodeReference(pParent);
      pNode->pParent = pParent;
    }
    pNode->nRef++;
    *ppNode = pNode;
    return SQLITE_OK;
  }

  sqlite3_bind_int64(pRtree->pReadNode, 1, iNode);
  rc = sqlite3_step(pRtree->pReadNode);
  if( rc==SQLITE_ROW ){
    const u8 *zBlob = sqlite3_column_blob(pRtree->pReadNode, 0);
    if( pRtree->iNodeSize==sqlite3_column_bytes(pRtree->pReadNode, 0) ){
      pNode = (RtreeNode *)sqlite3_malloc(sizeof(RtreeNode)+pRtree->iNodeSize);
      if( !pNode ){
        rc2 = SQLITE_NOMEM;
      }else{
        pNode->pParent = pParent;
        pNode->zData = (u8 *)&pNode[1];
        pNode->nRef = 1;
        pNode->iNode = iNode;
        pNode->isDirty = 0;
        pNode->pNext = 0;
        memcpy(pNode->zData, zBlob, pRtree->iNodeSize);
        nodeReference(pParent);
      }
    }
  }
  rc = sqlite3_reset(pRtree->pReadNode);
  if( rc==SQLITE_OK ) rc = rc2;

  /* If the root node was just loaded, set pRtree->iDepth to the height
  ** of the r-tree structure. A height of zero means all data is stored on
  ** the root node. A height of one means the children of the root node
  ** are the leaves, and so on. If the depth as specified on the root node
  ** is greater than RTREE_MAX_DEPTH, the r-tree structure must be corrupt.
  */
  if( pNode && iNode==1 ){
    pRtree->iDepth = readInt16(pNode->zData);
    if( pRtree->iDepth>RTREE_MAX_DEPTH ){
      rc = SQLITE_CORRUPT_VTAB;
    }
  }

  /* If no error has occurred so far, check if the "number of entries"
  ** field on the node is too large. If so, set the return code to 
  ** SQLITE_CORRUPT_VTAB.
  */
  if( pNode && rc==SQLITE_OK ){
    if( NCELL(pNode)>((pRtree->iNodeSize-4)/pRtree->nBytesPerCell) ){
      rc = SQLITE_CORRUPT_VTAB;
    }
  }

  if( rc==SQLITE_OK ){
    if( pNode!=0 ){
      nodeHashInsert(pRtree, pNode);
    }else{
      rc = SQLITE_CORRUPT_VTAB;
    }
    *ppNode = pNode;
  }else{
    sqlite3_free(pNode);
    *ppNode = 0;
  }

  return rc;
}

/*
** Overwrite cell iCell of node pNode with the contents of pCell.
*/
static void nodeOverwriteCell(
  Rtree *pRtree, 
  RtreeNode *pNode,  
  RtreeCell *pCell, 
  int iCell
){
  int ii;
  u8 *p = &pNode->zData[4 + pRtree->nBytesPerCell*iCell];
  p += writeInt64(p, pCell->iRowid);
  for(ii=0; ii<(pRtree->nDim*2); ii++){
    p += writeCoord(p, &pCell->aCoord[ii]);
  }
  pNode->isDirty = 1;
}

/*
** Remove cell the cell with index iCell from node pNode.
*/
static void nodeDeleteCell(Rtree *pRtree, RtreeNode *pNode, int iCell){
  u8 *pDst = &pNode->zData[4 + pRtree->nBytesPerCell*iCell];
  u8 *pSrc = &pDst[pRtree->nBytesPerCell];
  int nByte = (NCELL(pNode) - iCell - 1) * pRtree->nBytesPerCell;
  memmove(pDst, pSrc, nByte);
  writeInt16(&pNode->zData[2], NCELL(pNode)-1);
  pNode->isDirty = 1;
}

/*
** Insert the contents of cell pCell into node pNode. If the insert
** is successful, return SQLITE_OK.
**
** If there is not enough free space in pNode, return SQLITE_FULL.
*/
static int
nodeInsertCell(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  RtreeCell *pCell 
){
  int nCell;                    /* Current number of cells in pNode */
  int nMaxCell;                 /* Maximum number of cells for pNode */

  nMaxCell = (pRtree->iNodeSize-4)/pRtree->nBytesPerCell;
  nCell = NCELL(pNode);

  assert( nCell<=nMaxCell );
  if( nCell<nMaxCell ){
    nodeOverwriteCell(pRtree, pNode, pCell, nCell);
    writeInt16(&pNode->zData[2], nCell+1);
    pNode->isDirty = 1;
  }

  return (nCell==nMaxCell);
}

/*
** If the node is dirty, write it out to the database.
*/
static int
nodeWrite(Rtree *pRtree, RtreeNode *pNode){
  int rc = SQLITE_OK;
  if( pNode->isDirty ){
    sqlite3_stmt *p = pRtree->pWriteNode;
    if( pNode->iNode ){
      sqlite3_bind_int64(p, 1, pNode->iNode);
    }else{
      sqlite3_bind_null(p, 1);
    }
    sqlite3_bind_blob(p, 2, pNode->zData, pRtree->iNodeSize, SQLITE_STATIC);
    sqlite3_step(p);
    pNode->isDirty = 0;
    rc = sqlite3_reset(p);
    if( pNode->iNode==0 && rc==SQLITE_OK ){
      pNode->iNode = sqlite3_last_insert_rowid(pRtree->db);
      nodeHashInsert(pRtree, pNode);
    }
  }
  return rc;
}

/*
** Release a reference to a node. If the node is dirty and the reference
** count drops to zero, the node data is written to the database.
*/
static int
nodeRelease(Rtree *pRtree, RtreeNode *pNode){
  int rc = SQLITE_OK;
  if( pNode ){
    assert( pNode->nRef>0 );
    pNode->nRef--;
    if( pNode->nRef==0 ){
      if( pNode->iNode==1 ){
        pRtree->iDepth = -1;
      }
      if( pNode->pParent ){
        rc = nodeRelease(pRtree, pNode->pParent);
      }
      if( rc==SQLITE_OK ){
        rc = nodeWrite(pRtree, pNode);
      }
      nodeHashDelete(pRtree, pNode);
      sqlite3_free(pNode);
    }
  }
  return rc;
}

/*
** Return the 64-bit integer value associated with cell iCell of
** node pNode. If pNode is a leaf node, this is a rowid. If it is
** an internal node, then the 64-bit integer is a child page number.
*/
static i64 nodeGetRowid(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  int iCell
){
  assert( iCell<NCELL(pNode) );
  return readInt64(&pNode->zData[4 + pRtree->nBytesPerCell*iCell]);
}

/*
** Return coordinate iCoord from cell iCell in node pNode.
*/
static void nodeGetCoord(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  int iCell,
  int iCoord,
  RtreeCoord *pCoord           /* Space to write result to */
){
  readCoord(&pNode->zData[12 + pRtree->nBytesPerCell*iCell + 4*iCoord], pCoord);
}

/*
** Deserialize cell iCell of node pNode. Populate the structure pointed
** to by pCell with the results.
*/
static void nodeGetCell(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  int iCell,
  RtreeCell *pCell
){
  int ii;
  pCell->iRowid = nodeGetRowid(pRtree, pNode, iCell);
  for(ii=0; ii<pRtree->nDim*2; ii++){
    nodeGetCoord(pRtree, pNode, iCell, ii, &pCell->aCoord[ii]);
  }
}


/* Forward declaration for the function that does the work of
** the virtual table module xCreate() and xConnect() methods.
*/
static int rtreeInit(
  sqlite3 *, void *, int, const char *const*, sqlite3_vtab **, char **, int
);

/* 
** Rtree virtual table module xCreate method.
*/
static int rtreeCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return rtreeInit(db, pAux, argc, argv, ppVtab, pzErr, 1);
}

/* 
** Rtree virtual table module xConnect method.
*/
static int rtreeConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return rtreeInit(db, pAux, argc, argv, ppVtab, pzErr, 0);
}

/*
** Increment the r-tree reference count.
*/
static void rtreeReference(Rtree *pRtree){
  pRtree->nBusy++;
}

/*
** Decrement the r-tree reference count. When the reference count reaches
** zero the structure is deleted.
*/
static void rtreeRelease(Rtree *pRtree){
  pRtree->nBusy--;
  if( pRtree->nBusy==0 ){
    sqlite3_finalize(pRtree->pReadNode);
    sqlite3_finalize(pRtree->pWriteNode);
    sqlite3_finalize(pRtree->pDeleteNode);
    sqlite3_finalize(pRtree->pReadRowid);
    sqlite3_finalize(pRtree->pWriteRowid);
    sqlite3_finalize(pRtree->pDeleteRowid);
    sqlite3_finalize(pRtree->pReadParent);
    sqlite3_finalize(pRtree->pWriteParent);
    sqlite3_finalize(pRtree->pDeleteParent);
    sqlite3_free(pRtree);
  }
}

/* 
** Rtree virtual table module xDisconnect method.
*/
static int rtreeDisconnect(sqlite3_vtab *pVtab){
  rtreeRelease((Rtree *)pVtab);
  return SQLITE_OK;
}

/* 
** Rtree virtual table module xDestroy method.
*/
static int rtreeDestroy(sqlite3_vtab *pVtab){
  Rtree *pRtree = (Rtree *)pVtab;
  int rc;
  char *zCreate = sqlite3_mprintf(
    "DROP TABLE '%q'.'%q_node';"
    "DROP TABLE '%q'.'%q_rowid';"
    "DROP TABLE '%q'.'%q_parent';",
    pRtree->zDb, pRtree->zName, 
    pRtree->zDb, pRtree->zName,
    pRtree->zDb, pRtree->zName
  );
  if( !zCreate ){
    rc = SQLITE_NOMEM;
  }else{
    rc = sqlite3_exec(pRtree->db, zCreate, 0, 0, 0);
    sqlite3_free(zCreate);
  }
  if( rc==SQLITE_OK ){
    rtreeRelease(pRtree);
  }

  return rc;
}

/* 
** Rtree virtual table module xOpen method.
*/
static int rtreeOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  int rc = SQLITE_NOMEM;
  RtreeCursor *pCsr;

  pCsr = (RtreeCursor *)sqlite3_malloc(sizeof(RtreeCursor));
  if( pCsr ){
    memset(pCsr, 0, sizeof(RtreeCursor));
    pCsr->base.pVtab = pVTab;
    rc = SQLITE_OK;
  }
  *ppCursor = (sqlite3_vtab_cursor *)pCsr;

  return rc;
}


/*
** Free the RtreeCursor.aConstraint[] array and its contents.
*/
static void freeCursorConstraints(RtreeCursor *pCsr){
  if( pCsr->aConstraint ){
    int i;                        /* Used to iterate through constraint array */
    for(i=0; i<pCsr->nConstraint; i++){
      sqlite3_rtree_geometry *pGeom = pCsr->aConstraint[i].pGeom;
      if( pGeom ){
        if( pGeom->xDelUser ) pGeom->xDelUser(pGeom->pUser);
        sqlite3_free(pGeom);
      }
    }
    sqlite3_free(pCsr->aConstraint);
    pCsr->aConstraint = 0;
  }
}

/* 
** Rtree virtual table module xClose method.
*/
static int rtreeClose(sqlite3_vtab_cursor *cur){
  Rtree *pRtree = (Rtree *)(cur->pVtab);
  int rc;
  RtreeCursor *pCsr = (RtreeCursor *)cur;
  freeCursorConstraints(pCsr);
  rc = nodeRelease(pRtree, pCsr->pNode);
  sqlite3_free(pCsr);
  return rc;
}

/*
** Rtree virtual table module xEof method.
**
** Return non-zero if the cursor does not currently point to a valid 
** record (i.e if the scan has finished), or zero otherwise.
*/
static int rtreeEof(sqlite3_vtab_cursor *cur){
  RtreeCursor *pCsr = (RtreeCursor *)cur;
  return (pCsr->pNode==0);
}

/*
** The r-tree constraint passed as the second argument to this function is
** guaranteed to be a MATCH constraint.
*/
static int testRtreeGeom(
  Rtree *pRtree,                  /* R-Tree object */
  RtreeConstraint *pConstraint,   /* MATCH constraint to test */
  RtreeCell *pCell,               /* Cell to test */
  int *pbRes                      /* OUT: Test result */
){
  int i;
  RtreeDValue aCoord[RTREE_MAX_DIMENSIONS*2];
  int nCoord = pRtree->nDim*2;

  assert( pConstraint->op==RTREE_MATCH );
  assert( pConstraint->pGeom );

  for(i=0; i<nCoord; i++){
    aCoord[i] = DCOORD(pCell->aCoord[i]);
  }
  return pConstraint->xGeom(pConstraint->pGeom, nCoord, aCoord, pbRes);
}

/* 
** Cursor pCursor currently points to a cell in a non-leaf page.
** Set *pbEof to true if the sub-tree headed by the cell is filtered
** (excluded) by the constraints in the pCursor->aConstraint[] 
** array, or false otherwise.
**
** Return SQLITE_OK if successful or an SQLite error code if an error
** occurs within a geometry callback.
*/
static int testRtreeCell(Rtree *pRtree, RtreeCursor *pCursor, int *pbEof){
  RtreeCell cell;
  int ii;
  int bRes = 0;
  int rc = SQLITE_OK;

  nodeGetCell(pRtree, pCursor->pNode, pCursor->iCell, &cell);
  for(ii=0; bRes==0 && ii<pCursor->nConstraint; ii++){
    RtreeConstraint *p = &pCursor->aConstraint[ii];
    RtreeDValue cell_min = DCOORD(cell.aCoord[(p->iCoord>>1)*2]);
    RtreeDValue cell_max = DCOORD(cell.aCoord[(p->iCoord>>1)*2+1]);

    assert(p->op==RTREE_LE || p->op==RTREE_LT || p->op==RTREE_GE 
        || p->op==RTREE_GT || p->op==RTREE_EQ || p->op==RTREE_MATCH
    );

    switch( p->op ){
      case RTREE_LE: case RTREE_LT: 
        bRes = p->rValue<cell_min; 
        break;

      case RTREE_GE: case RTREE_GT: 
        bRes = p->rValue>cell_max; 
        break;

      case RTREE_EQ:
        bRes = (p->rValue>cell_max || p->rValue<cell_min);
        break;

      default: {
        assert( p->op==RTREE_MATCH );
        rc = testRtreeGeom(pRtree, p, &cell, &bRes);
        bRes = !bRes;
        break;
      }
    }
  }

  *pbEof = bRes;
  return rc;
}

/* 
** Test if the cell that cursor pCursor currently points to
** would be filtered (excluded) by the constraints in the 
** pCursor->aConstraint[] array. If so, set *pbEof to true before
** returning. If the cell is not filtered (excluded) by the constraints,
** set pbEof to zero.
**
** Return SQLITE_OK if successful or an SQLite error code if an error
** occurs within a geometry callback.
**
** This function assumes that the cell is part of a leaf node.
*/
static int testRtreeEntry(Rtree *pRtree, RtreeCursor *pCursor, int *pbEof){
  RtreeCell cell;
  int ii;
  *pbEof = 0;

  nodeGetCell(pRtree, pCursor->pNode, pCursor->iCell, &cell);
  for(ii=0; ii<pCursor->nConstraint; ii++){
    RtreeConstraint *p = &pCursor->aConstraint[ii];
    RtreeDValue coord = DCOORD(cell.aCoord[p->iCoord]);
    int res;
    assert(p->op==RTREE_LE || p->op==RTREE_LT || p->op==RTREE_GE 
        || p->op==RTREE_GT || p->op==RTREE_EQ || p->op==RTREE_MATCH
    );
    switch( p->op ){
      case RTREE_LE: res = (coord<=p->rValue); break;
      case RTREE_LT: res = (coord<p->rValue);  break;
      case RTREE_GE: res = (coord>=p->rValue); break;
      case RTREE_GT: res = (coord>p->rValue);  break;
      case RTREE_EQ: res = (coord==p->rValue); break;
      default: {
        int rc;
        assert( p->op==RTREE_MATCH );
        rc = testRtreeGeom(pRtree, p, &cell, &res);
        if( rc!=SQLITE_OK ){
          return rc;
        }
        break;
      }
    }

    if( !res ){
      *pbEof = 1;
      return SQLITE_OK;
    }
  }

  return SQLITE_OK;
}

/*
** Cursor pCursor currently points at a node that heads a sub-tree of
** height iHeight (if iHeight==0, then the node is a leaf). Descend
** to point to the left-most cell of the sub-tree that matches the 
** configured constraints.
*/
static int descendToCell(
  Rtree *pRtree, 
  RtreeCursor *pCursor, 
  int iHeight,
  int *pEof                 /* OUT: Set to true if cannot descend */
){
  int isEof;
  int rc;
  int ii;
  RtreeNode *pChild;
  sqlite3_int64 iRowid;

  RtreeNode *pSavedNode = pCursor->pNode;
  int iSavedCell = pCursor->iCell;

  assert( iHeight>=0 );

  if( iHeight==0 ){
    rc = testRtreeEntry(pRtree, pCursor, &isEof);
  }else{
    rc = testRtreeCell(pRtree, pCursor, &isEof);
  }
  if( rc!=SQLITE_OK || isEof || iHeight==0 ){
    goto descend_to_cell_out;
  }

  iRowid = nodeGetRowid(pRtree, pCursor->pNode, pCursor->iCell);
  rc = nodeAcquire(pRtree, iRowid, pCursor->pNode, &pChild);
  if( rc!=SQLITE_OK ){
    goto descend_to_cell_out;
  }

  nodeRelease(pRtree, pCursor->pNode);
  pCursor->pNode = pChild;
  isEof = 1;
  for(ii=0; isEof && ii<NCELL(pChild); ii++){
    pCursor->iCell = ii;
    rc = descendToCell(pRtree, pCursor, iHeight-1, &isEof);
    if( rc!=SQLITE_OK ){
      goto descend_to_cell_out;
    }
  }

  if( isEof ){
    assert( pCursor->pNode==pChild );
    nodeReference(pSavedNode);
    nodeRelease(pRtree, pChild);
    pCursor->pNode = pSavedNode;
    pCursor->iCell = iSavedCell;
  }

descend_to_cell_out:
  *pEof = isEof;
  return rc;
}

/*
** One of the cells in node pNode is guaranteed to have a 64-bit 
** integer value equal to iRowid. Return the index of this cell.
*/
static int nodeRowidIndex(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  i64 iRowid,
  int *piIndex
){
  int ii;
  int nCell = NCELL(pNode);
  for(ii=0; ii<nCell; ii++){
    if( nodeGetRowid(pRtree, pNode, ii)==iRowid ){
      *piIndex = ii;
      return SQLITE_OK;
    }
  }
  return SQLITE_CORRUPT_VTAB;
}

/*
** Return the index of the cell containing a pointer to node pNode
** in its parent. If pNode is the root node, return -1.
*/
static int nodeParentIndex(Rtree *pRtree, RtreeNode *pNode, int *piIndex){
  RtreeNode *pParent = pNode->pParent;
  if( pParent ){
    return nodeRowidIndex(pRtree, pParent, pNode->iNode, piIndex);
  }
  *piIndex = -1;
  return SQLITE_OK;
}

/* 
** Rtree virtual table module xNext method.
*/
static int rtreeNext(sqlite3_vtab_cursor *pVtabCursor){
  Rtree *pRtree = (Rtree *)(pVtabCursor->pVtab);
  RtreeCursor *pCsr = (RtreeCursor *)pVtabCursor;
  int rc = SQLITE_OK;

  /* RtreeCursor.pNode must not be NULL. If is is NULL, then this cursor is
  ** already at EOF. It is against the rules to call the xNext() method of
  ** a cursor that has already reached EOF.
  */
  assert( pCsr->pNode );

  if( pCsr->iStrategy==1 ){
    /* This "scan" is a direct lookup by rowid. There is no next entry. */
    nodeRelease(pRtree, pCsr->pNode);
    pCsr->pNode = 0;
  }else{
    /* Move to the next entry that matches the configured constraints. */
    int iHeight = 0;
    while( pCsr->pNode ){
      RtreeNode *pNode = pCsr->pNode;
      int nCell = NCELL(pNode);
      for(pCsr->iCell++; pCsr->iCell<nCell; pCsr->iCell++){
        int isEof;
        rc = descendToCell(pRtree, pCsr, iHeight, &isEof);
        if( rc!=SQLITE_OK || !isEof ){
          return rc;
        }
      }
      pCsr->pNode = pNode->pParent;
      rc = nodeParentIndex(pRtree, pNode, &pCsr->iCell);
      if( rc!=SQLITE_OK ){
        return rc;
      }
      nodeReference(pCsr->pNode);
      nodeRelease(pRtree, pNode);
      iHeight++;
    }
  }

  return rc;
}

/* 
** Rtree virtual table module xRowid method.
*/
static int rtreeRowid(sqlite3_vtab_cursor *pVtabCursor, sqlite_int64 *pRowid){
  Rtree *pRtree = (Rtree *)pVtabCursor->pVtab;
  RtreeCursor *pCsr = (RtreeCursor *)pVtabCursor;

  assert(pCsr->pNode);
  *pRowid = nodeGetRowid(pRtree, pCsr->pNode, pCsr->iCell);

  return SQLITE_OK;
}

/* 
** Rtree virtual table module xColumn method.
*/
static int rtreeColumn(sqlite3_vtab_cursor *cur, sqlite3_context *ctx, int i){
  Rtree *pRtree = (Rtree *)cur->pVtab;
  RtreeCursor *pCsr = (RtreeCursor *)cur;

  if( i==0 ){
    i64 iRowid = nodeGetRowid(pRtree, pCsr->pNode, pCsr->iCell);
    sqlite3_result_int64(ctx, iRowid);
  }else{
    RtreeCoord c;
    nodeGetCoord(pRtree, pCsr->pNode, pCsr->iCell, i-1, &c);
#ifndef SQLITE_RTREE_INT_ONLY
    if( pRtree->eCoordType==RTREE_COORD_REAL32 ){
      sqlite3_result_double(ctx, c.f);
    }else
#endif
    {
      assert( pRtree->eCoordType==RTREE_COORD_INT32 );
      sqlite3_result_int(ctx, c.i);
    }
  }

  return SQLITE_OK;
}

/* 
** Use nodeAcquire() to obtain the leaf node containing the record with 
** rowid iRowid. If successful, set *ppLeaf to point to the node and
** return SQLITE_OK. If there is no such record in the table, set
** *ppLeaf to 0 and return SQLITE_OK. If an error occurs, set *ppLeaf
** to zero and return an SQLite error code.
*/
static int findLeafNode(Rtree *pRtree, i64 iRowid, RtreeNode **ppLeaf){
  int rc;
  *ppLeaf = 0;
  sqlite3_bind_int64(pRtree->pReadRowid, 1, iRowid);
  if( sqlite3_step(pRtree->pReadRowid)==SQLITE_ROW ){
    i64 iNode = sqlite3_column_int64(pRtree->pReadRowid, 0);
    rc = nodeAcquire(pRtree, iNode, 0, ppLeaf);
    sqlite3_reset(pRtree->pReadRowid);
  }else{
    rc = sqlite3_reset(pRtree->pReadRowid);
  }
  return rc;
}

/*
** This function is called to configure the RtreeConstraint object passed
** as the second argument for a MATCH constraint. The value passed as the
** first argument to this function is the right-hand operand to the MATCH
** operator.
*/
static int deserializeGeometry(sqlite3_value *pValue, RtreeConstraint *pCons){
  RtreeMatchArg *p;
  sqlite3_rtree_geometry *pGeom;
  int nBlob;

  /* Check that value is actually a blob. */
  if( sqlite3_value_type(pValue)!=SQLITE_BLOB ) return SQLITE_ERROR;

  /* Check that the blob is roughly the right size. */
  nBlob = sqlite3_value_bytes(pValue);
  if( nBlob<(int)sizeof(RtreeMatchArg) 
   || ((nBlob-sizeof(RtreeMatchArg))%sizeof(RtreeDValue))!=0
  ){
    return SQLITE_ERROR;
  }

  pGeom = (sqlite3_rtree_geometry *)sqlite3_malloc(
      sizeof(sqlite3_rtree_geometry) + nBlob
  );
  if( !pGeom ) return SQLITE_NOMEM;
  memset(pGeom, 0, sizeof(sqlite3_rtree_geometry));
  p = (RtreeMatchArg *)&pGeom[1];

  memcpy(p, sqlite3_value_blob(pValue), nBlob);
  if( p->magic!=RTREE_GEOMETRY_MAGIC 
   || nBlob!=(int)(sizeof(RtreeMatchArg) + (p->nParam-1)*sizeof(RtreeDValue))
  ){
    sqlite3_free(pGeom);
    return SQLITE_ERROR;
  }

  pGeom->pContext = p->pContext;
  pGeom->nParam = p->nParam;
  pGeom->aParam = p->aParam;

  pCons->xGeom = p->xGeom;
  pCons->pGeom = pGeom;
  return SQLITE_OK;
}

/* 
** Rtree virtual table module xFilter method.
*/
static int rtreeFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  Rtree *pRtree = (Rtree *)pVtabCursor->pVtab;
  RtreeCursor *pCsr = (RtreeCursor *)pVtabCursor;

  RtreeNode *pRoot = 0;
  int ii;
  int rc = SQLITE_OK;

  rtreeReference(pRtree);

  freeCursorConstraints(pCsr);
  pCsr->iStrategy = idxNum;

  if( idxNum==1 ){
    /* Special case - lookup by rowid. */
    RtreeNode *pLeaf;        /* Leaf on which the required cell resides */
    i64 iRowid = sqlite3_value_int64(argv[0]);
    rc = findLeafNode(pRtree, iRowid, &pLeaf);
    pCsr->pNode = pLeaf; 
    if( pLeaf ){
      assert( rc==SQLITE_OK );
      rc = nodeRowidIndex(pRtree, pLeaf, iRowid, &pCsr->iCell);
    }
  }else{
    /* Normal case - r-tree scan. Set up the RtreeCursor.aConstraint array 
    ** with the configured constraints. 
    */
    if( argc>0 ){
      pCsr->aConstraint = sqlite3_malloc(sizeof(RtreeConstraint)*argc);
      pCsr->nConstraint = argc;
      if( !pCsr->aConstraint ){
        rc = SQLITE_NOMEM;
      }else{
        memset(pCsr->aConstraint, 0, sizeof(RtreeConstraint)*argc);
        assert( (idxStr==0 && argc==0)
                || (idxStr && (int)strlen(idxStr)==argc*2) );
        for(ii=0; ii<argc; ii++){
          RtreeConstraint *p = &pCsr->aConstraint[ii];
          p->op = idxStr[ii*2];
          p->iCoord = idxStr[ii*2+1]-'a';
          if( p->op==RTREE_MATCH ){
            /* A MATCH operator. The right-hand-side must be a blob that
            ** can be cast into an RtreeMatchArg object. One created using
            ** an sqlite3_rtree_geometry_callback() SQL user function.
            */
            rc = deserializeGeometry(argv[ii], p);
            if( rc!=SQLITE_OK ){
              break;
            }
          }else{
#ifdef SQLITE_RTREE_INT_ONLY
            p->rValue = sqlite3_value_int64(argv[ii]);
#else
            p->rValue = sqlite3_value_double(argv[ii]);
#endif
          }
        }
      }
    }
  
    if( rc==SQLITE_OK ){
      pCsr->pNode = 0;
      rc = nodeAcquire(pRtree, 1, 0, &pRoot);
    }
    if( rc==SQLITE_OK ){
      int isEof = 1;
      int nCell = NCELL(pRoot);
      pCsr->pNode = pRoot;
      for(pCsr->iCell=0; rc==SQLITE_OK && pCsr->iCell<nCell; pCsr->iCell++){
        assert( pCsr->pNode==pRoot );
        rc = descendToCell(pRtree, pCsr, pRtree->iDepth, &isEof);
        if( !isEof ){
          break;
        }
      }
      if( rc==SQLITE_OK && isEof ){
        assert( pCsr->pNode==pRoot );
        nodeRelease(pRtree, pRoot);
        pCsr->pNode = 0;
      }
      assert( rc!=SQLITE_OK || !pCsr->pNode || pCsr->iCell<NCELL(pCsr->pNode) );
    }
  }

  rtreeRelease(pRtree);
  return rc;
}

/*
** Set the pIdxInfo->estimatedRows variable to nRow. Unless this
** extension is currently being used by a version of SQLite too old to
** support estimatedRows. In that case this function is a no-op.
*/
static void setEstimatedRows(sqlite3_index_info *pIdxInfo, i64 nRow){
#if SQLITE_VERSION_NUMBER>=3008002
  if( sqlite3_libversion_number()>=3008002 ){
    pIdxInfo->estimatedRows = nRow;
  }
#endif
}

/*
** Rtree virtual table module xBestIndex method. There are three
** table scan strategies to choose from (in order from most to 
** least desirable):
**
**   idxNum     idxStr        Strategy
**   ------------------------------------------------
**     1        Unused        Direct lookup by rowid.
**     2        See below     R-tree query or full-table scan.
**   ------------------------------------------------
**
** If strategy 1 is used, then idxStr is not meaningful. If strategy
** 2 is used, idxStr is formatted to contain 2 bytes for each 
** constraint used. The first two bytes of idxStr correspond to 
** the constraint in sqlite3_index_info.aConstraintUsage[] with
** (argvIndex==1) etc.
**
** The first of each pair of bytes in idxStr identifies the constraint
** operator as follows:
**
**   Operator    Byte Value
**   ----------------------
**      =        0x41 ('A')
**     <=        0x42 ('B')
**      <        0x43 ('C')
**     >=        0x44 ('D')
**      >        0x45 ('E')
**   MATCH       0x46 ('F')
**   ----------------------
**
** The second of each pair of bytes identifies the coordinate column
** to which the constraint applies. The leftmost coordinate column
** is 'a', the second from the left 'b' etc.
*/
static int rtreeBestIndex(sqlite3_vtab *tab, sqlite3_index_info *pIdxInfo){
  Rtree *pRtree = (Rtree*)tab;
  int rc = SQLITE_OK;
  int ii;
  i64 nRow;                       /* Estimated rows returned by this scan */

  int iIdx = 0;
  char zIdxStr[RTREE_MAX_DIMENSIONS*8+1];
  memset(zIdxStr, 0, sizeof(zIdxStr));

  assert( pIdxInfo->idxStr==0 );
  for(ii=0; ii<pIdxInfo->nConstraint && iIdx<(int)(sizeof(zIdxStr)-1); ii++){
    struct sqlite3_index_constraint *p = &pIdxInfo->aConstraint[ii];

    if( p->usable && p->iColumn==0 && p->op==SQLITE_INDEX_CONSTRAINT_EQ ){
      /* We have an equality constraint on the rowid. Use strategy 1. */
      int jj;
      for(jj=0; jj<ii; jj++){
        pIdxInfo->aConstraintUsage[jj].argvIndex = 0;
        pIdxInfo->aConstraintUsage[jj].omit = 0;
      }
      pIdxInfo->idxNum = 1;
      pIdxInfo->aConstraintUsage[ii].argvIndex = 1;
      pIdxInfo->aConstraintUsage[jj].omit = 1;

      /* This strategy involves a two rowid lookups on an B-Tree structures
      ** and then a linear search of an R-Tree node. This should be 
      ** considered almost as quick as a direct rowid lookup (for which 
      ** sqlite uses an internal cost of 0.0). It is expected to return
      ** a single row.
      */ 
      pIdxInfo->estimatedCost = 30.0;
      setEstimatedRows(pIdxInfo, 1);
      return SQLITE_OK;
    }

    if( p->usable && (p->iColumn>0 || p->op==SQLITE_INDEX_CONSTRAINT_MATCH) ){
      u8 op;
      switch( p->op ){
        case SQLITE_INDEX_CONSTRAINT_EQ: op = RTREE_EQ; break;
        case SQLITE_INDEX_CONSTRAINT_GT: op = RTREE_GT; break;
        case SQLITE_INDEX_CONSTRAINT_LE: op = RTREE_LE; break;
        case SQLITE_INDEX_CONSTRAINT_LT: op = RTREE_LT; break;
        case SQLITE_INDEX_CONSTRAINT_GE: op = RTREE_GE; break;
        default:
          assert( p->op==SQLITE_INDEX_CONSTRAINT_MATCH );
          op = RTREE_MATCH; 
          break;
      }
      zIdxStr[iIdx++] = op;
      zIdxStr[iIdx++] = p->iColumn - 1 + 'a';
      pIdxInfo->aConstraintUsage[ii].argvIndex = (iIdx/2);
      pIdxInfo->aConstraintUsage[ii].omit = 1;
    }
  }

  pIdxInfo->idxNum = 2;
  pIdxInfo->needToFreeIdxStr = 1;
  if( iIdx>0 && 0==(pIdxInfo->idxStr = sqlite3_mprintf("%s", zIdxStr)) ){
    return SQLITE_NOMEM;
  }

  nRow = pRtree->nRowEst / (iIdx + 1);
  pIdxInfo->estimatedCost = (double)6.0 * (double)nRow;
  setEstimatedRows(pIdxInfo, nRow);

  return rc;
}

/*
** Return the N-dimensional volumn of the cell stored in *p.
*/
static RtreeDValue cellArea(Rtree *pRtree, RtreeCell *p){
  RtreeDValue area = (RtreeDValue)1;
  int ii;
  for(ii=0; ii<(pRtree->nDim*2); ii+=2){
    area = (area * (DCOORD(p->aCoord[ii+1]) - DCOORD(p->aCoord[ii])));
  }
  return area;
}

/*
** Return the margin length of cell p. The margin length is the sum
** of the objects size in each dimension.
*/
static RtreeDValue cellMargin(Rtree *pRtree, RtreeCell *p){
  RtreeDValue margin = (RtreeDValue)0;
  int ii;
  for(ii=0; ii<(pRtree->nDim*2); ii+=2){
    margin += (DCOORD(p->aCoord[ii+1]) - DCOORD(p->aCoord[ii]));
  }
  return margin;
}

/*
** Store the union of cells p1 and p2 in p1.
*/
static void cellUnion(Rtree *pRtree, RtreeCell *p1, RtreeCell *p2){
  int ii;
  if( pRtree->eCoordType==RTREE_COORD_REAL32 ){
    for(ii=0; ii<(pRtree->nDim*2); ii+=2){
      p1->aCoord[ii].f = MIN(p1->aCoord[ii].f, p2->aCoord[ii].f);
      p1->aCoord[ii+1].f = MAX(p1->aCoord[ii+1].f, p2->aCoord[ii+1].f);
    }
  }else{
    for(ii=0; ii<(pRtree->nDim*2); ii+=2){
      p1->aCoord[ii].i = MIN(p1->aCoord[ii].i, p2->aCoord[ii].i);
      p1->aCoord[ii+1].i = MAX(p1->aCoord[ii+1].i, p2->aCoord[ii+1].i);
    }
  }
}

/*
** Return true if the area covered by p2 is a subset of the area covered
** by p1. False otherwise.
*/
static int cellContains(Rtree *pRtree, RtreeCell *p1, RtreeCell *p2){
  int ii;
  int isInt = (pRtree->eCoordType==RTREE_COORD_INT32);
  for(ii=0; ii<(pRtree->nDim*2); ii+=2){
    RtreeCoord *a1 = &p1->aCoord[ii];
    RtreeCoord *a2 = &p2->aCoord[ii];
    if( (!isInt && (a2[0].f<a1[0].f || a2[1].f>a1[1].f)) 
     || ( isInt && (a2[0].i<a1[0].i || a2[1].i>a1[1].i)) 
    ){
      return 0;
    }
  }
  return 1;
}

/*
** Return the amount cell p would grow by if it were unioned with pCell.
*/
static RtreeDValue cellGrowth(Rtree *pRtree, RtreeCell *p, RtreeCell *pCell){
  RtreeDValue area;
  RtreeCell cell;
  memcpy(&cell, p, sizeof(RtreeCell));
  area = cellArea(pRtree, &cell);
  cellUnion(pRtree, &cell, pCell);
  return (cellArea(pRtree, &cell)-area);
}

#if VARIANT_RSTARTREE_CHOOSESUBTREE || VARIANT_RSTARTREE_SPLIT
static RtreeDValue cellOverlap(
  Rtree *pRtree, 
  RtreeCell *p, 
  RtreeCell *aCell, 
  int nCell, 
  int iExclude
){
  int ii;
  RtreeDValue overlap = 0.0;
  for(ii=0; ii<nCell; ii++){
#if VARIANT_RSTARTREE_CHOOSESUBTREE
    if( ii!=iExclude )
#else
    assert( iExclude==-1 );
    UNUSED_PARAMETER(iExclude);
#endif
    {
      int jj;
      RtreeDValue o = (RtreeDValue)1;
      for(jj=0; jj<(pRtree->nDim*2); jj+=2){
        RtreeDValue x1, x2;

        x1 = MAX(DCOORD(p->aCoord[jj]), DCOORD(aCell[ii].aCoord[jj]));
        x2 = MIN(DCOORD(p->aCoord[jj+1]), DCOORD(aCell[ii].aCoord[jj+1]));

        if( x2<x1 ){
          o = 0.0;
          break;
        }else{
          o = o * (x2-x1);
        }
      }
      overlap += o;
    }
  }
  return overlap;
}
#endif

#if VARIANT_RSTARTREE_CHOOSESUBTREE
static RtreeDValue cellOverlapEnlargement(
  Rtree *pRtree, 
  RtreeCell *p, 
  RtreeCell *pInsert, 
  RtreeCell *aCell, 
  int nCell, 
  int iExclude
){
  RtreeDValue before, after;
  before = cellOverlap(pRtree, p, aCell, nCell, iExclude);
  cellUnion(pRtree, p, pInsert);
  after = cellOverlap(pRtree, p, aCell, nCell, iExclude);
  return (after-before);
}
#endif


/*
** This function implements the ChooseLeaf algorithm from Gutman[84].
** ChooseSubTree in r*tree terminology.
*/
static int ChooseLeaf(
  Rtree *pRtree,               /* Rtree table */
  RtreeCell *pCell,            /* Cell to insert into rtree */
  int iHeight,                 /* Height of sub-tree rooted at pCell */
  RtreeNode **ppLeaf           /* OUT: Selected leaf page */
){
  int rc;
  int ii;
  RtreeNode *pNode;
  rc = nodeAcquire(pRtree, 1, 0, &pNode);

  for(ii=0; rc==SQLITE_OK && ii<(pRtree->iDepth-iHeight); ii++){
    int iCell;
    sqlite3_int64 iBest = 0;

    RtreeDValue fMinGrowth = 0.0;
    RtreeDValue fMinArea = 0.0;
#if VARIANT_RSTARTREE_CHOOSESUBTREE
    RtreeDValue fMinOverlap = 0.0;
    RtreeDValue overlap;
#endif

    int nCell = NCELL(pNode);
    RtreeCell cell;
    RtreeNode *pChild;

    RtreeCell *aCell = 0;

#if VARIANT_RSTARTREE_CHOOSESUBTREE
    if( ii==(pRtree->iDepth-1) ){
      int jj;
      aCell = sqlite3_malloc(sizeof(RtreeCell)*nCell);
      if( !aCell ){
        rc = SQLITE_NOMEM;
        nodeRelease(pRtree, pNode);
        pNode = 0;
        continue;
      }
      for(jj=0; jj<nCell; jj++){
        nodeGetCell(pRtree, pNode, jj, &aCell[jj]);
      }
    }
#endif

    /* Select the child node which will be enlarged the least if pCell
    ** is inserted into it. Resolve ties by choosing the entry with
    ** the smallest area.
    */
    for(iCell=0; iCell<nCell; iCell++){
      int bBest = 0;
      RtreeDValue growth;
      RtreeDValue area;
      nodeGetCell(pRtree, pNode, iCell, &cell);
      growth = cellGrowth(pRtree, &cell, pCell);
      area = cellArea(pRtree, &cell);

#if VARIANT_RSTARTREE_CHOOSESUBTREE
      if( ii==(pRtree->iDepth-1) ){
        overlap = cellOverlapEnlargement(pRtree,&cell,pCell,aCell,nCell,iCell);
      }else{
        overlap = 0.0;
      }
      if( (iCell==0) 
       || (overlap<fMinOverlap) 
       || (overlap==fMinOverlap && growth<fMinGrowth)
       || (overlap==fMinOverlap && growth==fMinGrowth && area<fMinArea)
      ){
        bBest = 1;
        fMinOverlap = overlap;
      }
#else
      if( iCell==0||growth<fMinGrowth||(growth==fMinGrowth && area<fMinArea) ){
        bBest = 1;
      }
#endif
      if( bBest ){
        fMinGrowth = growth;
        fMinArea = area;
        iBest = cell.iRowid;
      }
    }

    sqlite3_free(aCell);
    rc = nodeAcquire(pRtree, iBest, pNode, &pChild);
    nodeRelease(pRtree, pNode);
    pNode = pChild;
  }

  *ppLeaf = pNode;
  return rc;
}

/*
** A cell with the same content as pCell has just been inserted into
** the node pNode. This function updates the bounding box cells in
** all ancestor elements.
*/
static int AdjustTree(
  Rtree *pRtree,                    /* Rtree table */
  RtreeNode *pNode,                 /* Adjust ancestry of this node. */
  RtreeCell *pCell                  /* This cell was just inserted */
){
  RtreeNode *p = pNode;
  while( p->pParent ){
    RtreeNode *pParent = p->pParent;
    RtreeCell cell;
    int iCell;

    if( nodeParentIndex(pRtree, p, &iCell) ){
      return SQLITE_CORRUPT_VTAB;
    }

    nodeGetCell(pRtree, pParent, iCell, &cell);
    if( !cellContains(pRtree, &cell, pCell) ){
      cellUnion(pRtree, &cell, pCell);
      nodeOverwriteCell(pRtree, pParent, &cell, iCell);
    }
 
    p = pParent;
  }
  return SQLITE_OK;
}

/*
** Write mapping (iRowid->iNode) to the <rtree>_rowid table.
*/
static int rowidWrite(Rtree *pRtree, sqlite3_int64 iRowid, sqlite3_int64 iNode){
  sqlite3_bind_int64(pRtree->pWriteRowid, 1, iRowid);
  sqlite3_bind_int64(pRtree->pWriteRowid, 2, iNode);
  sqlite3_step(pRtree->pWriteRowid);
  return sqlite3_reset(pRtree->pWriteRowid);
}

/*
** Write mapping (iNode->iPar) to the <rtree>_parent table.
*/
static int parentWrite(Rtree *pRtree, sqlite3_int64 iNode, sqlite3_int64 iPar){
  sqlite3_bind_int64(pRtree->pWriteParent, 1, iNode);
  sqlite3_bind_int64(pRtree->pWriteParent, 2, iPar);
  sqlite3_step(pRtree->pWriteParent);
  return sqlite3_reset(pRtree->pWriteParent);
}

static int rtreeInsertCell(Rtree *, RtreeNode *, RtreeCell *, int);

#if VARIANT_GUTTMAN_LINEAR_SPLIT
/*
** Implementation of the linear variant of the PickNext() function from
** Guttman[84].
*/
static RtreeCell *LinearPickNext(
  Rtree *pRtree,
  RtreeCell *aCell, 
  int nCell, 
  RtreeCell *pLeftBox, 
  RtreeCell *pRightBox,
  int *aiUsed
){
  int ii;
  for(ii=0; aiUsed[ii]; ii++);
  aiUsed[ii] = 1;
  return &aCell[ii];
}

/*
** Implementation of the linear variant of the PickSeeds() function from
** Guttman[84].
*/
static void LinearPickSeeds(
  Rtree *pRtree,
  RtreeCell *aCell, 
  int nCell, 
  int *piLeftSeed, 
  int *piRightSeed
){
  int i;
  int iLeftSeed = 0;
  int iRightSeed = 1;
  RtreeDValue maxNormalInnerWidth = (RtreeDValue)0;

  /* Pick two "seed" cells from the array of cells. The algorithm used
  ** here is the LinearPickSeeds algorithm from Gutman[1984]. The 
  ** indices of the two seed cells in the array are stored in local
  ** variables iLeftSeek and iRightSeed.
  */
  for(i=0; i<pRtree->nDim; i++){
    RtreeDValue x1 = DCOORD(aCell[0].aCoord[i*2]);
    RtreeDValue x2 = DCOORD(aCell[0].aCoord[i*2+1]);
    RtreeDValue x3 = x1;
    RtreeDValue x4 = x2;
    int jj;

    int iCellLeft = 0;
    int iCellRight = 0;

    for(jj=1; jj<nCell; jj++){
      RtreeDValue left = DCOORD(aCell[jj].aCoord[i*2]);
      RtreeDValue right = DCOORD(aCell[jj].aCoord[i*2+1]);

      if( left<x1 ) x1 = left;
      if( right>x4 ) x4 = right;
      if( left>x3 ){
        x3 = left;
        iCellRight = jj;
      }
      if( right<x2 ){
        x2 = right;
        iCellLeft = jj;
      }
    }

    if( x4!=x1 ){
      RtreeDValue normalwidth = (x3 - x2) / (x4 - x1);
      if( normalwidth>maxNormalInnerWidth ){
        iLeftSeed = iCellLeft;
        iRightSeed = iCellRight;
      }
    }
  }

  *piLeftSeed = iLeftSeed;
  *piRightSeed = iRightSeed;
}
#endif /* VARIANT_GUTTMAN_LINEAR_SPLIT */

#if VARIANT_GUTTMAN_QUADRATIC_SPLIT
/*
** Implementation of the quadratic variant of the PickNext() function from
** Guttman[84].
*/
static RtreeCell *QuadraticPickNext(
  Rtree *pRtree,
  RtreeCell *aCell, 
  int nCell, 
  RtreeCell *pLeftBox, 
  RtreeCell *pRightBox,
  int *aiUsed
){
  #define FABS(a) ((a)<0.0?-1.0*(a):(a))

  int iSelect = -1;
  RtreeDValue fDiff;
  int ii;
  for(ii=0; ii<nCell; ii++){
    if( aiUsed[ii]==0 ){
      RtreeDValue left = cellGrowth(pRtree, pLeftBox, &aCell[ii]);
      RtreeDValue right = cellGrowth(pRtree, pLeftBox, &aCell[ii]);
      RtreeDValue diff = FABS(right-left);
      if( iSelect<0 || diff>fDiff ){
        fDiff = diff;
        iSelect = ii;
      }
    }
  }
  aiUsed[iSelect] = 1;
  return &aCell[iSelect];
}

/*
** Implementation of the quadratic variant of the PickSeeds() function from
** Guttman[84].
*/
static void QuadraticPickSeeds(
  Rtree *pRtree,
  RtreeCell *aCell, 
  int nCell, 
  int *piLeftSeed, 
  int *piRightSeed
){
  int ii;
  int jj;

  int iLeftSeed = 0;
  int iRightSeed = 1;
  RtreeDValue fWaste = 0.0;

  for(ii=0; ii<nCell; ii++){
    for(jj=ii+1; jj<nCell; jj++){
      RtreeDValue right = cellArea(pRtree, &aCell[jj]);
      RtreeDValue growth = cellGrowth(pRtree, &aCell[ii], &aCell[jj]);
      RtreeDValue waste = growth - right;

      if( waste>fWaste ){
        iLeftSeed = ii;
        iRightSeed = jj;
        fWaste = waste;
      }
    }
  }

  *piLeftSeed = iLeftSeed;
  *piRightSeed = iRightSeed;
}
#endif /* VARIANT_GUTTMAN_QUADRATIC_SPLIT */

/*
** Arguments aIdx, aDistance and aSpare all point to arrays of size
** nIdx. The aIdx array contains the set of integers from 0 to 
** (nIdx-1) in no particular order. This function sorts the values
** in aIdx according to the indexed values in aDistance. For
** example, assuming the inputs:
**
**   aIdx      = { 0,   1,   2,   3 }
**   aDistance = { 5.0, 2.0, 7.0, 6.0 }
**
** this function sets the aIdx array to contain:
**
**   aIdx      = { 0,   1,   2,   3 }
**
** The aSpare array is used as temporary working space by the
** sorting algorithm.
*/
static void SortByDistance(
  int *aIdx, 
  int nIdx, 
  RtreeDValue *aDistance, 
  int *aSpare
){
  if( nIdx>1 ){
    int iLeft = 0;
    int iRight = 0;

    int nLeft = nIdx/2;
    int nRight = nIdx-nLeft;
    int *aLeft = aIdx;
    int *aRight = &aIdx[nLeft];

    SortByDistance(aLeft, nLeft, aDistance, aSpare);
    SortByDistance(aRight, nRight, aDistance, aSpare);

    memcpy(aSpare, aLeft, sizeof(int)*nLeft);
    aLeft = aSpare;

    while( iLeft<nLeft || iRight<nRight ){
      if( iLeft==nLeft ){
        aIdx[iLeft+iRight] = aRight[iRight];
        iRight++;
      }else if( iRight==nRight ){
        aIdx[iLeft+iRight] = aLeft[iLeft];
        iLeft++;
      }else{
        RtreeDValue fLeft = aDistance[aLeft[iLeft]];
        RtreeDValue fRight = aDistance[aRight[iRight]];
        if( fLeft<fRight ){
          aIdx[iLeft+iRight] = aLeft[iLeft];
          iLeft++;
        }else{
          aIdx[iLeft+iRight] = aRight[iRight];
          iRight++;
        }
      }
    }

#if 0
    /* Check that the sort worked */
    {
      int jj;
      for(jj=1; jj<nIdx; jj++){
        RtreeDValue left = aDistance[aIdx[jj-1]];
        RtreeDValue right = aDistance[aIdx[jj]];
        assert( left<=right );
      }
    }
#endif
  }
}

/*
** Arguments aIdx, aCell and aSpare all point to arrays of size
** nIdx. The aIdx array contains the set of integers from 0 to 
** (nIdx-1) in no particular order. This function sorts the values
** in aIdx according to dimension iDim of the cells in aCell. The
** minimum value of dimension iDim is considered first, the
** maximum used to break ties.
**
** The aSpare array is used as temporary working space by the
** sorting algorithm.
*/
static void SortByDimension(
  Rtree *pRtree,
  int *aIdx, 
  int nIdx, 
  int iDim, 
  RtreeCell *aCell, 
  int *aSpare
){
  if( nIdx>1 ){

    int iLeft = 0;
    int iRight = 0;

    int nLeft = nIdx/2;
    int nRight = nIdx-nLeft;
    int *aLeft = aIdx;
    int *aRight = &aIdx[nLeft];

    SortByDimension(pRtree, aLeft, nLeft, iDim, aCell, aSpare);
    SortByDimension(pRtree, aRight, nRight, iDim, aCell, aSpare);

    memcpy(aSpare, aLeft, sizeof(int)*nLeft);
    aLeft = aSpare;
    while( iLeft<nLeft || iRight<nRight ){
      RtreeDValue xleft1 = DCOORD(aCell[aLeft[iLeft]].aCoord[iDim*2]);
      RtreeDValue xleft2 = DCOORD(aCell[aLeft[iLeft]].aCoord[iDim*2+1]);
      RtreeDValue xright1 = DCOORD(aCell[aRight[iRight]].aCoord[iDim*2]);
      RtreeDValue xright2 = DCOORD(aCell[aRight[iRight]].aCoord[iDim*2+1]);
      if( (iLeft!=nLeft) && ((iRight==nRight)
       || (xleft1<xright1)
       || (xleft1==xright1 && xleft2<xright2)
      )){
        aIdx[iLeft+iRight] = aLeft[iLeft];
        iLeft++;
      }else{
        aIdx[iLeft+iRight] = aRight[iRight];
        iRight++;
      }
    }

#if 0
    /* Check that the sort worked */
    {
      int jj;
      for(jj=1; jj<nIdx; jj++){
        RtreeDValue xleft1 = aCell[aIdx[jj-1]].aCoord[iDim*2];
        RtreeDValue xleft2 = aCell[aIdx[jj-1]].aCoord[iDim*2+1];
        RtreeDValue xright1 = aCell[aIdx[jj]].aCoord[iDim*2];
        RtreeDValue xright2 = aCell[aIdx[jj]].aCoord[iDim*2+1];
        assert( xleft1<=xright1 && (xleft1<xright1 || xleft2<=xright2) );
      }
    }
#endif
  }
}

#if VARIANT_RSTARTREE_SPLIT
/*
** Implementation of the R*-tree variant of SplitNode from Beckman[1990].
*/
static int splitNodeStartree(
  Rtree *pRtree,
  RtreeCell *aCell,
  int nCell,
  RtreeNode *pLeft,
  RtreeNode *pRight,
  RtreeCell *pBboxLeft,
  RtreeCell *pBboxRight
){
  int **aaSorted;
  int *aSpare;
  int ii;

  int iBestDim = 0;
  int iBestSplit = 0;
  RtreeDValue fBestMargin = 0.0;

  int nByte = (pRtree->nDim+1)*(sizeof(int*)+nCell*sizeof(int));

  aaSorted = (int **)sqlite3_malloc(nByte);
  if( !aaSorted ){
    return SQLITE_NOMEM;
  }

  aSpare = &((int *)&aaSorted[pRtree->nDim])[pRtree->nDim*nCell];
  memset(aaSorted, 0, nByte);
  for(ii=0; ii<pRtree->nDim; ii++){
    int jj;
    aaSorted[ii] = &((int *)&aaSorted[pRtree->nDim])[ii*nCell];
    for(jj=0; jj<nCell; jj++){
      aaSorted[ii][jj] = jj;
    }
    SortByDimension(pRtree, aaSorted[ii], nCell, ii, aCell, aSpare);
  }

  for(ii=0; ii<pRtree->nDim; ii++){
    RtreeDValue margin = 0.0;
    RtreeDValue fBestOverlap = 0.0;
    RtreeDValue fBestArea = 0.0;
    int iBestLeft = 0;
    int nLeft;

    for(
      nLeft=RTREE_MINCELLS(pRtree); 
      nLeft<=(nCell-RTREE_MINCELLS(pRtree)); 
      nLeft++
    ){
      RtreeCell left;
      RtreeCell right;
      int kk;
      RtreeDValue overlap;
      RtreeDValue area;

      memcpy(&left, &aCell[aaSorted[ii][0]], sizeof(RtreeCell));
      memcpy(&right, &aCell[aaSorted[ii][nCell-1]], sizeof(RtreeCell));
      for(kk=1; kk<(nCell-1); kk++){
        if( kk<nLeft ){
          cellUnion(pRtree, &left, &aCell[aaSorted[ii][kk]]);
        }else{
          cellUnion(pRtree, &right, &aCell[aaSorted[ii][kk]]);
        }
      }
      margin += cellMargin(pRtree, &left);
      margin += cellMargin(pRtree, &right);
      overlap = cellOverlap(pRtree, &left, &right, 1, -1);
      area = cellArea(pRtree, &left) + cellArea(pRtree, &right);
      if( (nLeft==RTREE_MINCELLS(pRtree))
       || (overlap<fBestOverlap)
       || (overlap==fBestOverlap && area<fBestArea)
      ){
        iBestLeft = nLeft;
        fBestOverlap = overlap;
        fBestArea = area;
      }
    }

    if( ii==0 || margin<fBestMargin ){
      iBestDim = ii;
      fBestMargin = margin;
      iBestSplit = iBestLeft;
    }
  }

  memcpy(pBboxLeft, &aCell[aaSorted[iBestDim][0]], sizeof(RtreeCell));
  memcpy(pBboxRight, &aCell[aaSorted[iBestDim][iBestSplit]], sizeof(RtreeCell));
  for(ii=0; ii<nCell; ii++){
    RtreeNode *pTarget = (ii<iBestSplit)?pLeft:pRight;
    RtreeCell *pBbox = (ii<iBestSplit)?pBboxLeft:pBboxRight;
    RtreeCell *pCell = &aCell[aaSorted[iBestDim][ii]];
    nodeInsertCell(pRtree, pTarget, pCell);
    cellUnion(pRtree, pBbox, pCell);
  }

  sqlite3_free(aaSorted);
  return SQLITE_OK;
}
#endif

#if VARIANT_GUTTMAN_SPLIT
/*
** Implementation of the regular R-tree SplitNode from Guttman[1984].
*/
static int splitNodeGuttman(
  Rtree *pRtree,
  RtreeCell *aCell,
  int nCell,
  RtreeNode *pLeft,
  RtreeNode *pRight,
  RtreeCell *pBboxLeft,
  RtreeCell *pBboxRight
){
  int iLeftSeed = 0;
  int iRightSeed = 1;
  int *aiUsed;
  int i;

  aiUsed = sqlite3_malloc(sizeof(int)*nCell);
  if( !aiUsed ){
    return SQLITE_NOMEM;
  }
  memset(aiUsed, 0, sizeof(int)*nCell);

  PickSeeds(pRtree, aCell, nCell, &iLeftSeed, &iRightSeed);

  memcpy(pBboxLeft, &aCell[iLeftSeed], sizeof(RtreeCell));
  memcpy(pBboxRight, &aCell[iRightSeed], sizeof(RtreeCell));
  nodeInsertCell(pRtree, pLeft, &aCell[iLeftSeed]);
  nodeInsertCell(pRtree, pRight, &aCell[iRightSeed]);
  aiUsed[iLeftSeed] = 1;
  aiUsed[iRightSeed] = 1;

  for(i=nCell-2; i>0; i--){
    RtreeCell *pNext;
    pNext = PickNext(pRtree, aCell, nCell, pBboxLeft, pBboxRight, aiUsed);
    RtreeDValue diff =  
      cellGrowth(pRtree, pBboxLeft, pNext) - 
      cellGrowth(pRtree, pBboxRight, pNext)
    ;
    if( (RTREE_MINCELLS(pRtree)-NCELL(pRight)==i)
     || (diff>0.0 && (RTREE_MINCELLS(pRtree)-NCELL(pLeft)!=i))
    ){
      nodeInsertCell(pRtree, pRight, pNext);
      cellUnion(pRtree, pBboxRight, pNext);
    }else{
      nodeInsertCell(pRtree, pLeft, pNext);
      cellUnion(pRtree, pBboxLeft, pNext);
    }
  }

  sqlite3_free(aiUsed);
  return SQLITE_OK;
}
#endif

static int updateMapping(
  Rtree *pRtree, 
  i64 iRowid, 
  RtreeNode *pNode, 
  int iHeight
){
  int (*xSetMapping)(Rtree *, sqlite3_int64, sqlite3_int64);
  xSetMapping = ((iHeight==0)?rowidWrite:parentWrite);
  if( iHeight>0 ){
    RtreeNode *pChild = nodeHashLookup(pRtree, iRowid);
    if( pChild ){
      nodeRelease(pRtree, pChild->pParent);
      nodeReference(pNode);
      pChild->pParent = pNode;
    }
  }
  return xSetMapping(pRtree, iRowid, pNode->iNode);
}

static int SplitNode(
  Rtree *pRtree,
  RtreeNode *pNode,
  RtreeCell *pCell,
  int iHeight
){
  int i;
  int newCellIsRight = 0;

  int rc = SQLITE_OK;
  int nCell = NCELL(pNode);
  RtreeCell *aCell;
  int *aiUsed;

  RtreeNode *pLeft = 0;
  RtreeNode *pRight = 0;

  RtreeCell leftbbox;
  RtreeCell rightbbox;

  /* Allocate an array and populate it with a copy of pCell and 
  ** all cells from node pLeft. Then zero the original node.
  */
  aCell = sqlite3_malloc((sizeof(RtreeCell)+sizeof(int))*(nCell+1));
  if( !aCell ){
    rc = SQLITE_NOMEM;
    goto splitnode_out;
  }
  aiUsed = (int *)&aCell[nCell+1];
  memset(aiUsed, 0, sizeof(int)*(nCell+1));
  for(i=0; i<nCell; i++){
    nodeGetCell(pRtree, pNode, i, &aCell[i]);
  }
  nodeZero(pRtree, pNode);
  memcpy(&aCell[nCell], pCell, sizeof(RtreeCell));
  nCell++;

  if( pNode->iNode==1 ){
    pRight = nodeNew(pRtree, pNode);
    pLeft = nodeNew(pRtree, pNode);
    pRtree->iDepth++;
    pNode->isDirty = 1;
    writeInt16(pNode->zData, pRtree->iDepth);
  }else{
    pLeft = pNode;
    pRight = nodeNew(pRtree, pLeft->pParent);
    nodeReference(pLeft);
  }

  if( !pLeft || !pRight ){
    rc = SQLITE_NOMEM;
    goto splitnode_out;
  }

  memset(pLeft->zData, 0, pRtree->iNodeSize);
  memset(pRight->zData, 0, pRtree->iNodeSize);

  rc = AssignCells(pRtree, aCell, nCell, pLeft, pRight, &leftbbox, &rightbbox);
  if( rc!=SQLITE_OK ){
    goto splitnode_out;
  }

  /* Ensure both child nodes have node numbers assigned to them by calling
  ** nodeWrite(). Node pRight always needs a node number, as it was created
  ** by nodeNew() above. But node pLeft sometimes already has a node number.
  ** In this case avoid the all to nodeWrite().
  */
  if( SQLITE_OK!=(rc = nodeWrite(pRtree, pRight))
   || (0==pLeft->iNode && SQLITE_OK!=(rc = nodeWrite(pRtree, pLeft)))
  ){
    goto splitnode_out;
  }

  rightbbox.iRowid = pRight->iNode;
  leftbbox.iRowid = pLeft->iNode;

  if( pNode->iNode==1 ){
    rc = rtreeInsertCell(pRtree, pLeft->pParent, &leftbbox, iHeight+1);
    if( rc!=SQLITE_OK ){
      goto splitnode_out;
    }
  }else{
    RtreeNode *pParent = pLeft->pParent;
    int iCell;
    rc = nodeParentIndex(pRtree, pLeft, &iCell);
    if( rc==SQLITE_OK ){
      nodeOverwriteCell(pRtree, pParent, &leftbbox, iCell);
      rc = AdjustTree(pRtree, pParent, &leftbbox);
    }
    if( rc!=SQLITE_OK ){
      goto splitnode_out;
    }
  }
  if( (rc = rtreeInsertCell(pRtree, pRight->pParent, &rightbbox, iHeight+1)) ){
    goto splitnode_out;
  }

  for(i=0; i<NCELL(pRight); i++){
    i64 iRowid = nodeGetRowid(pRtree, pRight, i);
    rc = updateMapping(pRtree, iRowid, pRight, iHeight);
    if( iRowid==pCell->iRowid ){
      newCellIsRight = 1;
    }
    if( rc!=SQLITE_OK ){
      goto splitnode_out;
    }
  }
  if( pNode->iNode==1 ){
    for(i=0; i<NCELL(pLeft); i++){
      i64 iRowid = nodeGetRowid(pRtree, pLeft, i);
      rc = updateMapping(pRtree, iRowid, pLeft, iHeight);
      if( rc!=SQLITE_OK ){
        goto splitnode_out;
      }
    }
  }else if( newCellIsRight==0 ){
    rc = updateMapping(pRtree, pCell->iRowid, pLeft, iHeight);
  }

  if( rc==SQLITE_OK ){
    rc = nodeRelease(pRtree, pRight);
    pRight = 0;
  }
  if( rc==SQLITE_OK ){
    rc = nodeRelease(pRtree, pLeft);
    pLeft = 0;
  }

splitnode_out:
  nodeRelease(pRtree, pRight);
  nodeRelease(pRtree, pLeft);
  sqlite3_free(aCell);
  return rc;
}

/*
** If node pLeaf is not the root of the r-tree and its pParent pointer is 
** still NULL, load all ancestor nodes of pLeaf into memory and populate
** the pLeaf->pParent chain all the way up to the root node.
**
** This operation is required when a row is deleted (or updated - an update
** is implemented as a delete followed by an insert). SQLite provides the
** rowid of the row to delete, which can be used to find the leaf on which
** the entry resides (argument pLeaf). Once the leaf is located, this 
** function is called to determine its ancestry.
*/
static int fixLeafParent(Rtree *pRtree, RtreeNode *pLeaf){
  int rc = SQLITE_OK;
  RtreeNode *pChild = pLeaf;
  while( rc==SQLITE_OK && pChild->iNode!=1 && pChild->pParent==0 ){
    int rc2 = SQLITE_OK;          /* sqlite3_reset() return code */
    sqlite3_bind_int64(pRtree->pReadParent, 1, pChild->iNode);
    rc = sqlite3_step(pRtree->pReadParent);
    if( rc==SQLITE_ROW ){
      RtreeNode *pTest;           /* Used to test for reference loops */
      i64 iNode;                  /* Node number of parent node */

      /* Before setting pChild->pParent, test that we are not creating a
      ** loop of references (as we would if, say, pChild==pParent). We don't
      ** want to do this as it leads to a memory leak when trying to delete
      ** the referenced counted node structures.
      */
      iNode = sqlite3_column_int64(pRtree->pReadParent, 0);
      for(pTest=pLeaf; pTest && pTest->iNode!=iNode; pTest=pTest->pParent);
      if( !pTest ){
        rc2 = nodeAcquire(pRtree, iNode, 0, &pChild->pParent);
      }
    }
    rc = sqlite3_reset(pRtree->pReadParent);
    if( rc==SQLITE_OK ) rc = rc2;
    if( rc==SQLITE_OK && !pChild->pParent ) rc = SQLITE_CORRUPT_VTAB;
    pChild = pChild->pParent;
  }
  return rc;
}

static int deleteCell(Rtree *, RtreeNode *, int, int);

static int removeNode(Rtree *pRtree, RtreeNode *pNode, int iHeight){
  int rc;
  int rc2;
  RtreeNode *pParent = 0;
  int iCell;

  assert( pNode->nRef==1 );

  /* Remove the entry in the parent cell. */
  rc = nodeParentIndex(pRtree, pNode, &iCell);
  if( rc==SQLITE_OK ){
    pParent = pNode->pParent;
    pNode->pParent = 0;
    rc = deleteCell(pRtree, pParent, iCell, iHeight+1);
  }
  rc2 = nodeRelease(pRtree, pParent);
  if( rc==SQLITE_OK ){
    rc = rc2;
  }
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* Remove the xxx_node entry. */
  sqlite3_bind_int64(pRtree->pDeleteNode, 1, pNode->iNode);
  sqlite3_step(pRtree->pDeleteNode);
  if( SQLITE_OK!=(rc = sqlite3_reset(pRtree->pDeleteNode)) ){
    return rc;
  }

  /* Remove the xxx_parent entry. */
  sqlite3_bind_int64(pRtree->pDeleteParent, 1, pNode->iNode);
  sqlite3_step(pRtree->pDeleteParent);
  if( SQLITE_OK!=(rc = sqlite3_reset(pRtree->pDeleteParent)) ){
    return rc;
  }
  
  /* Remove the node from the in-memory hash table and link it into
  ** the Rtree.pDeleted list. Its contents will be re-inserted later on.
  */
  nodeHashDelete(pRtree, pNode);
  pNode->iNode = iHeight;
  pNode->pNext = pRtree->pDeleted;
  pNode->nRef++;
  pRtree->pDeleted = pNode;

  return SQLITE_OK;
}

static int fixBoundingBox(Rtree *pRtree, RtreeNode *pNode){
  RtreeNode *pParent = pNode->pParent;
  int rc = SQLITE_OK; 
  if( pParent ){
    int ii; 
    int nCell = NCELL(pNode);
    RtreeCell box;                            /* Bounding box for pNode */
    nodeGetCell(pRtree, pNode, 0, &box);
    for(ii=1; ii<nCell; ii++){
      RtreeCell cell;
      nodeGetCell(pRtree, pNode, ii, &cell);
      cellUnion(pRtree, &box, &cell);
    }
    box.iRowid = pNode->iNode;
    rc = nodeParentIndex(pRtree, pNode, &ii);
    if( rc==SQLITE_OK ){
      nodeOverwriteCell(pRtree, pParent, &box, ii);
      rc = fixBoundingBox(pRtree, pParent);
    }
  }
  return rc;
}

/*
** Delete the cell at index iCell of node pNode. After removing the
** cell, adjust the r-tree data structure if required.
*/
static int deleteCell(Rtree *pRtree, RtreeNode *pNode, int iCell, int iHeight){
  RtreeNode *pParent;
  int rc;

  if( SQLITE_OK!=(rc = fixLeafParent(pRtree, pNode)) ){
    return rc;
  }

  /* Remove the cell from the node. This call just moves bytes around
  ** the in-memory node image, so it cannot fail.
  */
  nodeDeleteCell(pRtree, pNode, iCell);

  /* If the node is not the tree root and now has less than the minimum
  ** number of cells, remove it from the tree. Otherwise, update the
  ** cell in the parent node so that it tightly contains the updated
  ** node.
  */
  pParent = pNode->pParent;
  assert( pParent || pNode->iNode==1 );
  if( pParent ){
    if( NCELL(pNode)<RTREE_MINCELLS(pRtree) ){
      rc = removeNode(pRtree, pNode, iHeight);
    }else{
      rc = fixBoundingBox(pRtree, pNode);
    }
  }

  return rc;
}

static int Reinsert(
  Rtree *pRtree, 
  RtreeNode *pNode, 
  RtreeCell *pCell, 
  int iHeight
){
  int *aOrder;
  int *aSpare;
  RtreeCell *aCell;
  RtreeDValue *aDistance;
  int nCell;
  RtreeDValue aCenterCoord[RTREE_MAX_DIMENSIONS];
  int iDim;
  int ii;
  int rc = SQLITE_OK;
  int n;

  memset(aCenterCoord, 0, sizeof(RtreeDValue)*RTREE_MAX_DIMENSIONS);

  nCell = NCELL(pNode)+1;
  n = (nCell+1)&(~1);

  /* Allocate the buffers used by this operation. The allocation is
  ** relinquished before this function returns.
  */
  aCell = (RtreeCell *)sqlite3_malloc(n * (
    sizeof(RtreeCell)     +         /* aCell array */
    sizeof(int)           +         /* aOrder array */
    sizeof(int)           +         /* aSpare array */
    sizeof(RtreeDValue)             /* aDistance array */
  ));
  if( !aCell ){
    return SQLITE_NOMEM;
  }
  aOrder    = (int *)&aCell[n];
  aSpare    = (int *)&aOrder[n];
  aDistance = (RtreeDValue *)&aSpare[n];

  for(ii=0; ii<nCell; ii++){
    if( ii==(nCell-1) ){
      memcpy(&aCell[ii], pCell, sizeof(RtreeCell));
    }else{
      nodeGetCell(pRtree, pNode, ii, &aCell[ii]);
    }
    aOrder[ii] = ii;
    for(iDim=0; iDim<pRtree->nDim; iDim++){
      aCenterCoord[iDim] += DCOORD(aCell[ii].aCoord[iDim*2]);
      aCenterCoord[iDim] += DCOORD(aCell[ii].aCoord[iDim*2+1]);
    }
  }
  for(iDim=0; iDim<pRtree->nDim; iDim++){
    aCenterCoord[iDim] = (aCenterCoord[iDim]/(nCell*(RtreeDValue)2));
  }

  for(ii=0; ii<nCell; ii++){
    aDistance[ii] = 0.0;
    for(iDim=0; iDim<pRtree->nDim; iDim++){
      RtreeDValue coord = (DCOORD(aCell[ii].aCoord[iDim*2+1]) - 
                               DCOORD(aCell[ii].aCoord[iDim*2]));
      aDistance[ii] += (coord-aCenterCoord[iDim])*(coord-aCenterCoord[iDim]);
    }
  }

  SortByDistance(aOrder, nCell, aDistance, aSpare);
  nodeZero(pRtree, pNode);

  for(ii=0; rc==SQLITE_OK && ii<(nCell-(RTREE_MINCELLS(pRtree)+1)); ii++){
    RtreeCell *p = &aCell[aOrder[ii]];
    nodeInsertCell(pRtree, pNode, p);
    if( p->iRowid==pCell->iRowid ){
      if( iHeight==0 ){
        rc = rowidWrite(pRtree, p->iRowid, pNode->iNode);
      }else{
        rc = parentWrite(pRtree, p->iRowid, pNode->iNode);
      }
    }
  }
  if( rc==SQLITE_OK ){
    rc = fixBoundingBox(pRtree, pNode);
  }
  for(; rc==SQLITE_OK && ii<nCell; ii++){
    /* Find a node to store this cell in. pNode->iNode currently contains
    ** the height of the sub-tree headed by the cell.
    */
    RtreeNode *pInsert;
    RtreeCell *p = &aCell[aOrder[ii]];
    rc = ChooseLeaf(pRtree, p, iHeight, &pInsert);
    if( rc==SQLITE_OK ){
      int rc2;
      rc = rtreeInsertCell(pRtree, pInsert, p, iHeight);
      rc2 = nodeRelease(pRtree, pInsert);
      if( rc==SQLITE_OK ){
        rc = rc2;
      }
    }
  }

  sqlite3_free(aCell);
  return rc;
}

/*
** Insert cell pCell into node pNode. Node pNode is the head of a 
** subtree iHeight high (leaf nodes have iHeight==0).
*/
static int rtreeInsertCell(
  Rtree *pRtree,
  RtreeNode *pNode,
  RtreeCell *pCell,
  int iHeight
){
  int rc = SQLITE_OK;
  if( iHeight>0 ){
    RtreeNode *pChild = nodeHashLookup(pRtree, pCell->iRowid);
    if( pChild ){
      nodeRelease(pRtree, pChild->pParent);
      nodeReference(pNode);
      pChild->pParent = pNode;
    }
  }
  if( nodeInsertCell(pRtree, pNode, pCell) ){
#if VARIANT_RSTARTREE_REINSERT
    if( iHeight<=pRtree->iReinsertHeight || pNode->iNode==1){
      rc = SplitNode(pRtree, pNode, pCell, iHeight);
    }else{
      pRtree->iReinsertHeight = iHeight;
      rc = Reinsert(pRtree, pNode, pCell, iHeight);
    }
#else
    rc = SplitNode(pRtree, pNode, pCell, iHeight);
#endif
  }else{
    rc = AdjustTree(pRtree, pNode, pCell);
    if( rc==SQLITE_OK ){
      if( iHeight==0 ){
        rc = rowidWrite(pRtree, pCell->iRowid, pNode->iNode);
      }else{
        rc = parentWrite(pRtree, pCell->iRowid, pNode->iNode);
      }
    }
  }
  return rc;
}

static int reinsertNodeContent(Rtree *pRtree, RtreeNode *pNode){
  int ii;
  int rc = SQLITE_OK;
  int nCell = NCELL(pNode);

  for(ii=0; rc==SQLITE_OK && ii<nCell; ii++){
    RtreeNode *pInsert;
    RtreeCell cell;
    nodeGetCell(pRtree, pNode, ii, &cell);

    /* Find a node to store this cell in. pNode->iNode currently contains
    ** the height of the sub-tree headed by the cell.
    */
    rc = ChooseLeaf(pRtree, &cell, (int)pNode->iNode, &pInsert);
    if( rc==SQLITE_OK ){
      int rc2;
      rc = rtreeInsertCell(pRtree, pInsert, &cell, (int)pNode->iNode);
      rc2 = nodeRelease(pRtree, pInsert);
      if( rc==SQLITE_OK ){
        rc = rc2;
      }
    }
  }
  return rc;
}

/*
** Select a currently unused rowid for a new r-tree record.
*/
static int newRowid(Rtree *pRtree, i64 *piRowid){
  int rc;
  sqlite3_bind_null(pRtree->pWriteRowid, 1);
  sqlite3_bind_null(pRtree->pWriteRowid, 2);
  sqlite3_step(pRtree->pWriteRowid);
  rc = sqlite3_reset(pRtree->pWriteRowid);
  *piRowid = sqlite3_last_insert_rowid(pRtree->db);
  return rc;
}

/*
** Remove the entry with rowid=iDelete from the r-tree structure.
*/
static int rtreeDeleteRowid(Rtree *pRtree, sqlite3_int64 iDelete){
  int rc;                         /* Return code */
  RtreeNode *pLeaf = 0;           /* Leaf node containing record iDelete */
  int iCell;                      /* Index of iDelete cell in pLeaf */
  RtreeNode *pRoot;               /* Root node of rtree structure */


  /* Obtain a reference to the root node to initialize Rtree.iDepth */
  rc = nodeAcquire(pRtree, 1, 0, &pRoot);

  /* Obtain a reference to the leaf node that contains the entry 
  ** about to be deleted. 
  */
  if( rc==SQLITE_OK ){
    rc = findLeafNode(pRtree, iDelete, &pLeaf);
  }

  /* Delete the cell in question from the leaf node. */
  if( rc==SQLITE_OK ){
    int rc2;
    rc = nodeRowidIndex(pRtree, pLeaf, iDelete, &iCell);
    if( rc==SQLITE_OK ){
      rc = deleteCell(pRtree, pLeaf, iCell, 0);
    }
    rc2 = nodeRelease(pRtree, pLeaf);
    if( rc==SQLITE_OK ){
      rc = rc2;
    }
  }

  /* Delete the corresponding entry in the <rtree>_rowid table. */
  if( rc==SQLITE_OK ){
    sqlite3_bind_int64(pRtree->pDeleteRowid, 1, iDelete);
    sqlite3_step(pRtree->pDeleteRowid);
    rc = sqlite3_reset(pRtree->pDeleteRowid);
  }

  /* Check if the root node now has exactly one child. If so, remove
  ** it, schedule the contents of the child for reinsertion and 
  ** reduce the tree height by one.
  **
  ** This is equivalent to copying the contents of the child into
  ** the root node (the operation that Gutman's paper says to perform 
  ** in this scenario).
  */
  if( rc==SQLITE_OK && pRtree->iDepth>0 && NCELL(pRoot)==1 ){
    int rc2;
    RtreeNode *pChild;
    i64 iChild = nodeGetRowid(pRtree, pRoot, 0);
    rc = nodeAcquire(pRtree, iChild, pRoot, &pChild);
    if( rc==SQLITE_OK ){
      rc = removeNode(pRtree, pChild, pRtree->iDepth-1);
    }
    rc2 = nodeRelease(pRtree, pChild);
    if( rc==SQLITE_OK ) rc = rc2;
    if( rc==SQLITE_OK ){
      pRtree->iDepth--;
      writeInt16(pRoot->zData, pRtree->iDepth);
      pRoot->isDirty = 1;
    }
  }

  /* Re-insert the contents of any underfull nodes removed from the tree. */
  for(pLeaf=pRtree->pDeleted; pLeaf; pLeaf=pRtree->pDeleted){
    if( rc==SQLITE_OK ){
      rc = reinsertNodeContent(pRtree, pLeaf);
    }
    pRtree->pDeleted = pLeaf->pNext;
    sqlite3_free(pLeaf);
  }

  /* Release the reference to the root node. */
  if( rc==SQLITE_OK ){
    rc = nodeRelease(pRtree, pRoot);
  }else{
    nodeRelease(pRtree, pRoot);
  }

  return rc;
}

/*
** Rounding constants for float->double conversion.
*/
#define RNDTOWARDS  (1.0 - 1.0/8388608.0)  /* Round towards zero */
#define RNDAWAY     (1.0 + 1.0/8388608.0)  /* Round away from zero */

#if !defined(SQLITE_RTREE_INT_ONLY)
/*
** Convert an sqlite3_value into an RtreeValue (presumably a float)
** while taking care to round toward negative or positive, respectively.
*/
static RtreeValue rtreeValueDown(sqlite3_value *v){
  double d = sqlite3_value_double(v);
  float f = (float)d;
  if( f>d ){
    f = (float)(d*(d<0 ? RNDAWAY : RNDTOWARDS));
  }
  return f;
}
static RtreeValue rtreeValueUp(sqlite3_value *v){
  double d = sqlite3_value_double(v);
  float f = (float)d;
  if( f<d ){
    f = (float)(d*(d<0 ? RNDTOWARDS : RNDAWAY));
  }
  return f;
}
#endif /* !defined(SQLITE_RTREE_INT_ONLY) */


/*
** The xUpdate method for rtree module virtual tables.
*/
static int rtreeUpdate(
  sqlite3_vtab *pVtab, 
  int nData, 
  sqlite3_value **azData, 
  sqlite_int64 *pRowid
){
  Rtree *pRtree = (Rtree *)pVtab;
  int rc = SQLITE_OK;
  RtreeCell cell;                 /* New cell to insert if nData>1 */
  int bHaveRowid = 0;             /* Set to 1 after new rowid is determined */

  rtreeReference(pRtree);
  assert(nData>=1);

  /* Constraint handling. A write operation on an r-tree table may return
  ** SQLITE_CONSTRAINT for two reasons:
  **
  **   1. A duplicate rowid value, or
  **   2. The supplied data violates the "x2>=x1" constraint.
  **
  ** In the first case, if the conflict-handling mode is REPLACE, then
  ** the conflicting row can be removed before proceeding. In the second
  ** case, SQLITE_CONSTRAINT must be returned regardless of the
  ** conflict-handling mode specified by the user.
  */
  if( nData>1 ){
    int ii;

    /* Populate the cell.aCoord[] array. The first coordinate is azData[3]. */
    assert( nData==(pRtree->nDim*2 + 3) );
#ifndef SQLITE_RTREE_INT_ONLY
    if( pRtree->eCoordType==RTREE_COORD_REAL32 ){
      for(ii=0; ii<(pRtree->nDim*2); ii+=2){
        cell.aCoord[ii].f = rtreeValueDown(azData[ii+3]);
        cell.aCoord[ii+1].f = rtreeValueUp(azData[ii+4]);
        if( cell.aCoord[ii].f>cell.aCoord[ii+1].f ){
          rc = SQLITE_CONSTRAINT;
          goto constraint;
        }
      }
    }else
#endif
    {
      for(ii=0; ii<(pRtree->nDim*2); ii+=2){
        cell.aCoord[ii].i = sqlite3_value_int(azData[ii+3]);
        cell.aCoord[ii+1].i = sqlite3_value_int(azData[ii+4]);
        if( cell.aCoord[ii].i>cell.aCoord[ii+1].i ){
          rc = SQLITE_CONSTRAINT;
          goto constraint;
        }
      }
    }

    /* If a rowid value was supplied, check if it is already present in 
    ** the table. If so, the constraint has failed. */
    if( sqlite3_value_type(azData[2])!=SQLITE_NULL ){
      cell.iRowid = sqlite3_value_int64(azData[2]);
      if( sqlite3_value_type(azData[0])==SQLITE_NULL
       || sqlite3_value_int64(azData[0])!=cell.iRowid
      ){
        int steprc;
        sqlite3_bind_int64(pRtree->pReadRowid, 1, cell.iRowid);
        steprc = sqlite3_step(pRtree->pReadRowid);
        rc = sqlite3_reset(pRtree->pReadRowid);
        if( SQLITE_ROW==steprc ){
          if( sqlite3_vtab_on_conflict(pRtree->db)==SQLITE_REPLACE ){
            rc = rtreeDeleteRowid(pRtree, cell.iRowid);
          }else{
            rc = SQLITE_CONSTRAINT;
            goto constraint;
          }
        }
      }
      bHaveRowid = 1;
    }
  }

  /* If azData[0] is not an SQL NULL value, it is the rowid of a
  ** record to delete from the r-tree table. The following block does
  ** just that.
  */
  if( sqlite3_value_type(azData[0])!=SQLITE_NULL ){
    rc = rtreeDeleteRowid(pRtree, sqlite3_value_int64(azData[0]));
  }

  /* If the azData[] array contains more than one element, elements
  ** (azData[2]..azData[argc-1]) contain a new record to insert into
  ** the r-tree structure.
  */
  if( rc==SQLITE_OK && nData>1 ){
    /* Insert the new record into the r-tree */
    RtreeNode *pLeaf = 0;

    /* Figure out the rowid of the new row. */
    if( bHaveRowid==0 ){
      rc = newRowid(pRtree, &cell.iRowid);
    }
    *pRowid = cell.iRowid;

    if( rc==SQLITE_OK ){
      rc = ChooseLeaf(pRtree, &cell, 0, &pLeaf);
    }
    if( rc==SQLITE_OK ){
      int rc2;
      pRtree->iReinsertHeight = -1;
      rc = rtreeInsertCell(pRtree, pLeaf, &cell, 0);
      rc2 = nodeRelease(pRtree, pLeaf);
      if( rc==SQLITE_OK ){
        rc = rc2;
      }
    }
  }

constraint:
  rtreeRelease(pRtree);
  return rc;
}

/*
** The xRename method for rtree module virtual tables.
*/
static int rtreeRename(sqlite3_vtab *pVtab, const char *zNewName){
  Rtree *pRtree = (Rtree *)pVtab;
  int rc = SQLITE_NOMEM;
  char *zSql = sqlite3_mprintf(
    "ALTER TABLE %Q.'%q_node'   RENAME TO \"%w_node\";"
    "ALTER TABLE %Q.'%q_parent' RENAME TO \"%w_parent\";"
    "ALTER TABLE %Q.'%q_rowid'  RENAME TO \"%w_rowid\";"
    , pRtree->zDb, pRtree->zName, zNewName 
    , pRtree->zDb, pRtree->zName, zNewName 
    , pRtree->zDb, pRtree->zName, zNewName
  );
  if( zSql ){
    rc = sqlite3_exec(pRtree->db, zSql, 0, 0, 0);
    sqlite3_free(zSql);
  }
  return rc;
}

/*
** This function populates the pRtree->nRowEst variable with an estimate
** of the number of rows in the virtual table. If possible, this is based
** on sqlite_stat1 data. Otherwise, use RTREE_DEFAULT_ROWEST.
*/
static int rtreeQueryStat1(sqlite3 *db, Rtree *pRtree){
  const char *zSql = "SELECT stat FROM sqlite_stat1 WHERE tbl= ? || '_rowid'";
  sqlite3_stmt *p;
  int rc;
  i64 nRow = 0;

  rc = sqlite3_prepare_v2(db, zSql, -1, &p, 0);
  if( rc==SQLITE_OK ){
    sqlite3_bind_text(p, 1, pRtree->zName, -1, SQLITE_STATIC);
    if( sqlite3_step(p)==SQLITE_ROW ) nRow = sqlite3_column_int64(p, 0);
    rc = sqlite3_finalize(p);
  }else if( rc!=SQLITE_NOMEM ){
    rc = SQLITE_OK;
  }

  if( rc==SQLITE_OK ){
    if( nRow==0 ){
      pRtree->nRowEst = RTREE_DEFAULT_ROWEST;
    }else{
      pRtree->nRowEst = MAX(nRow, RTREE_MIN_ROWEST);
    }
  }

  return rc;
}

static sqlite3_module rtreeModule = {
  0,                          /* iVersion */
  rtreeCreate,                /* xCreate - create a table */
  rtreeConnect,               /* xConnect - connect to an existing table */
  rtreeBestIndex,             /* xBestIndex - Determine search strategy */
  rtreeDisconnect,            /* xDisconnect - Disconnect from a table */
  rtreeDestroy,               /* xDestroy - Drop a table */
  rtreeOpen,                  /* xOpen - open a cursor */
  rtreeClose,                 /* xClose - close a cursor */
  rtreeFilter,                /* xFilter - configure scan constraints */
  rtreeNext,                  /* xNext - advance a cursor */
  rtreeEof,                   /* xEof */
  rtreeColumn,                /* xColumn - read data */
  rtreeRowid,                 /* xRowid - read data */
  rtreeUpdate,                /* xUpdate - write data */
  0,                          /* xBegin - begin transaction */
  0,                          /* xSync - sync transaction */
  0,                          /* xCommit - commit transaction */
  0,                          /* xRollback - rollback transaction */
  0,                          /* xFindFunction - function overloading */
  rtreeRename,                /* xRename - rename the table */
  0,                          /* xSavepoint */
  0,                          /* xRelease */
  0                           /* xRollbackTo */
};

static int rtreeSqlInit(
  Rtree *pRtree, 
  sqlite3 *db, 
  const char *zDb, 
  const char *zPrefix, 
  int isCreate
){
  int rc = SQLITE_OK;

  #define N_STATEMENT 9
  static const char *azSql[N_STATEMENT] = {
    /* Read and write the xxx_node table */
    "SELECT data FROM '%q'.'%q_node' WHERE nodeno = :1",
    "INSERT OR REPLACE INTO '%q'.'%q_node' VALUES(:1, :2)",
    "DELETE FROM '%q'.'%q_node' WHERE nodeno = :1",

    /* Read and write the xxx_rowid table */
    "SELECT nodeno FROM '%q'.'%q_rowid' WHERE rowid = :1",
    "INSERT OR REPLACE INTO '%q'.'%q_rowid' VALUES(:1, :2)",
    "DELETE FROM '%q'.'%q_rowid' WHERE rowid = :1",

    /* Read and write the xxx_parent table */
    "SELECT parentnode FROM '%q'.'%q_parent' WHERE nodeno = :1",
    "INSERT OR REPLACE INTO '%q'.'%q_parent' VALUES(:1, :2)",
    "DELETE FROM '%q'.'%q_parent' WHERE nodeno = :1"
  };
  sqlite3_stmt **appStmt[N_STATEMENT];
  int i;

  pRtree->db = db;

  if( isCreate ){
    char *zCreate = sqlite3_mprintf(
"CREATE TABLE \"%w\".\"%w_node\"(nodeno INTEGER PRIMARY KEY, data BLOB);"
"CREATE TABLE \"%w\".\"%w_rowid\"(rowid INTEGER PRIMARY KEY, nodeno INTEGER);"
"CREATE TABLE \"%w\".\"%w_parent\"(nodeno INTEGER PRIMARY KEY, parentnode INTEGER);"
"INSERT INTO '%q'.'%q_node' VALUES(1, zeroblob(%d))",
      zDb, zPrefix, zDb, zPrefix, zDb, zPrefix, zDb, zPrefix, pRtree->iNodeSize
    );
    if( !zCreate ){
      return SQLITE_NOMEM;
    }
    rc = sqlite3_exec(db, zCreate, 0, 0, 0);
    sqlite3_free(zCreate);
    if( rc!=SQLITE_OK ){
      return rc;
    }
  }

  appStmt[0] = &pRtree->pReadNode;
  appStmt[1] = &pRtree->pWriteNode;
  appStmt[2] = &pRtree->pDeleteNode;
  appStmt[3] = &pRtree->pReadRowid;
  appStmt[4] = &pRtree->pWriteRowid;
  appStmt[5] = &pRtree->pDeleteRowid;
  appStmt[6] = &pRtree->pReadParent;
  appStmt[7] = &pRtree->pWriteParent;
  appStmt[8] = &pRtree->pDeleteParent;

  rc = rtreeQueryStat1(db, pRtree);
  for(i=0; i<N_STATEMENT && rc==SQLITE_OK; i++){
    char *zSql = sqlite3_mprintf(azSql[i], zDb, zPrefix);
    if( zSql ){
      rc = sqlite3_prepare_v2(db, zSql, -1, appStmt[i], 0); 
    }else{
      rc = SQLITE_NOMEM;
    }
    sqlite3_free(zSql);
  }

  return rc;
}

/*
** The second argument to this function contains the text of an SQL statement
** that returns a single integer value. The statement is compiled and executed
** using database connection db. If successful, the integer value returned
** is written to *piVal and SQLITE_OK returned. Otherwise, an SQLite error
** code is returned and the value of *piVal after returning is not defined.
*/
static int getIntFromStmt(sqlite3 *db, const char *zSql, int *piVal){
  int rc = SQLITE_NOMEM;
  if( zSql ){
    sqlite3_stmt *pStmt = 0;
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
    if( rc==SQLITE_OK ){
      if( SQLITE_ROW==sqlite3_step(pStmt) ){
        *piVal = sqlite3_column_int(pStmt, 0);
      }
      rc = sqlite3_finalize(pStmt);
    }
  }
  return rc;
}

/*
** This function is called from within the xConnect() or xCreate() method to
** determine the node-size used by the rtree table being created or connected
** to. If successful, pRtree->iNodeSize is populated and SQLITE_OK returned.
** Otherwise, an SQLite error code is returned.
**
** If this function is being called as part of an xConnect(), then the rtree
** table already exists. In this case the node-size is determined by inspecting
** the root node of the tree.
**
** Otherwise, for an xCreate(), use 64 bytes less than the database page-size. 
** This ensures that each node is stored on a single database page. If the 
** database page-size is so large that more than RTREE_MAXCELLS entries 
** would fit in a single node, use a smaller node-size.
*/
static int getNodeSize(
  sqlite3 *db,                    /* Database handle */
  Rtree *pRtree,                  /* Rtree handle */
  int isCreate,                   /* True for xCreate, false for xConnect */
  char **pzErr                    /* OUT: Error message, if any */
){
  int rc;
  char *zSql;
  if( isCreate ){
    int iPageSize = 0;
    zSql = sqlite3_mprintf("PRAGMA %Q.page_size", pRtree->zDb);
    rc = getIntFromStmt(db, zSql, &iPageSize);
    if( rc==SQLITE_OK ){
      pRtree->iNodeSize = iPageSize-64;
      if( (4+pRtree->nBytesPerCell*RTREE_MAXCELLS)<pRtree->iNodeSize ){
        pRtree->iNodeSize = 4+pRtree->nBytesPerCell*RTREE_MAXCELLS;
      }
    }else{
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    }
  }else{
    zSql = sqlite3_mprintf(
        "SELECT length(data) FROM '%q'.'%q_node' WHERE nodeno = 1",
        pRtree->zDb, pRtree->zName
    );
    rc = getIntFromStmt(db, zSql, &pRtree->iNodeSize);
    if( rc!=SQLITE_OK ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    }
  }

  sqlite3_free(zSql);
  return rc;
}

/* 
** This function is the implementation of both the xConnect and xCreate
** methods of the r-tree virtual table.
**
**   argv[0]   -> module name
**   argv[1]   -> database name
**   argv[2]   -> table name
**   argv[...] -> column names...
*/
static int rtreeInit(
  sqlite3 *db,                        /* Database connection */
  void *pAux,                         /* One of the RTREE_COORD_* constants */
  int argc, const char *const*argv,   /* Parameters to CREATE TABLE statement */
  sqlite3_vtab **ppVtab,              /* OUT: New virtual table */
  char **pzErr,                       /* OUT: Error message, if any */
  int isCreate                        /* True for xCreate, false for xConnect */
){
  int rc = SQLITE_OK;
  Rtree *pRtree;
  int nDb;              /* Length of string argv[1] */
  int nName;            /* Length of string argv[2] */
  int eCoordType = (pAux ? RTREE_COORD_INT32 : RTREE_COORD_REAL32);

  const char *aErrMsg[] = {
    0,                                                    /* 0 */
    "Wrong number of columns for an rtree table",         /* 1 */
    "Too few columns for an rtree table",                 /* 2 */
    "Too many columns for an rtree table"                 /* 3 */
  };

  int iErr = (argc<6) ? 2 : argc>(RTREE_MAX_DIMENSIONS*2+4) ? 3 : argc%2;
  if( aErrMsg[iErr] ){
    *pzErr = sqlite3_mprintf("%s", aErrMsg[iErr]);
    return SQLITE_ERROR;
  }

  sqlite3_vtab_config(db, SQLITE_VTAB_CONSTRAINT_SUPPORT, 1);

  /* Allocate the sqlite3_vtab structure */
  nDb = (int)strlen(argv[1]);
  nName = (int)strlen(argv[2]);
  pRtree = (Rtree *)sqlite3_malloc(sizeof(Rtree)+nDb+nName+2);
  if( !pRtree ){
    return SQLITE_NOMEM;
  }
  memset(pRtree, 0, sizeof(Rtree)+nDb+nName+2);
  pRtree->nBusy = 1;
  pRtree->base.pModule = &rtreeModule;
  pRtree->zDb = (char *)&pRtree[1];
  pRtree->zName = &pRtree->zDb[nDb+1];
  pRtree->nDim = (argc-4)/2;
  pRtree->nBytesPerCell = 8 + pRtree->nDim*4*2;
  pRtree->eCoordType = eCoordType;
  memcpy(pRtree->zDb, argv[1], nDb);
  memcpy(pRtree->zName, argv[2], nName);

  /* Figure out the node size to use. */
  rc = getNodeSize(db, pRtree, isCreate, pzErr);

  /* Create/Connect to the underlying relational database schema. If
  ** that is successful, call sqlite3_declare_vtab() to configure
  ** the r-tree table schema.
  */
  if( rc==SQLITE_OK ){
    if( (rc = rtreeSqlInit(pRtree, db, argv[1], argv[2], isCreate)) ){
      *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
    }else{
      char *zSql = sqlite3_mprintf("CREATE TABLE x(%s", argv[3]);
      char *zTmp;
      int ii;
      for(ii=4; zSql && ii<argc; ii++){
        zTmp = zSql;
        zSql = sqlite3_mprintf("%s, %s", zTmp, argv[ii]);
        sqlite3_free(zTmp);
      }
      if( zSql ){
        zTmp = zSql;
        zSql = sqlite3_mprintf("%s);", zTmp);
        sqlite3_free(zTmp);
      }
      if( !zSql ){
        rc = SQLITE_NOMEM;
      }else if( SQLITE_OK!=(rc = sqlite3_declare_vtab(db, zSql)) ){
        *pzErr = sqlite3_mprintf("%s", sqlite3_errmsg(db));
      }
      sqlite3_free(zSql);
    }
  }

  if( rc==SQLITE_OK ){
    *ppVtab = (sqlite3_vtab *)pRtree;
  }else{
    rtreeRelease(pRtree);
  }
  return rc;
}


/*
** Implementation of a scalar function that decodes r-tree nodes to
** human readable strings. This can be used for debugging and analysis.
**
** The scalar function takes two arguments, a blob of data containing
** an r-tree node, and the number of dimensions the r-tree indexes.
** For a two-dimensional r-tree structure called "rt", to deserialize
** all nodes, a statement like:
**
**   SELECT rtreenode(2, data) FROM rt_node;
**
** The human readable string takes the form of a Tcl list with one
** entry for each cell in the r-tree node. Each entry is itself a
** list, containing the 8-byte rowid/pageno followed by the 
** <num-dimension>*2 coordinates.
*/
static void rtreenode(sqlite3_context *ctx, int nArg, sqlite3_value **apArg){
  char *zText = 0;
  RtreeNode node;
  Rtree tree;
  int ii;

  UNUSED_PARAMETER(nArg);
  memset(&node, 0, sizeof(RtreeNode));
  memset(&tree, 0, sizeof(Rtree));
  tree.nDim = sqlite3_value_int(apArg[0]);
  tree.nBytesPerCell = 8 + 8 * tree.nDim;
  node.zData = (u8 *)sqlite3_value_blob(apArg[1]);

  for(ii=0; ii<NCELL(&node); ii++){
    char zCell[512];
    int nCell = 0;
    RtreeCell cell;
    int jj;

    nodeGetCell(&tree, &node, ii, &cell);
    sqlite3_snprintf(512-nCell,&zCell[nCell],"%lld", cell.iRowid);
    nCell = (int)strlen(zCell);
    for(jj=0; jj<tree.nDim*2; jj++){
#ifndef SQLITE_RTREE_INT_ONLY
      sqlite3_snprintf(512-nCell,&zCell[nCell], " %f",
                       (double)cell.aCoord[jj].f);
#else
      sqlite3_snprintf(512-nCell,&zCell[nCell], " %d",
                       cell.aCoord[jj].i);
#endif
      nCell = (int)strlen(zCell);
    }

    if( zText ){
      char *zTextNew = sqlite3_mprintf("%s {%s}", zText, zCell);
      sqlite3_free(zText);
      zText = zTextNew;
    }else{
      zText = sqlite3_mprintf("{%s}", zCell);
    }
  }
  
  sqlite3_result_text(ctx, zText, -1, sqlite3_free);
}

static void rtreedepth(sqlite3_context *ctx, int nArg, sqlite3_value **apArg){
  UNUSED_PARAMETER(nArg);
  if( sqlite3_value_type(apArg[0])!=SQLITE_BLOB 
   || sqlite3_value_bytes(apArg[0])<2
  ){
    sqlite3_result_error(ctx, "Invalid argument to rtreedepth()", -1); 
  }else{
    u8 *zBlob = (u8 *)sqlite3_value_blob(apArg[0]);
    sqlite3_result_int(ctx, readInt16(zBlob));
  }
}

/*
** Register the r-tree module with database handle db. This creates the
** virtual table module "rtree" and the debugging/analysis scalar 
** function "rtreenode".
*/
int sqlite3RtreeInit(sqlite3 *db){
  const int utf8 = SQLITE_UTF8;
  int rc;

  rc = sqlite3_create_function(db, "rtreenode", 2, utf8, 0, rtreenode, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "rtreedepth", 1, utf8, 0,rtreedepth, 0, 0);
  }
  if( rc==SQLITE_OK ){
#ifdef SQLITE_RTREE_INT_ONLY
    void *c = (void *)RTREE_COORD_INT32;
#else
    void *c = (void *)RTREE_COORD_REAL32;
#endif
    rc = sqlite3_create_module_v2(db, "rtree", &rtreeModule, c, 0);
  }
  if( rc==SQLITE_OK ){
    void *c = (void *)RTREE_COORD_INT32;
    rc = sqlite3_create_module_v2(db, "rtree_i32", &rtreeModule, c, 0);
  }

  return rc;
}

/*
** A version of sqlite3_free() that can be used as a callback. This is used
** in two places - as the destructor for the blob value returned by the
** invocation of a geometry function, and as the destructor for the geometry
** functions themselves.
*/
static void doSqlite3Free(void *p){
  sqlite3_free(p);
}

/*
** Each call to sqlite3_rtree_geometry_callback() creates an ordinary SQLite
** scalar user function. This C function is the callback used for all such
** registered SQL functions.
**
** The scalar user functions return a blob that is interpreted by r-tree
** table MATCH operators.
*/
static void geomCallback(sqlite3_context *ctx, int nArg, sqlite3_value **aArg){
  RtreeGeomCallback *pGeomCtx = (RtreeGeomCallback *)sqlite3_user_data(ctx);
  RtreeMatchArg *pBlob;
  int nBlob;

  nBlob = sizeof(RtreeMatchArg) + (nArg-1)*sizeof(RtreeDValue);
  pBlob = (RtreeMatchArg *)sqlite3_malloc(nBlob);
  if( !pBlob ){
    sqlite3_result_error_nomem(ctx);
  }else{
    int i;
    pBlob->magic = RTREE_GEOMETRY_MAGIC;
    pBlob->xGeom = pGeomCtx->xGeom;
    pBlob->pContext = pGeomCtx->pContext;
    pBlob->nParam = nArg;
    for(i=0; i<nArg; i++){
#ifdef SQLITE_RTREE_INT_ONLY
      pBlob->aParam[i] = sqlite3_value_int64(aArg[i]);
#else
      pBlob->aParam[i] = sqlite3_value_double(aArg[i]);
#endif
    }
    sqlite3_result_blob(ctx, pBlob, nBlob, doSqlite3Free);
  }
}

/*
** Register a new geometry function for use with the r-tree MATCH operator.
*/
int sqlite3_rtree_geometry_callback(
  sqlite3 *db,
  const char *zGeom,
  int (*xGeom)(sqlite3_rtree_geometry *, int, RtreeDValue *, int *),
  void *pContext
){
  RtreeGeomCallback *pGeomCtx;      /* Context object for new user-function */

  /* Allocate and populate the context object. */
  pGeomCtx = (RtreeGeomCallback *)sqlite3_malloc(sizeof(RtreeGeomCallback));
  if( !pGeomCtx ) return SQLITE_NOMEM;
  pGeomCtx->xGeom = xGeom;
  pGeomCtx->pContext = pContext;

  /* Create the new user-function. Register a destructor function to delete
  ** the context object when it is no longer required.  */
  return sqlite3_create_function_v2(db, zGeom, -1, SQLITE_ANY, 
      (void *)pGeomCtx, geomCallback, 0, 0, doSqlite3Free
  );
}

#if !SQLITE_CORE
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_rtree_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi)
  return sqlite3RtreeInit(db);
}
#endif

#endif
