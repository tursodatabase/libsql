/*
** 2014 May 31
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** Low level access to the FTS index stored in the database file. The 
** routines in this file file implement all read and write access to the
** %_data table. Other parts of the system access this functionality via
** the interface defined in fts5Int.h.
*/

#include "fts5Int.h"

/*
** Overview:
**
** The %_data table contains all the FTS indexes for an FTS5 virtual table.
** As well as the main term index, there may be up to 31 prefix indexes.
** The format is similar to FTS3/4, except that:
**
**   * all segment b-tree leaf data is stored in fixed size page records 
**     (e.g. 1000 bytes). A single doclist may span multiple pages. Care is 
**     taken to ensure it is possible to iterate in either direction through 
**     the entries in a doclist, or to seek to a specific entry within a 
**     doclist, without loading it into memory.
**
**   * large doclists that span many pages have associated "doclist index"
**     records that contain a copy of the first docid on each page spanned by
**     the doclist. This is used to speed up seek operations, and merges of
**     large doclists with very small doclists.
**
**   * extra fields in the "structure record" record the state of ongoing
**     incremental merge operations.
**
*/

#define FTS5_OPT_WORK_UNIT  1000  /* Number of leaf pages per optimize step */
#define FTS5_WORK_UNIT      64    /* Number of leaf pages in unit of work */
#define FTS5_CRISIS_MERGE   16    /* Maximum number of segments to merge */

#define FTS5_MIN_DLIDX_SIZE  4    /* Add dlidx if this many empty pages */

/*
** Details:
**
** The %_data table managed by this module,
**
**     CREATE TABLE %_data(id INTEGER PRIMARY KEY, block BLOB);
**
** , contains the following 5 types of records. See the comments surrounding
** the FTS5_*_ROWID macros below for a description of how %_data rowids are 
** assigned to each fo them.
**
** 1. Structure Records:
**
**   The set of segments that make up an index - the index structure - are
**   recorded in a single record within the %_data table. The record consists
**   of a single 32-bit configuration cookie value followed by a list of 
**   SQLite varints. If the FTS table features more than one index (because
**   there are one or more prefix indexes), it is guaranteed that all share
**   the same cookie value.
**
**   Immediately following the configuration cookie, the record begins with
**   three varints:
**
**     + number of levels,
**     + total number of segments on all levels,
**     + value of write counter.
**
**   Then, for each level from 0 to nMax:
**
**     + number of input segments in ongoing merge.
**     + total number of segments in level.
**     + for each segment from oldest to newest:
**         + segment id (always > 0)
**         + b-tree height (1 -> root is leaf, 2 -> root is parent of leaf etc.)
**         + first leaf page number (often 1, always greater than 0)
**         + final leaf page number
**
** 2. The Averages Record:
**
**   A single record within the %_data table. The data is a list of varints.
**   The first value is the number of rows in the index. Then, for each column
**   from left to right, the total number of tokens in the column for all 
**   rows of the table.
**
** 3. Segment leaves:
**
**   TERM DOCLIST FORMAT:
**
**     Most of each segment leaf is taken up by term/doclist data. The 
**     general format of the term/doclist data is:
**
**         varint : size of first term
**         blob:    first term data
**         doclist: first doclist
**         zero-or-more {
**           varint:  number of bytes in common with previous term
**           varint:  number of bytes of new term data (nNew)
**           blob:    nNew bytes of new term data
**           doclist: next doclist
**         }
**
**     doclist format:
**
**         varint:  first rowid
**         poslist: first poslist
**         zero-or-more {
**           varint:  rowid delta (always > 0)
**           poslist: first poslist
**         }
**         0x00 byte
**
**     poslist format:
**
**         varint: size of poslist in bytes. not including this field.
**         collist: collist for column 0
**         zero-or-more {
**           0x01 byte
**           varint: column number (I)
**           collist: collist for column I
**         }
**
**     collist format:
**
**         varint: first offset + 2
**         zero-or-more {
**           varint: offset delta + 2
**         }
**
**   PAGINATION
**
**     The format described above is only accurate if the entire term/doclist
**     data fits on a single leaf page. If this is not the case, the format
**     is changed in two ways:
**
**       + if the first rowid on a page occurs before the first term, it
**         is stored as a literal value:
**
**             varint:  first rowid
**
**       + the first term on each page is stored in the same way as the
**         very first term of the segment:
**
**             varint : size of first term
**             blob:    first term data
**
**     Each leaf page begins with:
**
**       + 2-byte unsigned containing offset to first rowid (or 0).
**       + 2-byte unsigned containing offset to first term (or 0).
**
**   Followed by term/doclist data.
**
** 4. Segment interior nodes:
**
**   The interior nodes turn the list of leaves into a b+tree. 
**
**   Each interior node begins with a varint - the page number of the left
**   most child node. Following this, for each leaf page except the first,
**   the interior nodes contain:
**
**     a) If the leaf page contains at least one term, then a term-prefix that
**        is greater than all previous terms, and less than or equal to the
**        first term on the leaf page.
**
**     b) If the leaf page no terms, a record indicating how many consecutive
**        leaves contain no terms, and whether or not there is an associated
**        by-rowid index record.
**
**   By definition, there is never more than one type (b) record in a row.
**   Type (b) records only ever appear on height=1 pages - immediate parents
**   of leaves. Only type (a) records are pushed to higher levels.
**
**   Term format:
**
**     * Number of bytes in common with previous term plus 2, as a varint.
**     * Number of bytes of new term data, as a varint.
**     * new term data.
**
**   No-term format:
**
**     * either an 0x00 or 0x01 byte. If the value 0x01 is used, then there 
**       is an associated index-by-rowid record.
**     * the number of zero-term leaves as a varint.
**
** 5. Segment doclist indexes:
**
**   A list of varints - the first docid on each page (starting with the
**   first termless page) of the doclist. First element in the list is a
**   literal docid. Each docid thereafter is a (negative) delta. If there
**   are no docids at all on a page, a 0x00 byte takes the place of the
**   delta value.
*/

/*
** Rowids for the averages and structure records in the %_data table.
*/
#define FTS5_AVERAGES_ROWID     1    /* Rowid used for the averages record */
#define FTS5_STRUCTURE_ROWID(iIdx) (10 + (iIdx))     /* For structure records */

/*
** Macros determining the rowids used by segment nodes. All nodes in all
** segments for all indexes (the regular FTS index and any prefix indexes)
** are stored in the %_data table with large positive rowids.
**
** The %_data table may contain up to (1<<FTS5_SEGMENT_INDEX_BITS) 
** indexes - one regular term index and zero or more prefix indexes.
**
** Each segment in an index has a unique id greater than zero.
**
** Each node in a segment b-tree is assigned a "page number" that is unique
** within nodes of its height within the segment (leaf nodes have a height 
** of 0, parents 1, etc.). Page numbers are allocated sequentially so that
** a nodes page number is always one more than its left sibling.
**
** The rowid for a node is then found using the FTS5_SEGMENT_ROWID() macro
** below. The FTS5_SEGMENT_*_BITS macros define the number of bits used
** to encode the three FTS5_SEGMENT_ROWID() arguments. This module returns
** SQLITE_FULL and fails the current operation if they ever prove too small.
*/
#define FTS5_DATA_IDX_B     5     /* Max of 31 prefix indexes */
#define FTS5_DATA_ID_B     16     /* Max seg id number 65535 */
#define FTS5_DATA_HEIGHT_B  5     /* Max b-tree height of 32 */
#define FTS5_DATA_PAGE_B   31     /* Max page number of 2147483648 */

#define FTS5_SEGMENT_ROWID(idx, segid, height, pgno) (                         \
 ((i64)(idx)    << (FTS5_DATA_ID_B + FTS5_DATA_PAGE_B + FTS5_DATA_HEIGHT_B)) + \
 ((i64)(segid)  << (FTS5_DATA_PAGE_B + FTS5_DATA_HEIGHT_B)) +                  \
 ((i64)(height) << (FTS5_DATA_PAGE_B)) +                                       \
 ((i64)(pgno))                                                                 \
)

#if FTS5_MAX_PREFIX_INDEXES > ((1<<FTS5_DATA_IDX_B)-1) 
# error "FTS5_MAX_PREFIX_INDEXES is too large"
#endif

/*
** The height of segment b-trees is actually limited to one less than 
** (1<<HEIGHT_BITS). This is because the rowid address space for nodes
** with such a height is used by doclist indexes.
*/
#define FTS5_SEGMENT_MAX_HEIGHT ((1 << FTS5_DATA_HEIGHT_B)-1)

/*
** The rowid for the doclist index associated with leaf page pgno of segment
** segid in index idx.
*/
#define FTS5_DOCLIST_IDX_ROWID(idx, segid, pgno) \
        FTS5_SEGMENT_ROWID(idx, segid, FTS5_SEGMENT_MAX_HEIGHT, pgno)

#ifdef SQLITE_DEBUG
static int fts5Corrupt() { return SQLITE_CORRUPT_VTAB; }
# define FTS5_CORRUPT fts5Corrupt()
#else
# define FTS5_CORRUPT SQLITE_CORRUPT_VTAB
#endif


typedef struct Fts5BtreeIter Fts5BtreeIter;
typedef struct Fts5BtreeIterLevel Fts5BtreeIterLevel;
typedef struct Fts5ChunkIter Fts5ChunkIter;
typedef struct Fts5Data Fts5Data;
typedef struct Fts5DlidxIter Fts5DlidxIter;
typedef struct Fts5MultiSegIter Fts5MultiSegIter;
typedef struct Fts5NodeIter Fts5NodeIter;
typedef struct Fts5PageWriter Fts5PageWriter;
typedef struct Fts5PosIter Fts5PosIter;
typedef struct Fts5SegIter Fts5SegIter;
typedef struct Fts5DoclistIter Fts5DoclistIter;
typedef struct Fts5SegWriter Fts5SegWriter;
typedef struct Fts5Structure Fts5Structure;
typedef struct Fts5StructureLevel Fts5StructureLevel;
typedef struct Fts5StructureSegment Fts5StructureSegment;

/*
** One object per %_data table.
*/
struct Fts5Index {
  Fts5Config *pConfig;            /* Virtual table configuration */
  char *zDataTbl;                 /* Name of %_data table */
  int nCrisisMerge;               /* Maximum allowed segments per level */
  int nWorkUnit;                  /* Leaf pages in a "unit" of work */

  /*
  ** Variables related to the accumulation of tokens and doclists within the
  ** in-memory hash tables before they are flushed to disk.
  */
  Fts5Hash **apHash;              /* Array of hash tables */
  int nMaxPendingData;            /* Max pending data before flush to disk */
  int nPendingData;               /* Current bytes of pending data */
  i64 iWriteRowid;                /* Rowid for current doc being written */

  /* Error state. */
  int rc;                         /* Current error code */

  /* State used by the fts5DataXXX() functions. */
  sqlite3_blob *pReader;          /* RO incr-blob open on %_data table */
  sqlite3_stmt *pWriter;          /* "INSERT ... %_data VALUES(?,?)" */
  sqlite3_stmt *pDeleter;         /* "DELETE FROM %_data ... id>=? AND id<=?" */
  int nRead;                      /* Total number of blocks read */
};

struct Fts5DoclistIter {
  int bAsc;
  u8 *a;
  int n;
  int i;

  /* Output variables. aPoslist==0 at EOF */
  i64 iRowid;
  u8 *aPoslist;
  int nPoslist;
};

/*
** Each iterator used by external modules is an instance of this type.
*/
struct Fts5IndexIter {
  Fts5Index *pIndex;
  Fts5Structure *pStruct;
  Fts5MultiSegIter *pMulti;
  Fts5DoclistIter *pDoclist;
  Fts5Buffer poslist;             /* Buffer containing current poslist */
};

/*
** A single record read from the %_data table.
*/
struct Fts5Data {
  u8 *p;                          /* Pointer to buffer containing record */
  int n;                          /* Size of record in bytes */
  int nRef;                       /* Ref count */
};

/*
** The contents of the "structure" record for each index are represented
** using an Fts5Structure record in memory. Which uses instances of the 
** other Fts5StructureXXX types as components.
*/
struct Fts5StructureSegment {
  int iSegid;                     /* Segment id */
  int nHeight;                    /* Height of segment b-tree */
  int pgnoFirst;                  /* First leaf page number in segment */
  int pgnoLast;                   /* Last leaf page number in segment */
};
struct Fts5StructureLevel {
  int nMerge;                     /* Number of segments in incr-merge */
  int nSeg;                       /* Total number of segments on level */
  Fts5StructureSegment *aSeg;     /* Array of segments. aSeg[0] is oldest. */
};
struct Fts5Structure {
  u64 nWriteCounter;              /* Total leaves written to level 0 */
  int nLevel;                     /* Number of levels in this index */
  Fts5StructureLevel aLevel[0];   /* Array of nLevel level objects */
};

/*
** An object of type Fts5SegWriter is used to write to segments.
*/
struct Fts5PageWriter {
  int pgno;                       /* Page number for this page */
  Fts5Buffer buf;                 /* Buffer containing page data */
  Fts5Buffer term;                /* Buffer containing previous term on page */
};
struct Fts5SegWriter {
  int iIdx;                       /* Index to write to */
  int iSegid;                     /* Segid to write to */
  int nWriter;                    /* Number of entries in aWriter */
  Fts5PageWriter *aWriter;        /* Array of PageWriter objects */
  i64 iPrevRowid;                 /* Previous docid written to current leaf */
  u8 bFirstRowidInDoclist;        /* True if next rowid is first in doclist */
  u8 bFirstRowidInPage;           /* True if next rowid is first in page */
  int nLeafWritten;               /* Number of leaf pages written */
  int nEmpty;                     /* Number of contiguous term-less nodes */
  Fts5Buffer dlidx;               /* Doclist index */
  i64 iDlidxPrev;                 /* Previous rowid appended to dlidx */
  int bDlidxPrevValid;            /* True if iDlidxPrev is valid */
};

/*
** Object for iterating through the merged results of one or more segments,
** visiting each term/docid pair in the merged data.
**
** nSeg is always a power of two greater than or equal to the number of
** segments that this object is merging data from. Both the aSeg[] and
** aFirst[] arrays are sized at nSeg entries. The aSeg[] array is padded
** with zeroed objects - these are handled as if they were iterators opened
** on empty segments.
**
** The results of comparing segments aSeg[N] and aSeg[N+1], where N is an
** even number, is stored in aFirst[(nSeg+N)/2]. The "result" of the 
** comparison in this context is the index of the iterator that currently
** points to the smaller term/rowid combination. Iterators at EOF are
** considered to be greater than all other iterators.
**
** aFirst[1] contains the index in aSeg[] of the iterator that points to
** the smallest key overall. aFirst[0] is unused. 
*/
struct Fts5MultiSegIter {
  int nSeg;                       /* Size of aSeg[] array */
  int bRev;                       /* True to iterate in reverse order */
  int bSkipEmpty;                 /* True to skip deleted entries */
  Fts5SegIter *aSeg;              /* Array of segment iterators */
  u16 *aFirst;                    /* Current merge state (see above) */
};

/*
** Object for iterating through a single segment, visiting each term/docid
** pair in the segment.
**
** pSeg:
**   The segment to iterate through.
**
** iLeafPgno:
**   Current leaf page number within segment.
**
** iLeafOffset:
**   Byte offset within the current leaf that is one byte past the end of the
**   rowid field of the current entry. Usually this is the size field of the
**   position list data. The exception is if the rowid for the current entry 
**   is the last thing on the leaf page.
**
** pLeaf:
**   Buffer containing current leaf page data. Set to NULL at EOF.
**
** iTermLeafPgno, iTermLeafOffset:
**   Leaf page number containing the last term read from the segment. And
**   the offset immediately following the term data.
**
** flags:
**   Mask of FTS5_SEGITER_XXX values. Interpreted as follows:
**
**   FTS5_SEGITER_ONETERM:
**     If set, set the iterator to point to EOF after the current doclist 
**     has been exhausted. Do not proceed to the next term in the segment.
**
**   FTS5_SEGITER_REVERSE:
**     This flag is only ever set if FTS5_SEGITER_ONETERM is also set. If
**     it is set, iterate through docids in ascending order instead of the
**     default descending order.
**
** iRowidOffset/nRowidOffset/aRowidOffset:
**     These are used if the FTS5_SEGITER_REVERSE flag is set.
**
**     Each time a new page is loaded, the iterator is set to point to the
**     final rowid. Additionally, the aRowidOffset[] array is populated 
**     with the byte offsets of all relevant rowid fields on the page. 
*/
struct Fts5SegIter {
  Fts5StructureSegment *pSeg;     /* Segment to iterate through */
  int iIdx;                       /* Byte offset within current leaf */
  int flags;                      /* Mask of configuration flags */
  int iLeafPgno;                  /* Current leaf page number */
  Fts5Data *pLeaf;                /* Current leaf data */
  int iLeafOffset;                /* Byte offset within current leaf */

  /* The page and offset from which the current term was read. The offset 
  ** is the offset of the first rowid in the current doclist.  */
  int iTermLeafPgno;
  int iTermLeafOffset;

  /* The following are only used if the FTS5_SEGITER_REVERSE flag is set. */
  int iRowidOffset;               /* Current entry in aRowidOffset[] */
  int nRowidOffset;               /* Allocated size of aRowidOffset[] array */
  int *aRowidOffset;              /* Array of offset to rowid fields */

  Fts5DlidxIter *pDlidx;          /* If there is a doclist-index */

  /* Variables populated based on current entry. */
  Fts5Buffer term;                /* Current term */
  i64 iRowid;                     /* Current rowid */
};

#define FTS5_SEGITER_ONETERM 0x01
#define FTS5_SEGITER_REVERSE 0x02


/*
** Object for iterating through paginated data.
*/
struct Fts5ChunkIter {
  Fts5Data *pLeaf;                /* Current leaf data. NULL -> EOF. */
  i64 iLeafRowid;                 /* Absolute rowid of current leaf */
  int nRem;                       /* Remaining bytes of data to read */

  /* Output parameters */
  u8 *p;                          /* Pointer to chunk of data */
  int n;                          /* Size of buffer p in bytes */
};

/*
** Object for iterating through a single position list on disk.
*/
struct Fts5PosIter {
  Fts5ChunkIter chunk;            /* Current chunk of data */
  int iOff;                       /* Offset within chunk data */

  int iCol;
  int iPos;
};

/*
** Object for iterating through the conents of a single internal node in 
** memory.
*/
struct Fts5NodeIter {
  /* Internal. Set and managed by fts5NodeIterXXX() functions. Except, 
  ** the EOF test for the iterator is (Fts5NodeIter.aData==0).  */
  const u8 *aData;
  int nData;
  int iOff;

  /* Output variables */
  Fts5Buffer term;
  int nEmpty;
  int iChild;
  int bDlidx;
};

/*
** An instance of the following type is used to iterate through the contents
** of a doclist-index record.
**
** pData:
**   Record containing the doclist-index data.
**
** bEof:
**   Set to true once iterator has reached EOF.
**
** iOff:
**   Set to the current offset within record pData.
*/
struct Fts5DlidxIter {
  Fts5Data *pData;              /* Data for doclist index, if any */
  int iOff;                     /* Current offset into pDlidx */
  int bEof;                     /* At EOF already */
  int iFirstOff;                /* Used by reverse iterators only */

  /* Output variables */
  int iLeafPgno;                /* Page number of current leaf page */
  i64 iRowid;                   /* First rowid on leaf iLeafPgno */
};


/*
** An Fts5BtreeIter object is used to iterate through all entries in the
** b-tree hierarchy belonging to a single fts5 segment. In this case the
** "b-tree hierarchy" is all b-tree nodes except leaves. Each entry in the
** b-tree hierarchy consists of the following:
**
**   iLeaf:  The page number of the leaf page the entry points to.
**
**   term:   A split-key that all terms on leaf page $iLeaf must be greater
**           than or equal to. The "term" associated with the first b-tree
**           hierarchy entry (the one that points to leaf page 1) is always 
**           an empty string.
**
**   nEmpty: The number of empty (termless) leaf pages that immediately
**           following iLeaf.
**
** The Fts5BtreeIter object is only used as part of the integrity-check code.
*/
struct Fts5BtreeIterLevel {
  Fts5NodeIter s;                 /* Iterator for the current node */
  Fts5Data *pData;                /* Data for the current node */
};
struct Fts5BtreeIter {
  Fts5Index *p;                   /* FTS5 backend object */
  Fts5StructureSegment *pSeg;     /* Iterate through this segment's b-tree */
  int iIdx;                       /* Index pSeg belongs to */
  int nLvl;                       /* Size of aLvl[] array */
  Fts5BtreeIterLevel *aLvl;       /* Level for each tier of b-tree */

  /* Output variables */
  Fts5Buffer term;                /* Current term */
  int iLeaf;                      /* Leaf containing terms >= current term */
  int nEmpty;                     /* Number of "empty" leaves following iLeaf */
  int bEof;                       /* Set to true at EOF */
  int bDlidx;                     /* True if there exists a dlidx */
};


static void fts5PutU16(u8 *aOut, u16 iVal){
  aOut[0] = (iVal>>8);
  aOut[1] = (iVal&0xFF);
}

static u16 fts5GetU16(const u8 *aIn){
  return ((u16)aIn[0] << 8) + aIn[1];
}

/*
** Allocate and return a buffer at least nByte bytes in size.
**
** If an OOM error is encountered, return NULL and set the error code in
** the Fts5Index handle passed as the first argument.
*/
static void *fts5IdxMalloc(Fts5Index *p, int nByte){
  void *pRet = 0;
  if( p->rc==SQLITE_OK ){
    pRet = sqlite3_malloc(nByte);
    if( pRet==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      memset(pRet, 0, nByte);
    }
  }
  return pRet;
}

/*
** Compare the contents of the pLeft buffer with the pRight/nRight blob.
**
** Return -ve if pLeft is smaller than pRight, 0 if they are equal or
** +ve if pRight is smaller than pLeft. In other words:
**
**     res = *pLeft - *pRight
*/
static int fts5BufferCompareBlob(
  Fts5Buffer *pLeft,              /* Left hand side of comparison */
  const u8 *pRight, int nRight    /* Right hand side of comparison */
){
  int nCmp = MIN(pLeft->n, nRight);
  int res = memcmp(pLeft->p, pRight, nCmp);
  return (res==0 ? (pLeft->n - nRight) : res);
}

#if 0
static int fts5CompareBlob(
  const u8 *pLeft, int nLeft,
  const u8 *pRight, int nRight
){
  int nCmp = MIN(nLeft, nRight);
  int res = memcmp(pLeft, pRight, nCmp);
  return (res==0 ? (nLeft - nRight) : res);
}
#endif

/*
** Compare the contents of the two buffers using memcmp(). If one buffer
** is a prefix of the other, it is considered the lesser.
**
** Return -ve if pLeft is smaller than pRight, 0 if they are equal or
** +ve if pRight is smaller than pLeft. In other words:
**
**     res = *pLeft - *pRight
*/
static int fts5BufferCompare(Fts5Buffer *pLeft, Fts5Buffer *pRight){
  int nCmp = MIN(pLeft->n, pRight->n);
  int res = memcmp(pLeft->p, pRight->p, nCmp);
  return (res==0 ? (pLeft->n - pRight->n) : res);
}


/*
** Close the read-only blob handle, if it is open.
*/
static void fts5CloseReader(Fts5Index *p){
  if( p->pReader ){
    sqlite3_blob *pReader = p->pReader;
    p->pReader = 0;
    sqlite3_blob_close(pReader);
  }
}

static Fts5Data *fts5DataReadOrBuffer(
  Fts5Index *p, 
  Fts5Buffer *pBuf, 
  i64 iRowid
){
  Fts5Data *pRet = 0;
  if( p->rc==SQLITE_OK ){
    int rc = SQLITE_OK;

#if 0
Fts5Buffer buf = {0,0,0};
fts5DebugRowid(&rc, &buf, iRowid);
fprintf(stdout, "read: %s\n", buf.p);
fflush(stdout);
sqlite3_free(buf.p);
#endif
    if( p->pReader ){
      /* This call may return SQLITE_ABORT if there has been a savepoint
      ** rollback since it was last used. In this case a new blob handle
      ** is required.  */
      rc = sqlite3_blob_reopen(p->pReader, iRowid);
      if( rc==SQLITE_ABORT ){
        fts5CloseReader(p);
        rc = SQLITE_OK;
      }
    }

    /* If the blob handle is not yet open, open and seek it. Otherwise, use
    ** the blob_reopen() API to reseek the existing blob handle.  */
    if( p->pReader==0 ){
      Fts5Config *pConfig = p->pConfig;
      rc = sqlite3_blob_open(pConfig->db, 
          pConfig->zDb, p->zDataTbl, "block", iRowid, 0, &p->pReader
      );
    }

    if( rc==SQLITE_OK ){
      u8 *aOut;                   /* Read blob data into this buffer */
      int nByte = sqlite3_blob_bytes(p->pReader);
      if( pBuf ){
        fts5BufferZero(pBuf);
        fts5BufferGrow(&rc, pBuf, nByte);
        aOut = pBuf->p;
        pBuf->n = nByte;
      }else{
        pRet = (Fts5Data*)sqlite3Fts5MallocZero(&rc, nByte+sizeof(Fts5Data));
        if( pRet ){
          pRet->n = nByte;
          aOut = pRet->p = (u8*)&pRet[1];
          pRet->nRef = 1;
        }
      }

      if( rc==SQLITE_OK ){
        rc = sqlite3_blob_read(p->pReader, aOut, nByte, 0);
      }
      if( rc!=SQLITE_OK ){
        sqlite3_free(pRet);
        pRet = 0;
      }
    }
    p->rc = rc;
    p->nRead++;
  }

  return pRet;
}

/*
** Retrieve a record from the %_data table.
**
** If an error occurs, NULL is returned and an error left in the 
** Fts5Index object.
*/
static Fts5Data *fts5DataRead(Fts5Index *p, i64 iRowid){
  Fts5Data *pRet = fts5DataReadOrBuffer(p, 0, iRowid);
  assert( (pRet==0)==(p->rc!=SQLITE_OK) );
  return pRet;
}

/*
** Read a record from the %_data table into the buffer supplied as the
** second argument.
**
** If an error occurs, an error is left in the Fts5Index object. If an
** error has already occurred when this function is called, it is a 
** no-op.
*/
static void fts5DataBuffer(Fts5Index *p, Fts5Buffer *pBuf, i64 iRowid){
  (void)fts5DataReadOrBuffer(p, pBuf, iRowid);
}

/*
** Release a reference to data record returned by an earlier call to
** fts5DataRead().
*/
static void fts5DataRelease(Fts5Data *pData){
  if( pData ){
    assert( pData->nRef>0 );
    pData->nRef--;
    if( pData->nRef==0 ) sqlite3_free(pData);
  }
}

static void fts5DataReference(Fts5Data *pData){
  pData->nRef++;
}

/*
** INSERT OR REPLACE a record into the %_data table.
*/
static void fts5DataWrite(Fts5Index *p, i64 iRowid, const u8 *pData, int nData){
  if( p->rc!=SQLITE_OK ) return;

  if( p->pWriter==0 ){
    int rc;
    Fts5Config *pConfig = p->pConfig;
    char *zSql = sqlite3_mprintf(
        "REPLACE INTO '%q'.%Q(id, block) VALUES(?,?)", pConfig->zDb, p->zDataTbl
    );
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(pConfig->db, zSql, -1, &p->pWriter, 0);
      sqlite3_free(zSql);
    }
    if( rc!=SQLITE_OK ){
      p->rc = rc;
      return;
    }
  }

  sqlite3_bind_int64(p->pWriter, 1, iRowid);
  sqlite3_bind_blob(p->pWriter, 2, pData, nData, SQLITE_STATIC);
  sqlite3_step(p->pWriter);
  p->rc = sqlite3_reset(p->pWriter);
}

/*
** Execute the following SQL:
**
**     DELETE FROM %_data WHERE id BETWEEN $iFirst AND $iLast
*/
static void fts5DataDelete(Fts5Index *p, i64 iFirst, i64 iLast){
  if( p->rc!=SQLITE_OK ) return;

  if( p->pDeleter==0 ){
    int rc;
    Fts5Config *pConfig = p->pConfig;
    char *zSql = sqlite3_mprintf(
        "DELETE FROM '%q'.%Q WHERE id>=? AND id<=?", pConfig->zDb, p->zDataTbl
    );
    if( zSql==0 ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(pConfig->db, zSql, -1, &p->pDeleter, 0);
      sqlite3_free(zSql);
    }
    if( rc!=SQLITE_OK ){
      p->rc = rc;
      return;
    }
  }

  sqlite3_bind_int64(p->pDeleter, 1, iFirst);
  sqlite3_bind_int64(p->pDeleter, 2, iLast);
  sqlite3_step(p->pDeleter);
  p->rc = sqlite3_reset(p->pDeleter);
}

/*
** Close the sqlite3_blob handle used to read records from the %_data table.
** And discard any cached reads. This function is called at the end of
** a read transaction or when any sub-transaction is rolled back.
*/
#if 0
static void fts5DataReset(Fts5Index *p){
  if( p->pReader ){
    sqlite3_blob_close(p->pReader);
    p->pReader = 0;
  }
}
#endif

/*
** Remove all records associated with segment iSegid in index iIdx.
*/
static void fts5DataRemoveSegment(Fts5Index *p, int iIdx, int iSegid){
  i64 iFirst = FTS5_SEGMENT_ROWID(iIdx, iSegid, 0, 0);
  i64 iLast = FTS5_SEGMENT_ROWID(iIdx, iSegid+1, 0, 0)-1;
  fts5DataDelete(p, iFirst, iLast);
}

/*
** Release a reference to an Fts5Structure object returned by an earlier 
** call to fts5StructureRead() or fts5StructureDecode().
*/
static void fts5StructureRelease(Fts5Structure *pStruct){
  if( pStruct ){
    int i;
    for(i=0; i<pStruct->nLevel; i++){
      sqlite3_free(pStruct->aLevel[i].aSeg);
    }
    sqlite3_free(pStruct);
  }
}

/*
** Deserialize and return the structure record currently stored in serialized
** form within buffer pData/nData.
**
** The Fts5Structure.aLevel[] and each Fts5StructureLevel.aSeg[] array
** are over-allocated by one slot. This allows the structure contents
** to be more easily edited.
**
** If an error occurs, *ppOut is set to NULL and an SQLite error code
** returned. Otherwise, *ppOut is set to point to the new object and
** SQLITE_OK returned.
*/
static int fts5StructureDecode(
  const u8 *pData,                /* Buffer containing serialized structure */
  int nData,                      /* Size of buffer pData in bytes */
  int *piCookie,                  /* Configuration cookie value */
  Fts5Structure **ppOut           /* OUT: Deserialized object */
){
  int rc = SQLITE_OK;
  int i = 0;
  int iLvl;
  int nLevel = 0;
  int nSegment = 0;
  int nByte;                      /* Bytes of space to allocate at pRet */
  Fts5Structure *pRet = 0;        /* Structure object to return */

  /* Grab the cookie value */
  if( piCookie ) *piCookie = sqlite3Fts5Get32(pData);
  i = 4;

  /* Read the total number of levels and segments from the start of the
  ** structure record.  */
  i += getVarint32(&pData[i], nLevel);
  i += getVarint32(&pData[i], nSegment);
  nByte = (
      sizeof(Fts5Structure) +                    /* Main structure */
      sizeof(Fts5StructureLevel) * (nLevel)      /* aLevel[] array */
  );
  pRet = (Fts5Structure*)sqlite3Fts5MallocZero(&rc, nByte);

  if( pRet ){
    pRet->nLevel = nLevel;
    i += sqlite3GetVarint(&pData[i], &pRet->nWriteCounter);

    for(iLvl=0; rc==SQLITE_OK && iLvl<nLevel; iLvl++){
      Fts5StructureLevel *pLvl = &pRet->aLevel[iLvl];
      int nTotal;
      int iSeg;

      i += getVarint32(&pData[i], pLvl->nMerge);
      i += getVarint32(&pData[i], nTotal);
      assert( nTotal>=pLvl->nMerge );
      pLvl->aSeg = (Fts5StructureSegment*)sqlite3Fts5MallocZero(&rc, 
          nTotal * sizeof(Fts5StructureSegment)
      );

      if( rc==SQLITE_OK ){
        pLvl->nSeg = nTotal;
        for(iSeg=0; iSeg<nTotal; iSeg++){
          i += getVarint32(&pData[i], pLvl->aSeg[iSeg].iSegid);
          i += getVarint32(&pData[i], pLvl->aSeg[iSeg].nHeight);
          i += getVarint32(&pData[i], pLvl->aSeg[iSeg].pgnoFirst);
          i += getVarint32(&pData[i], pLvl->aSeg[iSeg].pgnoLast);
        }
      }else{
        fts5StructureRelease(pRet);
        pRet = 0;
      }
    }
  }

  *ppOut = pRet;
  return rc;
}

/*
**
*/
static void fts5StructureAddLevel(int *pRc, Fts5Structure **ppStruct){
  if( *pRc==SQLITE_OK ){
    Fts5Structure *pStruct = *ppStruct;
    int nLevel = pStruct->nLevel;
    int nByte = (
        sizeof(Fts5Structure) +                  /* Main structure */
        sizeof(Fts5StructureLevel) * (nLevel+1)  /* aLevel[] array */
    );

    pStruct = sqlite3_realloc(pStruct, nByte);
    if( pStruct ){
      memset(&pStruct->aLevel[nLevel], 0, sizeof(Fts5StructureLevel));
      pStruct->nLevel++;
      *ppStruct = pStruct;
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }
}

/*
** Extend level iLvl so that there is room for at least nExtra more
** segments.
*/
static void fts5StructureExtendLevel(
  int *pRc, 
  Fts5Structure *pStruct, 
  int iLvl, 
  int nExtra, 
  int bInsert
){
  if( *pRc==SQLITE_OK ){
    Fts5StructureLevel *pLvl = &pStruct->aLevel[iLvl];
    Fts5StructureSegment *aNew;
    int nByte;

    nByte = (pLvl->nSeg + nExtra) * sizeof(Fts5StructureSegment);
    aNew = sqlite3_realloc(pLvl->aSeg, nByte);
    if( aNew ){
      if( bInsert==0 ){
        memset(&aNew[pLvl->nSeg], 0, sizeof(Fts5StructureSegment) * nExtra);
      }else{
        int nMove = pLvl->nSeg * sizeof(Fts5StructureSegment);
        memmove(&aNew[nExtra], aNew, nMove);
        memset(aNew, 0, sizeof(Fts5StructureSegment) * nExtra);
      }
      pLvl->aSeg = aNew;
    }else{
      *pRc = SQLITE_NOMEM;
    }
  }
}

/*
** Read, deserialize and return the structure record for index iIdx.
**
** The Fts5Structure.aLevel[] and each Fts5StructureLevel.aSeg[] array
** are over-allocated as described for function fts5StructureDecode() 
** above.
**
** If an error occurs, NULL is returned and an error code left in the
** Fts5Index handle. If an error has already occurred when this function
** is called, it is a no-op.
*/
static Fts5Structure *fts5StructureRead(Fts5Index *p, int iIdx){
  Fts5Config *pConfig = p->pConfig;
  Fts5Structure *pRet = 0;        /* Object to return */
  Fts5Data *pData;                /* %_data entry containing structure record */
  int iCookie;                    /* Configuration cookie */

  assert( iIdx<=pConfig->nPrefix );
  pData = fts5DataRead(p, FTS5_STRUCTURE_ROWID(iIdx));
  if( !pData ) return 0;
  p->rc = fts5StructureDecode(pData->p, pData->n, &iCookie, &pRet);

  if( p->rc==SQLITE_OK && p->pConfig->iCookie!=iCookie ){
    p->rc = sqlite3Fts5ConfigLoad(p->pConfig, iCookie);
  }

  fts5DataRelease(pData);
  if( p->rc!=SQLITE_OK ){
    fts5StructureRelease(pRet);
    pRet = 0;
  }
  return pRet;
}

/*
** Return the total number of segments in index structure pStruct.
*/
static int fts5StructureCountSegments(Fts5Structure *pStruct){
  int nSegment = 0;               /* Total number of segments */
  int iLvl;                       /* Used to iterate through levels */

  for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
    nSegment += pStruct->aLevel[iLvl].nSeg;
  }

  return nSegment;
}

/*
** Serialize and store the "structure" record for index iIdx.
**
** If an error occurs, leave an error code in the Fts5Index object. If an
** error has already occurred, this function is a no-op.
*/
static void fts5StructureWrite(Fts5Index *p, int iIdx, Fts5Structure *pStruct){
  if( p->rc==SQLITE_OK ){
    int nSegment;                 /* Total number of segments */
    Fts5Buffer buf;               /* Buffer to serialize record into */
    int iLvl;                     /* Used to iterate through levels */
    int iCookie;                  /* Cookie value to store */

    nSegment = fts5StructureCountSegments(pStruct);
    memset(&buf, 0, sizeof(Fts5Buffer));

    /* Append the current configuration cookie */
    iCookie = p->pConfig->iCookie;
    if( iCookie<0 ) iCookie = 0;
    fts5BufferAppend32(&p->rc, &buf, iCookie);

    fts5BufferAppendVarint(&p->rc, &buf, pStruct->nLevel);
    fts5BufferAppendVarint(&p->rc, &buf, nSegment);
    fts5BufferAppendVarint(&p->rc, &buf, (i64)pStruct->nWriteCounter);

    for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
      int iSeg;                     /* Used to iterate through segments */
      Fts5StructureLevel *pLvl = &pStruct->aLevel[iLvl];
      fts5BufferAppendVarint(&p->rc, &buf, pLvl->nMerge);
      fts5BufferAppendVarint(&p->rc, &buf, pLvl->nSeg);
      assert( pLvl->nMerge<=pLvl->nSeg );

      for(iSeg=0; iSeg<pLvl->nSeg; iSeg++){
        fts5BufferAppendVarint(&p->rc, &buf, pLvl->aSeg[iSeg].iSegid);
        fts5BufferAppendVarint(&p->rc, &buf, pLvl->aSeg[iSeg].nHeight);
        fts5BufferAppendVarint(&p->rc, &buf, pLvl->aSeg[iSeg].pgnoFirst);
        fts5BufferAppendVarint(&p->rc, &buf, pLvl->aSeg[iSeg].pgnoLast);
      }
    }

    fts5DataWrite(p, FTS5_STRUCTURE_ROWID(iIdx), buf.p, buf.n);
    fts5BufferFree(&buf);
  }
}

#if 0
static void fts5PrintStructure(const char *zCaption, Fts5Structure *pStruct){
  int rc = SQLITE_OK;
  Fts5Buffer buf;
  memset(&buf, 0, sizeof(buf));
  fts5DebugStructure(&rc, &buf, pStruct);
  fprintf(stdout, "%s: %s\n", zCaption, buf.p);
  fflush(stdout);
  fts5BufferFree(&buf);
}
#else
# define fts5PrintStructure(x,y)
#endif

static int fts5SegmentSize(Fts5StructureSegment *pSeg){
  return 1 + pSeg->pgnoLast - pSeg->pgnoFirst;
}

/*
** Return a copy of index structure pStruct. Except, promote as many segments
** as possible to level iPromote. If an OOM occurs, NULL is returned.
*/
static void fts5StructurePromoteTo(
  Fts5Index *p,
  int iPromote,
  int szPromote,
  Fts5Structure *pStruct
){
  int il, is;
  Fts5StructureLevel *pOut = &pStruct->aLevel[iPromote];

  for(il=iPromote+1; il<pStruct->nLevel; il++){
    Fts5StructureLevel *pLvl = &pStruct->aLevel[il];
    for(is=pLvl->nSeg-1; is>=0; is--){
      int sz = fts5SegmentSize(&pLvl->aSeg[is]);
      if( sz>szPromote ) return;
      fts5StructureExtendLevel(&p->rc, pStruct, iPromote, 1, 1);
      if( p->rc ) return;
      memcpy(pOut->aSeg, &pLvl->aSeg[is], sizeof(Fts5StructureSegment));
      pOut->nSeg++;
      pLvl->nSeg--;
    }
  }
}

/*
** A new segment has just been written to level iLvl of index structure
** pStruct. This function determines if any segments should be promoted
** as a result. Segments are promoted in two scenarios:
**
**   a) If the segment just written is smaller than one or more segments
**      within the previous populated level, it is promoted to the previous
**      populated level.
**
**   b) If the segment just written is larger than the newest segment on
**      the next populated level, then that segment, and any other adjacent
**      segments that are also smaller than the one just written, are 
**      promoted. 
**
** If one or more segments are promoted, the structure object is updated
** to reflect this.
*/
static void fts5StructurePromote(
  Fts5Index *p,                   /* FTS5 backend object */
  int iLvl,                       /* Index level just updated */
  Fts5Structure *pStruct          /* Index structure */
){
  if( p->rc==SQLITE_OK ){
    int iTst;
    int iPromote = -1;
    int szPromote;                /* Promote anything this size or smaller */
    Fts5StructureSegment *pSeg;   /* Segment just written */
    Fts5StructureLevel *pTst;
    int szSeg;                    /* Size of segment just written */


    pSeg = &pStruct->aLevel[iLvl].aSeg[pStruct->aLevel[iLvl].nSeg-1];
    szSeg = (1 + pSeg->pgnoLast - pSeg->pgnoFirst);

    /* Check for condition (a) */
    for(iTst=iLvl-1; iTst>=0 && pStruct->aLevel[iTst].nSeg==0; iTst--);
    pTst = &pStruct->aLevel[iTst];
    if( iTst>=0 && pTst->nMerge==0 ){
      int i;
      int szMax = 0;
      for(i=0; i<pTst->nSeg; i++){
        int sz = pTst->aSeg[i].pgnoLast - pTst->aSeg[i].pgnoFirst + 1;
        if( sz>szMax ) szMax = sz;
      }
      if( szMax>=szSeg ){
        /* Condition (a) is true. Promote the newest segment on level 
        ** iLvl to level iTst.  */
        iPromote = iTst;
        szPromote = szMax;
      }
    }

    /* Check for condition (b) */
    if( iPromote<0 ){
      Fts5StructureLevel *pTst;
      for(iTst=iLvl+1; iTst<pStruct->nLevel; iTst++){
        pTst = &pStruct->aLevel[iTst];
        if( pTst->nSeg ) break;
      }
      if( iTst<pStruct->nLevel && pTst->nMerge==0 ){
        Fts5StructureSegment *pSeg2 = &pTst->aSeg[pTst->nSeg-1];
        int sz = pSeg2->pgnoLast - pSeg2->pgnoFirst + 1;
        if( sz<=szSeg ){
          iPromote = iLvl;
          szPromote = szSeg;
        }
      }
    }

    /* If iPromote is greater than or equal to zero at this point, then it
    ** is the level number of a level to which segments that consist of
    ** szPromote or fewer pages should be promoted. */ 
    if( iPromote>=0 ){
      fts5PrintStructure("BEFORE", pStruct);
      fts5StructurePromoteTo(p, iPromote, szPromote, pStruct);
      fts5PrintStructure("AFTER", pStruct);
    }
  }
}


/*
** If the pIter->iOff offset currently points to an entry indicating one
** or more term-less nodes, advance past it and set pIter->nEmpty to
** the number of empty child nodes.
*/
static void fts5NodeIterGobbleNEmpty(Fts5NodeIter *pIter){
  if( pIter->iOff<pIter->nData && 0==(pIter->aData[pIter->iOff] & 0xfe) ){
    pIter->bDlidx = pIter->aData[pIter->iOff] & 0x01;
    pIter->iOff++;
    pIter->iOff += getVarint32(&pIter->aData[pIter->iOff], pIter->nEmpty);
  }else{
    pIter->nEmpty = 0;
    pIter->bDlidx = 0;
  }
}

/*
** Advance to the next entry within the node.
*/
static void fts5NodeIterNext(int *pRc, Fts5NodeIter *pIter){
  if( pIter->iOff>=pIter->nData ){
    pIter->aData = 0;
    pIter->iChild += pIter->nEmpty;
  }else{
    int nPre, nNew;
    pIter->iOff += getVarint32(&pIter->aData[pIter->iOff], nPre);
    pIter->iOff += getVarint32(&pIter->aData[pIter->iOff], nNew);
    pIter->term.n = nPre-2;
    fts5BufferAppendBlob(pRc, &pIter->term, nNew, pIter->aData+pIter->iOff);
    pIter->iOff += nNew;
    pIter->iChild += (1 + pIter->nEmpty);
    fts5NodeIterGobbleNEmpty(pIter);
    if( *pRc ) pIter->aData = 0;
  }
}


/*
** Initialize the iterator object pIter to iterate through the internal
** segment node in pData.
*/
static void fts5NodeIterInit(const u8 *aData, int nData, Fts5NodeIter *pIter){
  memset(pIter, 0, sizeof(*pIter));
  pIter->aData = aData;
  pIter->nData = nData;
  pIter->iOff = getVarint32(aData, pIter->iChild);
  fts5NodeIterGobbleNEmpty(pIter);
}

/*
** Free any memory allocated by the iterator object.
*/
static void fts5NodeIterFree(Fts5NodeIter *pIter){
  fts5BufferFree(&pIter->term);
}

/*
** The iterator passed as the first argument has the following fields set
** as follows. This function sets up the rest of the iterator so that it
** points to the first rowid in the doclist-index.
**
**   pData: pointer to doclist-index record, 
**   iLeafPgno: page number that this doclist-index is associated with.
*/
static int fts5DlidxIterFirst(Fts5DlidxIter *pIter){
  Fts5Data *pData = pIter->pData;
  int i;

  assert( pIter->pData );
  assert( pIter->iLeafPgno>0 );

  /* Count the number of leading 0x00 bytes. Then set iLeafPgno. */
  for(i=0; i<pData->n; i++){ 
    if( pData->p[i] ) break;
  }
  pIter->iLeafPgno += (i+1);
  pIter->iOff = i;

  /* Unless we are already at the end of the doclist-index, load the first
  ** rowid value.  */
  if( pIter->iOff<pData->n ){
    pIter->iOff += getVarint(&pData->p[pIter->iOff], (u64*)&pIter->iRowid);
  }else{
    pIter->bEof = 1;
  }
  pIter->iFirstOff = pIter->iOff;
  return pIter->bEof;
}

/*
** Advance the iterator passed as the only argument.
*/
static int fts5DlidxIterNext(Fts5DlidxIter *pIter){
  Fts5Data *pData = pIter->pData;
  int iOff;

  for(iOff=pIter->iOff; iOff<pData->n; iOff++){
    if( pData->p[iOff] ) break; 
  }

  if( iOff<pData->n ){
    i64 iVal;
    pIter->iLeafPgno += (iOff - pIter->iOff) + 1;
    iOff += getVarint(&pData->p[iOff], (u64*)&iVal);
    pIter->iRowid -= iVal;
    pIter->iOff = iOff;
  }else{
    pIter->bEof = 1;
  }

  return pIter->bEof;
}

static int fts5DlidxIterEof(Fts5Index *p, Fts5DlidxIter *pIter){
  return (p->rc!=SQLITE_OK || pIter->bEof);
}

static void fts5DlidxIterLast(Fts5DlidxIter *pIter){
  if( fts5DlidxIterFirst(pIter)==0 ){
    while( 0==fts5DlidxIterNext(pIter) );
    pIter->bEof = 0;
  }
}

static int fts5DlidxIterPrev(Fts5DlidxIter *pIter){
  int iOff = pIter->iOff;

  assert( pIter->bEof==0 );
  if( iOff<=pIter->iFirstOff ){
    pIter->bEof = 1;
  }else{
    u8 *a = pIter->pData->p;
    i64 iVal;
    int iLimit;

    /* Currently iOff points to the first byte of a varint. This block 
    ** decrements iOff until it points to the first byte of the previous 
    ** varint. Taking care not to read any memory locations that occur
    ** before the buffer in memory.  */
    iLimit = (iOff>9 ? iOff-9 : 0);
    for(iOff--; iOff>iLimit; iOff--){
      if( (a[iOff-1] & 0x80)==0 ) break;
    }

    getVarint(&a[iOff], (u64*)&iVal);
    pIter->iRowid += iVal;
    pIter->iLeafPgno--;

    while( iOff>pIter->iFirstOff 
        && a[iOff-1]==0x00 && (a[iOff-2] & 0x80)==0 
    ){
      iOff--;
      pIter->iLeafPgno--;
    }
    pIter->iOff = iOff;
  }

  return pIter->bEof;
}

static void fts5DlidxIterInit(
  Fts5Index *p,                   /* Fts5 Backend to iterate within */
  int bRev,                       /* True for ORDER BY ASC */
  int iIdx, int iSegid,           /* Segment iSegid within index iIdx */
  int iLeafPgno,                  /* Leaf page number to load dlidx for */
  Fts5DlidxIter **ppIter          /* OUT: Populated iterator */
){
  Fts5DlidxIter *pIter = *ppIter;
  Fts5Data *pDlidx;

  pDlidx = fts5DataRead(p, FTS5_DOCLIST_IDX_ROWID(iIdx, iSegid, iLeafPgno));
  if( pDlidx==0 ) return;
  if( pIter==0 ){
    *ppIter = pIter = (Fts5DlidxIter*)fts5IdxMalloc(p, sizeof(Fts5DlidxIter));
    if( pIter==0 ){ 
      fts5DataRelease(pDlidx);
      return;
    }
  }else{
    memset(pIter, 0, sizeof(Fts5DlidxIter));
  }

  pIter->pData = pDlidx;
  pIter->iLeafPgno = iLeafPgno;
  if( bRev==0 ){
    fts5DlidxIterFirst(pIter);
  }else{
    fts5DlidxIterLast(pIter);
  }
}

/*
** Free a doclist-index iterator object allocated by fts5DlidxIterInit().
*/
static void fts5DlidxIterFree(Fts5DlidxIter *pIter){
  if( pIter ){
    fts5DataRelease(pIter->pData);
    sqlite3_free(pIter);
  }
}

/*
** Load the next leaf page into the segment iterator.
*/
static void fts5SegIterNextPage(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter              /* Iterator to advance to next page */
){
  Fts5StructureSegment *pSeg = pIter->pSeg;
  if( pIter->pLeaf ) fts5DataRelease(pIter->pLeaf);
  pIter->iLeafPgno++;
  if( pIter->iLeafPgno<=pSeg->pgnoLast ){
    pIter->pLeaf = fts5DataRead(p, 
        FTS5_SEGMENT_ROWID(pIter->iIdx, pSeg->iSegid, 0, pIter->iLeafPgno)
    );
  }else{
    pIter->pLeaf = 0;
  }
}

/*
** Leave pIter->iLeafOffset as the offset to the size field of the first
** position list. The position list belonging to document pIter->iRowid.
*/
static void fts5SegIterLoadTerm(Fts5Index *p, Fts5SegIter *pIter, int nKeep){
  u8 *a = pIter->pLeaf->p;        /* Buffer to read data from */
  int iOff = pIter->iLeafOffset;  /* Offset to read at */
  int nNew;                       /* Bytes of new data */

  iOff += getVarint32(&a[iOff], nNew);
  pIter->term.n = nKeep;
  fts5BufferAppendBlob(&p->rc, &pIter->term, nNew, &a[iOff]);
  iOff += nNew;
  pIter->iTermLeafOffset = iOff;
  pIter->iTermLeafPgno = pIter->iLeafPgno;
  if( iOff>=pIter->pLeaf->n ){
    fts5SegIterNextPage(p, pIter);
    if( pIter->pLeaf==0 ){
      if( p->rc==SQLITE_OK ) p->rc = FTS5_CORRUPT;
      return;
    }
    iOff = 4;
    a = pIter->pLeaf->p;
  }
  iOff += sqlite3GetVarint(&a[iOff], (u64*)&pIter->iRowid);
  pIter->iLeafOffset = iOff;
}

/*
** Initialize the iterator object pIter to iterate through the entries in
** segment pSeg within index iIdx. The iterator is left pointing to the 
** first entry when this function returns.
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. If 
** an error has already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterInit(
  Fts5Index *p,          
  int iIdx,                       /* Config.aHash[] index of FTS index */
  Fts5StructureSegment *pSeg,     /* Description of segment */
  Fts5SegIter *pIter              /* Object to populate */
){
  if( pSeg->pgnoFirst==0 ){
    /* This happens if the segment is being used as an input to an incremental
    ** merge and all data has already been "trimmed". See function
    ** fts5TrimSegments() for details. In this case leave the iterator empty.
    ** The caller will see the (pIter->pLeaf==0) and assume the iterator is
    ** at EOF already. */
    assert( pIter->pLeaf==0 );
    return;
  }

  if( p->rc==SQLITE_OK ){
    memset(pIter, 0, sizeof(*pIter));
    pIter->pSeg = pSeg;
    pIter->iIdx = iIdx;
    pIter->iLeafPgno = pSeg->pgnoFirst-1;
    fts5SegIterNextPage(p, pIter);
  }

  if( p->rc==SQLITE_OK ){
    u8 *a = pIter->pLeaf->p;
    pIter->iLeafOffset = fts5GetU16(&a[2]);
    fts5SegIterLoadTerm(p, pIter, 0);
  }
}

static void fts5LeafHeader(Fts5Data *pLeaf, int *piRowid, int *piTerm){
  *piRowid = (int)fts5GetU16(&pLeaf->p[0]);
  *piTerm = (int)fts5GetU16(&pLeaf->p[2]);
}

/*
** This function is only ever called on iterators created by calls to
** Fts5IndexQuery() with the FTS5INDEX_QUERY_ASC flag set.
**
** When this function is called, iterator pIter points to the first rowid
** on the current leaf associated with the term being queried. This function
** advances it to point to the last such rowid and, if necessary, initializes
** the aRowidOffset[] and iRowidOffset variables.
*/
static void fts5SegIterReverseInitPage(Fts5Index *p, Fts5SegIter *pIter){
  int n = pIter->pLeaf->n;
  int i = pIter->iLeafOffset;
  u8 *a = pIter->pLeaf->p;
  int iRowidOffset = 0;

  while( p->rc==SQLITE_OK && i<n ){
    i64 iDelta = 0;
    int nPos;

    i += getVarint32(&a[i], nPos);
    i += nPos;
    if( i>=n ) break;
    i += getVarint(&a[i], (u64*)&iDelta);
    if( iDelta==0 ) break;
    pIter->iRowid -= iDelta;

    if( iRowidOffset>=pIter->nRowidOffset ){
      int nNew = pIter->nRowidOffset + 8;
      int *aNew = (int*)sqlite3_realloc(pIter->aRowidOffset, nNew*sizeof(int));
      if( aNew==0 ){
        p->rc = SQLITE_NOMEM;
        break;
      }
      pIter->aRowidOffset = aNew;
      pIter->nRowidOffset = nNew;
    }

    pIter->aRowidOffset[iRowidOffset++] = pIter->iLeafOffset;
    pIter->iLeafOffset = i;
  }
  pIter->iRowidOffset = iRowidOffset;
}

/*
**
*/
static void fts5SegIterReverseNewPage(Fts5Index *p, Fts5SegIter *pIter){
  assert( pIter->flags & FTS5_SEGITER_REVERSE );
  assert( pIter->flags & FTS5_SEGITER_ONETERM );

  fts5DataRelease(pIter->pLeaf);
  pIter->pLeaf = 0;
  while( p->rc==SQLITE_OK && pIter->iLeafPgno>pIter->iTermLeafPgno ){
    Fts5Data *pNew;
    pIter->iLeafPgno--;
    pNew = fts5DataRead(p, FTS5_SEGMENT_ROWID(
          pIter->iIdx, pIter->pSeg->iSegid, 0, pIter->iLeafPgno
    ));
    if( pNew ){
      if( pIter->iLeafPgno==pIter->iTermLeafPgno ){
        if( pIter->iTermLeafOffset<pNew->n ){
          pIter->pLeaf = pNew;
          pIter->iLeafOffset = pIter->iTermLeafOffset;
        }
      }else{
        int iRowidOff, dummy;
        fts5LeafHeader(pNew, &iRowidOff, &dummy);
        if( iRowidOff ){
          pIter->pLeaf = pNew;
          pIter->iLeafOffset = iRowidOff;
        }
      }

      if( pIter->pLeaf ){
        u8 *a = &pIter->pLeaf->p[pIter->iLeafOffset];
        pIter->iLeafOffset += getVarint(a, (u64*)&pIter->iRowid);
        break;
      }else{
        fts5DataRelease(pNew);
      }
    }
  }

  if( pIter->pLeaf ){
    fts5SegIterReverseInitPage(p, pIter);
  }
}

/*
** Return true if the iterator passed as the second argument currently
** points to a delete marker. A delete marker is an entry with a 0 byte
** position-list.
*/
static int fts5SegIterIsDelete(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter              /* Iterator to advance */
){
  int bRet = 0;
  Fts5Data *pLeaf = pIter->pLeaf;
  if( p->rc==SQLITE_OK && pLeaf ){
    if( pIter->iLeafOffset<pLeaf->n ){
      bRet = (pLeaf->p[pIter->iLeafOffset]==0x00);
    }else{
      Fts5Data *pNew = fts5DataRead(p, FTS5_SEGMENT_ROWID(
            pIter->iIdx, pIter->pSeg->iSegid, 0, pIter->iLeafPgno
      ));
      if( pNew ){
        bRet = (pNew->p[4]==0x00);
        fts5DataRelease(pNew);
      }
    }
  }
  return bRet;
}

/*
** Advance iterator pIter to the next entry. 
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. It 
** is not considered an error if the iterator reaches EOF. If an error has 
** already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterNext(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter              /* Iterator to advance */
){
  if( p->rc==SQLITE_OK ){
    if( pIter->flags & FTS5_SEGITER_REVERSE ){
      if( pIter->iRowidOffset>0 ){
        u8 *a = pIter->pLeaf->p;
        int iOff;
        int nPos;
        i64 iDelta;
        pIter->iRowidOffset--;

        pIter->iLeafOffset = iOff = pIter->aRowidOffset[pIter->iRowidOffset];
        iOff += getVarint32(&a[iOff], nPos);
        iOff += nPos;
        getVarint(&a[iOff], (u64*)&iDelta);
        pIter->iRowid += iDelta;
      }else{
        fts5SegIterReverseNewPage(p, pIter);
      }
    }else{
      Fts5Data *pLeaf = pIter->pLeaf;
      int iOff;
      int bNewTerm = 0;
      int nKeep = 0;

      /* Search for the end of the position list within the current page. */
      u8 *a = pLeaf->p;
      int n = pLeaf->n;

      iOff = pIter->iLeafOffset;
      if( iOff<n ){
        int nPoslist;
        iOff += getVarint32(&a[iOff], nPoslist);
        iOff += nPoslist;
      }

      if( iOff<n ){
        /* The next entry is on the current page */
        u64 iDelta;
        iOff += sqlite3GetVarint(&a[iOff], &iDelta);
        pIter->iLeafOffset = iOff;
        if( iDelta==0 ){
          bNewTerm = 1;
          if( iOff>=n ){
            fts5SegIterNextPage(p, pIter);
            pIter->iLeafOffset = 4;
          }else if( iOff!=fts5GetU16(&a[2]) ){
            pIter->iLeafOffset += getVarint32(&a[iOff], nKeep);
          }
        }else{
          pIter->iRowid -= iDelta;
        }
      }else{
        iOff = 0;
        /* Next entry is not on the current page */
        while( iOff==0 ){
          fts5SegIterNextPage(p, pIter);
          pLeaf = pIter->pLeaf;
          if( pLeaf==0 ) break;
          if( (iOff = fts5GetU16(&pLeaf->p[0])) ){
            iOff += sqlite3GetVarint(&pLeaf->p[iOff], (u64*)&pIter->iRowid);
            pIter->iLeafOffset = iOff;
          }
          else if( (iOff = fts5GetU16(&pLeaf->p[2])) ){
            pIter->iLeafOffset = iOff;
            bNewTerm = 1;
          }
        }
      }

      /* Check if the iterator is now at EOF. If so, return early. */
      if( pIter->pLeaf && bNewTerm ){
        if( pIter->flags & FTS5_SEGITER_ONETERM ){
          fts5DataRelease(pIter->pLeaf);
          pIter->pLeaf = 0;
        }else{
          fts5SegIterLoadTerm(p, pIter, nKeep);
        }
      }
    }
  }
}

/*
** Iterator pIter currently points to the first rowid in a doclist. This
** function sets the iterator up so that iterates in reverse order through
** the doclist.
*/
static void fts5SegIterReverse(Fts5Index *p, int iIdx, Fts5SegIter *pIter){
  Fts5Data *pLeaf;                /* Current leaf data */
  int iOff = pIter->iLeafOffset;  /* Byte offset within current leaf */
  Fts5Data *pLast = 0;
  int pgnoLast = 0;

  /* Move to the page that contains the last rowid in this doclist. */
  pLeaf = pIter->pLeaf;

  if( pIter->pDlidx ){
    int iSegid = pIter->pSeg->iSegid;
    pgnoLast = pIter->pDlidx->iLeafPgno;
    pLast = fts5DataRead(p, FTS5_SEGMENT_ROWID(iIdx, iSegid, 0, pgnoLast));
  }else{
    while( iOff<pLeaf->n ){
      int nPos;
      i64 iDelta;

      /* Position list size in bytes */
      iOff += getVarint32(&pLeaf->p[iOff], nPos);
      iOff += nPos;
      if( iOff>=pLeaf->n ) break;

      /* Rowid delta. Or, if 0x00, the end of doclist marker. */
      nPos = getVarint(&pLeaf->p[iOff], (u64*)&iDelta);
      if( iDelta==0 ) break;
      iOff += nPos;
    }

    if( iOff>=pLeaf->n ){
      Fts5StructureSegment *pSeg = pIter->pSeg;
      i64 iAbs = FTS5_SEGMENT_ROWID(iIdx, pSeg->iSegid, 0, pIter->iLeafPgno);
      i64 iLast = FTS5_SEGMENT_ROWID(iIdx, pSeg->iSegid, 0, pSeg->pgnoLast);

      /* The last rowid in the doclist may not be on the current page. Search
       ** forward to find the page containing the last rowid.  */
      for(iAbs++; p->rc==SQLITE_OK && iAbs<=iLast; iAbs++){
        Fts5Data *pNew = fts5DataRead(p, iAbs);
        if( pNew ){
          int iRowid, iTerm;
          fts5LeafHeader(pNew, &iRowid, &iTerm);
          if( iRowid ){
            Fts5Data *pTmp = pLast;
            pLast = pNew;
            pNew = pTmp;
            pgnoLast = iAbs & (((i64)1 << FTS5_DATA_PAGE_B) - 1);
          }
          if( iTerm ){
            iAbs = iLast;
          }
          fts5DataRelease(pNew);
        }
      }
    }
  }

  /* If pLast is NULL at this point, then the last rowid for this doclist
  ** lies on the page currently indicated by the iterator. In this case 
  ** iLastOff is set to the value that pIter->iLeafOffset will take when
  ** the iterator points to that rowid.
  **
  ** Or, if pLast is non-NULL, then it is the page that contains the last
  ** rowid.
  */
  if( pLast ){
    int dummy;
    fts5DataRelease(pIter->pLeaf);
    pIter->pLeaf = pLast;
    pIter->iLeafPgno = pgnoLast;
    fts5LeafHeader(pLast, &iOff, &dummy);
    iOff += getVarint(&pLast->p[iOff], (u64*)&pIter->iRowid);
    pIter->iLeafOffset = iOff;
  }

  fts5SegIterReverseInitPage(p, pIter);
}

/*
** Iterator pIter currently points to the first rowid of a doclist within
** index iIdx. There is a doclist-index associated with the final term on
** the current page. If the current term is the last term on the page, 
** load the doclist-index from disk and initialize an iterator at 
** (pIter->pDlidx).
*/
static void fts5SegIterLoadDlidx(Fts5Index *p, int iIdx, Fts5SegIter *pIter){
  int iSegid = pIter->pSeg->iSegid;
  int bRev = (pIter->flags & FTS5_SEGITER_REVERSE);
  Fts5Data *pLeaf = pIter->pLeaf; /* Current leaf data */
  int iOff = pIter->iLeafOffset;  /* Byte offset within current leaf */

  assert( pIter->flags & FTS5_SEGITER_ONETERM );
  assert( pIter->pDlidx==0 );

  /* Check if the current doclist ends on this page. If it does, return
  ** early without loading the doclist-index (as it belongs to a different
  ** term. */
  while( iOff<pLeaf->n ){
    i64 iDelta;
    int nPoslist;

    /* iOff is currently the offset of the size field of a position list. */
    iOff += getVarint32(&pLeaf->p[iOff], nPoslist);
    iOff += nPoslist;

    if( iOff<pLeaf->n ){
      iOff += getVarint(&pLeaf->p[iOff], (u64*)&iDelta);
      if( iDelta==0 ) return;
    }
  }

  fts5DlidxIterInit(p, bRev, iIdx, iSegid, pIter->iLeafPgno, &pIter->pDlidx);
}

/*
** Initialize the object pIter to point to term pTerm/nTerm within segment
** pSeg, index iIdx. If there is no such term in the index, the iterator
** is set to EOF.
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. If 
** an error has already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterSeekInit(
  Fts5Index *p,                   /* FTS5 backend */
  int iIdx,                       /* Config.aHash[] index of FTS index */
  const u8 *pTerm, int nTerm,     /* Term to seek to */
  int flags,                      /* Mask of FTS5INDEX_XXX flags */
  Fts5StructureSegment *pSeg,     /* Description of segment */
  Fts5SegIter *pIter              /* Object to populate */
){
  int iPg = 1;
  int h;
  int bGe = ((flags & FTS5INDEX_QUERY_PREFIX) && iIdx==0);
  int bDlidx = 0;                 /* True if there is a doclist-index */

  assert( bGe==0 || (flags & FTS5INDEX_QUERY_ASC)==0 );
  assert( pTerm && nTerm );
  memset(pIter, 0, sizeof(*pIter));
  pIter->pSeg = pSeg;
  pIter->iIdx = iIdx;

  /* This block sets stack variable iPg to the leaf page number that may
  ** contain term (pTerm/nTerm), if it is present in the segment. */
  for(h=pSeg->nHeight-1; h>0; h--){
    Fts5NodeIter node;              /* For iterating through internal nodes */
    i64 iRowid = FTS5_SEGMENT_ROWID(iIdx, pSeg->iSegid, h, iPg);
    Fts5Data *pNode = fts5DataRead(p, iRowid);
    if( pNode==0 ) break;

    fts5NodeIterInit(pNode->p, pNode->n, &node);
    assert( node.term.n==0 );

    iPg = node.iChild;
    bDlidx = node.bDlidx;
    for(fts5NodeIterNext(&p->rc, &node);
        node.aData && fts5BufferCompareBlob(&node.term, pTerm, nTerm)<=0;
        fts5NodeIterNext(&p->rc, &node)
    ){
      iPg = node.iChild;
      bDlidx = node.bDlidx;
    }
    fts5NodeIterFree(&node);
    fts5DataRelease(pNode);
  }

  if( iPg<pSeg->pgnoFirst ){
    iPg = pSeg->pgnoFirst;
    bDlidx = 0;
  }

  pIter->iLeafPgno = iPg - 1;
  fts5SegIterNextPage(p, pIter);

  if( pIter->pLeaf ){
    int res;
    pIter->iLeafOffset = fts5GetU16(&pIter->pLeaf->p[2]);
    fts5SegIterLoadTerm(p, pIter, 0);
    do {
      res = fts5BufferCompareBlob(&pIter->term, pTerm, nTerm);
      if( res>=0 ) break;
      fts5SegIterNext(p, pIter);
    }while( pIter->pLeaf && p->rc==SQLITE_OK );

    if( bGe==0 && res ){
      /* Set iterator to point to EOF */
      fts5DataRelease(pIter->pLeaf);
      pIter->pLeaf = 0;
    }
  }

  if( p->rc==SQLITE_OK && bGe==0 ){
    pIter->flags |= FTS5_SEGITER_ONETERM;
    if( pIter->pLeaf ){
      if( flags & FTS5INDEX_QUERY_ASC ){
        pIter->flags |= FTS5_SEGITER_REVERSE;
      }
      if( bDlidx ){
        fts5SegIterLoadDlidx(p, iIdx, pIter);
      }
      if( flags & FTS5INDEX_QUERY_ASC ){
        fts5SegIterReverse(p, iIdx, pIter);
      }
    }
  }
}

/*
** Zero the iterator passed as the only argument.
*/
static void fts5SegIterClear(Fts5SegIter *pIter){
  fts5BufferFree(&pIter->term);
  fts5DataRelease(pIter->pLeaf);
  fts5DlidxIterFree(pIter->pDlidx);
  sqlite3_free(pIter->aRowidOffset);
  memset(pIter, 0, sizeof(Fts5SegIter));
}

/*
** Do the comparison necessary to populate pIter->aFirst[iOut].
**
** If the returned value is non-zero, then it is the index of an entry
** in the pIter->aSeg[] array that is (a) not at EOF, and (b) pointing
** to a key that is a duplicate of another, higher priority, 
** segment-iterator in the pSeg->aSeg[] array.
*/
static int fts5MultiIterDoCompare(Fts5MultiSegIter *pIter, int iOut){
  int i1;                         /* Index of left-hand Fts5SegIter */
  int i2;                         /* Index of right-hand Fts5SegIter */
  int iRes;
  Fts5SegIter *p1;                /* Left-hand Fts5SegIter */
  Fts5SegIter *p2;                /* Right-hand Fts5SegIter */

  assert( iOut<pIter->nSeg && iOut>0 );
  assert( pIter->bRev==0 || pIter->bRev==1 );

  if( iOut>=(pIter->nSeg/2) ){
    i1 = (iOut - pIter->nSeg/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pIter->aFirst[iOut*2];
    i2 = pIter->aFirst[iOut*2+1];
  }
  p1 = &pIter->aSeg[i1];
  p2 = &pIter->aSeg[i2];

  if( p1->pLeaf==0 ){           /* If p1 is at EOF */
    iRes = i2;
  }else if( p2->pLeaf==0 ){     /* If p2 is at EOF */
    iRes = i1;
  }else{
    int res = fts5BufferCompare(&p1->term, &p2->term);
    if( res==0 ){
      assert( i2>i1 );
      assert( i2!=0 );
      if( p1->iRowid==p2->iRowid ) return i2;
      res = ((p1->iRowid < p2->iRowid)==pIter->bRev) ? -1 : +1;
    }
    assert( res!=0 );
    if( res<0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pIter->aFirst[iOut] = iRes;
  return 0;
}

/*
** Free the iterator object passed as the second argument.
*/
static void fts5MultiIterFree(Fts5Index *p, Fts5MultiSegIter *pIter){
  if( pIter ){
    int i;
    for(i=0; i<pIter->nSeg; i++){
      fts5SegIterClear(&pIter->aSeg[i]);
    }
    sqlite3_free(pIter);
  }
}

static void fts5MultiIterAdvanced(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  Fts5MultiSegIter *pIter,        /* Iterator to update aFirst[] array for */
  int iChanged,                   /* Index of sub-iterator just advanced */
  int iMinset                     /* Minimum entry in aFirst[] to set */
){
  int i;
  for(i=(pIter->nSeg+iChanged)/2; i>=iMinset && p->rc==SQLITE_OK; i=i/2){
    int iEq;
    if( (iEq = fts5MultiIterDoCompare(pIter, i)) ){
      fts5SegIterNext(p, &pIter->aSeg[iEq]);
      i = pIter->nSeg + iEq;
    }
  }
}

/*
** Move the seg-iter so that it points to the first rowid on page iLeafPgno.
** It is an error if leaf iLeafPgno contains no rowid.
*/
static void fts5SegIterGotoPage(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter,             /* Iterator to advance */
  int iLeafPgno
){
  assert( iLeafPgno>pIter->iLeafPgno );
  if( p->rc==SQLITE_OK ){
    pIter->iLeafPgno = iLeafPgno-1;
    fts5SegIterNextPage(p, pIter);
    assert( p->rc!=SQLITE_OK || pIter->iLeafPgno==iLeafPgno );
  }

  if( p->rc==SQLITE_OK ){
    int iOff;
    u8 *a = pIter->pLeaf->p;
    int n = pIter->pLeaf->n;

    iOff = fts5GetU16(&a[0]);
    if( iOff<4 || iOff>=n ){
      p->rc = FTS5_CORRUPT;
    }else{
      iOff += getVarint(&a[iOff], (u64*)&pIter->iRowid);
      pIter->iLeafOffset = iOff;
    }
  }
}

/*
** Advance the iterator passed as the second argument until it is at or 
** past rowid iFrom. Regardless of the value of iFrom, the iterator is
** always advanced at least once.
*/
static void fts5SegIterNextFrom(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter,             /* Iterator to advance */
  i64 iMatch                      /* Advance iterator at least this far */
){
  int bRev = (pIter->flags & FTS5_SEGITER_REVERSE);
  Fts5DlidxIter *pDlidx = pIter->pDlidx;
  int iLeafPgno = pIter->iLeafPgno;
  int bMove = 1;

  assert( pIter->flags & FTS5_SEGITER_ONETERM );
  assert( pIter->pDlidx );
  assert( pIter->pLeaf );

  if( bRev==0 ){
    while( fts5DlidxIterEof(p, pDlidx)==0 && iMatch<pDlidx->iRowid ){
      iLeafPgno = pDlidx->iLeafPgno;
      fts5DlidxIterNext(pDlidx);
    }
    assert( iLeafPgno>=pIter->iLeafPgno || p->rc );
    if( iLeafPgno>pIter->iLeafPgno ){
      fts5SegIterGotoPage(p, pIter, iLeafPgno);
      bMove = 0;
    }
  }else{
    assert( iMatch>pIter->iRowid );
    while( fts5DlidxIterEof(p, pDlidx)==0 && iMatch>pDlidx->iRowid ){
      fts5DlidxIterPrev(pDlidx);
    }
    iLeafPgno = pDlidx->iLeafPgno;

    assert( fts5DlidxIterEof(p, pDlidx) || iLeafPgno<=pIter->iLeafPgno );

    if( iLeafPgno<pIter->iLeafPgno ){
      pIter->iLeafPgno = iLeafPgno+1;
      fts5SegIterReverseNewPage(p, pIter);
      bMove = 0;
    }
  }

  while( 1 ){
    if( bMove ) fts5SegIterNext(p, pIter);
    if( pIter->pLeaf==0 ) break;
    if( bRev==0 && pIter->iRowid<=iMatch ) break;
    if( bRev!=0 && pIter->iRowid>=iMatch ) break;
    bMove = 1;
  }
}

/*
** Move the iterator to the next entry. 
**
** If an error occurs, an error code is left in Fts5Index.rc. It is not 
** considered an error if the iterator reaches EOF, or if it is already at 
** EOF when this function is called.
*/
static void fts5MultiIterNext(
  Fts5Index *p, 
  Fts5MultiSegIter *pIter,
  int bFrom,                      /* True if argument iFrom is valid */
  i64 iFrom                       /* Advance at least as far as this */
){
  if( p->rc==SQLITE_OK ){
    int bUseFrom = bFrom;
    do {
      int iFirst = pIter->aFirst[1];
      Fts5SegIter *pSeg = &pIter->aSeg[iFirst];
      if( bUseFrom && pSeg->pDlidx ){
        fts5SegIterNextFrom(p, pSeg, iFrom);
      }else{
        fts5SegIterNext(p, pSeg);
      }
      fts5MultiIterAdvanced(p, pIter, iFirst, 1);
      bUseFrom = 0;
    }while( pIter->bSkipEmpty 
         && fts5SegIterIsDelete(p, &pIter->aSeg[pIter->aFirst[1]])
    );
  }
}

/*
** Allocate a new Fts5MultiSegIter object.
**
** The new object will be used to iterate through data in structure pStruct.
** If iLevel is -ve, then all data in all segments is merged. Or, if iLevel
** is zero or greater, data from the first nSegment segments on level iLevel
** is merged.
**
** The iterator initially points to the first term/rowid entry in the 
** iterated data.
*/
static void fts5MultiIterNew(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  Fts5Structure *pStruct,         /* Structure of specific index */
  int iIdx,                       /* Config.aHash[] index of FTS index */
  int bSkipEmpty,
  int flags,                      /* True for >= */
  const u8 *pTerm, int nTerm,     /* Term to seek to (or NULL/0) */
  int iLevel,                     /* Level to iterate (-1 for all) */
  int nSegment,                   /* Number of segments to merge (iLevel>=0) */
  Fts5MultiSegIter **ppOut        /* New object */
){
  int nSeg;                       /* Number of segments merged */
  int nSlot;                      /* Power of two >= nSeg */
  int iIter = 0;                  /* */
  int iSeg;                       /* Used to iterate through segments */
  Fts5StructureLevel *pLvl;
  Fts5MultiSegIter *pNew;

  assert( (pTerm==0 && nTerm==0) || iLevel<0 );

  /* Allocate space for the new multi-seg-iterator. */
  if( iLevel<0 ){
    nSeg = fts5StructureCountSegments(pStruct);
  }else{
    nSeg = MIN(pStruct->aLevel[iLevel].nSeg, nSegment);
  }
  for(nSlot=2; nSlot<nSeg; nSlot=nSlot*2);
  *ppOut = pNew = fts5IdxMalloc(p, 
      sizeof(Fts5MultiSegIter) +          /* pNew */
      sizeof(Fts5SegIter) * nSlot +       /* pNew->aSeg[] */
      sizeof(u16) * nSlot                 /* pNew->aFirst[] */
  );
  if( pNew==0 ) return;
  pNew->nSeg = nSlot;
  pNew->aSeg = (Fts5SegIter*)&pNew[1];
  pNew->aFirst = (u16*)&pNew->aSeg[nSlot];
  pNew->bRev = (0!=(flags & FTS5INDEX_QUERY_ASC));
  pNew->bSkipEmpty = bSkipEmpty;

  /* Initialize each of the component segment iterators. */
  if( iLevel<0 ){
    Fts5StructureLevel *pEnd = &pStruct->aLevel[pStruct->nLevel];
    for(pLvl=&pStruct->aLevel[0]; pLvl<pEnd; pLvl++){
      for(iSeg=pLvl->nSeg-1; iSeg>=0; iSeg--){
        Fts5StructureSegment *pSeg = &pLvl->aSeg[iSeg];
        Fts5SegIter *pIter = &pNew->aSeg[iIter++];
        if( pTerm==0 ){
          fts5SegIterInit(p, iIdx, pSeg, pIter);
        }else{
          fts5SegIterSeekInit(p, iIdx, pTerm, nTerm, flags, pSeg, pIter);
        }
      }
    }
  }else{
    pLvl = &pStruct->aLevel[iLevel];
    for(iSeg=nSeg-1; iSeg>=0; iSeg--){
      fts5SegIterInit(p, iIdx, &pLvl->aSeg[iSeg], &pNew->aSeg[iIter++]);
    }
  }
  assert( iIter==nSeg );

  /* If the above was successful, each component iterators now points 
  ** to the first entry in its segment. In this case initialize the 
  ** aFirst[] array. Or, if an error has occurred, free the iterator
  ** object and set the output variable to NULL.  */
  if( p->rc==SQLITE_OK ){
    for(iIter=nSlot-1; iIter>0; iIter--){
      int iEq;
      if( (iEq = fts5MultiIterDoCompare(pNew, iIter)) ){
        fts5SegIterNext(p, &pNew->aSeg[iEq]);
        fts5MultiIterAdvanced(p, pNew, iEq, iIter);
      }
    }

    if( pNew->bSkipEmpty 
     && fts5SegIterIsDelete(p, &pNew->aSeg[pNew->aFirst[1]]) 
    ){
      fts5MultiIterNext(p, pNew, 0, 0);
    }
  }else{
    fts5MultiIterFree(p, pNew);
    *ppOut = 0;
  }
}

/*
** Return true if the iterator is at EOF or if an error has occurred. 
** False otherwise.
*/
static int fts5MultiIterEof(Fts5Index *p, Fts5MultiSegIter *pIter){
  return (p->rc || pIter->aSeg[ pIter->aFirst[1] ].pLeaf==0);
}

/*
** Return the rowid of the entry that the iterator currently points
** to. If the iterator points to EOF when this function is called the
** results are undefined.
*/
static i64 fts5MultiIterRowid(Fts5MultiSegIter *pIter){
  assert( pIter->aSeg[ pIter->aFirst[1] ].pLeaf );
  return pIter->aSeg[ pIter->aFirst[1] ].iRowid;
}

/*
** Move the iterator to the next entry at or following iMatch.
*/
static void fts5MultiIterNextFrom(
  Fts5Index *p, 
  Fts5MultiSegIter *pIter, 
  i64 iMatch
){
  while( 1 ){
    i64 iRowid;
    fts5MultiIterNext(p, pIter, 1, iMatch);
    if( fts5MultiIterEof(p, pIter) ) break;
    iRowid = fts5MultiIterRowid(pIter);
    if( pIter->bRev==0 && iRowid<=iMatch ) break;
    if( pIter->bRev!=0 && iRowid>=iMatch ) break;
  }
}

/*
** Return a pointer to a buffer containing the term associated with the 
** entry that the iterator currently points to.
*/
static const u8 *fts5MultiIterTerm(Fts5MultiSegIter *pIter, int *pn){
  Fts5SegIter *p = &pIter->aSeg[ pIter->aFirst[1] ];
  *pn = p->term.n;
  return p->term.p;
}

/*
** Return true if the chunk iterator passed as the second argument is
** at EOF. Or if an error has already occurred. Otherwise, return false.
*/
static int fts5ChunkIterEof(Fts5Index *p, Fts5ChunkIter *pIter){
  return (p->rc || pIter->pLeaf==0);
}

/*
** Advance the chunk-iterator to the next chunk of data to read.
*/
static void fts5ChunkIterNext(Fts5Index *p, Fts5ChunkIter *pIter){
  assert( pIter->nRem>=pIter->n );
  pIter->nRem -= pIter->n;
  fts5DataRelease(pIter->pLeaf);
  pIter->pLeaf = 0;
  pIter->p = 0;
  if( pIter->nRem>0 ){
    Fts5Data *pLeaf;
    pIter->iLeafRowid++;
    pLeaf = pIter->pLeaf = fts5DataRead(p, pIter->iLeafRowid);
    if( pLeaf ){
      pIter->n = MIN(pIter->nRem, pLeaf->n-4);
      pIter->p = pLeaf->p+4;
    }
  }
}

/*
** Intialize the chunk iterator to read the position list data for which 
** the size field is at offset iOff of leaf pLeaf. 
*/
static void fts5ChunkIterInit(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pSeg,              /* Segment iterator to read poslist from */
  Fts5ChunkIter *pIter            /* Initialize this object */
){
  int iId = pSeg->pSeg->iSegid;
  i64 rowid = FTS5_SEGMENT_ROWID(pSeg->iIdx, iId, 0, pSeg->iLeafPgno);
  Fts5Data *pLeaf = pSeg->pLeaf;
  int iOff = pSeg->iLeafOffset;

  memset(pIter, 0, sizeof(*pIter));
  pIter->iLeafRowid = rowid;
  if( iOff<pLeaf->n ){
    fts5DataReference(pLeaf);
    pIter->pLeaf = pLeaf;
  }else{
    pIter->nRem = 1;
    fts5ChunkIterNext(p, pIter);
    if( p->rc ) return;
    iOff = 4;
    pLeaf = pIter->pLeaf;
  }

  iOff += getVarint32(&pLeaf->p[iOff], pIter->nRem);
  pIter->n = MIN(pLeaf->n - iOff, pIter->nRem);
  pIter->p = pLeaf->p + iOff;

  if( pIter->n==0 ){
    fts5ChunkIterNext(p, pIter);
  }
}

static void fts5ChunkIterRelease(Fts5ChunkIter *pIter){
  fts5DataRelease(pIter->pLeaf);
  pIter->pLeaf = 0;
}

/*
** Read and return the next 32-bit varint from the position-list iterator 
** passed as the second argument.
**
** If an error occurs, zero is returned an an error code left in 
** Fts5Index.rc. If an error has already occurred when this function is
** called, it is a no-op.
*/
static int fts5PosIterReadVarint(Fts5Index *p, Fts5PosIter *pIter){
  int iVal = 0;
  if( p->rc==SQLITE_OK ){
    if( pIter->iOff>=pIter->chunk.n ){
      fts5ChunkIterNext(p, &pIter->chunk);
      if( fts5ChunkIterEof(p, &pIter->chunk) ) return 0;
      pIter->iOff = 0;
    }
    pIter->iOff += getVarint32(&pIter->chunk.p[pIter->iOff], iVal);
  }
  return iVal;
}

/*
** Advance the position list iterator to the next entry.
*/
static void fts5PosIterNext(Fts5Index *p, Fts5PosIter *pIter){
  int iVal;
  assert( fts5ChunkIterEof(p, &pIter->chunk)==0 );
  iVal = fts5PosIterReadVarint(p, pIter);
  if( fts5ChunkIterEof(p, &pIter->chunk)==0 ){
    if( iVal==1 ){
      pIter->iCol = fts5PosIterReadVarint(p, pIter);
      pIter->iPos = fts5PosIterReadVarint(p, pIter) - 2;
    }else{
      pIter->iPos += (iVal - 2);
    }
  }
}

/*
** Initialize the Fts5PosIter object passed as the final argument to iterate
** through the position-list associated with the index entry that iterator 
** pMulti currently points to.
*/
static void fts5PosIterInit(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5MultiSegIter *pMulti,       /* Multi-seg iterator to read pos-list from */
  Fts5PosIter *pIter              /* Initialize this object */
){
  if( p->rc==SQLITE_OK ){
    Fts5SegIter *pSeg = &pMulti->aSeg[ pMulti->aFirst[1] ];
    memset(pIter, 0, sizeof(*pIter));
    fts5ChunkIterInit(p, pSeg, &pIter->chunk);
    if( fts5ChunkIterEof(p, &pIter->chunk)==0 ){
      fts5PosIterNext(p, pIter);
    }
  }
}

/*
** Return true if the position iterator passed as the second argument is
** at EOF. Or if an error has already occurred. Otherwise, return false.
*/
static int fts5PosIterEof(Fts5Index *p, Fts5PosIter *pIter){
  return (p->rc || pIter->chunk.pLeaf==0);
}

/*
** Add an entry for (iRowid/iCol/iPos) to the doclist for (pToken/nToken)
** in hash table for index iIdx. If iIdx is zero, this is the main terms 
** index. Values of 1 and greater for iIdx are prefix indexes.
**
** If an OOM error is encountered, set the Fts5Index.rc error code 
** accordingly.
*/
static void fts5AddTermToHash(
  Fts5Index *p,                   /* Index object to write to */
  int iIdx,                       /* Entry in p->aHash[] to update */
  int iCol,                       /* Column token appears in (-ve -> delete) */
  int iPos,                       /* Position of token within column */
  const char *pToken, int nToken  /* Token to add or remove to or from index */
){
  if( p->rc==SQLITE_OK ){
    p->rc = sqlite3Fts5HashWrite(
        p->apHash[iIdx], p->iWriteRowid, iCol, iPos, pToken, nToken
    );
  }
}

/*
** Allocate a new segment-id for the structure pStruct.
**
** If an error has already occurred, this function is a no-op. 0 is 
** returned in this case.
*/
static int fts5AllocateSegid(Fts5Index *p, Fts5Structure *pStruct){
  int i;
  if( p->rc!=SQLITE_OK ) return 0;

  for(i=0; i<100; i++){
    int iSegid;
    sqlite3_randomness(sizeof(int), (void*)&iSegid);
    iSegid = iSegid & ((1 << FTS5_DATA_ID_B)-1);
    if( iSegid ){
      int iLvl, iSeg;
      for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
        for(iSeg=0; iSeg<pStruct->aLevel[iLvl].nSeg; iSeg++){
          if( iSegid==pStruct->aLevel[iLvl].aSeg[iSeg].iSegid ){
            iSegid = 0;
          }
        }
      }
    }
    if( iSegid ) return iSegid;
  }

  p->rc = SQLITE_ERROR;
  return 0;
}

/*
** Discard all data currently cached in the hash-tables.
*/
static void fts5IndexDiscardData(Fts5Index *p){
  assert( p->apHash || p->nPendingData==0 );
  if( p->apHash ){
    Fts5Config *pConfig = p->pConfig;
    int i;
    for(i=0; i<=pConfig->nPrefix; i++){
      if( p->apHash[i] ) sqlite3Fts5HashClear(p->apHash[i]);
    }
    p->nPendingData = 0;
  }
}

/*
** Return the size of the prefix, in bytes, that buffer (nNew/pNew) shares
** with buffer (nOld/pOld).
*/
static int fts5PrefixCompress(
  int nOld, const u8 *pOld,
  int nNew, const u8 *pNew
){
  int i;
  for(i=0; i<nNew && i<nOld; i++){
    if( pOld[i]!=pNew[i] ) break;
  }
  return i;
}

/*
** If an "nEmpty" record must be written to the b-tree before the next
** term, write it now.
*/
static void fts5WriteBtreeNEmpty(Fts5Index *p, Fts5SegWriter *pWriter){
  if( pWriter->nEmpty ){
    int bFlag = 0;
    Fts5PageWriter *pPg;
    pPg = &pWriter->aWriter[1];
    if( pWriter->nEmpty>=FTS5_MIN_DLIDX_SIZE ){
      i64 iKey = FTS5_DOCLIST_IDX_ROWID(
          pWriter->iIdx, pWriter->iSegid, 
          pWriter->aWriter[0].pgno - 1 - pWriter->nEmpty
      );
      assert( pWriter->dlidx.n>0 );
      fts5DataWrite(p, iKey, pWriter->dlidx.p, pWriter->dlidx.n);
      bFlag = 1;
    }
    fts5BufferAppendVarint(&p->rc, &pPg->buf, bFlag);
    fts5BufferAppendVarint(&p->rc, &pPg->buf, pWriter->nEmpty);
    pWriter->nEmpty = 0;
  }

  /* Whether or not it was written to disk, zero the doclist index at this
  ** point */
  sqlite3Fts5BufferZero(&pWriter->dlidx);
  pWriter->bDlidxPrevValid = 0;
}

static void fts5WriteBtreeGrow(Fts5Index *p, Fts5SegWriter *pWriter){
  Fts5PageWriter *aNew;
  Fts5PageWriter *pNew;
  int nNew = sizeof(Fts5PageWriter) * (pWriter->nWriter+1);

  aNew = (Fts5PageWriter*)sqlite3_realloc(pWriter->aWriter, nNew);
  if( aNew==0 ) return;

  pNew = &aNew[pWriter->nWriter];
  memset(pNew, 0, sizeof(Fts5PageWriter));
  pNew->pgno = 1;
  fts5BufferAppendVarint(&p->rc, &pNew->buf, 1);

  pWriter->nWriter++;
  pWriter->aWriter = aNew;
}

/*
** This is called once for each leaf page except the first that contains
** at least one term. Argument (nTerm/pTerm) is the split-key - a term that
** is larger than all terms written to earlier leaves, and equal to or
** smaller than the first term on the new leaf.
**
** If an error occurs, an error code is left in Fts5Index.rc. If an error
** has already occurred when this function is called, it is a no-op.
*/
static void fts5WriteBtreeTerm(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegWriter *pWriter,         /* Writer object */
  int nTerm, const u8 *pTerm      /* First term on new page */
){
  int iHeight;
  for(iHeight=1; 1; iHeight++){
    Fts5PageWriter *pPage;

    if( iHeight>=pWriter->nWriter ){
      fts5WriteBtreeGrow(p, pWriter);
      if( p->rc ) return;
    }
    pPage = &pWriter->aWriter[iHeight];

    fts5WriteBtreeNEmpty(p, pWriter);

    if( pPage->buf.n>=p->pConfig->pgsz ){
      /* pPage will be written to disk. The term will be written into the
      ** parent of pPage.  */
      i64 iRowid = FTS5_SEGMENT_ROWID(
          pWriter->iIdx, pWriter->iSegid, iHeight, pPage->pgno
      );
      fts5DataWrite(p, iRowid, pPage->buf.p, pPage->buf.n);
      fts5BufferZero(&pPage->buf);
      fts5BufferZero(&pPage->term);
      fts5BufferAppendVarint(&p->rc, &pPage->buf, pPage[-1].pgno);
      pPage->pgno++;
    }else{
      int nPre = fts5PrefixCompress(pPage->term.n, pPage->term.p, nTerm, pTerm);
      fts5BufferAppendVarint(&p->rc, &pPage->buf, nPre+2);
      fts5BufferAppendVarint(&p->rc, &pPage->buf, nTerm-nPre);
      fts5BufferAppendBlob(&p->rc, &pPage->buf, nTerm-nPre, pTerm+nPre);
      fts5BufferSet(&p->rc, &pPage->term, nTerm, pTerm);
      break;
    }
  }
}

static void fts5WriteBtreeNoTerm(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegWriter *pWriter          /* Writer object */
){
  if( pWriter->bFirstRowidInPage ){
    /* No rowids on this page. Append an 0x00 byte to the current 
    ** doclist-index */
    sqlite3Fts5BufferAppendVarint(&p->rc, &pWriter->dlidx, 0);
  }
  pWriter->nEmpty++;
}

/*
** Rowid iRowid has just been appended to the current leaf page. As it is
** the first on its page, append an entry to the current doclist-index.
*/
static void fts5WriteDlidxAppend(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  i64 iRowid
){
  i64 iVal;
  if( pWriter->bDlidxPrevValid ){
    iVal = pWriter->iDlidxPrev - iRowid;
  }else{
    iVal = iRowid;
  }
  sqlite3Fts5BufferAppendVarint(&p->rc, &pWriter->dlidx, iVal);
  pWriter->bDlidxPrevValid = 1;
  pWriter->iDlidxPrev = iRowid;
}

static void fts5WriteFlushLeaf(Fts5Index *p, Fts5SegWriter *pWriter){
  static const u8 zero[] = { 0x00, 0x00, 0x00, 0x00 };
  Fts5PageWriter *pPage = &pWriter->aWriter[0];
  i64 iRowid;

  if( pPage->term.n==0 ){
    /* No term was written to this page. */
    assert( 0==fts5GetU16(&pPage->buf.p[2]) );
    fts5WriteBtreeNoTerm(p, pWriter);
  }

  /* Write the current page to the db. */
  iRowid = FTS5_SEGMENT_ROWID(pWriter->iIdx, pWriter->iSegid, 0, pPage->pgno);
  fts5DataWrite(p, iRowid, pPage->buf.p, pPage->buf.n);

  /* Initialize the next page. */
  fts5BufferZero(&pPage->buf);
  fts5BufferZero(&pPage->term);
  fts5BufferAppendBlob(&p->rc, &pPage->buf, 4, zero);
  pPage->pgno++;

  /* Increase the leaves written counter */
  pWriter->nLeafWritten++;
}

/*
** Append term pTerm/nTerm to the segment being written by the writer passed
** as the second argument.
**
** If an error occurs, set the Fts5Index.rc error code. If an error has 
** already occurred, this function is a no-op.
*/
static void fts5WriteAppendTerm(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,
  int nTerm, const u8 *pTerm 
){
  int nPrefix;                    /* Bytes of prefix compression for term */
  Fts5PageWriter *pPage = &pWriter->aWriter[0];

  assert( pPage==0 || pPage->buf.n==0 || pPage->buf.n>4 );
  if( pPage && pPage->buf.n==0 ){
    /* Zero the first term and first docid fields */
    static const u8 zero[] = { 0x00, 0x00, 0x00, 0x00 };
    fts5BufferAppendBlob(&p->rc, &pPage->buf, 4, zero);
    assert( pPage->term.n==0 );
  }
  if( p->rc ) return;
  
  if( pPage->term.n==0 ){
    /* Update the "first term" field of the page header. */
    assert( pPage->buf.p[2]==0 && pPage->buf.p[3]==0 );
    fts5PutU16(&pPage->buf.p[2], pPage->buf.n);
    nPrefix = 0;
    if( pWriter->aWriter[0].pgno!=1 ){
      fts5WriteBtreeTerm(p, pWriter, nTerm, pTerm);
      pPage = &pWriter->aWriter[0];
    }
  }else{
    nPrefix = fts5PrefixCompress(
        pPage->term.n, pPage->term.p, nTerm, pTerm
    );
    fts5BufferAppendVarint(&p->rc, &pPage->buf, nPrefix);
  }

  /* Append the number of bytes of new data, then the term data itself
  ** to the page. */
  fts5BufferAppendVarint(&p->rc, &pPage->buf, nTerm - nPrefix);
  fts5BufferAppendBlob(&p->rc, &pPage->buf, nTerm - nPrefix, &pTerm[nPrefix]);

  /* Update the Fts5PageWriter.term field. */
  fts5BufferSet(&p->rc, &pPage->term, nTerm, pTerm);

  pWriter->bFirstRowidInPage = 0;
  pWriter->bFirstRowidInDoclist = 1;

  /* If the current leaf page is full, flush it to disk. */
  if( pPage->buf.n>=p->pConfig->pgsz ){
    fts5WriteFlushLeaf(p, pWriter);
    pWriter->bFirstRowidInPage = 1;
  }
}

/*
** Append a docid to the writers output. 
*/
static void fts5WriteAppendRowid(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,
  i64 iRowid
){
  if( p->rc==SQLITE_OK ){
    Fts5PageWriter *pPage = &pWriter->aWriter[0];

    /* If this is to be the first docid written to the page, set the 
    ** docid-pointer in the page-header. Also append a value to the dlidx
    ** buffer, in case a doclist-index is required.  */
    if( pWriter->bFirstRowidInPage ){
      fts5PutU16(pPage->buf.p, pPage->buf.n);
      fts5WriteDlidxAppend(p, pWriter, iRowid);
    }

    /* Write the docid. */
    if( pWriter->bFirstRowidInDoclist || pWriter->bFirstRowidInPage ){
      fts5BufferAppendVarint(&p->rc, &pPage->buf, iRowid);
    }else{
      assert( p->rc || iRowid<pWriter->iPrevRowid );
      fts5BufferAppendVarint(&p->rc, &pPage->buf, pWriter->iPrevRowid - iRowid);
    }
    pWriter->iPrevRowid = iRowid;
    pWriter->bFirstRowidInDoclist = 0;
    pWriter->bFirstRowidInPage = 0;

    if( pPage->buf.n>=p->pConfig->pgsz ){
      fts5WriteFlushLeaf(p, pWriter);
      pWriter->bFirstRowidInPage = 1;
    }
  }
}

static void fts5WriteAppendPoslistInt(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,
  int iVal
){
  if( p->rc==SQLITE_OK ){
    Fts5PageWriter *pPage = &pWriter->aWriter[0];
    fts5BufferAppendVarint(&p->rc, &pPage->buf, iVal);
    if( pPage->buf.n>=p->pConfig->pgsz ){
      fts5WriteFlushLeaf(p, pWriter);
      pWriter->bFirstRowidInPage = 1;
    }
  }
}

static void fts5WriteAppendPoslistData(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  const u8 *aData, 
  int nData
){
  Fts5PageWriter *pPage = &pWriter->aWriter[0];
  const u8 *a = aData;
  int n = nData;
  
  assert( p->pConfig->pgsz>0 );
  while( p->rc==SQLITE_OK && (pPage->buf.n + n)>=p->pConfig->pgsz ){
    int nReq = p->pConfig->pgsz - pPage->buf.n;
    int nCopy = 0;
    while( nCopy<nReq ){
      i64 dummy;
      nCopy += getVarint(&a[nCopy], (u64*)&dummy);
    }
    fts5BufferAppendBlob(&p->rc, &pPage->buf, nCopy, a);
    a += nCopy;
    n -= nCopy;
    fts5WriteFlushLeaf(p, pWriter);
    pWriter->bFirstRowidInPage = 1;
  }
  if( n>0 ){
    fts5BufferAppendBlob(&p->rc, &pPage->buf, n, a);
  }
}

static void fts5WriteAppendZerobyte(Fts5Index *p, Fts5SegWriter *pWriter){
  fts5BufferAppendVarint(&p->rc, &pWriter->aWriter[0].buf, 0);
}

/*
** Flush any data cached by the writer object to the database. Free any
** allocations associated with the writer.
*/
static void fts5WriteFinish(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,         /* Writer object */
  int *pnHeight,                  /* OUT: Height of the b-tree */
  int *pnLeaf                     /* OUT: Number of leaf pages in b-tree */
){
  int i;
  if( p->rc==SQLITE_OK ){
    *pnLeaf = pWriter->aWriter[0].pgno;
    if( *pnLeaf==1 && pWriter->aWriter[0].buf.n==0 ){
      *pnLeaf = 0;
      *pnHeight = 0;
    }else{
      fts5WriteFlushLeaf(p, pWriter);
      if( pWriter->nWriter==1 && pWriter->nEmpty>=FTS5_MIN_DLIDX_SIZE ){
        fts5WriteBtreeGrow(p, pWriter);
      }
      if( pWriter->nWriter>1 ){
        fts5WriteBtreeNEmpty(p, pWriter);
      }
      *pnHeight = pWriter->nWriter;

      for(i=1; i<pWriter->nWriter; i++){
        Fts5PageWriter *pPg = &pWriter->aWriter[i];
        fts5DataWrite(p, 
            FTS5_SEGMENT_ROWID(pWriter->iIdx, pWriter->iSegid, i, pPg->pgno), 
            pPg->buf.p, pPg->buf.n
        );
      }
    }
  }
  for(i=0; i<pWriter->nWriter; i++){
    Fts5PageWriter *pPg = &pWriter->aWriter[i];
    assert( pPg || p->rc!=SQLITE_OK );
    if( pPg ){
      fts5BufferFree(&pPg->term);
      fts5BufferFree(&pPg->buf);
    }
  }
  sqlite3_free(pWriter->aWriter);
  sqlite3Fts5BufferFree(&pWriter->dlidx);
}

static void fts5WriteInit(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  int iIdx, int iSegid
){
  memset(pWriter, 0, sizeof(Fts5SegWriter));
  pWriter->iIdx = iIdx;
  pWriter->iSegid = iSegid;

  pWriter->aWriter = (Fts5PageWriter*)fts5IdxMalloc(p,sizeof(Fts5PageWriter));
  if( pWriter->aWriter==0 ) return;
  pWriter->nWriter = 1;
  pWriter->aWriter[0].pgno = 1;
}

static void fts5WriteInitForAppend(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegWriter *pWriter,         /* Writer to initialize */
  int iIdx,                       /* Index segment is a part of */
  Fts5StructureSegment *pSeg      /* Segment object to append to */
){
  int nByte = pSeg->nHeight * sizeof(Fts5PageWriter);
  memset(pWriter, 0, sizeof(Fts5SegWriter));
  pWriter->iIdx = iIdx;
  pWriter->iSegid = pSeg->iSegid;
  pWriter->aWriter = (Fts5PageWriter*)fts5IdxMalloc(p, nByte);
  pWriter->nWriter = pSeg->nHeight;

  if( p->rc==SQLITE_OK ){
    int pgno = 1;
    int i;
    pWriter->aWriter[0].pgno = pSeg->pgnoLast+1;
    for(i=pSeg->nHeight-1; i>0; i--){
      i64 iRowid = FTS5_SEGMENT_ROWID(pWriter->iIdx, pWriter->iSegid, i, pgno);
      Fts5PageWriter *pPg = &pWriter->aWriter[i];
      pPg->pgno = pgno;
      fts5DataBuffer(p, &pPg->buf, iRowid);
      if( p->rc==SQLITE_OK ){
        Fts5NodeIter ss;
        fts5NodeIterInit(pPg->buf.p, pPg->buf.n, &ss);
        while( ss.aData ) fts5NodeIterNext(&p->rc, &ss);
        fts5BufferSet(&p->rc, &pPg->term, ss.term.n, ss.term.p);
        pgno = ss.iChild;
        fts5NodeIterFree(&ss);
      }
    }
    if( pSeg->nHeight==1 ){
      pWriter->nEmpty = pSeg->pgnoLast-1;
    }
    assert( (pgno+pWriter->nEmpty)==pSeg->pgnoLast );
  }
}

/*
** Iterator pIter was used to iterate through the input segments of on an
** incremental merge operation. This function is called if the incremental
** merge step has finished but the input has not been completely exhausted.
*/
static void fts5TrimSegments(Fts5Index *p, Fts5MultiSegIter *pIter){
  int i;
  Fts5Buffer buf;
  memset(&buf, 0, sizeof(Fts5Buffer));
  for(i=0; i<pIter->nSeg; i++){
    Fts5SegIter *pSeg = &pIter->aSeg[i];
    if( pSeg->pSeg==0 ){
      /* no-op */
    }else if( pSeg->pLeaf==0 ){
      pSeg->pSeg->pgnoLast = 0;
      pSeg->pSeg->pgnoFirst = 0;
    }else{
      int iOff = pSeg->iTermLeafOffset;     /* Offset on new first leaf page */
      i64 iLeafRowid;
      Fts5Data *pData;
      int iId = pSeg->pSeg->iSegid;
      u8 aHdr[4] = {0x00, 0x00, 0x00, 0x04};

      iLeafRowid = FTS5_SEGMENT_ROWID(pSeg->iIdx, iId, 0, pSeg->iTermLeafPgno);
      pData = fts5DataRead(p, iLeafRowid);
      if( pData ){
        fts5BufferZero(&buf);
        fts5BufferAppendBlob(&p->rc, &buf, sizeof(aHdr), aHdr);
        fts5BufferAppendVarint(&p->rc, &buf, pSeg->term.n);
        fts5BufferAppendBlob(&p->rc, &buf, pSeg->term.n, pSeg->term.p);
        fts5BufferAppendBlob(&p->rc, &buf, pData->n - iOff, &pData->p[iOff]);
        fts5DataRelease(pData);
        pSeg->pSeg->pgnoFirst = pSeg->iTermLeafPgno;
        fts5DataDelete(p, FTS5_SEGMENT_ROWID(pSeg->iIdx, iId, 0, 1),iLeafRowid);
        fts5DataWrite(p, iLeafRowid, buf.p, buf.n);
      }
    }
  }
  fts5BufferFree(&buf);
}

/*
**
*/
static void fts5IndexMergeLevel(
  Fts5Index *p,                   /* FTS5 backend object */
  int iIdx,                       /* Index to work on */
  Fts5Structure **ppStruct,       /* IN/OUT: Stucture of index iIdx */
  int iLvl,                       /* Level to read input from */
  int *pnRem                      /* Write up to this many output leaves */
){
  Fts5Structure *pStruct = *ppStruct;
  Fts5StructureLevel *pLvl = &pStruct->aLevel[iLvl];
  Fts5StructureLevel *pLvlOut;
  Fts5MultiSegIter *pIter = 0;    /* Iterator to read input data */
  int nRem = pnRem ? *pnRem : 0;  /* Output leaf pages left to write */
  int nInput;                     /* Number of input segments */
  Fts5SegWriter writer;           /* Writer object */
  Fts5StructureSegment *pSeg;     /* Output segment */
  Fts5Buffer term;
  int bRequireDoclistTerm = 0;    /* Doclist terminator (0x00) required */
  int bOldest;                    /* True if the output segment is the oldest */

  assert( iLvl<pStruct->nLevel );
  assert( pLvl->nMerge<=pLvl->nSeg );

  memset(&writer, 0, sizeof(Fts5SegWriter));
  memset(&term, 0, sizeof(Fts5Buffer));
  writer.iIdx = iIdx;
  if( pLvl->nMerge ){
    pLvlOut = &pStruct->aLevel[iLvl+1];
    assert( pLvlOut->nSeg>0 );
    nInput = pLvl->nMerge;
    fts5WriteInitForAppend(p, &writer, iIdx, &pLvlOut->aSeg[pLvlOut->nSeg-1]);
    pSeg = &pLvlOut->aSeg[pLvlOut->nSeg-1];
  }else{
    int iSegid = fts5AllocateSegid(p, pStruct);

    /* Extend the Fts5Structure object as required to ensure the output
    ** segment exists. */
    if( iLvl==pStruct->nLevel-1 ){
      fts5StructureAddLevel(&p->rc, ppStruct);
      pStruct = *ppStruct;
    }
    fts5StructureExtendLevel(&p->rc, pStruct, iLvl+1, 1, 0);
    if( p->rc ) return;
    pLvl = &pStruct->aLevel[iLvl];
    pLvlOut = &pStruct->aLevel[iLvl+1];

    fts5WriteInit(p, &writer, iIdx, iSegid);

    /* Add the new segment to the output level */
    if( iLvl+1==pStruct->nLevel ) pStruct->nLevel++;
    pSeg = &pLvlOut->aSeg[pLvlOut->nSeg];
    pLvlOut->nSeg++;
    pSeg->pgnoFirst = 1;
    pSeg->iSegid = iSegid;

    /* Read input from all segments in the input level */
    nInput = pLvl->nSeg;
  }
  bOldest = (pLvlOut->nSeg==1 && pStruct->nLevel==iLvl+2);

#if 0
fprintf(stdout, "merging %d segments from level %d!", nInput, iLvl);
fflush(stdout);
#endif

  for(fts5MultiIterNew(p, pStruct, iIdx, 0, 0, 0, 0, iLvl, nInput, &pIter);
      fts5MultiIterEof(p, pIter)==0;
      fts5MultiIterNext(p, pIter, 0, 0)
  ){
    Fts5SegIter *pSeg = &pIter->aSeg[ pIter->aFirst[1] ];
    Fts5ChunkIter sPos;           /* Used to iterate through position list */

    /* If the segment being written is the oldest in the entire index and
    ** the position list is empty (i.e. the entry is a delete marker), no
    ** entry need be written to the output.  */
    fts5ChunkIterInit(p, pSeg, &sPos);
    if( bOldest==0 || sPos.nRem>0 ){
      int nTerm;
      const u8 *pTerm = fts5MultiIterTerm(pIter, &nTerm);
      if( nTerm!=term.n || memcmp(pTerm, term.p, nTerm) ){
        if( pnRem && writer.nLeafWritten>nRem ){
          fts5ChunkIterRelease(&sPos);
          break;
        }

        /* This is a new term. Append a term to the output segment. */
        if( bRequireDoclistTerm ){
          fts5WriteAppendZerobyte(p, &writer);
        }
        fts5WriteAppendTerm(p, &writer, nTerm, pTerm);
        fts5BufferSet(&p->rc, &term, nTerm, pTerm);
        bRequireDoclistTerm = 1;
      }

      /* Append the rowid to the output */
      fts5WriteAppendRowid(p, &writer, fts5MultiIterRowid(pIter));

      /* Copy the position list from input to output */
      fts5WriteAppendPoslistInt(p, &writer, sPos.nRem);
      for(/* noop */; !fts5ChunkIterEof(p, &sPos); fts5ChunkIterNext(p, &sPos)){
        fts5WriteAppendPoslistData(p, &writer, sPos.p, sPos.n);
      }
    }

    fts5ChunkIterRelease(&sPos);
  }

  /* Flush the last leaf page to disk. Set the output segment b-tree height
  ** and last leaf page number at the same time.  */
  fts5WriteFinish(p, &writer, &pSeg->nHeight, &pSeg->pgnoLast);

  if( fts5MultiIterEof(p, pIter) ){
    int i;

    /* Remove the redundant segments from the %_data table */
    for(i=0; i<nInput; i++){
      fts5DataRemoveSegment(p, iIdx, pLvl->aSeg[i].iSegid);
    }

    /* Remove the redundant segments from the input level */
    if( pLvl->nSeg!=nInput ){
      int nMove = (pLvl->nSeg - nInput) * sizeof(Fts5StructureSegment);
      memmove(pLvl->aSeg, &pLvl->aSeg[nInput], nMove);
    }
    pLvl->nSeg -= nInput;
    pLvl->nMerge = 0;
    if( pSeg->pgnoLast==0 ){
      pLvlOut->nSeg--;
    }
  }else{
    assert( pSeg->nHeight>0 && pSeg->pgnoLast>0 );
    fts5TrimSegments(p, pIter);
    pLvl->nMerge = nInput;
  }

  fts5MultiIterFree(p, pIter);
  fts5BufferFree(&term);
  if( pnRem ) *pnRem -= writer.nLeafWritten;
}

/*
** A total of nLeaf leaf pages of data has just been flushed to a level-0
** segments in index iIdx with structure pStruct. This function updates the
** write-counter accordingly and, if necessary, performs incremental merge
** work.
**
** If an error occurs, set the Fts5Index.rc error code. If an error has 
** already occurred, this function is a no-op.
*/
static void fts5IndexWork(
  Fts5Index *p,                   /* FTS5 backend object */
  int iIdx,                       /* Index to work on */
  Fts5Structure **ppStruct,       /* IN/OUT: Current structure of index */
  int nLeaf                       /* Number of output leaves just written */
){
  if( p->rc==SQLITE_OK ){
    Fts5Structure *pStruct = *ppStruct;
    i64 nWrite;                   /* Initial value of write-counter */
    int nWork;                    /* Number of work-quanta to perform */
    int nRem;                     /* Number of leaf pages left to write */

    /* Update the write-counter. While doing so, set nWork. */
    nWrite = pStruct->nWriteCounter;
    nWork = ((nWrite + nLeaf) / p->nWorkUnit) - (nWrite / p->nWorkUnit);
    pStruct->nWriteCounter += nLeaf;
    nRem = p->nWorkUnit * nWork * pStruct->nLevel;

    while( nRem>0 ){
      int iLvl;                   /* To iterate through levels */
      int iBestLvl = 0;           /* Level offering the most input segments */
      int nBest = 0;              /* Number of input segments on best level */

      /* Set iBestLvl to the level to read input segments from. */
      assert( pStruct->nLevel>0 );
      for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
        Fts5StructureLevel *pLvl = &pStruct->aLevel[iLvl];
        if( pLvl->nMerge ){
          if( pLvl->nMerge>nBest ){
            iBestLvl = iLvl;
            nBest = pLvl->nMerge;
          }
          break;
        }
        if( pLvl->nSeg>nBest ){
          nBest = pLvl->nSeg;
          iBestLvl = iLvl;
        }
      }

      /* If nBest is still 0, then the index must be empty. */
#ifdef SQLITE_DEBUG
      for(iLvl=0; nBest==0 && iLvl<pStruct->nLevel; iLvl++){
        assert( pStruct->aLevel[iLvl].nSeg==0 );
      }
#endif

      if( nBest<p->pConfig->nAutomerge 
          && pStruct->aLevel[iBestLvl].nMerge==0 
        ){
        break;
      }
      fts5IndexMergeLevel(p, iIdx, &pStruct, iBestLvl, &nRem);
      fts5StructurePromote(p, iBestLvl+1, pStruct);
      assert( nRem==0 || p->rc==SQLITE_OK );
      *ppStruct = pStruct;
    }
  }
}

static void fts5IndexCrisisMerge(
  Fts5Index *p,                   /* FTS5 backend object */
  int iIdx,                       /* Index to work on */
  Fts5Structure **ppStruct        /* IN/OUT: Current structure of index */
){
  Fts5Structure *pStruct = *ppStruct;
  int iLvl = 0;
  while( p->rc==SQLITE_OK 
      && iLvl<pStruct->nLevel
      && pStruct->aLevel[iLvl].nSeg>=p->nCrisisMerge 
  ){
    fts5IndexMergeLevel(p, iIdx, &pStruct, iLvl, 0);
    fts5StructurePromote(p, iLvl+1, pStruct);
    iLvl++;
  }
  *ppStruct = pStruct;
}

static int fts5IndexReturn(Fts5Index *p){
  int rc = p->rc;
  p->rc = SQLITE_OK;
  return rc;
}

typedef struct Fts5FlushCtx Fts5FlushCtx;
struct Fts5FlushCtx {
  Fts5Index *pIdx;
  Fts5SegWriter writer; 
};

static int fts5FlushNewTerm(void *pCtx, const char *zTerm, int nTerm){
  Fts5FlushCtx *p = (Fts5FlushCtx*)pCtx;
  int rc = SQLITE_OK;
  fts5WriteAppendTerm(p->pIdx, &p->writer, nTerm, (const u8*)zTerm);
  return rc;
}

static int fts5FlushTermDone(void *pCtx){
  Fts5FlushCtx *p = (Fts5FlushCtx*)pCtx;
  int rc = SQLITE_OK;
  /* Write the doclist terminator */
  fts5WriteAppendZerobyte(p->pIdx, &p->writer);
  return rc;
}

static int fts5FlushNewEntry(
  void *pCtx, 
  i64 iRowid, 
  const u8 *aPoslist, 
  int nPoslist
){
  Fts5FlushCtx *p = (Fts5FlushCtx*)pCtx;
  Fts5Index *pIdx = p->pIdx;

  /* Append the rowid itself */
  fts5WriteAppendRowid(pIdx, &p->writer, iRowid);

  /* Append the size of the position list in bytes */
  fts5WriteAppendPoslistInt(pIdx, &p->writer, nPoslist);

  /* And the poslist data */
  fts5WriteAppendPoslistData(pIdx, &p->writer, aPoslist, nPoslist);
  return pIdx->rc;
}

/*
** Flush the contents of in-memory hash table iHash to a new level-0 
** segment on disk. Also update the corresponding structure record.
**
** If an error occurs, set the Fts5Index.rc error code. If an error has 
** already occurred, this function is a no-op.
*/
static void fts5FlushOneHash(Fts5Index *p, int iHash, int *pnLeaf){
  Fts5Structure *pStruct;
  int iSegid;
  int pgnoLast = 0;                 /* Last leaf page number in segment */

  /* Obtain a reference to the index structure and allocate a new segment-id
  ** for the new level-0 segment.  */
  pStruct = fts5StructureRead(p, iHash);
  iSegid = fts5AllocateSegid(p, pStruct);

  if( iSegid ){
    Fts5StructureSegment *pSeg;   /* New segment within pStruct */
    int nHeight;                  /* Height of new segment b-tree */
    int rc;
    Fts5FlushCtx ctx;

    fts5WriteInit(p, &ctx.writer, iHash, iSegid);
    ctx.pIdx = p;

    rc = sqlite3Fts5HashIterate( p->apHash[iHash], (void*)&ctx, 
        fts5FlushNewTerm, fts5FlushNewEntry, fts5FlushTermDone
    );
    if( p->rc==SQLITE_OK ) p->rc = rc;
    fts5WriteFinish(p, &ctx.writer, &nHeight, &pgnoLast);

    /* Update the Fts5Structure. It is written back to the database by the
    ** fts5StructureRelease() call below.  */
    if( pStruct->nLevel==0 ){
      fts5StructureAddLevel(&p->rc, &pStruct);
    }
    fts5StructureExtendLevel(&p->rc, pStruct, 0, 1, 0);
    if( p->rc==SQLITE_OK ){
      pSeg = &pStruct->aLevel[0].aSeg[ pStruct->aLevel[0].nSeg++ ];
      pSeg->iSegid = iSegid;
      pSeg->nHeight = nHeight;
      pSeg->pgnoFirst = 1;
      pSeg->pgnoLast = pgnoLast;
    }
  }

  if( p->pConfig->nAutomerge>0 ) fts5IndexWork(p, iHash, &pStruct, pgnoLast);
  fts5IndexCrisisMerge(p, iHash, &pStruct);
  fts5StructureWrite(p, iHash, pStruct);
  fts5StructureRelease(pStruct);
}

/*
** Flush any data stored in the in-memory hash tables to the database.
*/
static void fts5IndexFlush(Fts5Index *p){
  Fts5Config *pConfig = p->pConfig;
  int i;                          /* Used to iterate through indexes */
  int nLeaf = 0;                  /* Number of leaves written */

  /* If an error has already occured this call is a no-op. */
  if( p->rc!=SQLITE_OK || p->nPendingData==0 ) return;
  assert( p->apHash );

  /* Flush the terms and each prefix index to disk */
  for(i=0; i<=pConfig->nPrefix; i++){
    fts5FlushOneHash(p, i, &nLeaf);
  }
  p->nPendingData = 0;
}


int sqlite3Fts5IndexOptimize(Fts5Index *p){
  Fts5Config *pConfig = p->pConfig;
  int i;

  fts5IndexFlush(p);
  for(i=0; i<=pConfig->nPrefix; i++){
    Fts5Structure *pStruct = fts5StructureRead(p, i);
    Fts5Structure *pNew = 0;
    int nSeg = 0;
    if( pStruct ){
      nSeg = fts5StructureCountSegments(pStruct);
      if( nSeg>1 ){
        int nByte = sizeof(Fts5Structure);
        nByte += (pStruct->nLevel+1) * sizeof(Fts5StructureLevel);
        pNew = (Fts5Structure*)sqlite3Fts5MallocZero(&p->rc, nByte);
      }
    }
    if( pNew ){
      Fts5StructureLevel *pLvl;
      int nByte = nSeg * sizeof(Fts5StructureSegment);
      pNew->nLevel = pStruct->nLevel+1;
      pNew->nWriteCounter = pStruct->nWriteCounter;
      pLvl = &pNew->aLevel[pStruct->nLevel];
      pLvl->aSeg = (Fts5StructureSegment*)sqlite3Fts5MallocZero(&p->rc, nByte);
      if( pLvl->aSeg ){
        int iLvl, iSeg;
        int iSegOut = 0;
        for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
          for(iSeg=0; iSeg<pStruct->aLevel[iLvl].nSeg; iSeg++){
            pLvl->aSeg[iSegOut] = pStruct->aLevel[iLvl].aSeg[iSeg];
            iSegOut++;
          }
        }
        pLvl->nSeg = nSeg;
      }else{
        sqlite3_free(pNew);
        pNew = 0;
      }
    }

    if( pNew ){
      int iLvl = pNew->nLevel-1;
      while( p->rc==SQLITE_OK && pNew->aLevel[iLvl].nSeg>0 ){
        int nRem = FTS5_OPT_WORK_UNIT;
        fts5IndexMergeLevel(p, i, &pNew, iLvl, &nRem);
      }

      fts5StructureWrite(p, i, pNew);
      fts5StructureRelease(pNew);
    }

    fts5StructureRelease(pStruct);
  }

  return fts5IndexReturn(p); 
}



/*
** Return a simple checksum value based on the arguments.
*/
static u64 fts5IndexEntryCksum(
  i64 iRowid, 
  int iCol, 
  int iPos, 
  const char *pTerm, 
  int nTerm
){
  int i;
  u64 ret = iRowid;
  ret += (ret<<3) + iCol;
  ret += (ret<<3) + iPos;
  for(i=0; i<nTerm; i++) ret += (ret<<3) + pTerm[i];
  return ret;
}

static void fts5BtreeIterInit(
  Fts5Index *p, 
  int iIdx,
  Fts5StructureSegment *pSeg, 
  Fts5BtreeIter *pIter
){
  int nByte;
  int i;
  nByte = sizeof(pIter->aLvl[0]) * (pSeg->nHeight-1);
  memset(pIter, 0, sizeof(*pIter));
  pIter->nLvl = pSeg->nHeight-1;
  pIter->iIdx = iIdx;
  pIter->p = p;
  pIter->pSeg = pSeg;
  if( nByte && p->rc==SQLITE_OK ){
    pIter->aLvl = (Fts5BtreeIterLevel*)fts5IdxMalloc(p, nByte);
  }
  for(i=0; p->rc==SQLITE_OK && i<pIter->nLvl; i++){
    i64 iRowid = FTS5_SEGMENT_ROWID(iIdx, pSeg->iSegid, i+1, 1);
    Fts5Data *pData;
    pIter->aLvl[i].pData = pData = fts5DataRead(p, iRowid);
    if( pData ){
      fts5NodeIterInit(pData->p, pData->n, &pIter->aLvl[i].s);
    }
  }

  if( pIter->nLvl==0 || p->rc ){
    pIter->bEof = 1;
    pIter->iLeaf = pSeg->pgnoLast;
  }else{
    pIter->nEmpty = pIter->aLvl[0].s.nEmpty;
    pIter->iLeaf = pIter->aLvl[0].s.iChild;
    pIter->bDlidx = pIter->aLvl[0].s.bDlidx;
  }
}

static void fts5BtreeIterNext(Fts5BtreeIter *pIter){
  Fts5Index *p = pIter->p;
  int i;

  assert( pIter->bEof==0 && pIter->aLvl[0].s.aData );
  for(i=0; i<pIter->nLvl && p->rc==SQLITE_OK; i++){
    Fts5BtreeIterLevel *pLvl = &pIter->aLvl[i];
    fts5NodeIterNext(&p->rc, &pLvl->s);
    if( pLvl->s.aData ){
      fts5BufferSet(&p->rc, &pIter->term, pLvl->s.term.n, pLvl->s.term.p);
      break;
    }else{
      fts5NodeIterFree(&pLvl->s);
      fts5DataRelease(pLvl->pData);
      pLvl->pData = 0;
    }
  }
  if( i==pIter->nLvl || p->rc ){
    pIter->bEof = 1;
  }else{
    int iSegid = pIter->pSeg->iSegid;
    for(i--; i>=0; i--){
      Fts5BtreeIterLevel *pLvl = &pIter->aLvl[i];
      i64 iRowid = FTS5_SEGMENT_ROWID(pIter->iIdx,iSegid,i+1,pLvl[1].s.iChild);
      pLvl->pData = fts5DataRead(p, iRowid);
      if( pLvl->pData ){
        fts5NodeIterInit(pLvl->pData->p, pLvl->pData->n, &pLvl->s);
      }
    }
  }

  pIter->nEmpty = pIter->aLvl[0].s.nEmpty;
  pIter->bDlidx = pIter->aLvl[0].s.bDlidx;
  pIter->iLeaf = pIter->aLvl[0].s.iChild;
  assert( p->rc==SQLITE_OK || pIter->bEof );
}

static void fts5BtreeIterFree(Fts5BtreeIter *pIter){
  int i;
  for(i=0; i<pIter->nLvl; i++){
    Fts5BtreeIterLevel *pLvl = &pIter->aLvl[i];
    fts5NodeIterFree(&pLvl->s);
    if( pLvl->pData ){
      fts5DataRelease(pLvl->pData);
      pLvl->pData = 0;
    }
  }
  sqlite3_free(pIter->aLvl);
  fts5BufferFree(&pIter->term);
}

/*
** This function is purely an internal test. It does not contribute to 
** FTS functionality, or even the integrity-check, in any way.
**
** Instead, it tests that the same set of pgno/rowid combinations are 
** visited regardless of whether the doclist-index identified by parameters
** iIdx/iSegid/iLeaf is iterated in forwards or reverse order.
*/
#ifdef SQLITE_DEBUG
static void fts5DlidxIterTestReverse(
  Fts5Index *p, 
  int iIdx,                       /* Index to load doclist-index from */
  int iSegid,                     /* Segment id to load from */
  int iLeaf                       /* Load doclist-index for this leaf */
){
  Fts5DlidxIter *pDlidx = 0;
  i64 cksum1 = 13;
  i64 cksum2 = 13;

  for(fts5DlidxIterInit(p, 0, iIdx, iSegid, iLeaf, &pDlidx);
      fts5DlidxIterEof(p, pDlidx)==0;
      fts5DlidxIterNext(pDlidx)
  ){
    assert( pDlidx->iLeafPgno>iLeaf );
    cksum1 = (cksum1 ^ ( (i64)(pDlidx->iLeafPgno) << 32 ));
    cksum1 = (cksum1 ^ pDlidx->iRowid);
  }
  fts5DlidxIterFree(pDlidx);
  pDlidx = 0;

  for(fts5DlidxIterInit(p, 1, iIdx, iSegid, iLeaf, &pDlidx);
      fts5DlidxIterEof(p, pDlidx)==0;
      fts5DlidxIterPrev(pDlidx)
  ){
    assert( pDlidx->iLeafPgno>iLeaf );
    cksum2 = (cksum2 ^ ( (i64)(pDlidx->iLeafPgno) << 32 ));
    cksum2 = (cksum2 ^ pDlidx->iRowid);
  }
  fts5DlidxIterFree(pDlidx);
  pDlidx = 0;

  if( p->rc==SQLITE_OK && cksum1!=cksum2 ) p->rc = FTS5_CORRUPT; 
}
#else
# define fts5DlidxIterTestReverse(w,x,y,z)
#endif

static void fts5IndexIntegrityCheckSegment(
  Fts5Index *p,                   /* FTS5 backend object */
  int iIdx,                       /* Index that pSeg is a part of */
  Fts5StructureSegment *pSeg      /* Segment to check internal consistency */
){
  Fts5BtreeIter iter;             /* Used to iterate through b-tree hierarchy */

  /* Iterate through the b-tree hierarchy.  */
  for(fts5BtreeIterInit(p, iIdx, pSeg, &iter);
      iter.bEof==0;
      fts5BtreeIterNext(&iter)
  ){
    i64 iRow;                     /* Rowid for this leaf */
    Fts5Data *pLeaf;              /* Data for this leaf */
    int iOff;                     /* Offset of first term on leaf */
    int i;                        /* Used to iterate through empty leaves */

    /* If the leaf in question has already been trimmed from the segment, 
    ** ignore this b-tree entry. Otherwise, load it into memory. */
    if( iter.iLeaf<pSeg->pgnoFirst ) continue;
    iRow = FTS5_SEGMENT_ROWID(iIdx, pSeg->iSegid, 0, iter.iLeaf);
    pLeaf = fts5DataRead(p, iRow);
    if( pLeaf==0 ) break;

    /* Check that the leaf contains at least one term, and that it is equal
    ** to or larger than the split-key in iter.term.  */
    iOff = fts5GetU16(&pLeaf->p[2]);
    if( iOff==0 ){
      p->rc = FTS5_CORRUPT;
    }else{
      int nTerm;                  /* Size of term on leaf in bytes */
      int res;                    /* Comparison of term and split-key */
      iOff += getVarint32(&pLeaf->p[iOff], nTerm);
      res = memcmp(&pLeaf->p[iOff], iter.term.p, MIN(nTerm, iter.term.n));
      if( res==0 ) res = nTerm - iter.term.n;
      if( res<0 ){
        p->rc = FTS5_CORRUPT;
      }
    }
    fts5DataRelease(pLeaf);
    if( p->rc ) break;

    /* Now check that the iter.nEmpty leaves following the current leaf
    ** (a) exist and (b) contain no terms. */
    for(i=1; p->rc==SQLITE_OK && i<=iter.nEmpty; i++){
      pLeaf = fts5DataRead(p, iRow+i);
      if( pLeaf && 0!=fts5GetU16(&pLeaf->p[2]) ){
        p->rc = FTS5_CORRUPT;
      }
      fts5DataRelease(pLeaf);
    }

    /* If there is a doclist-index, check that it looks right. */
    if( iter.bDlidx ){
      Fts5DlidxIter *pDlidx = 0;  /* For iterating through doclist index */
      int iPrevLeaf = iter.iLeaf;
      int iSegid = pSeg->iSegid;
      int iPg;
      i64 iKey;

      for(fts5DlidxIterInit(p, 0, iIdx, iSegid, iter.iLeaf, &pDlidx);
          fts5DlidxIterEof(p, pDlidx)==0;
          fts5DlidxIterNext(pDlidx)
      ){

        /* Check any rowid-less pages that occur before the current leaf. */
        for(iPg=iPrevLeaf+1; iPg<pDlidx->iLeafPgno; iPg++){
          iKey = FTS5_SEGMENT_ROWID(iIdx, iSegid, 0, iPg);
          pLeaf = fts5DataRead(p, iKey);
          if( pLeaf ){
            if( fts5GetU16(&pLeaf->p[0])!=0 ) p->rc = FTS5_CORRUPT;
            fts5DataRelease(pLeaf);
          }
        }
        iPrevLeaf = pDlidx->iLeafPgno;

        /* Check that the leaf page indicated by the iterator really does
        ** contain the rowid suggested by the same. */
        iKey = FTS5_SEGMENT_ROWID(iIdx, iSegid, 0, pDlidx->iLeafPgno);
        pLeaf = fts5DataRead(p, iKey);
        if( pLeaf ){
          i64 iRowid;
          int iRowidOff = fts5GetU16(&pLeaf->p[0]);
          getVarint(&pLeaf->p[iRowidOff], (u64*)&iRowid);
          if( iRowid!=pDlidx->iRowid ) p->rc = FTS5_CORRUPT;
          fts5DataRelease(pLeaf);
        }

      }

      for(iPg=iPrevLeaf+1; iPg<=(iter.iLeaf + iter.nEmpty); iPg++){
        iKey = FTS5_SEGMENT_ROWID(iIdx, iSegid, 0, iPg);
        pLeaf = fts5DataRead(p, iKey);
        if( pLeaf ){
          if( fts5GetU16(&pLeaf->p[0])!=0 ) p->rc = FTS5_CORRUPT;
          fts5DataRelease(pLeaf);
        }
      }

      fts5DlidxIterFree(pDlidx);
      fts5DlidxIterTestReverse(p, iIdx, iSegid, iter.iLeaf);
    }
  }

  if( p->rc==SQLITE_OK && iter.iLeaf!=pSeg->pgnoLast ){
    p->rc = FTS5_CORRUPT;
  }

  fts5BtreeIterFree(&iter);
}

/*
** Iterator pMulti currently points to a valid entry (not EOF). This
** function appends a copy of the position-list of the entry pMulti 
** currently points to to buffer pBuf.
**
** If an error occurs, an error code is left in p->rc. It is assumed
** no error has already occurred when this function is called.
*/
static void fts5MultiIterPoslist(
  Fts5Index *p,
  Fts5MultiSegIter *pMulti,
  int bSz,
  Fts5Buffer *pBuf
){
  if( p->rc==SQLITE_OK ){
    Fts5ChunkIter iter;
    Fts5SegIter *pSeg = &pMulti->aSeg[ pMulti->aFirst[1] ];
    assert( fts5MultiIterEof(p, pMulti)==0 );
    fts5ChunkIterInit(p, pSeg, &iter);
    if( fts5ChunkIterEof(p, &iter)==0 ){
      if( bSz ){
        fts5BufferAppendVarint(&p->rc, pBuf, iter.nRem);
      }
      while( fts5ChunkIterEof(p, &iter)==0 ){
        fts5BufferAppendBlob(&p->rc, pBuf, iter.n, iter.p);
        fts5ChunkIterNext(p, &iter);
      }
    }
    fts5ChunkIterRelease(&iter);
  }
}

static void fts5DoclistIterNext(Fts5DoclistIter *pIter){
  if( pIter->i<pIter->n ){
    if( pIter->i ){
      i64 iDelta;
      pIter->i += getVarint(&pIter->a[pIter->i], (u64*)&iDelta);
      if( pIter->bAsc ){
        pIter->iRowid += iDelta;
      }else{
        pIter->iRowid -= iDelta;
      }
    }else{
      pIter->i += getVarint(&pIter->a[pIter->i], (u64*)&pIter->iRowid);
    }
    pIter->i += getVarint32(&pIter->a[pIter->i], pIter->nPoslist);
    pIter->aPoslist = &pIter->a[pIter->i];
    pIter->i += pIter->nPoslist;
  }else{
    pIter->aPoslist = 0;
  }
}

static void fts5DoclistIterInit(
  Fts5Buffer *pBuf, 
  int bAsc, 
  Fts5DoclistIter *pIter
){
  memset(pIter, 0, sizeof(*pIter));
  pIter->a = pBuf->p;
  pIter->n = pBuf->n;
  pIter->bAsc = bAsc;
  fts5DoclistIterNext(pIter);
}

/*
** Append a doclist to buffer pBuf.
*/
static void fts5MergeAppendDocid(
  int *pRc,                       /* IN/OUT: Error code */
  int bAsc,
  Fts5Buffer *pBuf,               /* Buffer to write to */
  i64 *piLastRowid,               /* IN/OUT: Previous rowid written (if any) */
  i64 iRowid                      /* Rowid to append */
){
  if( pBuf->n==0 ){
    fts5BufferAppendVarint(pRc, pBuf, iRowid);
  }else if( bAsc==0 ){
    fts5BufferAppendVarint(pRc, pBuf, *piLastRowid - iRowid);
  }else{
    fts5BufferAppendVarint(pRc, pBuf, iRowid - *piLastRowid);
  }
  *piLastRowid = iRowid;
}

/*
** Buffers p1 and p2 contain doclists. This function merges the content
** of the two doclists together and sets buffer p1 to the result before
** returning.
**
** If an error occurs, an error code is left in p->rc. If an error has
** already occurred, this function is a no-op.
*/
static void fts5MergePrefixLists(
  Fts5Index *p,                   /* FTS5 backend object */
  int bAsc,
  Fts5Buffer *p1,                 /* First list to merge */
  Fts5Buffer *p2                  /* Second list to merge */
){
  if( p2->n ){
    i64 iLastRowid = 0;
    Fts5DoclistIter i1;
    Fts5DoclistIter i2;
    Fts5Buffer out;
    Fts5Buffer tmp;
    memset(&out, 0, sizeof(out));
    memset(&tmp, 0, sizeof(tmp));

    fts5DoclistIterInit(p1, bAsc, &i1);
    fts5DoclistIterInit(p2, bAsc, &i2);
    while( i1.aPoslist!=0 || i2.aPoslist!=0 ){
      if( i2.aPoslist==0 || (i1.aPoslist && 
           ( (!bAsc && i1.iRowid>i2.iRowid) || (bAsc && i1.iRowid<i2.iRowid) )
      )){
        /* Copy entry from i1 */
        fts5MergeAppendDocid(&p->rc, bAsc, &out, &iLastRowid, i1.iRowid);
        fts5BufferAppendVarint(&p->rc, &out, i1.nPoslist);
        fts5BufferAppendBlob(&p->rc, &out, i1.nPoslist, i1.aPoslist);
        fts5DoclistIterNext(&i1);
      }
      else if( i1.aPoslist==0 || i2.iRowid!=i1.iRowid ){
        /* Copy entry from i2 */
        fts5MergeAppendDocid(&p->rc, bAsc, &out, &iLastRowid, i2.iRowid);
        fts5BufferAppendVarint(&p->rc, &out, i2.nPoslist);
        fts5BufferAppendBlob(&p->rc, &out, i2.nPoslist, i2.aPoslist);
        fts5DoclistIterNext(&i2);
      }
      else{
        Fts5PoslistReader r1;
        Fts5PoslistReader r2;
        Fts5PoslistWriter writer;

        memset(&writer, 0, sizeof(writer));

        /* Merge the two position lists. */ 
        fts5MergeAppendDocid(&p->rc, bAsc, &out, &iLastRowid, i2.iRowid);
        fts5BufferZero(&tmp);
        sqlite3Fts5PoslistReaderInit(-1, i1.aPoslist, i1.nPoslist, &r1);
        sqlite3Fts5PoslistReaderInit(-1, i2.aPoslist, i2.nPoslist, &r2);
        while( p->rc==SQLITE_OK && (r1.bEof==0 || r2.bEof==0) ){
          i64 iNew;
          if( r2.bEof || (r1.bEof==0 && r1.iPos<r2.iPos) ){
            iNew = r1.iPos;
            sqlite3Fts5PoslistReaderNext(&r1);
          }else{
            iNew = r2.iPos;
            sqlite3Fts5PoslistReaderNext(&r2);
            if( r1.iPos==r2.iPos ) sqlite3Fts5PoslistReaderNext(&r1);
          }
          p->rc = sqlite3Fts5PoslistWriterAppend(&tmp, &writer, iNew);
        }

        fts5BufferAppendVarint(&p->rc, &out, tmp.n);
        fts5BufferAppendBlob(&p->rc, &out, tmp.n, tmp.p);
        fts5DoclistIterNext(&i1);
        fts5DoclistIterNext(&i2);
      }
    }

    fts5BufferSet(&p->rc, p1, out.n, out.p);
    fts5BufferFree(&tmp);
    fts5BufferFree(&out);
  }
}

static void fts5BufferSwap(Fts5Buffer *p1, Fts5Buffer *p2){
  Fts5Buffer tmp = *p1;
  *p1 = *p2;
  *p2 = tmp;
}

static void fts5SetupPrefixIter(
  Fts5Index *p,                   /* Index to read from */
  int bAsc,                       /* True for "ORDER BY rowid ASC" */
  const u8 *pToken,               /* Buffer containing prefix to match */
  int nToken,                     /* Size of buffer pToken in bytes */
  Fts5IndexIter *pIter            /* Populate this object */
){
  Fts5Structure *pStruct;
  Fts5Buffer *aBuf;
  const int nBuf = 32;

  aBuf = (Fts5Buffer*)fts5IdxMalloc(p, sizeof(Fts5Buffer)*nBuf);
  pStruct = fts5StructureRead(p, 0);

  if( aBuf && pStruct ){
    Fts5DoclistIter *pDoclist;
    int i;
    i64 iLastRowid = 0;
    Fts5MultiSegIter *p1 = 0;     /* Iterator used to gather data from index */
    Fts5Buffer doclist;

    memset(&doclist, 0, sizeof(doclist));
    for(fts5MultiIterNew(p, pStruct, 0, 1, 1, pToken, nToken, -1, 0, &p1);
        fts5MultiIterEof(p, p1)==0;
        fts5MultiIterNext(p, p1, 0, 0)
    ){
      i64 iRowid = fts5MultiIterRowid(p1);
      int nTerm;
      const u8 *pTerm = fts5MultiIterTerm(p1, &nTerm);
      assert( memcmp(pToken, pTerm, MIN(nToken, nTerm))<=0 );
      if( nTerm<nToken || memcmp(pToken, pTerm, nToken) ) break;

      if( doclist.n>0 
       && ((!bAsc && iRowid>=iLastRowid) || (bAsc && iRowid<=iLastRowid))
      ){

        for(i=0; doclist.n && p->rc==SQLITE_OK; i++){
          assert( i<nBuf );
          if( aBuf[i].n==0 ){
            fts5BufferSwap(&doclist, &aBuf[i]);
            fts5BufferZero(&doclist);
          }else{
            fts5MergePrefixLists(p, bAsc, &doclist, &aBuf[i]);
            fts5BufferZero(&aBuf[i]);
          }
        }
      }
      if( doclist.n==0 ){
        fts5BufferAppendVarint(&p->rc, &doclist, iRowid);
      }else if( bAsc==0 ){
        fts5BufferAppendVarint(&p->rc, &doclist, iLastRowid - iRowid);
      }else{
        fts5BufferAppendVarint(&p->rc, &doclist, iRowid - iLastRowid);
      }
      iLastRowid = iRowid;
      fts5MultiIterPoslist(p, p1, 1, &doclist);
    }

    for(i=0; i<nBuf; i++){
      fts5MergePrefixLists(p, bAsc, &doclist, &aBuf[i]);
      fts5BufferFree(&aBuf[i]);
    }
    fts5MultiIterFree(p, p1);

    pDoclist = (Fts5DoclistIter*)fts5IdxMalloc(p, sizeof(Fts5DoclistIter));
    if( !pDoclist ){
      fts5BufferFree(&doclist);
    }else{
      pIter->pDoclist = pDoclist;
      fts5DoclistIterInit(&doclist, bAsc, pIter->pDoclist);
    }
  }

  fts5StructureRelease(pStruct);
  sqlite3_free(aBuf);
}

/*
** Run internal checks to ensure that the FTS index (a) is internally 
** consistent and (b) contains entries for which the XOR of the checksums
** as calculated by fts5IndexEntryCksum() is cksum.
**
** Return SQLITE_CORRUPT if any of the internal checks fail, or if the
** checksum does not match. Return SQLITE_OK if all checks pass without
** error, or some other SQLite error code if another error (e.g. OOM)
** occurs.
*/
int sqlite3Fts5IndexIntegrityCheck(Fts5Index *p, u64 cksum){
  Fts5Config *pConfig = p->pConfig;
  int iIdx;                       /* Used to iterate through indexes */
  int rc;                         /* Return code */
  u64 cksum2 = 0;                 /* Checksum based on contents of indexes */

  /* Check that the checksum of the index matches the argument checksum */
  for(iIdx=0; iIdx<=pConfig->nPrefix; iIdx++){
    Fts5MultiSegIter *pIter;
    Fts5Structure *pStruct = fts5StructureRead(p, iIdx);
    for(fts5MultiIterNew(p, pStruct, iIdx, 0, 0, 0, 0, -1, 0, &pIter);
        fts5MultiIterEof(p, pIter)==0;
        fts5MultiIterNext(p, pIter, 0, 0)
    ){
      Fts5PosIter sPos;           /* Used to iterate through position list */
      int n;                      /* Size of term in bytes */
      i64 iRowid = fts5MultiIterRowid(pIter);
      char *z = (char*)fts5MultiIterTerm(pIter, &n);

      for(fts5PosIterInit(p, pIter, &sPos);
          fts5PosIterEof(p, &sPos)==0;
          fts5PosIterNext(p, &sPos)
      ){
        cksum2 ^= fts5IndexEntryCksum(iRowid, sPos.iCol, sPos.iPos, z, n);
#if 0
        fprintf(stdout, "rowid=%d ", (int)iRowid);
        fprintf(stdout, "term=%.*s ", n, z);
        fprintf(stdout, "col=%d ", sPos.iCol);
        fprintf(stdout, "off=%d\n", sPos.iPos);
        fflush(stdout);
#endif
      }
    }
    fts5MultiIterFree(p, pIter);
    fts5StructureRelease(pStruct);
  }
  rc = p->rc;
  if( rc==SQLITE_OK && cksum!=cksum2 ) rc = FTS5_CORRUPT;

  /* Check that the internal nodes of each segment match the leaves */
  for(iIdx=0; rc==SQLITE_OK && iIdx<=pConfig->nPrefix; iIdx++){
    Fts5Structure *pStruct = fts5StructureRead(p, iIdx);
    if( pStruct ){
      int iLvl, iSeg;
      for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
        for(iSeg=0; iSeg<pStruct->aLevel[iLvl].nSeg; iSeg++){
          Fts5StructureSegment *pSeg = &pStruct->aLevel[iLvl].aSeg[iSeg];
          fts5IndexIntegrityCheckSegment(p, iIdx, pSeg);
        }
      }
    }
    fts5StructureRelease(pStruct);
    rc = p->rc;
  }

  return rc;
}


/*
** Indicate that all subsequent calls to sqlite3Fts5IndexWrite() pertain
** to the document with rowid iRowid.
*/
int sqlite3Fts5IndexBeginWrite(Fts5Index *p, i64 iRowid){
  assert( p->rc==SQLITE_OK );
  if( iRowid<=p->iWriteRowid || (p->nPendingData > p->nMaxPendingData) ){
    fts5IndexFlush(p);
  }
  p->iWriteRowid = iRowid;
  return fts5IndexReturn(p);
}

/*
** Commit data to disk.
*/
int sqlite3Fts5IndexSync(Fts5Index *p, int bCommit){
  assert( p->rc==SQLITE_OK );
  fts5IndexFlush(p);
  if( bCommit ) fts5CloseReader(p);
  return fts5IndexReturn(p);
}

/*
** Discard any data stored in the in-memory hash tables. Do not write it
** to the database. Additionally, assume that the contents of the %_data
** table may have changed on disk. So any in-memory caches of %_data 
** records must be invalidated.
*/
int sqlite3Fts5IndexRollback(Fts5Index *p){
  fts5CloseReader(p);
  fts5IndexDiscardData(p);
  assert( p->rc==SQLITE_OK );
  return SQLITE_OK;
}

/*
** The %_data table is completely empty when this function is called. This
** function populates it with the initial structure objects for each index,
** and the initial version of the "averages" record (a zero-byte blob).
*/
int sqlite3Fts5IndexReinit(Fts5Index *p){
  int i;
  Fts5Structure s;

  memset(&s, 0, sizeof(Fts5Structure));
  for(i=0; i<p->pConfig->nPrefix+1; i++){
    fts5StructureWrite(p, i, &s);
  }
  if( p->rc==SQLITE_OK ){
    p->rc = sqlite3Fts5IndexSetAverages(p, (const u8*)"", 0);
  }

  return fts5IndexReturn(p);
}

/*
** Open a new Fts5Index handle. If the bCreate argument is true, create
** and initialize the underlying %_data table.
**
** If successful, set *pp to point to the new object and return SQLITE_OK.
** Otherwise, set *pp to NULL and return an SQLite error code.
*/
int sqlite3Fts5IndexOpen(
  Fts5Config *pConfig, 
  int bCreate, 
  Fts5Index **pp,
  char **pzErr
){
  int rc = SQLITE_OK;
  Fts5Index *p;                   /* New object */

  *pp = p = (Fts5Index*)sqlite3_malloc(sizeof(Fts5Index));
  if( !p ) return SQLITE_NOMEM;

  memset(p, 0, sizeof(Fts5Index));
  p->pConfig = pConfig;
  p->nCrisisMerge = FTS5_CRISIS_MERGE;
  p->nWorkUnit = FTS5_WORK_UNIT;
  p->nMaxPendingData = 1024*1024;
  p->zDataTbl = sqlite3_mprintf("%s_data", pConfig->zName);
  if( p->zDataTbl==0 ){
    rc = SQLITE_NOMEM;
  }else if( bCreate ){
    rc = sqlite3Fts5CreateTable(
        pConfig, "data", "id INTEGER PRIMARY KEY, block BLOB", 0, pzErr
    );
    if( rc==SQLITE_OK ){
      rc = sqlite3Fts5IndexReinit(p);
    }
  }

  assert( p->rc==SQLITE_OK || rc!=SQLITE_OK );
  if( rc ){
    sqlite3Fts5IndexClose(p, 0);
    *pp = 0;
  }
  return rc;
}

/*
** Close a handle opened by an earlier call to sqlite3Fts5IndexOpen().
*/
int sqlite3Fts5IndexClose(Fts5Index *p, int bDestroy){
  int rc = SQLITE_OK;
  if( p ){
    if( bDestroy ){
      rc = sqlite3Fts5DropTable(p->pConfig, "data");
    }
    assert( p->pReader==0 );
    sqlite3_finalize(p->pWriter);
    sqlite3_finalize(p->pDeleter);
    if( p->apHash ){
      int i;
      for(i=0; i<=p->pConfig->nPrefix; i++){
        sqlite3Fts5HashFree(p->apHash[i]);
      }
      sqlite3_free(p->apHash);
    }
    sqlite3_free(p->zDataTbl);
    sqlite3_free(p);
  }
  return rc;
}

/*
** Argument p points to a buffer containing utf-8 text that is n bytes in 
** size. Return the number of bytes in the nChar character prefix of the
** buffer, or 0 if there are less than nChar characters in total.
*/
static int fts5IndexCharlenToBytelen(const char *p, int nByte, int nChar){
  int n = 0;
  int i;
  for(i=0; i<nChar; i++){
    if( n>=nByte ) return 0;      /* Input contains fewer than nChar chars */
    if( (unsigned char)p[n++]>=0xc0 ){
      while( (p[n] & 0xc0)==0x80 ) n++;
    }
  }
  return n;
}

/*
** pIn is a UTF-8 encoded string, nIn bytes in size. Return the number of
** unicode characters in the string.
*/
int fts5IndexCharlen(const char *pIn, int nIn){
  int nChar = 0;            
  int i = 0;
  while( i<nIn ){
    if( (unsigned char)pIn[i++]>=0xc0 ){
      while( i<nIn && (pIn[i] & 0xc0)==0x80 ) i++;
    }
    nChar++;
  }
  return nChar;
}

/*
** Calculate and return a checksum that is the XOR of the index entry
** checksum of all entries that would be generated by the token specified
** by the final 5 arguments.
*/
u64 sqlite3Fts5IndexCksum(
  Fts5Config *pConfig,            /* Configuration object */
  i64 iRowid,                     /* Document term appears in */
  int iCol,                       /* Column term appears in */
  int iPos,                       /* Position term appears in */
  const char *pTerm, int nTerm    /* Term at iPos */
){
  u64 ret = 0;                    /* Return value */
  int iIdx;                       /* For iterating through indexes */

  ret = fts5IndexEntryCksum(iRowid, iCol, iPos, pTerm, nTerm);

  for(iIdx=0; iIdx<pConfig->nPrefix; iIdx++){
    int nByte = fts5IndexCharlenToBytelen(pTerm, nTerm, pConfig->aPrefix[iIdx]);
    if( nByte ){
      ret ^= fts5IndexEntryCksum(iRowid, iCol, iPos, pTerm, nByte);
    }
  }

  return ret;
}

/*
** Insert or remove data to or from the index. Each time a document is 
** added to or removed from the index, this function is called one or more
** times.
**
** For an insert, it must be called once for each token in the new document.
** If the operation is a delete, it must be called (at least) once for each
** unique token in the document with an iCol value less than zero. The iPos
** argument is ignored for a delete.
*/
int sqlite3Fts5IndexWrite(
  Fts5Index *p,                   /* Index to write to */
  int iCol,                       /* Column token appears in (-ve -> delete) */
  int iPos,                       /* Position of token within column */
  const char *pToken, int nToken  /* Token to add or remove to or from index */
){
  int i;                          /* Used to iterate through indexes */
  Fts5Config *pConfig = p->pConfig;
  assert( p->rc==SQLITE_OK );

  /* Allocate hash tables if they have not already been allocated */
  if( p->apHash==0 ){
    int nHash = pConfig->nPrefix + 1;
    p->apHash = (Fts5Hash**)fts5IdxMalloc(p, sizeof(Fts5Hash*) * nHash);
    for(i=0; p->rc==SQLITE_OK && i<nHash; i++){
      p->rc = sqlite3Fts5HashNew(&p->apHash[i], &p->nPendingData);
    }
  }

  /* Add the new token to the main terms hash table. And to each of the
  ** prefix hash tables that it is large enough for. */
  fts5AddTermToHash(p, 0, iCol, iPos, pToken, nToken);
  for(i=0; i<pConfig->nPrefix; i++){
    int nByte = fts5IndexCharlenToBytelen(pToken, nToken, pConfig->aPrefix[i]);
    if( nByte ){
      fts5AddTermToHash(p, i+1, iCol, iPos, pToken, nByte);
    }
  }

  return fts5IndexReturn(p);
}

/*
** Open a new iterator to iterate though all docids that match the 
** specified token or token prefix.
*/
int sqlite3Fts5IndexQuery(
  Fts5Index *p,                   /* FTS index to query */
  const char *pToken, int nToken, /* Token (or prefix) to query for */
  int flags,                      /* Mask of FTS5INDEX_QUERY_X flags */
  Fts5IndexIter **ppIter          /* OUT: New iterator object */
){
  Fts5IndexIter *pRet;
  int iIdx = 0;

  if( flags & FTS5INDEX_QUERY_PREFIX ){
    Fts5Config *pConfig = p->pConfig;
    int nChar = fts5IndexCharlen(pToken, nToken);
    for(iIdx=1; iIdx<=pConfig->nPrefix; iIdx++){
      if( pConfig->aPrefix[iIdx-1]==nChar ) break;
    }
    if( iIdx>pConfig->nPrefix ){
      iIdx = -1;
    }
  }

  pRet = (Fts5IndexIter*)sqlite3Fts5MallocZero(&p->rc, sizeof(Fts5IndexIter));
  if( pRet ){
    memset(pRet, 0, sizeof(Fts5IndexIter));

    pRet->pIndex = p;
    if( iIdx>=0 ){
      pRet->pStruct = fts5StructureRead(p, iIdx);
      if( pRet->pStruct ){
        fts5MultiIterNew(p, pRet->pStruct, 
            iIdx, 1, flags, (const u8*)pToken, nToken, -1, 0, &pRet->pMulti
        );
      }
    }else{
      int bAsc = (flags & FTS5INDEX_QUERY_ASC)!=0;
      fts5SetupPrefixIter(p, bAsc, (const u8*)pToken, nToken, pRet);
    }
  }

  if( p->rc ){
    sqlite3Fts5IterClose(pRet);
    pRet = 0;
  }
  *ppIter = pRet;
  return fts5IndexReturn(p);
}

/*
** Return true if the iterator passed as the only argument is at EOF.
*/
int sqlite3Fts5IterEof(Fts5IndexIter *pIter){
  assert( pIter->pIndex->rc==SQLITE_OK );
  if( pIter->pDoclist ){ 
    return pIter->pDoclist->aPoslist==0; 
  }else{
    return fts5MultiIterEof(pIter->pIndex, pIter->pMulti);
  }
}

/*
** Move to the next matching rowid. 
*/
int sqlite3Fts5IterNext(Fts5IndexIter *pIter){
  assert( pIter->pIndex->rc==SQLITE_OK );
  if( pIter->pDoclist ){
    fts5DoclistIterNext(pIter->pDoclist);
  }else{
    fts5BufferZero(&pIter->poslist);
    fts5MultiIterNext(pIter->pIndex, pIter->pMulti, 0, 0);
  }
  return fts5IndexReturn(pIter->pIndex);
}

/*
** Move the doclist-iter passed as the first argument to the next 
** matching rowid that occurs at or after iMatch. The definition of "at 
** or after" depends on whether this iterator iterates in ascending or 
** descending rowid order.
*/
static void fts5DoclistIterNextFrom(Fts5DoclistIter *p, i64 iMatch){
  do{
    i64 iRowid = p->iRowid;
    if( p->bAsc!=0 && iRowid>=iMatch ) break;
    if( p->bAsc==0 && iRowid<=iMatch ) break;
    fts5DoclistIterNext(p);
  }while( p->aPoslist );
}

/*
** Move to the next matching rowid that occurs at or after iMatch. The
** definition of "at or after" depends on whether this iterator iterates
** in ascending or descending rowid order.
*/
int sqlite3Fts5IterNextFrom(Fts5IndexIter *pIter, i64 iMatch){
  if( pIter->pDoclist ){
    fts5DoclistIterNextFrom(pIter->pDoclist, iMatch);
  }else{
    fts5MultiIterNextFrom(pIter->pIndex, pIter->pMulti, iMatch);
  }
  return fts5IndexReturn(pIter->pIndex);
}

/*
** Return the current rowid.
*/
i64 sqlite3Fts5IterRowid(Fts5IndexIter *pIter){
  if( pIter->pDoclist ){
    return pIter->pDoclist->iRowid;
  }else{
    return fts5MultiIterRowid(pIter->pMulti);
  }
}


/*
** Return a pointer to a buffer containing a copy of the position list for
** the current entry. Output variable *pn is set to the size of the buffer 
** in bytes before returning.
**
** The returned buffer does not include the 0x00 terminator byte stored on
** disk.
*/
int sqlite3Fts5IterPoslist(Fts5IndexIter *pIter, const u8 **pp, int *pn){
  assert( pIter->pIndex->rc==SQLITE_OK );
  if( pIter->pDoclist ){
    *pn = pIter->pDoclist->nPoslist;
    *pp = pIter->pDoclist->aPoslist;
  }else{
    Fts5Index *p = pIter->pIndex;
    fts5BufferZero(&pIter->poslist);
    fts5MultiIterPoslist(p, pIter->pMulti, 0, &pIter->poslist);
    *pn = pIter->poslist.n;
    *pp = pIter->poslist.p;
  }
  return fts5IndexReturn(pIter->pIndex);
}

/*
** Close an iterator opened by an earlier call to sqlite3Fts5IndexQuery().
*/
void sqlite3Fts5IterClose(Fts5IndexIter *pIter){
  if( pIter ){
    if( pIter->pDoclist ){
      sqlite3_free(pIter->pDoclist->a);
      sqlite3_free(pIter->pDoclist);
    }else{
      fts5MultiIterFree(pIter->pIndex, pIter->pMulti);
      fts5StructureRelease(pIter->pStruct);
      fts5BufferFree(&pIter->poslist);
    }
    fts5CloseReader(pIter->pIndex);
    sqlite3_free(pIter);
  }
}

/*
** Read the "averages" record into the buffer supplied as the second 
** argument. Return SQLITE_OK if successful, or an SQLite error code
** if an error occurs.
*/
int sqlite3Fts5IndexGetAverages(Fts5Index *p, Fts5Buffer *pBuf){
  assert( p->rc==SQLITE_OK );
  fts5DataReadOrBuffer(p, pBuf, FTS5_AVERAGES_ROWID);
  return fts5IndexReturn(p);
}

/*
** Replace the current "averages" record with the contents of the buffer 
** supplied as the second argument.
*/
int sqlite3Fts5IndexSetAverages(Fts5Index *p, const u8 *pData, int nData){
  assert( p->rc==SQLITE_OK );
  fts5DataWrite(p, FTS5_AVERAGES_ROWID, pData, nData);
  return fts5IndexReturn(p);
}

/*
** Return the total number of blocks this module has read from the %_data
** table since it was created.
*/
int sqlite3Fts5IndexReads(Fts5Index *p){
  return p->nRead;
}

/*
** Set the 32-bit cookie value stored at the start of all structure 
** records to the value passed as the second argument.
**
** Return SQLITE_OK if successful, or an SQLite error code if an error
** occurs.
*/
int sqlite3Fts5IndexSetCookie(Fts5Index *p, int iNew){
  int rc = SQLITE_OK;
  Fts5Config *pConfig = p->pConfig;
  u8 aCookie[4];
  int i;

  assert( p->rc==SQLITE_OK );
  sqlite3Fts5Put32(aCookie, iNew);
  for(i=0; rc==SQLITE_OK && i<=pConfig->nPrefix; i++){
    sqlite3_blob *pBlob = 0;
    i64 iRowid = FTS5_STRUCTURE_ROWID(i);
    rc = sqlite3_blob_open(
        pConfig->db, pConfig->zDb, p->zDataTbl, "block", iRowid, 1, &pBlob
    );
    if( rc==SQLITE_OK ){
      sqlite3_blob_write(pBlob, aCookie, 4, 0);
      rc = sqlite3_blob_close(pBlob);
    }
  }

  return rc;
}

int sqlite3Fts5IndexLoadConfig(Fts5Index *p){
  Fts5Structure *pStruct;
  pStruct = fts5StructureRead(p, 0);
  fts5StructureRelease(pStruct);
  return fts5IndexReturn(p);
}

/*************************************************************************
**************************************************************************
** Below this point is the implementation of the fts5_decode() scalar
** function only.
*/

/*
** Decode a segment-data rowid from the %_data table. This function is
** the opposite of macro FTS5_SEGMENT_ROWID().
*/
static void fts5DecodeRowid(
  i64 iRowid,                     /* Rowid from %_data table */
  int *piIdx,                     /* OUT: Index */
  int *piSegid,                   /* OUT: Segment id */
  int *piHeight,                  /* OUT: Height */
  int *piPgno                     /* OUT: Page number */
){
  *piPgno = (int)(iRowid & (((i64)1 << FTS5_DATA_PAGE_B) - 1));
  iRowid >>= FTS5_DATA_PAGE_B;

  *piHeight = (int)(iRowid & (((i64)1 << FTS5_DATA_HEIGHT_B) - 1));
  iRowid >>= FTS5_DATA_HEIGHT_B;

  *piSegid = (int)(iRowid & (((i64)1 << FTS5_DATA_ID_B) - 1));
  iRowid >>= FTS5_DATA_ID_B;

  *piIdx = (int)(iRowid & (((i64)1 << FTS5_DATA_IDX_B) - 1));
}

static void fts5DebugRowid(int *pRc, Fts5Buffer *pBuf, i64 iKey){
  int iIdx,iSegid,iHeight,iPgno;  /* Rowid compenents */
  fts5DecodeRowid(iKey, &iIdx, &iSegid, &iHeight, &iPgno);

  if( iSegid==0 ){
    if( iKey==FTS5_AVERAGES_ROWID ){
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(averages) ");
    }else{
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, 
          "{structure idx=%d}", (int)(iKey-10)
      );
    }
  }
  else if( iHeight==FTS5_SEGMENT_MAX_HEIGHT ){
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(dlidx idx=%d segid=%d pgno=%d)",
        iIdx, iSegid, iPgno
    );
  }else{
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(idx=%d segid=%d h=%d pgno=%d)",
        iIdx, iSegid, iHeight, iPgno
    );
  }
}

static void fts5DebugStructure(
  int *pRc,                       /* IN/OUT: error code */
  Fts5Buffer *pBuf,
  Fts5Structure *p
){
  int iLvl, iSeg;                 /* Iterate through levels, segments */

  for(iLvl=0; iLvl<p->nLevel; iLvl++){
    Fts5StructureLevel *pLvl = &p->aLevel[iLvl];
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, 
        " {lvl=%d nMerge=%d", iLvl, pLvl->nMerge
    );
    for(iSeg=0; iSeg<pLvl->nSeg; iSeg++){
      Fts5StructureSegment *pSeg = &pLvl->aSeg[iSeg];
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, 
          " {id=%d h=%d leaves=%d..%d}", pSeg->iSegid, pSeg->nHeight, 
          pSeg->pgnoFirst, pSeg->pgnoLast
      );
    }
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "}");
  }
}

/*
** This is part of the fts5_decode() debugging aid.
**
** Arguments pBlob/nBlob contain a serialized Fts5Structure object. This
** function appends a human-readable representation of the same object
** to the buffer passed as the second argument. 
*/
static void fts5DecodeStructure(
  int *pRc,                       /* IN/OUT: error code */
  Fts5Buffer *pBuf,
  const u8 *pBlob, int nBlob
){
  int rc;                         /* Return code */
  Fts5Structure *p = 0;           /* Decoded structure object */

  rc = fts5StructureDecode(pBlob, nBlob, 0, &p);
  if( rc!=SQLITE_OK ){
    *pRc = rc;
    return;
  }

  fts5DebugStructure(pRc, pBuf, p);
  fts5StructureRelease(p);
}

/*
** Buffer (a/n) is assumed to contain a list of serialized varints. Read
** each varint and append its string representation to buffer pBuf. Return
** after either the input buffer is exhausted or a 0 value is read.
**
** The return value is the number of bytes read from the input buffer.
*/
static int fts5DecodePoslist(int *pRc, Fts5Buffer *pBuf, const u8 *a, int n){
  int iOff = 0;
  while( iOff<n ){
    int iVal;
    iOff += getVarint32(&a[iOff], iVal);
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, " %d", iVal);
  }
  return iOff;
}

/*
** The start of buffer (a/n) contains the start of a doclist. The doclist
** may or may not finish within the buffer. This function appends a text
** representation of the part of the doclist that is present to buffer
** pBuf. 
**
** The return value is the number of bytes read from the input buffer.
*/
static int fts5DecodeDoclist(int *pRc, Fts5Buffer *pBuf, const u8 *a, int n){
  i64 iDocid;
  int iOff = 0;

  if( iOff<n ){
    iOff += sqlite3GetVarint(&a[iOff], (u64*)&iDocid);
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, " rowid=%lld", iDocid);
  }
  while( iOff<n ){
    int nPos;
    iOff += getVarint32(&a[iOff], nPos);
    iOff += fts5DecodePoslist(pRc, pBuf, &a[iOff], MIN(n-iOff, nPos));
    if( iOff<n ){
      i64 iDelta;
      iOff += sqlite3GetVarint(&a[iOff], (u64*)&iDelta);
      if( iDelta==0 ) return iOff;
      iDocid -= iDelta;
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, " rowid=%lld", iDocid);
    }
  }

  return iOff;
}

/*
** The implementation of user-defined scalar function fts5_decode().
*/
static void fts5DecodeFunction(
  sqlite3_context *pCtx,          /* Function call context */
  int nArg,                       /* Number of args (always 2) */
  sqlite3_value **apVal           /* Function arguments */
){
  i64 iRowid;                     /* Rowid for record being decoded */
  int iIdx,iSegid,iHeight,iPgno;  /* Rowid components */
  const u8 *a; int n;             /* Record to decode */
  Fts5Buffer s;                   /* Build up text to return here */
  int rc = SQLITE_OK;             /* Return code */

  assert( nArg==2 );
  memset(&s, 0, sizeof(Fts5Buffer));
  iRowid = sqlite3_value_int64(apVal[0]);
  n = sqlite3_value_bytes(apVal[1]);
  a = sqlite3_value_blob(apVal[1]);
  fts5DecodeRowid(iRowid, &iIdx, &iSegid, &iHeight, &iPgno);

  fts5DebugRowid(&rc, &s, iRowid);
  if( iHeight==FTS5_SEGMENT_MAX_HEIGHT ){
    int i = 0;
    i64 iPrev;
    if( n>0 ){
      i = getVarint(&a[i], (u64*)&iPrev);
      sqlite3Fts5BufferAppendPrintf(&rc, &s, " %lld", iPrev);
    }
    while( i<n ){
      i64 iVal;
      i += getVarint(&a[i], (u64*)&iVal);
      if( iVal==0 ){
        sqlite3Fts5BufferAppendPrintf(&rc, &s, " x");
      }else{
        iPrev = iPrev - iVal;
        sqlite3Fts5BufferAppendPrintf(&rc, &s, " %lld", iPrev);
      }
    }

  }else
  if( iSegid==0 ){
    if( iRowid==FTS5_AVERAGES_ROWID ){
      /* todo */
    }else{
      fts5DecodeStructure(&rc, &s, a, n);
    }
  }else{

    Fts5Buffer term;
    memset(&term, 0, sizeof(Fts5Buffer));

    if( iHeight==0 ){
      int iTermOff = 0;
      int iRowidOff = 0;
      int iOff;
      int nKeep = 0;

      iRowidOff = fts5GetU16(&a[0]);
      iTermOff = fts5GetU16(&a[2]);

      if( iRowidOff ){
        iOff = iRowidOff;
      }else if( iTermOff ){
        iOff = iTermOff;
      }else{
        iOff = n;
      }
      fts5DecodePoslist(&rc, &s, &a[4], iOff-4);


      assert( iRowidOff==0 || iOff==iRowidOff );
      if( iRowidOff ){
        iOff += fts5DecodeDoclist(&rc, &s, &a[iOff], n-iOff);
      }

      assert( iTermOff==0 || iOff==iTermOff );
      while( iOff<n ){
        int nByte;
        iOff += getVarint32(&a[iOff], nByte);
        term.n= nKeep;
        fts5BufferAppendBlob(&rc, &term, nByte, &a[iOff]);
        iOff += nByte;

        sqlite3Fts5BufferAppendPrintf(
            &rc, &s, " term=%.*s", term.n, (const char*)term.p
        );
        iOff += fts5DecodeDoclist(&rc, &s, &a[iOff], n-iOff);
        if( iOff<n ){
          iOff += getVarint32(&a[iOff], nKeep);
        }
      }
      fts5BufferFree(&term);
    }else{
      Fts5NodeIter ss;
      for(fts5NodeIterInit(a, n, &ss); ss.aData; fts5NodeIterNext(&rc, &ss)){
        if( ss.term.n==0 ){
          sqlite3Fts5BufferAppendPrintf(&rc, &s, " left=%d", ss.iChild);
        }else{
          sqlite3Fts5BufferAppendPrintf(&rc,&s, " \"%.*s\"", 
              ss.term.n, ss.term.p
          );
        }
        if( ss.nEmpty ){
          sqlite3Fts5BufferAppendPrintf(&rc, &s, " empty=%d%s", ss.nEmpty,
              ss.bDlidx ? "*" : ""
          );
        }
      }
      fts5NodeIterFree(&ss);
    }
  }
  
  if( rc==SQLITE_OK ){
    sqlite3_result_text(pCtx, (const char*)s.p, s.n, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_error_code(pCtx, rc);
  }
  fts5BufferFree(&s);
}

/*
** The implementation of user-defined scalar function fts5_rowid().
*/
static void fts5RowidFunction(
  sqlite3_context *pCtx,          /* Function call context */
  int nArg,                       /* Number of args (always 2) */
  sqlite3_value **apVal           /* Function arguments */
){
  const char *zArg;
  if( nArg==0 ){
    sqlite3_result_error(pCtx, "should be: fts5_rowid(subject, ....)", -1);
  }else{
    zArg = (const char*)sqlite3_value_text(apVal[0]);
    if( 0==sqlite3_stricmp(zArg, "segment") ){
      i64 iRowid;
      int idx, segid, height, pgno;
      if( nArg!=5 ){
        sqlite3_result_error(pCtx, 
            "should be: fts5_rowid('segment', idx, segid, height, pgno))", -1
        );
      }else{
        idx = sqlite3_value_int(apVal[1]);
        segid = sqlite3_value_int(apVal[2]);
        height = sqlite3_value_int(apVal[3]);
        pgno = sqlite3_value_int(apVal[4]);
        iRowid = FTS5_SEGMENT_ROWID(idx, segid, height, pgno);
        sqlite3_result_int64(pCtx, iRowid);
      }
    }else if( 0==sqlite3_stricmp(zArg, "start-of-index") ){
      i64 iRowid;
      int idx;
      if( nArg!=2 ){
        sqlite3_result_error(pCtx, 
            "should be: fts5_rowid('start-of-index', idx)", -1
        );
      }else{
        idx = sqlite3_value_int(apVal[1]);
        iRowid = FTS5_SEGMENT_ROWID(idx, 1, 0, 0);
        sqlite3_result_int64(pCtx, iRowid);
      }
    }else {
      sqlite3_result_error(pCtx, 
        "first arg to fts5_rowid() must be 'segment' "
        "or 'start-of-index' ..."
        , -1
      );
    }
  }
}

/*
** This is called as part of registering the FTS5 module with database
** connection db. It registers several user-defined scalar functions useful
** with FTS5.
**
** If successful, SQLITE_OK is returned. If an error occurs, some other
** SQLite error code is returned instead.
*/
int sqlite3Fts5IndexInit(sqlite3 *db){
  int rc = sqlite3_create_function(
      db, "fts5_decode", 2, SQLITE_UTF8, 0, fts5DecodeFunction, 0, 0
  );
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(
        db, "fts5_rowid", -1, SQLITE_UTF8, 0, fts5RowidFunction, 0, 0
    );
  }
  return rc;
}

