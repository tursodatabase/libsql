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

#define FTS5_MIN_DLIDX_SIZE 4     /* Add dlidx if this many empty pages */

#define FTS5_MAIN_PREFIX '0'

#if FTS5_MAX_PREFIX_INDEXES > 31
# error "FTS5_MAX_PREFIX_INDEXES is too large"
#endif

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
**           poslist: next poslist
**         }
**         0x00 byte
**
**     poslist format:
**
**         varint: size of poslist in bytes multiplied by 2, not including
**                 this field. Plus 1 if this entry carries the "delete" flag.
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
**   Doclist indexes are themselves b-trees, however they usually consist of
**   a single leaf record only. The format of each doclist index leaf page 
**   is:
**
**     * Flags byte. Bits are:
**         0x01: Clear if leaf is also the root page, otherwise set.
**
**     * Page number of fts index leaf page. As a varint.
**
**     * First docid on page indicated by previous field. As a varint.
**
**     * A list of varints, one for each subsequent termless page. A 
**       positive delta if the termless page contains at least one docid, 
**       or an 0x00 byte otherwise.
**
**   Internal doclist index nodes are:
**
**     * Flags byte. Bits are:
**         0x01: Clear for root page, otherwise set.
**
**     * Page number of first child page. As a varint.
**
**     * Copy of first docid on page indicated by previous field. As a varint.
**
**     * A list of delta-encoded varints - the first docid on each subsequent
**       child page. 
**
*/

/*
** Rowids for the averages and structure records in the %_data table.
*/
#define FTS5_AVERAGES_ROWID     1    /* Rowid used for the averages record */
#define FTS5_STRUCTURE_ROWID   10    /* The structure record */

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
#define FTS5_DATA_ID_B     16     /* Max seg id number 65535 */
#define FTS5_DATA_DLI_B     1     /* Doclist-index flag (1 bit) */
#define FTS5_DATA_HEIGHT_B  5     /* Max b-tree height of 32 */
#define FTS5_DATA_PAGE_B   31     /* Max page number of 2147483648 */

#define fts5_dri(segid, dlidx, height, pgno) (                                 \
 ((i64)(segid)  << (FTS5_DATA_PAGE_B+FTS5_DATA_HEIGHT_B+FTS5_DATA_DLI_B)) +    \
 ((i64)(dlidx)  << (FTS5_DATA_PAGE_B + FTS5_DATA_HEIGHT_B)) +                  \
 ((i64)(height) << (FTS5_DATA_PAGE_B)) +                                       \
 ((i64)(pgno))                                                                 \
)

#define FTS5_SEGMENT_ROWID(segid, height, pgno) fts5_dri(segid, 0, height, pgno)
#define FTS5_DLIDX_ROWID(segid, height, pgno)   fts5_dri(segid, 1, height, pgno)

/*
** Maximum segments permitted in a single index 
*/
#define FTS5_MAX_SEGMENT 2000

#ifdef SQLITE_DEBUG
int sqlite3Fts5Corrupt() { return SQLITE_CORRUPT_VTAB; }
#endif


/*
** Each time a blob is read from the %_data table, it is padded with this
** many zero bytes. This makes it easier to decode the various record formats
** without overreading if the records are corrupt.
*/
#define FTS5_DATA_ZERO_PADDING 8
#define FTS5_DATA_PADDING 20

typedef struct Fts5Data Fts5Data;
typedef struct Fts5DlidxIter Fts5DlidxIter;
typedef struct Fts5DlidxLvl Fts5DlidxLvl;
typedef struct Fts5DlidxWriter Fts5DlidxWriter;
typedef struct Fts5NodeIter Fts5NodeIter;
typedef struct Fts5PageWriter Fts5PageWriter;
typedef struct Fts5SegIter Fts5SegIter;
typedef struct Fts5DoclistIter Fts5DoclistIter;
typedef struct Fts5SegWriter Fts5SegWriter;
typedef struct Fts5Structure Fts5Structure;
typedef struct Fts5StructureLevel Fts5StructureLevel;
typedef struct Fts5StructureSegment Fts5StructureSegment;

struct Fts5Data {
  u8 *p;                          /* Pointer to buffer containing record */
  int n;                          /* Size of record in bytes */
};

/*
** One object per %_data table.
*/
struct Fts5Index {
  Fts5Config *pConfig;            /* Virtual table configuration */
  char *zDataTbl;                 /* Name of %_data table */
  int nWorkUnit;                  /* Leaf pages in a "unit" of work */

  /*
  ** Variables related to the accumulation of tokens and doclists within the
  ** in-memory hash tables before they are flushed to disk.
  */
  Fts5Hash *pHash;                /* Hash table for in-memory data */
  int nMaxPendingData;            /* Max pending data before flush to disk */
  int nPendingData;               /* Current bytes of pending data */
  i64 iWriteRowid;                /* Rowid for current doc being written */
  Fts5Buffer scratch;

  /* Error state. */
  int rc;                         /* Current error code */

  /* State used by the fts5DataXXX() functions. */
  sqlite3_blob *pReader;          /* RO incr-blob open on %_data table */
  sqlite3_stmt *pWriter;          /* "INSERT ... %_data VALUES(?,?)" */
  sqlite3_stmt *pDeleter;         /* "DELETE FROM %_data ... id>=? AND id<=?" */
  sqlite3_stmt *pIdxWriter;       /* "INSERT ... %_idx VALUES(?,?,?,?)" */
  sqlite3_stmt *pIdxDeleter;      /* "DELETE FROM %_idx WHERE segid=? */
  sqlite3_stmt *pIdxSelect;
  int nRead;                      /* Total number of blocks read */
};

struct Fts5DoclistIter {
  u8 *a;
  int n;
  int i;

  /* Output variables. aPoslist==0 at EOF */
  i64 iRowid;
  u8 *aPoslist;
  int nPoslist;
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
  int nRef;                       /* Object reference count */
  u64 nWriteCounter;              /* Total leaves written to level 0 */
  int nSegment;                   /* Total segments in this structure */
  int nLevel;                     /* Number of levels in this index */
  Fts5StructureLevel aLevel[1];   /* Array of nLevel level objects */
};

/*
** An object of type Fts5SegWriter is used to write to segments.
*/
struct Fts5PageWriter {
  int pgno;                       /* Page number for this page */
  Fts5Buffer buf;                 /* Buffer containing page data */
  Fts5Buffer term;                /* Buffer containing previous term on page */
};
struct Fts5DlidxWriter {
  int pgno;                       /* Page number for this page */
  int bPrevValid;                 /* True if iPrev is valid */
  i64 iPrev;                      /* Previous docid value written to page */
  Fts5Buffer buf;                 /* Buffer containing page data */
};
struct Fts5SegWriter {
  int iSegid;                     /* Segid to write to */
  Fts5PageWriter writer;          /* PageWriter object */
  i64 iPrevRowid;                 /* Previous docid written to current leaf */
  u8 bFirstRowidInDoclist;        /* True if next rowid is first in doclist */
  u8 bFirstRowidInPage;           /* True if next rowid is first in page */
  u8 bFirstTermInPage;            /* True if next term will be first in leaf */
  int nLeafWritten;               /* Number of leaf pages written */
  int nEmpty;                     /* Number of contiguous term-less nodes */

  int nDlidx;                     /* Allocated size of aDlidx[] array */
  Fts5DlidxWriter *aDlidx;        /* Array of Fts5DlidxWriter objects */

  /* Values to insert into the %_idx table */
  Fts5Buffer btterm;              /* Next term to insert into %_idx table */
  int iBtPage;                    /* Page number corresponding to btterm */
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

typedef struct Fts5CResult Fts5CResult;
struct Fts5CResult {
  u16 iFirst;                     /* aSeg[] index of firstest iterator */
  u8 bTermEq;                     /* True if the terms are equal */
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
**   Byte offset within the current leaf that is the first byte of the 
**   position list data (one byte passed the position-list size field).
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
**     it is set, iterate through docids in descending order instead of the
**     default ascending order.
**
** iRowidOffset/nRowidOffset/aRowidOffset:
**     These are used if the FTS5_SEGITER_REVERSE flag is set.
**
**     For each rowid on the page corresponding to the current term, the
**     corresponding aRowidOffset[] entry is set to the byte offset of the
**     start of the "position-list-size" field within the page.
*/
struct Fts5SegIter {
  Fts5StructureSegment *pSeg;     /* Segment to iterate through */
  int flags;                      /* Mask of configuration flags */
  int iLeafPgno;                  /* Current leaf page number */
  Fts5Data *pLeaf;                /* Current leaf data */
  Fts5Data *pNextLeaf;            /* Leaf page (iLeafPgno+1) */
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
  int nPos;                       /* Number of bytes in current position list */
  int bDel;                       /* True if the delete flag is set */
};

#define FTS5_SEGITER_ONETERM 0x01
#define FTS5_SEGITER_REVERSE 0x02


/*
** poslist:
**   Used by sqlite3Fts5IterPoslist() when the poslist needs to be buffered.
**   There is no way to tell if this is populated or not.
*/
struct Fts5IndexIter {
  Fts5Index *pIndex;              /* Index that owns this iterator */
  Fts5Structure *pStruct;         /* Database structure for this iterator */
  Fts5Buffer poslist;             /* Buffer containing current poslist */

  int nSeg;                       /* Size of aSeg[] array */
  int bRev;                       /* True to iterate in reverse order */
  int bSkipEmpty;                 /* True to skip deleted entries */
  int bEof;                       /* True at EOF */

  i64 iSwitchRowid;               /* Firstest rowid of other than aFirst[1] */
  Fts5CResult *aFirst;            /* Current merge state (see above) */
  Fts5SegIter aSeg[1];            /* Array of segment iterators */
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
struct Fts5DlidxLvl {
  Fts5Data *pData;              /* Data for current page of this level */
  int iOff;                     /* Current offset into pData */
  int bEof;                     /* At EOF already */
  int iFirstOff;                /* Used by reverse iterators */

  /* Output variables */
  int iLeafPgno;                /* Page number of current leaf page */
  i64 iRowid;                   /* First rowid on leaf iLeafPgno */
};
struct Fts5DlidxIter {
  int nLvl;
  int iSegid;
  Fts5DlidxLvl aLvl[1];
};



/*
** The first argument passed to this macro is a pointer to an Fts5Buffer
** object.
*/
#define fts5BufferSize(pBuf,n) {                \
  if( pBuf->nSpace<n ) {                        \
    u8 *pNew = sqlite3_realloc(pBuf->p, n);     \
    if( pNew==0 ){                              \
      sqlite3_free(pBuf->p);                    \
    }                                           \
    pBuf->nSpace = n;                           \
    pBuf->p = pNew;                             \
  }                                             \
}

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
  return sqlite3Fts5MallocZero(&p->rc, nByte);
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

#ifdef SQLITE_DEBUG
static int fts5BlobCompare(
  const u8 *pLeft, int nLeft, 
  const u8 *pRight, int nRight
){
  int nCmp = MIN(nLeft, nRight);
  int res = memcmp(pLeft, pRight, nCmp);
  return (res==0 ? (nLeft - nRight) : res);
}
#endif


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

    if( p->pReader ){
      /* This call may return SQLITE_ABORT if there has been a savepoint
      ** rollback since it was last used. In this case a new blob handle
      ** is required.  */
      sqlite3_blob *pBlob = p->pReader;
      p->pReader = 0;
      rc = sqlite3_blob_reopen(pBlob, iRowid);
      assert( p->pReader==0 );
      p->pReader = pBlob;
      if( rc!=SQLITE_OK ){
        fts5CloseReader(p);
      }
      if( rc==SQLITE_ABORT ) rc = SQLITE_OK;
    }

    /* If the blob handle is not yet open, open and seek it. Otherwise, use
    ** the blob_reopen() API to reseek the existing blob handle.  */
    if( p->pReader==0 && rc==SQLITE_OK ){
      Fts5Config *pConfig = p->pConfig;
      rc = sqlite3_blob_open(pConfig->db, 
          pConfig->zDb, p->zDataTbl, "block", iRowid, 0, &p->pReader
      );
    }

    /* If either of the sqlite3_blob_open() or sqlite3_blob_reopen() calls
    ** above returned SQLITE_ERROR, return SQLITE_CORRUPT_VTAB instead.
    ** All the reasons those functions might return SQLITE_ERROR - missing
    ** table, missing row, non-blob/text in block column - indicate 
    ** backing store corruption.  */
    if( rc==SQLITE_ERROR ) rc = FTS5_CORRUPT;

    if( rc==SQLITE_OK ){
      u8 *aOut = 0;               /* Read blob data into this buffer */
      int nByte = sqlite3_blob_bytes(p->pReader);
      if( pBuf ){
        fts5BufferSize(pBuf, MAX(nByte, p->pConfig->pgsz) + 20);
        pBuf->n = nByte;
        aOut = pBuf->p;
        if( aOut==0 ){
          rc = SQLITE_NOMEM;
        }
      }else{
        int nSpace = nByte + FTS5_DATA_PADDING;
        pRet = (Fts5Data*)sqlite3_malloc(nSpace+sizeof(Fts5Data));
        if( pRet ){
          pRet->n = nByte;
          aOut = pRet->p = (u8*)&pRet[1];
        }else{
          rc = SQLITE_NOMEM;
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
  sqlite3_free(pData);
}

static int fts5IndexPrepareStmt(
  Fts5Index *p,
  sqlite3_stmt **ppStmt,
  char *zSql
){
  if( p->rc==SQLITE_OK ){
    if( zSql ){
      p->rc = sqlite3_prepare_v2(p->pConfig->db, zSql, -1, ppStmt, 0);
    }else{
      p->rc = SQLITE_NOMEM;
    }
  }
  sqlite3_free(zSql);
  return p->rc;
}


/*
** INSERT OR REPLACE a record into the %_data table.
*/
static void fts5DataWrite(Fts5Index *p, i64 iRowid, const u8 *pData, int nData){
  if( p->rc!=SQLITE_OK ) return;

  if( p->pWriter==0 ){
    int rc = SQLITE_OK;
    Fts5Config *pConfig = p->pConfig;
    fts5IndexPrepareStmt(p, &p->pWriter, sqlite3_mprintf(
          "REPLACE INTO '%q'.'%q_data'(id, block) VALUES(?,?)", 
          pConfig->zDb, pConfig->zName
    ));
    if( p->rc ) return;
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
        "DELETE FROM '%q'.'%q_data' WHERE id>=? AND id<=?", 
          pConfig->zDb, pConfig->zName
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
** Remove all records associated with segment iSegid.
*/
static void fts5DataRemoveSegment(Fts5Index *p, int iSegid){
  i64 iFirst = FTS5_SEGMENT_ROWID(iSegid, 0, 0);
  i64 iLast = FTS5_SEGMENT_ROWID(iSegid+1, 0, 0)-1;
  fts5DataDelete(p, iFirst, iLast);
  if( p->pIdxDeleter==0 ){
    Fts5Config *pConfig = p->pConfig;
    fts5IndexPrepareStmt(p, &p->pIdxDeleter, sqlite3_mprintf(
          "DELETE FROM '%q'.'%q_idx' WHERE segid=?",
          pConfig->zDb, pConfig->zName
    ));
  }
  if( p->rc==SQLITE_OK ){
    sqlite3_bind_int(p->pIdxDeleter, 1, iSegid);
    sqlite3_step(p->pIdxDeleter);
    p->rc = sqlite3_reset(p->pIdxDeleter);
  }
}

/*
** Release a reference to an Fts5Structure object returned by an earlier 
** call to fts5StructureRead() or fts5StructureDecode().
*/
static void fts5StructureRelease(Fts5Structure *pStruct){
  if( pStruct && 0>=(--pStruct->nRef) ){
    int i;
    assert( pStruct->nRef==0 );
    for(i=0; i<pStruct->nLevel; i++){
      sqlite3_free(pStruct->aLevel[i].aSeg);
    }
    sqlite3_free(pStruct);
  }
}

static void fts5StructureRef(Fts5Structure *pStruct){
  pStruct->nRef++;
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
  i += fts5GetVarint32(&pData[i], nLevel);
  i += fts5GetVarint32(&pData[i], nSegment);
  nByte = (
      sizeof(Fts5Structure) +                    /* Main structure */
      sizeof(Fts5StructureLevel) * (nLevel-1)    /* aLevel[] array */
  );
  pRet = (Fts5Structure*)sqlite3Fts5MallocZero(&rc, nByte);

  if( pRet ){
    pRet->nRef = 1;
    pRet->nLevel = nLevel;
    pRet->nSegment = nSegment;
    i += sqlite3Fts5GetVarint(&pData[i], &pRet->nWriteCounter);

    for(iLvl=0; rc==SQLITE_OK && iLvl<nLevel; iLvl++){
      Fts5StructureLevel *pLvl = &pRet->aLevel[iLvl];
      int nTotal;
      int iSeg;

      i += fts5GetVarint32(&pData[i], pLvl->nMerge);
      i += fts5GetVarint32(&pData[i], nTotal);
      assert( nTotal>=pLvl->nMerge );
      pLvl->aSeg = (Fts5StructureSegment*)sqlite3Fts5MallocZero(&rc, 
          nTotal * sizeof(Fts5StructureSegment)
      );

      if( rc==SQLITE_OK ){
        pLvl->nSeg = nTotal;
        for(iSeg=0; iSeg<nTotal; iSeg++){
          i += fts5GetVarint32(&pData[i], pLvl->aSeg[iSeg].iSegid);
          i += fts5GetVarint32(&pData[i], pLvl->aSeg[iSeg].nHeight);
          i += fts5GetVarint32(&pData[i], pLvl->aSeg[iSeg].pgnoFirst);
          i += fts5GetVarint32(&pData[i], pLvl->aSeg[iSeg].pgnoLast);
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
** Read, deserialize and return the structure record.
**
** The Fts5Structure.aLevel[] and each Fts5StructureLevel.aSeg[] array
** are over-allocated as described for function fts5StructureDecode() 
** above.
**
** If an error occurs, NULL is returned and an error code left in the
** Fts5Index handle. If an error has already occurred when this function
** is called, it is a no-op.
*/
static Fts5Structure *fts5StructureRead(Fts5Index *p){
  Fts5Config *pConfig = p->pConfig;
  Fts5Structure *pRet = 0;        /* Object to return */
  int iCookie;                    /* Configuration cookie */
  Fts5Buffer buf = {0, 0, 0};

  fts5DataBuffer(p, &buf, FTS5_STRUCTURE_ROWID);
  if( buf.p==0 ) return 0;
  assert( buf.nSpace>=(buf.n + FTS5_DATA_ZERO_PADDING) );
  memset(&buf.p[buf.n], 0, FTS5_DATA_ZERO_PADDING);
  p->rc = fts5StructureDecode(buf.p, buf.n, &iCookie, &pRet);

  if( p->rc==SQLITE_OK && pConfig->iCookie!=iCookie ){
    p->rc = sqlite3Fts5ConfigLoad(pConfig, iCookie);
  }

  fts5BufferFree(&buf);
  if( p->rc!=SQLITE_OK ){
    fts5StructureRelease(pRet);
    pRet = 0;
  }
  return pRet;
}

/*
** Return the total number of segments in index structure pStruct. This
** function is only ever used as part of assert() conditions.
*/
#ifdef SQLITE_DEBUG
static int fts5StructureCountSegments(Fts5Structure *pStruct){
  int nSegment = 0;               /* Total number of segments */
  if( pStruct ){
    int iLvl;                     /* Used to iterate through levels */
    for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
      nSegment += pStruct->aLevel[iLvl].nSeg;
    }
  }

  return nSegment;
}
#endif

/*
** Serialize and store the "structure" record.
**
** If an error occurs, leave an error code in the Fts5Index object. If an
** error has already occurred, this function is a no-op.
*/
static void fts5StructureWrite(Fts5Index *p, Fts5Structure *pStruct){
  if( p->rc==SQLITE_OK ){
    Fts5Buffer buf;               /* Buffer to serialize record into */
    int iLvl;                     /* Used to iterate through levels */
    int iCookie;                  /* Cookie value to store */

    assert( pStruct->nSegment==fts5StructureCountSegments(pStruct) );
    memset(&buf, 0, sizeof(Fts5Buffer));

    /* Append the current configuration cookie */
    iCookie = p->pConfig->iCookie;
    if( iCookie<0 ) iCookie = 0;
    fts5BufferAppend32(&p->rc, &buf, iCookie);

    fts5BufferAppendVarint(&p->rc, &buf, pStruct->nLevel);
    fts5BufferAppendVarint(&p->rc, &buf, pStruct->nSegment);
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

    fts5DataWrite(p, FTS5_STRUCTURE_ROWID, buf.p, buf.n);
    fts5BufferFree(&buf);
  }
}

#if 0
static void fts5DebugStructure(int*,Fts5Buffer*,Fts5Structure*);
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
** Return a copy of index structure pStruct. Except, promote as many 
** segments as possible to level iPromote. If an OOM occurs, NULL is 
** returned.
*/
static void fts5StructurePromoteTo(
  Fts5Index *p,
  int iPromote,
  int szPromote,
  Fts5Structure *pStruct
){
  int il, is;
  Fts5StructureLevel *pOut = &pStruct->aLevel[iPromote];

  if( pOut->nMerge==0 ){
    for(il=iPromote+1; il<pStruct->nLevel; il++){
      Fts5StructureLevel *pLvl = &pStruct->aLevel[il];
      if( pLvl->nMerge ) return;
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
    int szPromote = 0;            /* Promote anything this size or smaller */
    Fts5StructureSegment *pSeg;   /* Segment just written */
    int szSeg;                    /* Size of segment just written */


    pSeg = &pStruct->aLevel[iLvl].aSeg[pStruct->aLevel[iLvl].nSeg-1];
    szSeg = (1 + pSeg->pgnoLast - pSeg->pgnoFirst);

    /* Check for condition (a) */
    for(iTst=iLvl-1; iTst>=0 && pStruct->aLevel[iTst].nSeg==0; iTst--);
    if( iTst>=0 ){
      int i;
      int szMax = 0;
      Fts5StructureLevel *pTst = &pStruct->aLevel[iTst];
      assert( pTst->nMerge==0 );
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

    /* If condition (a) is not met, assume (b) is true. StructurePromoteTo()
    ** is a no-op if it is not.  */
    if( iPromote<0 ){
      iPromote = iLvl;
      szPromote = szSeg;
    }
    fts5StructurePromoteTo(p, iPromote, szPromote, pStruct);
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
    pIter->iOff += fts5GetVarint32(&pIter->aData[pIter->iOff], pIter->nEmpty);
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
    pIter->iOff += fts5GetVarint32(&pIter->aData[pIter->iOff], nPre);
    pIter->iOff += fts5GetVarint32(&pIter->aData[pIter->iOff], nNew);
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
  pIter->iOff = fts5GetVarint32(aData, pIter->iChild);
  fts5NodeIterGobbleNEmpty(pIter);
}

/*
** Free any memory allocated by the iterator object.
*/
static void fts5NodeIterFree(Fts5NodeIter *pIter){
  fts5BufferFree(&pIter->term);
}

/*
** Advance the iterator passed as the only argument. If the end of the 
** doclist-index page is reached, return non-zero.
*/
static int fts5DlidxLvlNext(Fts5DlidxLvl *pLvl){
  Fts5Data *pData = pLvl->pData;

  if( pLvl->iOff==0 ){
    assert( pLvl->bEof==0 );
    pLvl->iOff = 1;
    pLvl->iOff += fts5GetVarint32(&pData->p[1], pLvl->iLeafPgno);
    pLvl->iOff += fts5GetVarint(&pData->p[pLvl->iOff], (u64*)&pLvl->iRowid);
    pLvl->iFirstOff = pLvl->iOff;
  }else{
    int iOff;
    for(iOff=pLvl->iOff; iOff<pData->n; iOff++){
      if( pData->p[iOff] ) break; 
    }

    if( iOff<pData->n ){
      i64 iVal;
      pLvl->iLeafPgno += (iOff - pLvl->iOff) + 1;
      iOff += fts5GetVarint(&pData->p[iOff], (u64*)&iVal);
      pLvl->iRowid += iVal;
      pLvl->iOff = iOff;
    }else{
      pLvl->bEof = 1;
    }
  }

  return pLvl->bEof;
}

/*
** Advance the iterator passed as the only argument.
*/
static int fts5DlidxIterNextR(Fts5Index *p, Fts5DlidxIter *pIter, int iLvl){
  Fts5DlidxLvl *pLvl = &pIter->aLvl[iLvl];

  assert( iLvl<pIter->nLvl );
  if( fts5DlidxLvlNext(pLvl) ){
    if( (iLvl+1) < pIter->nLvl ){
      fts5DlidxIterNextR(p, pIter, iLvl+1);
      if( pLvl[1].bEof==0 ){
        fts5DataRelease(pLvl->pData);
        memset(pLvl, 0, sizeof(Fts5DlidxLvl));
        pLvl->pData = fts5DataRead(p, 
            FTS5_DLIDX_ROWID(pIter->iSegid, iLvl, pLvl[1].iLeafPgno)
        );
        if( pLvl->pData ) fts5DlidxLvlNext(pLvl);
      }
    }
  }

  return pIter->aLvl[0].bEof;
}
static int fts5DlidxIterNext(Fts5Index *p, Fts5DlidxIter *pIter){
  return fts5DlidxIterNextR(p, pIter, 0);
}

/*
** The iterator passed as the first argument has the following fields set
** as follows. This function sets up the rest of the iterator so that it
** points to the first rowid in the doclist-index.
**
**   pData:
**     pointer to doclist-index record, 
**
** When this function is called pIter->iLeafPgno is the page number the
** doclist is associated with (the one featuring the term).
*/
static int fts5DlidxIterFirst(Fts5DlidxIter *pIter){
  int i;
  for(i=0; i<pIter->nLvl; i++){
    fts5DlidxLvlNext(&pIter->aLvl[i]);
  }
  return pIter->aLvl[0].bEof;
}


static int fts5DlidxIterEof(Fts5Index *p, Fts5DlidxIter *pIter){
  return p->rc!=SQLITE_OK || pIter->aLvl[0].bEof;
}

static void fts5DlidxIterLast(Fts5Index *p, Fts5DlidxIter *pIter){
  int i;

  /* Advance each level to the last entry on the last page */
  for(i=pIter->nLvl-1; p->rc==SQLITE_OK && i>=0; i--){
    Fts5DlidxLvl *pLvl = &pIter->aLvl[i];
    while( fts5DlidxLvlNext(pLvl)==0 );
    pLvl->bEof = 0;

    if( i>0 ){
      Fts5DlidxLvl *pChild = &pLvl[-1];
      fts5DataRelease(pChild->pData);
      memset(pChild, 0, sizeof(Fts5DlidxLvl));
      pChild->pData = fts5DataRead(p, 
          FTS5_DLIDX_ROWID(pIter->iSegid, i-1, pLvl->iLeafPgno)
      );
    }
  }
}

/*
** Move the iterator passed as the only argument to the previous entry.
*/
static int fts5DlidxLvlPrev(Fts5DlidxLvl *pLvl){
  int iOff = pLvl->iOff;

  assert( pLvl->bEof==0 );
  if( iOff<=pLvl->iFirstOff ){
    pLvl->bEof = 1;
  }else{
    u8 *a = pLvl->pData->p;
    i64 iVal;
    int iLimit;
    int ii;
    int nZero = 0;

    /* Currently iOff points to the first byte of a varint. This block 
    ** decrements iOff until it points to the first byte of the previous 
    ** varint. Taking care not to read any memory locations that occur
    ** before the buffer in memory.  */
    iLimit = (iOff>9 ? iOff-9 : 0);
    for(iOff--; iOff>iLimit; iOff--){
      if( (a[iOff-1] & 0x80)==0 ) break;
    }

    fts5GetVarint(&a[iOff], (u64*)&iVal);
    pLvl->iRowid -= iVal;
    pLvl->iLeafPgno--;

    /* Skip backwards past any 0x00 varints. */
    for(ii=iOff-1; ii>=pLvl->iFirstOff && a[ii]==0x00; ii--){
      nZero++;
    }
    if( ii>=pLvl->iFirstOff && (a[ii] & 0x80) ){
      /* The byte immediately before the last 0x00 byte has the 0x80 bit
      ** set. So the last 0x00 is only a varint 0 if there are 8 more 0x80
      ** bytes before a[ii]. */
      int bZero = 0;              /* True if last 0x00 counts */
      if( (ii-8)>=pLvl->iFirstOff ){
        int j;
        for(j=1; j<=8 && (a[ii-j] & 0x80); j++);
        bZero = (j>8);
      }
      if( bZero==0 ) nZero--;
    }
    pLvl->iLeafPgno -= nZero;
    pLvl->iOff = iOff - nZero;
  }

  return pLvl->bEof;
}

static int fts5DlidxIterPrevR(Fts5Index *p, Fts5DlidxIter *pIter, int iLvl){
  Fts5DlidxLvl *pLvl = &pIter->aLvl[iLvl];

  assert( iLvl<pIter->nLvl );
  if( fts5DlidxLvlPrev(pLvl) ){
    if( (iLvl+1) < pIter->nLvl ){
      fts5DlidxIterPrevR(p, pIter, iLvl+1);
      if( pLvl[1].bEof==0 ){
        fts5DataRelease(pLvl->pData);
        memset(pLvl, 0, sizeof(Fts5DlidxLvl));
        pLvl->pData = fts5DataRead(p, 
            FTS5_DLIDX_ROWID(pIter->iSegid, iLvl, pLvl[1].iLeafPgno)
        );
        if( pLvl->pData ){
          while( fts5DlidxLvlNext(pLvl)==0 );
          pLvl->bEof = 0;
        }
      }
    }
  }

  return pIter->aLvl[0].bEof;
}
static int fts5DlidxIterPrev(Fts5Index *p, Fts5DlidxIter *pIter){
  return fts5DlidxIterPrevR(p, pIter, 0);
}

/*
** Free a doclist-index iterator object allocated by fts5DlidxIterInit().
*/
static void fts5DlidxIterFree(Fts5DlidxIter *pIter){
  if( pIter ){
    int i;
    for(i=0; i<pIter->nLvl; i++){
      fts5DataRelease(pIter->aLvl[i].pData);
    }
    sqlite3_free(pIter);
  }
}

static Fts5DlidxIter *fts5DlidxIterInit(
  Fts5Index *p,                   /* Fts5 Backend to iterate within */
  int bRev,                       /* True for ORDER BY ASC */
  int iSegid,                     /* Segment id */
  int iLeafPg                     /* Leaf page number to load dlidx for */
){
  Fts5DlidxIter *pIter = 0;
  int i;
  int bDone = 0;

  for(i=0; p->rc==SQLITE_OK && bDone==0; i++){
    int nByte = sizeof(Fts5DlidxIter) + i * sizeof(Fts5DlidxLvl);
    Fts5DlidxIter *pNew;

    pNew = (Fts5DlidxIter*)sqlite3_realloc(pIter, nByte);
    if( pNew==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      i64 iRowid = FTS5_DLIDX_ROWID(iSegid, i, iLeafPg);
      Fts5DlidxLvl *pLvl = &pNew->aLvl[i];
      pIter = pNew;
      memset(pLvl, 0, sizeof(Fts5DlidxLvl));
      pLvl->pData = fts5DataRead(p, iRowid);
      if( pLvl->pData && (pLvl->pData->p[0] & 0x0001)==0 ){
        bDone = 1;
      }
      pIter->nLvl = i+1;
    }
  }

  if( p->rc==SQLITE_OK ){
    pIter->iSegid = iSegid;
    if( bRev==0 ){
      fts5DlidxIterFirst(pIter);
    }else{
      fts5DlidxIterLast(p, pIter);
    }
  }

  if( p->rc!=SQLITE_OK ){
    fts5DlidxIterFree(pIter);
    pIter = 0;
  }

  return pIter;
}

static i64 fts5DlidxIterRowid(Fts5DlidxIter *pIter){
  return pIter->aLvl[0].iRowid;
}
static int fts5DlidxIterPgno(Fts5DlidxIter *pIter){
  return pIter->aLvl[0].iLeafPgno;
}

static void fts5LeafHeader(Fts5Data *pLeaf, int *piRowid, int *piTerm){
  *piRowid = (int)fts5GetU16(&pLeaf->p[0]);
  *piTerm = (int)fts5GetU16(&pLeaf->p[2]);
}

/*
** Load the next leaf page into the segment iterator.
*/
static void fts5SegIterNextPage(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter              /* Iterator to advance to next page */
){
  Fts5StructureSegment *pSeg = pIter->pSeg;
  fts5DataRelease(pIter->pLeaf);
  pIter->iLeafPgno++;
  if( pIter->pNextLeaf ){
    assert( pIter->iLeafPgno<=pSeg->pgnoLast );
    pIter->pLeaf = pIter->pNextLeaf;
    pIter->pNextLeaf = 0;
  }else if( pIter->iLeafPgno<=pSeg->pgnoLast ){
    pIter->pLeaf = fts5DataRead(p, 
        FTS5_SEGMENT_ROWID(pSeg->iSegid, 0, pIter->iLeafPgno)
    );
  }else{
    pIter->pLeaf = 0;
  }
}

/*
** Argument p points to a buffer containing a varint to be interpreted as a
** position list size field. Read the varint and return the number of bytes
** read. Before returning, set *pnSz to the number of bytes in the position
** list, and *pbDel to true if the delete flag is set, or false otherwise.
*/
static int fts5GetPoslistSize(const u8 *p, int *pnSz, int *pbDel){
  int nSz;
  int n = fts5GetVarint32(p, nSz);
  assert_nc( nSz>=0 );
  *pnSz = nSz/2;
  *pbDel = nSz & 0x0001;
  return n;
}

/*
** Fts5SegIter.iLeafOffset currently points to the first byte of a
** position-list size field. Read the value of the field and store it
** in the following variables:
**
**   Fts5SegIter.nPos
**   Fts5SegIter.bDel
**
** Leave Fts5SegIter.iLeafOffset pointing to the first byte of the 
** position list content (if any).
*/
static void fts5SegIterLoadNPos(Fts5Index *p, Fts5SegIter *pIter){
  if( p->rc==SQLITE_OK ){
    int iOff = pIter->iLeafOffset;  /* Offset to read at */
    if( iOff>=pIter->pLeaf->n ){
      p->rc = FTS5_CORRUPT;
    }else{
      const u8 *a = &pIter->pLeaf->p[iOff];
      pIter->iLeafOffset += fts5GetPoslistSize(a, &pIter->nPos, &pIter->bDel);
    }
  }
}

static void fts5SegIterLoadRowid(Fts5Index *p, Fts5SegIter *pIter){
  u8 *a = pIter->pLeaf->p;        /* Buffer to read data from */
  int iOff = pIter->iLeafOffset;

  if( iOff>=pIter->pLeaf->n ){
    fts5SegIterNextPage(p, pIter);
    if( pIter->pLeaf==0 ){
      if( p->rc==SQLITE_OK ) p->rc = FTS5_CORRUPT;
      return;
    }
    iOff = 4;
    a = pIter->pLeaf->p;
  }
  iOff += sqlite3Fts5GetVarint(&a[iOff], (u64*)&pIter->iRowid);
  pIter->iLeafOffset = iOff;
}

/*
** Fts5SegIter.iLeafOffset currently points to the first byte of the 
** "nSuffix" field of a term. Function parameter nKeep contains the value
** of the "nPrefix" field (if there was one - it is passed 0 if this is
** the first term in the segment).
**
** This function populates:
**
**   Fts5SegIter.term
**   Fts5SegIter.rowid
**
** accordingly and leaves (Fts5SegIter.iLeafOffset) set to the content of
** the first position list. The position list belonging to document 
** (Fts5SegIter.iRowid).
*/
static void fts5SegIterLoadTerm(Fts5Index *p, Fts5SegIter *pIter, int nKeep){
  u8 *a = pIter->pLeaf->p;        /* Buffer to read data from */
  int iOff = pIter->iLeafOffset;  /* Offset to read at */
  int nNew;                       /* Bytes of new data */

  iOff += fts5GetVarint32(&a[iOff], nNew);
  pIter->term.n = nKeep;
  fts5BufferAppendBlob(&p->rc, &pIter->term, nNew, &a[iOff]);
  iOff += nNew;
  pIter->iTermLeafOffset = iOff;
  pIter->iTermLeafPgno = pIter->iLeafPgno;
  pIter->iLeafOffset = iOff;

  fts5SegIterLoadRowid(p, pIter);
}

/*
** Initialize the iterator object pIter to iterate through the entries in
** segment pSeg. The iterator is left pointing to the first entry when 
** this function returns.
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. If 
** an error has already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterInit(
  Fts5Index *p,                   /* FTS index object */
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
    pIter->iLeafPgno = pSeg->pgnoFirst-1;
    fts5SegIterNextPage(p, pIter);
  }

  if( p->rc==SQLITE_OK ){
    u8 *a = pIter->pLeaf->p;
    pIter->iLeafOffset = fts5GetU16(&a[2]);
    fts5SegIterLoadTerm(p, pIter, 0);
    fts5SegIterLoadNPos(p, pIter);
  }
}

/*
** This function is only ever called on iterators created by calls to
** Fts5IndexQuery() with the FTS5INDEX_QUERY_DESC flag set.
**
** The iterator is in an unusual state when this function is called: the
** Fts5SegIter.iLeafOffset variable is set to the offset of the start of
** the position-list size field for the first relevant rowid on the page.
** Fts5SegIter.rowid is set, but nPos and bDel are not.
**
** This function advances the iterator so that it points to the last 
** relevant rowid on the page and, if necessary, initializes the 
** aRowidOffset[] and iRowidOffset variables. At this point the iterator
** is in its regular state - Fts5SegIter.iLeafOffset points to the first
** byte of the position list content associated with said rowid.
*/
static void fts5SegIterReverseInitPage(Fts5Index *p, Fts5SegIter *pIter){
  int n = pIter->pLeaf->n;
  int i = pIter->iLeafOffset;
  u8 *a = pIter->pLeaf->p;
  int iRowidOffset = 0;

  while( 1 ){
    i64 iDelta = 0;
    int nPos;
    int bDummy;

    i += fts5GetPoslistSize(&a[i], &nPos, &bDummy);
    i += nPos;
    if( i>=n ) break;
    i += fts5GetVarint(&a[i], (u64*)&iDelta);
    if( iDelta==0 ) break;
    pIter->iRowid += iDelta;

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
  fts5SegIterLoadNPos(p, pIter);
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
          pIter->pSeg->iSegid, 0, pIter->iLeafPgno
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
        pIter->iLeafOffset += fts5GetVarint(a, (u64*)&pIter->iRowid);
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
static int fts5MultiIterIsEmpty(Fts5Index *p, Fts5IndexIter *pIter){
  Fts5SegIter *pSeg = &pIter->aSeg[pIter->aFirst[1].iFirst];
  return (p->rc==SQLITE_OK && pSeg->pLeaf && pSeg->nPos==0);
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
  Fts5SegIter *pIter,             /* Iterator to advance */
  int *pbNewTerm                  /* OUT: Set for new term */
){
  assert( pbNewTerm==0 || *pbNewTerm==0 );
  if( p->rc==SQLITE_OK ){
    if( pIter->flags & FTS5_SEGITER_REVERSE ){
      assert( pIter->pNextLeaf==0 );
      if( pIter->iRowidOffset>0 ){
        u8 *a = pIter->pLeaf->p;
        int iOff;
        int nPos;
        int bDummy;
        i64 iDelta;

        pIter->iRowidOffset--;
        pIter->iLeafOffset = iOff = pIter->aRowidOffset[pIter->iRowidOffset];
        iOff += fts5GetPoslistSize(&a[iOff], &nPos, &bDummy);
        iOff += nPos;
        fts5GetVarint(&a[iOff], (u64*)&iDelta);
        pIter->iRowid -= iDelta;
        fts5SegIterLoadNPos(p, pIter);
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

      iOff = pIter->iLeafOffset + pIter->nPos;

      if( iOff<n ){
        /* The next entry is on the current page */
        u64 iDelta;
        iOff += sqlite3Fts5GetVarint(&a[iOff], &iDelta);
        pIter->iLeafOffset = iOff;
        if( iDelta==0 ){
          bNewTerm = 1;
          if( iOff>=n ){
            fts5SegIterNextPage(p, pIter);
            pIter->iLeafOffset = 4;
          }else if( iOff!=fts5GetU16(&a[2]) ){
            pIter->iLeafOffset += fts5GetVarint32(&a[iOff], nKeep);
          }
        }else{
          pIter->iRowid += iDelta;
        }
      }else if( pIter->pSeg==0 ){
        const u8 *pList = 0;
        const char *zTerm = 0;
        int nList = 0;
        if( 0==(pIter->flags & FTS5_SEGITER_ONETERM) ){
          sqlite3Fts5HashScanNext(p->pHash);
          sqlite3Fts5HashScanEntry(p->pHash, &zTerm, &pList, &nList);
        }
        if( pList==0 ){
          fts5DataRelease(pIter->pLeaf);
          pIter->pLeaf = 0;
        }else{
          pIter->pLeaf->p = (u8*)pList;
          pIter->pLeaf->n = nList;
          sqlite3Fts5BufferSet(&p->rc, &pIter->term, strlen(zTerm), (u8*)zTerm);
          pIter->iLeafOffset = fts5GetVarint(pList, (u64*)&pIter->iRowid);
        }
      }else{
        iOff = 0;
        /* Next entry is not on the current page */
        while( iOff==0 ){
          fts5SegIterNextPage(p, pIter);
          pLeaf = pIter->pLeaf;
          if( pLeaf==0 ) break;
          if( (iOff = fts5GetU16(&pLeaf->p[0])) && iOff<pLeaf->n ){
            iOff += sqlite3Fts5GetVarint(&pLeaf->p[iOff], (u64*)&pIter->iRowid);
            pIter->iLeafOffset = iOff;
          }
          else if( (iOff = fts5GetU16(&pLeaf->p[2])) ){
            pIter->iLeafOffset = iOff;
            bNewTerm = 1;
          }
          if( iOff>=pLeaf->n ){
            p->rc = FTS5_CORRUPT;
            return;
          }
        }
      }

      /* Check if the iterator is now at EOF. If so, return early. */
      if( pIter->pLeaf ){
        if( bNewTerm ){
          if( pIter->flags & FTS5_SEGITER_ONETERM ){
            fts5DataRelease(pIter->pLeaf);
            pIter->pLeaf = 0;
          }else{
            fts5SegIterLoadTerm(p, pIter, nKeep);
            fts5SegIterLoadNPos(p, pIter);
            if( pbNewTerm ) *pbNewTerm = 1;
          }
        }else{
          fts5SegIterLoadNPos(p, pIter);
        }
      }
    }
  }
}

#define SWAPVAL(T, a, b) { T tmp; tmp=a; a=b; b=tmp; }

/*
** Iterator pIter currently points to the first rowid in a doclist. This
** function sets the iterator up so that iterates in reverse order through
** the doclist.
*/
static void fts5SegIterReverse(Fts5Index *p, Fts5SegIter *pIter){
  Fts5DlidxIter *pDlidx = pIter->pDlidx;
  Fts5Data *pLast = 0;
  int pgnoLast = 0;

  if( pDlidx ){
    int iSegid = pIter->pSeg->iSegid;
    pgnoLast = fts5DlidxIterPgno(pDlidx);
    pLast = fts5DataRead(p, FTS5_SEGMENT_ROWID(iSegid, 0, pgnoLast));
  }else{
    int iOff;                               /* Byte offset within pLeaf */
    Fts5Data *pLeaf = pIter->pLeaf;         /* Current leaf data */

    /* Currently, Fts5SegIter.iLeafOffset (and iOff) points to the first 
    ** byte of position-list content for the current rowid. Back it up
    ** so that it points to the start of the position-list size field. */
    pIter->iLeafOffset -= sqlite3Fts5GetVarintLen(pIter->nPos*2+pIter->bDel);
    iOff = pIter->iLeafOffset;
    assert( iOff>=4 );

    /* Search for a new term within the current leaf. If one can be found,
    ** then this page contains the largest rowid for the current term. */
    while( iOff<pLeaf->n ){
      int nPos;
      i64 iDelta;
      int bDummy;

      /* Read the position-list size field */
      iOff += fts5GetPoslistSize(&pLeaf->p[iOff], &nPos, &bDummy);
      iOff += nPos;
      if( iOff>=pLeaf->n ) break;

      /* Rowid delta. Or, if 0x00, the end of doclist marker. */
      nPos = fts5GetVarint(&pLeaf->p[iOff], (u64*)&iDelta);
      if( iDelta==0 ) break;
      iOff += nPos;
    }

    /* If this condition is true then the largest rowid for the current
    ** term may not be stored on the current page. So search forward to
    ** see where said rowid really is.  */
    if( iOff>=pLeaf->n ){
      int pgno;
      Fts5StructureSegment *pSeg = pIter->pSeg;

      /* The last rowid in the doclist may not be on the current page. Search
      ** forward to find the page containing the last rowid.  */
      for(pgno=pIter->iLeafPgno+1; !p->rc && pgno<=pSeg->pgnoLast; pgno++){
        i64 iAbs = FTS5_SEGMENT_ROWID(pSeg->iSegid, 0, pgno);
        Fts5Data *pNew = fts5DataRead(p, iAbs);
        if( pNew ){
          int iRowid, iTerm;
          fts5LeafHeader(pNew, &iRowid, &iTerm);
          if( iRowid ){
            SWAPVAL(Fts5Data*, pNew, pLast);
            pgnoLast = pgno;
          }
          fts5DataRelease(pNew);
          if( iTerm ) break;
        }
      }
    }
  }

  /* If pLast is NULL at this point, then the last rowid for this doclist
  ** lies on the page currently indicated by the iterator. In this case 
  ** pIter->iLeafOffset is already set to point to the position-list size
  ** field associated with the first relevant rowid on the page.
  **
  ** Or, if pLast is non-NULL, then it is the page that contains the last
  ** rowid. In this case configure the iterator so that it points to the
  ** first rowid on this page.
  */
  if( pLast ){
    int dummy;
    int iOff;
    fts5DataRelease(pIter->pLeaf);
    pIter->pLeaf = pLast;
    pIter->iLeafPgno = pgnoLast;
    fts5LeafHeader(pLast, &iOff, &dummy);
    iOff += fts5GetVarint(&pLast->p[iOff], (u64*)&pIter->iRowid);
    pIter->iLeafOffset = iOff;
  }

  fts5SegIterReverseInitPage(p, pIter);
}

/*
** Iterator pIter currently points to the first rowid of a doclist.
** There is a doclist-index associated with the final term on the current 
** page. If the current term is the last term on the page, load the 
** doclist-index from disk and initialize an iterator at (pIter->pDlidx).
*/
static void fts5SegIterLoadDlidx(Fts5Index *p, Fts5SegIter *pIter){
  int iSeg = pIter->pSeg->iSegid;
  int bRev = (pIter->flags & FTS5_SEGITER_REVERSE);
  Fts5Data *pLeaf = pIter->pLeaf; /* Current leaf data */

  assert( pIter->flags & FTS5_SEGITER_ONETERM );
  assert( pIter->pDlidx==0 );

  /* Check if the current doclist ends on this page. If it does, return
  ** early without loading the doclist-index (as it belongs to a different
  ** term. */
  if( pIter->iTermLeafPgno==pIter->iLeafPgno ){
    int iOff = pIter->iLeafOffset + pIter->nPos;
    while( iOff<pLeaf->n ){
      int bDummy;
      int nPos;
      i64 iDelta;

      /* iOff is currently the offset of the start of position list data */
      iOff += fts5GetVarint(&pLeaf->p[iOff], (u64*)&iDelta);
      if( iDelta==0 ) return;
      assert_nc( iOff<pLeaf->n );
      iOff += fts5GetPoslistSize(&pLeaf->p[iOff], &nPos, &bDummy);
      iOff += nPos;
    }
  }

  pIter->pDlidx = fts5DlidxIterInit(p, bRev, iSeg, pIter->iTermLeafPgno);
}

#ifdef SQLITE_DEBUG
static void fts5AssertNodeSeekOk(
  Fts5Buffer *pNode,
  const u8 *pTerm, int nTerm,     /* Term to search for */
  int iExpectPg,
  int bExpectDlidx
){
  int bDlidx;
  int iPg;
  int rc = SQLITE_OK;
  Fts5NodeIter node;

  fts5NodeIterInit(pNode->p, pNode->n, &node);
  assert( node.term.n==0 );
  iPg = node.iChild;
  bDlidx = node.bDlidx;
  for(fts5NodeIterNext(&rc, &node);
      node.aData && fts5BufferCompareBlob(&node.term, pTerm, nTerm)<=0;
      fts5NodeIterNext(&rc, &node)
  ){
    iPg = node.iChild;
    bDlidx = node.bDlidx;
  }
  fts5NodeIterFree(&node);

  assert( rc!=SQLITE_OK || iPg==iExpectPg );
  assert( rc!=SQLITE_OK || bDlidx==bExpectDlidx );
}
#else
#define fts5AssertNodeSeekOk(v,w,x,y,z)
#endif

/*
** Argument pNode is an internal b-tree node. This function searches
** within the node for the largest term that is smaller than or equal
** to (pTerm/nTerm).
**
** It returns the associated page number. Or, if (pTerm/nTerm) is smaller
** than all terms within the node, the leftmost child page number. 
**
** Before returning, (*pbDlidx) is set to true if the last term on the
** returned child page number has a doclist-index. Or left as is otherwise.
*/
static int fts5NodeSeek(
  Fts5Buffer *pNode,              /* Node to search */
  const u8 *pTerm, int nTerm,     /* Term to search for */
  int *pbDlidx                    /* OUT: True if dlidx flag is set */
){
  int iPg;
  u8 *pPtr = pNode->p;
  u8 *pEnd = &pPtr[pNode->n];
  int nMatch = 0;                 /* Number of bytes of pTerm already matched */
  
  assert( *pbDlidx==0 );

  pPtr += fts5GetVarint32(pPtr, iPg);
  while( pPtr<pEnd ){
    int nEmpty = 0;
    int nKeep;
    int nNew;

    /* If there is a "no terms" record at pPtr, read it now. Store the
    ** number of termless pages in nEmpty. If it indicates a doclist-index, 
    ** set (*pbDlidx) to true.*/
    if( *pPtr<2 ){
      *pbDlidx = (*pPtr==0x01);
      pPtr++;
      pPtr += fts5GetVarint32(pPtr, nEmpty);
      if( pPtr>=pEnd ) break;
    }

    /* Read the next "term" pointer. Set nKeep to the number of bytes to
    ** keep from the previous term, and nNew to the number of bytes of
    ** new data that will be appended to it. */
    nKeep = (int)*pPtr++;
    nNew = (int)*pPtr++;
    if( (nKeep | nNew) & 0x0080 ){
      pPtr -= 2;
      pPtr += fts5GetVarint32(pPtr, nKeep);
      pPtr += fts5GetVarint32(pPtr, nNew);
    }
    nKeep -= 2;

    /* Compare (pTerm/nTerm) to the current term on the node (the one described
    ** by nKeep/nNew). If the node term is larger, break out of the while()
    ** loop. 
    **
    ** Otherwise, if (pTerm/nTerm) is larger or the two terms are equal, 
    ** leave variable nMatch set to the size of the largest prefix common to
    ** both terms in bytes.  */
    if( nKeep==nMatch ){
      int nTst = MIN(nNew, nTerm-nMatch);
      int i;
      for(i=0; i<nTst; i++){
        if( pTerm[nKeep+i]!=pPtr[i] ) break;
      }
      nMatch += i;
      assert( nMatch<=nTerm );

      if( i<nNew && (nMatch==nTerm || pPtr[i] > pTerm[nMatch]) ) break;
    }else if( nKeep<nMatch ){
      break;
    }

    iPg += 1 + nEmpty;
    *pbDlidx = 0;
    pPtr += nNew;
  }

  fts5AssertNodeSeekOk(pNode, pTerm, nTerm, iPg, *pbDlidx);
  return iPg;
}

#define fts5IndexGetVarint32(a, iOff, nVal) {     \
  nVal = a[iOff++];                               \
  if( nVal & 0x80 ){                              \
    iOff--;                                       \
    iOff += fts5GetVarint32(&a[iOff], nVal);      \
  }                                               \
}

#define fts5IndexSkipVarint(a, iOff) {            \
  int iEnd = iOff+9;                              \
  while( (a[iOff++] & 0x80) && iOff<iEnd );       \
}

/*
** The iterator object passed as the second argument currently contains
** no valid values except for the Fts5SegIter.pLeaf member variable. This
** function searches the leaf page for a term matching (pTerm/nTerm).
**
** If the specified term is found on the page, then the iterator is left
** pointing to it. If argument bGe is zero and the term is not found,
** the iterator is left pointing at EOF.
**
** If bGe is non-zero and the specified term is not found, then the
** iterator is left pointing to the smallest term in the segment that
** is larger than the specified term, even if this term is not on the
** current page.
*/
static void fts5LeafSeek(
  Fts5Index *p,                   /* Leave any error code here */
  int bGe,                        /* True for a >= search */
  Fts5SegIter *pIter,             /* Iterator to seek */
  const u8 *pTerm, int nTerm      /* Term to search for */
){
  int iOff;
  const u8 *a = pIter->pLeaf->p;
  int n = pIter->pLeaf->n;

  int nMatch = 0;
  int nKeep = 0;
  int nNew = 0;

  assert( p->rc==SQLITE_OK );
  assert( pIter->pLeaf );

  iOff = fts5GetU16(&a[2]);
  if( iOff<4 || iOff>=n ){
    p->rc = FTS5_CORRUPT;
    return;
  }

  while( 1 ){
    int i;
    int nCmp;

    /* Figure out how many new bytes are in this term */
    fts5IndexGetVarint32(a, iOff, nNew);

    if( nKeep<nMatch ){
      goto search_failed;
    }

    assert( nKeep>=nMatch );
    if( nKeep==nMatch ){
      nCmp = MIN(nNew, nTerm-nMatch);
      for(i=0; i<nCmp; i++){
        if( a[iOff+i]!=pTerm[nMatch+i] ) break;
      }
      nMatch += i;

      if( nTerm==nMatch ){
        if( i==nNew ){
          goto search_success;
        }else{
          goto search_failed;
        }
      }else if( i<nNew && a[iOff+i]>pTerm[nMatch] ){
        goto search_failed;
      }
    }
    iOff += nNew;

    /* Skip past the doclist. If the end of the page is reached, bail out. */
    while( 1 ){
      int nPos;

      /* Skip past docid delta */
      fts5IndexSkipVarint(a, iOff);

      /* Skip past position list */
      fts5IndexGetVarint32(a, iOff, nPos);
      iOff += (nPos >> 1);
      if( iOff>=(n-1) ){
        iOff = n;
        goto search_failed;
      }

      /* If this is the end of the doclist, break out of the loop */
      if( a[iOff]==0x00 ){
        iOff++;
        break;
      }
    };

    /* Read the nKeep field of the next term. */
    fts5IndexGetVarint32(a, iOff, nKeep);
  }

 search_failed:
  if( bGe==0 ){
    fts5DataRelease(pIter->pLeaf);
    pIter->pLeaf = 0;
    return;
  }else if( iOff>=n ){
    do {
      fts5SegIterNextPage(p, pIter);
      if( pIter->pLeaf==0 ) return;
      a = pIter->pLeaf->p;
      iOff = fts5GetU16(&a[2]);
      if( iOff ){
        if( iOff<4 || iOff>=n ){
          p->rc = FTS5_CORRUPT;
        }else{
          nKeep = 0;
          iOff += fts5GetVarint32(&a[iOff], nNew);
          break;
        }
      }
    }while( 1 );
  }

 search_success:
  pIter->iLeafOffset = iOff + nNew;
  pIter->iTermLeafOffset = pIter->iLeafOffset;
  pIter->iTermLeafPgno = pIter->iLeafPgno;

  fts5BufferSet(&p->rc, &pIter->term, nKeep, pTerm);
  fts5BufferAppendBlob(&p->rc, &pIter->term, nNew, &a[iOff]);

  fts5SegIterLoadRowid(p, pIter);
  fts5SegIterLoadNPos(p, pIter);
}

/*
** Initialize the object pIter to point to term pTerm/nTerm within segment
** pSeg. If there is no such term in the index, the iterator is set to EOF.
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. If 
** an error has already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterSeekInit(
  Fts5Index *p,                   /* FTS5 backend */
  Fts5Buffer *pBuf,               /* Buffer to use for loading pages */
  const u8 *pTerm, int nTerm,     /* Term to seek to */
  int flags,                      /* Mask of FTS5INDEX_XXX flags */
  Fts5StructureSegment *pSeg,     /* Description of segment */
  Fts5SegIter *pIter              /* Object to populate */
){
  int iPg = 1;
  int h;
  int bGe = (flags & FTS5INDEX_QUERY_SCAN);
  int bDlidx = 0;                 /* True if there is a doclist-index */

  static int nCall = 0;
  nCall++;

  assert( bGe==0 || (flags & FTS5INDEX_QUERY_DESC)==0 );
  assert( pTerm && nTerm );
  memset(pIter, 0, sizeof(*pIter));
  pIter->pSeg = pSeg;

  /* This block sets stack variable iPg to the leaf page number that may
  ** contain term (pTerm/nTerm), if it is present in the segment. */
  if( p->pIdxSelect==0 ){
    Fts5Config *pConfig = p->pConfig;
    fts5IndexPrepareStmt(p, &p->pIdxSelect, sqlite3_mprintf(
          "SELECT pgno FROM '%q'.'%q_idx' WHERE "
          "segid=? AND term<=? ORDER BY term DESC LIMIT 1",
          pConfig->zDb, pConfig->zName
    ));
  }
  if( p->rc ) return;
  sqlite3_bind_int(p->pIdxSelect, 1, pSeg->iSegid);
  sqlite3_bind_blob(p->pIdxSelect, 2, pTerm, nTerm, SQLITE_STATIC);
  if( SQLITE_ROW==sqlite3_step(p->pIdxSelect) ){
    i64 val = sqlite3_column_int(p->pIdxSelect, 0);
    iPg = (val>>1);
    bDlidx = (val & 0x0001);
  }
  p->rc = sqlite3_reset(p->pIdxSelect);

  if( iPg<pSeg->pgnoFirst ){
    iPg = pSeg->pgnoFirst;
    bDlidx = 0;
  }

  pIter->iLeafPgno = iPg - 1;
  fts5SegIterNextPage(p, pIter);

  if( pIter->pLeaf ){
    fts5LeafSeek(p, bGe, pIter, pTerm, nTerm);
  }

  if( p->rc==SQLITE_OK && bGe==0 ){
    pIter->flags |= FTS5_SEGITER_ONETERM;
    if( pIter->pLeaf ){
      if( flags & FTS5INDEX_QUERY_DESC ){
        pIter->flags |= FTS5_SEGITER_REVERSE;
      }
      if( bDlidx ){
        fts5SegIterLoadDlidx(p, pIter);
      }
      if( flags & FTS5INDEX_QUERY_DESC ){
        fts5SegIterReverse(p, pIter);
      }
    }
  }

  /* Either:
  **
  **   1) an error has occurred, or
  **   2) the iterator points to EOF, or
  **   3) the iterator points to an entry with term (pTerm/nTerm), or
  **   4) the FTS5INDEX_QUERY_SCAN flag was set and the iterator points
  **      to an entry with a term greater than or equal to (pTerm/nTerm).
  */
  assert( p->rc!=SQLITE_OK                                          /* 1 */
   || pIter->pLeaf==0                                               /* 2 */
   || fts5BufferCompareBlob(&pIter->term, pTerm, nTerm)==0          /* 3 */
   || (bGe && fts5BufferCompareBlob(&pIter->term, pTerm, nTerm)>0)  /* 4 */
  );
}

/*
** Initialize the object pIter to point to term pTerm/nTerm within the
** in-memory hash table. If there is no such term in the hash-table, the 
** iterator is set to EOF.
**
** If an error occurs, Fts5Index.rc is set to an appropriate error code. If 
** an error has already occurred when this function is called, it is a no-op.
*/
static void fts5SegIterHashInit(
  Fts5Index *p,                   /* FTS5 backend */
  const u8 *pTerm, int nTerm,     /* Term to seek to */
  int flags,                      /* Mask of FTS5INDEX_XXX flags */
  Fts5SegIter *pIter              /* Object to populate */
){
  const u8 *pList = 0;
  int nList = 0;
  const u8 *z = 0;
  int n = 0;

  assert( p->pHash );
  assert( p->rc==SQLITE_OK );

  if( pTerm==0 || (flags & FTS5INDEX_QUERY_SCAN) ){
    p->rc = sqlite3Fts5HashScanInit(p->pHash, (const char*)pTerm, nTerm);
    sqlite3Fts5HashScanEntry(p->pHash, (const char**)&z, &pList, &nList);
    n = (z ? strlen((const char*)z) : 0);
  }else{
    pIter->flags |= FTS5_SEGITER_ONETERM;
    sqlite3Fts5HashQuery(p->pHash, (const char*)pTerm, nTerm, &pList, &nList);
    z = pTerm;
    n = nTerm;
  }

  if( pList ){
    Fts5Data *pLeaf;
    sqlite3Fts5BufferSet(&p->rc, &pIter->term, n, z);
    pLeaf = fts5IdxMalloc(p, sizeof(Fts5Data));
    if( pLeaf==0 ) return;
    pLeaf->p = (u8*)pList;
    pLeaf->n = nList;
    pIter->pLeaf = pLeaf;
    pIter->iLeafOffset = fts5GetVarint(pLeaf->p, (u64*)&pIter->iRowid);

    if( flags & FTS5INDEX_QUERY_DESC ){
      pIter->flags |= FTS5_SEGITER_REVERSE;
      fts5SegIterReverseInitPage(p, pIter);
    }else{
      fts5SegIterLoadNPos(p, pIter);
    }
  }
}

/*
** Zero the iterator passed as the only argument.
*/
static void fts5SegIterClear(Fts5SegIter *pIter){
  fts5BufferFree(&pIter->term);
  fts5DataRelease(pIter->pLeaf);
  fts5DataRelease(pIter->pNextLeaf);
  fts5DlidxIterFree(pIter->pDlidx);
  sqlite3_free(pIter->aRowidOffset);
  memset(pIter, 0, sizeof(Fts5SegIter));
}

#ifdef SQLITE_DEBUG

/*
** This function is used as part of the big assert() procedure implemented by
** fts5AssertMultiIterSetup(). It ensures that the result currently stored
** in *pRes is the correct result of comparing the current positions of the
** two iterators.
*/
static void fts5AssertComparisonResult(
  Fts5IndexIter *pIter, 
  Fts5SegIter *p1,
  Fts5SegIter *p2,
  Fts5CResult *pRes
){
  int i1 = p1 - pIter->aSeg;
  int i2 = p2 - pIter->aSeg;

  if( p1->pLeaf || p2->pLeaf ){
    if( p1->pLeaf==0 ){
      assert( pRes->iFirst==i2 );
    }else if( p2->pLeaf==0 ){
      assert( pRes->iFirst==i1 );
    }else{
      int nMin = MIN(p1->term.n, p2->term.n);
      int res = memcmp(p1->term.p, p2->term.p, nMin);
      if( res==0 ) res = p1->term.n - p2->term.n;

      if( res==0 ){
        assert( pRes->bTermEq==1 );
        assert( p1->iRowid!=p2->iRowid );
        res = ((p1->iRowid > p2->iRowid)==pIter->bRev) ? -1 : 1;
      }else{
        assert( pRes->bTermEq==0 );
      }

      if( res<0 ){
        assert( pRes->iFirst==i1 );
      }else{
        assert( pRes->iFirst==i2 );
      }
    }
  }
}

/*
** This function is a no-op unless SQLITE_DEBUG is defined when this module
** is compiled. In that case, this function is essentially an assert() 
** statement used to verify that the contents of the pIter->aFirst[] array
** are correct.
*/
static void fts5AssertMultiIterSetup(Fts5Index *p, Fts5IndexIter *pIter){
  if( p->rc==SQLITE_OK ){
    Fts5SegIter *pFirst = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
    int i;

    assert( (pFirst->pLeaf==0)==pIter->bEof );

    /* Check that pIter->iSwitchRowid is set correctly. */
    for(i=0; i<pIter->nSeg; i++){
      Fts5SegIter *p1 = &pIter->aSeg[i];
      assert( p1==pFirst 
           || p1->pLeaf==0 
           || fts5BufferCompare(&pFirst->term, &p1->term) 
           || p1->iRowid==pIter->iSwitchRowid
           || (p1->iRowid<pIter->iSwitchRowid)==pIter->bRev
      );
    }

    for(i=0; i<pIter->nSeg; i+=2){
      Fts5SegIter *p1 = &pIter->aSeg[i];
      Fts5SegIter *p2 = &pIter->aSeg[i+1];
      Fts5CResult *pRes = &pIter->aFirst[(pIter->nSeg + i) / 2];
      fts5AssertComparisonResult(pIter, p1, p2, pRes);
    }

    for(i=1; i<(pIter->nSeg / 2); i+=2){
      Fts5SegIter *p1 = &pIter->aSeg[ pIter->aFirst[i*2].iFirst ];
      Fts5SegIter *p2 = &pIter->aSeg[ pIter->aFirst[i*2+1].iFirst ];
      Fts5CResult *pRes = &pIter->aFirst[i];
      fts5AssertComparisonResult(pIter, p1, p2, pRes);
    }
  }
}
#else
# define fts5AssertMultiIterSetup(x,y)
#endif

/*
** Do the comparison necessary to populate pIter->aFirst[iOut].
**
** If the returned value is non-zero, then it is the index of an entry
** in the pIter->aSeg[] array that is (a) not at EOF, and (b) pointing
** to a key that is a duplicate of another, higher priority, 
** segment-iterator in the pSeg->aSeg[] array.
*/
static int fts5MultiIterDoCompare(Fts5IndexIter *pIter, int iOut){
  int i1;                         /* Index of left-hand Fts5SegIter */
  int i2;                         /* Index of right-hand Fts5SegIter */
  int iRes;
  Fts5SegIter *p1;                /* Left-hand Fts5SegIter */
  Fts5SegIter *p2;                /* Right-hand Fts5SegIter */
  Fts5CResult *pRes = &pIter->aFirst[iOut];

  assert( iOut<pIter->nSeg && iOut>0 );
  assert( pIter->bRev==0 || pIter->bRev==1 );

  if( iOut>=(pIter->nSeg/2) ){
    i1 = (iOut - pIter->nSeg/2) * 2;
    i2 = i1 + 1;
  }else{
    i1 = pIter->aFirst[iOut*2].iFirst;
    i2 = pIter->aFirst[iOut*2+1].iFirst;
  }
  p1 = &pIter->aSeg[i1];
  p2 = &pIter->aSeg[i2];

  pRes->bTermEq = 0;
  if( p1->pLeaf==0 ){           /* If p1 is at EOF */
    iRes = i2;
  }else if( p2->pLeaf==0 ){     /* If p2 is at EOF */
    iRes = i1;
  }else{
    int res = fts5BufferCompare(&p1->term, &p2->term);
    if( res==0 ){
      assert( i2>i1 );
      assert( i2!=0 );
      pRes->bTermEq = 1;
      if( p1->iRowid==p2->iRowid ){
        p1->bDel = p2->bDel;
        return i2;
      }
      res = ((p1->iRowid > p2->iRowid)==pIter->bRev) ? -1 : +1;
    }
    assert( res!=0 );
    if( res<0 ){
      iRes = i1;
    }else{
      iRes = i2;
    }
  }

  pRes->iFirst = iRes;
  return 0;
}

/*
** Move the seg-iter so that it points to the first rowid on page iLeafPgno.
** It is an error if leaf iLeafPgno does not exist or contains no rowids.
*/
static void fts5SegIterGotoPage(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegIter *pIter,             /* Iterator to advance */
  int iLeafPgno
){
  assert( iLeafPgno>pIter->iLeafPgno );

  if( iLeafPgno>pIter->pSeg->pgnoLast ){
    p->rc = FTS5_CORRUPT;
  }else{
    fts5DataRelease(pIter->pNextLeaf);
    pIter->pNextLeaf = 0;
    pIter->iLeafPgno = iLeafPgno-1;
    fts5SegIterNextPage(p, pIter);
    assert( p->rc!=SQLITE_OK || pIter->iLeafPgno==iLeafPgno );

    if( p->rc==SQLITE_OK ){
      int iOff;
      u8 *a = pIter->pLeaf->p;
      int n = pIter->pLeaf->n;

      iOff = fts5GetU16(&a[0]);
      if( iOff<4 || iOff>=n ){
        p->rc = FTS5_CORRUPT;
      }else{
        iOff += fts5GetVarint(&a[iOff], (u64*)&pIter->iRowid);
        pIter->iLeafOffset = iOff;
        fts5SegIterLoadNPos(p, pIter);
      }
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
    while( !fts5DlidxIterEof(p, pDlidx) && iMatch>fts5DlidxIterRowid(pDlidx) ){
      iLeafPgno = fts5DlidxIterPgno(pDlidx);
      fts5DlidxIterNext(p, pDlidx);
    }
    assert_nc( iLeafPgno>=pIter->iLeafPgno || p->rc );
    if( iLeafPgno>pIter->iLeafPgno ){
      fts5SegIterGotoPage(p, pIter, iLeafPgno);
      bMove = 0;
    }
  }else{
    assert( pIter->pNextLeaf==0 );
    assert( iMatch<pIter->iRowid );
    while( !fts5DlidxIterEof(p, pDlidx) && iMatch<fts5DlidxIterRowid(pDlidx) ){
      fts5DlidxIterPrev(p, pDlidx);
    }
    iLeafPgno = fts5DlidxIterPgno(pDlidx);

    assert( fts5DlidxIterEof(p, pDlidx) || iLeafPgno<=pIter->iLeafPgno );

    if( iLeafPgno<pIter->iLeafPgno ){
      pIter->iLeafPgno = iLeafPgno+1;
      fts5SegIterReverseNewPage(p, pIter);
      bMove = 0;
    }
  }

  while( p->rc==SQLITE_OK ){
    if( bMove ) fts5SegIterNext(p, pIter, 0);
    if( pIter->pLeaf==0 ) break;
    if( bRev==0 && pIter->iRowid>=iMatch ) break;
    if( bRev!=0 && pIter->iRowid<=iMatch ) break;
    bMove = 1;
  }
}


/*
** Free the iterator object passed as the second argument.
*/
static void fts5MultiIterFree(Fts5Index *p, Fts5IndexIter *pIter){
  if( pIter ){
    int i;
    for(i=0; i<pIter->nSeg; i++){
      fts5SegIterClear(&pIter->aSeg[i]);
    }
    fts5StructureRelease(pIter->pStruct);
    fts5BufferFree(&pIter->poslist);
    sqlite3_free(pIter);
  }
}

static void fts5MultiIterAdvanced(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  Fts5IndexIter *pIter,           /* Iterator to update aFirst[] array for */
  int iChanged,                   /* Index of sub-iterator just advanced */
  int iMinset                     /* Minimum entry in aFirst[] to set */
){
  int i;
  for(i=(pIter->nSeg+iChanged)/2; i>=iMinset && p->rc==SQLITE_OK; i=i/2){
    int iEq;
    if( (iEq = fts5MultiIterDoCompare(pIter, i)) ){
      fts5SegIterNext(p, &pIter->aSeg[iEq], 0);
      i = pIter->nSeg + iEq;
    }
  }
}

/*
** Sub-iterator iChanged of iterator pIter has just been advanced. It still
** points to the same term though - just a different rowid. This function
** attempts to update the contents of the pIter->aFirst[] accordingly.
** If it does so successfully, 0 is returned. Otherwise 1.
**
** If non-zero is returned, the caller should call fts5MultiIterAdvanced()
** on the iterator instead. That function does the same as this one, except
** that it deals with more complicated cases as well.
*/ 
static int fts5MultiIterAdvanceRowid(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  Fts5IndexIter *pIter,           /* Iterator to update aFirst[] array for */
  int iChanged                    /* Index of sub-iterator just advanced */
){
  Fts5SegIter *pNew = &pIter->aSeg[iChanged];

  if( pNew->iRowid==pIter->iSwitchRowid
   || (pNew->iRowid<pIter->iSwitchRowid)==pIter->bRev
  ){
    int i;
    Fts5SegIter *pOther = &pIter->aSeg[iChanged ^ 0x0001];
    pIter->iSwitchRowid = pIter->bRev ? SMALLEST_INT64 : LARGEST_INT64;
    for(i=(pIter->nSeg+iChanged)/2; 1; i=i/2){
      Fts5CResult *pRes = &pIter->aFirst[i];

      assert( pNew->pLeaf );
      assert( pRes->bTermEq==0 || pOther->pLeaf );

      if( pRes->bTermEq ){
        if( pNew->iRowid==pOther->iRowid ){
          return 1;
        }else if( (pOther->iRowid>pNew->iRowid)==pIter->bRev ){
          pIter->iSwitchRowid = pOther->iRowid;
          pNew = pOther;
        }else if( (pOther->iRowid>pIter->iSwitchRowid)==pIter->bRev ){
          pIter->iSwitchRowid = pOther->iRowid;
        }
      }
      pRes->iFirst = (pNew - pIter->aSeg);
      if( i==1 ) break;

      pOther = &pIter->aSeg[ pIter->aFirst[i ^ 0x0001].iFirst ];
    }
  }

  return 0;
}

/*
** Set the pIter->bEof variable based on the state of the sub-iterators.
*/
static void fts5MultiIterSetEof(Fts5IndexIter *pIter){
  Fts5SegIter *pSeg = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
  pIter->bEof = pSeg->pLeaf==0;
  pIter->iSwitchRowid = pSeg->iRowid;
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
  Fts5IndexIter *pIter,
  int bFrom,                      /* True if argument iFrom is valid */
  i64 iFrom                       /* Advance at least as far as this */
){
  if( p->rc==SQLITE_OK ){
    int bUseFrom = bFrom;
    do {
      int iFirst = pIter->aFirst[1].iFirst;
      int bNewTerm = 0;
      Fts5SegIter *pSeg = &pIter->aSeg[iFirst];
      assert( p->rc==SQLITE_OK );
      if( bUseFrom && pSeg->pDlidx ){
        fts5SegIterNextFrom(p, pSeg, iFrom);
      }else{
        fts5SegIterNext(p, pSeg, &bNewTerm);
      }

      if( pSeg->pLeaf==0 || bNewTerm 
       || fts5MultiIterAdvanceRowid(p, pIter, iFirst)
      ){
        fts5MultiIterAdvanced(p, pIter, iFirst, 1);
        fts5MultiIterSetEof(pIter);
      }
      fts5AssertMultiIterSetup(p, pIter);

      bUseFrom = 0;
    }while( pIter->bSkipEmpty && fts5MultiIterIsEmpty(p, pIter) );
  }
}

static Fts5IndexIter *fts5MultiIterAlloc(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  int nSeg
){
  Fts5IndexIter *pNew;
  int nSlot;                      /* Power of two >= nSeg */

  for(nSlot=2; nSlot<nSeg; nSlot=nSlot*2);
  pNew = fts5IdxMalloc(p, 
      sizeof(Fts5IndexIter) +             /* pNew */
      sizeof(Fts5SegIter) * (nSlot-1) +   /* pNew->aSeg[] */
      sizeof(Fts5CResult) * nSlot         /* pNew->aFirst[] */
  );
  if( pNew ){
    pNew->nSeg = nSlot;
    pNew->aFirst = (Fts5CResult*)&pNew->aSeg[nSlot];
    pNew->pIndex = p;
  }
  return pNew;
}

/*
** Allocate a new Fts5IndexIter object.
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
  int bSkipEmpty,                 /* True to ignore delete-keys */
  int flags,                      /* FTS5INDEX_QUERY_XXX flags */
  const u8 *pTerm, int nTerm,     /* Term to seek to (or NULL/0) */
  int iLevel,                     /* Level to iterate (-1 for all) */
  int nSegment,                   /* Number of segments to merge (iLevel>=0) */
  Fts5IndexIter **ppOut           /* New object */
){
  int nSeg = 0;                   /* Number of segment-iters in use */
  int iIter = 0;                  /* */
  int iSeg;                       /* Used to iterate through segments */
  Fts5Buffer buf = {0,0,0};       /* Buffer used by fts5SegIterSeekInit() */
  Fts5StructureLevel *pLvl;
  Fts5IndexIter *pNew;

  assert( (pTerm==0 && nTerm==0) || iLevel<0 );

  /* Allocate space for the new multi-seg-iterator. */
  if( p->rc==SQLITE_OK ){
    if( iLevel<0 ){
      assert( pStruct->nSegment==fts5StructureCountSegments(pStruct) );
      nSeg = pStruct->nSegment;
      nSeg += (p->pHash ? 1 : 0);
    }else{
      nSeg = MIN(pStruct->aLevel[iLevel].nSeg, nSegment);
    }
  }
  *ppOut = pNew = fts5MultiIterAlloc(p, nSeg);
  if( pNew==0 ) return;
  pNew->bRev = (0!=(flags & FTS5INDEX_QUERY_DESC));
  pNew->bSkipEmpty = bSkipEmpty;
  pNew->pStruct = pStruct;
  fts5StructureRef(pStruct);

  /* Initialize each of the component segment iterators. */
  if( iLevel<0 ){
    Fts5StructureLevel *pEnd = &pStruct->aLevel[pStruct->nLevel];
    if( p->pHash ){
      /* Add a segment iterator for the current contents of the hash table. */
      Fts5SegIter *pIter = &pNew->aSeg[iIter++];
      fts5SegIterHashInit(p, pTerm, nTerm, flags, pIter);
    }
    for(pLvl=&pStruct->aLevel[0]; pLvl<pEnd; pLvl++){
      for(iSeg=pLvl->nSeg-1; iSeg>=0; iSeg--){
        Fts5StructureSegment *pSeg = &pLvl->aSeg[iSeg];
        Fts5SegIter *pIter = &pNew->aSeg[iIter++];
        if( pTerm==0 ){
          fts5SegIterInit(p, pSeg, pIter);
        }else{
          fts5SegIterSeekInit(p, &buf, pTerm, nTerm, flags, pSeg, pIter);
        }
      }
    }
  }else{
    pLvl = &pStruct->aLevel[iLevel];
    for(iSeg=nSeg-1; iSeg>=0; iSeg--){
      fts5SegIterInit(p, &pLvl->aSeg[iSeg], &pNew->aSeg[iIter++]);
    }
  }
  assert( iIter==nSeg );

  /* If the above was successful, each component iterators now points 
  ** to the first entry in its segment. In this case initialize the 
  ** aFirst[] array. Or, if an error has occurred, free the iterator
  ** object and set the output variable to NULL.  */
  if( p->rc==SQLITE_OK ){
    for(iIter=pNew->nSeg-1; iIter>0; iIter--){
      int iEq;
      if( (iEq = fts5MultiIterDoCompare(pNew, iIter)) ){
        fts5SegIterNext(p, &pNew->aSeg[iEq], 0);
        fts5MultiIterAdvanced(p, pNew, iEq, iIter);
      }
    }
    fts5MultiIterSetEof(pNew);
    fts5AssertMultiIterSetup(p, pNew);

    if( pNew->bSkipEmpty && fts5MultiIterIsEmpty(p, pNew) ){
      fts5MultiIterNext(p, pNew, 0, 0);
    }
  }else{
    fts5MultiIterFree(p, pNew);
    *ppOut = 0;
  }
  fts5BufferFree(&buf);
}

/*
** Create an Fts5IndexIter that iterates through the doclist provided
** as the second argument.
*/
static void fts5MultiIterNew2(
  Fts5Index *p,                   /* FTS5 backend to iterate within */
  Fts5Data *pData,                /* Doclist to iterate through */
  int bDesc,                      /* True for descending rowid order */
  Fts5IndexIter **ppOut           /* New object */
){
  Fts5IndexIter *pNew;
  pNew = fts5MultiIterAlloc(p, 2);
  if( pNew ){
    Fts5SegIter *pIter = &pNew->aSeg[1];

    pIter->flags = FTS5_SEGITER_ONETERM;
    if( pData->n>0 ){
      pIter->pLeaf = pData;
      pIter->iLeafOffset = fts5GetVarint(pData->p, (u64*)&pIter->iRowid);
      pNew->aFirst[1].iFirst = 1;
      if( bDesc ){
        pNew->bRev = 1;
        pIter->flags |= FTS5_SEGITER_REVERSE;
        fts5SegIterReverseInitPage(p, pIter);
      }else{
        fts5SegIterLoadNPos(p, pIter);
      }
      pData = 0;
    }else{
      pNew->bEof = 1;
    }

    *ppOut = pNew;
  }

  fts5DataRelease(pData);
}

/*
** Return true if the iterator is at EOF or if an error has occurred. 
** False otherwise.
*/
static int fts5MultiIterEof(Fts5Index *p, Fts5IndexIter *pIter){
  assert( p->rc 
      || (pIter->aSeg[ pIter->aFirst[1].iFirst ].pLeaf==0)==pIter->bEof 
  );
  return (p->rc || pIter->bEof);
}

/*
** Return the rowid of the entry that the iterator currently points
** to. If the iterator points to EOF when this function is called the
** results are undefined.
*/
static i64 fts5MultiIterRowid(Fts5IndexIter *pIter){
  assert( pIter->aSeg[ pIter->aFirst[1].iFirst ].pLeaf );
  return pIter->aSeg[ pIter->aFirst[1].iFirst ].iRowid;
}

/*
** Move the iterator to the next entry at or following iMatch.
*/
static void fts5MultiIterNextFrom(
  Fts5Index *p, 
  Fts5IndexIter *pIter, 
  i64 iMatch
){
  while( 1 ){
    i64 iRowid;
    fts5MultiIterNext(p, pIter, 1, iMatch);
    if( fts5MultiIterEof(p, pIter) ) break;
    iRowid = fts5MultiIterRowid(pIter);
    if( pIter->bRev==0 && iRowid>=iMatch ) break;
    if( pIter->bRev!=0 && iRowid<=iMatch ) break;
  }
}

/*
** Return a pointer to a buffer containing the term associated with the 
** entry that the iterator currently points to.
*/
static const u8 *fts5MultiIterTerm(Fts5IndexIter *pIter, int *pn){
  Fts5SegIter *p = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
  *pn = p->term.n;
  return p->term.p;
}

static void fts5ChunkIterate(
  Fts5Index *p,                   /* Index object */
  Fts5SegIter *pSeg,              /* Poslist of this iterator */
  void *pCtx,                     /* Context pointer for xChunk callback */
  void (*xChunk)(Fts5Index*, void*, const u8*, int)
){
  int nRem = pSeg->nPos;          /* Number of bytes still to come */
  Fts5Data *pData = 0;
  u8 *pChunk = &pSeg->pLeaf->p[pSeg->iLeafOffset];
  int nChunk = MIN(nRem, pSeg->pLeaf->n - pSeg->iLeafOffset);
  int pgno = pSeg->iLeafPgno;
  int pgnoSave = 0;

  if( (pSeg->flags & FTS5_SEGITER_REVERSE)==0 ){
    pgnoSave = pgno+1;
  }

  while( 1 ){
    xChunk(p, pCtx, pChunk, nChunk);
    nRem -= nChunk;
    fts5DataRelease(pData);
    if( nRem<=0 ){
      break;
    }else{
      pgno++;
      pData = fts5DataRead(p, FTS5_SEGMENT_ROWID(pSeg->pSeg->iSegid, 0, pgno));
      if( pData==0 ) break;
      pChunk = &pData->p[4];
      nChunk = MIN(nRem, pData->n - 4);
      if( pgno==pgnoSave ){
        assert( pSeg->pNextLeaf==0 );
        pSeg->pNextLeaf = pData;
        pData = 0;
      }
    }
  }
}



/*
** Allocate a new segment-id for the structure pStruct. The new segment
** id must be between 1 and 65335 inclusive, and must not be used by 
** any currently existing segment. If a free segment id cannot be found,
** SQLITE_FULL is returned.
**
** If an error has already occurred, this function is a no-op. 0 is 
** returned in this case.
*/
static int fts5AllocateSegid(Fts5Index *p, Fts5Structure *pStruct){
  int iSegid = 0;

  if( p->rc==SQLITE_OK ){
    if( pStruct->nSegment>=FTS5_MAX_SEGMENT ){
      p->rc = SQLITE_FULL;
    }else{
      while( iSegid==0 ){
        int iLvl, iSeg;
        sqlite3_randomness(sizeof(u32), (void*)&iSegid);
        iSegid = iSegid & ((1 << FTS5_DATA_ID_B)-1);
        for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
          for(iSeg=0; iSeg<pStruct->aLevel[iLvl].nSeg; iSeg++){
            if( iSegid==pStruct->aLevel[iLvl].aSeg[iSeg].iSegid ){
              iSegid = 0;
            }
          }
        }
      }
    }
  }

  return iSegid;
}

/*
** Discard all data currently cached in the hash-tables.
*/
static void fts5IndexDiscardData(Fts5Index *p){
  assert( p->pHash || p->nPendingData==0 );
  if( p->pHash ){
    sqlite3Fts5HashClear(p->pHash);
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
  assert( fts5BlobCompare(pOld, nOld, pNew, nNew)<0 );
  for(i=0; i<nOld; i++){
    if( pOld[i]!=pNew[i] ) break;
  }
  return i;
}

static void fts5WriteDlidxClear(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,
  int bFlush                      /* If true, write dlidx to disk */
){
  int i;
  assert( bFlush==0 || (pWriter->nDlidx>0 && pWriter->aDlidx[0].buf.n>0) );
  for(i=0; i<pWriter->nDlidx; i++){
    Fts5DlidxWriter *pDlidx = &pWriter->aDlidx[i];
    if( pDlidx->buf.n==0 ) break;
    if( bFlush ){
      assert( pDlidx->pgno!=0 );
      fts5DataWrite(p, 
          FTS5_DLIDX_ROWID(pWriter->iSegid, i, pDlidx->pgno),
          pDlidx->buf.p, pDlidx->buf.n
      );
    }
    sqlite3Fts5BufferZero(&pDlidx->buf);
    pDlidx->bPrevValid = 0;
  }
}

/*
** Grow the pWriter->aDlidx[] array to at least nLvl elements in size.
** Any new array elements are zeroed before returning.
*/
static int fts5WriteDlidxGrow(
  Fts5Index *p,
  Fts5SegWriter *pWriter,
  int nLvl
){
  if( p->rc==SQLITE_OK && nLvl>=pWriter->nDlidx ){
    Fts5DlidxWriter *aDlidx = (Fts5DlidxWriter*)sqlite3_realloc(
        pWriter->aDlidx, sizeof(Fts5DlidxWriter) * nLvl
    );
    if( aDlidx==0 ){
      p->rc = SQLITE_NOMEM;
    }else{
      int nByte = sizeof(Fts5DlidxWriter) * (nLvl - pWriter->nDlidx);
      memset(&aDlidx[pWriter->nDlidx], 0, nByte);
      pWriter->aDlidx = aDlidx;
      pWriter->nDlidx = nLvl;
    }
  }
  return p->rc;
}

/*
** If the current doclist-index accumulating in pWriter->aDlidx[] is large
** enough, flush it to disk and return 1. Otherwise discard it and return
** zero.
*/
static int fts5WriteFlushDlidx(Fts5Index *p, Fts5SegWriter *pWriter){
  int bFlag = 0;

  /* If there were FTS5_MIN_DLIDX_SIZE or more empty leaf pages written
  ** to the database, also write the doclist-index to disk.  */
  if( pWriter->aDlidx[0].buf.n>0 && pWriter->nEmpty>=FTS5_MIN_DLIDX_SIZE ){
    bFlag = 1;
  }
  fts5WriteDlidxClear(p, pWriter, bFlag);
  pWriter->nEmpty = 0;
  return bFlag;
}

/*
** This function is called whenever processing of the doclist for the 
** last term on leaf page (pWriter->iBtPage) is completed. 
**
** The doclist-index for that term is currently stored in-memory within the
** Fts5SegWriter.aDlidx[] array. If it is large enough, this function
** writes it out to disk. Or, if it is too small to bother with, discards
** it.
**
** Fts5SegWriter.btterm currently contains the first term on page iBtPage.
*/
static void fts5WriteFlushBtree(Fts5Index *p, Fts5SegWriter *pWriter){
  int bFlag;

  assert( pWriter->iBtPage || pWriter->nEmpty==0 );
  if( pWriter->iBtPage==0 ) return;
  bFlag = fts5WriteFlushDlidx(p, pWriter);

  if( p->rc==SQLITE_OK ){
    const char *z = (pWriter->btterm.n>0?(const char*)pWriter->btterm.p:"");
    /* The following was already done in fts5WriteInit(): */
    /* sqlite3_bind_int(p->pIdxWriter, 1, pWriter->iSegid); */
    sqlite3_bind_blob(p->pIdxWriter, 2, z, pWriter->btterm.n, SQLITE_STATIC);
    sqlite3_bind_int64(p->pIdxWriter, 3, bFlag + ((i64)pWriter->iBtPage<<1));
    sqlite3_step(p->pIdxWriter);
    p->rc = sqlite3_reset(p->pIdxWriter);
  }
  pWriter->iBtPage = 0;
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
  fts5WriteFlushBtree(p, pWriter);
  fts5BufferSet(&p->rc, &pWriter->btterm, nTerm, pTerm);
  pWriter->iBtPage = pWriter->writer.pgno;
}

/*
** This function is called when flushing a leaf page that contains no
** terms at all to disk.
*/
static void fts5WriteBtreeNoTerm(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5SegWriter *pWriter          /* Writer object */
){
  /* If there were no rowids on the leaf page either and the doclist-index
  ** has already been started, append an 0x00 byte to it.  */
  if( pWriter->bFirstRowidInPage && pWriter->aDlidx[0].buf.n>0 ){
    Fts5DlidxWriter *pDlidx = &pWriter->aDlidx[0];
    assert( pDlidx->bPrevValid );
    sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx->buf, 0);
  }

  /* Increment the "number of sequential leaves without a term" counter. */
  pWriter->nEmpty++;
}

static i64 fts5DlidxExtractFirstRowid(Fts5Buffer *pBuf){
  i64 iRowid;
  int iOff;

  iOff = 1 + fts5GetVarint(&pBuf->p[1], (u64*)&iRowid);
  fts5GetVarint(&pBuf->p[iOff], (u64*)&iRowid);
  return iRowid;
}

/*
** Rowid iRowid has just been appended to the current leaf page. It is the
** first on the page. This function appends an appropriate entry to the current
** doclist-index.
*/
static void fts5WriteDlidxAppend(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  i64 iRowid
){
  int i;
  int bDone = 0;

  for(i=0; p->rc==SQLITE_OK && bDone==0; i++){
    i64 iVal;
    Fts5DlidxWriter *pDlidx = &pWriter->aDlidx[i];

    if( pDlidx->buf.n>=p->pConfig->pgsz ){
      /* The current doclist-index page is full. Write it to disk and push
      ** a copy of iRowid (which will become the first rowid on the next
      ** doclist-index leaf page) up into the next level of the b-tree 
      ** hierarchy. If the node being flushed is currently the root node,
      ** also push its first rowid upwards. */
      pDlidx->buf.p[0] = 0x01;    /* Not the root node */
      fts5DataWrite(p, 
          FTS5_DLIDX_ROWID(pWriter->iSegid, i, pDlidx->pgno),
          pDlidx->buf.p, pDlidx->buf.n
      );
      fts5WriteDlidxGrow(p, pWriter, i+2);
      pDlidx = &pWriter->aDlidx[i];
      if( p->rc==SQLITE_OK && pDlidx[1].buf.n==0 ){
        i64 iFirst = fts5DlidxExtractFirstRowid(&pDlidx->buf);

        /* This was the root node. Push its first rowid up to the new root. */
        pDlidx[1].pgno = pDlidx->pgno;
        sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx[1].buf, 0);
        sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx[1].buf, pDlidx->pgno);
        sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx[1].buf, iFirst);
        pDlidx[1].bPrevValid = 1;
        pDlidx[1].iPrev = iFirst;
      }

      sqlite3Fts5BufferZero(&pDlidx->buf);
      pDlidx->bPrevValid = 0;
      pDlidx->pgno++;
    }else{
      bDone = 1;
    }

    if( pDlidx->bPrevValid ){
      iVal = iRowid - pDlidx->iPrev;
    }else{
      i64 iPgno = (i==0 ? pWriter->writer.pgno : pDlidx[-1].pgno);
      assert( pDlidx->buf.n==0 );
      sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx->buf, !bDone);
      sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx->buf, iPgno);
      iVal = iRowid;
    }

    sqlite3Fts5BufferAppendVarint(&p->rc, &pDlidx->buf, iVal);
    pDlidx->bPrevValid = 1;
    pDlidx->iPrev = iRowid;
  }
}

static void fts5WriteFlushLeaf(Fts5Index *p, Fts5SegWriter *pWriter){
  static const u8 zero[] = { 0x00, 0x00, 0x00, 0x00 };
  Fts5PageWriter *pPage = &pWriter->writer;
  i64 iRowid;

  if( pWriter->bFirstTermInPage ){
    /* No term was written to this page. */
    assert( 0==fts5GetU16(&pPage->buf.p[2]) );
    fts5WriteBtreeNoTerm(p, pWriter);
  }

  /* Write the current page to the db. */
  iRowid = FTS5_SEGMENT_ROWID(pWriter->iSegid, 0, pPage->pgno);
  fts5DataWrite(p, iRowid, pPage->buf.p, pPage->buf.n);

  /* Initialize the next page. */
  fts5BufferZero(&pPage->buf);
  fts5BufferAppendBlob(&p->rc, &pPage->buf, 4, zero);
  pPage->pgno++;

  /* Increase the leaves written counter */
  pWriter->nLeafWritten++;

  /* The new leaf holds no terms or rowids */
  pWriter->bFirstTermInPage = 1;
  pWriter->bFirstRowidInPage = 1;
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
  Fts5PageWriter *pPage = &pWriter->writer;

  assert( pPage->buf.n==0 || pPage->buf.n>4 );
  if( pPage->buf.n==0 ){
    /* Zero the first term and first docid fields */
    static const u8 zero[] = { 0x00, 0x00, 0x00, 0x00 };
    fts5BufferAppendBlob(&p->rc, &pPage->buf, 4, zero);
    assert( pWriter->bFirstTermInPage );
  }
  if( p->rc ) return;
  
  if( pWriter->bFirstTermInPage ){
    /* Update the "first term" field of the page header. */
    assert( pPage->buf.p[2]==0 && pPage->buf.p[3]==0 );
    fts5PutU16(&pPage->buf.p[2], pPage->buf.n);
    nPrefix = 0;
    if( pPage->pgno!=1 ){
      /* This is the first term on a leaf that is not the leftmost leaf in
      ** the segment b-tree. In this case it is necessary to add a term to
      ** the b-tree hierarchy that is (a) larger than the largest term 
      ** already written to the segment and (b) smaller than or equal to
      ** this term. In other words, a prefix of (pTerm/nTerm) that is one
      ** byte longer than the longest prefix (pTerm/nTerm) shares with the
      ** previous term. 
      **
      ** Usually, the previous term is available in pPage->term. The exception
      ** is if this is the first term written in an incremental-merge step.
      ** In this case the previous term is not available, so just write a
      ** copy of (pTerm/nTerm) into the parent node. This is slightly
      ** inefficient, but still correct.  */
      int n = nTerm;
      if( pPage->term.n ){
        n = 1 + fts5PrefixCompress(pPage->term.n, pPage->term.p, nTerm, pTerm);
      }
      fts5WriteBtreeTerm(p, pWriter, n, pTerm);
      pPage = &pWriter->writer;
    }
  }else{
    nPrefix = fts5PrefixCompress(pPage->term.n, pPage->term.p, nTerm, pTerm);
    fts5BufferAppendVarint(&p->rc, &pPage->buf, nPrefix);
  }

  /* Append the number of bytes of new data, then the term data itself
  ** to the page. */
  fts5BufferAppendVarint(&p->rc, &pPage->buf, nTerm - nPrefix);
  fts5BufferAppendBlob(&p->rc, &pPage->buf, nTerm - nPrefix, &pTerm[nPrefix]);

  /* Update the Fts5PageWriter.term field. */
  fts5BufferSet(&p->rc, &pPage->term, nTerm, pTerm);
  pWriter->bFirstTermInPage = 0;

  pWriter->bFirstRowidInPage = 0;
  pWriter->bFirstRowidInDoclist = 1;

  assert( p->rc || (pWriter->nDlidx>0 && pWriter->aDlidx[0].buf.n==0) );
  pWriter->aDlidx[0].pgno = pPage->pgno;

  /* If the current leaf page is full, flush it to disk. */
  if( pPage->buf.n>=p->pConfig->pgsz ){
    fts5WriteFlushLeaf(p, pWriter);
  }
}

/*
** Append a docid and position-list size field to the writers output. 
*/
static void fts5WriteAppendRowid(
  Fts5Index *p, 
  Fts5SegWriter *pWriter,
  i64 iRowid,
  int nPos
){
  if( p->rc==SQLITE_OK ){
    Fts5PageWriter *pPage = &pWriter->writer;

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
      assert( p->rc || iRowid>pWriter->iPrevRowid );
      fts5BufferAppendVarint(&p->rc, &pPage->buf, iRowid - pWriter->iPrevRowid);
    }
    pWriter->iPrevRowid = iRowid;
    pWriter->bFirstRowidInDoclist = 0;
    pWriter->bFirstRowidInPage = 0;

    fts5BufferAppendVarint(&p->rc, &pPage->buf, nPos);

    if( pPage->buf.n>=p->pConfig->pgsz ){
      fts5WriteFlushLeaf(p, pWriter);
    }
  }
}

static void fts5WriteAppendPoslistData(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  const u8 *aData, 
  int nData
){
  Fts5PageWriter *pPage = &pWriter->writer;
  const u8 *a = aData;
  int n = nData;
  
  assert( p->pConfig->pgsz>0 );
  while( p->rc==SQLITE_OK && (pPage->buf.n + n)>=p->pConfig->pgsz ){
    int nReq = p->pConfig->pgsz - pPage->buf.n;
    int nCopy = 0;
    while( nCopy<nReq ){
      i64 dummy;
      nCopy += fts5GetVarint(&a[nCopy], (u64*)&dummy);
    }
    fts5BufferAppendBlob(&p->rc, &pPage->buf, nCopy, a);
    a += nCopy;
    n -= nCopy;
    fts5WriteFlushLeaf(p, pWriter);
  }
  if( n>0 ){
    fts5BufferAppendBlob(&p->rc, &pPage->buf, n, a);
  }
}

static void fts5WriteAppendZerobyte(Fts5Index *p, Fts5SegWriter *pWriter){
  fts5BufferAppendVarint(&p->rc, &pWriter->writer.buf, 0);
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
  Fts5PageWriter *pLeaf = &pWriter->writer;
  if( p->rc==SQLITE_OK ){
    if( pLeaf->pgno==1 && pLeaf->buf.n==0 ){
      *pnLeaf = 0;
      *pnHeight = 0;
    }else{
      if( pLeaf->buf.n>4 ){
        fts5WriteFlushLeaf(p, pWriter);
      }
      *pnLeaf = pLeaf->pgno-1;

      fts5WriteFlushBtree(p, pWriter);
      *pnHeight = 0;
    }
  }
  fts5BufferFree(&pLeaf->term);
  fts5BufferFree(&pLeaf->buf);
  fts5BufferFree(&pWriter->btterm);

  for(i=0; i<pWriter->nDlidx; i++){
    sqlite3Fts5BufferFree(&pWriter->aDlidx[i].buf);
  }
  sqlite3_free(pWriter->aDlidx);
}

static void fts5WriteInit(
  Fts5Index *p, 
  Fts5SegWriter *pWriter, 
  int iSegid
){
  memset(pWriter, 0, sizeof(Fts5SegWriter));
  pWriter->iSegid = iSegid;

  fts5WriteDlidxGrow(p, pWriter, 1);
  pWriter->writer.pgno = 1;
  pWriter->bFirstTermInPage = 1;
  pWriter->iBtPage = 1;

  if( p->pIdxWriter==0 ){
    Fts5Config *pConfig = p->pConfig;
    fts5IndexPrepareStmt(p, &p->pIdxWriter, sqlite3_mprintf(
          "INSERT INTO '%q'.'%q_idx'(segid,term,pgno) VALUES(?,?,?)", 
          pConfig->zDb, pConfig->zName
    ));
  }

  if( p->rc==SQLITE_OK ){
    sqlite3_bind_int(p->pIdxWriter, 1, pWriter->iSegid);
  }
}

/*
** Iterator pIter was used to iterate through the input segments of on an
** incremental merge operation. This function is called if the incremental
** merge step has finished but the input has not been completely exhausted.
*/
static void fts5TrimSegments(Fts5Index *p, Fts5IndexIter *pIter){
  int i;
  Fts5Buffer buf;
  memset(&buf, 0, sizeof(Fts5Buffer));
  for(i=0; i<pIter->nSeg; i++){
    Fts5SegIter *pSeg = &pIter->aSeg[i];
    if( pSeg->pSeg==0 ){
      /* no-op */
    }else if( pSeg->pLeaf==0 ){
      /* All keys from this input segment have been transfered to the output.
      ** Set both the first and last page-numbers to 0 to indicate that the
      ** segment is now empty. */
      pSeg->pSeg->pgnoLast = 0;
      pSeg->pSeg->pgnoFirst = 0;
    }else{
      int iOff = pSeg->iTermLeafOffset;     /* Offset on new first leaf page */
      i64 iLeafRowid;
      Fts5Data *pData;
      int iId = pSeg->pSeg->iSegid;
      u8 aHdr[4] = {0x00, 0x00, 0x00, 0x04};

      iLeafRowid = FTS5_SEGMENT_ROWID(iId, 0, pSeg->iTermLeafPgno);
      pData = fts5DataRead(p, iLeafRowid);
      if( pData ){
        fts5BufferZero(&buf);
        fts5BufferAppendBlob(&p->rc, &buf, sizeof(aHdr), aHdr);
        fts5BufferAppendVarint(&p->rc, &buf, pSeg->term.n);
        fts5BufferAppendBlob(&p->rc, &buf, pSeg->term.n, pSeg->term.p);
        fts5BufferAppendBlob(&p->rc, &buf, pData->n - iOff, &pData->p[iOff]);
        fts5DataRelease(pData);
        pSeg->pSeg->pgnoFirst = pSeg->iTermLeafPgno;
        fts5DataDelete(p, FTS5_SEGMENT_ROWID(iId, 0, 1), iLeafRowid);
        fts5DataWrite(p, iLeafRowid, buf.p, buf.n);
      }
    }
  }
  fts5BufferFree(&buf);
}

static void fts5MergeChunkCallback(
  Fts5Index *p, 
  void *pCtx, 
  const u8 *pChunk, int nChunk
){
  Fts5SegWriter *pWriter = (Fts5SegWriter*)pCtx;
  fts5WriteAppendPoslistData(p, pWriter, pChunk, nChunk);
}

/*
**
*/
static void fts5IndexMergeLevel(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5Structure **ppStruct,       /* IN/OUT: Stucture of index */
  int iLvl,                       /* Level to read input from */
  int *pnRem                      /* Write up to this many output leaves */
){
  Fts5Structure *pStruct = *ppStruct;
  Fts5StructureLevel *pLvl = &pStruct->aLevel[iLvl];
  Fts5StructureLevel *pLvlOut;
  Fts5IndexIter *pIter = 0;       /* Iterator to read input data */
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
  if( pLvl->nMerge ){
    pLvlOut = &pStruct->aLevel[iLvl+1];
    assert( pLvlOut->nSeg>0 );
    nInput = pLvl->nMerge;
    pSeg = &pLvlOut->aSeg[pLvlOut->nSeg-1];

    fts5WriteInit(p, &writer, pSeg->iSegid);
    writer.writer.pgno = pSeg->pgnoLast+1;
    writer.iBtPage = 0;
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

    fts5WriteInit(p, &writer, iSegid);

    /* Add the new segment to the output level */
    pSeg = &pLvlOut->aSeg[pLvlOut->nSeg];
    pLvlOut->nSeg++;
    pSeg->pgnoFirst = 1;
    pSeg->iSegid = iSegid;
    pStruct->nSegment++;

    /* Read input from all segments in the input level */
    nInput = pLvl->nSeg;
  }
  bOldest = (pLvlOut->nSeg==1 && pStruct->nLevel==iLvl+2);

  assert( iLvl>=0 );
  for(fts5MultiIterNew(p, pStruct, 0, 0, 0, 0, iLvl, nInput, &pIter);
      fts5MultiIterEof(p, pIter)==0;
      fts5MultiIterNext(p, pIter, 0, 0)
  ){
    Fts5SegIter *pSegIter = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
    int nPos;                     /* position-list size field value */
    int nTerm;
    const u8 *pTerm;

    /* Check for key annihilation. */
    if( pSegIter->nPos==0 && (bOldest || pSegIter->bDel==0) ) continue;

    pTerm = fts5MultiIterTerm(pIter, &nTerm);
    if( nTerm!=term.n || memcmp(pTerm, term.p, nTerm) ){
      if( pnRem && writer.nLeafWritten>nRem ){
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
    /* WRITEPOSLISTSIZE */
    nPos = pSegIter->nPos*2 + pSegIter->bDel;
    fts5WriteAppendRowid(p, &writer, fts5MultiIterRowid(pIter), nPos);

    /* Append the position-list data to the output */
    fts5ChunkIterate(p, pSegIter, (void*)&writer, fts5MergeChunkCallback);
  }

  /* Flush the last leaf page to disk. Set the output segment b-tree height
  ** and last leaf page number at the same time.  */
  fts5WriteFinish(p, &writer, &pSeg->nHeight, &pSeg->pgnoLast);

  if( fts5MultiIterEof(p, pIter) ){
    int i;

    /* Remove the redundant segments from the %_data table */
    for(i=0; i<nInput; i++){
      fts5DataRemoveSegment(p, pLvl->aSeg[i].iSegid);
    }

    /* Remove the redundant segments from the input level */
    if( pLvl->nSeg!=nInput ){
      int nMove = (pLvl->nSeg - nInput) * sizeof(Fts5StructureSegment);
      memmove(pLvl->aSeg, &pLvl->aSeg[nInput], nMove);
    }
    pStruct->nSegment -= nInput;
    pLvl->nSeg -= nInput;
    pLvl->nMerge = 0;
    if( pSeg->pgnoLast==0 ){
      pLvlOut->nSeg--;
      pStruct->nSegment--;
    }
  }else{
    assert( pSeg->pgnoLast>0 );
    fts5TrimSegments(p, pIter);
    pLvl->nMerge = nInput;
  }

  fts5MultiIterFree(p, pIter);
  fts5BufferFree(&term);
  if( pnRem ) *pnRem -= writer.nLeafWritten;
}

/*
** Do up to nPg pages of automerge work on the index.
*/
static void fts5IndexMerge(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5Structure **ppStruct,       /* IN/OUT: Current structure of index */
  int nPg                         /* Pages of work to do */
){
  int nRem = nPg;
  Fts5Structure *pStruct = *ppStruct;
  while( nRem>0 && p->rc==SQLITE_OK ){
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
    fts5IndexMergeLevel(p, &pStruct, iBestLvl, &nRem);
    if( p->rc==SQLITE_OK && pStruct->aLevel[iBestLvl].nMerge==0 ){
      fts5StructurePromote(p, iBestLvl+1, pStruct);
    }
  }
  *ppStruct = pStruct;
}

/*
** A total of nLeaf leaf pages of data has just been flushed to a level-0
** segment. This function updates the write-counter accordingly and, if
** necessary, performs incremental merge work.
**
** If an error occurs, set the Fts5Index.rc error code. If an error has 
** already occurred, this function is a no-op.
*/
static void fts5IndexAutomerge(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5Structure **ppStruct,       /* IN/OUT: Current structure of index */
  int nLeaf                       /* Number of output leaves just written */
){
  if( p->rc==SQLITE_OK && p->pConfig->nAutomerge>0 ){
    Fts5Structure *pStruct = *ppStruct;
    u64 nWrite;                   /* Initial value of write-counter */
    int nWork;                    /* Number of work-quanta to perform */
    int nRem;                     /* Number of leaf pages left to write */

    /* Update the write-counter. While doing so, set nWork. */
    nWrite = pStruct->nWriteCounter;
    nWork = (int)(((nWrite + nLeaf) / p->nWorkUnit) - (nWrite / p->nWorkUnit));
    pStruct->nWriteCounter += nLeaf;
    nRem = (int)(p->nWorkUnit * nWork * pStruct->nLevel);

    fts5IndexMerge(p, ppStruct, nRem);
  }
}

static void fts5IndexCrisismerge(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5Structure **ppStruct        /* IN/OUT: Current structure of index */
){
  const int nCrisis = p->pConfig->nCrisisMerge;
  Fts5Structure *pStruct = *ppStruct;
  int iLvl = 0;

  assert( p->rc!=SQLITE_OK || pStruct->nLevel>0 );
  while( p->rc==SQLITE_OK && pStruct->aLevel[iLvl].nSeg>=nCrisis ){
    fts5IndexMergeLevel(p, &pStruct, iLvl, 0);
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

/*
** Buffer aBuf[] contains a list of varints, all small enough to fit
** in a 32-bit integer. Return the size of the largest prefix of this 
** list nMax bytes or less in size.
*/
static int fts5PoslistPrefix(const u8 *aBuf, int nMax){
  int ret;
  u32 dummy;
  ret = fts5GetVarint32(aBuf, dummy);
  while( 1 ){
    int i = fts5GetVarint32(&aBuf[ret], dummy);
    if( (ret + i) > nMax ) break;
    ret += i;
  }
  return ret;
}

#define fts5BufferSafeAppendBlob(pBuf, pBlob, nBlob) { \
  assert( pBuf->nSpace>=(pBuf->n+nBlob) );             \
  memcpy(&pBuf->p[pBuf->n], pBlob, nBlob);             \
  pBuf->n += nBlob;                                    \
}

/*
** Flush the contents of in-memory hash table iHash to a new level-0 
** segment on disk. Also update the corresponding structure record.
**
** If an error occurs, set the Fts5Index.rc error code. If an error has 
** already occurred, this function is a no-op.
*/
static void fts5FlushOneHash(Fts5Index *p){
  Fts5Hash *pHash = p->pHash;
  Fts5Structure *pStruct;
  int iSegid;
  int pgnoLast = 0;                 /* Last leaf page number in segment */

  /* Obtain a reference to the index structure and allocate a new segment-id
  ** for the new level-0 segment.  */
  pStruct = fts5StructureRead(p);
  iSegid = fts5AllocateSegid(p, pStruct);

  if( iSegid ){
    const int pgsz = p->pConfig->pgsz;

    Fts5StructureSegment *pSeg;   /* New segment within pStruct */
    int nHeight;                  /* Height of new segment b-tree */
    Fts5Buffer *pBuf;             /* Buffer in which to assemble leaf page */
    const u8 *zPrev = 0;

    Fts5SegWriter writer;
    fts5WriteInit(p, &writer, iSegid);

    /* Pre-allocate the buffer used to assemble leaf pages to the target
    ** page size.  */
    assert( pgsz>0 );
    pBuf = &writer.writer.buf;
    fts5BufferGrow(&p->rc, pBuf, pgsz + 20);

    /* Begin scanning through hash table entries. This loop runs once for each
    ** term/doclist currently stored within the hash table. */
    if( p->rc==SQLITE_OK ){
      memset(pBuf->p, 0, 4);
      pBuf->n = 4;
      p->rc = sqlite3Fts5HashScanInit(pHash, 0, 0);
    }
    while( p->rc==SQLITE_OK && 0==sqlite3Fts5HashScanEof(pHash) ){
      const char *zTerm;          /* Buffer containing term */
      int nTerm;                  /* Size of zTerm in bytes */
      const u8 *pDoclist;         /* Pointer to doclist for this term */
      int nDoclist;               /* Size of doclist in bytes */
      int nSuffix;                /* Size of term suffix */

      sqlite3Fts5HashScanEntry(pHash, &zTerm, &pDoclist, &nDoclist);
      nTerm = strlen(zTerm);

      /* Decide if the term will fit on the current leaf. If it will not, 
      ** flush the leaf to disk here.  */
      if( (pBuf->n + nTerm + 2) > pgsz ){
        fts5WriteFlushLeaf(p, &writer);
        pBuf = &writer.writer.buf;
        if( (nTerm + 32) > pBuf->nSpace ){
          fts5BufferGrow(&p->rc, pBuf, nTerm + 32 - pBuf->n);
          if( p->rc ) break;
        }
      }

      /* Write the term to the leaf. And if it is the first on the leaf, and
      ** the leaf is not page number 1, push it up into the b-tree hierarchy 
      ** as well.  */
      if( writer.bFirstTermInPage==0 ){
        int nPre = fts5PrefixCompress(nTerm, zPrev, nTerm, (const u8*)zTerm);
        pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], nPre);
        nSuffix = nTerm - nPre;
      }else{
        fts5PutU16(&pBuf->p[2], pBuf->n);
        writer.bFirstTermInPage = 0;
        if( writer.writer.pgno!=1 ){
          int nPre = fts5PrefixCompress(nTerm, zPrev, nTerm, (const u8*)zTerm);
          fts5WriteBtreeTerm(p, &writer, nPre+1, (const u8*)zTerm);
          pBuf = &writer.writer.buf;
          assert( nPre<nTerm );
        }
        nSuffix = nTerm;
      }
      pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], nSuffix);
      fts5BufferSafeAppendBlob(pBuf, (const u8*)&zTerm[nTerm-nSuffix], nSuffix);

      /* We just wrote a term into page writer.aWriter[0].pgno. If a 
      ** doclist-index is to be generated for this doclist, it will be
      ** associated with this page. */
      assert( writer.nDlidx>0 && writer.aDlidx[0].buf.n==0 );
      writer.aDlidx[0].pgno = writer.writer.pgno;

      if( pgsz>=(pBuf->n + nDoclist + 1) ){
        /* The entire doclist will fit on the current leaf. */
        fts5BufferSafeAppendBlob(pBuf, pDoclist, nDoclist);
      }else{
        i64 iRowid = 0;
        i64 iDelta = 0;
        int iOff = 0;

        writer.bFirstRowidInPage = 0;

        /* The entire doclist will not fit on this leaf. The following 
        ** loop iterates through the poslists that make up the current 
        ** doclist.  */
        while( p->rc==SQLITE_OK && iOff<nDoclist ){
          int nPos;
          int nCopy;
          int bDummy;
          iOff += fts5GetVarint(&pDoclist[iOff], (u64*)&iDelta);
          nCopy = fts5GetPoslistSize(&pDoclist[iOff], &nPos, &bDummy);
          nCopy += nPos;
          iRowid += iDelta;
          
          if( writer.bFirstRowidInPage ){
            fts5PutU16(&pBuf->p[0], pBuf->n);   /* first docid on page */
            pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], iRowid);
            writer.bFirstRowidInPage = 0;
            fts5WriteDlidxAppend(p, &writer, iRowid);
          }else{
            pBuf->n += sqlite3Fts5PutVarint(&pBuf->p[pBuf->n], iDelta);
          }
          assert( pBuf->n<=pBuf->nSpace );

          if( (pBuf->n + nCopy) <= pgsz ){
            /* The entire poslist will fit on the current leaf. So copy
            ** it in one go. */
            fts5BufferSafeAppendBlob(pBuf, &pDoclist[iOff], nCopy);
          }else{
            /* The entire poslist will not fit on this leaf. So it needs
            ** to be broken into sections. The only qualification being
            ** that each varint must be stored contiguously.  */
            const u8 *pPoslist = &pDoclist[iOff];
            int iPos = 0;
            while( p->rc==SQLITE_OK ){
              int nSpace = pgsz - pBuf->n;
              int n = 0;
              if( (nCopy - iPos)<=nSpace ){
                n = nCopy - iPos;
              }else{
                n = fts5PoslistPrefix(&pPoslist[iPos], nSpace);
              }
              assert( n>0 );
              fts5BufferSafeAppendBlob(pBuf, &pPoslist[iPos], n);
              iPos += n;
              if( pBuf->n>=pgsz ){
                fts5WriteFlushLeaf(p, &writer);
                pBuf = &writer.writer.buf;
              }
              if( iPos>=nCopy ) break;
            }
          }
          iOff += nCopy;
        }
      }

      pBuf->p[pBuf->n++] = '\0';
      assert( pBuf->n<=pBuf->nSpace );
      zPrev = (const u8*)zTerm;
      sqlite3Fts5HashScanNext(pHash);
    }
    sqlite3Fts5HashClear(pHash);
    fts5WriteFinish(p, &writer, &nHeight, &pgnoLast);

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
      pStruct->nSegment++;
    }
    fts5StructurePromote(p, 0, pStruct);
  }

  fts5IndexAutomerge(p, &pStruct, pgnoLast);
  fts5IndexCrisismerge(p, &pStruct);
  fts5StructureWrite(p, pStruct);
  fts5StructureRelease(pStruct);
}

/*
** Flush any data stored in the in-memory hash tables to the database.
*/
static void fts5IndexFlush(Fts5Index *p){
  /* Unless it is empty, flush the hash table to disk */
  if( p->nPendingData ){
    assert( p->pHash );
    p->nPendingData = 0;
    fts5FlushOneHash(p);
  }
}


int sqlite3Fts5IndexOptimize(Fts5Index *p){
  Fts5Structure *pStruct;
  Fts5Structure *pNew = 0;
  int nSeg = 0;

  assert( p->rc==SQLITE_OK );
  fts5IndexFlush(p);
  pStruct = fts5StructureRead(p);

  if( pStruct ){
    assert( pStruct->nSegment==fts5StructureCountSegments(pStruct) );
    nSeg = pStruct->nSegment;
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
    pNew->nRef = 1;
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
      pNew->nSegment = pLvl->nSeg = nSeg;
    }else{
      sqlite3_free(pNew);
      pNew = 0;
    }
  }

  if( pNew ){
    int iLvl = pNew->nLevel-1;
    while( p->rc==SQLITE_OK && pNew->aLevel[iLvl].nSeg>0 ){
      int nRem = FTS5_OPT_WORK_UNIT;
      fts5IndexMergeLevel(p, &pNew, iLvl, &nRem);
    }

    fts5StructureWrite(p, pNew);
    fts5StructureRelease(pNew);
  }

  fts5StructureRelease(pStruct);
  return fts5IndexReturn(p); 
}

int sqlite3Fts5IndexMerge(Fts5Index *p, int nMerge){
  Fts5Structure *pStruct;

  pStruct = fts5StructureRead(p);
  if( pStruct && pStruct->nLevel ){
    fts5IndexMerge(p, &pStruct, nMerge);
    fts5StructureWrite(p, pStruct);
  }
  fts5StructureRelease(pStruct);

  return fts5IndexReturn(p);
}

static void fts5PoslistCallback(
  Fts5Index *p, 
  void *pCtx, 
  const u8 *pChunk, int nChunk
){
  fts5BufferAppendBlob(&p->rc, (Fts5Buffer*)pCtx, nChunk, pChunk);
}

/*
** Iterator pIter currently points to a valid entry (not EOF). This
** function appends the position list data for the current entry to
** buffer pBuf. It does not make a copy of the position-list size
** field.
*/
static void fts5SegiterPoslist(
  Fts5Index *p,
  Fts5SegIter *pSeg,
  Fts5Buffer *pBuf
){
  fts5ChunkIterate(p, pSeg, (void*)pBuf, fts5PoslistCallback);
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
  Fts5IndexIter *pMulti,
  int bSz,                        /* Append a size field before the data */
  Fts5Buffer *pBuf
){
  if( p->rc==SQLITE_OK ){
    Fts5SegIter *pSeg = &pMulti->aSeg[ pMulti->aFirst[1].iFirst ];
    assert( fts5MultiIterEof(p, pMulti)==0 );

    if( bSz ){
      /* WRITEPOSLISTSIZE */
      fts5BufferAppendVarint(&p->rc, pBuf, pSeg->nPos*2);
    }
    fts5SegiterPoslist(p, pSeg, pBuf);
  }
}

static void fts5DoclistIterNext(Fts5DoclistIter *pIter){
  if( pIter->i<pIter->n ){
    int bDummy;
    if( pIter->i ){
      i64 iDelta;
      pIter->i += fts5GetVarint(&pIter->a[pIter->i], (u64*)&iDelta);
      pIter->iRowid += iDelta;
    }else{
      pIter->i += fts5GetVarint(&pIter->a[pIter->i], (u64*)&pIter->iRowid);
    }
    pIter->i += fts5GetPoslistSize(
        &pIter->a[pIter->i], &pIter->nPoslist, &bDummy
    );
    pIter->aPoslist = &pIter->a[pIter->i];
    pIter->i += pIter->nPoslist;
  }else{
    pIter->aPoslist = 0;
  }
}

static void fts5DoclistIterInit(
  Fts5Buffer *pBuf, 
  Fts5DoclistIter *pIter
){
  memset(pIter, 0, sizeof(*pIter));
  pIter->a = pBuf->p;
  pIter->n = pBuf->n;
  fts5DoclistIterNext(pIter);
}

/*
** Append a doclist to buffer pBuf.
*/
static void fts5MergeAppendDocid(
  int *pRc,                       /* IN/OUT: Error code */
  Fts5Buffer *pBuf,               /* Buffer to write to */
  i64 *piLastRowid,               /* IN/OUT: Previous rowid written (if any) */
  i64 iRowid                      /* Rowid to append */
){
  if( pBuf->n==0 ){
    fts5BufferAppendVarint(pRc, pBuf, iRowid);
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

    fts5DoclistIterInit(p1, &i1);
    fts5DoclistIterInit(p2, &i2);
    while( p->rc==SQLITE_OK && (i1.aPoslist!=0 || i2.aPoslist!=0) ){
      if( i2.aPoslist==0 || (i1.aPoslist && i1.iRowid<i2.iRowid) ){
        /* Copy entry from i1 */
        fts5MergeAppendDocid(&p->rc, &out, &iLastRowid, i1.iRowid);
        /* WRITEPOSLISTSIZE */
        fts5BufferAppendVarint(&p->rc, &out, i1.nPoslist * 2);
        fts5BufferAppendBlob(&p->rc, &out, i1.nPoslist, i1.aPoslist);
        fts5DoclistIterNext(&i1);
      }
      else if( i1.aPoslist==0 || i2.iRowid!=i1.iRowid ){
        /* Copy entry from i2 */
        fts5MergeAppendDocid(&p->rc, &out, &iLastRowid, i2.iRowid);
        /* WRITEPOSLISTSIZE */
        fts5BufferAppendVarint(&p->rc, &out, i2.nPoslist * 2);
        fts5BufferAppendBlob(&p->rc, &out, i2.nPoslist, i2.aPoslist);
        fts5DoclistIterNext(&i2);
      }
      else{
        Fts5PoslistReader r1;
        Fts5PoslistReader r2;
        Fts5PoslistWriter writer;

        memset(&writer, 0, sizeof(writer));

        /* Merge the two position lists. */ 
        fts5MergeAppendDocid(&p->rc, &out, &iLastRowid, i2.iRowid);
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

        /* WRITEPOSLISTSIZE */
        fts5BufferAppendVarint(&p->rc, &out, tmp.n * 2);
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
  int bDesc,                      /* True for "ORDER BY rowid DESC" */
  const u8 *pToken,               /* Buffer containing prefix to match */
  int nToken,                     /* Size of buffer pToken in bytes */
  Fts5IndexIter **ppIter       /* OUT: New iterator */
){
  Fts5Structure *pStruct;
  Fts5Buffer *aBuf;
  const int nBuf = 32;

  aBuf = (Fts5Buffer*)fts5IdxMalloc(p, sizeof(Fts5Buffer)*nBuf);
  pStruct = fts5StructureRead(p);

  if( aBuf && pStruct ){
    const int flags = FTS5INDEX_QUERY_SCAN;
    int i;
    i64 iLastRowid = 0;
    Fts5IndexIter *p1 = 0;     /* Iterator used to gather data from index */
    Fts5Data *pData;
    Fts5Buffer doclist;

    memset(&doclist, 0, sizeof(doclist));
    for(fts5MultiIterNew(p, pStruct, 1, flags, pToken, nToken, -1, 0, &p1);
        fts5MultiIterEof(p, p1)==0;
        fts5MultiIterNext(p, p1, 0, 0)
    ){
      i64 iRowid = fts5MultiIterRowid(p1);
      int nTerm;
      const u8 *pTerm = fts5MultiIterTerm(p1, &nTerm);
      assert( memcmp(pToken, pTerm, MIN(nToken, nTerm))<=0 );
      if( nTerm<nToken || memcmp(pToken, pTerm, nToken) ) break;

      if( doclist.n>0 && iRowid<=iLastRowid ){
        for(i=0; p->rc==SQLITE_OK && doclist.n; i++){
          assert( i<nBuf );
          if( aBuf[i].n==0 ){
            fts5BufferSwap(&doclist, &aBuf[i]);
            fts5BufferZero(&doclist);
          }else{
            fts5MergePrefixLists(p, &doclist, &aBuf[i]);
            fts5BufferZero(&aBuf[i]);
          }
        }
      }

      fts5MergeAppendDocid(&p->rc, &doclist, &iLastRowid, iRowid);
      fts5MultiIterPoslist(p, p1, 1, &doclist);
    }

    for(i=0; i<nBuf; i++){
      fts5MergePrefixLists(p, &doclist, &aBuf[i]);
      fts5BufferFree(&aBuf[i]);
    }
    fts5MultiIterFree(p, p1);

    pData = fts5IdxMalloc(p, sizeof(Fts5Data) + doclist.n);
    if( pData ){
      pData->p = (u8*)&pData[1];
      pData->n = doclist.n;
      memcpy(pData->p, doclist.p, doclist.n);
      fts5MultiIterNew2(p, pData, bDesc, ppIter);
    }
    fts5BufferFree(&doclist);
  }

  fts5StructureRelease(pStruct);
  sqlite3_free(aBuf);
}


/*
** Indicate that all subsequent calls to sqlite3Fts5IndexWrite() pertain
** to the document with rowid iRowid.
*/
int sqlite3Fts5IndexBeginWrite(Fts5Index *p, i64 iRowid){
  assert( p->rc==SQLITE_OK );

  /* Allocate the hash table if it has not already been allocated */
  if( p->pHash==0 ){
    p->rc = sqlite3Fts5HashNew(&p->pHash, &p->nPendingData);
  }

  /* Flush the hash table to disk if required */
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
  Fts5Structure s;

  assert( p->rc==SQLITE_OK );
  p->rc = sqlite3Fts5IndexSetAverages(p, (const u8*)"", 0);

  memset(&s, 0, sizeof(Fts5Structure));
  fts5StructureWrite(p, &s);

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

  *pp = p = (Fts5Index*)sqlite3Fts5MallocZero(&rc, sizeof(Fts5Index));
  if( rc==SQLITE_OK ){
    p->pConfig = pConfig;
    p->nWorkUnit = FTS5_WORK_UNIT;
    p->nMaxPendingData = 1024*1024;
    p->zDataTbl = sqlite3Fts5Mprintf(&rc, "%s_data", pConfig->zName);
    if( p->zDataTbl && bCreate ){
      rc = sqlite3Fts5CreateTable(
          pConfig, "data", "id INTEGER PRIMARY KEY, block BLOB", 0, pzErr
      );
      if( rc==SQLITE_OK ){
        rc = sqlite3Fts5CreateTable(pConfig, "idx", 
            "segid, term, pgno, PRIMARY KEY(segid, term)", 
            1, pzErr
        );
      }
      if( rc==SQLITE_OK ){
        rc = sqlite3Fts5IndexReinit(p);
      }
    }
  }

  assert( rc!=SQLITE_OK || p->rc==SQLITE_OK );
  if( rc ){
    sqlite3Fts5IndexClose(p);
    *pp = 0;
  }
  return rc;
}

/*
** Close a handle opened by an earlier call to sqlite3Fts5IndexOpen().
*/
int sqlite3Fts5IndexClose(Fts5Index *p){
  int rc = SQLITE_OK;
  if( p ){
    assert( p->pReader==0 );
    sqlite3_finalize(p->pWriter);
    sqlite3_finalize(p->pDeleter);
    sqlite3_finalize(p->pIdxWriter);
    sqlite3_finalize(p->pIdxDeleter);
    sqlite3_finalize(p->pIdxSelect);
    sqlite3Fts5HashFree(p->pHash);
    sqlite3Fts5BufferFree(&p->scratch);
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
static int fts5IndexCharlen(const char *pIn, int nIn){
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
  int rc = SQLITE_OK;             /* Return code */
  Fts5Config *pConfig = p->pConfig;

  assert( p->rc==SQLITE_OK );

  /* Add the entry to the main terms index. */
  rc = sqlite3Fts5HashWrite(
      p->pHash, p->iWriteRowid, iCol, iPos, FTS5_MAIN_PREFIX, pToken, nToken
  );

  for(i=0; i<pConfig->nPrefix && rc==SQLITE_OK; i++){
    int nByte = fts5IndexCharlenToBytelen(pToken, nToken, pConfig->aPrefix[i]);
    if( nByte ){
      rc = sqlite3Fts5HashWrite(p->pHash, 
          p->iWriteRowid, iCol, iPos, FTS5_MAIN_PREFIX+i+1, pToken, nByte
      );
    }
  }

  return rc;
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
  Fts5Config *pConfig = p->pConfig;
  Fts5IndexIter *pRet = 0;
  int iIdx = 0;
  Fts5Buffer buf = {0, 0, 0};

  /* If the QUERY_SCAN flag is set, all other flags must be clear. */
  assert( (flags & FTS5INDEX_QUERY_SCAN)==0
       || (flags & FTS5INDEX_QUERY_SCAN)==FTS5INDEX_QUERY_SCAN
  );

  if( sqlite3Fts5BufferGrow(&p->rc, &buf, nToken+1)==0 ){
    memcpy(&buf.p[1], pToken, nToken);

#ifdef SQLITE_DEBUG
    if( flags & FTS5INDEX_QUERY_TEST_NOIDX ){
      assert( flags & FTS5INDEX_QUERY_PREFIX );
      iIdx = 1+pConfig->nPrefix;
    }else
#endif
    if( flags & FTS5INDEX_QUERY_PREFIX ){
      int nChar = fts5IndexCharlen(pToken, nToken);
      for(iIdx=1; iIdx<=pConfig->nPrefix; iIdx++){
        if( pConfig->aPrefix[iIdx-1]==nChar ) break;
      }
    }

    if( iIdx<=pConfig->nPrefix ){
      Fts5Structure *pStruct = fts5StructureRead(p);
      buf.p[0] = FTS5_MAIN_PREFIX + iIdx;
      if( pStruct ){
        fts5MultiIterNew(p, pStruct, 1, flags, buf.p, nToken+1, -1, 0, &pRet);
        fts5StructureRelease(pStruct);
      }
    }else{
      int bDesc = (flags & FTS5INDEX_QUERY_DESC)!=0;
      buf.p[0] = FTS5_MAIN_PREFIX;
      fts5SetupPrefixIter(p, bDesc, buf.p, nToken+1, &pRet);
    }

    if( p->rc ){
      sqlite3Fts5IterClose(pRet);
      pRet = 0;
      fts5CloseReader(p);
    }
    *ppIter = pRet;
    sqlite3Fts5BufferFree(&buf);
  }
  return fts5IndexReturn(p);
}

/*
** Return true if the iterator passed as the only argument is at EOF.
*/
int sqlite3Fts5IterEof(Fts5IndexIter *pIter){
  assert( pIter->pIndex->rc==SQLITE_OK );
  return pIter->bEof;
}

/*
** Move to the next matching rowid. 
*/
int sqlite3Fts5IterNext(Fts5IndexIter *pIter){
  assert( pIter->pIndex->rc==SQLITE_OK );
  fts5MultiIterNext(pIter->pIndex, pIter, 0, 0);
  return fts5IndexReturn(pIter->pIndex);
}

/*
** Move to the next matching term/rowid. Used by the fts5vocab module.
*/
int sqlite3Fts5IterNextScan(Fts5IndexIter *pIter){
  Fts5Index *p = pIter->pIndex;

  assert( pIter->pIndex->rc==SQLITE_OK );

  fts5MultiIterNext(p, pIter, 0, 0);
  if( p->rc==SQLITE_OK ){
    Fts5SegIter *pSeg = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
    if( pSeg->pLeaf && pSeg->term.p[0]!=FTS5_MAIN_PREFIX ){
      fts5DataRelease(pSeg->pLeaf);
      pSeg->pLeaf = 0;
      pIter->bEof = 1;
    }
  }

  return fts5IndexReturn(pIter->pIndex);
}

/*
** Move to the next matching rowid that occurs at or after iMatch. The
** definition of "at or after" depends on whether this iterator iterates
** in ascending or descending rowid order.
*/
int sqlite3Fts5IterNextFrom(Fts5IndexIter *pIter, i64 iMatch){
  fts5MultiIterNextFrom(pIter->pIndex, pIter, iMatch);
  return fts5IndexReturn(pIter->pIndex);
}

/*
** Return the current rowid.
*/
i64 sqlite3Fts5IterRowid(Fts5IndexIter *pIter){
  return fts5MultiIterRowid(pIter);
}

/*
** Return the current term.
*/
const char *sqlite3Fts5IterTerm(Fts5IndexIter *pIter, int *pn){
  int n;
  const char *z = (const char*)fts5MultiIterTerm(pIter, &n);
  *pn = n-1;
  return &z[1];
}


/*
** Return a pointer to a buffer containing a copy of the position list for
** the current entry. Output variable *pn is set to the size of the buffer 
** in bytes before returning.
**
** The returned position list does not include the "number of bytes" varint
** field that starts the position list on disk.
*/
int sqlite3Fts5IterPoslist(
  Fts5IndexIter *pIter, 
  const u8 **pp,                  /* OUT: Pointer to position-list data */
  int *pn,                        /* OUT: Size of position-list in bytes */
  i64 *piRowid                    /* OUT: Current rowid */
){
  Fts5SegIter *pSeg = &pIter->aSeg[ pIter->aFirst[1].iFirst ];
  assert( pIter->pIndex->rc==SQLITE_OK );
  *piRowid = pSeg->iRowid;
  *pn = pSeg->nPos;
  if( pSeg->iLeafOffset+pSeg->nPos <= pSeg->pLeaf->n ){
    *pp = &pSeg->pLeaf->p[pSeg->iLeafOffset];
  }else{
    fts5BufferZero(&pIter->poslist);
    fts5SegiterPoslist(pIter->pIndex, pSeg, &pIter->poslist);
    *pp = pIter->poslist.p;
  }
  return fts5IndexReturn(pIter->pIndex);
}

/*
** This function is similar to sqlite3Fts5IterPoslist(), except that it
** copies the position list into the buffer supplied as the second 
** argument.
*/
int sqlite3Fts5IterPoslistBuffer(Fts5IndexIter *pIter, Fts5Buffer *pBuf){
  Fts5Index *p = pIter->pIndex;

  assert( p->rc==SQLITE_OK );
  fts5BufferZero(pBuf);
  fts5MultiIterPoslist(p, pIter, 0, pBuf);
  return fts5IndexReturn(p);
}

/*
** Close an iterator opened by an earlier call to sqlite3Fts5IndexQuery().
*/
void sqlite3Fts5IterClose(Fts5IndexIter *pIter){
  if( pIter ){
    Fts5Index *pIndex = pIter->pIndex;
    fts5MultiIterFree(pIter->pIndex, pIter);
    fts5CloseReader(pIndex);
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
  int rc;                              /* Return code */
  Fts5Config *pConfig = p->pConfig;    /* Configuration object */
  u8 aCookie[4];                       /* Binary representation of iNew */
  sqlite3_blob *pBlob = 0;

  assert( p->rc==SQLITE_OK );
  sqlite3Fts5Put32(aCookie, iNew);

  rc = sqlite3_blob_open(pConfig->db, pConfig->zDb, p->zDataTbl, 
      "block", FTS5_STRUCTURE_ROWID, 1, &pBlob
  );
  if( rc==SQLITE_OK ){
    sqlite3_blob_write(pBlob, aCookie, 4, 0);
    rc = sqlite3_blob_close(pBlob);
  }

  return rc;
}

int sqlite3Fts5IndexLoadConfig(Fts5Index *p){
  Fts5Structure *pStruct;
  pStruct = fts5StructureRead(p);
  fts5StructureRelease(pStruct);
  return fts5IndexReturn(p);
}


/*************************************************************************
**************************************************************************
** Below this point is the implementation of the integrity-check 
** functionality.
*/

/*
** Return a simple checksum value based on the arguments.
*/
static u64 fts5IndexEntryCksum(
  i64 iRowid, 
  int iCol, 
  int iPos, 
  int iIdx,
  const char *pTerm,
  int nTerm
){
  int i;
  u64 ret = iRowid;
  ret += (ret<<3) + iCol;
  ret += (ret<<3) + iPos;
  if( iIdx>=0 ) ret += (ret<<3) + (FTS5_MAIN_PREFIX + iIdx);
  for(i=0; i<nTerm; i++) ret += (ret<<3) + pTerm[i];
  return ret;
}

#ifdef SQLITE_DEBUG
/*
** This function is purely an internal test. It does not contribute to 
** FTS functionality, or even the integrity-check, in any way.
**
** Instead, it tests that the same set of pgno/rowid combinations are 
** visited regardless of whether the doclist-index identified by parameters
** iSegid/iLeaf is iterated in forwards or reverse order.
*/
static void fts5TestDlidxReverse(
  Fts5Index *p, 
  int iSegid,                     /* Segment id to load from */
  int iLeaf                       /* Load doclist-index for this leaf */
){
  Fts5DlidxIter *pDlidx = 0;
  u64 cksum1 = 13;
  u64 cksum2 = 13;

  for(pDlidx=fts5DlidxIterInit(p, 0, iSegid, iLeaf);
      fts5DlidxIterEof(p, pDlidx)==0;
      fts5DlidxIterNext(p, pDlidx)
  ){
    i64 iRowid = fts5DlidxIterRowid(pDlidx);
    int pgno = fts5DlidxIterPgno(pDlidx);
    assert( pgno>iLeaf );
    cksum1 += iRowid + ((i64)pgno<<32);
  }
  fts5DlidxIterFree(pDlidx);
  pDlidx = 0;

  for(pDlidx=fts5DlidxIterInit(p, 1, iSegid, iLeaf);
      fts5DlidxIterEof(p, pDlidx)==0;
      fts5DlidxIterPrev(p, pDlidx)
  ){
    i64 iRowid = fts5DlidxIterRowid(pDlidx);
    int pgno = fts5DlidxIterPgno(pDlidx);
    assert( fts5DlidxIterPgno(pDlidx)>iLeaf );
    cksum2 += iRowid + ((i64)pgno<<32);
  }
  fts5DlidxIterFree(pDlidx);
  pDlidx = 0;

  if( p->rc==SQLITE_OK && cksum1!=cksum2 ) p->rc = FTS5_CORRUPT;
}

static int fts5QueryCksum(
  Fts5Index *p,                   /* Fts5 index object */
  int iIdx,
  const char *z,                  /* Index key to query for */
  int n,                          /* Size of index key in bytes */
  int flags,                      /* Flags for Fts5IndexQuery */
  u64 *pCksum                     /* IN/OUT: Checksum value */
){
  u64 cksum = *pCksum;
  Fts5IndexIter *pIdxIter = 0;
  int rc = sqlite3Fts5IndexQuery(p, z, n, flags, &pIdxIter);

  while( rc==SQLITE_OK && 0==sqlite3Fts5IterEof(pIdxIter) ){
    i64 dummy;
    const u8 *pPos;
    int nPos;
    i64 rowid = sqlite3Fts5IterRowid(pIdxIter);
    rc = sqlite3Fts5IterPoslist(pIdxIter, &pPos, &nPos, &dummy);
    if( rc==SQLITE_OK ){
      Fts5PoslistReader sReader;
      for(sqlite3Fts5PoslistReaderInit(-1, pPos, nPos, &sReader);
          sReader.bEof==0;
          sqlite3Fts5PoslistReaderNext(&sReader)
      ){
        int iCol = FTS5_POS2COLUMN(sReader.iPos);
        int iOff = FTS5_POS2OFFSET(sReader.iPos);
        cksum ^= fts5IndexEntryCksum(rowid, iCol, iOff, iIdx, z, n);
      }
      rc = sqlite3Fts5IterNext(pIdxIter);
    }
  }
  sqlite3Fts5IterClose(pIdxIter);

  *pCksum = cksum;
  return rc;
}


/*
** This function is also purely an internal test. It does not contribute to 
** FTS functionality, or even the integrity-check, in any way.
*/
static void fts5TestTerm(
  Fts5Index *p, 
  Fts5Buffer *pPrev,              /* Previous term */
  const char *z, int n,           /* Possibly new term to test */
  u64 expected,
  u64 *pCksum
){
  int rc = p->rc;
  if( pPrev->n==0 ){
    fts5BufferSet(&rc, pPrev, n, (const u8*)z);
  }else
  if( rc==SQLITE_OK && (pPrev->n!=n || memcmp(pPrev->p, z, n)) ){
    u64 cksum3 = *pCksum;
    const char *zTerm = (const char*)&pPrev->p[1];  /* term sans prefix-byte */
    int nTerm = pPrev->n-1;            /* Size of zTerm in bytes */
    int iIdx = (pPrev->p[0] - FTS5_MAIN_PREFIX);
    int flags = (iIdx==0 ? 0 : FTS5INDEX_QUERY_PREFIX);
    u64 ck1 = 0;
    u64 ck2 = 0;

    /* Check that the results returned for ASC and DESC queries are
    ** the same. If not, call this corruption.  */
    rc = fts5QueryCksum(p, iIdx, zTerm, nTerm, flags, &ck1);
    if( rc==SQLITE_OK ){
      int f = flags|FTS5INDEX_QUERY_DESC;
      rc = fts5QueryCksum(p, iIdx, zTerm, nTerm, f, &ck2);
    }
    if( rc==SQLITE_OK && ck1!=ck2 ) rc = FTS5_CORRUPT;

    /* If this is a prefix query, check that the results returned if the
    ** the index is disabled are the same. In both ASC and DESC order. */
    if( iIdx>0 && rc==SQLITE_OK ){
      int f = flags|FTS5INDEX_QUERY_TEST_NOIDX;
      ck2 = 0;
      rc = fts5QueryCksum(p, iIdx, zTerm, nTerm, f, &ck2);
      if( rc==SQLITE_OK && ck1!=ck2 ) rc = FTS5_CORRUPT;
    }
    if( iIdx>0 && rc==SQLITE_OK ){
      int f = flags|FTS5INDEX_QUERY_TEST_NOIDX|FTS5INDEX_QUERY_DESC;
      ck2 = 0;
      rc = fts5QueryCksum(p, iIdx, zTerm, nTerm, f, &ck2);
      if( rc==SQLITE_OK && ck1!=ck2 ) rc = FTS5_CORRUPT;
    }

    cksum3 ^= ck1;
    fts5BufferSet(&rc, pPrev, n, (const u8*)z);

    if( rc==SQLITE_OK && cksum3!=expected ){
      rc = FTS5_CORRUPT;
    }
    *pCksum = cksum3;
  }
  p->rc = rc;
}
 
#else
# define fts5TestDlidxReverse(x,y,z)
# define fts5TestTerm(u,v,w,x,y,z)
#endif

/*
** Check that:
**
**   1) All leaves of pSeg between iFirst and iLast (inclusive) exist and
**      contain zero terms.
**   2) All leaves of pSeg between iNoRowid and iLast (inclusive) exist and
**      contain zero rowids.
*/
static void fts5IndexIntegrityCheckEmpty(
  Fts5Index *p,
  Fts5StructureSegment *pSeg,     /* Segment to check internal consistency */
  int iFirst,
  int iNoRowid,
  int iLast
){
  int i;

  /* Now check that the iter.nEmpty leaves following the current leaf
  ** (a) exist and (b) contain no terms. */
  for(i=iFirst; p->rc==SQLITE_OK && i<=iLast; i++){
    Fts5Data *pLeaf = fts5DataRead(p, FTS5_SEGMENT_ROWID(pSeg->iSegid, 0, i));
    if( pLeaf ){
      if( 0!=fts5GetU16(&pLeaf->p[2]) ) p->rc = FTS5_CORRUPT;
      if( i>=iNoRowid && 0!=fts5GetU16(&pLeaf->p[0]) ) p->rc = FTS5_CORRUPT;
    }
    fts5DataRelease(pLeaf);
    if( p->rc ) break;
  }
}

static void fts5IndexIntegrityCheckSegment(
  Fts5Index *p,                   /* FTS5 backend object */
  Fts5StructureSegment *pSeg      /* Segment to check internal consistency */
){
  Fts5Config *pConfig = p->pConfig;
  sqlite3_stmt *pStmt = 0;
  int rc2;
  int iIdxPrevLeaf = pSeg->pgnoFirst-1;
  int iDlidxPrevLeaf = pSeg->pgnoLast;

  if( pSeg->pgnoFirst==0 ) return;

  fts5IndexPrepareStmt(p, &pStmt, sqlite3_mprintf(
      "SELECT segid, term, (pgno>>1), (pgno & 1) FROM '%q'.'%q_idx' WHERE segid=%d",
      pConfig->zDb, pConfig->zName, pSeg->iSegid
  ));

  /* Iterate through the b-tree hierarchy.  */
  while( p->rc==SQLITE_OK && SQLITE_ROW==sqlite3_step(pStmt) ){
    i64 iRow;                     /* Rowid for this leaf */
    Fts5Data *pLeaf;              /* Data for this leaf */
    int iOff;                     /* Offset of first term on leaf */
    int i;                        /* Used to iterate through empty leaves */

    int nIdxTerm = sqlite3_column_bytes(pStmt, 1);
    const char *zIdxTerm = (const char*)sqlite3_column_text(pStmt, 1);
    int iIdxLeaf = sqlite3_column_int(pStmt, 2);
    int bIdxDlidx = sqlite3_column_int(pStmt, 3);

    /* If the leaf in question has already been trimmed from the segment, 
    ** ignore this b-tree entry. Otherwise, load it into memory. */
    if( iIdxLeaf<pSeg->pgnoFirst ) continue;
    iRow = FTS5_SEGMENT_ROWID(pSeg->iSegid, 0, iIdxLeaf);
    pLeaf = fts5DataRead(p, iRow);
    if( pLeaf==0 ) break;

    /* Check that the leaf contains at least one term, and that it is equal
    ** to or larger than the split-key in zIdxTerm.  Also check that if there
    ** is also a rowid pointer within the leaf page header, it points to a
    ** location before the term.  */
    iOff = fts5GetU16(&pLeaf->p[2]);
    if( iOff==0 ){
      p->rc = FTS5_CORRUPT;
    }else{
      int iRowidOff;
      int nTerm;                  /* Size of term on leaf in bytes */
      int res;                    /* Comparison of term and split-key */

      iRowidOff = fts5GetU16(&pLeaf->p[0]);
      if( iRowidOff>=iOff ){
        p->rc = FTS5_CORRUPT;
      }else{
        iOff += fts5GetVarint32(&pLeaf->p[iOff], nTerm);
        res = memcmp(&pLeaf->p[iOff], zIdxTerm, MIN(nTerm, nIdxTerm));
        if( res==0 ) res = nTerm - nIdxTerm;
        if( res<0 ) p->rc = FTS5_CORRUPT;
      }
    }
    fts5DataRelease(pLeaf);
    if( p->rc ) break;


    /* Now check that the iter.nEmpty leaves following the current leaf
    ** (a) exist and (b) contain no terms. */
    fts5IndexIntegrityCheckEmpty(
        p, pSeg, iIdxPrevLeaf+1, iDlidxPrevLeaf+1, iIdxLeaf-1
    );
    if( p->rc ) break;

    /* If there is a doclist-index, check that it looks right. */
    if( bIdxDlidx ){
      Fts5DlidxIter *pDlidx = 0;  /* For iterating through doclist index */
      int iPrevLeaf = iIdxLeaf;
      int iSegid = pSeg->iSegid;
      int iPg;
      i64 iKey;

      for(pDlidx=fts5DlidxIterInit(p, 0, iSegid, iIdxLeaf);
          fts5DlidxIterEof(p, pDlidx)==0;
          fts5DlidxIterNext(p, pDlidx)
      ){

        /* Check any rowid-less pages that occur before the current leaf. */
        for(iPg=iPrevLeaf+1; iPg<fts5DlidxIterPgno(pDlidx); iPg++){
          iKey = FTS5_SEGMENT_ROWID(iSegid, 0, iPg);
          pLeaf = fts5DataRead(p, iKey);
          if( pLeaf ){
            if( fts5GetU16(&pLeaf->p[0])!=0 ) p->rc = FTS5_CORRUPT;
            fts5DataRelease(pLeaf);
          }
        }
        iPrevLeaf = fts5DlidxIterPgno(pDlidx);

        /* Check that the leaf page indicated by the iterator really does
        ** contain the rowid suggested by the same. */
        iKey = FTS5_SEGMENT_ROWID(iSegid, 0, iPrevLeaf);
        pLeaf = fts5DataRead(p, iKey);
        if( pLeaf ){
          i64 iRowid;
          int iRowidOff = fts5GetU16(&pLeaf->p[0]);
          if( iRowidOff>=pLeaf->n ){
            p->rc = FTS5_CORRUPT;
          }else{
            fts5GetVarint(&pLeaf->p[iRowidOff], (u64*)&iRowid);
            if( iRowid!=fts5DlidxIterRowid(pDlidx) ) p->rc = FTS5_CORRUPT;
          }
          fts5DataRelease(pLeaf);
        }
      }

      iDlidxPrevLeaf = iPg;
      fts5DlidxIterFree(pDlidx);
      fts5TestDlidxReverse(p, iSegid, iIdxLeaf);
    }else{
      iDlidxPrevLeaf = pSeg->pgnoLast;
      /* TODO: Check there is no doclist index */
    }

    iIdxPrevLeaf = iIdxLeaf;
  }

  rc2 = sqlite3_finalize(pStmt);
  if( p->rc==SQLITE_OK ) p->rc = rc2;

  /* Page iter.iLeaf must now be the rightmost leaf-page in the segment */
#if 0
  if( p->rc==SQLITE_OK && iter.iLeaf!=pSeg->pgnoLast ){
    p->rc = FTS5_CORRUPT;
  }
#endif
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
  u64 cksum2 = 0;                 /* Checksum based on contents of indexes */
  Fts5Buffer poslist = {0,0,0};   /* Buffer used to hold a poslist */
  Fts5IndexIter *pIter;           /* Used to iterate through entire index */
  Fts5Structure *pStruct;         /* Index structure */

  /* Used by extra internal tests only run if NDEBUG is not defined */
  u64 cksum3 = 0;                 /* Checksum based on contents of indexes */
  Fts5Buffer term = {0,0,0};      /* Buffer used to hold most recent term */
  
  /* Load the FTS index structure */
  pStruct = fts5StructureRead(p);

  /* Check that the internal nodes of each segment match the leaves */
  if( pStruct ){
    int iLvl, iSeg;
    for(iLvl=0; iLvl<pStruct->nLevel; iLvl++){
      for(iSeg=0; iSeg<pStruct->aLevel[iLvl].nSeg; iSeg++){
        Fts5StructureSegment *pSeg = &pStruct->aLevel[iLvl].aSeg[iSeg];
        fts5IndexIntegrityCheckSegment(p, pSeg);
      }
    }
  }

  /* The cksum argument passed to this function is a checksum calculated
  ** based on all expected entries in the FTS index (including prefix index
  ** entries). This block checks that a checksum calculated based on the
  ** actual contents of FTS index is identical.
  **
  ** Two versions of the same checksum are calculated. The first (stack
  ** variable cksum2) based on entries extracted from the full-text index
  ** while doing a linear scan of each individual index in turn. 
  **
  ** As each term visited by the linear scans, a separate query for the
  ** same term is performed. cksum3 is calculated based on the entries
  ** extracted by these queries.
  */
  for(fts5MultiIterNew(p, pStruct, 0, 0, 0, 0, -1, 0, &pIter);
      fts5MultiIterEof(p, pIter)==0;
      fts5MultiIterNext(p, pIter, 0, 0)
  ){
    int n;                      /* Size of term in bytes */
    i64 iPos = 0;               /* Position read from poslist */
    int iOff = 0;               /* Offset within poslist */
    i64 iRowid = fts5MultiIterRowid(pIter);
    char *z = (char*)fts5MultiIterTerm(pIter, &n);

    /* If this is a new term, query for it. Update cksum3 with the results. */
    fts5TestTerm(p, &term, z, n, cksum2, &cksum3);

    poslist.n = 0;
    fts5MultiIterPoslist(p, pIter, 0, &poslist);
    while( 0==sqlite3Fts5PoslistNext64(poslist.p, poslist.n, &iOff, &iPos) ){
      int iCol = FTS5_POS2COLUMN(iPos);
      int iTokOff = FTS5_POS2OFFSET(iPos);
      cksum2 ^= fts5IndexEntryCksum(iRowid, iCol, iTokOff, -1, z, n);
    }
  }
  fts5TestTerm(p, &term, 0, 0, cksum2, &cksum3);

  fts5MultiIterFree(p, pIter);
  if( p->rc==SQLITE_OK && cksum!=cksum2 ) p->rc = FTS5_CORRUPT;

  fts5StructureRelease(pStruct);
  fts5BufferFree(&term);
  fts5BufferFree(&poslist);
  return fts5IndexReturn(p);
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

  ret = fts5IndexEntryCksum(iRowid, iCol, iPos, 0, pTerm, nTerm);

  for(iIdx=0; iIdx<pConfig->nPrefix; iIdx++){
    int nByte = fts5IndexCharlenToBytelen(pTerm, nTerm, pConfig->aPrefix[iIdx]);
    if( nByte ){
      ret ^= fts5IndexEntryCksum(iRowid, iCol, iPos, iIdx+1, pTerm, nByte);
    }
  }

  return ret;
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
  int *piSegid,                   /* OUT: Segment id */
  int *pbDlidx,                   /* OUT: Dlidx flag */
  int *piHeight,                  /* OUT: Height */
  int *piPgno                     /* OUT: Page number */
){
  *piPgno = (int)(iRowid & (((i64)1 << FTS5_DATA_PAGE_B) - 1));
  iRowid >>= FTS5_DATA_PAGE_B;

  *piHeight = (int)(iRowid & (((i64)1 << FTS5_DATA_HEIGHT_B) - 1));
  iRowid >>= FTS5_DATA_HEIGHT_B;

  *pbDlidx = (int)(iRowid & 0x0001);
  iRowid >>= FTS5_DATA_DLI_B;

  *piSegid = (int)(iRowid & (((i64)1 << FTS5_DATA_ID_B) - 1));
}

static void fts5DebugRowid(int *pRc, Fts5Buffer *pBuf, i64 iKey){
  int iSegid, iHeight, iPgno, bDlidx;       /* Rowid compenents */
  fts5DecodeRowid(iKey, &iSegid, &bDlidx, &iHeight, &iPgno);

  if( iSegid==0 ){
    if( iKey==FTS5_AVERAGES_ROWID ){
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(averages) ");
    }else{
      sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(structure)");
    }
  }
  else{
    sqlite3Fts5BufferAppendPrintf(pRc, pBuf, "(%ssegid=%d h=%d pgno=%d)",
        bDlidx ? "dlidx " : "", iSegid, iHeight, iPgno
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
        " {lvl=%d nMerge=%d nSeg=%d", iLvl, pLvl->nMerge, pLvl->nSeg
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
    iOff += fts5GetVarint32(&a[iOff], iVal);
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

  iOff = sqlite3Fts5GetVarint(&a[iOff], (u64*)&iDocid);
  sqlite3Fts5BufferAppendPrintf(pRc, pBuf, " rowid=%lld", iDocid);
  while( iOff<n ){
    int nPos;
    int bDummy;
    iOff += fts5GetPoslistSize(&a[iOff], &nPos, &bDummy);
    iOff += fts5DecodePoslist(pRc, pBuf, &a[iOff], MIN(n-iOff, nPos));
    if( iOff<n ){
      i64 iDelta;
      iOff += sqlite3Fts5GetVarint(&a[iOff], (u64*)&iDelta);
      if( iDelta==0 ) return iOff;
      iDocid += iDelta;
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
  int iSegid,iHeight,iPgno,bDlidx;/* Rowid components */
  const u8 *aBlob; int n;         /* Record to decode */
  u8 *a = 0;
  Fts5Buffer s;                   /* Build up text to return here */
  int rc = SQLITE_OK;             /* Return code */
  int nSpace = 0;

  assert( nArg==2 );
  memset(&s, 0, sizeof(Fts5Buffer));
  iRowid = sqlite3_value_int64(apVal[0]);
  n = sqlite3_value_bytes(apVal[1]);
  aBlob = sqlite3_value_blob(apVal[1]);

  nSpace = n + FTS5_DATA_ZERO_PADDING;
  a = (u8*)sqlite3Fts5MallocZero(&rc, nSpace);
  if( a==0 ) goto decode_out;
  memcpy(a, aBlob, n);
  fts5DecodeRowid(iRowid, &iSegid, &bDlidx, &iHeight, &iPgno);

  fts5DebugRowid(&rc, &s, iRowid);
  if( bDlidx ){
    Fts5Data dlidx;
    Fts5DlidxLvl lvl;

    dlidx.p = a;
    dlidx.n = n;

    memset(&lvl, 0, sizeof(Fts5DlidxLvl));
    lvl.pData = &dlidx;
    lvl.iLeafPgno = iPgno;

    for(fts5DlidxLvlNext(&lvl); lvl.bEof==0; fts5DlidxLvlNext(&lvl)){
      sqlite3Fts5BufferAppendPrintf(&rc, &s, 
          " %d(%lld)", lvl.iLeafPgno, lvl.iRowid
      );
    }
  }else if( iSegid==0 ){
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

      if( n>=4 ){
        iRowidOff = fts5GetU16(&a[0]);
        iTermOff = fts5GetU16(&a[2]);
      }else{
        sqlite3Fts5BufferSet(&rc, &s, 8, (const u8*)"corrupt");
        goto decode_out;
      }

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
        iOff += fts5GetVarint32(&a[iOff], nByte);
        term.n= nKeep;
        fts5BufferAppendBlob(&rc, &term, nByte, &a[iOff]);
        iOff += nByte;

        sqlite3Fts5BufferAppendPrintf(
            &rc, &s, " term=%.*s", term.n, (const char*)term.p
        );
        iOff += fts5DecodeDoclist(&rc, &s, &a[iOff], n-iOff);
        if( iOff<n ){
          iOff += fts5GetVarint32(&a[iOff], nKeep);
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
  
 decode_out:
  sqlite3_free(a);
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
      int segid, height, pgno;
      if( nArg!=4 ){
        sqlite3_result_error(pCtx, 
            "should be: fts5_rowid('segment', segid, height, pgno))", -1
        );
      }else{
        segid = sqlite3_value_int(apVal[1]);
        height = sqlite3_value_int(apVal[2]);
        pgno = sqlite3_value_int(apVal[3]);
        iRowid = FTS5_SEGMENT_ROWID(segid, height, pgno);
        sqlite3_result_int64(pCtx, iRowid);
      }
    }else {
      sqlite3_result_error(pCtx, 
        "first arg to fts5_rowid() must be 'segment' "
        "or 'start-of-index'"
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

