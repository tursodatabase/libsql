/* The author disclaims copyright to this source code.
 *
 * This is an SQLite module implementing full-text search.
 */

/*
** The code in this file is only compiled if:
**
**     * The FTS1 module is being built as an extension
**       (in which case SQLITE_CORE is not defined), or
**
**     * The FTS1 module is being built into the core of
**       SQLite (in which case SQLITE_ENABLE_FTS1 is defined).
*/
#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_FTS1)

#include <assert.h>
#if !defined(__APPLE__)
#include <malloc.h>
#else
#include <stdlib.h>
#endif
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include "fts1.h"
#include "fts1_hash.h"
#include "fts1_tokenizer.h"
#include "sqlite3.h"
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1


#if 0
# define TRACE(A)  printf A; fflush(stdout)
#else
# define TRACE(A)
#endif

/* utility functions */

/* We encode variable-length integers in little-endian order using seven bits
 * per byte as follows:
**
** KEY:
**         A = 0xxxxxxx    7 bits of data and one flag bit
**         B = 1xxxxxxx    7 bits of data and one flag bit
**
**  7 bits - A
** 14 bits - BA
** 21 bits - BBA
** and so on.
*/

/* We may need up to VARINT_MAX bytes to store an encoded 64-bit integer. */
#define VARINT_MAX 10

/* Write a 64-bit variable-length integer to memory starting at p[0].
 * The length of data written will be between 1 and VARINT_MAX bytes.
 * The number of bytes written is returned. */
static int putVarint(char *p, sqlite_int64 v){
  unsigned char *q = (unsigned char *) p;
  sqlite_uint64 vu = v;
  do{
    *q++ = (unsigned char) ((vu & 0x7f) | 0x80);
    vu >>= 7;
  }while( vu!=0 );
  q[-1] &= 0x7f;  /* turn off high bit in final byte */
  assert( q - (unsigned char *)p <= VARINT_MAX );
  return (int) (q - (unsigned char *)p);
}

/* Read a 64-bit variable-length integer from memory starting at p[0].
 * Return the number of bytes read, or 0 on error.
 * The value is stored in *v. */
static int getVarint(const char *p, sqlite_int64 *v){
  const unsigned char *q = (const unsigned char *) p;
  sqlite_uint64 x = 0, y = 1;
  while( (*q & 0x80) == 0x80 ){
    x += y * (*q++ & 0x7f);
    y <<= 7;
    if( q - (unsigned char *)p >= VARINT_MAX ){  /* bad data */
      assert( 0 );
      return 0;
    }
  }
  x += y * (*q++);
  *v = (sqlite_int64) x;
  return (int) (q - (unsigned char *)p);
}

static int getVarint32(const char *p, int *pi){
 sqlite_int64 i;
 int ret = getVarint(p, &i);
 *pi = (int) i;
 assert( *pi==i );
 return ret;
}

/*** Document lists ***
 *
 * A document list holds a sorted list of varint-encoded document IDs.
 *
 * A doclist with type DL_POSITIONS_OFFSETS is stored like this:
 *
 * array {
 *   varint docid;
 *   array {
 *     varint position;     (delta from previous position plus 1, or 0 for end)
 *     varint startOffset;  (delta from previous startOffset)
 *     varint endOffset;    (delta from startOffset)
 *   }
 * }
 *
 * Here, array { X } means zero or more occurrences of X, adjacent in memory.
 *
 * A doclist with type DL_POSITIONS is like the above, but holds only docids
 * and positions without offset information.
 *
 * A doclist with type DL_DOCIDS is like the above, but holds only docids
 * without positions or offset information.
 *
 * On disk, every document list has positions and offsets, so we don't bother
 * to serialize a doclist's type.
 * 
 * We don't yet delta-encode document IDs; doing so will probably be a
 * modest win.
 *
 * NOTE(shess) I've thought of a slightly (1%) better offset encoding.
 * After the first offset, estimate the next offset by using the
 * current token position and the previous token position and offset,
 * offset to handle some variance.  So the estimate would be
 * (iPosition*w->iStartOffset/w->iPosition-64), which is delta-encoded
 * as normal.  Offsets more than 64 chars from the estimate are
 * encoded as the delta to the previous start offset + 128.  An
 * additional tiny increment can be gained by using the end offset of
 * the previous token to make the estimate a tiny bit more precise.
*/

typedef enum DocListType {
  DL_DOCIDS,              /* docids only */
  DL_POSITIONS,           /* docids + positions */
  DL_POSITIONS_OFFSETS    /* docids + positions + offsets */
} DocListType;

typedef struct DocList {
  char *pData;
  int nData;
  DocListType iType;
  int iLastPos;       /* the last position written */
  int iLastOffset;    /* the last start offset written */
} DocList;

/* Initialize a new DocList to hold the given data. */
static void docListInit(DocList *d, DocListType iType,
                        const char *pData, int nData){
  d->nData = nData;
  if( nData>0 ){
    d->pData = malloc(nData);
    memcpy(d->pData, pData, nData);
  } else {
    d->pData = NULL;
  }
  d->iType = iType;
  d->iLastPos = 0;
  d->iLastOffset = 0;
}

/* Create a new dynamically-allocated DocList. */
static DocList *docListNew(DocListType iType){
  DocList *d = (DocList *) malloc(sizeof(DocList));
  docListInit(d, iType, 0, 0);
  return d;
}

static void docListDestroy(DocList *d){
  free(d->pData);
#ifndef NDEBUG
  memset(d, 0x55, sizeof(*d));
#endif
}

static void docListDelete(DocList *d){
  docListDestroy(d);
  free(d);
}

static char *docListEnd(DocList *d){
  return d->pData + d->nData;
}

/* Append a varint to a DocList's data. */
static void appendVarint(DocList *d, sqlite_int64 i){
  char c[VARINT_MAX];
  int n = putVarint(c, i);
  d->pData = realloc(d->pData, d->nData + n);
  memcpy(d->pData + d->nData, c, n);
  d->nData += n;
}

static void docListAddDocid(DocList *d, sqlite_int64 iDocid){
  appendVarint(d, iDocid);
  if( d->iType>=DL_POSITIONS ){
    appendVarint(d, 0);  /* initially empty position list */
    d->iLastPos = 0;
  }
}

/* helper function for docListAddPos and docListAddPosOffset */
static void addPos(DocList *d, int iPos) {
  appendVarint(d, iPos-d->iLastPos+1);
  d->iLastPos = iPos;
}

/* Add a position to the last position list in a doclist. */
static void docListAddPos(DocList *d, int iPos){
  assert( d->iType==DL_POSITIONS );
  assert( d->nData>0 );
  --d->nData;  /* remove previous terminator */
  addPos(d, iPos);
  appendVarint(d, 0);  /* add new terminator */
}

static void docListAddPosOffset(DocList *d, int iPos,
                                int iStartOffset, int iEndOffset){
  assert( d->iType==DL_POSITIONS_OFFSETS );
  assert( d->nData>0 );
  --d->nData;  /* remove previous terminator */
  addPos(d, iPos);
  appendVarint(d, iStartOffset-d->iLastOffset);
  d->iLastOffset = iStartOffset;
  appendVarint(d, iEndOffset-iStartOffset);
  appendVarint(d, 0);  /* add new terminator */
}

/*
** A DocListReader object is a cursor into a doclist.  Initialize
** the cursor to the beginning of the doclist by calling readerInit().
** Then use routines
**
**      peekDocid()
**      readDocid()
**      readPosition()
**      skipPositionList()
**      and so forth...
**
** to read information out of the doclist.  When we reach the end
** of the doclist, atEnd() returns TRUE.
*/
typedef struct DocListReader {
  DocList *pDoclist;  /* The document list we are stepping through */
  char *p;            /* Pointer to next unread byte in the doclist */
  int iLastPos;  /* the last position read, or -1 when not in a position list */
} DocListReader;

/*
** Initialize the DocListReader r to point to the beginning of pDoclist.
*/
static void readerInit(DocListReader *r, DocList *pDoclist){
  r->pDoclist = pDoclist;
  if( pDoclist!=NULL ){
    r->p = pDoclist->pData;
  }
  r->iLastPos = -1;
}

/*
** Return TRUE if we have reached then end of pReader and there is
** nothing else left to read.
*/
static int atEnd(DocListReader *pReader){
  return pReader->pDoclist==0 || (pReader->p >= docListEnd(pReader->pDoclist));
}

/* Peek at the next docid without advancing the read pointer. 
*/
static sqlite_int64 peekDocid(DocListReader *pReader){
  sqlite_int64 ret;
  assert( !atEnd(pReader) );
  assert( pReader->iLastPos==-1 );
  getVarint(pReader->p, &ret);
  return ret;
}

/* Read the next docid.   See also nextValidDocid().
*/
static sqlite_int64 readDocid(DocListReader *pReader){
  sqlite_int64 ret;
  assert( !atEnd(pReader) );
  assert( pReader->iLastPos==-1 );
  pReader->p += getVarint(pReader->p, &ret);
  if( pReader->pDoclist->iType>=DL_POSITIONS ){
    pReader->iLastPos = 0;
  }
  return ret;
}

/* Read the next position from a position list.
 * Returns the position, or -1 at the end of the list. */
static int readPosition(DocListReader *pReader){
  int i;
  int iType = pReader->pDoclist->iType;

  if( pReader->iLastPos==-1 ){
    return -1;
  }
  assert( !atEnd(pReader) );

  if( iType<DL_POSITIONS ){
    return -1;
  }
  pReader->p += getVarint32(pReader->p, &i);
  if( i==0 ){
    pReader->iLastPos = -1;
    return -1;
  }
  pReader->iLastPos += ((int) i)-1;
  if( iType>=DL_POSITIONS_OFFSETS ){
    /* Skip over offsets, ignoring them for now. */
    int iStart, iEnd;
    pReader->p += getVarint32(pReader->p, &iStart);
    pReader->p += getVarint32(pReader->p, &iEnd);
  }
  return pReader->iLastPos;
}

/* Skip past the end of a position list. */
static void skipPositionList(DocListReader *pReader){
  DocList *p = pReader->pDoclist;
  if( p && p->iType>=DL_POSITIONS ){
    while( readPosition(pReader)!=-1 ){}
  }
}

/* Skip over a docid, including its position list if the doclist has
 * positions. */
static void skipDocument(DocListReader *pReader){
  readDocid(pReader);
  skipPositionList(pReader);
}

/* Skip past all docids which are less than [iDocid].  Returns 1 if a docid
 * matching [iDocid] was found.  */
static int skipToDocid(DocListReader *pReader, sqlite_int64 iDocid){
  sqlite_int64 d = 0;
  while( !atEnd(pReader) && (d=peekDocid(pReader))<iDocid ){
    skipDocument(pReader);
  }
  return !atEnd(pReader) && d==iDocid;
}

/* Return the first document in a document list.
*/
static sqlite_int64 firstDocid(DocList *d){
  DocListReader r;
  readerInit(&r, d);
  return readDocid(&r);
}

#ifdef SQLITE_DEBUG
/*
** This routine is used for debugging purpose only.
**
** Write the content of a doclist to standard output.
*/
static void printDoclist(DocList *p){
  DocListReader r;
  const char *zSep = "";

  readerInit(&r, p);
  while( !atEnd(&r) ){
    sqlite_int64 docid = readDocid(&r);
    if( docid==0 ){
      skipPositionList(&r);
      continue;
    }
    printf("%s%lld", zSep, docid);
    zSep =  ",";
    if( p->iType>=DL_POSITIONS ){
      int iPos;
      const char *zDiv = "";
      printf("(");
      while( (iPos = readPosition(&r))>=0 ){
        printf("%s%d", zDiv, iPos);
        zDiv = ":";
      }
      printf(")");
    }
  }
  printf("\n");
  fflush(stdout);
}
#endif /* SQLITE_DEBUG */

/* Helper function for docListUpdate() and docListAccumulate().
** Splices a doclist element into the doclist represented by r,
** leaving r pointing after the newly spliced element.
*/
static void docListSpliceElement(DocListReader *r, sqlite_int64 iDocid,
                                 const char *pSource, int nSource){
  DocList *d = r->pDoclist;
  char *pTarget;
  int nTarget, found;

  found = skipToDocid(r, iDocid);

  /* Describe slice in d to place pSource/nSource. */
  pTarget = r->p;
  if( found ){
    skipDocument(r);
    nTarget = r->p-pTarget;
  }else{
    nTarget = 0;
  }

  /* The sense of the following is that there are three possibilities.
  ** If nTarget==nSource, we should not move any memory nor realloc.
  ** If nTarget>nSource, trim target and realloc.
  ** If nTarget<nSource, realloc then expand target.
  */
  if( nTarget>nSource ){
    memmove(pTarget+nSource, pTarget+nTarget, docListEnd(d)-(pTarget+nTarget));
  }
  if( nTarget!=nSource ){
    int iDoclist = pTarget-d->pData;
    d->pData = realloc(d->pData, d->nData+nSource-nTarget);
    pTarget = d->pData+iDoclist;
  }
  if( nTarget<nSource ){
    memmove(pTarget+nSource, pTarget+nTarget, docListEnd(d)-(pTarget+nTarget));
  }

  memcpy(pTarget, pSource, nSource);
  d->nData += nSource-nTarget;
  r->p = pTarget+nSource;
}

/* Insert/update pUpdate into the doclist. */
static void docListUpdate(DocList *d, DocList *pUpdate){
  DocListReader reader;

  assert( d!=NULL && pUpdate!=NULL );
  assert( d->iType==pUpdate->iType);

  readerInit(&reader, d);
  docListSpliceElement(&reader, firstDocid(pUpdate),
                       pUpdate->pData, pUpdate->nData);
}

/* Propagate elements from pUpdate to pAcc, overwriting elements with
** matching docids.
*/
static void docListAccumulate(DocList *pAcc, DocList *pUpdate){
  DocListReader accReader, updateReader;

  /* Handle edge cases where one doclist is empty. */
  assert( pAcc!=NULL );
  if( pUpdate==NULL || pUpdate->nData==0 ) return;
  if( pAcc->nData==0 ){
    pAcc->pData = malloc(pUpdate->nData);
    memcpy(pAcc->pData, pUpdate->pData, pUpdate->nData);
    pAcc->nData = pUpdate->nData;
    return;
  }

  readerInit(&accReader, pAcc);
  readerInit(&updateReader, pUpdate);

  while( !atEnd(&updateReader) ){
    char *pSource = updateReader.p;
    sqlite_int64 iDocid = readDocid(&updateReader);
    skipPositionList(&updateReader);
    docListSpliceElement(&accReader, iDocid, pSource, updateReader.p-pSource);
  }
}

/*
** Read the next non-deleted docid off of pIn.  Return
** 0 if we reach the end of pDoclist.
*/
static sqlite_int64 nextValidDocid(DocListReader *pIn){
  sqlite_int64 docid = 0;
  skipPositionList(pIn);
  while( !atEnd(pIn) && (docid = readDocid(pIn))==0 ){
    skipPositionList(pIn);
  }
  return docid;
}

/*
** pLeft and pRight are two DocListReaders that are pointing to
** positions lists of the same document: iDocid. 
**
** If there are no instances in pLeft or pRight where the position
** of pLeft is one less than the position of pRight, then this
** routine adds nothing to pOut.
**
** If there are one or more instances where positions from pLeft
** are exactly one less than positions from pRight, then add a new
** document record to pOut.  If pOut wants to hold positions, then
** include the positions from pRight that are one more than a
** position in pLeft.  In other words:  pRight.iPos==pLeft.iPos+1.
**
** pLeft and pRight are left pointing at the next document record.
*/
static void mergePosList(
  DocListReader *pLeft,    /* Left position list */
  DocListReader *pRight,   /* Right position list */
  sqlite_int64 iDocid,     /* The docid from pLeft and pRight */
  DocList *pOut            /* Write the merged document record here */
){
  int iLeftPos = readPosition(pLeft);
  int iRightPos = readPosition(pRight);
  int match = 0;

  /* Loop until we've reached the end of both position lists. */
  while( iLeftPos!=-1 && iRightPos!=-1 ){
    if( iLeftPos+1==iRightPos ){
      if( !match ){
        docListAddDocid(pOut, iDocid);
        match = 1;
      }
      if( pOut->iType>=DL_POSITIONS ){
        docListAddPos(pOut, iRightPos);
      }
      iLeftPos = readPosition(pLeft);
      iRightPos = readPosition(pRight);
    }else if( iRightPos<iLeftPos+1 ){
      iRightPos = readPosition(pRight);
    }else{
      iLeftPos = readPosition(pLeft);
    }
  }
  if( iLeftPos>=0 ) skipPositionList(pLeft);
  if( iRightPos>=0 ) skipPositionList(pRight);
}

/* We have two doclists:  pLeft and pRight.
** Write the phrase intersection of these two doclists into pOut.
**
** A phrase intersection means that two documents only match
** if pLeft.iPos+1==pRight.iPos.
**
** The output pOut may or may not contain positions.  If pOut
** does contain positions, they are the positions of pRight.
*/
static void docListPhraseMerge(
  DocList *pLeft,    /* Doclist resulting from the words on the left */
  DocList *pRight,   /* Doclist for the next word to the right */
  DocList *pOut      /* Write the combined doclist here */
){
  DocListReader left, right;
  sqlite_int64 docidLeft, docidRight;

  readerInit(&left, pLeft);
  readerInit(&right, pRight);
  docidLeft = nextValidDocid(&left);
  docidRight = nextValidDocid(&right);

  while( docidLeft>0 && docidRight>0 ){
    if( docidLeft<docidRight ){
      docidLeft = nextValidDocid(&left);
    }else if( docidRight<docidLeft ){
      docidRight = nextValidDocid(&right);
    }else{
      mergePosList(&left, &right, docidLeft, pOut);
      docidLeft = nextValidDocid(&left);
      docidRight = nextValidDocid(&right);
    }
  }
}

/* We have two doclists:  pLeft and pRight.
** Write the intersection of these two doclists into pOut.
** Only docids are matched.  Position information is ignored.
**
** The output pOut never holds positions.
*/
static void docListAndMerge(
  DocList *pLeft,    /* Doclist resulting from the words on the left */
  DocList *pRight,   /* Doclist for the next word to the right */
  DocList *pOut      /* Write the combined doclist here */
){
  DocListReader left, right;
  sqlite_int64 docidLeft, docidRight;

  assert( pOut->iType<DL_POSITIONS );

  readerInit(&left, pLeft);
  readerInit(&right, pRight);
  docidLeft = nextValidDocid(&left);
  docidRight = nextValidDocid(&right);

  while( docidLeft>0 && docidRight>0 ){
    if( docidLeft<docidRight ){
      docidLeft = nextValidDocid(&left);
    }else if( docidRight<docidLeft ){
      docidRight = nextValidDocid(&right);
    }else{
      docListAddDocid(pOut, docidLeft);
      docidLeft = nextValidDocid(&left);
      docidRight = nextValidDocid(&right);
    }
  }
}

/* We have two doclists:  pLeft and pRight.
** Write the union of these two doclists into pOut.
** Only docids are matched.  Position information is ignored.
**
** The output pOut never holds positions.
*/
static void docListOrMerge(
  DocList *pLeft,    /* Doclist resulting from the words on the left */
  DocList *pRight,   /* Doclist for the next word to the right */
  DocList *pOut      /* Write the combined doclist here */
){
  DocListReader left, right;
  sqlite_int64 docidLeft, docidRight, priorLeft;

  readerInit(&left, pLeft);
  readerInit(&right, pRight);
  docidLeft = nextValidDocid(&left);
  docidRight = nextValidDocid(&right);

  while( docidLeft>0 && docidRight>0 ){
    if( docidLeft<=docidRight ){
      docListAddDocid(pOut, docidLeft);
    }else{
      docListAddDocid(pOut, docidRight);
    }
    priorLeft = docidLeft;
    if( docidLeft<=docidRight ){
      docidLeft = nextValidDocid(&left);
    }
    if( docidRight>0 && docidRight<=priorLeft ){
      docidRight = nextValidDocid(&right);
    }
  }
  while( docidLeft>0 ){
    docListAddDocid(pOut, docidLeft);
    docidLeft = nextValidDocid(&left);
  }
  while( docidRight>0 ){
    docListAddDocid(pOut, docidRight);
    docidRight = nextValidDocid(&right);
  }
}

/* We have two doclists:  pLeft and pRight.
** Write into pOut all documents that occur in pLeft but not
** in pRight.
**
** Only docids are matched.  Position information is ignored.
**
** The output pOut never holds positions.
*/
static void docListExceptMerge(
  DocList *pLeft,    /* Doclist resulting from the words on the left */
  DocList *pRight,   /* Doclist for the next word to the right */
  DocList *pOut      /* Write the combined doclist here */
){
  DocListReader left, right;
  sqlite_int64 docidLeft, docidRight, priorLeft;

  readerInit(&left, pLeft);
  readerInit(&right, pRight);
  docidLeft = nextValidDocid(&left);
  docidRight = nextValidDocid(&right);

  while( docidLeft>0 && docidRight>0 ){
    priorLeft = docidLeft;
    if( docidLeft<docidRight ){
      docListAddDocid(pOut, docidLeft);
    }
    if( docidLeft<=docidRight ){
      docidLeft = nextValidDocid(&left);
    }
    if( docidRight>0 && docidRight<=priorLeft ){
      docidRight = nextValidDocid(&right);
    }
  }
  while( docidLeft>0 ){
    docListAddDocid(pOut, docidLeft);
    docidLeft = nextValidDocid(&left);
  }
}

/* Duplicate a string; the caller must free() the returned string.
 * (We don't use strdup() since it's not part of the standard C library and
 * may not be available everywhere.) */
static char *string_dup(const char *s){
  int n = strlen(s);
  char *str = malloc(n + 1);
  memcpy(str, s, n);
  str[n] = '\0';
  return str;
}

/* Format a string, replacing each occurrence of the % character with
 * zName.  This may be more convenient than sqlite_mprintf()
 * when one string is used repeatedly in a format string.
 * The caller must free() the returned string. */
static char *string_format(const char *zFormat, const char *zName){
  const char *p;
  size_t len = 0;
  size_t nName = strlen(zName);
  char *result;
  char *r;

  /* first compute length needed */
  for(p = zFormat ; *p ; ++p){
    len += (*p=='%' ? nName : 1);
  }
  len += 1;  /* for null terminator */

  r = result = malloc(len);
  for(p = zFormat; *p; ++p){
    if( *p=='%' ){
      memcpy(r, zName, nName);
      r += nName;
    } else {
      *r++ = *p;
    }
  }
  *r++ = '\0';
  assert( r == result + len );
  return result;
}

static int sql_exec(sqlite3 *db, const char *zName, const char *zFormat){
  char *zCommand = string_format(zFormat, zName);
  int rc;
  TRACE(("FTS1 sql: %s\n", zCommand));
  rc = sqlite3_exec(db, zCommand, NULL, 0, NULL);
  free(zCommand);
  return rc;
}

static int sql_prepare(sqlite3 *db, const char *zName, sqlite3_stmt **ppStmt,
                const char *zFormat){
  char *zCommand = string_format(zFormat, zName);
  int rc;
  TRACE(("FTS1 prepare: %s\n", zCommand));
  rc = sqlite3_prepare(db, zCommand, -1, ppStmt, NULL);
  free(zCommand);
  return rc;
}

/* end utility functions */

#define QUERY_GENERIC 0
#define QUERY_FULLTEXT 1

/* TODO(shess) CHUNK_MAX controls how much data we allow in segment 0
** before we start aggregating into larger segments.  Lower CHUNK_MAX
** means that for a given input we have more individual segments per
** term, which means more rows in the table and a bigger index (due to
** both more rows and bigger rowids).  But it also reduces the average
** cost of adding new elements to the segment 0 doclist, and it seems
** to reduce the number of pages read and written during inserts.  256
** was chosen by measuring insertion times for a certain input (first
** 10k documents of Enron corpus), though including query performance
** in the decision may argue for a larger value.
*/
#define CHUNK_MAX 256

typedef enum fulltext_statement {
  CONTENT_INSERT_STMT,
  CONTENT_SELECT_STMT,
  CONTENT_DELETE_STMT,

  TERM_SELECT_STMT,
  TERM_SELECT_ALL_STMT,
  TERM_INSERT_STMT,
  TERM_UPDATE_STMT,
  TERM_DELETE_STMT,

  MAX_STMT                     /* Always at end! */
} fulltext_statement;

/* These must exactly match the enum above. */
/* TODO(adam): Is there some risk that a statement (in particular,
** pTermSelectStmt) will be used in two cursors at once, e.g.  if a
** query joins a virtual table to itself?  If so perhaps we should
** move some of these to the cursor object.
*/
static const char *const fulltext_zStatement[MAX_STMT] = {
  /* CONTENT_INSERT */ "insert into %_content (rowid, content) values (?, ?)",
  /* CONTENT_SELECT */ "select content from %_content where rowid = ?",
  /* CONTENT_DELETE */ "delete from %_content where rowid = ?",

  /* TERM_SELECT */
  "select rowid, doclist from %_term where term = ? and segment = ?",
  /* TERM_SELECT_ALL */
  "select doclist from %_term where term = ? order by segment",
  /* TERM_INSERT */
  "insert into %_term (term, segment, doclist) values (?, ?, ?)",
  /* TERM_UPDATE */ "update %_term set doclist = ? where rowid = ?",
  /* TERM_DELETE */ "delete from %_term where rowid = ?",
};

typedef struct fulltext_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  const char *zName;               /* virtual table name */
  sqlite3_tokenizer *pTokenizer;   /* tokenizer for inserts and queries */

  /* Precompiled statements which we keep as long as the table is
  ** open.
  */
  sqlite3_stmt *pFulltextStatements[MAX_STMT];
} fulltext_vtab;

typedef struct fulltext_cursor {
  sqlite3_vtab_cursor base;
  int iCursorType;  /* QUERY_GENERIC or QUERY_FULLTEXT */

  sqlite3_stmt *pStmt;

  int eof;

  /* The following is used only when iCursorType == QUERY_FULLTEXT. */
  DocListReader result;
} fulltext_cursor;

static struct fulltext_vtab *cursor_vtab(fulltext_cursor *c){
  return (fulltext_vtab *) c->base.pVtab;
}

static const sqlite3_module fulltextModule;   /* forward declaration */

/* Puts a freshly-prepared statement determined by iStmt in *ppStmt.
** If the indicated statement has never been prepared, it is prepared
** and cached, otherwise the cached version is reset.
*/
static int sql_get_statement(fulltext_vtab *v, fulltext_statement iStmt,
                             sqlite3_stmt **ppStmt){
  assert( iStmt<MAX_STMT );
  if( v->pFulltextStatements[iStmt]==NULL ){
    int rc = sql_prepare(v->db, v->zName, &v->pFulltextStatements[iStmt],
                         fulltext_zStatement[iStmt]);
    if( rc!=SQLITE_OK ) return rc;
  } else {
    int rc = sqlite3_reset(v->pFulltextStatements[iStmt]);
    if( rc!=SQLITE_OK ) return rc;
  }

  *ppStmt = v->pFulltextStatements[iStmt];
  return SQLITE_OK;
}

/* Step the indicated statement, handling errors SQLITE_BUSY (by
** retrying) and SQLITE_SCHEMA (by re-preparing and transferring
** bindings to the new statement).
** TODO(adam): We should extend this function so that it can work with
** statements declared locally, not only globally cached statements.
*/
static int sql_step_statement(fulltext_vtab *v, fulltext_statement iStmt,
                              sqlite3_stmt **ppStmt){
  int rc;
  sqlite3_stmt *s = *ppStmt;
  assert( iStmt<MAX_STMT );
  assert( s==v->pFulltextStatements[iStmt] );

  while( (rc=sqlite3_step(s))!=SQLITE_DONE && rc!=SQLITE_ROW ){
    sqlite3_stmt *pNewStmt;

    if( rc==SQLITE_BUSY ) continue;
    if( rc!=SQLITE_ERROR ) return rc;

    rc = sqlite3_reset(s);
    if( rc!=SQLITE_SCHEMA ) return SQLITE_ERROR;

    v->pFulltextStatements[iStmt] = NULL;   /* Still in s */
    rc = sql_get_statement(v, iStmt, &pNewStmt);
    if( rc!=SQLITE_OK ) goto err;
    *ppStmt = pNewStmt;

    rc = sqlite3_transfer_bindings(s, pNewStmt);
    if( rc!=SQLITE_OK ) goto err;

    rc = sqlite3_finalize(s);
    if( rc!=SQLITE_OK ) return rc;
    s = pNewStmt;
  }
  return rc;

 err:
  sqlite3_finalize(s);
  return rc;
}

/* Like sql_step_statement(), but convert SQLITE_DONE to SQLITE_OK.
** Useful for statements like UPDATE, where we expect no results.
*/
static int sql_single_step_statement(fulltext_vtab *v,
                                     fulltext_statement iStmt,
                                     sqlite3_stmt **ppStmt){
  int rc = sql_step_statement(v, iStmt, ppStmt);
  return (rc==SQLITE_DONE) ? SQLITE_OK : rc;
}

/* insert into %_content (rowid, content) values ([rowid], [zContent]) */
static int content_insert(fulltext_vtab *v, sqlite3_value *rowid,
                          const char *pContent, int nContent){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, CONTENT_INSERT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_value(s, 1, rowid);
  if( rc!=SQLITE_OK ) return rc;

  assert( nContent>=0 );
  rc = sqlite3_bind_text(s, 2, pContent, nContent, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  return sql_single_step_statement(v, CONTENT_INSERT_STMT, &s);
}

/* select content from %_content where rowid = [iRow]
 * The caller must delete the returned string. */
static int content_select(fulltext_vtab *v, sqlite_int64 iRow,
                          char **ppContent, int *pnContent){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, CONTENT_SELECT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int64(s, 1, iRow);
  if( rc!=SQLITE_OK ) return rc;

  rc = sql_step_statement(v, CONTENT_SELECT_STMT, &s);
  if( rc!=SQLITE_ROW ) return rc;

  *pnContent = sqlite3_column_bytes(s, 0);
  *ppContent = malloc(*pnContent);
  memcpy(*ppContent, sqlite3_column_blob(s, 0), *pnContent);

  /* We expect only one row.  We must execute another sqlite3_step()
   * to complete the iteration; otherwise the table will remain locked. */
  rc = sqlite3_step(s);
  if( rc==SQLITE_DONE ) return SQLITE_OK;

  free(*ppContent);
  return rc;
}

/* delete from %_content where rowid = [iRow ] */
static int content_delete(fulltext_vtab *v, sqlite_int64 iRow){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, CONTENT_DELETE_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int64(s, 1, iRow);
  if( rc!=SQLITE_OK ) return rc;

  return sql_single_step_statement(v, CONTENT_DELETE_STMT, &s);
}

/* select rowid, doclist from %_term
 *  where term = [pTerm] and segment = [iSegment]
 * If found, returns SQLITE_ROW; the caller must free the
 * returned doclist.  If no rows found, returns SQLITE_DONE. */
static int term_select(fulltext_vtab *v, const char *pTerm, int nTerm,
                       int iSegment,
                       sqlite_int64 *rowid, DocList *out){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_SELECT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_text(s, 1, pTerm, nTerm, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int(s, 2, iSegment);
  if( rc!=SQLITE_OK ) return rc;

  rc = sql_step_statement(v, TERM_SELECT_STMT, &s);
  if( rc!=SQLITE_ROW ) return rc;

  *rowid = sqlite3_column_int64(s, 0);
  docListInit(out, DL_POSITIONS_OFFSETS,
              sqlite3_column_blob(s, 1), sqlite3_column_bytes(s, 1));

  /* We expect only one row.  We must execute another sqlite3_step()
   * to complete the iteration; otherwise the table will remain locked. */
  rc = sqlite3_step(s);
  return rc==SQLITE_DONE ? SQLITE_ROW : rc;
}

/* Load the segment doclists for term pTerm and merge them in
** appropriate order into out.  Returns SQLITE_OK if successful.  If
** there are no segments for pTerm, successfully returns an empty
** doclist in out.
*/
static int term_select_all(fulltext_vtab *v, const char *pTerm, int nTerm,
                           DocList *out){
  DocList doclist;
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_SELECT_ALL_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_text(s, 1, pTerm, nTerm, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  docListInit(&doclist, DL_POSITIONS_OFFSETS, 0, 0);

  /* TODO(shess) Handle schema and busy errors. */
  while( (rc=sql_step_statement(v, TERM_SELECT_ALL_STMT, &s))==SQLITE_ROW ){
    DocList old;

    /* TODO(shess) If we processed doclists from oldest to newest, we
    ** could skip the malloc() involved with the following call.  For
    ** now, I'd rather keep this logic similar to index_insert_term().
    ** We could additionally drop elements when we see deletes, but
    ** that would require a distinct version of docListAccumulate().
    */
    docListInit(&old, doclist.iType,
                sqlite3_column_blob(s, 0), sqlite3_column_bytes(s, 0));

    /* doclist contains the newer data, so write it over old.  Then
    ** steal accumulated result for doclist.
    */
    docListAccumulate(&old, &doclist);
    docListDestroy(&doclist);
    doclist = old;
  }
  if( rc!=SQLITE_DONE ){
    docListDestroy(&doclist);
    return rc;
  }

  *out = doclist;
  return SQLITE_OK;
}

/* insert into %_term (term, segment, doclist)
               values ([pTerm], [iSegment], [doclist]) */
static int term_insert(fulltext_vtab *v, const char *pTerm, int nTerm,
                       int iSegment, DocList *doclist){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_INSERT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_text(s, 1, pTerm, nTerm, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int(s, 2, iSegment);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_blob(s, 3, doclist->pData, doclist->nData, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  return sql_single_step_statement(v, TERM_INSERT_STMT, &s);
}

/* update %_term set doclist = [doclist] where rowid = [rowid] */
static int term_update(fulltext_vtab *v, sqlite_int64 rowid,
                       DocList *doclist){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_UPDATE_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_blob(s, 1, doclist->pData, doclist->nData, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int64(s, 2, rowid);
  if( rc!=SQLITE_OK ) return rc;

  return sql_single_step_statement(v, TERM_UPDATE_STMT, &s);
}

static int term_delete(fulltext_vtab *v, sqlite_int64 rowid){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_DELETE_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int64(s, 1, rowid);
  if( rc!=SQLITE_OK ) return rc;

  return sql_single_step_statement(v, TERM_DELETE_STMT, &s);
}

static void fulltext_vtab_destroy(fulltext_vtab *v){
  int iStmt;

  TRACE(("FTS1 Destroy %p\n", v));
  for( iStmt=0; iStmt<MAX_STMT; iStmt++ ){
    if( v->pFulltextStatements[iStmt]!=NULL ){
      sqlite3_finalize(v->pFulltextStatements[iStmt]);
      v->pFulltextStatements[iStmt] = NULL;
    }
  }

  if( v->pTokenizer!=NULL ){
    v->pTokenizer->pModule->xDestroy(v->pTokenizer);
    v->pTokenizer = NULL;
  }

  free((void *) v->zName);
  free(v);
}

/* Current interface:
** argv[0] - module name
** argv[1] - database name
** argv[2] - table name
** argv[3] - tokenizer name (optional, a sensible default is provided)
** argv[4..] - passed to tokenizer (optional based on tokenizer)
**/
static int fulltextConnect(sqlite3 *db, void *pAux, int argc, char **argv,
                           sqlite3_vtab **ppVTab){
  int rc;
  fulltext_vtab *v;
  const sqlite3_tokenizer_module *m = NULL;

  assert( argc>=3 );
  v = (fulltext_vtab *) malloc(sizeof(fulltext_vtab));
  /* sqlite will initialize v->base */
  v->db = db;
  v->zName = string_dup(argv[2]);
  v->pTokenizer = NULL;

  if( argc==3 ){
    sqlite3Fts1SimpleTokenizerModule(&m);
  } else {
    /* TODO(shess) For now, add new tokenizers as else if clauses. */
    if( !strcmp(argv[3], "simple") ){
      sqlite3Fts1SimpleTokenizerModule(&m);
    } else {
      assert( "unrecognized tokenizer"==NULL );
    }
  }

  /* TODO(shess) Since tokenization impacts the index, the parameters
  ** to the tokenizer need to be identical when a persistent virtual
  ** table is re-created.  One solution would be a meta-table to track
  ** such information in the database.  Then we could verify that the
  ** information is identical on subsequent creates.
  */
  /* TODO(shess) Why isn't argv already (const char **)? */
  rc = m->xCreate(argc-3, (const char **) (argv+3), &v->pTokenizer);
  if( rc!=SQLITE_OK ) return rc;
  v->pTokenizer->pModule = m;

  /* TODO: verify the existence of backing tables foo_content, foo_term */

  rc = sqlite3_declare_vtab(db, "create table x(content text)");
  if( rc!=SQLITE_OK ) return rc;

  memset(v->pFulltextStatements, 0, sizeof(v->pFulltextStatements));

  *ppVTab = &v->base;
  TRACE(("FTS1 Connect %p\n", v));
  return SQLITE_OK;
}

static int fulltextCreate(sqlite3 *db, void *pAux, int argc, char **argv,
                          sqlite3_vtab **ppVTab){
  int rc;
  assert( argc>=3 );
  TRACE(("FTS1 Create\n"));

  /* The %_content table holds the text of each full-text item, with
  ** the rowid used as the docid.
  **
  ** The %_term table maps each term to a document list blob
  ** containing elements sorted by ascending docid, each element
  ** encoded as:
  **
  **   docid varint-encoded
  **   token elements:
  **     position+1 varint-encoded as delta from previous position
  **     start offset varint-encoded as delta from previous start offset
  **     end offset varint-encoded as delta from start offset
  **
  ** The sentinel position of 0 indicates the end of the token list.
  **
  ** Additionally, doclist blobs are chunked into multiple segments,
  ** using segment to order the segments.  New elements are added to
  ** the segment at segment 0, until it exceeds CHUNK_MAX.  Then
  ** segment 0 is deleted, and the doclist is inserted at segment 1.
  ** If there is already a doclist at segment 1, the segment 0 doclist
  ** is merged with it, the segment 1 doclist is deleted, and the
  ** merged doclist is inserted at segment 2, repeating those
  ** operations until an insert succeeds.
  **
  ** Since this structure doesn't allow us to update elements in place
  ** in case of deletion or update, these are simply written to
  ** segment 0 (with an empty token list in case of deletion), with
  ** docListAccumulate() taking care to retain lower-segment
  ** information in preference to higher-segment information.
  */
  /* TODO(shess) Provide a VACUUM type operation which both removes
  ** deleted elements which are no longer necessary, and duplicated
  ** elements.  I suspect this will probably not be necessary in
  ** practice, though.
  */
  rc = sql_exec(db, argv[2],
    "create table %_content(content text);"
    "create table %_term(term text, segment integer, doclist blob, "
                        "primary key(term, segment));");
  if( rc!=SQLITE_OK ) return rc;

  return fulltextConnect(db, pAux, argc, argv, ppVTab);
}

/* Decide how to handle an SQL query.
 * At the moment, MATCH queries can include implicit boolean ANDs; we
 * haven't implemented phrase searches or OR yet. */
static int fulltextBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo){
  int i;

  for(i=0; i<pInfo->nConstraint; ++i){
    const struct sqlite3_index_constraint *pConstraint;
    pConstraint = &pInfo->aConstraint[i];
    if( pConstraint->iColumn==0 &&
        pConstraint->op==SQLITE_INDEX_CONSTRAINT_MATCH &&
        pConstraint->usable ){   /* a full-text search */
      pInfo->aConstraintUsage[i].argvIndex = 1;
      pInfo->aConstraintUsage[i].omit = 1;
      pInfo->idxNum = QUERY_FULLTEXT;
      pInfo->estimatedCost = 1.0;   /* an arbitrary value for now */
      return SQLITE_OK;
    }
  }
  pInfo->idxNum = QUERY_GENERIC;
  TRACE(("FTS1 BestIndex\n"));
  return SQLITE_OK;
}

static int fulltextDisconnect(sqlite3_vtab *pVTab){
  TRACE(("FTS1 Disconnect %p\n", pVTab));
  fulltext_vtab_destroy((fulltext_vtab *)pVTab);
  return SQLITE_OK;
}

static int fulltextDestroy(sqlite3_vtab *pVTab){
  fulltext_vtab *v = (fulltext_vtab *)pVTab;
  int rc;

  TRACE(("FTS1 Destroy %p\n", pVTab));
  rc = sql_exec(v->db, v->zName,
                    "drop table %_content; drop table %_term");
  if( rc!=SQLITE_OK ) return rc;

  fulltext_vtab_destroy((fulltext_vtab *)pVTab);
  return SQLITE_OK;
}

static int fulltextOpen(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor){
  fulltext_cursor *c;

  c = (fulltext_cursor *) calloc(sizeof(fulltext_cursor), 1);
  /* sqlite will initialize c->base */
  *ppCursor = &c->base;
  TRACE(("FTS1 Open %p: %p\n", pVTab, c));

  return SQLITE_OK;
}

static int fulltextClose(sqlite3_vtab_cursor *pCursor){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  TRACE(("FTS1 Close %p\n", c));
  sqlite3_finalize(c->pStmt);
  if( c->result.pDoclist!=NULL ){
    docListDelete(c->result.pDoclist);
  }
  free(c);
  return SQLITE_OK;
}

static int fulltextNext(sqlite3_vtab_cursor *pCursor){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  sqlite_int64 iDocid;
  int rc;

  TRACE(("FTS1 Next %p\n", pCursor));
  switch( c->iCursorType ){
    case QUERY_GENERIC:
      /* TODO(shess) Handle SQLITE_SCHEMA AND SQLITE_BUSY. */
      rc = sqlite3_step(c->pStmt);
      switch( rc ){
        case SQLITE_ROW:
          c->eof = 0;
          return SQLITE_OK;
        case SQLITE_DONE:
          c->eof = 1;
          return SQLITE_OK;
        default:
          c->eof = 1;
          return rc;
      }
    case QUERY_FULLTEXT:
      rc = sqlite3_reset(c->pStmt);
      if( rc!=SQLITE_OK ) return rc;

      iDocid = nextValidDocid(&c->result);
      if( iDocid==0 ){
        c->eof = 1;
        return SQLITE_OK;
      }
      rc = sqlite3_bind_int64(c->pStmt, 1, iDocid);
      if( rc!=SQLITE_OK ) return rc;
      /* TODO(shess) Handle SQLITE_SCHEMA AND SQLITE_BUSY. */
      rc = sqlite3_step(c->pStmt);
      if( rc==SQLITE_ROW ){   /* the case we expect */
        c->eof = 0;
        return SQLITE_OK;
      }
      /* an error occurred; abort */
      return rc==SQLITE_DONE ? SQLITE_ERROR : rc;
    default:
      assert( 0 );
      return SQLITE_ERROR;  /* not reached */
  }
}

/* A single term in a query is represented by an instances of
** the following structure.
*/
typedef struct QueryTerm {
  int nPhrase;       /* How many following terms are part of the same phrase */
  int isOr;          /* this term is preceded by "OR" */
  int isNot;         /* this term is preceded by "-" */
  char *pTerm;       /* text of the term.  '\000' terminated.  malloced */
  int nTerm;         /* Number of bytes in pTerm[] */
} QueryTerm;


/* Return a DocList corresponding to the query term *pTerm.  If *pTerm
** is the first term of a phrase query, go ahead and evaluate the phrase
** query and return the doclist for the entire phrase query.
**
** The result is stored in pTerm->doclist.
*/
static int docListOfTerm(
  fulltext_vtab *v,     /* The full text index */
  QueryTerm *pQTerm,    /* Term we are looking for, or 1st term of a phrase */
  DocList **ppResult    /* Write the result here */
){
  DocList *pLeft, *pRight, *pNew;
  int i, rc;

  pLeft = docListNew(DL_POSITIONS);
  rc = term_select_all(v, pQTerm->pTerm, pQTerm->nTerm, pLeft);
  if( rc ) return rc;
  for(i=1; i<=pQTerm->nPhrase; i++){
    pRight = docListNew(DL_POSITIONS);
    rc = term_select_all(v, pQTerm[i].pTerm, pQTerm[i].nTerm, pRight);
    if( rc ){
      docListDelete(pLeft);
      return rc;
    }
    pNew = docListNew(i<pQTerm->nPhrase ? DL_POSITIONS : DL_DOCIDS);
    docListPhraseMerge(pLeft, pRight, pNew);
    docListDelete(pLeft);
    docListDelete(pRight);
    pLeft = pNew;
  }
  *ppResult = pLeft;
  return SQLITE_OK;
}



/* Parse a query string into a Query structure.
 *
 * We could, in theory, allow query strings to be complicated
 * nested expressions with precedence determined by parentheses.
 * But none of the major search engines do this.  (Perhaps the
 * feeling is that an parenthesized expression is two complex of
 * an idea for the average user to grasp.)  Taking our lead from
 * the major search engines, we will allow queries to be a list
 * of terms (with an implied AND operator) or phrases in double-quotes,
 * with a single optional "-" before each non-phrase term to designate
 * negation and an optional OR connector.
 *
 * OR binds more tightly than the implied AND, which is what the
 * major search engines seem to do.  So, for example:
 * 
 *    [one two OR three]     ==>    one AND (two OR three)
 *    [one OR two three]     ==>    (one OR two) AND three
 *
 * A "-" before a term matches all entries that lack that term.
 * The "-" must occur immediately before the term with in intervening
 * space.  This is how the search engines do it.
 *
 * A NOT term cannot be the right-hand operand of an OR.  If this
 * occurs in the query string, the NOT is ignored:
 *
 *    [one OR -two]          ==>    one OR two
 *
 */
typedef struct Query {
  int nTerms;           /* Number of terms in the query */
  QueryTerm *pTerms;    /* Array of terms.  Space obtained from malloc() */
  int nextIsOr;         /* Set the isOr flag on the next inserted term */
} Query;

/* Add a new term pTerm[0..nTerm-1] to the query *q.
*/
static void queryAdd(Query *q, const char *pTerm, int nTerm){
  QueryTerm *t;
  ++q->nTerms;
  q->pTerms = realloc(q->pTerms, q->nTerms * sizeof(q->pTerms[0]));
  if( q->pTerms==0 ){
    q->nTerms = 0;
    return;
  }
  t = &q->pTerms[q->nTerms - 1];
  memset(t, 0, sizeof(*t));
  t->pTerm = malloc(nTerm+1);
  memcpy(t->pTerm, pTerm, nTerm);
  t->pTerm[nTerm] = 0;
  t->nTerm = nTerm;
  t->isOr = q->nextIsOr;
  q->nextIsOr = 0;
}

/* Free all of the memory that was malloced in order to build *q.
*/
static void queryDestroy(Query *q){
  int i;
  for(i = 0; i < q->nTerms; ++i){
    free(q->pTerms[i].pTerm);
  }
  free(q->pTerms);
}

/*
** Parse the text at pSegment[0..nSegment-1].  Add additional terms
** to the query being assemblied in pQuery.
**
** inPhrase is true if pSegment[0..nSegement-1] is contained within
** double-quotes.  If inPhrase is true, then the first term
** is marked with the number of terms in the phrase less one and
** OR and "-" syntax is ignored.  If inPhrase is false, then every
** term found is marked with nPhrase=0 and OR and "-" syntax is significant.
*/
static int tokenizeSegment(
  sqlite3_tokenizer *pTokenizer,          /* The tokenizer to use */
  const char *pSegment, int nSegment,     /* Query expression being parsed */
  int inPhrase,                           /* True if within "..." */
  Query *pQuery                           /* Append results here */
){
  const sqlite3_tokenizer_module *pModule = pTokenizer->pModule;
  sqlite3_tokenizer_cursor *pCursor;
  int firstIndex = pQuery->nTerms;
  
  int rc = pModule->xOpen(pTokenizer, pSegment, nSegment, &pCursor);
  if( rc!=SQLITE_OK ) return rc;
  pCursor->pTokenizer = pTokenizer;

  while( 1 ){
    const char *pToken;
    int nToken, iBegin, iEnd, iPos;

    rc = pModule->xNext(pCursor,
                        &pToken, &nToken,
                        &iBegin, &iEnd, &iPos);
    if( rc!=SQLITE_OK ) break;
    if( !inPhrase && pQuery->nTerms>0 && nToken==2
         && pSegment[iBegin]=='O' && pSegment[iBegin+1]=='R' ){
      pQuery->nextIsOr = 1;
      continue;
    }
    queryAdd(pQuery, pToken, nToken);
    if( !inPhrase && iBegin>0 && pSegment[iBegin-1]=='-' ){
      pQuery->pTerms[pQuery->nTerms-1].isNot = 1;
    }
  }

  if( inPhrase && pQuery->nTerms>firstIndex ){
    pQuery->pTerms[firstIndex].nPhrase = pQuery->nTerms - firstIndex - 1;
  }

  return pModule->xClose(pCursor);
}

/* Parse a query string, yielding a Query object [pQuery], which the caller
 * must free. */
static int parseQuery(fulltext_vtab *v, const char *pInput, int nInput,
                      Query *pQuery){
  int iInput, inPhrase = 0;

  if( nInput<0 ) nInput = strlen(pInput);
  pQuery->nTerms = 0;
  pQuery->pTerms = NULL;
  pQuery->nextIsOr = 0;

  for(iInput=0; iInput<nInput; ++iInput){
    int i;
    for(i=iInput; i<nInput && pInput[i]!='"'; ++i)
      ;
    if( i>iInput ){
      tokenizeSegment(v->pTokenizer, pInput+iInput, i-iInput, inPhrase,
                       pQuery);
    }
    iInput = i;
    if( i<nInput ){
      assert( pInput[i]=='"' );
      inPhrase = !inPhrase;
    }
  }

  if(inPhrase) {  /* unmatched quote */
    queryDestroy(pQuery);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

/* Perform a full-text query using the search expression in
** pInput[0..nInput-1].  Return a list of matching documents
** in pResult.
*/
static int fulltextQuery(fulltext_vtab *v, const char *pInput, int nInput,
                          DocList **pResult){
  Query q;
  int i, rc;
  DocList *pLeft = NULL;
  DocList *pRight, *pNew;
  int nNot = 0;

  rc = parseQuery(v, pInput, nInput, &q);
  if( rc!=SQLITE_OK ) return rc;

  /* Merge AND terms. */
  for(i = 0 ; i < q.nTerms; i += q.pTerms[i].nPhrase + 1){

    if( q.pTerms[i].isNot ){
      /* Handle all NOT terms in a separate pass */
      nNot++;
      continue;
    }

    rc = docListOfTerm(v, &q.pTerms[i], &pRight);
    if( rc ){
      queryDestroy(&q);
      return rc;
    }
    if( pLeft==0 ){
      pLeft = pRight;
    }else{
      pNew = docListNew(DL_DOCIDS);
      if( q.pTerms[i].isOr ){
        docListOrMerge(pLeft, pRight, pNew);
      }else{
        docListAndMerge(pLeft, pRight, pNew);
      }
      docListDelete(pRight);
      docListDelete(pLeft);
      pLeft = pNew;
    }
  }

  if( nNot && pLeft==0 ){
    /* We do not yet know how to handle a query of only NOT terms */
    return SQLITE_ERROR;
  }


  /* Do the EXCEPT terms */
  for(i=0; i<q.nTerms;  i += q.pTerms[i].nPhrase + 1){
    if( !q.pTerms[i].isNot ) continue;
    rc = docListOfTerm(v, &q.pTerms[i], &pRight);
    if( rc ){
      queryDestroy(&q);
      docListDelete(pLeft);
      return rc;
    }
    pNew = docListNew(DL_DOCIDS);
    docListExceptMerge(pLeft, pRight, pNew);
    docListDelete(pRight);
    docListDelete(pLeft);
    pLeft = pNew;
  }

  queryDestroy(&q);
  *pResult = pLeft;
  return rc;
}

static int fulltextFilter(sqlite3_vtab_cursor *pCursor,
                          int idxNum, const char *idxStr,
                          int argc, sqlite3_value **argv){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  fulltext_vtab *v = cursor_vtab(c);
  int rc;
  const char *zStatement;

  TRACE(("FTS1 Filter %p\n",pCursor));
  c->iCursorType = idxNum;
  switch( idxNum ){
    case QUERY_GENERIC:
      zStatement = "select rowid, content from %_content";
      break;

    case QUERY_FULLTEXT:   /* full-text search */
    {
      const char *zQuery = (const char *)sqlite3_value_text(argv[0]);
      DocList *pResult;
      assert( argc==1 );
      rc = fulltextQuery(v, zQuery, -1, &pResult);
      if( rc!=SQLITE_OK ) return rc;
      readerInit(&c->result, pResult);
      zStatement = "select rowid, content from %_content where rowid = ?";
      break;
    }

    default:
      assert( 0 );
  }

  rc = sql_prepare(v->db, v->zName, &c->pStmt, zStatement);
  if( rc!=SQLITE_OK ) return rc;

  return fulltextNext(pCursor);
}

static int fulltextEof(sqlite3_vtab_cursor *pCursor){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  return c->eof;
}

static int fulltextColumn(sqlite3_vtab_cursor *pCursor,
                          sqlite3_context *pContext, int idxCol){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  const char *s;

  assert( idxCol==0 );
  s = (const char *) sqlite3_column_text(c->pStmt, 1);
  sqlite3_result_text(pContext, s, -1, SQLITE_TRANSIENT);

  return SQLITE_OK;
}

static int fulltextRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;

  *pRowid = sqlite3_column_int64(c->pStmt, 0);
  return SQLITE_OK;
}

/* Build a hash table containing all terms in pText. */
static int buildTerms(fts1Hash *terms, sqlite3_tokenizer *pTokenizer,
                       const char *pText, int nText, sqlite_int64 iDocid){
  sqlite3_tokenizer_cursor *pCursor;
  const char *pToken;
  int nTokenBytes;
  int iStartOffset, iEndOffset, iPosition;
  int rc;

  assert( nText>=0 );

  rc = pTokenizer->pModule->xOpen(pTokenizer, pText, nText, &pCursor);
  if( rc!=SQLITE_OK ) return rc;

  pCursor->pTokenizer = pTokenizer;
  fts1HashInit(terms, FTS1_HASH_STRING, 1);
  while( SQLITE_OK==pTokenizer->pModule->xNext(pCursor,
                                               &pToken, &nTokenBytes,
                                               &iStartOffset, &iEndOffset,
                                               &iPosition) ){
    DocList *p;

    /* Positions can't be negative; we use -1 as a terminator internally. */
    if( iPosition<0 ) {
      rc = SQLITE_ERROR;  
      goto err;
    }

    p = fts1HashFind(terms, pToken, nTokenBytes);
    if( p==NULL ){
      p = docListNew(DL_POSITIONS_OFFSETS);
      docListAddDocid(p, iDocid);
      fts1HashInsert(terms, pToken, nTokenBytes, p);
    }
    docListAddPosOffset(p, iPosition, iStartOffset, iEndOffset);
  }

err:
  /* TODO(shess) Check return?  Should this be able to cause errors at
  ** this point?  Actually, same question about sqlite3_finalize(),
  ** though one could argue that failure there means that the data is
  ** not durable.  *ponder*
  */
  pTokenizer->pModule->xClose(pCursor);
  return rc;
}

/* Update the %_terms table to map the term [pTerm] to the given rowid. */
static int index_insert_term(fulltext_vtab *v, const char *pTerm, int nTerm,
                             DocList *d){
  sqlite_int64 iIndexRow;
  DocList doclist;
  int iSegment = 0, rc;

  rc = term_select(v, pTerm, nTerm, iSegment, &iIndexRow, &doclist);
  if( rc==SQLITE_DONE ){
    docListInit(&doclist, DL_POSITIONS_OFFSETS, 0, 0);
    docListUpdate(&doclist, d);
    /* TODO(shess) Consider length(doclist)>CHUNK_MAX? */
    rc = term_insert(v, pTerm, nTerm, iSegment, &doclist);
    goto err;
  }
  if( rc!=SQLITE_ROW ) return SQLITE_ERROR;

  docListUpdate(&doclist, d);
  if( doclist.nData<=CHUNK_MAX ){
    rc = term_update(v, iIndexRow, &doclist);
    goto err;
  }

  /* Doclist doesn't fit, delete what's there, and accumulate
  ** forward.
  */
  rc = term_delete(v, iIndexRow);
  if( rc!=SQLITE_OK ) goto err;

  /* Try to insert the doclist into a higher segment bucket.  On
  ** failure, accumulate existing doclist with the doclist from that
  ** bucket, and put results in the next bucket.
  */
  iSegment++;
  while( (rc=term_insert(v, pTerm, nTerm, iSegment, &doclist))!=SQLITE_OK ){
    DocList old;
    int rc2;

    /* Retain old error in case the term_insert() error was really an
    ** error rather than a bounced insert.
    */
    rc2 = term_select(v, pTerm, nTerm, iSegment, &iIndexRow, &old);
    if( rc2!=SQLITE_ROW ) goto err;

    rc = term_delete(v, iIndexRow);
    if( rc!=SQLITE_OK ) goto err;

    /* doclist contains the newer data, so accumulate it over old.
    ** Then steal accumulated data for doclist.
    */
    docListAccumulate(&old, &doclist);
    docListDestroy(&doclist);
    doclist = old;

    iSegment++;
  }

 err:
  docListDestroy(&doclist);
  return rc;
}

/* Insert a row into the full-text index; set *piRowid to be the ID of the
 * new row. */
static int index_insert(fulltext_vtab *v, sqlite3_value *pRequestRowid,
                        const char *pText, int nText,
                        sqlite_int64 *piRowid){
  fts1Hash terms;  /* maps term string -> PosList */
  fts1HashElem *e;
  int rc;

  assert( nText>=0 );

  rc = content_insert(v, pRequestRowid, pText, nText);
  if( rc!=SQLITE_OK ) return rc;
  *piRowid = sqlite3_last_insert_rowid(v->db);

  if( !pText || !nText ) return SQLITE_OK;   /* nothing to index */

  rc = buildTerms(&terms, v->pTokenizer, pText, nText, *piRowid);
  if( rc!=SQLITE_OK ) return rc;

  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    DocList *p = fts1HashData(e);
    rc = index_insert_term(v, fts1HashKey(e), fts1HashKeysize(e), p);
    if( rc!=SQLITE_OK ) break;
  }

  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    DocList *p = fts1HashData(e);
    docListDelete(p);
  }
  fts1HashClear(&terms);
  return rc;
}

/* Delete a row from the full-text index. */
static int index_delete(fulltext_vtab *v, sqlite_int64 iRow){
  char *pText = 0;
  int nText = 0;
  fts1Hash terms;
  fts1HashElem *e;
  DocList doclist;

  int rc = content_select(v, iRow, &pText, &nText);
  if( rc!=SQLITE_OK ) return rc;

  rc = buildTerms(&terms, v->pTokenizer, pText, nText, iRow);
  free(pText);
  if( rc!=SQLITE_OK ) return rc;

  /* Delete by inserting a doclist with no positions.  This will
  ** overwrite existing data as it is merged forward by
  ** index_insert_term().
  */
  docListInit(&doclist, DL_POSITIONS_OFFSETS, 0, 0);
  docListAddDocid(&doclist, iRow);

  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    rc = index_insert_term(v, fts1HashKey(e), fts1HashKeysize(e), &doclist);
    if( rc!=SQLITE_OK ) break;
  }
  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    DocList *p = fts1HashData(e);
    docListDelete(p);
  }
  fts1HashClear(&terms);
  docListDestroy(&doclist);

  if( rc!=SQLITE_OK ) return rc;
  return content_delete(v, iRow);
}

static int fulltextUpdate(sqlite3_vtab *pVtab, int nArg, sqlite3_value **ppArg,
                   sqlite_int64 *pRowid){
  fulltext_vtab *v = (fulltext_vtab *) pVtab;

  TRACE(("FTS1 Update %p\n", pVtab));
  if( nArg<2 ){
    return index_delete(v, sqlite3_value_int64(ppArg[0]));
  }

  if( sqlite3_value_type(ppArg[0]) != SQLITE_NULL ){
    return SQLITE_ERROR;   /* an update; not yet supported */
  }

  assert( nArg==3 );    /* ppArg[1] = rowid, ppArg[2] = content */
  return index_insert(v, ppArg[1],
                      sqlite3_value_blob(ppArg[2]),
                      sqlite3_value_bytes(ppArg[2]),
                      pRowid);
}

static const sqlite3_module fulltextModule = {
  0,
  fulltextCreate,
  fulltextConnect,
  fulltextBestIndex,
  fulltextDisconnect,
  fulltextDestroy,
  fulltextOpen,
  fulltextClose,
  fulltextFilter,
  fulltextNext,
  fulltextEof,
  fulltextColumn,
  fulltextRowid,
  fulltextUpdate
};

int sqlite3Fts1Init(sqlite3 *db){
 return sqlite3_create_module(db, "fts1", &fulltextModule, 0);
}

#if !SQLITE_CORE
int sqlite3_extension_init(sqlite3 *db, char **pzErrMsg,
                           const sqlite3_api_routines *pApi){
 SQLITE_EXTENSION_INIT2(pApi)
 return sqlite3Fts1Init(db);
}
#endif

#endif /* !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_FTS1) */
