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

typedef struct StringBuffer {
  int len;  /* length, not including null terminator */
  char *s;
} StringBuffer;

void initStringBuffer(StringBuffer *sb){
  sb->len = 0;
  sb->s = malloc(1);
  sb->s[0] = '\0';
}

void append(StringBuffer *sb, const char *zFrom){
  int nFrom = strlen(zFrom);
  sb->s = realloc(sb->s, sb->len + nFrom + 1);
  strcpy(sb->s + sb->len, zFrom);
  sb->len += nFrom;
}

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
 *     varint position;     (delta from previous position plus POS_BASE)
 *     varint startOffset;  (delta from previous startOffset)
 *     varint endOffset;    (delta from startOffset)
 *   }
 * }
 *
 * Here, array { X } means zero or more occurrences of X, adjacent in memory.
 *
 * A position list may hold positions for text in multiple columns.  A position
 * POS_COLUMN is followed by a varint containing the index of the column for
 * following positions in the list.  Any positions appearing before any
 * occurrences of POS_COLUMN are for column 0.
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
  int iLastColumn;    /* the last column written */
  int iLastPos;       /* the last position written */
  int iLastOffset;    /* the last start offset written */
} DocList;

enum {
  POS_END = 0,        /* end of this position list */
  POS_COLUMN,         /* followed by new column number */
  POS_BASE
};

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
  d->iLastColumn = 0;
  d->iLastPos = d->iLastOffset = 0;
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
    appendVarint(d, POS_END);  /* initially empty position list */
    d->iLastColumn = 0;
    d->iLastPos = d->iLastOffset = 0;
  }
}

/* helper function for docListAddPos and docListAddPosOffset */
static void addPos(DocList *d, int iColumn, int iPos){
  assert( d->nData>0 );
  --d->nData;  /* remove previous terminator */
  if( iColumn!=d->iLastColumn ){
    assert( iColumn>d->iLastColumn );
    appendVarint(d, POS_COLUMN);
    appendVarint(d, iColumn);
    d->iLastColumn = iColumn;
    d->iLastPos = d->iLastOffset = 0;
  }
  assert( iPos>=d->iLastPos );
  appendVarint(d, iPos-d->iLastPos+POS_BASE);
  d->iLastPos = iPos;
}

/* Add a position to the last position list in a doclist. */
static void docListAddPos(DocList *d, int iColumn, int iPos){
  assert( d->iType==DL_POSITIONS );
  addPos(d, iColumn, iPos);
  appendVarint(d, POS_END);  /* add new terminator */
}

static void docListAddPosOffset(DocList *d, int iColumn, int iPos,
                                int iStartOffset, int iEndOffset){
  assert( d->iType==DL_POSITIONS_OFFSETS );
  addPos(d, iColumn, iPos);

  assert( iStartOffset>=d->iLastOffset );
  appendVarint(d, iStartOffset-d->iLastOffset);
  d->iLastOffset = iStartOffset;

  assert( iEndOffset>=iStartOffset );
  appendVarint(d, iEndOffset-iStartOffset);

  appendVarint(d, POS_END);  /* add new terminator */
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
  int iLastColumn;
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
  r->iLastColumn = -1;
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
    pReader->iLastColumn = 0;
    pReader->iLastPos = 0;
  }
  return ret;
}

/* Read the next position and column index from a position list.
 * Returns the position, or -1 at the end of the list. */
static int readPosition(DocListReader *pReader, int *iColumn){
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
  if( i==POS_END ){
    pReader->iLastColumn = pReader->iLastPos = -1;
    *iColumn = -1;
    return -1;
  }
  if( i==POS_COLUMN ){
    pReader->p += getVarint32(pReader->p, &pReader->iLastColumn);
    pReader->iLastPos = 0;
    pReader->p += getVarint32(pReader->p, &i);
    assert( i>=POS_BASE );
  }
  pReader->iLastPos += ((int) i)-POS_BASE;
  if( iType>=DL_POSITIONS_OFFSETS ){
    /* Skip over offsets, ignoring them for now. */
    int iStart, iEnd;
    pReader->p += getVarint32(pReader->p, &iStart);
    pReader->p += getVarint32(pReader->p, &iEnd);
  }
  *iColumn = pReader->iLastColumn;
  return pReader->iLastPos;
}

/* Skip past the end of a position list. */
static void skipPositionList(DocListReader *pReader){
  DocList *p = pReader->pDoclist;
  if( p && p->iType>=DL_POSITIONS ){
    int iColumn;
    while( readPosition(pReader, &iColumn)!=-1 ){}
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
      int iPos, iCol;
      const char *zDiv = "";
      printf("(");
      while( (iPos = readPosition(&r, &iCol))>=0 ){
        printf("%s%d:%d", zDiv, iCol, iPos);
        zDiv = ":";
      }
      printf(")");
    }
  }
  printf("\n");
  fflush(stdout);
}
#endif /* SQLITE_DEBUG */

/* Trim the given doclist to contain only positions in column [iRestrictColumn],
 * discarding any docids without any remaining positions. */
static void docListRestrictColumn(DocList *in, int iRestrictColumn){
  DocListReader r;
  DocList out;

  assert( in->iType>=DL_POSITIONS );
  readerInit(&r, in);
  docListInit(&out, DL_POSITIONS, NULL, 0);

  while( !atEnd(&r) ){
    sqlite_int64 iDocid = readDocid(&r);
    int match = 0;
    int iPos, iColumn;
    while( (iPos = readPosition(&r, &iColumn)) != -1 ){
      if( iColumn==iRestrictColumn ){
        if( !match ){
          docListAddDocid(&out, iDocid);
          match = 1;
        }
        docListAddPos(&out, iColumn, iPos);
      }
    }
  }

  docListDestroy(in);
  *in = out;
}

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
  int iLeftCol, iLeftPos = readPosition(pLeft, &iLeftCol);
  int iRightCol, iRightPos = readPosition(pRight, &iRightCol);
  int match = 0;

  /* Loop until we've reached the end of both position lists. */
  while( iLeftPos!=-1 && iRightPos!=-1 ){
    if( iLeftCol==iRightCol && iLeftPos+1==iRightPos ){
      if( !match ){
        docListAddDocid(pOut, iDocid);
        match = 1;
      }
      if( pOut->iType>=DL_POSITIONS ){
        docListAddPos(pOut, iRightCol, iRightPos);
      }
      iLeftPos = readPosition(pLeft, &iLeftCol);
      iRightPos = readPosition(pRight, &iRightCol);
    }else if( iRightCol<iLeftCol ||
              (iRightCol==iLeftCol && iRightPos<iLeftPos+1) ){
      iRightPos = readPosition(pRight, &iRightCol);
    }else{
      iLeftPos = readPosition(pLeft, &iLeftCol);
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

static char *string_dup_n(const char *s, int n){
  char *str = malloc(n + 1);
  memcpy(str, s, n);
  str[n] = '\0';
  return str;
}

/* Duplicate a string; the caller must free() the returned string.
 * (We don't use strdup() since it's not part of the standard C library and
 * may not be available everywhere.) */
static char *string_dup(const char *s){
  return string_dup_n(s, strlen(s));
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

typedef enum QueryType {
  QUERY_GENERIC,   /* table scan */
  QUERY_ROWID,     /* lookup by rowid */
  QUERY_FULLTEXT   /* QUERY_FULLTEXT + [i] is a full-text search for column i*/
} QueryType;

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
  /* CONTENT_INSERT */ NULL,  /* generated in contentInsertStatement() */
  /* CONTENT_SELECT */ "select * from %_content where rowid = ?",
  /* CONTENT_DELETE */ "delete from %_content where rowid = ?",

  /* TERM_SELECT */
  "select rowid, doclist from %_term where term = ? and segment = ?",
  /* TERM_SELECT_ALL */
  "select doclist from %_term where term = ? order by segment",
  /* TERM_INSERT */
  "insert into %_term (rowid, term, segment, doclist) values (?, ?, ?, ?)",
  /* TERM_UPDATE */ "update %_term set doclist = ? where rowid = ?",
  /* TERM_DELETE */ "delete from %_term where rowid = ?",
};

typedef struct fulltext_vtab {
  sqlite3_vtab base;
  sqlite3 *db;
  const char *zName;               /* virtual table name */
  int nColumn;                     /* number of columns in virtual table */
  char **azColumn;                 /* column names.  malloced */
  char *zColumnList;               /* comma-separate list of column names */
  sqlite3_tokenizer *pTokenizer;   /* tokenizer for inserts and queries */

  /* Precompiled statements which we keep as long as the table is
  ** open.
  */
  sqlite3_stmt *pFulltextStatements[MAX_STMT];
} fulltext_vtab;

typedef struct fulltext_cursor {
  sqlite3_vtab_cursor base;
  QueryType iCursorType;

  sqlite3_stmt *pStmt;
  int eof;

  DocListReader result;  /* used when iCursorType == QUERY_FULLTEXT */ 
} fulltext_cursor;

static struct fulltext_vtab *cursor_vtab(fulltext_cursor *c){
  return (fulltext_vtab *) c->base.pVtab;
}

static const sqlite3_module fulltextModule;   /* forward declaration */

/* Return a dynamically generated statement of the form
 *   insert into %_content (rowid, ...) values (?, ...)
 */
static const char *contentInsertStatement(fulltext_vtab *v){
  StringBuffer sb;
  int i;

  initStringBuffer(&sb);
  append(&sb, "insert into %_content (rowid, ");
  append(&sb, v->zColumnList);
  append(&sb, ") values (?");
  for(i=0; i<v->nColumn; ++i)
    append(&sb, ", ?");
  append(&sb, ")");
  return sb.s;
}

/* Puts a freshly-prepared statement determined by iStmt in *ppStmt.
** If the indicated statement has never been prepared, it is prepared
** and cached, otherwise the cached version is reset.
*/
static int sql_get_statement(fulltext_vtab *v, fulltext_statement iStmt,
                             sqlite3_stmt **ppStmt){
  assert( iStmt<MAX_STMT );
  if( v->pFulltextStatements[iStmt]==NULL ){
    const char *zStmt;
    int rc;
    zStmt = iStmt==CONTENT_INSERT_STMT ? contentInsertStatement(v) : 
                                         fulltext_zStatement[iStmt];
    rc = sql_prepare(v->db, v->zName, &v->pFulltextStatements[iStmt],
                         zStmt);
    if( iStmt==CONTENT_INSERT_STMT ) free((void *) zStmt);
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

/* insert into %_content (rowid, ...) values ([rowid], [pValues]) */
static int content_insert(fulltext_vtab *v, sqlite3_value *rowid,
                          sqlite3_value **pValues){
  sqlite3_stmt *s;
  int i;
  int rc = sql_get_statement(v, CONTENT_INSERT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_value(s, 1, rowid);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<v->nColumn; ++i){
    rc = sqlite3_bind_value(s, 2+i, pValues[i]);
    if( rc!=SQLITE_OK ) return rc;
  }

  return sql_single_step_statement(v, CONTENT_INSERT_STMT, &s);
}

void freeStringArray(int nString, const char **pString){
  int i;

  for (i=0 ; i < nString ; ++i) {
    free((void *) pString[i]);
  }
  free((void *) pString);
}

/* select * from %_content where rowid = [iRow]
 * The caller must delete the returned array and all strings in it.
 *
 * TODO: Perhaps we should return pointer/length strings here for consistency
 * with other code which uses pointer/length. */
static int content_select(fulltext_vtab *v, sqlite_int64 iRow,
                          const char ***pValues){
  sqlite3_stmt *s;
  const char **values;
  int i;
  int rc;

  *pValues = NULL;

  rc = sql_get_statement(v, CONTENT_SELECT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int64(s, 1, iRow);
  if( rc!=SQLITE_OK ) return rc;

  rc = sql_step_statement(v, CONTENT_SELECT_STMT, &s);
  if( rc!=SQLITE_ROW ) return rc;

  values = (const char **) malloc(v->nColumn * sizeof(const char *));
  for(i=0; i<v->nColumn; ++i){
    values[i] = string_dup((char*)sqlite3_column_text(s, i));
  }

  /* We expect only one row.  We must execute another sqlite3_step()
   * to complete the iteration; otherwise the table will remain locked. */
  rc = sqlite3_step(s);
  if( rc==SQLITE_DONE ){
    *pValues = values;
    return SQLITE_OK;
  }

  freeStringArray(v->nColumn, values);
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
**
** Each document consists of 1 or more "columns".  The number of
** columns is v->nColumn.  If iColumn==v->nColumn, then return
** position information about all columns.  If iColumn<v->nColumn,
** then only return position information about the iColumn-th column
** (where the first column is 0).
*/
static int term_select_all(
  fulltext_vtab *v,     /* The fulltext index we are querying against */
  int iColumn,          /* If <nColumn, only look at the iColumn-th column */
  const char *pTerm,    /* The term whose posting lists we want */
  int nTerm,            /* Number of bytes in pTerm */
  DocList *out          /* Write the resulting doclist here */
){
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

    if( iColumn<v->nColumn ){   /* querying a single column */
      docListRestrictColumn(&old, iColumn);
    }

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

/* insert into %_term (rowid, term, segment, doclist)
               values ([piRowid], [pTerm], [iSegment], [doclist])
** Lets sqlite select rowid if piRowid is NULL, else uses *piRowid.
**
** NOTE(shess) piRowid is IN, with values of "space of int64" plus
** null, it is not used to pass data back to the caller.
*/
static int term_insert(fulltext_vtab *v, sqlite_int64 *piRowid,
                       const char *pTerm, int nTerm,
                       int iSegment, DocList *doclist){
  sqlite3_stmt *s;
  int rc = sql_get_statement(v, TERM_INSERT_STMT, &s);
  if( rc!=SQLITE_OK ) return rc;

  if( piRowid==NULL ){
    rc = sqlite3_bind_null(s, 1);
  }else{
    rc = sqlite3_bind_int64(s, 1, *piRowid);
  }
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_text(s, 2, pTerm, nTerm, SQLITE_STATIC);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_int(s, 3, iSegment);
  if( rc!=SQLITE_OK ) return rc;

  rc = sqlite3_bind_blob(s, 4, doclist->pData, doclist->nData, SQLITE_STATIC);
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

/*
** Free the memory used to contain a fulltext_vtab structure.
*/
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
  
  free(v->azColumn);
  free(v->zColumnList);
  free(v);
}

/*
** Token types for parsing the arguments to xConnect or xCreate.
*/
#define TOKEN_EOF         0    /* End of file */
#define TOKEN_SPACE       1    /* Any kind of whitespace */
#define TOKEN_ID          2    /* An identifier */
#define TOKEN_STRING      3    /* A string literal */
#define TOKEN_PUNCT       4    /* A single punctuation character */

/*
** If X is a character that can be used in an identifier then
** IdChar(X) will be true.  Otherwise it is false.
**
** For ASCII, any character with the high-order bit set is
** allowed in an identifier.  For 7-bit characters, 
** sqlite3IsIdChar[X] must be 1.
**
** Ticket #1066.  the SQL standard does not allow '$' in the
** middle of identfiers.  But many SQL implementations do. 
** SQLite will allow '$' in identifiers for compatibility.
** But the feature is undocumented.
*/
static const char isIdChar[] = {
/* x0 x1 x2 x3 x4 x5 x6 x7 x8 x9 xA xB xC xD xE xF */
    0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  /* 2x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,  /* 3x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  /* 4x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 1,  /* 5x */
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,  /* 6x */
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0,  /* 7x */
};
#define IdChar(C)  (((c=C)&0x80)!=0 || (c>0x1f && isIdChar[c-0x20]))


/*
** Return the length of the token that begins at z[0]. 
** Store the token type in *tokenType before returning.
*/
static int getToken(const char *z, int *tokenType){
  int i, c;
  switch( *z ){
    case 0: {
      *tokenType = TOKEN_EOF;
      return 0;
    }
    case ' ': case '\t': case '\n': case '\f': case '\r': {
      for(i=1; isspace(z[i]); i++){}
      *tokenType = TOKEN_SPACE;
      return i;
    }
    case '\'':
    case '"': {
      int delim = z[0];
      for(i=1; (c=z[i])!=0; i++){
        if( c==delim ){
          if( z[i+1]==delim ){
            i++;
          }else{
            break;
          }
        }
      }
      *tokenType = TOKEN_STRING;
      return i + (c!=0);
    }
    case '[': {
      for(i=1, c=z[0]; c!=']' && (c=z[i])!=0; i++){}
      *tokenType = TOKEN_ID;
      return i;
    }
    default: {
      if( !IdChar(*z) ){
        break;
      }
      for(i=1; IdChar(z[i]); i++){}
      *tokenType = TOKEN_ID;
      return i;
    }
  }
  *tokenType = TOKEN_PUNCT;
  return 1;
}

/*
** A token extracted from a string is an instance of the following
** structure.
*/
typedef struct Token {
  const char *z;       /* Pointer to token text.  Not '\000' terminated */
  short int n;         /* Length of the token text in bytes. */
} Token;

/*
** Given a input string (which is really one of the argv[] parameters
** passed into xConnect or xCreate) split the string up into tokens.
** Return an array of pointers to '\000' terminated strings, one string
** for each non-whitespace token.
**
** The returned array is terminated by a single NULL pointer.
**
** Space to hold the returned array is obtained from a single
** malloc and should be freed by passing the return value to free().
** The individual strings within the token list are all a part of
** the single memory allocation and will all be freed at once.
*/
static char **tokenizeString(const char *z, int *pnToken){
  int nToken = 0;
  Token *aToken = malloc( strlen(z) * sizeof(aToken[0]) );
  int n = 1;
  int e, i;
  int totalSize = 0;
  char **azToken;
  char *zCopy;
  while( n>0 ){
    n = getToken(z, &e);
    if( e!=TOKEN_SPACE ){
      aToken[nToken].z = z;
      aToken[nToken].n = n;
      nToken++;
      totalSize += n+1;
    }
    z += n;
  }
  azToken = (char**)malloc( nToken*sizeof(char*) + totalSize );
  zCopy = (char*)&azToken[nToken];
  nToken--;
  for(i=0; i<nToken; i++){
    azToken[i] = zCopy;
    n = aToken[i].n;
    memcpy(zCopy, aToken[i].z, n);
    zCopy[n] = 0;
    zCopy += n+1;
  }
  azToken[nToken] = 0;
  free(aToken);
  *pnToken = nToken;
  return azToken;
}

/*
** Convert an SQL-style quoted string into a normal string by removing
** the quote characters.  The conversion is done in-place.  If the
** input does not begin with a quote character, then this routine
** is a no-op.
**
** Examples:
**
**     "abc"   becomes   abc
**     'xyz'   becomes   xyz
**     [pqr]   becomes   pqr
**     `mno`   becomes   mno
*/
void dequoteString(char *z){
  int quote;
  int i, j;
  if( z==0 ) return;
  quote = z[0];
  switch( quote ){
    case '\'':  break;
    case '"':   break;
    case '`':   break;                /* For MySQL compatibility */
    case '[':   quote = ']';  break;  /* For MS SqlServer compatibility */
    default:    return;
  }
  for(i=1, j=0; z[i]; i++){
    if( z[i]==quote ){
      if( z[i+1]==quote ){
        z[j++] = quote;
        i++;
      }else{
        z[j++] = 0;
        break;
      }
    }else{
      z[j++] = z[i];
    }
  }
}

/*
** The input azIn is a NULL-terminated list of tokens.  Remove the first
** token and all punctuation tokens.  Remove the quotes from
** around string literal tokens.
**
** Example:
**
**     input:      tokenize chinese ( 'simplifed' , 'mixed' )
**     output:     chinese simplifed mixed
**
** Another example:
**
**     input:      delimiters ( '[' , ']' , '...' )
**     output:     [ ] ...
*/
void tokenListToIdList(char **azIn){
  int i, j;
  if( azIn ){
    for(i=0, j=-1; azIn[i]; i++){
      if( isalnum(azIn[i][0]) || azIn[i][1] ){
        dequoteString(azIn[i]);
        if( j>=0 ){
          azIn[j] = azIn[i];
        }
        j++;
      }
    }
    azIn[j] = 0;
  }
}


/*
** Find the first alphanumeric token in the string zIn.  Null-terminate
** this token.  Remove any quotation marks.  And return a pointer to
** the result.
*/
static char *firstToken(char *zIn, char **pzTail){
  int i, n, ttype;
  i = 0;
  while(1){
    n = getToken(zIn, &ttype);
    if( ttype==TOKEN_SPACE ){
      zIn += n;
    }else if( ttype==TOKEN_EOF ){
      *pzTail = zIn;
      return 0;
    }else{
      zIn[n] = 0;
      *pzTail = &zIn[1];
      dequoteString(zIn);
      return zIn;
    }
  }
  /*NOTREACHED*/
}

/* Return true if...
**
**   *  s begins with the string t, ignoring case
**   *  s is longer than t
**   *  The first character of s beyond t is not a alphanumeric
** 
** Ignore leading space in *s.
**
** To put it another way, return true if the first token of
** s[] is t[].
*/
static int startsWith(const char *s, const char *t){
  while( isspace(*s) ){ s++; }
  while( *t ){
    if( tolower(*s++)!=tolower(*t++) ) return 0;
  }
  return *s!='_' && !isalnum(*s);
}

/*
** An instance of this structure defines the "spec" of a the
** full text index.  This structure is populated by parseSpec
** and use by fulltextConnect and fulltextCreate.
*/
typedef struct TableSpec {
  const char *zName;       /* Name of the full-text index */
  int nColumn;             /* Number of columns to be indexed */
  char **azColumn;         /* Original names of columns to be indexed */
  char *zColumnList;       /* Comma-separated list of names for %_content */
  char **azTokenizer;      /* Name of tokenizer and its arguments */
  char **azDelimiter;      /* Delimiters used for snippets */
} TableSpec;

/*
** Reclaim all of the memory used by a TableSpec
*/
void clearTableSpec(TableSpec *p) {
  free(p->azColumn);
  free(p->zColumnList);
  free(p->azTokenizer);
  free(p->azDelimiter);
}

/* Parse a CREATE VIRTUAL TABLE statement, which looks like this:
 *
 * CREATE VIRTUAL TABLE email
 *        USING fts1(subject, body, tokenize mytokenizer(myarg))
 *
 * We return parsed information in a TableSpec structure.
 * 
 */
int parseSpec(TableSpec *pSpec, int argc, const char *const*argv, char**pzErr){
  int i, j, n;
  char *z, *zDummy;
  char **azArg;
  const char *zTokenizer = 0;    /* argv[] entry describing the tokenizer */
  const char *zDelimiter = 0;    /* argv[] entry describing the delimiters */

  assert( argc>=3 );
  /* Current interface:
  ** argv[0] - module name
  ** argv[1] - database name
  ** argv[2] - table name
  ** argv[3..] - columns, optionally followed by tokenizer specification
  **             and snippet delimiters specification.
  */

  /* Make a copy of the complete argv[][] array in a single allocation.
  ** The argv[][] array is read-only and transient.  We can write to the
  ** copy in order to modify things and the copy is persistent.
  */
  memset(pSpec, 0, sizeof(pSpec));
  for(i=n=0; i<argc; i++){
    n += strlen(argv[i]) + 1;
  }
  azArg = malloc( sizeof(char*)*argc + n );
  if( azArg==0 ){
    return SQLITE_NOMEM;
  }
  z = (char*)&azArg[argc];
  for(i=0; i<argc; i++){
    azArg[i] = z;
    strcpy(z, argv[i]);
    z += strlen(z)+1;
  }

  /* Identify the column names and the tokenizer and delimiter arguments
  ** in the argv[][] array.
  */
  pSpec->zName = azArg[2];
  pSpec->nColumn = 0;
  pSpec->azColumn = azArg;
  zTokenizer = "tokenize simple";
  zDelimiter = "delimiters('[',']','...')";
  n = 0;
  for(i=3, j=0; i<argc; ++i){
    if( startsWith(azArg[i],"tokenize") ){
      zTokenizer = azArg[i];
    }else if( startsWith(azArg[i],"delimiters") ){
      zDelimiter = azArg[i];
    }else{
      z = azArg[pSpec->nColumn] = firstToken(azArg[i], &zDummy);
      pSpec->nColumn++;
      n += strlen(z) + 6;
    }
  }
  if( pSpec->nColumn==0 ){
    azArg[0] = "content";
    pSpec->nColumn = 1;
  }

  /*
  ** Construct the comma-separated list of column names.
  **
  ** Each column name will be of the form cNNAAAA
  ** where NN is the column number and AAAA is the sanitized
  ** column name.  "sanitized" means that special characters are
  ** converted to "_".  The cNN prefix guarantees that all column
  ** names are unique.
  **
  ** The AAAA suffix is not strictly necessary.  It is included
  ** for the convenience of people who might examine the generated
  ** %_content table and wonder what the columns are used for.
  */
  z = pSpec->zColumnList = malloc( n );
  if( z==0 ){
    clearTableSpec(pSpec);
    return SQLITE_NOMEM;
  }
  for(i=0; i<pSpec->nColumn; i++){
    sqlite3_snprintf(n, z, "c%d%s", i, azArg[i]);
    for(j=0; z[j]; j++){
      if( !isalnum(z[j]) ) z[j] = '_';
    }
    z[j] = ',';
    z += j+1;
  }
  z[-1] = 0;

  /*
  ** Parse the tokenizer specification string.
  */
  pSpec->azTokenizer = tokenizeString(zTokenizer, &n);
  tokenListToIdList(pSpec->azTokenizer);

  /*
  ** Parse the delimiter specification string.
  */
  pSpec->azDelimiter = tokenizeString(zDelimiter, &n);
  tokenListToIdList(pSpec->azDelimiter);

  return SQLITE_OK;
}

/*
** Generate a CREATE TABLE statement that describes the schema of
** the virtual table.  Return a pointer to this schema.  
**
** If the addAllColumn parameter is true, then add a column named
** "_all" to the end of the schema.
**
** Space is obtained from sqlite3_mprintf() and should be freed
** using sqlite3_free().
*/
static char *fulltextSchema(
  int nColumn,                  /* Number of columns */
  const char *const* azColumn   /* List of columns */
){
  int i;
  char *zSchema, *zNext;
  const char *zSep = "(";
  zSchema = sqlite3_mprintf("CREATE TABLE x");
  for(i=0; i<nColumn; i++){
    zNext = sqlite3_mprintf("%s%s%Q", zSchema, zSep, azColumn[i]);
    sqlite3_free(zSchema);
    zSchema = zNext;
    zSep = ",";
  }
  zNext = sqlite3_mprintf("%s,_all)", zSchema);
  sqlite3_free(zSchema);
  return zNext;
}

/*
** Build a new sqlite3_vtab structure that will describe the
** fulltext index defined by spec.
*/
static int constructVtab(
  sqlite3 *db,              /* The SQLite database connection */
  TableSpec *spec,          /* Parsed spec information from parseSpec() */
  sqlite3_vtab **ppVTab,    /* Write the resulting vtab structure here */
  char **pzErr              /* Write any error message here */
){
  int rc;
  int n;
  fulltext_vtab *v = 0;
  const sqlite3_tokenizer_module *m = NULL;
  char *schema;

  v = (fulltext_vtab *) malloc(sizeof(fulltext_vtab));
  if( v==0 ) return SQLITE_NOMEM;
  memset(v, 0, sizeof(*v));
  /* sqlite will initialize v->base */
  v->db = db;
  v->zName = spec->zName;   /* Freed when azColumn is freed */
  v->nColumn = spec->nColumn;
  v->zColumnList = spec->zColumnList;
  spec->zColumnList = 0;
  v->azColumn = spec->azColumn;
  spec->azColumn = 0;

  if( spec->azTokenizer==0 ){
    return SQLITE_NOMEM;
  }
  /* TODO(shess) For now, add new tokenizers as else if clauses. */
  if( spec->azTokenizer[0]==0 || !strcmp(spec->azTokenizer[0], "simple") ){
    sqlite3Fts1SimpleTokenizerModule(&m);
  } else {
    *pzErr = sqlite3_mprintf("unknown tokenizer: %s", spec->azTokenizer[0]);
    rc = SQLITE_ERROR;
    goto err;
  }
  for(n=0; spec->azTokenizer[n]; n++){}
  if( n ){
    rc = m->xCreate(n-1, (const char*const*)&spec->azTokenizer[1],
                    &v->pTokenizer);
  }else{
    rc = m->xCreate(0, 0, &v->pTokenizer);
  }
  if( rc!=SQLITE_OK ) goto err;
  v->pTokenizer->pModule = m;

  /* TODO: verify the existence of backing tables foo_content, foo_term */

  schema = fulltextSchema(v->nColumn, (const char*const*)v->azColumn);
  rc = sqlite3_declare_vtab(db, schema);
  sqlite3_free(schema);
  if( rc!=SQLITE_OK ) goto err;

  memset(v->pFulltextStatements, 0, sizeof(v->pFulltextStatements));

  *ppVTab = &v->base;
  TRACE(("FTS1 Connect %p\n", v));

  return rc;

err:
  fulltext_vtab_destroy(v);
  return rc;
}

static int fulltextConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVTab,
  char **pzErr
){
  TableSpec spec;
  int rc = parseSpec(&spec, argc, argv, pzErr);
  if( rc!=SQLITE_OK ) return rc;

  rc = constructVtab(db, &spec, ppVTab, pzErr);
  clearTableSpec(&spec);
  return rc;
}

  /* The %_content table holds the text of each document, with
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
static int fulltextCreate(sqlite3 *db, void *pAux,
                          int argc, const char * const *argv,
                          sqlite3_vtab **ppVTab, char **pzErr){
  int rc;
  TableSpec spec;
  char *schema;
  TRACE(("FTS1 Create\n"));

  rc = parseSpec(&spec, argc, argv, pzErr);
  if( rc!=SQLITE_OK ) return rc;

  schema = sqlite3_mprintf("CREATE TABLE %%_content(%s)", spec.zColumnList);
  rc = sql_exec(db, spec.zName, schema);
  sqlite3_free(schema);
  if( rc!=SQLITE_OK ) goto out;

  rc = sql_exec(db, spec.zName,
    "create table %_term(term text, segment integer, doclist blob, "
                        "primary key(term, segment));");
  if( rc!=SQLITE_OK ) goto out;

  rc = constructVtab(db, &spec, ppVTab, pzErr);

out:
  clearTableSpec(&spec);
  return rc;
}

/* Decide how to handle an SQL query. */
static int fulltextBestIndex(sqlite3_vtab *pVTab, sqlite3_index_info *pInfo){
  int i;

  for(i=0; i<pInfo->nConstraint; ++i){
    const struct sqlite3_index_constraint *pConstraint;
    pConstraint = &pInfo->aConstraint[i];
    if( pConstraint->usable ) {
      if( pConstraint->iColumn==-1 &&
          pConstraint->op==SQLITE_INDEX_CONSTRAINT_EQ ){
        pInfo->idxNum = QUERY_ROWID;      /* lookup by rowid */
      } else if( pConstraint->iColumn>=0 &&
                 pConstraint->op==SQLITE_INDEX_CONSTRAINT_MATCH ){
        /* full-text search */
        pInfo->idxNum = QUERY_FULLTEXT + pConstraint->iColumn;
      } else continue;

      pInfo->aConstraintUsage[i].argvIndex = 1;
      pInfo->aConstraintUsage[i].omit = 1;

      /* An arbitrary value for now.
       * TODO: Perhaps rowid matches should be considered cheaper than
       * full-text searches. */
      pInfo->estimatedCost = 1.0;   

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
  if( c->iCursorType < QUERY_FULLTEXT ){
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
  } else {  /* full-text query */
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
  int iColumn,          /* column to restrict to.  No restrition if >=nColumn */
  QueryTerm *pQTerm,    /* Term we are looking for, or 1st term of a phrase */
  DocList **ppResult    /* Write the result here */
){
  DocList *pLeft, *pRight, *pNew;
  int i, rc;

  pLeft = docListNew(DL_POSITIONS);
  rc = term_select_all(v, iColumn, pQTerm->pTerm, pQTerm->nTerm, pLeft);
  if( rc ) return rc;
  for(i=1; i<=pQTerm->nPhrase; i++){
    pRight = docListNew(DL_POSITIONS);
    rc = term_select_all(v, iColumn, pQTerm[i].pTerm, pQTerm[i].nTerm, pRight);
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

  if( inPhrase ){  /* unmatched quote */
    queryDestroy(pQuery);
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

/* Perform a full-text query using the search expression in
** pInput[0..nInput-1].  Return a list of matching documents
** in pResult.
*/
static int fulltextQuery(fulltext_vtab *v, int iColumn,
                         const char *pInput, int nInput, DocList **pResult){
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

    rc = docListOfTerm(v, iColumn, &q.pTerms[i], &pRight);
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
    rc = docListOfTerm(v, iColumn, &q.pTerms[i], &pRight);
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

/*
** This is the xFilter interface for the virtual table.  See
** the virtual table xFilter method documentation for additional
** information.
**
** If idxNum==QUERY_GENERIC then do a full table scan against
** the %_content table.
**
** If idxNum==QUERY_ROWID then do a rowid lookup for a single entry
** in the %_content table.
**
** If idxNum>=QUERY_FULLTEXT then use the full text index.  The
** column on the left-hand side of the MATCH operator is column
** number idxNum-QUERY_FULLTEXT, 0 indexed.  argv[0] is the right-hand
** side of the MATCH operator.
*/
static int fulltextFilter(
  sqlite3_vtab_cursor *pCursor,     /* The cursor used for this query */
  int idxNum, const char *idxStr,   /* Which indexing scheme to use */
  int argc, sqlite3_value **argv    /* Arguments for the indexing scheme */
){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  fulltext_vtab *v = cursor_vtab(c);
  int rc;
  char *zSql;

  TRACE(("FTS1 Filter %p\n",pCursor));

  zSql = sqlite3_mprintf("select rowid, * from %%_content %s",
                          idxNum==QUERY_GENERIC ? "" : "where rowid=?");
  rc = sql_prepare(v->db, v->zName, &c->pStmt, zSql);
  sqlite3_free(zSql);
  if( rc!=SQLITE_OK ) goto out;

  c->iCursorType = idxNum;
  switch( idxNum ){
    case QUERY_GENERIC:
      break;

    case QUERY_ROWID:
      rc = sqlite3_bind_int64(c->pStmt, 1, sqlite3_value_int64(argv[0]));
      if( rc!=SQLITE_OK ) goto out;
      break;

    default:   /* full-text search */
    {
      const char *zQuery = (const char *)sqlite3_value_text(argv[0]);
      DocList *pResult;
      assert( idxNum<=QUERY_FULLTEXT+v->nColumn);
      assert( argc==1 );
      rc = fulltextQuery(v, idxNum-QUERY_FULLTEXT, zQuery, -1, &pResult);
      if( rc!=SQLITE_OK ) goto out;
      readerInit(&c->result, pResult);
      break;
    }
  }

  rc = fulltextNext(pCursor);

out:
  return rc;
}

static int fulltextEof(sqlite3_vtab_cursor *pCursor){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  return c->eof;
}

static int fulltextColumn(sqlite3_vtab_cursor *pCursor,
                          sqlite3_context *pContext, int idxCol){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;
  fulltext_vtab *v = cursor_vtab(c);
  const char *s;

  if( idxCol==v->nColumn ){  /* a request for _all */
    sqlite3_result_null(pContext);
  } else {
    assert( idxCol<v->nColumn );
    s = (const char *) sqlite3_column_text(c->pStmt, idxCol+1);
    sqlite3_result_text(pContext, s, -1, SQLITE_TRANSIENT);
  }

  return SQLITE_OK;
}

static int fulltextRowid(sqlite3_vtab_cursor *pCursor, sqlite_int64 *pRowid){
  fulltext_cursor *c = (fulltext_cursor *) pCursor;

  *pRowid = sqlite3_column_int64(c->pStmt, 0);
  return SQLITE_OK;
}

/* Add all terms/positions in [zText] to the given hash table. */
static int buildTerms(fulltext_vtab *v, fts1Hash *terms, int iColumn,
                      const char *zText, int nText, sqlite_int64 iDocid){
  sqlite3_tokenizer *pTokenizer = v->pTokenizer;
  sqlite3_tokenizer_cursor *pCursor;
  const char *pToken;
  int nTokenBytes;
  int iStartOffset, iEndOffset, iPosition;
  int rc;

  rc = pTokenizer->pModule->xOpen(pTokenizer, zText, nText, &pCursor);
  if( rc!=SQLITE_OK ) return rc;

  pCursor->pTokenizer = pTokenizer;
  while( SQLITE_OK==pTokenizer->pModule->xNext(pCursor,
                                               &pToken, &nTokenBytes,
                                               &iStartOffset, &iEndOffset,
                                               &iPosition) ){
    DocList *p;

    /* Positions can't be negative; we use -1 as a terminator internally. */
    if( iPosition<0 ){
      pTokenizer->pModule->xClose(pCursor);
      return SQLITE_ERROR;
    }

    p = fts1HashFind(terms, pToken, nTokenBytes);
    if( p==NULL ){
      p = docListNew(DL_POSITIONS_OFFSETS);
      docListAddDocid(p, iDocid);
      fts1HashInsert(terms, pToken, nTokenBytes, p);
    }
    docListAddPosOffset(p, iColumn, iPosition, iStartOffset, iEndOffset);
  }

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
    rc = term_insert(v, NULL, pTerm, nTerm, iSegment, &doclist);
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
  while( (rc=term_insert(v, &iIndexRow, pTerm, nTerm, iSegment,
                         &doclist))!=SQLITE_OK ){
    sqlite_int64 iSegmentRow;
    DocList old;
    int rc2;

    /* Retain old error in case the term_insert() error was really an
    ** error rather than a bounced insert.
    */
    rc2 = term_select(v, pTerm, nTerm, iSegment, &iSegmentRow, &old);
    if( rc2!=SQLITE_ROW ) goto err;

    rc = term_delete(v, iSegmentRow);
    if( rc!=SQLITE_OK ) goto err;

    /* Reusing lowest-number deleted row keeps the index smaller. */
    if( iSegmentRow<iIndexRow ) iIndexRow = iSegmentRow;

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
                        sqlite3_value **pValues,
                        sqlite_int64 *piRowid){
  int i;
  fts1Hash terms;  /* maps term string -> PosList */
  fts1HashElem *e;
  int rc;

  rc = content_insert(v, pRequestRowid, pValues);
  if( rc!=SQLITE_OK ) return rc;
  *piRowid = sqlite3_last_insert_rowid(v->db);

  fts1HashInit(&terms, FTS1_HASH_STRING, 1);
  for(i = 0; i < v->nColumn ; ++i){
    rc = buildTerms(v, &terms, i, (char*)sqlite3_value_text(pValues[i]), -1,
                    *piRowid);
    if( rc!=SQLITE_OK ) goto out;
  }

  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    DocList *p = fts1HashData(e);
    rc = index_insert_term(v, fts1HashKey(e), fts1HashKeysize(e), p);
    if( rc!=SQLITE_OK ) break;
  }

out:
  for(e=fts1HashFirst(&terms); e; e=fts1HashNext(e)){
    DocList *p = fts1HashData(e);
    docListDelete(p);
  }
  fts1HashClear(&terms);
  return rc;
}

/* Delete a row from the full-text index. */
static int index_delete(fulltext_vtab *v, sqlite_int64 iRow){
  const char **pValues;
  fts1Hash terms;
  int i;
  fts1HashElem *e;
  DocList doclist;

  int rc = content_select(v, iRow, &pValues);
  if( rc!=SQLITE_OK ) return rc;

  fts1HashInit(&terms, FTS1_HASH_STRING, 1);
  for(i = 0 ; i < v->nColumn; ++i) {
    rc = buildTerms(v, &terms, i, pValues[i], -1, iRow);
    if( rc!=SQLITE_OK ) goto out;
  }

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

out:
  freeStringArray(v->nColumn, pValues);
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

  /* ppArg[1] = rowid
   * ppArg[2..2+v->nColumn-1] = values
   * ppArg[2+v->nColumn] = value for _all (we ignore this) */
  assert( nArg==2+v->nColumn+1);    

  return index_insert(v, ppArg[1], &ppArg[2], pRowid);
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
