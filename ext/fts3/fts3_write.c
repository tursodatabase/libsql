/*
** 2009 Oct 23
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
** This file is part of the SQLite FTS3 extension module. Specifically,
** this file contains code to insert, update and delete rows from FTS3
** tables. It also contains code to merge FTS3 b-tree segments. Some
** of the sub-routines used to merge segments are also used by the query 
** code in fts3.c.
*/

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_FTS3)

#include "fts3Int.h"
#include <string.h>
#include <assert.h>
#include <stdlib.h>

typedef struct PendingList PendingList;
typedef struct SegmentNode SegmentNode;
typedef struct SegmentWriter SegmentWriter;

/*
** Data structure used while accumulating terms in the pending-terms hash
** table. The hash table entry maps from term (a string) to a malloced
** instance of this structure.
*/
struct PendingList {
  int nData;
  char *aData;
  int nSpace;
  sqlite3_int64 iLastDocid;
  sqlite3_int64 iLastCol;
  sqlite3_int64 iLastPos;
};

/*
** An instance of this structure is used to iterate through the terms on
** a contiguous set of segment b-tree leaf nodes. Although the details of
** this structure are only manipulated by code in this file, opaque handles
** of type Fts3SegReader* are also used by code in fts3.c to iterate through
** terms when querying the full-text index. See functions:
**
**   sqlite3Fts3SegReaderNew()
**   sqlite3Fts3SegReaderFree()
**   sqlite3Fts3SegReaderIterate()
*/
struct Fts3SegReader {
  int iIdx;                       /* Index within level */
  sqlite3_int64 iStartBlock;
  sqlite3_int64 iEndBlock;
  sqlite3_stmt *pStmt;            /* SQL Statement to access leaf nodes */
  char *aNode;                    /* Pointer to node data (or NULL) */
  int nNode;                      /* Size of buffer at aNode (or 0) */
  int nTermAlloc;                 /* Allocated size of zTerm buffer */

  /* Variables set by fts3SegReaderNext(). These may be read directly
  ** by the caller. They are valid from the time SegmentReaderNew() returns
  ** until SegmentReaderNext() returns something other than SQLITE_OK
  ** (i.e. SQLITE_DONE).
  */
  int nTerm;                      /* Number of bytes in current term */
  char *zTerm;                    /* Pointer to current term */
  char *aDoclist;                 /* Pointer to doclist of current entry */
  int nDoclist;                   /* Size of doclist in current entry */

  /* The following variables are used to iterate through the current doclist */
  char *pOffsetList;
  sqlite3_int64 iDocid;
};

/*
** An instance of this structure is used to create a segment b-tree in the
** database. The internal details of this type are only accessed by the
** following functions:
**
**   fts3SegWriterAdd()
**   fts3SegWriterFlush()
**   fts3SegWriterFree()
*/
struct SegmentWriter {
  SegmentNode *pTree;             /* Pointer to interior tree structure */
  sqlite3_int64 iFirst;           /* First slot in %_segments written */
  sqlite3_int64 iFree;            /* Next free slot in %_segments */
  char *zTerm;                    /* Pointer to previous term buffer */
  int nTerm;                      /* Number of bytes in zTerm */
  int nMalloc;                    /* Size of malloc'd buffer at zMalloc */
  char *zMalloc;                  /* Malloc'd space (possibly) used for zTerm */
  int nSize;                      /* Size of allocation at aData */
  int nData;                      /* Bytes of data in aData */
  char *aData;                    /* Pointer to block from malloc() */
};

/*
** Type SegmentNode is used by the following three functions to create
** the interior part of the segment b+-tree structures (everything except
** the leaf nodes). These functions and type are only ever used by code
** within the fts3SegWriterXXX() family of functions described above.
**
**   fts3NodeAddTerm()
**   fts3NodeWrite()
**   fts3NodeFree()
*/
struct SegmentNode {
  SegmentNode *pParent;           /* Parent node (or NULL for root node) */
  SegmentNode *pRight;            /* Pointer to right-sibling */
  SegmentNode *pLeftmost;         /* Pointer to left-most node of this depth */
  int nEntry;                     /* Number of terms written to node so far */
  char *zTerm;                    /* Pointer to previous term buffer */
  int nTerm;                      /* Number of bytes in zTerm */
  int nMalloc;                    /* Size of malloc'd buffer at zMalloc */
  char *zMalloc;                  /* Malloc'd space (possibly) used for zTerm */
  int nData;                      /* Bytes of valid data so far */
  char *aData;                    /* Node data */
};

/*
** Valid values for the second argument to fts3SqlStmt().
*/
#define SQL_DELETE_CONTENT             0
#define SQL_IS_EMPTY                   1
#define SQL_DELETE_ALL_CONTENT         2 
#define SQL_DELETE_ALL_SEGMENTS        3
#define SQL_DELETE_ALL_SEGDIR          4
#define SQL_SELECT_CONTENT_BY_ROWID    5
#define SQL_NEXT_SEGMENT_INDEX         6
#define SQL_INSERT_SEGMENTS            7
#define SQL_NEXT_SEGMENTS_ID           8
#define SQL_INSERT_SEGDIR              9
#define SQL_SELECT_LEVEL              10
#define SQL_SELECT_ALL_LEVEL          11
#define SQL_SELECT_LEVEL_COUNT        12
#define SQL_SELECT_SEGDIR_COUNT_MAX   13
#define SQL_DELETE_SEGDIR_BY_LEVEL    14
#define SQL_DELETE_SEGMENTS_RANGE     15
#define SQL_CONTENT_INSERT            16
#define SQL_GET_BLOCK                 17

/*
** This function is used to obtain an SQLite prepared statement handle
** for the statement identified by the second argument. If successful,
** *pp is set to the requested statement handle and SQLITE_OK returned.
** Otherwise, an SQLite error code is returned and *pp is set to 0.
**
** If argument apVal is not NULL, then it must point to an array with
** at least as many entries as the requested statement has bound 
** parameters. The values are bound to the statements parameters before
** returning.
*/
static int fts3SqlStmt(
  Fts3Table *p,                   /* Virtual table handle */
  int eStmt,                      /* One of the SQL_XXX constants above */
  sqlite3_stmt **pp,              /* OUT: Statement handle */
  sqlite3_value **apVal           /* Values to bind to statement */
){
  const char *azSql[] = {
/* 0  */  "DELETE FROM %Q.'%q_content' WHERE rowid = ?",
/* 1  */  "SELECT NOT EXISTS(SELECT docid FROM %Q.'%q_content' WHERE rowid!=?)",
/* 2  */  "DELETE FROM %Q.'%q_content'",
/* 3  */  "DELETE FROM %Q.'%q_segments'",
/* 4  */  "DELETE FROM %Q.'%q_segdir'",
/* 5  */  "SELECT * FROM %Q.'%q_content' WHERE rowid=?",
/* 6  */  "SELECT coalesce(max(idx)+1, 0) FROM %Q.'%q_segdir' WHERE level=?",
/* 7  */  "INSERT INTO %Q.'%q_segments'(blockid, block) VALUES(?, ?)",
/* 8  */  "SELECT coalesce(max(blockid)+1, 1) FROM %Q.'%q_segments'",
/* 9  */  "INSERT INTO %Q.'%q_segdir' VALUES(?,?,?,?,?,?)",

          /* Return segments in order from oldest to newest.*/ 
/* 10 */  "SELECT idx, start_block, leaves_end_block, end_block, root "
            "FROM %Q.'%q_segdir' WHERE level = ? ORDER BY idx ASC",
/* 11 */  "SELECT idx, start_block, leaves_end_block, end_block, root "
            "FROM %Q.'%q_segdir' ORDER BY level DESC, idx ASC",

/* 12 */  "SELECT count(*) FROM %Q.'%q_segdir' WHERE level = ?",
/* 13 */  "SELECT count(*), max(level) FROM %Q.'%q_segdir'",

/* 14 */  "DELETE FROM %Q.'%q_segdir' WHERE level = ?",
/* 15 */  "DELETE FROM %Q.'%q_segments' WHERE blockid BETWEEN ? AND ?",
/* 16 */  "INSERT INTO %Q.'%q_content' VALUES(%z)",
/* 17 */  "SELECT block FROM %Q.'%q_segments' WHERE blockid = ?",
  };
  int rc = SQLITE_OK;
  sqlite3_stmt *pStmt;

  assert( SizeofArray(azSql)==SizeofArray(p->aStmt) );
  assert( eStmt<SizeofArray(azSql) && eStmt>=0 );
  
  pStmt = p->aStmt[eStmt];
  if( !pStmt ){
    char *zSql;
    if( eStmt==SQL_CONTENT_INSERT ){
      int i;                      /* Iterator variable */  
      char *zVarlist;             /* The "?, ?, ..." string */
      zVarlist = (char *)sqlite3_malloc(2*p->nColumn+2);
      if( !zVarlist ){
        *pp = 0;
        return SQLITE_NOMEM;
      }
      zVarlist[0] = '?';
      zVarlist[p->nColumn*2+1] = '\0';
      for(i=1; i<=p->nColumn; i++){
        zVarlist[i*2-1] = ',';
        zVarlist[i*2] = '?';
      }
      zSql = sqlite3_mprintf(azSql[eStmt], p->zDb, p->zName, zVarlist);
    }else{
      zSql = sqlite3_mprintf(azSql[eStmt], p->zDb, p->zName);
    }
    if( !zSql ){
      rc = SQLITE_NOMEM;
    }else{
      rc = sqlite3_prepare_v2(p->db, zSql, -1, &pStmt, NULL);
      sqlite3_free(zSql);
      assert( rc==SQLITE_OK || pStmt==0 );
      p->aStmt[eStmt] = pStmt;
    }
  }
  if( apVal ){
    int i;
    int nParam = sqlite3_bind_parameter_count(pStmt);
    for(i=0; rc==SQLITE_OK && i<nParam; i++){
      rc = sqlite3_bind_value(pStmt, i+1, apVal[i]);
    }
  }
  *pp = pStmt;
  return rc;
}

/*
** Similar to fts3SqlStmt(). Except, after binding the parameters in
** array apVal[] to the SQL statement identified by eStmt, the statement
** is executed.
**
** Returns SQLITE_OK if the statement is successfully executed, or an
** SQLite error code otherwise.
*/
static int fts3SqlExec(Fts3Table *p, int eStmt, sqlite3_value **apVal){
  sqlite3_stmt *pStmt;
  int rc = fts3SqlStmt(p, eStmt, &pStmt, apVal); 
  if( rc==SQLITE_OK ){
    sqlite3_step(pStmt);
    rc = sqlite3_reset(pStmt);
  }
  return rc;
}


/*
** Read a single block from the %_segments table. If the specified block
** does not exist, return SQLITE_CORRUPT. If some other error (malloc, IO 
** etc.) occurs, return the appropriate SQLite error code.
**
** Otherwise, if successful, set *pzBlock to point to a buffer containing
** the block read from the database, and *pnBlock to the size of the read
** block in bytes.
**
** WARNING: The returned buffer is only valid until the next call to 
** sqlite3Fts3ReadBlock().
*/
int sqlite3Fts3ReadBlock(
  Fts3Table *p,
  sqlite3_int64 iBlock,
  char const **pzBlock,
  int *pnBlock
){
  sqlite3_stmt *pStmt;
  int rc = fts3SqlStmt(p, SQL_GET_BLOCK, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_reset(pStmt);

  if( pzBlock ){
    sqlite3_bind_int64(pStmt, 1, iBlock);
    rc = sqlite3_step(pStmt); 
    if( rc!=SQLITE_ROW ){
      return SQLITE_CORRUPT;
    }
  
    *pnBlock = sqlite3_column_bytes(pStmt, 0);
    *pzBlock = (char *)sqlite3_column_blob(pStmt, 0);
    if( !*pzBlock ){
      return SQLITE_NOMEM;
    }
  }
  return SQLITE_OK;
}

/*
** Set *ppStmt to a statement handle that may be used to iterate through
** all rows in the %_segdir table, from oldest to newest. If successful,
** return SQLITE_OK. If an error occurs while preparing the statement, 
** return an SQLite error code.
**
** There is only ever one instance of this SQL statement compiled for
** each FTS3 table.
**
** The statement returns the following columns from the %_segdir table:
**
**   0: idx
**   1: start_block
**   2: leaves_end_block
**   3: end_block
**   4: root
*/
int sqlite3Fts3AllSegdirs(Fts3Table *p, sqlite3_stmt **ppStmt){
  return fts3SqlStmt(p, SQL_SELECT_ALL_LEVEL, ppStmt, 0);
}


/*
** Append a single varint to a PendingList buffer. SQLITE_OK is returned
** if successful, or an SQLite error code otherwise.
**
** This function also serves to allocate the PendingList structure itself.
** For example, to create a new PendingList structure containing two
** varints:
**
**   PendingList *p = 0;
**   fts3PendingListAppendVarint(&p, 1);
**   fts3PendingListAppendVarint(&p, 2);
*/
static int fts3PendingListAppendVarint(
  PendingList **pp,               /* IN/OUT: Pointer to PendingList struct */
  sqlite3_int64 i                 /* Value to append to data */
){
  PendingList *p = *pp;

  /* Allocate or grow the PendingList as required. */
  if( !p ){
    p = sqlite3_malloc(sizeof(*p) + 100);
    if( !p ){
      return SQLITE_NOMEM;
    }
    p->nSpace = 100;
    p->aData = (char *)&p[1];
    p->nData = 0;
  }
  else if( p->nData+FTS3_VARINT_MAX+1>p->nSpace ){
    int nNew = p->nSpace * 2;
    p = sqlite3_realloc(p, sizeof(*p) + nNew);
    if( !p ){
      sqlite3_free(*pp);
      *pp = 0;
      return SQLITE_NOMEM;
    }
    p->nSpace = nNew;
    p->aData = (char *)&p[1];
  }

  /* Append the new serialized varint to the end of the list. */
  p->nData += sqlite3Fts3PutVarint(&p->aData[p->nData], i);
  p->aData[p->nData] = '\0';
  *pp = p;
  return SQLITE_OK;
}

/*
** Add a docid/column/position entry to a PendingList structure. Non-zero
** is returned if the structure is sqlite3_realloced as part of adding
** the entry. Otherwise, zero.
**
** If an OOM error occurs, *pRc is set to SQLITE_NOMEM before returning.
** Zero is always returned in this case. Otherwise, if no OOM error occurs,
** it is set to SQLITE_OK.
*/
static int fts3PendingListAppend(
  PendingList **pp,               /* IN/OUT: PendingList structure */
  sqlite3_int64 iDocid,           /* Docid for entry to add */
  sqlite3_int64 iCol,             /* Column for entry to add */
  sqlite3_int64 iPos,             /* Position of term for entry to add */
  int *pRc                        /* OUT: Return code */
){
  PendingList *p = *pp;
  int rc = SQLITE_OK;

  assert( !p || p->iLastDocid<=iDocid );

  if( !p || p->iLastDocid!=iDocid ){
    sqlite3_int64 iDelta = iDocid - (p ? p->iLastDocid : 0);
    if( p ){
      assert( p->nData<p->nSpace );
      assert( p->aData[p->nData]==0 );
      p->nData++;
    }
    if( SQLITE_OK!=(rc = fts3PendingListAppendVarint(&p, iDelta)) ){
      goto pendinglistappend_out;
    }
    p->iLastCol = -1;
    p->iLastPos = 0;
    p->iLastDocid = iDocid;
  }
  if( iCol>0 && p->iLastCol!=iCol ){
    if( SQLITE_OK!=(rc = fts3PendingListAppendVarint(&p, 1))
     || SQLITE_OK!=(rc = fts3PendingListAppendVarint(&p, iCol))
    ){
      goto pendinglistappend_out;
    }
    p->iLastCol = iCol;
    p->iLastPos = 0;
  }
  if( iCol>=0 ){
    assert( iPos>p->iLastPos || (iPos==0 && p->iLastPos==0) );
    rc = fts3PendingListAppendVarint(&p, 2+iPos-p->iLastPos);
    if( rc==SQLITE_OK ){
      p->iLastPos = iPos;
    }
  }

 pendinglistappend_out:
  *pRc = rc;
  if( p!=*pp ){
    *pp = p;
    return 1;
  }
  return 0;
}

/*
** Tokenize the nul-terminated string zText and add all tokens to the
** pending-terms hash-table. The docid used is that currently stored in
** p->iPrevDocid, and the column is specified by argument iCol.
**
** If successful, SQLITE_OK is returned. Otherwise, an SQLite error code.
*/
static int fts3PendingTermsAdd(Fts3Table *p, const char *zText, int iCol){
  int rc;
  int iStart;
  int iEnd;
  int iPos;

  char const *zToken;
  int nToken;

  sqlite3_tokenizer *pTokenizer = p->pTokenizer;
  sqlite3_tokenizer_module const *pModule = pTokenizer->pModule;
  sqlite3_tokenizer_cursor *pCsr;
  int (*xNext)(sqlite3_tokenizer_cursor *pCursor,
      const char**,int*,int*,int*,int*);

  assert( pTokenizer && pModule );

  rc = pModule->xOpen(pTokenizer, zText, -1, &pCsr);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pCsr->pTokenizer = pTokenizer;

  xNext = pModule->xNext;
  while( SQLITE_OK==rc
      && SQLITE_OK==(rc = xNext(pCsr, &zToken, &nToken, &iStart, &iEnd, &iPos))
  ){
    PendingList *pList;

    /* Positions cannot be negative; we use -1 as a terminator internally.
    ** Tokens must have a non-zero length.
    */
    if( iPos<0 || !zToken || nToken<=0 ){
      rc = SQLITE_ERROR;
      break;
    }

    pList = (PendingList *)fts3HashFind(&p->pendingTerms, zToken, nToken);
    if( pList ){
      p->nPendingData -= (pList->nData + nToken + sizeof(Fts3HashElem));
    }
    if( fts3PendingListAppend(&pList, p->iPrevDocid, iCol, iPos, &rc) ){
      if( pList==fts3HashInsert(&p->pendingTerms, zToken, nToken, pList) ){
        /* Malloc failed while inserting the new entry. This can only 
        ** happen if there was no previous entry for this token.
        */
        assert( 0==fts3HashFind(&p->pendingTerms, zToken, nToken) );
        sqlite3_free(pList);
        rc = SQLITE_NOMEM;
      }
    }
    if( rc==SQLITE_OK ){
      p->nPendingData += (pList->nData + nToken + sizeof(Fts3HashElem));
    }
  }

  pModule->xClose(pCsr);
  return (rc==SQLITE_DONE ? SQLITE_OK : rc);
}

/* 
** Calling this function indicates that subsequent calls to 
** fts3PendingTermsAdd() are to add term/position-list pairs for the
** contents of the document with docid iDocid.
*/
static int fts3PendingTermsDocid(Fts3Table *p, sqlite_int64 iDocid){
  /* TODO(shess) Explore whether partially flushing the buffer on
  ** forced-flush would provide better performance.  I suspect that if
  ** we ordered the doclists by size and flushed the largest until the
  ** buffer was half empty, that would let the less frequent terms
  ** generate longer doclists.
  */
  if( iDocid<=p->iPrevDocid || p->nPendingData>FTS3_MAX_PENDING_DATA ){
    int rc = sqlite3Fts3PendingTermsFlush(p);
    if( rc!=SQLITE_OK ) return rc;
  }
  p->iPrevDocid = iDocid;
  return SQLITE_OK;
}

void sqlite3Fts3PendingTermsClear(Fts3Table *p){
  Fts3HashElem *pElem;
  for(pElem=fts3HashFirst(&p->pendingTerms); pElem; pElem=fts3HashNext(pElem)){
    sqlite3_free(fts3HashData(pElem));
  }
  fts3HashClear(&p->pendingTerms);
  p->nPendingData = 0;
}

/*
** This function is called by the xUpdate() method as part of an INSERT
** operation. It adds entries for each term in the new record to the
** pendingTerms hash table.
**
** Argument apVal is the same as the similarly named argument passed to
** fts3InsertData(). Parameter iDocid is the docid of the new row.
*/
static int fts3InsertTerms(Fts3Table *p, sqlite3_value **apVal){
  int i;                          /* Iterator variable */
  for(i=2; i<p->nColumn+2; i++){
    const char *zText = (const char *)sqlite3_value_text(apVal[i]);
    if( zText ){
      int rc = fts3PendingTermsAdd(p, zText, i-2);
      if( rc!=SQLITE_OK ){
        return rc;
      }
    }
  }
  return SQLITE_OK;
}

/*
** This function is called by the xUpdate() method for an INSERT operation.
** The apVal parameter is passed a copy of the apVal argument passed by
** SQLite to the xUpdate() method. i.e:
**
**   apVal[0]                Not used for INSERT.
**   apVal[1]                rowid
**   apVal[2]                Left-most user-defined column
**   ...
**   apVal[p->nColumn+1]     Right-most user-defined column
**   apVal[p->nColumn+2]     Hidden column with same name as table
**   apVal[p->nColumn+3]     Hidden "docid" column (alias for rowid)
*/
static int fts3InsertData(
  Fts3Table *p,                   /* Full-text table */
  sqlite3_value **apVal,          /* Array of values to insert */
  sqlite3_int64 *piDocid          /* OUT: Docid for row just inserted */
){
  int rc;                         /* Return code */
  sqlite3_stmt *pContentInsert;   /* INSERT INTO %_content VALUES(...) */

  /* Locate the statement handle used to insert data into the %_content
  ** table. The SQL for this statement is:
  **
  **   INSERT INTO %_content VALUES(?, ?, ?, ...)
  **
  ** The statement features N '?' variables, where N is the number of user
  ** defined columns in the FTS3 table, plus one for the docid field.
  */
  rc = fts3SqlStmt(p, SQL_CONTENT_INSERT, &pContentInsert, &apVal[1]);
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* There is a quirk here. The users INSERT statement may have specified
  ** a value for the "rowid" field, for the "docid" field, or for both.
  ** Which is a problem, since "rowid" and "docid" are aliases for the
  ** same value. For example:
  **
  **   INSERT INTO fts3tbl(rowid, docid) VALUES(1, 2);
  **
  ** In FTS3, this is an error. It is an error to specify non-NULL values
  ** for both docid and some other rowid alias.
  */
  if( SQLITE_NULL!=sqlite3_value_type(apVal[3+p->nColumn]) ){
    if( SQLITE_NULL==sqlite3_value_type(apVal[0])
     && SQLITE_NULL!=sqlite3_value_type(apVal[1])
    ){
      /* A rowid/docid conflict. */
      return SQLITE_ERROR;
    }
    rc = sqlite3_bind_value(pContentInsert, 1, apVal[3+p->nColumn]);
    if( rc!=SQLITE_OK ) return rc;
  }

  /* Execute the statement to insert the record. Set *piDocid to the 
  ** new docid value. 
  */
  sqlite3_step(pContentInsert);
  rc = sqlite3_reset(pContentInsert);

  *piDocid = sqlite3_last_insert_rowid(p->db);
  return rc;
}



/*
** Remove all data from the FTS3 table. Clear the hash table containing
** pending terms.
*/
static int fts3DeleteAll(Fts3Table *p){
  int rc;                         /* Return code */

  /* Discard the contents of the pending-terms hash table. */
  sqlite3Fts3PendingTermsClear(p);

  /* Delete everything from the %_content, %_segments and %_segdir tables. */
  rc = fts3SqlExec(p, SQL_DELETE_ALL_CONTENT, 0);
  if( rc==SQLITE_OK ){
    rc = fts3SqlExec(p, SQL_DELETE_ALL_SEGMENTS, 0);
  }
  if( rc==SQLITE_OK ){
    rc = fts3SqlExec(p, SQL_DELETE_ALL_SEGDIR, 0);
  }
  return rc;
}

/*
** The first element in the apVal[] array is assumed to contain the docid
** (an integer) of a row about to be deleted. Remove all terms from the
** full-text index.
*/
static int fts3DeleteTerms(Fts3Table *p, sqlite3_value **apVal){
  int rc;
  sqlite3_stmt *pSelect;

  rc = fts3SqlStmt(p, SQL_SELECT_CONTENT_BY_ROWID, &pSelect, apVal);
  if( rc==SQLITE_OK ){
    if( SQLITE_ROW==sqlite3_step(pSelect) ){
      int i;
      for(i=1; i<=p->nColumn; i++){
        const char *zText = (const char *)sqlite3_column_text(pSelect, i);
        rc = fts3PendingTermsAdd(p, zText, -1);
        if( rc!=SQLITE_OK ){
          sqlite3_reset(pSelect);
          return rc;
        }
      }
    }
    rc = sqlite3_reset(pSelect);
  }else{
    sqlite3_reset(pSelect);
  }
  return rc;
}

/*
** Forward declaration to account for the circular dependency between
** functions fts3SegmentMerge() and fts3AllocateSegdirIdx().
*/
static int fts3SegmentMerge(Fts3Table *, int);

/* 
** This function allocates a new level iLevel index in the segdir table.
** Usually, indexes are allocated within a level sequentially starting
** with 0, so the allocated index is one greater than the value returned
** by:
**
**   SELECT max(idx) FROM %_segdir WHERE level = :iLevel
**
** However, if there are already FTS3_MERGE_COUNT indexes at the requested
** level, they are merged into a single level (iLevel+1) segment and the 
** allocated index is 0.
**
** If successful, *piIdx is set to the allocated index slot and SQLITE_OK
** returned. Otherwise, an SQLite error code is returned.
*/
static int fts3AllocateSegdirIdx(Fts3Table *p, int iLevel, int *piIdx){
  int rc;                         /* Return Code */
  sqlite3_stmt *pNextIdx;         /* Query for next idx at level iLevel */
  int iNext = 0;                  /* Result of query pNextIdx */

  /* Set variable iNext to the next available segdir index at level iLevel. */
  rc = fts3SqlStmt(p, SQL_NEXT_SEGMENT_INDEX, &pNextIdx, 0);
  if( rc==SQLITE_OK ){
    sqlite3_bind_int(pNextIdx, 1, iLevel);
    if( SQLITE_ROW==sqlite3_step(pNextIdx) ){
      iNext = sqlite3_column_int(pNextIdx, 0);
    }
    rc = sqlite3_reset(pNextIdx);
  }

  if( rc==SQLITE_OK ){
    /* If iNext is FTS3_MERGE_COUNT, indicating that level iLevel is already
    ** full, merge all segments in level iLevel into a single iLevel+1
    ** segment and allocate (newly freed) index 0 at level iLevel. Otherwise,
    ** if iNext is less than FTS3_MERGE_COUNT, allocate index iNext.
    */
    if( iNext>=FTS3_MERGE_COUNT ){
      rc = fts3SegmentMerge(p, iLevel);
      *piIdx = 0;
    }else{
      *piIdx = iNext;
    }
  }

  return rc;
}

/*
** Move the iterator passed as the first argument to the next term in the
** segment. If successful, SQLITE_OK is returned. If there is no next term,
** SQLITE_DONE. Otherwise, an SQLite error code.
*/
static int fts3SegReaderNext(Fts3SegReader *pReader){
  char *pNext;                    /* Cursor variable */
  int nPrefix;                    /* Number of bytes in term prefix */
  int nSuffix;                    /* Number of bytes in term suffix */

  if( !pReader->aDoclist ){
    pNext = pReader->aNode;
  }else{
    pNext = &pReader->aDoclist[pReader->nDoclist];
  }

  if( !pNext || pNext>=&pReader->aNode[pReader->nNode] ){
    int rc;
    if( !pReader->pStmt ){
      pReader->aNode = 0;
      return SQLITE_OK;
    }
    rc = sqlite3_step(pReader->pStmt);
    if( rc!=SQLITE_ROW ){
      pReader->aNode = 0;
      return (rc==SQLITE_DONE ? SQLITE_OK : rc);
    }
    pReader->nNode = sqlite3_column_bytes(pReader->pStmt, 0);
    pReader->aNode = (char *)sqlite3_column_blob(pReader->pStmt, 0);
    pNext = pReader->aNode;
  }
  
  pNext += sqlite3Fts3GetVarint32(pNext, &nPrefix);
  pNext += sqlite3Fts3GetVarint32(pNext, &nSuffix);

  if( nPrefix+nSuffix>pReader->nTermAlloc ){
    int nNew = (nPrefix+nSuffix)*2;
    char *zNew = sqlite3_realloc(pReader->zTerm, nNew);
    if( !zNew ){
      return SQLITE_NOMEM;
    }
    pReader->zTerm = zNew;
    pReader->nTermAlloc = nNew;
  }
  memcpy(&pReader->zTerm[nPrefix], pNext, nSuffix);
  pReader->nTerm = nPrefix+nSuffix;
  pNext += nSuffix;
  pNext += sqlite3Fts3GetVarint32(pNext, &pReader->nDoclist);
  assert( pNext<&pReader->aNode[pReader->nNode] );
  pReader->aDoclist = pNext;
  pReader->pOffsetList = 0;
  return SQLITE_OK;
}

/*
** Set the SegReader to point to the first docid in the doclist associated
** with the current term.
*/
static void fts3SegReaderFirstDocid(Fts3SegReader *pReader){
  int n;
  assert( pReader->aDoclist );
  assert( !pReader->pOffsetList );
  n = sqlite3Fts3GetVarint(pReader->aDoclist, &pReader->iDocid);
  pReader->pOffsetList = &pReader->aDoclist[n];
}

/*
** Advance the SegReader to point to the next docid in the doclist
** associated with the current term.
** 
** If arguments ppOffsetList and pnOffsetList are not NULL, then 
** *ppOffsetList is set to point to the first column-offset list
** in the doclist entry (i.e. immediately past the docid varint).
** *pnOffsetList is set to the length of the set of column-offset
** lists, not including the nul-terminator byte. For example:
*/
static void fts3SegReaderNextDocid(
  Fts3SegReader *pReader,
  char **ppOffsetList,
  int *pnOffsetList
){
  char *p = pReader->pOffsetList;
  char c = 0;

  /* Pointer p currently points at the first byte of an offset list. The
  ** following two lines advance it to point one byte past the end of
  ** the same offset list.
  */
  while( *p | c ) c = *p++ & 0x80;
  p++;

  /* If required, populate the output variables with a pointer to and the
  ** size of the previous offset-list.
  */
  if( ppOffsetList ){
    *ppOffsetList = pReader->pOffsetList;
    *pnOffsetList = (int)(p - pReader->pOffsetList - 1);
  }

  /* If there are no more entries in the doclist, set pOffsetList to
  ** NULL. Otherwise, set Fts3SegReader.iDocid to the next docid and
  ** Fts3SegReader.pOffsetList to point to the next offset list before
  ** returning.
  */
  if( p>=&pReader->aDoclist[pReader->nDoclist] ){
    pReader->pOffsetList = 0;
  }else{
    sqlite3_int64 iDelta;
    pReader->pOffsetList = p + sqlite3Fts3GetVarint(p, &iDelta);
    pReader->iDocid += iDelta;
  }
}

/*
** Free all allocations associated with the iterator passed as the 
** second argument.
*/
void sqlite3Fts3SegReaderFree(Fts3Table *p, Fts3SegReader *pReader){
  if( pReader ){
    if( pReader->pStmt ){
      /* Move the leaf-range SELECT statement to the aLeavesStmt[] array,
      ** so that it can be reused when required by another query.
      */
      assert( p->nLeavesStmt<p->nLeavesTotal );
      sqlite3_reset(pReader->pStmt);
      p->aLeavesStmt[p->nLeavesStmt++] = pReader->pStmt;
    }
    sqlite3_free(pReader->zTerm);
    sqlite3_free(pReader);
  }
}

/*
** Allocate a new SegReader object.
*/
int sqlite3Fts3SegReaderNew(
  Fts3Table *p,                   /* Virtual table handle */
  int iAge,                       /* Segment "age". */
  sqlite3_int64 iStartLeaf,       /* First leaf to traverse */
  sqlite3_int64 iEndLeaf,         /* Final leaf to traverse */
  sqlite3_int64 iEndBlock,        /* Final block of segment */
  const char *zRoot,              /* Buffer containing root node */
  int nRoot,                      /* Size of buffer containing root node */
  Fts3SegReader **ppReader        /* OUT: Allocated Fts3SegReader */
){
  int rc = SQLITE_OK;             /* Return code */
  Fts3SegReader *pReader;         /* Newly allocated SegReader object */
  int nExtra = 0;                 /* Bytes to allocate segment root node */

  if( iStartLeaf==0 ){
    nExtra = nRoot;
  }

  pReader = (Fts3SegReader *)sqlite3_malloc(sizeof(Fts3SegReader) + nExtra);
  if( !pReader ){
    return SQLITE_NOMEM;
  }
  memset(pReader, 0, sizeof(Fts3SegReader));
  pReader->iStartBlock = iStartLeaf;
  pReader->iIdx = iAge;
  pReader->iEndBlock = iEndBlock;

  if( nExtra ){
    /* The entire segment is stored in the root node. */
    pReader->aNode = (char *)&pReader[1];
    pReader->nNode = nRoot;
    memcpy(pReader->aNode, zRoot, nRoot);
  }else{
    /* If the text of the SQL statement to iterate through a contiguous
    ** set of entries in the %_segments table has not yet been composed,
    ** compose it now.
    */
    if( !p->zSelectLeaves ){
      p->zSelectLeaves = sqlite3_mprintf(
          "SELECT block FROM %Q.'%q_segments' WHERE blockid BETWEEN ? AND ? "
          "ORDER BY blockid", p->zDb, p->zName
      );
      if( !p->zSelectLeaves ){
        rc = SQLITE_NOMEM;
        goto finished;
      }
    }

    /* If there are no free statements in the aLeavesStmt[] array, prepare
    ** a new statement now. Otherwise, reuse a prepared statement from
    ** aLeavesStmt[].
    */
    if( p->nLeavesStmt==0 ){
      if( p->nLeavesTotal==p->nLeavesAlloc ){
        int nNew = p->nLeavesAlloc + 16;
        sqlite3_stmt **aNew = (sqlite3_stmt **)sqlite3_realloc(
            p->aLeavesStmt, nNew*sizeof(sqlite3_stmt *)
        );
        if( !aNew ){
          rc = SQLITE_NOMEM;
          goto finished;
        }
        p->nLeavesAlloc = nNew;
        p->aLeavesStmt = aNew;
      }
      rc = sqlite3_prepare_v2(p->db, p->zSelectLeaves, -1, &pReader->pStmt, 0);
      if( rc!=SQLITE_OK ){
        goto finished;
      }
      p->nLeavesTotal++;
    }else{
      pReader->pStmt = p->aLeavesStmt[--p->nLeavesStmt];
    }

    /* Bind the start and end leaf blockids to the prepared SQL statement. */
    sqlite3_bind_int64(pReader->pStmt, 1, iStartLeaf);
    sqlite3_bind_int64(pReader->pStmt, 2, iEndLeaf);
  }
  rc = fts3SegReaderNext(pReader);

 finished:
  if( rc==SQLITE_OK ){
    *ppReader = pReader;
  }else{
    sqlite3Fts3SegReaderFree(p, pReader);
  }
  return rc;
}


/*
** The second argument to this function is expected to be a statement of
** the form:
**
**   SELECT 
**     idx,                  -- col 0
**     start_block,          -- col 1
**     leaves_end_block,     -- col 2
**     end_block,            -- col 3
**     root                  -- col 4
**   FROM %_segdir ...
**
** This function allocates and initializes a Fts3SegReader structure to
** iterate through the terms stored in the segment identified by the
** current row that pStmt is pointing to. 
**
** If successful, the Fts3SegReader is left pointing to the first term
** in the segment and SQLITE_OK is returned. Otherwise, an SQLite error
** code is returned.
*/
static int fts3SegReaderNew(
  Fts3Table *p,                   /* Virtual table handle */
  sqlite3_stmt *pStmt,            /* See above */
  int iAge,                       /* Segment "age". */
  Fts3SegReader **ppReader        /* OUT: Allocated Fts3SegReader */
){
  return sqlite3Fts3SegReaderNew(p, iAge, 
      sqlite3_column_int64(pStmt, 1),
      sqlite3_column_int64(pStmt, 2),
      sqlite3_column_int64(pStmt, 3),
      sqlite3_column_blob(pStmt, 4),
      sqlite3_column_bytes(pStmt, 4),
      ppReader
  );
}

/*
** Compare the entries pointed to by two Fts3SegReader structures. 
** Comparison is as follows:
**
**   1) EOF is greater than not EOF.
**
**   2) The current terms (if any) are compared with memcmp(). If one
**      term is a prefix of another, the longer term is considered the
**      larger.
**
**   3) By segment age. An older segment is considered larger.
*/
static int fts3SegReaderCmp(Fts3SegReader *pLhs, Fts3SegReader *pRhs){
  int rc;
  if( pLhs->aNode && pRhs->aNode ){
    int rc2 = pLhs->nTerm - pRhs->nTerm;
    if( rc2<0 ){
      rc = memcmp(pLhs->zTerm, pRhs->zTerm, pLhs->nTerm);
    }else{
      rc = memcmp(pLhs->zTerm, pRhs->zTerm, pRhs->nTerm);
    }
    if( rc==0 ){
      rc = rc2;
    }
  }else{
    rc = (pLhs->aNode==0) - (pRhs->aNode==0);
  }
  if( rc==0 ){
    rc = pRhs->iIdx - pLhs->iIdx;
  }
  assert( rc!=0 );
  return rc;
}

/*
** A different comparison function for SegReader structures. In this
** version, it is assumed that each SegReader points to an entry in
** a doclist for identical terms. Comparison is made as follows:
**
**   1) EOF (end of doclist in this case) is greater than not EOF.
**
**   2) By current docid.
**
**   3) By segment age. An older segment is considered larger.
*/
static int fts3SegReaderDoclistCmp(Fts3SegReader *pLhs, Fts3SegReader *pRhs){
  int rc = (pLhs->pOffsetList==0)-(pRhs->pOffsetList==0);
  if( rc==0 ){
    if( pLhs->iDocid==pRhs->iDocid ){
      rc = pRhs->iIdx - pLhs->iIdx;
    }else{
      rc = (pLhs->iDocid > pRhs->iDocid) ? 1 : -1;
    }
  }
  assert( pLhs->aNode && pRhs->aNode );
  return rc;
}

/*
** Compare the term that the Fts3SegReader object passed as the first argument
** points to with the term specified by arguments zTerm and nTerm. 
**
** If the pSeg iterator is already at EOF, return 0. Otherwise, return
** -ve if the pSeg term is less than zTerm/nTerm, 0 if the two terms are
** equal, or +ve if the pSeg term is greater than zTerm/nTerm.
*/
static int fts3SegReaderTermCmp(
  Fts3SegReader *pSeg,            /* Segment reader object */
  const char *zTerm,              /* Term to compare to */
  int nTerm                       /* Size of term zTerm in bytes */
){
  int res = 0;
  if( pSeg->aNode ){
    if( pSeg->nTerm>nTerm ){
      res = memcmp(pSeg->zTerm, zTerm, nTerm);
    }else{
      res = memcmp(pSeg->zTerm, zTerm, pSeg->nTerm);
    }
    if( res==0 ){
      res = pSeg->nTerm-nTerm;
    }
  }
  return res;
}

/*
** Argument apSegment is an array of nSegment elements. It is known that
** the final (nSegment-nSuspect) members are already in sorted order
** (according to the comparison function provided). This function shuffles
** the array around until all entries are in sorted order.
*/
static void fts3SegReaderSort(
  Fts3SegReader **apSegment,                     /* Array to sort entries of */
  int nSegment,                                  /* Size of apSegment array */
  int nSuspect,                                  /* Unsorted entry count */
  int (*xCmp)(Fts3SegReader *, Fts3SegReader *)  /* Comparison function */
){
  int i;                          /* Iterator variable */

  assert( nSuspect<=nSegment );

  if( nSuspect==nSegment ) nSuspect--;
  for(i=nSuspect-1; i>=0; i--){
    int j;
    for(j=i; j<(nSegment-1); j++){
      Fts3SegReader *pTmp;
      if( xCmp(apSegment[j], apSegment[j+1])<0 ) break;
      pTmp = apSegment[j+1];
      apSegment[j+1] = apSegment[j];
      apSegment[j] = pTmp;
    }
  }

#ifndef NDEBUG
  /* Check that the list really is sorted now. */
  for(i=0; i<(nSuspect-1); i++){
    assert( xCmp(apSegment[i], apSegment[i+1])<0 );
  }
#endif
}

/* 
** Insert a record into the %_segments table.
*/
static int fts3WriteSegment(
  Fts3Table *p,                   /* Virtual table handle */
  sqlite3_int64 iBlock,           /* Block id for new block */
  char *z,                        /* Pointer to buffer containing block data */
  int n                           /* Size of buffer z in bytes */
){
  sqlite3_stmt *pStmt;
  int rc = fts3SqlStmt(p, SQL_INSERT_SEGMENTS, &pStmt, 0);
  if( rc==SQLITE_OK ){
    sqlite3_bind_int64(pStmt, 1, iBlock);
    rc = sqlite3_bind_blob(pStmt, 2, z, n, SQLITE_STATIC);
    if( rc==SQLITE_OK ){
      sqlite3_step(pStmt);
      rc = sqlite3_reset(pStmt);
    }
  }
  return rc;
}

/* 
** Insert a record into the %_segdir table.
*/
static int fts3WriteSegdir(
  Fts3Table *p,                   /* Virtual table handle */
  int iLevel,                     /* Value for "level" field */
  int iIdx,                       /* Value for "idx" field */
  sqlite3_int64 iStartBlock,      /* Value for "start_block" field */
  sqlite3_int64 iLeafEndBlock,    /* Value for "leaves_end_block" field */
  sqlite3_int64 iEndBlock,        /* Value for "end_block" field */
  char *zRoot,                    /* Blob value for "root" field */
  int nRoot                       /* Number of bytes in buffer zRoot */
){
  sqlite3_stmt *pStmt;
  int rc = fts3SqlStmt(p, SQL_INSERT_SEGDIR, &pStmt, 0);
  if( rc==SQLITE_OK ){
    sqlite3_bind_int(pStmt, 1, iLevel);
    sqlite3_bind_int(pStmt, 2, iIdx);
    sqlite3_bind_int64(pStmt, 3, iStartBlock);
    sqlite3_bind_int64(pStmt, 4, iLeafEndBlock);
    sqlite3_bind_int64(pStmt, 5, iEndBlock);
    rc = sqlite3_bind_blob(pStmt, 6, zRoot, nRoot, SQLITE_STATIC);
    if( rc==SQLITE_OK ){
      sqlite3_step(pStmt);
      rc = sqlite3_reset(pStmt);
    }
  }
  return rc;
}

/*
** Return the size of the common prefix (if any) shared by zPrev and
** zNext, in bytes. For example, 
**
**   fts3PrefixCompress("abc", 3, "abcdef", 6)   // returns 3
**   fts3PrefixCompress("abX", 3, "abcdef", 6)   // returns 2
**   fts3PrefixCompress("abX", 3, "Xbcdef", 6)   // returns 0
*/
static int fts3PrefixCompress(
  const char *zPrev,              /* Buffer containing previous term */
  int nPrev,                      /* Size of buffer zPrev in bytes */
  const char *zNext,              /* Buffer containing next term */
  int nNext                       /* Size of buffer zNext in bytes */
){
  int n;
  UNUSED_PARAMETER(nNext);
  for(n=0; n<nPrev && zPrev[n]==zNext[n]; n++);
  return n;
}

/*
** Add term zTerm to the SegmentNode. It is guaranteed that zTerm is larger
** (according to memcmp) than the previous term.
*/
static int fts3NodeAddTerm(
  Fts3Table *p,               /* Virtual table handle */
  SegmentNode **ppTree,           /* IN/OUT: SegmentNode handle */ 
  int isCopyTerm,                 /* True if zTerm/nTerm is transient */
  const char *zTerm,              /* Pointer to buffer containing term */
  int nTerm                       /* Size of term in bytes */
){
  SegmentNode *pTree = *ppTree;
  int rc;
  SegmentNode *pNew;

  /* First try to append the term to the current node. Return early if 
  ** this is possible.
  */
  if( pTree ){
    int nData = pTree->nData;     /* Current size of node in bytes */
    int nReq = nData;             /* Required space after adding zTerm */
    int nPrefix;                  /* Number of bytes of prefix compression */
    int nSuffix;                  /* Suffix length */

    nPrefix = fts3PrefixCompress(pTree->zTerm, pTree->nTerm, zTerm, nTerm);
    nSuffix = nTerm-nPrefix;

    nReq += sqlite3Fts3VarintLen(nPrefix)+sqlite3Fts3VarintLen(nSuffix)+nSuffix;
    if( nReq<=p->nNodeSize || !pTree->zTerm ){

      if( nReq>p->nNodeSize ){
        /* An unusual case: this is the first term to be added to the node
        ** and the static node buffer (p->nNodeSize bytes) is not large
        ** enough. Use a separately malloced buffer instead This wastes
        ** p->nNodeSize bytes, but since this scenario only comes about when
        ** the database contain two terms that share a prefix of almost 2KB, 
        ** this is not expected to be a serious problem. 
        */
        assert( pTree->aData==(char *)&pTree[1] );
        pTree->aData = (char *)sqlite3_malloc(nReq);
        if( !pTree->aData ){
          return SQLITE_NOMEM;
        }
      }

      if( pTree->zTerm ){
        /* There is no prefix-length field for first term in a node */
        nData += sqlite3Fts3PutVarint(&pTree->aData[nData], nPrefix);
      }

      nData += sqlite3Fts3PutVarint(&pTree->aData[nData], nSuffix);
      memcpy(&pTree->aData[nData], &zTerm[nPrefix], nSuffix);
      pTree->nData = nData + nSuffix;
      pTree->nEntry++;

      if( isCopyTerm ){
        if( pTree->nMalloc<nTerm ){
          char *zNew = sqlite3_realloc(pTree->zMalloc, nTerm*2);
          if( !zNew ){
            return SQLITE_NOMEM;
          }
          pTree->nMalloc = nTerm*2;
          pTree->zMalloc = zNew;
        }
        pTree->zTerm = pTree->zMalloc;
        memcpy(pTree->zTerm, zTerm, nTerm);
        pTree->nTerm = nTerm;
      }else{
        pTree->zTerm = (char *)zTerm;
        pTree->nTerm = nTerm;
      }
      return SQLITE_OK;
    }
  }

  /* If control flows to here, it was not possible to append zTerm to the
  ** current node. Create a new node (a right-sibling of the current node).
  ** If this is the first node in the tree, the term is added to it.
  **
  ** Otherwise, the term is not added to the new node, it is left empty for
  ** now. Instead, the term is inserted into the parent of pTree. If pTree 
  ** has no parent, one is created here.
  */
  pNew = (SegmentNode *)sqlite3_malloc(sizeof(SegmentNode) + p->nNodeSize);
  if( !pNew ){
    return SQLITE_NOMEM;
  }
  memset(pNew, 0, sizeof(SegmentNode));
  pNew->nData = 1 + FTS3_VARINT_MAX;
  pNew->aData = (char *)&pNew[1];

  if( pTree ){
    SegmentNode *pParent = pTree->pParent;
    rc = fts3NodeAddTerm(p, &pParent, isCopyTerm, zTerm, nTerm);
    if( pTree->pParent==0 ){
      pTree->pParent = pParent;
    }
    pTree->pRight = pNew;
    pNew->pLeftmost = pTree->pLeftmost;
    pNew->pParent = pParent;
    pNew->zMalloc = pTree->zMalloc;
    pNew->nMalloc = pTree->nMalloc;
    pTree->zMalloc = 0;
  }else{
    pNew->pLeftmost = pNew;
    rc = fts3NodeAddTerm(p, &pNew, isCopyTerm, zTerm, nTerm); 
  }

  *ppTree = pNew;
  return rc;
}

/*
** Helper function for fts3NodeWrite().
*/
static int fts3TreeFinishNode(
  SegmentNode *pTree, 
  int iHeight, 
  sqlite3_int64 iLeftChild
){
  int nStart;
  assert( iHeight>=1 && iHeight<128 );
  nStart = FTS3_VARINT_MAX - sqlite3Fts3VarintLen(iLeftChild);
  pTree->aData[nStart] = (char)iHeight;
  sqlite3Fts3PutVarint(&pTree->aData[nStart+1], iLeftChild);
  return nStart;
}

/*
** Write the buffer for the segment node pTree and all of its peers to the
** database. Then call this function recursively to write the parent of 
** pTree and its peers to the database. 
**
** Except, if pTree is a root node, do not write it to the database. Instead,
** set output variables *paRoot and *pnRoot to contain the root node.
**
** If successful, SQLITE_OK is returned and output variable *piLast is
** set to the largest blockid written to the database (or zero if no
** blocks were written to the db). Otherwise, an SQLite error code is 
** returned.
*/
static int fts3NodeWrite(
  Fts3Table *p,                   /* Virtual table handle */
  SegmentNode *pTree,             /* SegmentNode handle */
  int iHeight,                    /* Height of this node in tree */
  sqlite3_int64 iLeaf,            /* Block id of first leaf node */
  sqlite3_int64 iFree,            /* Block id of next free slot in %_segments */
  sqlite3_int64 *piLast,          /* OUT: Block id of last entry written */
  char **paRoot,                  /* OUT: Data for root node */
  int *pnRoot                     /* OUT: Size of root node in bytes */
){
  int rc = SQLITE_OK;

  if( !pTree->pParent ){
    /* Root node of the tree. */
    int nStart = fts3TreeFinishNode(pTree, iHeight, iLeaf);
    *piLast = iFree-1;
    *pnRoot = pTree->nData - nStart;
    *paRoot = &pTree->aData[nStart];
  }else{
    SegmentNode *pIter;
    sqlite3_int64 iNextFree = iFree;
    sqlite3_int64 iNextLeaf = iLeaf;
    for(pIter=pTree->pLeftmost; pIter && rc==SQLITE_OK; pIter=pIter->pRight){
      int nStart = fts3TreeFinishNode(pIter, iHeight, iNextLeaf);
      int nWrite = pIter->nData - nStart;
  
      rc = fts3WriteSegment(p, iNextFree, &pIter->aData[nStart], nWrite);
      iNextFree++;
      iNextLeaf += (pIter->nEntry+1);
    }
    if( rc==SQLITE_OK ){
      assert( iNextLeaf==iFree );
      rc = fts3NodeWrite(
          p, pTree->pParent, iHeight+1, iFree, iNextFree, piLast, paRoot, pnRoot
      );
    }
  }

  return rc;
}

/*
** Free all memory allocations associated with the tree pTree.
*/
static void fts3NodeFree(SegmentNode *pTree){
  if( pTree ){
    SegmentNode *p = pTree->pLeftmost;
    fts3NodeFree(p->pParent);
    while( p ){
      SegmentNode *pRight = p->pRight;
      if( p->aData!=(char *)&p[1] ){
        sqlite3_free(p->aData);
      }
      assert( pRight==0 || p->zMalloc==0 );
      sqlite3_free(p->zMalloc);
      sqlite3_free(p);
      p = pRight;
    }
  }
}

/*
** Add a term to the segment being constructed by the SegmentWriter object
** *ppWriter. When adding the first term to a segment, *ppWriter should
** be passed NULL. This function will allocate a new SegmentWriter object
** and return it via the input/output variable *ppWriter in this case.
**
** If successful, SQLITE_OK is returned. Otherwise, an SQLite error code.
*/
static int fts3SegWriterAdd(
  Fts3Table *p,                   /* Virtual table handle */
  SegmentWriter **ppWriter,       /* IN/OUT: SegmentWriter handle */ 
  int isCopyTerm,                 /* True if buffer zTerm must be copied */
  const char *zTerm,              /* Pointer to buffer containing term */
  int nTerm,                      /* Size of term in bytes */
  const char *aDoclist,           /* Pointer to buffer containing doclist */
  int nDoclist                    /* Size of doclist in bytes */
){
  int nPrefix;                    /* Size of term prefix in bytes */
  int nSuffix;                    /* Size of term suffix in bytes */
  int nReq;                       /* Number of bytes required on leaf page */
  int nData;
  SegmentWriter *pWriter = *ppWriter;

  if( !pWriter ){
    int rc;
    sqlite3_stmt *pStmt;

    /* Allocate the SegmentWriter structure */
    pWriter = (SegmentWriter *)sqlite3_malloc(sizeof(SegmentWriter));
    if( !pWriter ) return SQLITE_NOMEM;
    memset(pWriter, 0, sizeof(SegmentWriter));
    *ppWriter = pWriter;

    /* Allocate a buffer in which to accumulate data */
    pWriter->aData = (char *)sqlite3_malloc(p->nNodeSize);
    if( !pWriter->aData ) return SQLITE_NOMEM;
    pWriter->nSize = p->nNodeSize;

    /* Find the next free blockid in the %_segments table */
    rc = fts3SqlStmt(p, SQL_NEXT_SEGMENTS_ID, &pStmt, 0);
    if( rc!=SQLITE_OK ) return rc;
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      pWriter->iFree = sqlite3_column_int64(pStmt, 0);
      pWriter->iFirst = pWriter->iFree;
    }
    rc = sqlite3_reset(pStmt);
    if( rc!=SQLITE_OK ) return rc;
  }
  nData = pWriter->nData;

  nPrefix = fts3PrefixCompress(pWriter->zTerm, pWriter->nTerm, zTerm, nTerm);
  nSuffix = nTerm-nPrefix;

  /* Figure out how many bytes are required by this new entry */
  nReq = sqlite3Fts3VarintLen(nPrefix) +    /* varint containing prefix size */
    sqlite3Fts3VarintLen(nSuffix) +         /* varint containing suffix size */
    nSuffix +                               /* Term suffix */
    sqlite3Fts3VarintLen(nDoclist) +        /* Size of doclist */
    nDoclist;                               /* Doclist data */

  if( nData>0 && nData+nReq>p->nNodeSize ){
    int rc;

    /* The current leaf node is full. Write it out to the database. */
    rc = fts3WriteSegment(p, pWriter->iFree++, pWriter->aData, nData);
    if( rc!=SQLITE_OK ) return rc;

    /* Add the current term to the interior node tree. The term added to
    ** the interior tree must:
    **
    **   a) be greater than the largest term on the leaf node just written
    **      to the database (still available in pWriter->zTerm), and
    **
    **   b) be less than or equal to the term about to be added to the new
    **      leaf node (zTerm/nTerm).
    **
    ** In other words, it must be the prefix of zTerm 1 byte longer than
    ** the common prefix (if any) of zTerm and pWriter->zTerm.
    */
    assert( nPrefix<nTerm );
    rc = fts3NodeAddTerm(p, &pWriter->pTree, isCopyTerm, zTerm, nPrefix+1);
    if( rc!=SQLITE_OK ) return rc;

    nData = 0;
    pWriter->nTerm = 0;

    nPrefix = 0;
    nSuffix = nTerm;
    nReq = 1 +                              /* varint containing prefix size */
      sqlite3Fts3VarintLen(nTerm) +         /* varint containing suffix size */
      nTerm +                               /* Term suffix */
      sqlite3Fts3VarintLen(nDoclist) +      /* Size of doclist */
      nDoclist;                             /* Doclist data */
  }

  /* If the buffer currently allocated is too small for this entry, realloc
  ** the buffer to make it large enough.
  */
  if( nReq>pWriter->nSize ){
    char *aNew = sqlite3_realloc(pWriter->aData, nReq);
    if( !aNew ) return SQLITE_NOMEM;
    pWriter->aData = aNew;
    pWriter->nSize = nReq;
  }
  assert( nData+nReq<=pWriter->nSize );

  /* Append the prefix-compressed term and doclist to the buffer. */
  nData += sqlite3Fts3PutVarint(&pWriter->aData[nData], nPrefix);
  nData += sqlite3Fts3PutVarint(&pWriter->aData[nData], nSuffix);
  memcpy(&pWriter->aData[nData], &zTerm[nPrefix], nSuffix);
  nData += nSuffix;
  nData += sqlite3Fts3PutVarint(&pWriter->aData[nData], nDoclist);
  memcpy(&pWriter->aData[nData], aDoclist, nDoclist);
  pWriter->nData = nData + nDoclist;

  /* Save the current term so that it can be used to prefix-compress the next.
  ** If the isCopyTerm parameter is true, then the buffer pointed to by
  ** zTerm is transient, so take a copy of the term data. Otherwise, just
  ** store a copy of the pointer.
  */
  if( isCopyTerm ){
    if( nTerm>pWriter->nMalloc ){
      char *zNew = sqlite3_realloc(pWriter->zMalloc, nTerm*2);
      if( !zNew ){
        return SQLITE_NOMEM;
      }
      pWriter->nMalloc = nTerm*2;
      pWriter->zMalloc = zNew;
      pWriter->zTerm = zNew;
    }
    assert( pWriter->zTerm==pWriter->zMalloc );
    memcpy(pWriter->zTerm, zTerm, nTerm);
  }else{
    pWriter->zTerm = (char *)zTerm;
  }
  pWriter->nTerm = nTerm;

  return SQLITE_OK;
}

/*
** Flush all data associated with the SegmentWriter object pWriter to the
** database. This function must be called after all terms have been added
** to the segment using fts3SegWriterAdd(). If successful, SQLITE_OK is
** returned. Otherwise, an SQLite error code.
*/
static int fts3SegWriterFlush(
  Fts3Table *p,                   /* Virtual table handle */
  SegmentWriter *pWriter,         /* SegmentWriter to flush to the db */
  int iLevel,                     /* Value for 'level' column of %_segdir */
  int iIdx                        /* Value for 'idx' column of %_segdir */
){
  int rc;                         /* Return code */
  if( pWriter->pTree ){
    sqlite3_int64 iLast = 0;      /* Largest block id written to database */
    sqlite3_int64 iLastLeaf;      /* Largest leaf block id written to db */
    char *zRoot = NULL;           /* Pointer to buffer containing root node */
    int nRoot = 0;                /* Size of buffer zRoot */

    iLastLeaf = pWriter->iFree;
    rc = fts3WriteSegment(p, pWriter->iFree++, pWriter->aData, pWriter->nData);
    if( rc==SQLITE_OK ){
      rc = fts3NodeWrite(p, pWriter->pTree, 1,
          pWriter->iFirst, pWriter->iFree, &iLast, &zRoot, &nRoot);
    }
    if( rc==SQLITE_OK ){
      rc = fts3WriteSegdir(
          p, iLevel, iIdx, pWriter->iFirst, iLastLeaf, iLast, zRoot, nRoot);
    }
  }else{
    /* The entire tree fits on the root node. Write it to the segdir table. */
    rc = fts3WriteSegdir(
        p, iLevel, iIdx, 0, 0, 0, pWriter->aData, pWriter->nData);
  }
  return rc;
}

/*
** Release all memory held by the SegmentWriter object passed as the 
** first argument.
*/
static void fts3SegWriterFree(SegmentWriter *pWriter){
  if( pWriter ){
    sqlite3_free(pWriter->aData);
    sqlite3_free(pWriter->zMalloc);
    fts3NodeFree(pWriter->pTree);
    sqlite3_free(pWriter);
  }
}

/*
** The first value in the apVal[] array is assumed to contain an integer.
** This function tests if there exist any documents with docid values that
** are different from that integer. i.e. if deleting the document with docid
** apVal[0] would mean the FTS3 table were empty.
**
** If successful, *pisEmpty is set to true if the table is empty except for
** document apVal[0], or false otherwise, and SQLITE_OK is returned. If an
** error occurs, an SQLite error code is returned.
*/
static int fts3IsEmpty(Fts3Table *p, sqlite3_value **apVal, int *pisEmpty){
  sqlite3_stmt *pStmt;
  int rc;
  rc = fts3SqlStmt(p, SQL_IS_EMPTY, &pStmt, apVal);
  if( rc==SQLITE_OK ){
    if( SQLITE_ROW==sqlite3_step(pStmt) ){
      *pisEmpty = sqlite3_column_int(pStmt, 0);
    }
    rc = sqlite3_reset(pStmt);
  }
  return rc;
}

/*
** Set *pnSegment to the number of segments of level iLevel in the database.
**
** Return SQLITE_OK if successful, or an SQLite error code if not.
*/
static int fts3SegmentCount(Fts3Table *p, int iLevel, int *pnSegment){
  sqlite3_stmt *pStmt;
  int rc;

  assert( iLevel>=0 );
  rc = fts3SqlStmt(p, SQL_SELECT_LEVEL_COUNT, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  sqlite3_bind_int(pStmt, 1, iLevel);
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    *pnSegment = sqlite3_column_int(pStmt, 0);
  }
  return sqlite3_reset(pStmt);
}

/*
** Set *pnSegment to the total number of segments in the database. Set
** *pnMax to the largest segment level in the database (segment levels
** are stored in the 'level' column of the %_segdir table).
**
** Return SQLITE_OK if successful, or an SQLite error code if not.
*/
static int fts3SegmentCountMax(Fts3Table *p, int *pnSegment, int *pnMax){
  sqlite3_stmt *pStmt;
  int rc;

  rc = fts3SqlStmt(p, SQL_SELECT_SEGDIR_COUNT_MAX, &pStmt, 0);
  if( rc!=SQLITE_OK ) return rc;
  if( SQLITE_ROW==sqlite3_step(pStmt) ){
    *pnSegment = sqlite3_column_int(pStmt, 0);
    *pnMax = sqlite3_column_int(pStmt, 1);
  }
  return sqlite3_reset(pStmt);
}

/*
** This function is used after merging multiple segments into a single large
** segment to delete the old, now redundant, segment b-trees. Specifically,
** it:
** 
**   1) Deletes all %_segments entries for the segments associated with 
**      each of the SegReader objects in the array passed as the third 
**      argument, and
**
**   2) deletes all %_segdir entries with level iLevel, or all %_segdir
**      entries regardless of level if (iLevel<0).
**
** SQLITE_OK is returned if successful, otherwise an SQLite error code.
*/
static int fts3DeleteSegdir(
  Fts3Table *p,                   /* Virtual table handle */
  int iLevel,                     /* Level of %_segdir entries to delete */
  Fts3SegReader **apSegment,      /* Array of SegReader objects */
  int nReader                     /* Size of array apSegment */
){
  int rc;                         /* Return Code */
  int i;                          /* Iterator variable */
  sqlite3_stmt *pDelete;          /* SQL statement to delete rows */

  rc = fts3SqlStmt(p, SQL_DELETE_SEGMENTS_RANGE, &pDelete, 0);
  for(i=0; rc==SQLITE_OK && i<nReader; i++){
    Fts3SegReader *pSegment = apSegment[i];
    if( pSegment->iStartBlock ){
      sqlite3_bind_int64(pDelete, 1, pSegment->iStartBlock);
      sqlite3_bind_int64(pDelete, 2, pSegment->iEndBlock);
      sqlite3_step(pDelete);
      rc = sqlite3_reset(pDelete);
    }
  }
  if( rc!=SQLITE_OK ){
    return rc;
  }

  if( iLevel>=0 ){
    rc = fts3SqlStmt(p, SQL_DELETE_SEGDIR_BY_LEVEL, &pDelete, 0);
    if( rc==SQLITE_OK ){
      sqlite3_bind_int(pDelete, 1, iLevel);
      sqlite3_step(pDelete);
      rc = sqlite3_reset(pDelete);
    }
  }else{
    rc = fts3SqlExec(p, SQL_DELETE_ALL_SEGDIR, 0);
  }

  return rc;
}

/*
** When this function is called, buffer *ppList (size *pnList bytes) contains 
** a position list that may (or may not) feature multiple columns. This
** function adjusts the pointer *ppList and the length *pnList so that they
** identify the subset of the position list that corresponds to column iCol.
**
** If there are no entries in the input position list for column iCol, then
** *pnList is set to zero before returning.
*/
static void fts3ColumnFilter(
  int iCol,                       /* Column to filter on */
  char **ppList,                  /* IN/OUT: Pointer to position list */
  int *pnList                     /* IN/OUT: Size of buffer *ppList in bytes */
){
  char *pList = *ppList;
  int nList = *pnList;
  char *pEnd = &pList[nList];
  int iCurrent = 0;
  char *p = pList;

  assert( iCol>=0 );
  while( 1 ){
    char c = 0;
    while( p<pEnd && (c | *p)&0xFE ) c = *p++ & 0x80;
  
    if( iCol==iCurrent ){
      nList = (int)(p - pList);
      break;
    }

    nList -= (int)(p - pList);
    pList = p;
    if( nList==0 ){
      break;
    }
    p = &pList[1];
    p += sqlite3Fts3GetVarint32(p, &iCurrent);
  }

  *ppList = pList;
  *pnList = nList;
}

/*
** sqlite3Fts3SegReaderIterate() callback used when merging multiple 
** segments to create a single, larger segment.
*/
static int fts3MergeCallback(
  Fts3Table *p,
  void *pContext,
  char *zTerm,
  int nTerm,
  char *aDoclist,
  int nDoclist
){
  SegmentWriter **ppW = (SegmentWriter **)pContext;
  return fts3SegWriterAdd(p, ppW, 1, zTerm, nTerm, aDoclist, nDoclist);
}

/*
** This function is used to iterate through a contiguous set of terms 
** stored in the full-text index. It merges data contained in one or 
** more segments to support this.
**
** The second argument is passed an array of pointers to SegReader objects
** allocated with sqlite3Fts3SegReaderNew(). This function merges the range 
** of terms selected by each SegReader. If a single term is present in
** more than one segment, the associated doclists are merged. For each
** term and (possibly merged) doclist in the merged range, the callback
** function xFunc is invoked with its arguments set as follows.
**
**   arg 0: Copy of 'p' parameter passed to this function
**   arg 1: Copy of 'pContext' parameter passed to this function
**   arg 2: Pointer to buffer containing term
**   arg 3: Size of arg 2 buffer in bytes
**   arg 4: Pointer to buffer containing doclist
**   arg 5: Size of arg 2 buffer in bytes
**
** The 4th argument to this function is a pointer to a structure of type
** Fts3SegFilter, defined in fts3Int.h. The contents of this structure
** further restrict the range of terms that callbacks are made for and
** modify the behaviour of this function. See comments above structure
** definition for details.
*/
int sqlite3Fts3SegReaderIterate(
  Fts3Table *p,                   /* Virtual table handle */
  Fts3SegReader **apSegment,      /* Array of Fts3SegReader objects */
  int nSegment,                   /* Size of apSegment array */
  Fts3SegFilter *pFilter,         /* Restrictions on range of iteration */
  int (*xFunc)(Fts3Table *, void *, char *, int, char *, int),  /* Callback */
  void *pContext                  /* Callback context (2nd argument) */
){
  int i;                          /* Iterator variable */
  char *aBuffer = 0;              /* Buffer to merge doclists in */
  int nAlloc = 0;                 /* Allocated size of aBuffer buffer */
  int rc = SQLITE_OK;             /* Return code */

  int isIgnoreEmpty =  (pFilter->flags & FTS3_SEGMENT_IGNORE_EMPTY);
  int isRequirePos =   (pFilter->flags & FTS3_SEGMENT_REQUIRE_POS);
  int isColFilter =    (pFilter->flags & FTS3_SEGMENT_COLUMN_FILTER);
  int isPrefix =       (pFilter->flags & FTS3_SEGMENT_PREFIX);

  /* If there are zero segments, this function is a no-op. This scenario
  ** comes about only when reading from an empty database.
  */
  if( nSegment==0 ) goto finished;

  /* If the Fts3SegFilter defines a specific term (or term prefix) to search 
  ** for, then advance each segment iterator until it points to a term of
  ** equal or greater value than the specified term. This prevents many
  ** unnecessary merge/sort operations for the case where single segment
  ** b-tree leaf nodes contain more than one term.
  */
  if( pFilter->zTerm ){
    int nTerm = pFilter->nTerm;
    const char *zTerm = pFilter->zTerm;
    for(i=0; i<nSegment; i++){
      Fts3SegReader *pSeg = apSegment[i];
      while( fts3SegReaderTermCmp(pSeg, zTerm, nTerm)<0 ){
        rc = fts3SegReaderNext(pSeg);
        if( rc!=SQLITE_OK ) goto finished;
      }
    }
  }

  fts3SegReaderSort(apSegment, nSegment, nSegment, fts3SegReaderCmp);
  while( apSegment[0]->aNode ){
    int nTerm = apSegment[0]->nTerm;
    char *zTerm = apSegment[0]->zTerm;
    int nMerge = 1;

    /* If this is a prefix-search, and if the term that apSegment[0] points
    ** to does not share a suffix with pFilter->zTerm/nTerm, then all 
    ** required callbacks have been made. In this case exit early.
    **
    ** Similarly, if this is a search for an exact match, and the first term
    ** of segment apSegment[0] is not a match, exit early.
    */
    if( pFilter->zTerm ){
      if( nTerm<pFilter->nTerm 
       || (!isPrefix && nTerm>pFilter->nTerm)
       || memcmp(zTerm, pFilter->zTerm, pFilter->nTerm) 
    ){
        goto finished;
      }
    }

    while( nMerge<nSegment 
        && apSegment[nMerge]->aNode
        && apSegment[nMerge]->nTerm==nTerm 
        && 0==memcmp(zTerm, apSegment[nMerge]->zTerm, nTerm)
    ){
      nMerge++;
    }

    if( nMerge==1 && !isIgnoreEmpty && !isColFilter && isRequirePos ){
      Fts3SegReader *p0 = apSegment[0];
      rc = xFunc(p, pContext, zTerm, nTerm, p0->aDoclist, p0->nDoclist);
      if( rc!=SQLITE_OK ) goto finished;
    }else{
      int nDoclist = 0;           /* Size of doclist */
      sqlite3_int64 iPrev = 0;    /* Previous docid stored in doclist */

      /* The current term of the first nMerge entries in the array
      ** of Fts3SegReader objects is the same. The doclists must be merged
      ** and a single term added to the new segment.
      */
      for(i=0; i<nMerge; i++){
        fts3SegReaderFirstDocid(apSegment[i]);
      }
      fts3SegReaderSort(apSegment, nMerge, nMerge, fts3SegReaderDoclistCmp);
      while( apSegment[0]->pOffsetList ){
        int j;                    /* Number of segments that share a docid */
        char *pList;
        int nList;
        int nByte;
        sqlite3_int64 iDocid = apSegment[0]->iDocid;
        fts3SegReaderNextDocid(apSegment[0], &pList, &nList);
        j = 1;
        while( j<nMerge 
            && apSegment[j]->pOffsetList 
            && apSegment[j]->iDocid==iDocid 
        ){
          fts3SegReaderNextDocid(apSegment[j], 0, 0);
          j++;
        }

        if( isColFilter ){
          fts3ColumnFilter(pFilter->iCol, &pList, &nList);
        }

        if( !isIgnoreEmpty || nList>0 ){
          nByte = sqlite3Fts3VarintLen(iDocid-iPrev) + (isRequirePos?nList+1:0);
          if( nDoclist+nByte>nAlloc ){
            char *aNew;
            nAlloc = nDoclist+nByte*2;
            aNew = sqlite3_realloc(aBuffer, nAlloc);
            if( !aNew ){
              rc = SQLITE_NOMEM;
              goto finished;
            }
            aBuffer = aNew;
          }
          nDoclist += sqlite3Fts3PutVarint(&aBuffer[nDoclist], iDocid-iPrev);
          iPrev = iDocid;
          if( isRequirePos ){
            memcpy(&aBuffer[nDoclist], pList, nList);
            nDoclist += nList;
            aBuffer[nDoclist++] = '\0';
          }
        }

        fts3SegReaderSort(apSegment, nMerge, j, fts3SegReaderDoclistCmp);
      }

      if( nDoclist>0 ){
        rc = xFunc(p, pContext, zTerm, nTerm, aBuffer, nDoclist);
        if( rc!=SQLITE_OK ) goto finished;
      }
    }

    /* If there is a term specified to filter on, and this is not a prefix
    ** search, return now. The callback that corresponds to the required
    ** term (if such a term exists in the index) has already been made.
    */
    if( pFilter->zTerm && !isPrefix ){
      goto finished;
    }

    for(i=0; i<nMerge; i++){
      rc = fts3SegReaderNext(apSegment[i]);
      if( rc!=SQLITE_OK ) goto finished;
    }
    fts3SegReaderSort(apSegment, nSegment, nMerge, fts3SegReaderCmp);
  }

 finished:
  sqlite3_free(aBuffer);
  return rc;
}

/*
** Merge all level iLevel segments in the database into a single 
** iLevel+1 segment. Or, if iLevel<0, merge all segments into a
** single segment with a level equal to the numerically largest level 
** currently present in the database.
**
** If this function is called with iLevel<0, but there is only one
** segment in the database, SQLITE_DONE is returned immediately. 
** Otherwise, if successful, SQLITE_OK is returned. If an error occurs, 
** an SQLite error code is returned.
*/
static int fts3SegmentMerge(Fts3Table *p, int iLevel){
  int i;                          /* Iterator variable */
  int rc;                         /* Return code */
  int iIdx;                       /* Index of new segment */
  int iNewLevel;                  /* Level to create new segment at */
  sqlite3_stmt *pStmt;
  SegmentWriter *pWriter = 0;
  int nSegment = 0;               /* Number of segments being merged */
  Fts3SegReader **apSegment = 0;  /* Array of Segment iterators */
  Fts3SegFilter filter;           /* Segment term filter condition */

  if( iLevel<0 ){
    /* This call is to merge all segments in the database to a single
    ** segment. The level of the new segment is equal to the the numerically 
    ** greatest segment level currently present in the database. The index
    ** of the new segment is always 0.
    */
    iIdx = 0;
    rc = fts3SegmentCountMax(p, &nSegment, &iNewLevel);
    if( nSegment==1 ){
      return SQLITE_DONE;
    }
  }else{
    /* This call is to merge all segments at level iLevel. Find the next
    ** available segment index at level iLevel+1. The call to
    ** fts3AllocateSegdirIdx() will merge the segments at level iLevel+1 to 
    ** a single iLevel+2 segment if necessary.
    */
    iNewLevel = iLevel+1;
    rc = fts3AllocateSegdirIdx(p, iNewLevel, &iIdx);
    if( rc!=SQLITE_OK ) return rc;
    rc = fts3SegmentCount(p, iLevel, &nSegment);
  }
  if( rc!=SQLITE_OK ) return rc;
  assert( nSegment>0 );
  assert( iNewLevel>=0 );

  /* Allocate space for an array of pointers to segment iterators. */
  apSegment = (Fts3SegReader**)sqlite3_malloc(sizeof(Fts3SegReader *)*nSegment);
  if( !apSegment ){
    return SQLITE_NOMEM;
  }
  memset(apSegment, 0, sizeof(Fts3SegReader *)*nSegment);

  /* Allocate a Fts3SegReader structure for each segment being merged. A 
  ** Fts3SegReader stores the state data required to iterate through all 
  ** entries on all leaves of a single segment. 
  */
  assert( SQL_SELECT_LEVEL+1==SQL_SELECT_ALL_LEVEL);
  rc = fts3SqlStmt(p, SQL_SELECT_LEVEL+(iLevel<0), &pStmt, 0);
  if( rc!=SQLITE_OK ) goto finished;
  sqlite3_bind_int(pStmt, 1, iLevel);
  for(i=0; SQLITE_ROW==(sqlite3_step(pStmt)); i++){
    rc = fts3SegReaderNew(p, pStmt, i, &apSegment[i]);
    if( rc!=SQLITE_OK ){
      goto finished;
    }
  }
  rc = sqlite3_reset(pStmt);
  pStmt = 0;
  if( rc!=SQLITE_OK ) goto finished;

  memset(&filter, 0, sizeof(Fts3SegFilter));
  filter.flags = FTS3_SEGMENT_REQUIRE_POS;
  filter.flags |= (iLevel<0 ? FTS3_SEGMENT_IGNORE_EMPTY : 0);
  rc = sqlite3Fts3SegReaderIterate(p, apSegment, nSegment,
      &filter, fts3MergeCallback, (void *)&pWriter
  );
  if( rc!=SQLITE_OK ) goto finished;

  rc = fts3DeleteSegdir(p, iLevel, apSegment, nSegment);
  if( rc==SQLITE_OK ){
    rc = fts3SegWriterFlush(p, pWriter, iNewLevel, iIdx);
  }

 finished:
  fts3SegWriterFree(pWriter);
  if( apSegment ){
    for(i=0; i<nSegment; i++){
      sqlite3Fts3SegReaderFree(p, apSegment[i]);
    }
    sqlite3_free(apSegment);
  }
  sqlite3_reset(pStmt);
  return rc;
}

/*
** This is a comparison function used as a qsort() callback when sorting
** an array of pending terms by term. This occurs as part of flushing
** the contents of the pending-terms hash table to the database.
*/
static int qsortCompare(const void *lhs, const void *rhs){
  char *z1 = fts3HashKey(*(Fts3HashElem **)lhs);
  char *z2 = fts3HashKey(*(Fts3HashElem **)rhs);
  int n1 = fts3HashKeysize(*(Fts3HashElem **)lhs);
  int n2 = fts3HashKeysize(*(Fts3HashElem **)rhs);

  int n = (n1<n2 ? n1 : n2);
  int c = memcmp(z1, z2, n);
  if( c==0 ){
    c = n1 - n2;
  }
  return c;
}


/* 
** Flush the contents of pendingTerms to a level 0 segment.
*/
int sqlite3Fts3PendingTermsFlush(Fts3Table *p){
  Fts3HashElem *pElem;
  int idx, rc, i;
  Fts3HashElem **apElem;          /* Array of pointers to hash elements */
  int nElem;                      /* Number of terms in new segment */
  SegmentWriter *pWriter = 0;     /* Used to write the segment */

  /* Find the number of terms that will make up the new segment. If there
  ** are no terms, return early (do not bother to write an empty segment).
  */
  nElem = fts3HashCount(&p->pendingTerms);
  if( nElem==0 ){
    assert( p->nPendingData==0 );
    return SQLITE_OK;
  }

  /* Determine the next index at level 0, merging as necessary. */
  rc = fts3AllocateSegdirIdx(p, 0, &idx);
  if( rc!=SQLITE_OK ){
    return rc;
  } 

  apElem = sqlite3_malloc(nElem*sizeof(Fts3HashElem *));
  if( !apElem ){
    return SQLITE_NOMEM;
  }

  i = 0;
  for(pElem=fts3HashFirst(&p->pendingTerms); pElem; pElem=fts3HashNext(pElem)){
    apElem[i++] = pElem;
  }
  assert( i==nElem );

  /* TODO(shess) Should we allow user-defined collation sequences,
  ** here?  I think we only need that once we support prefix searches.
  ** Also, should we be using qsort()?
  */
  if( nElem>1 ){
    qsort(apElem, nElem, sizeof(Fts3HashElem *), qsortCompare);
  }


  /* Write the segment tree into the database. */
  for(i=0; rc==SQLITE_OK && i<nElem; i++){
    const char *z = fts3HashKey(apElem[i]);
    int n = fts3HashKeysize(apElem[i]);
    PendingList *pList = fts3HashData(apElem[i]);
    rc = fts3SegWriterAdd(p, &pWriter, 0, z, n, pList->aData, pList->nData+1);
  }
  if( rc==SQLITE_OK ){
    rc = fts3SegWriterFlush(p, pWriter, 0, idx);
  }

  /* Free all allocated resources before returning */
  fts3SegWriterFree(pWriter);
  sqlite3_free(apElem);
  sqlite3Fts3PendingTermsClear(p);
  return rc;
}

/*
** This function does the work for the xUpdate method of FTS3 virtual
** tables.
*/
int sqlite3Fts3UpdateMethod(
  sqlite3_vtab *pVtab,            /* FTS3 vtab object */
  int nArg,                       /* Size of argument array */
  sqlite3_value **apVal,          /* Array of arguments */
  sqlite_int64 *pRowid            /* OUT: The affected (or effected) rowid */
){
  Fts3Table *p = (Fts3Table *)pVtab;
  int rc = SQLITE_OK;             /* Return Code */
  int isRemove = 0;               /* True for an UPDATE or DELETE */
  sqlite3_int64 iRemove = 0;      /* Rowid removed by UPDATE or DELETE */

  /* If this is a DELETE or UPDATE operation, remove the old record. */
  if( sqlite3_value_type(apVal[0])!=SQLITE_NULL ){
    int isEmpty;
    rc = fts3IsEmpty(p, apVal, &isEmpty);
    if( rc==SQLITE_OK ){
      if( isEmpty ){
        /* Deleting this row means the whole table is empty. In this case
        ** delete the contents of all three tables and throw away any
        ** data in the pendingTerms hash table.
        */
        rc = fts3DeleteAll(p);
      }else{
        isRemove = 1;
        iRemove = sqlite3_value_int64(apVal[0]);
        rc = fts3PendingTermsDocid(p, iRemove);
        if( rc==SQLITE_OK ){
          rc = fts3DeleteTerms(p, apVal);
          if( rc==SQLITE_OK ){
            rc = fts3SqlExec(p, SQL_DELETE_CONTENT, apVal);
          }
        }
      }
    }
  }
  
  /* If this is an INSERT or UPDATE operation, insert the new record. */
  if( nArg>1 && rc==SQLITE_OK ){
    rc = fts3InsertData(p, apVal, pRowid);
    if( rc==SQLITE_OK && (!isRemove || *pRowid!=iRemove) ){
      rc = fts3PendingTermsDocid(p, *pRowid);
    }
    if( rc==SQLITE_OK ){
      rc = fts3InsertTerms(p, apVal);
    }
  }

  return rc;
}

/* 
** Flush any data in the pending-terms hash table to disk. If successful,
** merge all segments in the database (including the new segment, if 
** there was any data to flush) into a single segment. 
*/
int sqlite3Fts3Optimize(Fts3Table *p){
  int rc;
  rc = sqlite3_exec(p->db, "SAVEPOINT fts3", 0, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3Fts3PendingTermsFlush(p);
    if( rc==SQLITE_OK ){
      rc = fts3SegmentMerge(p, -1);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_exec(p->db, "RELEASE fts3", 0, 0, 0);
    }else{
      sqlite3_exec(p->db, "ROLLBACK TO fts3 ; RELEASE fts3", 0, 0, 0);
    }
  }
  return rc;
}

#endif
