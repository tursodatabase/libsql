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
** This header file defines the interface that the sqlite B-Tree file
** subsystem.  See comments in the source code for a detailed description
** of what each interface routine does.
**
** @(#) $Id: btree.h,v 1.37 2004/04/26 14:10:21 drh Exp $
*/
#ifndef _BTREE_H_
#define _BTREE_H_

/*
** Forward declarations of structure
*/
typedef struct Btree Btree;
typedef struct BtCursor BtCursor;


int sqlite3BtreeOpen(const char *zFilename, Btree **, int nCache, int flags);

/* The flags parameter to sqlite3BtreeOpen can be the bitwise or of the
** following values.
*/
#define BTREE_OMIT_JOURNAL  1  /* Do not use journal.  No argument */
#define BTREE_MEMORY        2  /* In-memory DB.  No argument */

int sqlite3BtreeClose(Btree*);
int sqlite3BtreeSetCacheSize(Btree*,int);
int sqlite3BtreeSetSafetyLevel(Btree*,int);
int sqlite3BtreeBeginTrans(Btree*);
int sqlite3BtreeCommit(Btree*);
int sqlite3BtreeRollback(Btree*);
int sqlite3BtreeBeginStmt(Btree*);
int sqlite3BtreeCommitStmt(Btree*);
int sqlite3BtreeRollbackStmt(Btree*);
int sqlite3BtreeCreateTable(Btree*, int*, int flags);

/* The flags parameter to sqlite3BtreeCreateTable can be the bitwise OR
** of the following flags:
*/
#define BTREE_INTKEY     1      /* Table has only 64-bit integer keys */
#define BTREE_ZERODATA   2      /* Table has keys only - no data */

int sqlite3BtreeDropTable(Btree*, int);
int sqlite3BtreeClearTable(Btree*, int);
int sqlite3BtreeGetMeta(Btree*, int idx, u32 *pValue);
int sqlite3BtreeUpdateMeta(Btree*, int idx, u32 value);

int sqlite3BtreeCursor(
  Btree*,                              /* BTree containing table to open */
  int iTable,                          /* Index of root page */
  int wrFlag,                          /* 1 for writing.  0 for read-only */
  int(*)(void*,int,const void*,int,const void*),  /* Key comparison function */
  void*,                               /* First argument to compare function */
  BtCursor **ppCursor                  /* Returned cursor */
);

int sqlite3BtreeCursorClose(BtCursor*);
int sqlite3BtreeMoveto(BtCursor*, const void *pKey, u64 nKey, int *pRes);
int sqlite3BtreeDelete(BtCursor*);
int sqlite3BtreeInsert(BtCursor*, const void *pKey, u64 nKey,
                                  const void *pData, int nData);
int sqlite3BtreeFirst(BtCursor*, int *pRes);
int sqlite3BtreeLast(BtCursor*, int *pRes);
int sqlite3BtreeNext(BtCursor*, int *pRes);
int sqlite3BtreePrevious(BtCursor*, int *pRes);
int sqlite3BtreeKeySize(BtCursor*, u64 *pSize);
int sqlite3BtreeKey(BtCursor*, u32 offset, u32 amt, void*);
void *sqlite3BtreeKeyFetch(BtCursor*);
int sqlite3BtreeDataSize(BtCursor*, u32 *pSize);
int sqlite3BtreeData(BtCursor*, u32 offset, u32 amt, void*);


#endif /* _BTREE_H_ */
