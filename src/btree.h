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
** subsystem.
**
** @(#) $Id: btree.h,v 1.16 2001/09/27 03:22:33 drh Exp $
*/
#ifndef _BTREE_H_
#define _BTREE_H_

typedef struct Btree Btree;
typedef struct BtCursor BtCursor;

int sqliteBtreeOpen(const char *zFilename, int mode, int nPg, Btree **ppBtree);
int sqliteBtreeClose(Btree*);
int sqliteBtreeSetCacheSize(Btree*, int);

int sqliteBtreeBeginTrans(Btree*);
int sqliteBtreeCommit(Btree*);
int sqliteBtreeRollback(Btree*);

int sqliteBtreeCreateTable(Btree*, int*);
int sqliteBtreeDropTable(Btree*, int);
int sqliteBtreeClearTable(Btree*, int);

int sqliteBtreeCursor(Btree*, int iTable, int wrFlag, BtCursor **ppCur);
int sqliteBtreeMoveto(BtCursor*, const void *pKey, int nKey, int *pRes);
int sqliteBtreeDelete(BtCursor*);
int sqliteBtreeInsert(BtCursor*, const void *pKey, int nKey,
                                 const void *pData, int nData);
int sqliteBtreeFirst(BtCursor*, int *pRes);
int sqliteBtreeNext(BtCursor*, int *pRes);
int sqliteBtreeKeySize(BtCursor*, int *pSize);
int sqliteBtreeKey(BtCursor*, int offset, int amt, char *zBuf);
int sqliteBtreeKeyCompare(BtCursor*, const void *pKey, int nKey, int *pRes);
int sqliteBtreeDataSize(BtCursor*, int *pSize);
int sqliteBtreeData(BtCursor*, int offset, int amt, char *zBuf);
int sqliteBtreeCloseCursor(BtCursor*);

#define SQLITE_N_BTREE_META 4
int sqliteBtreeGetMeta(Btree*, int*);
int sqliteBtreeUpdateMeta(Btree*, int*);


#ifdef SQLITE_TEST
int sqliteBtreePageDump(Btree*, int, int);
int sqliteBtreeCursorDump(BtCursor*, int*);
struct Pager *sqliteBtreePager(Btree*);
char *sqliteBtreeSanityCheck(Btree*, int*, int);
#endif

#endif /* _BTREE_H_ */
