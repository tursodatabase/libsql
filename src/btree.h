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
** @(#) $Id: btree.h,v 1.29 2003/04/01 21:16:43 paul Exp $
*/
#ifndef _BTREE_H_
#define _BTREE_H_

typedef struct Btree Btree;
typedef struct BtCursor BtCursor;

struct BtOps {
    int (*sqliteBtreeClose)(Btree*);
    int (*sqliteBtreeSetCacheSize)(Btree*, int);
    int (*sqliteBtreeSetSafetyLevel)(Btree*, int);

    int (*sqliteBtreeBeginTrans)(Btree*);
    int (*sqliteBtreeCommit)(Btree*);
    int (*sqliteBtreeRollback)(Btree*);
    int (*sqliteBtreeBeginCkpt)(Btree*);
    int (*sqliteBtreeCommitCkpt)(Btree*);
    int (*sqliteBtreeRollbackCkpt)(Btree*);

    int (*sqliteBtreeCreateTable)(Btree*, int*);
    int (*sqliteBtreeCreateIndex)(Btree*, int*);
    int (*sqliteBtreeDropTable)(Btree*, int);
    int (*sqliteBtreeClearTable)(Btree*, int);

    int (*sqliteBtreeCursor)(Btree*, int iTable, int wrFlag, BtCursor **ppCur);

    int (*sqliteBtreeGetMeta)(Btree*, int*);
    int (*sqliteBtreeUpdateMeta)(Btree*, int*);

    char *(*sqliteBtreeIntegrityCheck)(Btree*, int*, int);

#ifdef SQLITE_TEST
    int (*sqliteBtreePageDump)(Btree*, int, int);
    struct Pager * (*sqliteBtreePager)(Btree*);
#endif
};

typedef struct BtOps BtOps;

struct BtCursorOps {
    int (*sqliteBtreeMoveto)(BtCursor*, const void *pKey, int nKey, int *pRes);
    int (*sqliteBtreeDelete)(BtCursor*);
    int (*sqliteBtreeInsert)(BtCursor*, const void *pKey, int nKey,
                             const void *pData, int nData);
    int (*sqliteBtreeFirst)(BtCursor*, int *pRes);
    int (*sqliteBtreeLast)(BtCursor*, int *pRes);
    int (*sqliteBtreeNext)(BtCursor*, int *pRes);
    int (*sqliteBtreePrevious)(BtCursor*, int *pRes);
    int (*sqliteBtreeKeySize)(BtCursor*, int *pSize);
    int (*sqliteBtreeKey)(BtCursor*, int offset, int amt, char *zBuf);
    int (*sqliteBtreeKeyCompare)(BtCursor*, const void *pKey, int nKey,
                                 int nIgnore, int *pRes);
    int (*sqliteBtreeDataSize)(BtCursor*, int *pSize);
    int (*sqliteBtreeData)(BtCursor*, int offset, int amt, char *zBuf);
    int (*sqliteBtreeCloseCursor)(BtCursor*);
#ifdef SQLITE_TEST
    int (*sqliteBtreeCursorDump)(BtCursor*, int*);
#endif
};
    
typedef struct BtCursorOps BtCursorOps;

#define SQLITE_N_BTREE_META 10

int sqliteBtreeOpen(const char *zFilename, int mode, int nPg, Btree **ppBtree);

#if !defined(SQLITE_NO_BTREE_DEFS)
#define btOps(pBt) (*((BtOps **)(pBt)))
#define btCOps(pCur) (*((BtCursorOps **)(pCur)))

#define sqliteBtreeClose(pBt)\
                (btOps(pBt)->sqliteBtreeClose(pBt))
#define sqliteBtreeSetCacheSize(pBt, sz)\
                (btOps(pBt)->sqliteBtreeSetCacheSize(pBt, sz))
#define sqliteBtreeSetSafetyLevel(pBt, sl)\
                (btOps(pBt)->sqliteBtreeSetSafetyLevel(pBt, sl))
#define sqliteBtreeBeginTrans(pBt)\
                (btOps(pBt)->sqliteBtreeBeginTrans(pBt))
#define sqliteBtreeCommit(pBt)\
                (btOps(pBt)->sqliteBtreeCommit(pBt))
#define sqliteBtreeRollback(pBt)\
                (btOps(pBt)->sqliteBtreeRollback(pBt))
#define sqliteBtreeBeginCkpt(pBt)\
                (btOps(pBt)->sqliteBtreeBeginCkpt(pBt))
#define sqliteBtreeCommitCkpt(pBt)\
                (btOps(pBt)->sqliteBtreeCommitCkpt(pBt))
#define sqliteBtreeRollbackCkpt(pBt)\
                (btOps(pBt)->sqliteBtreeRollbackCkpt(pBt))
#define sqliteBtreeCreateTable(pBt, piTable)\
                (btOps(pBt)->sqliteBtreeCreateTable(pBt, piTable))
#define sqliteBtreeCreateIndex(pBt, piIndex)\
                (btOps(pBt)->sqliteBtreeCreateIndex(pBt, piIndex))
#define sqliteBtreeDropTable(pBt, iTable)\
                (btOps(pBt)->sqliteBtreeDropTable(pBt, iTable))
#define sqliteBtreeClearTable(pBt, iTable)\
                (btOps(pBt)->sqliteBtreeClearTable(pBt, iTable))
#define sqliteBtreeCursor(pBt, iTable, wrFlag, ppCur)\
                (btOps(pBt)->sqliteBtreeCursor(pBt, iTable, wrFlag, ppCur))
#define sqliteBtreeMoveto(pCur, pKey, nKey, pRes)\
                (btCOps(pCur)->sqliteBtreeMoveto(pCur, pKey, nKey, pRes))
#define sqliteBtreeDelete(pCur)\
                (btCOps(pCur)->sqliteBtreeDelete(pCur))
#define sqliteBtreeInsert(pCur, pKey, nKey, pData, nData) \
                (btCOps(pCur)->sqliteBtreeInsert(pCur, pKey, nKey, pData, nData))
#define sqliteBtreeFirst(pCur, pRes)\
                (btCOps(pCur)->sqliteBtreeFirst(pCur, pRes))
#define sqliteBtreeLast(pCur, pRes)\
                (btCOps(pCur)->sqliteBtreeLast(pCur, pRes))
#define sqliteBtreeNext(pCur, pRes)\
                (btCOps(pCur)->sqliteBtreeNext(pCur, pRes))
#define sqliteBtreePrevious(pCur, pRes)\
                (btCOps(pCur)->sqliteBtreePrevious(pCur, pRes))
#define sqliteBtreeKeySize(pCur, pSize)\
                (btCOps(pCur)->sqliteBtreeKeySize(pCur, pSize) )
#define sqliteBtreeKey(pCur, offset, amt, zBuf)\
                (btCOps(pCur)->sqliteBtreeKey(pCur, offset, amt, zBuf))
#define sqliteBtreeKeyCompare(pCur, pKey, nKey, nIgnore, pRes)\
                (btCOps(pCur)->sqliteBtreeKeyCompare(pCur, pKey, nKey, nIgnore, pRes))
#define sqliteBtreeDataSize(pCur, pSize)\
                (btCOps(pCur)->sqliteBtreeDataSize(pCur, pSize))
#define sqliteBtreeData(pCur, offset, amt, zBuf)\
                (btCOps(pCur)->sqliteBtreeData(pCur, offset, amt, zBuf))
#define sqliteBtreeCloseCursor(pCur)\
                (btCOps(pCur)->sqliteBtreeCloseCursor(pCur))
#define sqliteBtreeGetMeta(pBt, aMeta)\
                (btOps(pBt)->sqliteBtreeGetMeta(pBt, aMeta))
#define sqliteBtreeUpdateMeta(pBt, aMeta)\
                (btOps(pBt)->sqliteBtreeUpdateMeta(pBt, aMeta))
#define sqliteBtreeIntegrityCheck(pBt, aRoot, nRoot)\
                (btOps(pBt)->sqliteBtreeIntegrityCheck(pBt, aRoot, nRoot))
#endif

#ifdef SQLITE_TEST
#if !defined(SQLITE_NO_BTREE_DEFS)
#define sqliteBtreePageDump(pBt, pgno, recursive)\
                (btOps(pBt)->sqliteBtreePageDump(pBt, pgno, recursive))
#define sqliteBtreeCursorDump(pCur, aResult)\
                (btCOps(pCur)->sqliteBtreeCursorDump(pCur, aResult))
#define sqliteBtreePager(pBt)\
                (btOps(pBt)->sqliteBtreePager(pBt))
#endif

int btree_native_byte_order;
#endif

#endif /* _BTREE_H_ */
