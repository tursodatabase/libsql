/*
** Copyright (c) 2001 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** This header file defines the interface that the sqlite B-Tree file
** subsystem.
**
** @(#) $Id: btree.h,v 1.11 2001/09/13 13:46:56 drh Exp $
*/

typedef struct Btree Btree;
typedef struct BtCursor BtCursor;

int sqliteBtreeOpen(const char *zFilename, int mode, int nPg, Btree **ppBtree);
int sqliteBtreeClose(Btree*);

int sqliteBtreeBeginTrans(Btree*);
int sqliteBtreeCommit(Btree*);
int sqliteBtreeRollback(Btree*);

int sqliteBtreeCreateTable(Btree*, int*);
int sqliteBtreeDropTable(Btree*, int);
int sqliteBtreeClearTable(Btree*, int);

int sqliteBtreeCursor(Btree*, int iTable, BtCursor **ppCur);
int sqliteBtreeMoveto(BtCursor*, const void *pKey, int nKey, int *pRes);
int sqliteBtreeDelete(BtCursor*);
int sqliteBtreeInsert(BtCursor*, const void *pKey, int nKey,
                                 const void *pData, int nData);
int sqliteBtreeFirst(BtCursor*, int *pRes);
int sqliteBtreeNext(BtCursor*, int *pRes);
int sqliteBtreeKeySize(BtCursor*, int *pSize);
int sqliteBtreeKey(BtCursor*, int offset, int amt, char *zBuf);
int sqliteBtreeDataSize(BtCursor*, int *pSize);
int sqliteBtreeData(BtCursor*, int offset, int amt, char *zBuf);
int sqliteBtreeCloseCursor(BtCursor*);

#define SQLITE_N_BTREE_META 4
int sqliteBtreeGetMeta(Btree*, int*);
int sqliteBtreeUpdateMeta(Btree*, int*);


#ifdef SQLITE_TEST
int sqliteBtreePageDump(Btree*, int, int);
int sqliteBtreeCursorDump(BtCursor*, int*);
Pager *sqliteBtreePager(Btree*);
char *sqliteBtreeSanityCheck(Btree*, int*, int);
#endif
