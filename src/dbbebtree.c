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
** This file contains code to implement the database backend (DBBE)
** for sqlite.  The database backend is the interface between
** sqlite and the code that does the actually reading and writing
** of information to the disk.
**
** This file uses a custom B-Tree implementation as the database backend.
**
** $Id: dbbebtree.c,v 1.1 2001/09/13 13:46:56 drh Exp $
*/
#include "sqliteInt.h"
#include "btree.h"

/*
** The following structure contains all information used by B-Tree
** database driver.  This is a subclass of the Dbbe structure.
*/
typedef struct Dbbex Dbbex;
struct Dbbex {
  Dbbe dbbe;         /* The base class */
  int write;         /* True for write permission */
  int inTrans;       /* Currently in a transaction */
  char *zFile;       /* File containing the database */
  Btree *pBt;        /* Pointer to the open database */
  BtCursor *pCur;    /* Cursor for the main database table */
  DbbeCursor *pDCur; /* List of all Dbbe cursors */
};

/*
** An cursor into a database table is an instance of the following
** structure.
*/
struct DbbeCursor {
  DbbeCursor *pNext; /* Next on list of all cursors */
  DbbeCursor *pPrev; /* Previous on list of all cursors */
  Dbbex *pBe;        /* The database of which this record is a part */
  BtCursor *pCur;    /* The cursor */
  char *zTempFile;   /* Name of file if referring to a temporary table */
  Btree *pTempBt;    /* Database handle, if this is a temporary table */
  char *zKey;        /* Most recent key.  Memory obtained from sqliteMalloc() */
  int nKey;          /* Size of the key */
  char *zKeyBuf;     /* Space used during NextIndex() processing */
  char *zData;       /* Most recent data.  Memory from sqliteMalloc() */
  int needRewind;    /* Next call to Next() returns first entry in table */
  int skipNext;      /* Do not advance cursor for next NextIndex() call */
};

/*
** Forward declaration
*/
static void sqliteBtbeCloseCursor(DbbeCursor *pCursr);

/*
** Completely shutdown the given database.  Close all files.  Free all memory.
*/
static void sqliteBtbeClose(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  assert( pBe->pDCur==0 );
  if( pBe->pCur ){
    sqliteBtreeCloseCursor(pBe->pCur);
  }
  sqliteBtreeClose(pBe->pBt);
  sqliteFree(pBe->zFile);
  sqliteFree(pBe);
}

/*
** Translate a database table name into the table number for the database.
** The pBe->pCur cursor points to table number 2 of the database and that
** table maps all other database names into database number.  Return the
** database number of the table, or return 0 if not found.
*/
static int mapTableNameToNumber(Dbbex *pBe, char *zName){
  int nName = strlen(zName);
  int rc;
  int res;
  if( pBe->pCur==0 ){
    rc = sqliteBtreeCursor(pBe, 2, &pBe->pCur);
    if( rc!=SQLITE_OK ) return 0;
  }
  rc = sqliteBtreeMoveto(pBe->pCur, zName, nName, &res);
  if( rc!=SQLITE_OK || res!=0 ) return 0;
  rc = sqliteBtreeData(pBe->pCur, 0, sizeof(res), &res);
  if( rc!=SQLITE_OK ) return 0;
  return res;
}

/*
** Locate a directory where we can potentially create a temporary
** file.
*/
static const char *findTempDir(void){
  static const char *azDirs[] = {
     "/var/tmp",
     "/usr/tmp",
     "/tmp",
     "/temp",
     ".",
     "./temp",
  };
  int i;
  struct stat buf;
  for(i=0; i<sizeof(azDirs)/sizeof(azDirs[0]); i++){
    if( stat(azDirs[i], &buf)==0 && S_ISDIR(buf.st_mode)
         && S_IWUSR(buf.st_mode) ){
       return azDirs[i];
    }
  }
  return 0;
}

/*
** Open a new table cursor.  Write a pointer to the corresponding
** DbbeCursor structure into *ppCursr.  Return an integer success
** code:
**
**    SQLITE_OK          It worked!
**
**    SQLITE_NOMEM       sqliteMalloc() failed
**
**    SQLITE_PERM        Attempt to access a file for which file
**                       access permission is denied
**
**    SQLITE_BUSY        Another thread or process is already using
**                       the corresponding file and has that file locked.
**
**    SQLITE_READONLY    The current thread already has this file open
**                       readonly but you are trying to open for writing.
**                       (This can happen if a SELECT callback tries to
**                       do an UPDATE or DELETE.)
**
** If the table does not previously exist and writeable is TRUE then
** a new table is created.  If zTable is 0 or "", then a temporary 
** database table is created and a cursor to that temporary file is
** opened.  The temporary file will be deleted when it is closed.
*/
static int sqliteBtbeOpenCursor(
  Dbbe *pDbbe,            /* The database the table belongs to */
  const char *zTable,     /* The SQL name of the file to be opened */
  int writeable,          /* True to open for writing */
  int intKeyOnly,         /* True if only integer keys are used */
  DbbeCursor **ppCursr    /* Write the resulting table pointer here */
){
  char *zFile;            /* Name of the table file */
  DbbeCursor *pCursr;     /* The new table cursor */
  int rc = SQLITE_OK;     /* Return value */
  int rw_mask;            /* Permissions mask for opening a table */
  int mode;               /* Mode for opening a table */
  Dbbex *pBe = (Dbbex*)pDbbe;

  *ppCursr = 0;
  if( pBe->pCur==0 ){
    rc = sqliteBtreeCursor(pBe->pBt, 2, &pBe->pCur);
    if( rc!=SQLITE_OK ) return rc;
  }
  pCursr = sqliteMalloc( sizeof(*pCursr) );
  if( pCursr==0 ) return SQLITE_NOMEM;
  if( zTable ){
    char *zTab;
    int tabId, i;

    if( writeable && pBe->inTrans==0 ){
      rc = sqliteBeginTrans(pBe->pBt);
      if( rc!=SQLITE_OK ){
        sqliteFree(pCursr);
        return rc;
      }
      pBe->inTrans = 1;
    }
    zTab = sqliteStrDup(zTable);
    for(i=0; zTab[i]; i++){
       if( isupper(zTab[i]) ) zTab[i] = tolower(zTab[i]);
    }
    tabId = mapTableNameToNumber(pBe, zTab);
    if( tabId==0 ){
      if( writeable==0 ){
        pCursr->pCur = 0;
      }else{
        rc = sqliteBtreeCreateTable(pBe->pBt, &tabId);
        if( rc!=SQLITE_OK ){
          sqliteFree(pCursr);
          sqliteFree(zTab);
          return rc;
        }
        sqliteBtreeInsert(pBe->pCur, zTab, strlen(zTab), tabId, sizeof(tabId));
      }
    }
    sqliteFree(zTab);
    rc = sqliteBtreeCursor(pBe->pBt, tabId, &pCursr->pCur);
    if( rc!=SQLITE_OK ){
      sqliteFree(pCursr);
      return rc;
    }
    pCursr->zTempFile = 0;
    pCursr->pTempBt = 0;
  }else{
    int nTry = 5;
    char zFileName[200];
    while( nTry>0 ){
      nTry--;
      sprintf(zFileName,"%s/_sqlite_temp_file_%d",
           findTempDir(), sqliteRandomInteger());
      rc = sqliteBtreeOpen(zFileName, 0, 100, &pCursr->pTempBt);
      if( rc!=SQLITE_OK ) continue;
      rc = sqliteBtreeCursor(pCursr->pTempBt, 2, &pCursr->pCur*****
    pFile = 0;
    zFile = 0;
  }
  pCursr->pNext = pBe->pDCur;
  if( pBe->pDCur ){
    pBe->pDCur->pPrev = pCursr;
  }
  pCursr->pPrev = 0;
  pCursr->pBe = pBe;
  pCursr->skipNext = 0;
  pCursr->needRewind = 1;
  return SQLITE_OK;
}

/*
** Drop a table from the database. 
*/
static void sqliteBtbeDropTable(Dbbe *pDbbe, const char *zTable){
  int iTable;
  Dbbex *pBe = (Dbbex*)pDbbe;

  iTable = mapTableNameToNumber(zTable);
  if( iTable>0 ){
    sqliteBtreeDelete(pBe->pCur);
    sqliteBtreeDropTable(pBe->pBt, iTable);
  }
}

/*
** Clear the remembered key and data from the cursor.
*/
static void clearCursorCache(DbbeCursor *pCursr){
  if( pCursr->zKey ){
    sqliteFree(pCursr->zKey);
    pCursr->zKey = 0;
    pCursr->nKey = 0;
    pCursr->zKeyBuf = 0;
  }
  if( pCursr->zData ){
    sqliteFree(pCursr->zData);
    pCursr->zData = 0;
  }
}

/*
** Close a cursor previously opened by sqliteBtbeOpenCursor().
*/
static void sqliteBtbeCloseCursor(DbbeCursor *pCursr){
  Dbbex *pBe;
  if( pCursr==0 ) return;
  if( pCursr->pCur ){
    sqliteBtreeCloseCursor(pCursr->pCur);
  }
  if( pCursr->pTemp ){
    sqliteBtreeClose(pCursr->pTemp);
  }
  if( pCursr->zTempFile ){
    unlink(pCursr->zTempFile);
    sqliteFree(pCursr->zTempFile);
  }
  clearCursorCache(pCursr);
  pBe = pCursr->pBe;
  if( pCursr->pPrev ){
    pCursr->pPrev->pNext = pCursr->pNext;
  }else{
    pBe->pDCur = pCur->pNext;
  }
  if( pCursr->pNext ){
    pCursr->pNext->pPrev = pCursr->pPrev;
  }
  if( pBe->pDCur==0 && pBe->inTrans==0 && pBe->pCur!=0 ){
    sqliteBtreeCloseCursor(pBe->pCur);
    pBe->pCur = 0;
  }
  memset(pCursr, 0, sizeof(*pCursr));
  sqliteFree(pCursr);
}

/*
** Reorganize a table to reduce search times and disk usage.
*/
static int sqliteBtbeReorganizeTable(Dbbe *pBe, const char *zTable){
  return SQLITE_OK;
}

/*
** Move the cursor so that it points to the entry with a key that
** matches the argument.  Return 1 on success and 0 if no keys match
** the argument.
*/
static int sqliteBtbeFetch(DbbeCursor *pCursr, int nKey, char *pKey){
  int rc, res;
  clearCursorCache(pCursr);
  if( pCursr->pCur==0 ) return 0;
  rc = sqliteBtreeMoveto(pCursr->pCur, pKey, nKey, &res);
  return rc==SQLITE_OK && res==0;
}

/*
** Copy bytes from the current key or data into a buffer supplied by
** the calling function.  Return the number of bytes copied.
*/
static
int sqliteBtbeCopyKey(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  if( pCursr->pCur==0 ) return 0;
  int rc = sqliteBtreeKey(pCursr->pCur, offset, amt, zBuf);
  if( rc!=SQLITE_OK ) amt = 0;
  return amt;
}
static
int sqliteBtbeCopyData(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  if( pCursr->pCur==0 ) return 0;
  int rc = sqliteBtreeData(pCursr->pCur, offset, amt, zBuf);
  if( rc!=SQLITE_OK ) amt = 0;
  return amt;
}

/*
** Return a pointer to bytes from the key or data.  The data returned
** is ephemeral.
*/
static char *sqliteBtbeReadKey(DbbeCursor *pCursr, int offset){
  if( pCursr->zKey==0 && pCursr->pCur!=0 ){
    sqliteBtreeKeySize(pCursr->pCur, &pCursr->nKey);
    pCursr->zKey = sqliteMalloc( pCursr->nKey + 1 );
    if( pCursr->zKey==0 ) return 0;
    sqliteBtreeKey(pCursr->pCur, 0, pCursr->nKey, pCursr->zKey);
    pCursr->zKey[pCursor->nKey] = 0;
  }
  return pCursr->zKey;
}
static char *sqliteBtbeReadData(DbbeCursor *pCursr, int offset){
  if( pCursr->zData==0 && pCursr->pCur!=0 ){
    int nData;
    sqliteBtreeDataSize(pCursr->pCur, &nData);
    pCursr->zData = sqliteMalloc( nData + 1 );
    if( pCursr->zData==0 ) return 0;
    sqliteBtreeData(pCursr->pCur, 0, nData, pCursr->zData);
    pCursr->zData[nData] = 0;
  }
  return pCursr->zData;
}

/*
** Return the total number of bytes in either data or key.
*/
static int sqliteBtbeKeyLength(DbbeCursor *pCursr){
  int n;
  if( pCursr->pCur==0 ) return 0;
  sqliteBtreeKeySize(pCursr->pCur, &n);
  return n;
}
static int sqliteBtbeDataLength(DbbeCursor *pCursr){
  int n;
  if( pCursr->pCur==0 ) return 0;
  sqliteBtreeDataSize(pCursr->pCur, &n);
  return n;
}

/*
** Make is so that the next call to sqliteNextKey() finds the first
** key of the table.
*/
static int sqliteBtbeRewind(DbbeCursor *pCursr){
  pCursr->needRewind = 1;
  return SQLITE_OK;
}

/*
** Move the cursor so that it points to the next key in the table.
** Return 1 on success.  Return 0 if there are no more keys in this
** table.
**
** If the pCursr->needRewind flag is set, then move the cursor so
** that it points to the first key of the table.
*/
static int sqliteBtbeNextKey(DbbeCursor *pCursr){
  int rc, res;
  static char zNullKey[1] = { '\000' };
  assert( pCursr!=0 );
  clearCursorCache(pCursr);
  if( pCursr->pCur==0 ) return 0;
  if( pCursr->needRewind ){
    rc = sqliteBtreeFirst(pCursr->pCur, &res);
    return rc==SQLITE_OK && res==0;
  }
  rc = sqliteBtreeNext(pCursr->pCur);
  return rc==SQLITE_OK && res==0;
}

/*
** Get a new integer key.
*/
static int sqliteBtbeNew(DbbeCursor *pCursr){
  int rc;
  int res = 0;

  assert( pCursr->pCur!=0 );
  while( res==0 ){
    iKey = sqliteRandomInteger() & 0x7fffffff;
    if( iKey==0 ) continue;
    rc = sqliteBtreeMoveto(pCursr->pCur, &iKey, sizeof(iKey), &res);
    assert( rc==SQLITE_OK );
  }
  clearCursorCache(pCursr);
  return iKey;
}   

/*
** Write an entry into the table.  Overwrite any prior entry with the
** same key.
*/
static int sqliteBtbePut(
  DbbeCursor *pCursr,  /* Write to the database associated with this cursor */
  int nKey,            /* Number of bytes in the key */
  char *pKey,          /* The data for the key */
  int nData,           /* Number of bytes of data */
  char *pData          /* The data */
){
  clearCursorCache(pCursr);
  assert( pCursr->pCur!=0 );
  return sqliteBtreeInsert(pCursr->pCur, pKey, nKey, pData, nData);
}

/*
** Remove an entry from a table, if the entry exists.
*/
static int sqliteBtbeDelete(DbbeCursor *pCursr, int nKey, char *pKey){
  int rc;
  int res;
  clearCursorCache(pCursr);
  assert( pCursr->pCur!=0 );
  rc = sqliteBtreeMoveto(pCursr->pCur, pKey, nKey, &res);
  if( rc==SQLITE_OK && res==0 ){
    rc = sqliteBtreeDelete(pCursr->pCur);
  }
  return rc;
}

/*
** Begin a transaction.
*/
static int sqliteBtbeBeginTrans(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  if( pBe->inTrans ) return SQLITE_OK;
  sqliteBtreeBeginTrans(pBe->pBt);
  pBe->inTrans = 1;
  return SQLITE_OK;  
}

/*
** Commit a transaction.
*/
static int sqliteBtbeCommit(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  if( !pBe->inTrans ) return SQLITE_OK;
  pBe->inTrans = 0;
  return sqliteBtreeCommit(pBe->pBt);
}

/*
** Rollback a transaction.
*/
static int sqliteBtbeRollback(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  if( !pBe->inTrans ) return SQLITE_OK;
  if( pBt->pDCur!=0 ) return SQLITE_INTERNAL;
  pBe->inTrans = 0;
  if( pBe->pCur ){
    sqliteBtreeCloseCursor(pBe->pCur);
    pBe->pCur = 0;
  }
  return sqliteBtreeRollback(pBe->pBt);
}

/*
** Begin scanning an index for the given key.  Return 1 on success and
** 0 on failure.  (Vdbe ignores the return value.)
*/
static int sqliteBtbeBeginIndex(DbbeCursor *pCursr, int nKey, char *pKey){
  int rc;
  int res;
  clearCursorCache(pCursr);
  if( pCursr->pCur==0 ) return 0;
  pCursr->nKey = nKey;
  pCursr->zKey = sqliteMalloc( 2*(nKey + 1) );
  if( pCursr->zKey==0 ) return 0;
  pCursr->zKeyBuf = &pCursr->zKey[nKey+1];
  memcpy(pCursr->zKey, zKey, nKey);
  pCursr->zKey[nKey] = 0;
  rc = sqliteBtreeMoveTo(pCursr->pCur, pKey, nKey, res);
  pCursr->skipNext = res<0;
  return rc==SQLITE_OK;
}

/*
** Return an integer key which is the next record number in the index search
** that was started by a prior call to BeginIndex.  Return 0 if all records
** have already been searched.
*/
static int sqliteBtbeNextIndex(DbbeCursor *pCursr){
  int rc, res;
  int iRecno;
  BtCursor *pCur = pCursr->pCur;
  if( pCur==0 ) return 0;
  if( pCursr->zKey==0 || pCursr->zKeyBuf==0 ) return 0;
  if( !pCursr->skipNext ){
    rc = sqliteBtreeNext(pCur, &res);
    pCursr->skipNext = 0;
    if( res ) return 0;
  }
  if( sqliteBtreeKeySize(pCur)!=pCursr->nKey+4 ){
    return 0;
  }
  rc = sqliteBtreeKey(pCur, 0, pCursr->nKey, pCursr->zKeyBuf);
  if( rc!=SQLITE_OK || memcmp(pCursr->zKey, pCursr->zKeyBuf, pCursr->nKey)!=0 ){
    return 0;
  }
  sqliteBtreeKey(pCur, pCursr->nKey, 4, &iRecno);
  return iRecno;
}

/*
** Write a new record number and key into an index table.  Return a status
** code.
*/
static int sqliteBtbePutIndex(DbbeCursor *pCursr, int nKey, char *pKey, int N){
  char *zBuf;
  int rc;
  char zStaticSpace[200];

  assert( pCursr->pCur!=0 );
  if( nKey+4>sizeof(zStaticSpace){
    zBuf = sqliteMalloc( nKey + 4 );
    if( zBuf==0 ) return SQLITE_NOMEM;
  }else{
    zBuf = zStaticSpace;
  }
  memcpy(zBuf, pKey, nKey);
  memcpy(&zBuf[nKey], N, 4);
  rc = sqliteBtreeInsert(pCursr->pCur, zBuf, nKey+4, "", 0);
  if( zBuf!=zStaticSpace ){
    sqliteFree(zBuf);
  }
}

/*
** Delete an index entry.  Return a status code.
*/
static 
int sqliteBtbeDeleteIndex(DbbeCursor *pCursr, int nKey, char *pKey, int N){
  char *zBuf;
  int rc;
  char zStaticSpace[200];

  assert( pCursr->pCur!=0 );
  if( nKey+4>sizeof(zStaticSpace){
    zBuf = sqliteMalloc( nKey + 4 );
    if( zBuf==0 ) return SQLITE_NOMEM;
  }else{
    zBuf = zStaticSpace;
  }
  memcpy(zBuf, pKey, nKey);
  memcpy(&zBuf[nKey], N, 4);
  rc = sqliteBtreeMoveto(pCursr->pCur, zBuf, nKey+4, &res);
  if( rc==SQLITE_OK && res==0 ){
    sqliteBtreeDelete(pCursr->pCur);
  }
  if( zBuf!=zStaticSpace ){
    sqliteFree(zBuf);
  }
  return SQLITE_OK;
}

/*
** This variable contains pointers to all of the access methods
** used to implement the GDBM backend.
*/
static struct DbbeMethods btbeMethods = {
  /*           Close */   sqliteBtbeClose,
  /*      OpenCursor */   sqliteBtbeOpenCursor,
  /*       DropTable */   sqliteBtbeDropTable,
  /* ReorganizeTable */   sqliteBtbeReorganizeTable,
  /*     CloseCursor */   sqliteBtbeCloseCursor,
  /*           Fetch */   sqliteBtbeFetch,
  /*            Test */   sqliteBtbeFetch,
  /*         CopyKey */   sqliteBtbeCopyKey,
  /*        CopyData */   sqliteBtbeCopyData,
  /*         ReadKey */   sqliteBtbeReadKey,
  /*        ReadData */   sqliteBtbeReadData,
  /*       KeyLength */   sqliteBtbeKeyLength,
  /*      DataLength */   sqliteBtbeDataLength,
  /*         NextKey */   sqliteBtbeNextKey,
  /*          Rewind */   sqliteBtbeRewind,
  /*             New */   sqliteBtbeNew,
  /*             Put */   sqliteBtbePut,
  /*          Delete */   sqliteBtbeDelete,
  /*      BeginTrans */   sqliteBtbeBeginTrans,
  /*          Commit */   sqliteBtbeCommit,
  /*        Rollback */   sqliteBtbeRollback,
  /*      BeginIndex */   sqliteBtbeBeginIndex,
  /*       NextIndex */   sqliteBtbeNextIndex,
  /*        PutIndex */   sqliteBtbePutIndex,
  /*     DeleteIndex */   sqliteBtbeDeleteIndex,
};


/*
** This routine opens a new database.  For the BTree driver
** implemented here, the database name is the name of a single
** file that contains all tables of the database.
**
** If successful, a pointer to the Dbbe structure is returned.
** If there are errors, an appropriate error message is left
** in *pzErrMsg and NULL is returned.
*/
Dbbe *sqliteBtbeOpen(
  const char *zName,     /* The name of the database */
  int writeFlag,         /* True if we will be writing to the database */
  int createFlag,        /* True to create database if it doesn't exist */
  char **pzErrMsg        /* Write error messages (if any) here */
){
  Dbbex *pNew;
  char *zTemp;
  Btree *pBt;
  int rc;

  rc = sqliteBtreeOpen(zName, 0, 100, &pBt);
  if( rc!=SQLITE_OK ){
    sqliteSetString(pzErrMsg, "unable to open database file \"", zName, "\"",0);
    return 0;
  }
  pNew = sqliteMalloc(sizeof(Dbbex) + strlen(zName) + 1);
  if( pNew==0 ){
    sqliteBtreeCloseCursor(pCur);
    sqliteBtreeClose(pBt);
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  pNew->dbbe.x = &btbeMethods;
  pNew->write = writeFlag;
  pNew->inTrans = 0;
  pNew->zFile = (char*)&pNew[1];
  strcpy(pNew->zFile, zName);
  pNew->pBt = pBt;
  pNew->pCur = 0;
  return &pNew->dbbe;
}
#endif /* DISABLE_GDBM */
