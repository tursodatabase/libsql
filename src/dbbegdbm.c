/*
** Copyright (c) 2000 D. Richard Hipp
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
** This file uses GDBM as the database backend.  It should be
** relatively simple to convert to a different database such
** as NDBM, SDBM, or BerkeleyDB.
**
** $Id: dbbegdbm.c,v 1.9 2001/08/19 18:19:46 drh Exp $
*/
#ifndef DISABLE_GDBM
#include "sqliteInt.h"
#include <gdbm.h>
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>

/*
** Information about each open disk file is an instance of this 
** structure.  There will only be one such structure for each
** disk file.  If the VDBE opens the same file twice (as will happen
** for a self-join, for example) then two DbbeCursor structures are
** created but there is only a single BeFile structure with an
** nRef of 2.
**
** This backend uses a separate disk file for each database table
** and index.
*/
typedef struct BeFile BeFile;
struct BeFile {
  char *zName;            /* Name of the file */
  GDBM_FILE dbf;          /* The file itself */
  int nRef;               /* Number of references */
  int delOnClose;         /* Delete when closing */
  int writeable;          /* Opened for writing */
  BeFile *pNext, *pPrev;  /* Next and previous on list of open files */
};

/*
** The following structure contains all information used by GDBM
** database driver.  This is a subclass of the Dbbe structure.
*/
typedef struct Dbbex Dbbex;
struct Dbbex {
  Dbbe dbbe;         /* The base class */
  int write;         /* True for write permission */
  int inTrans;       /* Currently in a transaction */
  BeFile *pOpen;     /* List of open files */
  char *zDir;        /* Directory hold the database */
};

/*
** An cursor into a database file is an instance of the following structure.
** There can only be a single BeFile structure for each disk file, but
** there can be multiple DbbeCursor structures.  Each DbbeCursor represents
** a cursor pointing to a particular part of the open BeFile.  The
** BeFile.nRef field hold a count of the number of DbbeCursor structures
** associated with the same disk file.
*/
struct DbbeCursor {
  Dbbex *pBe;        /* The database of which this record is a part */
  BeFile *pFile;     /* The database file for this table */
  datum key;         /* Most recently used key */
  datum data;        /* Most recent data */
  int nextIndex;     /* Next index entry to search */
  int needRewind;    /* Next key should be the first */
  int readPending;   /* The fetch hasn't actually been done yet */
};

/*
** The "mkdir()" function only takes one argument under Windows.
*/
#if OS_WIN
# define mkdir(A,B) mkdir(A)
#endif

/*
** Forward declaration
*/
static void sqliteGdbmCloseCursor(DbbeCursor *pCursr);

/*
** Completely shutdown the given database.  Close all files.  Free all memory.
*/
static void sqliteGdbmClose(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  BeFile *pFile, *pNext;
  for(pFile=pBe->pOpen; pFile; pFile=pNext){
    pNext = pFile->pNext;
    gdbm_close(pFile->dbf);
    memset(pFile, 0, sizeof(*pFile));   
    sqliteFree(pFile);
  }
  memset(pBe, 0, sizeof(*pBe));
  sqliteFree(pBe);
}

/*
** Translate the name of an SQL table (or index) into the name 
** of a file that holds the key/data pairs for that table or
** index.  Space to hold the filename is obtained from
** sqliteMalloc() and must be freed by the calling function.
*/
static char *sqliteFileOfTable(Dbbex *pBe, const char *zTable){
  char *zFile = 0;
  int i;
  sqliteSetString(&zFile, pBe->zDir, "/", zTable, ".tbl", 0);
  if( zFile==0 ) return 0;
  for(i=strlen(pBe->zDir)+1; zFile[i]; i++){
    int c = zFile[i];
    if( isupper(c) ){
      zFile[i] = tolower(c);
    }else if( !isalnum(c) && c!='-' && c!='_' && c!='.' ){
      zFile[i] = '+';
    }
  }
  return zFile;
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
** If zTable is 0 or "", then a temporary database file is created and
** a cursor to that temporary file is opened.  The temporary file
** will be deleted from the disk when it is closed.
*/
static int sqliteGdbmOpenCursor(
  Dbbe *pDbbe,            /* The database the table belongs to */
  const char *zTable,     /* The SQL name of the file to be opened */
  int writeable,          /* True to open for writing */
  int intKeyOnly,         /* True if only integer keys are used */
  DbbeCursor **ppCursr    /* Write the resulting table pointer here */
){
  char *zFile;            /* Name of the table file */
  DbbeCursor *pCursr;     /* The new table cursor */
  BeFile *pFile;          /* The underlying data file for this table */
  int rc = SQLITE_OK;     /* Return value */
  int rw_mask;            /* Permissions mask for opening a table */
  int mode;               /* Mode for opening a table */
  Dbbex *pBe = (Dbbex*)pDbbe;

  if( pBe->inTrans ) writeable = 1;
  *ppCursr = 0;
  pCursr = sqliteMalloc( sizeof(*pCursr) );
  if( pCursr==0 ) return SQLITE_NOMEM;
  if( zTable ){
    zFile = sqliteFileOfTable(pBe, zTable);
    if( zFile==0 ) return SQLITE_NOMEM;
    for(pFile=pBe->pOpen; pFile; pFile=pFile->pNext){
      if( strcmp(pFile->zName,zFile)==0 ) break;
    }
  }else{
    pFile = 0;
    zFile = 0;
  }
  if( pFile==0 ){
    if( writeable ){
      rw_mask = GDBM_WRCREAT | GDBM_FAST;
      mode = 0640;
    }else{
      rw_mask = GDBM_READER;
      mode = 0640;
    }
    pFile = sqliteMalloc( sizeof(*pFile) );
    if( pFile==0 ){
      sqliteFree(zFile);
      return SQLITE_NOMEM;
    }
    if( zFile ){
      if( !writeable || pBe->write ){
        pFile->dbf = gdbm_open(zFile, 0, rw_mask, mode, 0);
      }else{
        pFile->dbf = 0;
      }
    }else{
      int limit;
      char zRandom[50];
      zFile = 0;
      limit = 5;
      do {
        sqliteRandomName(zRandom, "_temp_table_");
        sqliteFree(zFile);
        zFile = sqliteFileOfTable(pBe, zRandom);
        pFile->dbf = gdbm_open(zFile, 0, rw_mask, mode, 0);
      }while( pFile->dbf==0 && limit-- >= 0);
      pFile->delOnClose = 1;
    }
    pFile->writeable = writeable;
    pFile->zName = zFile;
    pFile->nRef = 1 + pBe->inTrans;
    pFile->pPrev = 0;
    if( pBe->pOpen ){
      pBe->pOpen->pPrev = pFile;
    }
    pFile->pNext = pBe->pOpen;
    pBe->pOpen = pFile;
    if( pFile->dbf==0 ){
      if( !writeable && access(zFile,0) ){
        /* Trying to read a non-existant file.  This is OK.  All the
        ** reads will return empty, which is what we want. */
        rc = SQLITE_OK;   
      }else if( pBe->write==0 ){
        rc = SQLITE_READONLY;
      }else if( access(zFile,W_OK|R_OK) ){
        rc = SQLITE_PERM;
      }else{
        rc = SQLITE_BUSY;
      }
    }
  }else{
    sqliteFree(zFile);
    pFile->nRef++;
    if( writeable && !pFile->writeable ){
      rc = SQLITE_READONLY;
    }
  }
  pCursr->pBe = pBe;
  pCursr->pFile = pFile;
  pCursr->readPending = 0;
  pCursr->needRewind = 1;
  if( rc!=SQLITE_OK ){
    sqliteGdbmCloseCursor(pCursr);
    *ppCursr = 0;
  }else{
    *ppCursr = pCursr;
  }
  return rc;
}

/*
** Drop a table from the database.  The file on the disk that corresponds
** to this table is deleted.
*/
static void sqliteGdbmDropTable(Dbbe *pBe, const char *zTable){
  char *zFile;            /* Name of the table file */

  zFile = sqliteFileOfTable((Dbbex*)pBe, zTable);
  unlink(zFile);
  sqliteFree(zFile);
}

/*
** Unlink a file pointer
*/
static void sqliteUnlinkFile(Dbbex *pBe, BeFile *pFile){
  if( pFile->dbf!=NULL ){
    gdbm_close(pFile->dbf);
  }
  if( pFile->pPrev ){
    pFile->pPrev->pNext = pFile->pNext;
  }else{
    pBe->pOpen = pFile->pNext;
  }
  if( pFile->pNext ){
    pFile->pNext->pPrev = pFile->pPrev;
  }
  if( pFile->delOnClose ){
    unlink(pFile->zName);
  }
  sqliteFree(pFile->zName);
  memset(pFile, 0, sizeof(*pFile));
  sqliteFree(pFile);
}

/*
** Close a cursor previously opened by sqliteGdbmOpenCursor().
**
** There can be multiple cursors pointing to the same open file.
** The underlying file is not closed until all cursors have been
** closed.  This routine decrements the BeFile.nref field of the
** underlying file and closes the file when nref reaches 0.
*/
static void sqliteGdbmCloseCursor(DbbeCursor *pCursr){
  BeFile *pFile;
  Dbbex *pBe;
  if( pCursr==0 ) return;
  pFile = pCursr->pFile;
  pBe = pCursr->pBe;
  pFile->nRef--;
  if( pFile->dbf!=NULL ){
    gdbm_sync(pFile->dbf);
  }
  if( pFile->nRef<=0 ){
    sqliteUnlinkFile(pBe, pFile);
  }
  if( pCursr->key.dptr ) free(pCursr->key.dptr);
  if( pCursr->data.dptr ) free(pCursr->data.dptr);
  memset(pCursr, 0, sizeof(*pCursr));
  sqliteFree(pCursr);
}

/*
** Reorganize a table to reduce search times and disk usage.
*/
static int sqliteGdbmReorganizeTable(Dbbe *pBe, const char *zTable){
  DbbeCursor *pCursr;
  int rc;

  rc = sqliteGdbmOpenCursor(pBe, zTable, 1, 0, &pCursr);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( pCursr && pCursr->pFile && pCursr->pFile->dbf ){
    gdbm_reorganize(pCursr->pFile->dbf);
  }
  if( pCursr ){
    sqliteGdbmCloseCursor(pCursr);
  }
  return SQLITE_OK;
}

/*
** Clear the given datum
*/
static void datumClear(datum *p){
  if( p->dptr ) free(p->dptr);
  p->dptr = 0;
  p->dsize = 0;
}

/*
** Fetch a single record from an open cursor.  Return 1 on success
** and 0 on failure.
*/
static int sqliteGdbmFetch(DbbeCursor *pCursr, int nKey, char *pKey){
  datum key;
  key.dsize = nKey;
  key.dptr = pKey;
  datumClear(&pCursr->key);
  datumClear(&pCursr->data);
  if( pCursr->pFile && pCursr->pFile->dbf ){
    pCursr->data = gdbm_fetch(pCursr->pFile->dbf, key);
  }
  return pCursr->data.dptr!=0;
}

/*
** Return 1 if the given key is already in the table.  Return 0
** if it is not.
*/
static int sqliteGdbmTest(DbbeCursor *pCursr, int nKey, char *pKey){
  datum key;
  int result = 0;
  key.dsize = nKey;
  key.dptr = pKey;
  if( pCursr->pFile && pCursr->pFile->dbf ){
    result = gdbm_exists(pCursr->pFile->dbf, key);
  }
  return result;
}

/*
** Copy bytes from the current key or data into a buffer supplied by
** the calling function.  Return the number of bytes copied.
*/
static
int sqliteGdbmCopyKey(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  if( offset>=pCursr->key.dsize ) return 0;
  if( offset+size>pCursr->key.dsize ){
    n = pCursr->key.dsize - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, &pCursr->key.dptr[offset], n);
  return n;
}
static
int sqliteGdbmCopyData(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  if( pCursr->readPending && pCursr->pFile && pCursr->pFile->dbf ){
    pCursr->data = gdbm_fetch(pCursr->pFile->dbf, pCursr->key);
    pCursr->readPending = 0;
  }
  if( offset>=pCursr->data.dsize ) return 0;
  if( offset+size>pCursr->data.dsize ){
    n = pCursr->data.dsize - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, &pCursr->data.dptr[offset], n);
  return n;
}

/*
** Return a pointer to bytes from the key or data.  The data returned
** is ephemeral.
*/
static char *sqliteGdbmReadKey(DbbeCursor *pCursr, int offset){
  if( offset<0 || offset>=pCursr->key.dsize ) return "";
  return &pCursr->key.dptr[offset];
}
static char *sqliteGdbmReadData(DbbeCursor *pCursr, int offset){
  if( pCursr->readPending && pCursr->pFile && pCursr->pFile->dbf ){
    pCursr->data = gdbm_fetch(pCursr->pFile->dbf, pCursr->key);
    pCursr->readPending = 0;
  }
  if( offset<0 || offset>=pCursr->data.dsize ) return "";
  return &pCursr->data.dptr[offset];
}

/*
** Return the total number of bytes in either data or key.
*/
static int sqliteGdbmKeyLength(DbbeCursor *pCursr){
  return pCursr->key.dsize;
}
static int sqliteGdbmDataLength(DbbeCursor *pCursr){
  if( pCursr->readPending && pCursr->pFile && pCursr->pFile->dbf ){
    pCursr->data = gdbm_fetch(pCursr->pFile->dbf, pCursr->key);
    pCursr->readPending = 0;
  }
  return pCursr->data.dsize;
}

/*
** Make is so that the next call to sqliteNextKey() finds the first
** key of the table.
*/
static int sqliteGdbmRewind(DbbeCursor *pCursr){
  pCursr->needRewind = 1;
  return SQLITE_OK;
}

/*
** Read the next key from the table.  Return 1 on success.  Return
** 0 if there are no more keys.
*/
static int sqliteGdbmNextKey(DbbeCursor *pCursr){
  datum nextkey;
  int rc;
  if( pCursr==0 || pCursr->pFile==0 || pCursr->pFile->dbf==0 ){
    pCursr->readPending = 0;
    return 0;
  }
  if( pCursr->needRewind ){
    nextkey = gdbm_firstkey(pCursr->pFile->dbf);
    pCursr->needRewind = 0;
  }else{
    nextkey = gdbm_nextkey(pCursr->pFile->dbf, pCursr->key);
  }
  datumClear(&pCursr->key);
  datumClear(&pCursr->data);
  pCursr->key = nextkey;
  if( pCursr->key.dptr ){
    pCursr->readPending = 1;
    rc = 1;
  }else{
    pCursr->needRewind = 1;
    pCursr->readPending = 0;
    rc = 0;
  }
  return rc;
}

/*
** Get a new integer key.
*/
static int sqliteGdbmNew(DbbeCursor *pCursr){
  int iKey;
  datum key;
  int go = 1;

  if( pCursr->pFile==0 || pCursr->pFile->dbf==0 ) return 1;
  while( go ){
    iKey = sqliteRandomInteger() & 0x7fffffff;
    if( iKey==0 ) continue;
    key.dptr = (char*)&iKey;
    key.dsize = 4;
    go = gdbm_exists(pCursr->pFile->dbf, key);
  }
  return iKey;
}   

/*
** Write an entry into the table.  Overwrite any prior entry with the
** same key.
*/
static int sqliteGdbmPut(
  DbbeCursor *pCursr,  /* Write to the database associated with this cursor */
  int nKey,            /* Number of bytes in the key */
  char *pKey,          /* The data for the key */
  int nData,           /* Number of bytes of data */
  char *pData          /* The data */
){
  datum data, key;
  int rc;
  if( pCursr->pFile==0 || pCursr->pFile->dbf==0 ) return SQLITE_ERROR;
  data.dsize = nData;
  data.dptr = pData;
  key.dsize = nKey;
  key.dptr = pKey;
  rc = gdbm_store(pCursr->pFile->dbf, key, data, GDBM_REPLACE);
  if( rc ) rc = SQLITE_ERROR;
  datumClear(&pCursr->key);
  datumClear(&pCursr->data);
  return rc;
}

/*
** Remove an entry from a table, if the entry exists.
*/
static int sqliteGdbmDelete(DbbeCursor *pCursr, int nKey, char *pKey){
  datum key;
  int rc;
  datumClear(&pCursr->key);
  datumClear(&pCursr->data);
  if( pCursr->pFile==0 || pCursr->pFile->dbf==0 ) return SQLITE_ERROR;
  key.dsize = nKey;
  key.dptr = pKey;
  rc = gdbm_delete(pCursr->pFile->dbf, key);
  if( rc ) rc = SQLITE_ERROR;
  return rc;
}

/*
** Begin a transaction.
*/
static int sqliteGdbmBeginTrans(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  BeFile *pFile;
  if( pBe->inTrans ) return SQLITE_OK;
  for(pFile=pBe->pOpen; pFile; pFile=pFile->pNext){
    pFile->nRef++;
  }
  pBe->inTrans = 1;
  return SQLITE_OK;  
}

/*
** End a transaction.
*/
static int sqliteGdbmEndTrans(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  BeFile *pFile, *pNext;
  if( !pBe->inTrans ) return SQLITE_OK;
  for(pFile=pBe->pOpen; pFile; pFile=pNext){
    pNext = pFile->pNext;
    pFile->nRef--;
    if( pFile->nRef<=0 ){
      sqliteUnlinkFile(pBe, pFile);
    }
  }
  pBe->inTrans = 0;
  return SQLITE_OK;  
}

/*
** Begin scanning an index for the given key.  Return 1 on success and
** 0 on failure.
*/
static int sqliteGdbmBeginIndex(DbbeCursor *pCursr, int nKey, char *pKey){
  if( !sqliteGdbmFetch(pCursr, nKey, pKey) ) return 0;
  pCursr->nextIndex = 0;
  return 1;
}

/*
** Return an integer key which is the next record number in the index search
** that was started by a prior call to BeginIndex.  Return 0 if all records
** have already been searched.
*/
static int sqliteGdbmNextIndex(DbbeCursor *pCursr){
  int *aIdx;
  int nIdx;
  int k;
  nIdx = pCursr->data.dsize/sizeof(int);
  aIdx = (int*)pCursr->data.dptr;
  if( nIdx>1 ){
    k = *(aIdx++);
    if( k>nIdx-1 ) k = nIdx-1;
  }else{
    k = nIdx;
  }
  while( pCursr->nextIndex < k ){
    int recno = aIdx[pCursr->nextIndex++];
    if( recno!=0 ) return recno;
  }
  pCursr->nextIndex = 0;
  return 0;
}

/*
** Write a new record number and key into an index table.  Return a status
** code.
*/
static int sqliteGdbmPutIndex(DbbeCursor *pCursr, int nKey, char *pKey, int N){
  int r = sqliteGdbmFetch(pCursr, nKey, pKey);
  if( r==0 ){
    /* Create a new record for this index */
    sqliteGdbmPut(pCursr, nKey, pKey, sizeof(int), (char*)&N);
  }else{
    /* Extend the existing record */
    int nIdx;
    int *aIdx;
    int k;
            
    nIdx = pCursr->data.dsize/sizeof(int);
    if( nIdx==1 ){
      aIdx = sqliteMalloc( sizeof(int)*4 );
      if( aIdx==0 ) return SQLITE_NOMEM;
      aIdx[0] = 2;
      sqliteGdbmCopyData(pCursr, 0, sizeof(int), (char*)&aIdx[1]);
      aIdx[2] = N;
      sqliteGdbmPut(pCursr, nKey, pKey, sizeof(int)*4, (char*)aIdx);
      sqliteFree(aIdx);
    }else{
      aIdx = (int*)sqliteGdbmReadData(pCursr, 0);
      k = aIdx[0];
      if( k<nIdx-1 ){
        aIdx[k+1] = N;
        aIdx[0]++;
        sqliteGdbmPut(pCursr, nKey, pKey, sizeof(int)*nIdx, (char*)aIdx);
      }else{
        nIdx *= 2;
        aIdx = sqliteMalloc( sizeof(int)*nIdx );
        if( aIdx==0 ) return SQLITE_NOMEM;
        sqliteGdbmCopyData(pCursr, 0, sizeof(int)*(k+1), (char*)aIdx);
        aIdx[k+1] = N;
        aIdx[0]++;
        sqliteGdbmPut(pCursr, nKey, pKey, sizeof(int)*nIdx, (char*)aIdx);
        sqliteFree(aIdx);
      }
    }
  }
  return SQLITE_OK;
}

/*
** Delete an index entry.  Return a status code.
*/
static 
int sqliteGdbmDeleteIndex(DbbeCursor *pCursr, int nKey, char *pKey, int N){
  int *aIdx;
  int nIdx;
  int j, k;
  int rc;
  rc = sqliteGdbmFetch(pCursr, nKey, pKey);
  if( !rc ) return SQLITE_OK;
  nIdx = pCursr->data.dsize/sizeof(int);
  aIdx = (int*)sqliteGdbmReadData(pCursr, 0);
  if( (nIdx==1 && aIdx[0]==N) || (aIdx[0]==1 && aIdx[1]==N) ){
    sqliteGdbmDelete(pCursr, nKey, pKey);
  }else{
    k = aIdx[0];
    for(j=1; j<=k && aIdx[j]!=N; j++){}
    if( j>k ) return SQLITE_OK;
    aIdx[j] = aIdx[k];
    aIdx[k] = 0;
    aIdx[0]--;
    if( aIdx[0]*3 + 1 < nIdx ){
      nIdx /= 2;
    }
    sqliteGdbmPut(pCursr, nKey, pKey, sizeof(int)*nIdx, (char*)aIdx);
  }
  return SQLITE_OK;
}

/*
** This variable contains pointers to all of the access methods
** used to implement the GDBM backend.
*/
static struct DbbeMethods gdbmMethods = {
  /*           Close */   sqliteGdbmClose,
  /*      OpenCursor */   sqliteGdbmOpenCursor,
  /*       DropTable */   sqliteGdbmDropTable,
  /* ReorganizeTable */   sqliteGdbmReorganizeTable,
  /*     CloseCursor */   sqliteGdbmCloseCursor,
  /*           Fetch */   sqliteGdbmFetch,
  /*            Test */   sqliteGdbmTest,
  /*         CopyKey */   sqliteGdbmCopyKey,
  /*        CopyData */   sqliteGdbmCopyData,
  /*         ReadKey */   sqliteGdbmReadKey,
  /*        ReadData */   sqliteGdbmReadData,
  /*       KeyLength */   sqliteGdbmKeyLength,
  /*      DataLength */   sqliteGdbmDataLength,
  /*         NextKey */   sqliteGdbmNextKey,
  /*          Rewind */   sqliteGdbmRewind,
  /*             New */   sqliteGdbmNew,
  /*             Put */   sqliteGdbmPut,
  /*          Delete */   sqliteGdbmDelete,
  /*      BeginTrans */   sqliteGdbmBeginTrans,
  /*          Commit */   sqliteGdbmEndTrans,
  /*        Rollback */   sqliteGdbmEndTrans,
  /*      BeginIndex */   sqliteGdbmBeginIndex,
  /*       NextIndex */   sqliteGdbmNextIndex,
  /*        PutIndex */   sqliteGdbmPutIndex,
  /*     DeleteIndex */   sqliteGdbmDeleteIndex,
};


/*
** This routine opens a new database.  For the GDBM driver
** implemented here, the database name is the name of the directory
** containing all the files of the database.
**
** If successful, a pointer to the Dbbe structure is returned.
** If there are errors, an appropriate error message is left
** in *pzErrMsg and NULL is returned.
*/
Dbbe *sqliteGdbmOpen(
  const char *zName,     /* The name of the database */
  int writeFlag,         /* True if we will be writing to the database */
  int createFlag,        /* True to create database if it doesn't exist */
  char **pzErrMsg        /* Write error messages (if any) here */
){
  Dbbex *pNew;
  struct stat statbuf;
  char *zMaster;

  if( !writeFlag ) createFlag = 0;
  if( stat(zName, &statbuf)!=0 ){
    if( createFlag ) mkdir(zName, 0750);
    if( stat(zName, &statbuf)!=0 ){
      sqliteSetString(pzErrMsg, createFlag ? 
         "can't find or create directory \"" : "can't find directory \"",
         zName, "\"", 0);
      return 0;
    }
  }
  if( !S_ISDIR(statbuf.st_mode) ){
    sqliteSetString(pzErrMsg, "not a directory: \"", zName, "\"", 0);
    return 0;
  }
  if( access(zName, writeFlag ? (X_OK|W_OK|R_OK) : (X_OK|R_OK)) ){
    sqliteSetString(pzErrMsg, "access permission denied", 0);
    return 0;
  }
  zMaster = 0;
  sqliteSetString(&zMaster, zName, "/" MASTER_NAME ".tbl", 0);
  if( stat(zMaster, &statbuf)==0
   && access(zMaster, writeFlag ? (W_OK|R_OK) : R_OK)!=0 ){
    sqliteSetString(pzErrMsg, "access permission denied for ", zMaster, 0);
    sqliteFree(zMaster);
    return 0;
  }
  sqliteFree(zMaster);
  pNew = sqliteMalloc(sizeof(Dbbex) + strlen(zName) + 1);
  if( pNew==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  pNew->dbbe.x = &gdbmMethods;
  pNew->zDir = (char*)&pNew[1];
  strcpy(pNew->zDir, zName);
  pNew->write = writeFlag;
  pNew->pOpen = 0;
  return &pNew->dbbe;
}
#endif /* DISABLE_GDBM */
