/*
** Copyright (c) 1999, 2000 D. Richard Hipp
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
** $Id: dbbe.c,v 1.19 2000/08/17 09:50:00 drh Exp $
*/
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
** The following structure holds the current state of the RC4 algorithm.
** We use RC4 as a random number generator.  Each call to RC4 gives
** a random 8-bit number.
**
** Nothing in this file or anywhere else in SQLite does any kind of
** encryption.  The RC4 algorithm is being used as a PRNG (pseudo-random
** number generator) not as an encryption device.
*/
struct rc4 {
  int i, j;
  int s[256];
};

/*
** The complete database is an instance of the following structure.
*/
struct Dbbe {
  char *zDir;        /* The directory containing the database */
  int write;         /* True for write permission */
  BeFile *pOpen;     /* List of open files */
  int nTemp;         /* Number of temporary files created */
  FILE **apTemp;     /* Space to hold temporary file pointers */
  char **azTemp;     /* Names of the temporary files */
  struct rc4 rc4;    /* The random number generator */
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
  Dbbe *pBe;         /* The database of which this record is a part */
  BeFile *pFile;     /* The database file for this table */
  datum key;         /* Most recently used key */
  datum data;        /* Most recent data */
  int needRewind;    /* Next key should be the first */
  int readPending;   /* The fetch hasn't actually been done yet */
};

/*
** Initialize the RC4 PRNG.  "seed" is a pointer to some random
** data used to initialize the PRNG.  
*/
static void rc4init(struct rc4 *p, char *seed, int seedlen){
  int i;
  char k[256];
  p->j = 0;
  p->i = 0;
  for(i=0; i<256; i++){
    p->s[i] = i;
    k[i] = seed[i%seedlen];
  }
  for(i=0; i<256; i++){
    int t;
    p->j = (p->j + p->s[i] + k[i]) & 0xff;
    t = p->s[p->j];
    p->s[p->j] = p->s[i];
    p->s[i] = t;
  }
}

/*
** Get a single 8-bit random value from the RC4 PRNG.
*/
static int rc4byte(struct rc4 *p){
  int t;
  p->i = (p->i + 1) & 0xff;
  p->j = (p->j + p->s[p->i]) & 0xff;
  t = p->s[p->i];
  p->s[p->i] = p->s[p->j];
  p->s[p->j] = t;
  t = p->s[p->i] + p->s[p->j];
  return t & 0xff;
}

/*
** The "mkdir()" function only takes one argument under Windows.
*/
#if OS_WIN
# define mkdir(A,B) mkdir(A)
#endif

/*
** This routine opens a new database.  For the GDBM driver
** implemented here, the database name is the name of the directory
** containing all the files of the database.
**
** If successful, a pointer to the Dbbe structure is returned.
** If there are errors, an appropriate error message is left
** in *pzErrMsg and NULL is returned.
*/
Dbbe *sqliteDbbeOpen(
  const char *zName,     /* The name of the database */
  int writeFlag,         /* True if we will be writing to the database */
  int createFlag,        /* True to create database if it doesn't exist */
  char **pzErrMsg        /* Write error messages (if any) here */
){
  Dbbe *pNew;
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
  pNew = sqliteMalloc(sizeof(Dbbe) + strlen(zName) + 1);
  if( pNew==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  pNew->zDir = (char*)&pNew[1];
  strcpy(pNew->zDir, zName);
  pNew->write = writeFlag;
  pNew->pOpen = 0;
  time(&statbuf.st_ctime);
  rc4init(&pNew->rc4, (char*)&statbuf, sizeof(statbuf));
  return pNew;
}

/*
** Completely shutdown the given database.  Close all files.  Free all memory.
*/
void sqliteDbbeClose(Dbbe *pBe){
  BeFile *pFile, *pNext;
  int i;
  for(pFile=pBe->pOpen; pFile; pFile=pNext){
    pNext = pFile->pNext;
    gdbm_close(pFile->dbf);
    memset(pFile, 0, sizeof(*pFile));   
    sqliteFree(pFile);
  }
  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]!=0 ){
      unlink(pBe->azTemp[i]);
      fclose(pBe->apTemp[i]);
      sqliteFree(pBe->azTemp[i]);
      pBe->apTemp[i] = 0;
      pBe->azTemp[i] = 0;
      break;
    }
  }
  sqliteFree(pBe->azTemp);
  sqliteFree(pBe->apTemp);
  memset(pBe, 0, sizeof(*pBe));
  sqliteFree(pBe);
}

/*
** Translate the name of an SQL table (or index) into the name 
** of a file that holds the key/data pairs for that table or
** index.  Space to hold the filename is obtained from
** sqliteMalloc() and must be freed by the calling function.
*/
static char *sqliteFileOfTable(Dbbe *pBe, const char *zTable){
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
** Generate a random filename with the given prefix.  The new filename
** is written into zBuf[].  The calling function must insure that
** zBuf[] is big enough to hold the prefix plus 20 or so extra
** characters.
**
** Very random names are chosen so that the chance of a
** collision with an existing filename is very very small.
*/
static void randomName(struct rc4 *pRc4, char *zBuf, char *zPrefix){
  int i, j;
  static const char zRandomChars[] = "abcdefghijklmnopqrstuvwxyz0123456789";
  strcpy(zBuf, zPrefix);
  j = strlen(zBuf);
  for(i=0; i<15; i++){
    int c = rc4byte(pRc4) % (sizeof(zRandomChars) - 1);
    zBuf[j++] = zRandomChars[c];
  }
  zBuf[j] = 0;
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
int sqliteDbbeOpenCursor(
  Dbbe *pBe,              /* The database the table belongs to */
  const char *zTable,     /* The SQL name of the file to be opened */
  int writeable,          /* True to open for writing */
  DbbeCursor **ppCursr    /* Write the resulting table pointer here */
){
  char *zFile;            /* Name of the table file */
  DbbeCursor *pCursr;     /* The new table cursor */
  BeFile *pFile;          /* The underlying data file for this table */
  int rc = SQLITE_OK;     /* Return value */
  int rw_mask;            /* Permissions mask for opening a table */
  int mode;               /* Mode for opening a table */

  *ppCursr = 0;
  pCursr = sqliteMalloc( sizeof(*pCursr) );
  if( pCursr==0 ) return SQLITE_NOMEM;
  if( zTable ){
    zFile = sqliteFileOfTable(pBe, zTable);
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
      struct rc4 *pRc4;
      char zRandom[50];
      pRc4 = &pBe->rc4;
      zFile = 0;
      limit = 5;
      do {
        randomName(&pBe->rc4, zRandom, "_temp_table_");
        sqliteFree(zFile);
        zFile = sqliteFileOfTable(pBe, zRandom);
        pFile->dbf = gdbm_open(zFile, 0, rw_mask, mode, 0);
      }while( pFile->dbf==0 && limit-- >= 0);
      pFile->delOnClose = 1;
    }
    pFile->writeable = writeable;
    pFile->zName = zFile;
    pFile->nRef = 1;
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
    sqliteDbbeCloseCursor(pCursr);
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
void sqliteDbbeDropTable(Dbbe *pBe, const char *zTable){
  char *zFile;            /* Name of the table file */

  zFile = sqliteFileOfTable(pBe, zTable);
  unlink(zFile);
  sqliteFree(zFile);
}

/*
** Reorganize a table to reduce search times and disk usage.
*/
int sqliteDbbeReorganizeTable(Dbbe *pBe, const char *zTable){
  DbbeCursor *pCrsr;
  int rc;

  rc = sqliteDbbeOpenCursor(pBe, zTable, 1, &pCrsr);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  if( pCrsr && pCrsr->pFile && pCrsr->pFile->dbf ){
    gdbm_reorganize(pCrsr->pFile->dbf);
  }
  if( pCrsr ){
    sqliteDbbeCloseCursor(pCrsr);
  }
  return SQLITE_OK;
}

/*
** Close a cursor previously opened by sqliteDbbeOpenCursor().
**
** There can be multiple cursors pointing to the same open file.
** The underlying file is not closed until all cursors have been
** closed.  This routine decrements the BeFile.nref field of the
** underlying file and closes the file when nref reaches 0.
*/
void sqliteDbbeCloseCursor(DbbeCursor *pCursr){
  BeFile *pFile;
  Dbbe *pBe;
  if( pCursr==0 ) return;
  pFile = pCursr->pFile;
  pBe = pCursr->pBe;
  pFile->nRef--;
  if( pFile->dbf!=NULL ){
    gdbm_sync(pFile->dbf);
  }
  if( pFile->nRef<=0 ){
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
  if( pCursr->key.dptr ) free(pCursr->key.dptr);
  if( pCursr->data.dptr ) free(pCursr->data.dptr);
  memset(pCursr, 0, sizeof(*pCursr));
  sqliteFree(pCursr);
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
int sqliteDbbeFetch(DbbeCursor *pCursr, int nKey, char *pKey){
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
int sqliteDbbeTest(DbbeCursor *pCursr, int nKey, char *pKey){
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
int sqliteDbbeCopyKey(DbbeCursor *pCursr, int offset, int size, char *zBuf){
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
int sqliteDbbeCopyData(DbbeCursor *pCursr, int offset, int size, char *zBuf){
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
char *sqliteDbbeReadKey(DbbeCursor *pCursr, int offset){
  if( offset<0 || offset>=pCursr->key.dsize ) return "";
  return &pCursr->key.dptr[offset];
}
char *sqliteDbbeReadData(DbbeCursor *pCursr, int offset){
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
int sqliteDbbeKeyLength(DbbeCursor *pCursr){
  return pCursr->key.dsize;
}
int sqliteDbbeDataLength(DbbeCursor *pCursr){
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
int sqliteDbbeRewind(DbbeCursor *pCursr){
  pCursr->needRewind = 1;
  return SQLITE_OK;
}

/*
** Read the next key from the table.  Return 1 on success.  Return
** 0 if there are no more keys.
*/
int sqliteDbbeNextKey(DbbeCursor *pCursr){
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
int sqliteDbbeNew(DbbeCursor *pCursr){
  int iKey;
  datum key;
  int go = 1;
  int i;
  struct rc4 *pRc4;

  if( pCursr->pFile==0 || pCursr->pFile->dbf==0 ) return 1;
  pRc4 = &pCursr->pBe->rc4;
  while( go ){
    iKey = 0;
    for(i=0; i<4; i++){
      iKey = (iKey<<8) + rc4byte(pRc4);
    }
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
int sqliteDbbePut(DbbeCursor *pCursr, int nKey,char *pKey,int nData,char *pData){
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
int sqliteDbbeDelete(DbbeCursor *pCursr, int nKey, char *pKey){
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
** Open a temporary file.  The file should be deleted when closed.
**
** Note that we can't use the old Unix trick of opening the file
** and then immediately unlinking the file.  That works great
** under Unix, but fails when we try to port to Windows.
*/
int sqliteDbbeOpenTempFile(Dbbe *pBe, FILE **ppFile){
  char *zFile;         /* Full name of the temporary file */
  char zBuf[50];       /* Base name of the temporary file */
  int i;               /* Loop counter */
  int limit;           /* Prevent an infinite loop */
  int rc = SQLITE_OK;  /* Value returned by this function */

  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]==0 ) break;
  }
  if( i>=pBe->nTemp ){
    pBe->nTemp++;
    pBe->apTemp = sqliteRealloc(pBe->apTemp, pBe->nTemp*sizeof(FILE*) );
    pBe->azTemp = sqliteRealloc(pBe->azTemp, pBe->nTemp*sizeof(char*) );
  }
  if( pBe->apTemp==0 ){
    *ppFile = 0;
    return SQLITE_NOMEM;
  }
  limit = 4;
  zFile = 0;
  do{
    randomName(&pBe->rc4, zBuf, "/_temp_file_");
    sqliteFree(zFile);
    zFile = 0;
    sqliteSetString(&zFile, pBe->zDir, zBuf, 0);
  }while( access(zFile,0)==0 && limit-- >= 0 );
  *ppFile = pBe->apTemp[i] = fopen(zFile, "w+");
  if( pBe->apTemp[i]==0 ){
    rc = SQLITE_ERROR;
    sqliteFree(zFile);
    pBe->azTemp[i] = 0;
  }else{
    pBe->azTemp[i] = zFile;
  }
  return rc;
}

/*
** Close a temporary file opened using sqliteDbbeOpenTempFile()
*/
void sqliteDbbeCloseTempFile(Dbbe *pBe, FILE *f){
  int i;
  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]==f ){
      unlink(pBe->azTemp[i]);
      sqliteFree(pBe->azTemp[i]);
      pBe->apTemp[i] = 0;
      pBe->azTemp[i] = 0;
      break;
    }
  }
  fclose(f);
}
