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
** This file implements a backend that constructs a database in
** memory using hash tables.  Nothing is ever read or written
** to disk.  Everything is forgotten when the program exits.
**
** $Id: dbbemem.c,v 1.1 2000/10/11 19:28:52 drh Exp $
*/
#include "sqliteInt.h"
#include <time.h>

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
** Key or data is stored as an instance of the following
*/
typedef struct datum {
  void *dptr;    /* The data */
  int dsize;     /* Number of bytes of data */
  void *kptr;    /* The key */
  int ksize;     /* Number of bytes of key */
  datum *pHash;  /* Next datum with the same hash */
}

/*
** Information about each open database table is an instance of this 
** structure.  There will only be one such structure for each
** table.  If the VDBE opens the same table twice (as will happen
** for a self-join, for example) then two DbbeCursor structures are
** created but there is only a single BeFile structure with an
** nRef of 2.
*/
typedef struct BeFile BeFile;
struct BeFile {
  char *zName;             /* Name of the table */
  BeFile *pNext, *pPrev;   /* Next and previous on list of all tables */
  BeFile *pHash;           /* Next table with same hash on zName */
  int nRef;                /* Number of cursor that have this file open */  
  int delOnClose;          /* Delete when the last cursor closes this file */
  int nRec;                /* Number of entries in the hash table */
  int nHash;               /* Number of slots in the hash table */
  datum **aHash;           /* The hash table */
};

/*
** The complete database is an instance of the following structure.
*/
struct Dbbe {
  BeFile *pOpen;    /* List of open tables */
  int nTemp;         /* Number of temporary files created */
  FILE **apTemp;     /* Space to hold temporary file pointers */
  char **azTemp;     /* Names of the temporary files */
  struct rc4 rc4;    /* The random number generator */
  BeFile aHash[331]; /* Hash table of tables */
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
  datum *pRec;       /* Most recently used key and data */
  int h;             /* Hash of pRec */
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
  time_t now;

  pNew = sqliteMalloc(sizeof(Dbbe));
  if( pNew==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  pNew->pOpen = 0;
  time(&now);
  rc4init(&pNew->rc4, (char*)&now, sizeof(now));
  return pNew;
}

/*
** Free all of the memory associated with a single BeFile structure.
** It is assumed that this BeFile structure has already been unlinked
** from its database.
*/
static void sqliteDbbeFreeFile(BeFile *pFile){
  int i;
  for(i=0; i<pFile->nHash; i++){
    datum *pDatum, *pNextDatum;
    for(pDatum = pFile->aHash[i]; pDatum; pDatum=pNextDatum){
      pNextDatum = pDatum->pHash;
      sqliteFree(pDatum->dptr);
      sqliteFree(pDatum);
    }
  }
  sqliteFree(pFile->zName);
  sqliteFree(pFile->aHash);
  memset(pFile, 0, sizeof(*pFile));   
  sqliteFree(pFile);
}

/*
** Completely shutdown the given database.  Close all files.  Free all memory.
*/
void sqliteDbbeClose(Dbbe *pBe){
  BeFile *pFile, *pNext;
  int i;
  for(pFile=pBe->pOpen; pFile; pFile=pNext){
    pNext = pFile->pNext;
    sqliteDbbeFreeFile(pFile);
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
** Hash a NULL-terminated string.
*/
static int sqliteStrHash(const char *z){
  int h = 0;
  while( *z ){
    h = (h<<3) ^ h ^ *(z++);
  }
  if( h<0 ) h = -h;
  return h;
}

/*
** Locate a file in a database
*/
static BeFile *sqliteDbbeFindFile(Dbbe *pBe, const char *zFile){
  int h;
  BeFile *pFile;

  h = sqliteStrHash(zFile) % (sizeof(pBe->aHash)/sizeof(pBe->aHash[0]));
  for(pFile=pBe->aHash[h]; pFile; pFile=pFile->pHash){
    if( strcmp(pFile->zName, zFile)==0 ) break;
  }
  return pFile;
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
    zFile = sqliteStrDup(zTable);
    pFile = sqliteDbbeFindFile(zFile);
  }else{
    pFile = 0;
    zFile = 0;
  }
  if( pFile==0 ){
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
      while( 1 ){
        randomName(&pBe->rc4, zRandom, "_temp_table_");
        sqliteFree(zFile);
        zFile = sqliteStrDup(zRandom);
        if( sqliteDbbeFindFile(pBe, zFile)==0 ) break;
      }
      pFile->delOnClose = 1;
    }
    pFile->zName = zFile;
    pFile->nRef = 1;
    pFile->pPrev = 0;
    if( pBe->pOpen ){
      pBe->pOpen->pPrev = pFile;
    }
    pFile->pNext = pBe->pOpen;
    pBe->pOpen = pFile;
  }else{
    sqliteFree(zFile);
    pFile->nRef++;
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
** Unlink a file from the database
*/
static void sqliteDbbeUnlinkFile(Dbbe *pBe, BeFile *pFile){
  int h = sqliteStrHash(pFile->zName) % 
             (sizeof(pBe->aHash)/sizeof(pBe->aHash[0])));
  if( pBe->aHash[h]==pFile ){
    pBe->aHash[h] = pFile->pHash;
  }else{
    BeFile *pProbe;
    for(pProbe=pBe->aHash[h]; pProbe; pProbe=pProbe->pHash){
      if( pProbe->pHash==pFile ){
        pProbe->pHash = pFile->pHash;
        break;
      }
    }
  }
}

/*
** Drop a table from the database.  The file that corresponds
** to this table is deleted.
*/
void sqliteDbbeDropTable(Dbbe *pBe, const char *zTable){
  File *pFile;

  pFile = sqliteDbbeFindFile(pBe, zTable);
  if( pFile ){
    sqliteDbbeUnlinkFile(pFile);
    sqliteDbbeFreeFile(pFile);
  }
}

/*
** Reorganize a table to reduce search times and disk usage.
*/
int sqliteDbbeReorganizeTable(Dbbe *pBe, const char *zTable){
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
  if( pFile->nRef<=0 ){
    if( pFile->pPrev ){
      pFile->pPrev->pNext = pFile->pNext;
    }else{
      pBe->pOpen = pFile->pNext;
    }
    if( pFile->pNext ){
      pFile->pNext->pPrev = pFile->pPrev;
    }
    if( pFile->delOnClose ){
      sqliteDbbeUnlinkFile(pFile);
      sqliteDbbeFreeFile(pFile);
    }
  }
  memset(pCursr, 0, sizeof(*pCursr));
  sqliteFree(pCursr);
}

/*
** Compute a hash on binary data
*/
static int sqliteBinaryHash(int n, char *z){
  int h = 0;
  while( n-- ){
    h = (h<<9) ^ (h<<3) ^ h ^ *(z++);
  }
  if( h<0 ) h = -h;
  return h;
}

/*
** Resize the hash table
*/
static void sqliteDbbeRehash(BeFile *pFile, int nHash){
  int i, h;
  datum *pRec, *pNextRec;
  datum **aHash;

  if( nHash<1 ) return;
  aHash = sqliteMalloc( sizeof(aHash[0])*nHash );
  if( aHash==0 ) return;
  for(i=0; i<pFile->nHash; i++){
    for(pRec=pFile->aHash[i]; pRec; pRec=pNextRec){
      pNextRec = pRec->pHash;
      h = sqliteBinaryHash(pRec->ksize, pRec->kptr) % nHash;
      pRec->pHash = aHash[h];
      aHash[h] = pRec;
    }
  }
  sqliteFree(pFile->aHash);
  pFile->aHash = aHash;
  pFile->nHash = nHash;
}

/*
** Locate a datum in a file.  Create it if it isn't already there and
** the createFlag is set.
*/
static datum **sqliteDbbeLookup(
  BeFile *pFile,      /* Where to look */
  int nKey,           /* The size of the key */
  char *pKey,         /* The key */
  int *pH,            /* Write the hash line here */
  int createFlag      /* Create a new entry if this is true */
){
  int h;
  datum **ppRec = 0;
  datum *pNew;
  if( pFile->nHash>0 ){
    h = sqliteBinaryHash(nKey, pKey) % pFile->nHash;
    ppRec = &pFile->aHash[h];
    while( *ppRec ){
      if( (**ppRec).ksize==nKey && memcpy((**ppRec).kptr, pKey, nKey)==0 ){
        if( *pH ) *pH = h;
        return ppRec;
      }
    }
  }
  if( createFlag==0 ) return 0;
  if( (pFile->nRec + 1) > pFile->nHash*2 ){
    int nHash = (pFile->nRec + 1)*4;
    if( nHash<51 ) nHash = 51;
    sqliteDbbeRehash(pFile, nHash);
    if( pFile->nHash==0 ) return 0;
  }
  h = sqliteBinaryHash(nKey, pKey) % pFile->nHash;
  pNew = sqliteMalloc( sizeof(*pNew) + nKey );
  if( pNew==0 ) return 0;
  pNew->kptr = (void*)&pNew[1];
  pNew->ksize = nKey;
  memcpy(pNew->kptr, pkey, nKey);
  pNew->pHash = pFile->aHash[h];
  pFile->aHash[h] = pNew;
  pNew->dsize = 0;
  pNew->dptr = 0;
  pFile->nRec++;
  if( pH ) *pH = h;
  return &pFile->aHash[h];
}

/*
** Fetch a single record from an open cursor.  Return 1 on success
** and 0 on failure.
*/
int sqliteDbbeFetch(DbbeCursor *pCursr, int nKey, char *pKey){
  datum **ppRec;
  ppRec = sqliteDbbeLookup(pCursr->pFile, nKey, pKey, &pCursr->h, 0);
  if( ppRec ){
    pCursr->pRec = *ppRec;
  }
  return pCursr->pRec!=0;
}

/*
** Return 1 if the given key is already in the table.  Return 0
** if it is not.
*/
int sqliteDbbeTest(DbbeCursor *pCursr, int nKey, char *pKey){
  return sqliteDbbeFetch(pCursr, nKey, pKey);
}

/*
** Copy bytes from the current key or data into a buffer supplied by
** the calling function.  Return the number of bytes copied.
*/
int sqliteDbbeCopyKey(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  datum *pRec;
  if( (pRec = pCursor->pRec)==0 || offset>=pRec->ksize ) return 0;
  if( offset+size>pRec->ksize ){
    n = pRec->ksize - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, pRec->kptr[offset], n);
  return n;
}
int sqliteDbbeCopyData(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  datum *pRec;
  if( (pRec = pCursr->pRec)==0 || offset>=pRec->dsize ) return 0;
  if( offset+size>pRec->dsize ){
    n = pRec->dsize - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, &pRec->dptr[offset], n);
  return n;
}

/*
** Return a pointer to bytes from the key or data.  The data returned
** is ephemeral.
*/
char *sqliteDbbeReadKey(DbbeCursor *pCursr, int offset){
  datum *pRec;
  if( (pRec = pCursr->pRec)==0 || offset<0 || offset>=pRec->ksize ) return "";
  return &pRec->kptr[offset];
}
char *sqliteDbbeReadData(DbbeCursor *pCursr, int offset){
  datum *pRec;
  if( (pRec = pCursr->pRec)==0 || offset<0 || offset>=pRec->dsize ) return "";
  return &pRec->dptr[offset];
}

/*
** Return the total number of bytes in either data or key.
*/
int sqliteDbbeKeyLength(DbbeCursor *pCursr){
  return pCursr->pRec ? pCursor->pRec->ksize : 0;
}
int sqliteDbbeDataLength(DbbeCursor *pCursr){
  return pCursr->pRec ? pCursor->pRec->dsize : 0;
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
  int h;
  BeFile *pFile;
  if( pCursr==0 || (pFile = pCursr->pFile)==0 || pFile->nHash==0 ){
    return 0;
  }
  if( pCursr->needRewind ){
    pCursr->pRec = 0;
    pCursr->h = -1;
  }
  if( pCursr->pRec ){
    pCursr->pRec = pCursr->pRec->pHash;
  }
  if( pCursr->pRec==0 ){
    for(h=pCursr->h; h<pFile->nHash && pFile->aHash[h]==0; h++){}
    if( h>=pFile->nHash ){
      pCursr->h = -1;
      return 0;
    }else{
      pCursr->h = h;
      pCursr->pRec = pFile->aHash[h];
      return 1;
    }
  }
}

/*
** Get a new integer key.
*/
int sqliteDbbeNew(DbbeCursor *pCursr){
  int iKey;
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
    go = sqliteDbbeLookup(pCursr->pFile, sizeof(iKey), &iKey, 0, 0)!=0;
  }
  return iKey;
}   

/*
** Write an entry into the table.  Overwrite any prior entry with the
** same key.
*/
int sqliteDbbePut(
  DbbeCursor *pCursr,   /* Write to this cursor */
  int nKey,             /* Size of the key */
  char *pKey,           /* The key */
  int nData,            /* Size of the data */
  char *pData           /* The data */
){
  int rc;
  datum **ppRec, *pRec;
  if( pCursr->pFile==0 ) return SQLITE_ERROR;
  ppRec = sqliteDbbeLookup(pCursr->pFile, nKey, pKey, &pCursr->h, 1);
  if( ppRec==0 ) return SQLITE_NOMEM;
  pRec = *ppRec;
  sqliteFree(pRec->dptr);
  pRec->dptr = sqliteMalloc( nData );
  if( pRec->dptr==0 ) return SQLITE_NOMEM;
  memcpy(pRec->dptr, pData, nData);
  pRec->dsize = nData;
  return SQLITE_OK;
}

/*
** Remove an entry from a table, if the entry exists.
*/
int sqliteDbbeDelete(DbbeCursor *pCursr, int nKey, char *pKey){
  datum **ppRec, *pRec;
  ppRec = sqliteDbbeLookcup(pCursr->pFile, nKey, pKey, 0, 0);
  if( ppRec ){
    pRec = *ppRec;
    *ppRec = pRec->pNext;
    if( pCursr->pRec==pRec ){
      pCursr->pRec = 0;
      pCursr->h = -1;
    }
    sqliteFree(pRec->dptr);
    sqliteFree(pRec);
    pCursr->pFile->nRec--;
  }
  return SQLITE_OK;
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
