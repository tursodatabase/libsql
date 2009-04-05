/*
** 2008 October 7
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code use to implement an in-memory rollback journal.
** The in-memory rollback journal is used to journal transactions for
** ":memory:" databases and when the journal_mode=MEMORY pragma is used.
**
** @(#) $Id: memjournal.c,v 1.11 2009/04/05 12:22:09 drh Exp $
*/
#include "sqliteInt.h"

/* Forward references to internal structures */
typedef struct MemJournal MemJournal;
typedef struct FilePoint FilePoint;
typedef struct FileChunk FileChunk;

/* Space to hold the rollback journal is allocated in increments of
** this many bytes.
**
** The size chosen is a little less than a power of two.  That way,
** the FileChunk object will have a size that almost exactly fills
** a power-of-two allocation.  This mimimizes wasted space in power-of-two
** memory allocators.
*/
#define JOURNAL_CHUNKSIZE ((int)(1024-sizeof(FileChunk*)))

/* Macro to find the minimum of two numeric values.
*/
#ifndef MIN
# define MIN(x,y) ((x)<(y)?(x):(y))
#endif

/*
** The rollback journal is composed of a linked list of these structures.
*/
struct FileChunk {
  FileChunk *pNext;               /* Next chunk in the journal */
  u8 zChunk[JOURNAL_CHUNKSIZE];   /* Content of this chunk */
};

/*
** An instance of this object serves as a cursor into the rollback journal.
** The cursor can be either for reading or writing.
*/
struct FilePoint {
  sqlite3_int64 iOffset;          /* Offset from the beginning of the file */
  FileChunk *pChunk;              /* Specific chunk into which cursor points */
};

/*
** This subclass is a subclass of sqlite3_file.  Each open memory-journal
** is an instance of this class.
*/
struct MemJournal {
  sqlite3_io_methods *pMethod;    /* Parent class. MUST BE FIRST */
  FileChunk *pFirst;              /* Head of in-memory chunk-list */
  FilePoint endpoint;             /* Pointer to the end of the file */
  FilePoint readpoint;            /* Pointer to the end of the last xRead() */
};

/*
** Read data from the in-memory journal file.  This is the implementation
** of the sqlite3_vfs.xRead method.
*/
static int memjrnlRead(
  sqlite3_file *pJfd,    /* The journal file from which to read */
  void *zBuf,            /* Put the results here */
  int iAmt,              /* Number of bytes to read */
  sqlite_int64 iOfst     /* Begin reading at this offset */
){
  MemJournal *p = (MemJournal *)pJfd;
  u8 *zOut = zBuf;
  int nRead = iAmt;
  int iChunkOffset;
  FileChunk *pChunk;

  /* SQLite never tries to read past the end of a rollback journal file */
  assert( iOfst+iAmt<=p->endpoint.iOffset );

  if( p->readpoint.iOffset!=iOfst || iOfst==0 ){
    sqlite3_int64 iOff = 0;
    for(pChunk=p->pFirst; 
        ALWAYS(pChunk) && (iOff+JOURNAL_CHUNKSIZE)<=iOfst;
        pChunk=pChunk->pNext
    ){
      iOff += JOURNAL_CHUNKSIZE;
    }
  }else{
    pChunk = p->readpoint.pChunk;
  }

  iChunkOffset = (int)(iOfst%JOURNAL_CHUNKSIZE);
  do {
    int iSpace = JOURNAL_CHUNKSIZE - iChunkOffset;
    int nCopy = MIN(nRead, (JOURNAL_CHUNKSIZE - iChunkOffset));
    memcpy(zOut, &pChunk->zChunk[iChunkOffset], nCopy);
    zOut += nCopy;
    nRead -= iSpace;
    iChunkOffset = 0;
  } while( nRead>=0 && (pChunk=pChunk->pNext)!=0 && nRead>0 );
  p->readpoint.iOffset = iOfst+iAmt;
  p->readpoint.pChunk = pChunk;

  return SQLITE_OK;
}

/*
** Write data to the file.
*/
static int memjrnlWrite(
  sqlite3_file *pJfd,    /* The journal file into which to write */
  const void *zBuf,      /* Take data to be written from here */
  int iAmt,              /* Number of bytes to write */
  sqlite_int64 iOfst     /* Begin writing at this offset into the file */
){
  MemJournal *p = (MemJournal *)pJfd;
  int nWrite = iAmt;
  u8 *zWrite = (u8 *)zBuf;

  /* An in-memory journal file should only ever be appended to. Random
  ** access writes are not required by sqlite.
  */
  assert(iOfst==p->endpoint.iOffset);
  UNUSED_PARAMETER(iOfst);

  while( nWrite>0 ){
    FileChunk *pChunk = p->endpoint.pChunk;
    int iChunkOffset = (int)(p->endpoint.iOffset%JOURNAL_CHUNKSIZE);
    int iSpace = MIN(nWrite, JOURNAL_CHUNKSIZE - iChunkOffset);

    if( iChunkOffset==0 ){
      /* New chunk is required to extend the file. */
      FileChunk *pNew = sqlite3_malloc(sizeof(FileChunk));
      if( !pNew ){
        return SQLITE_IOERR_NOMEM;
      }
      pNew->pNext = 0;
      if( pChunk ){
        assert( p->pFirst );
        pChunk->pNext = pNew;
      }else{
        assert( !p->pFirst );
        p->pFirst = pNew;
      }
      p->endpoint.pChunk = pNew;
    }

    memcpy(&p->endpoint.pChunk->zChunk[iChunkOffset], zWrite, iSpace);
    zWrite += iSpace;
    nWrite -= iSpace;
    p->endpoint.iOffset += iSpace;
  }

  return SQLITE_OK;
}

/*
** Truncate the file.
*/
static int memjrnlTruncate(sqlite3_file *pJfd, sqlite_int64 size){
  MemJournal *p = (MemJournal *)pJfd;
  FileChunk *pChunk;
  assert(size==0);
  UNUSED_PARAMETER(size);
  pChunk = p->pFirst;
  while( pChunk ){
    FileChunk *pTmp = pChunk;
    pChunk = pChunk->pNext;
    sqlite3_free(pTmp);
  }
  sqlite3MemJournalOpen(pJfd);
  return SQLITE_OK;
}

/*
** Close the file.
*/
static int memjrnlClose(sqlite3_file *pJfd){
  memjrnlTruncate(pJfd, 0);
  return SQLITE_OK;
}


/*
** Sync the file.
**
** Syncing an in-memory journal is a no-op.  And, in fact, this routine
** is never called in a working implementation.  This implementation
** exists purely as a contingency, in case some malfunction in some other
** part of SQLite causes Sync to be called by mistake.
*/
static int memjrnlSync(sqlite3_file *NotUsed, int NotUsed2){   /*NO_TEST*/
  UNUSED_PARAMETER2(NotUsed, NotUsed2);                        /*NO_TEST*/
  assert( 0 );                                                 /*NO_TEST*/
  return SQLITE_OK;                                            /*NO_TEST*/
}                                                              /*NO_TEST*/

/*
** Query the size of the file in bytes.
*/
static int memjrnlFileSize(sqlite3_file *pJfd, sqlite_int64 *pSize){
  MemJournal *p = (MemJournal *)pJfd;
  *pSize = (sqlite_int64) p->endpoint.iOffset;
  return SQLITE_OK;
}

/*
** Table of methods for MemJournal sqlite3_file object.
*/
static struct sqlite3_io_methods MemJournalMethods = {
  1,                /* iVersion */
  memjrnlClose,     /* xClose */
  memjrnlRead,      /* xRead */
  memjrnlWrite,     /* xWrite */
  memjrnlTruncate,  /* xTruncate */
  memjrnlSync,      /* xSync */
  memjrnlFileSize,  /* xFileSize */
  0,                /* xLock */
  0,                /* xUnlock */
  0,                /* xCheckReservedLock */
  0,                /* xFileControl */
  0,                /* xSectorSize */
  0                 /* xDeviceCharacteristics */
};

/* 
** Open a journal file.
*/
void sqlite3MemJournalOpen(sqlite3_file *pJfd){
  MemJournal *p = (MemJournal *)pJfd;
  assert( EIGHT_BYTE_ALIGNMENT(p) );
  memset(p, 0, sqlite3MemJournalSize());
  p->pMethod = &MemJournalMethods;
}

/*
** Return true if the file-handle passed as an argument is 
** an in-memory journal 
*/
int sqlite3IsMemJournal(sqlite3_file *pJfd){
  return pJfd->pMethods==&MemJournalMethods;
}

/* 
** Return the number of bytes required to store a MemJournal that uses vfs
** pVfs to create the underlying on-disk files.
*/
int sqlite3MemJournalSize(void){
  return sizeof(MemJournal);
}
