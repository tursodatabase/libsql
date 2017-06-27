/*
** 2017 April 24
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
*/

#include "sqliteInt.h"

/*
** Page-locking slot format:
**
**   Assuming HMA_MAX_TRANSACTIONID is set to 16.
**
**   The least-significant 16 bits are used for read locks. When a read
**   lock is taken, the client sets the bit associated with its 
**   transaction-id.
**
**   The next 8 bits are set to the number of transient-read locks 
**   currently held on the page.
**
**   The next 5 bits are set to 0 if no client currently holds a write
**   lock. Or to (transaction-id + 1) if a write lock is held.
*/

#ifdef SQLITE_SERVER_EDITION

/* Number of page-locking slots */
#define HMA_PAGELOCK_SLOTS (256*1024)

/* Maximum concurrent read/write transactions */
#define HMA_MAX_TRANSACTIONID 16

/*
** The argument to this macro is the value of a locking slot. It returns
** -1 if no client currently holds the write lock, or the transaction-id
** of the locker otherwise.
*/
#define slotGetWriter(v) (((int)((v) >> HMA_MAX_TRANSACTIONID) & 0x1f) -1)

#define slotReaderMask(v) ((v) & ((1 << HMA_MAX_TRANSACTIONID)-1))

#include "unistd.h"
#include "fcntl.h"
#include "sys/mman.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "errno.h"

typedef struct ServerDb ServerDb;
typedef struct ServerJournal ServerJournal;

struct ServerGlobal {
  ServerDb *pDb;                  /* Linked list of all ServerHMA objects */
};
static struct ServerGlobal g_server;

struct ServerJournal {
  char *zJournal;
  sqlite3_file *jfd;
};

/*
** There is one instance of the following structure for each distinct 
** database file opened in server mode by this process.
*/
struct ServerDb {
  sqlite3_mutex *mutex;           /* Non-recursive mutex */
  int nClient;                    /* Current number of clients */
  int bInit;                      /* True once initialized */
  u32 transmask;                  /* Bitmask of taken transaction ids */
  u32 *aSlot;                     /* Array of page locking slots */
  i64 aFileId[2];                 /* Opaque VFS file-id */
  ServerDb *pNext;                /* Next db in this process */

  sqlite3_vfs *pVfs;
  ServerJournal aJrnl[HMA_MAX_TRANSACTIONID];
  u8 *aJrnlFdSpace;
};

/*
** Once instance for each client connection open on a server mode database
** in this process.
*/
struct Server {
  ServerDb *pDb;                  /* Database object */
  Pager *pPager;                  /* Associated pager object */
  int iTransId;                   /* Current transaction id (or -1) */
  int nAlloc;                     /* Allocated size of aLock[] array */
  int nLock;                      /* Number of entries in aLock[] */
  u32 *aLock;                     /* Mapped lock file */
};

#define SERVER_WRITE_LOCK 3
#define SERVER_READ_LOCK  2
#define SERVER_NO_LOCK    1

/*
** Global mutex functions used by code in this file.
*/
static void serverEnterMutex(void){
  sqlite3_mutex_enter(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_APP1));
}
static void serverLeaveMutex(void){
  sqlite3_mutex_leave(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_APP1));
}
static void serverAssertMutexHeld(void){
  assert( sqlite3_mutex_held(sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_APP1)) );
}

static int serverFindDatabase(Server *pNew, i64 *aFileId){
  ServerDb *p;
  int rc = SQLITE_OK;
  serverEnterMutex();
  for(p=g_server.pDb; p; p=p->pNext){
    if( p->aFileId[0]==aFileId[0] && p->aFileId[1]==aFileId[1] ){
      break;
    }
  }
  if( p==0 ){
    p = (ServerDb*)sqlite3MallocZero(sizeof(ServerDb));
    if( p ){
      p->aSlot = (u32*)sqlite3MallocZero(sizeof(u32)*HMA_PAGELOCK_SLOTS);
      if( p->aSlot==0 ){
        rc = SQLITE_NOMEM_BKPT;
      }else{
        p->mutex = sqlite3_mutex_alloc(SQLITE_MUTEX_FAST);
#if SQLITE_THREADSAFE!=0
        if( p->mutex==0 ) rc = SQLITE_NOMEM_BKPT;
#endif
      }

      if( rc==SQLITE_NOMEM ){
        sqlite3_free(p->aSlot);
        sqlite3_free(p);
        p = 0;
      }else{
        p->nClient = 1;
        p->aFileId[0] = aFileId[0];
        p->aFileId[1] = aFileId[1];
        p->pNext = g_server.pDb;
        g_server.pDb = p;
      }
    }else{
      rc = SQLITE_NOMEM_BKPT;
    }
  }else{
    p->nClient++;
  }
  pNew->pDb = p;
  serverLeaveMutex();
  return rc;
}

/*
** Free all resources allocated by serverInitDatabase() associated with the
** object passed as the only argument.
*/
static void serverShutdownDatabase(ServerDb *pDb){
  int i;

  for(i=0; i<HMA_MAX_TRANSACTIONID; i++){
    ServerJournal *pJ = &pDb->aJrnl[i];
    if( pJ->jfd ){
      sqlite3OsClose(pJ->jfd);
      sqlite3OsDelete(pDb->pVfs, pJ->zJournal, 0);
    }
    sqlite3_free(pJ->zJournal);
  }
  memset(pDb->aJrnl, 0, sizeof(ServerJournal)*HMA_MAX_TRANSACTIONID);

  if( pDb->aJrnlFdSpace ){
    sqlite3_free(pDb->aJrnlFdSpace);
    pDb->aJrnlFdSpace = 0;
  }

  sqlite3_free(pDb->aSlot);
  pDb->bInit = 0;
}

/*
** This function is called when the very first connection to a database
** is established. It is responsible for rolling back any hot journal
** files found in the file-system.
*/
static int serverInitDatabase(Server *pNew){
  int nByte;
  int rc = SQLITE_OK;
  ServerDb *pDb = pNew->pDb;
  sqlite3_vfs *pVfs;
  const char *zFilename = sqlite3PagerFilename(pNew->pPager, 0);

  assert( zFilename );
  pVfs = pDb->pVfs = sqlite3PagerVfs(pNew->pPager);
  nByte = ROUND8(pVfs->szOsFile) * HMA_MAX_TRANSACTIONID;
  pDb->aJrnlFdSpace = (u8*)sqlite3MallocZero(nByte);
  if( pDb->aJrnlFdSpace==0 ){
    rc = SQLITE_NOMEM_BKPT;
  }else{
    u8 *a = pDb->aJrnlFdSpace;
    int i;
    for(i=0; rc==SQLITE_OK && i<HMA_MAX_TRANSACTIONID; i++){
      int bExists = 0;
      ServerJournal *pJ = &pDb->aJrnl[i];
      pJ->jfd = (sqlite3_file*)&a[ROUND8(pVfs->szOsFile)*i];
      pJ->zJournal = sqlite3_mprintf("%s-journal%d", zFilename, i);
      if( pJ->zJournal==0 ){
        rc = SQLITE_NOMEM_BKPT;
        break;
      }

      rc = sqlite3OsAccess(pVfs, pJ->zJournal, SQLITE_ACCESS_EXISTS, &bExists);
      if( rc==SQLITE_OK && bExists ){
        int flags = SQLITE_OPEN_READWRITE|SQLITE_OPEN_MAIN_JOURNAL;
        rc = sqlite3OsOpen(pVfs, pJ->zJournal, pJ->jfd, flags, &flags);
        if( rc==SQLITE_OK ){
          rc = sqlite3PagerRollbackJournal(pNew->pPager, pJ->jfd);
        }
      }
    }
  }

  if( rc==SQLITE_OK ){
    pDb->bInit = 1;
  }else{
    serverShutdownDatabase(pNew->pDb);
  }
  return rc;
}

/*
** Close the connection.
*/
void sqlite3ServerDisconnect(Server *p, sqlite3_file *dbfd){
  ServerDb *pDb = p->pDb;

  serverEnterMutex();
  pDb->nClient--;
  if( pDb->nClient==0 ){
    ServerDb **pp;
    serverShutdownDatabase(pDb);
    for(pp=&g_server.pDb; *pp!=pDb; pp=&((*pp)->pNext));
    *pp = pDb->pNext;
    sqlite3_mutex_free(pDb->mutex);
    sqlite3_free(pDb);
  }
  serverLeaveMutex();

  sqlite3_free(p->aLock);
  sqlite3_free(p);
}

/*
** Connect to the system.
*/
int sqlite3ServerConnect(
  Pager *pPager,
  Server **ppOut                  /* OUT: Server handle */
){
  Server *pNew = 0;
  sqlite3_file *dbfd = sqlite3PagerFile(pPager);
  i64 aFileId[2];
  int rc;

  rc = sqlite3OsFileControl(dbfd, SQLITE_FCNTL_FILEID, (void*)aFileId);
  if( rc==SQLITE_OK ){
    pNew = (Server*)sqlite3MallocZero(sizeof(Server));
    if( pNew ){
      pNew->pPager = pPager;
      pNew->iTransId = -1;
      rc = serverFindDatabase(pNew, aFileId);
      if( rc!=SQLITE_OK ){
        sqlite3_free(pNew);
        pNew = 0;
      }else{
        sqlite3_mutex_enter(pNew->pDb->mutex);
        if( pNew->pDb->bInit==0 ){
          rc = serverInitDatabase(pNew);
        }
        sqlite3_mutex_leave(pNew->pDb->mutex);
      }
    }else{
      rc = SQLITE_NOMEM_BKPT;
    }
  }

  *ppOut = pNew;
  return rc;
}

/*
** Begin a transaction.
*/
int sqlite3ServerBegin(Server *p){
  int rc = SQLITE_OK;

  if( p->iTransId<0 ){
    int id;
    ServerDb *pDb = p->pDb;
    u32 t;

    sqlite3_mutex_enter(pDb->mutex);

    /* Find a transaction id to use */
    rc = SQLITE_BUSY;
    t = pDb->transmask;
    for(id=0; id<HMA_MAX_TRANSACTIONID; id++){
      if( (t & (1 << id))==0 ){
        t = t | (1 << id);
        rc = SQLITE_OK;
        break;
      }
    }
    pDb->transmask = t;

    sqlite3_mutex_leave(pDb->mutex);

    if( rc==SQLITE_OK ){
      ServerJournal *pJrnl = &pDb->aJrnl[id];
      sqlite3PagerServerJournal(p->pPager, pJrnl->jfd, pJrnl->zJournal);
      p->iTransId = id;
    }
  }

  return rc;
}

static void serverReleaseLocks(Server *p){
  ServerDb *pDb = p->pDb;
  int i;
  assert( sqlite3_mutex_held(pDb->mutex) );

  for(i=0; i<p->nLock; i++){
    u32 *pSlot = &pDb->aSlot[p->aLock[i] % HMA_PAGELOCK_SLOTS];
    if( slotGetWriter(*pSlot)==p->iTransId ){
      *pSlot -= ((p->iTransId + 1) << HMA_MAX_TRANSACTIONID);
    }
    *pSlot &= ~((u32)1 << p->iTransId);
  }

  p->nLock = 0;
}

/*
** End a transaction (and release all locks).
*/
int sqlite3ServerEnd(Server *p){
  int rc = SQLITE_OK;
  ServerDb *pDb = p->pDb;
  sqlite3_mutex_enter(pDb->mutex);

  serverReleaseLocks(p);
  pDb->transmask &= ~((u32)1 << p->iTransId);

  sqlite3_mutex_leave(pDb->mutex);
  p->iTransId = -1;
  return rc;
}

/*
** Release all write-locks.
*/
int sqlite3ServerReleaseWriteLocks(Server *p){
  int rc = SQLITE_OK;
  return rc;
}

/*
** Lock page pgno for reading (bWrite==0) or writing (bWrite==1).
**
** If parameter bBlock is non-zero, then make this a blocking lock if
** possible.
*/
int sqlite3ServerLock(Server *p, Pgno pgno, int bWrite, int bBlock){
  int rc = SQLITE_OK;
  ServerDb *pDb = p->pDb;
  int iWriter;
  int bSkip = 0;
  u32 *pSlot;

  assert( p->nLock<=p->nAlloc );
  if( p->nLock==p->nAlloc ){
    int nNew = p->nLock ? p->nLock*2 : 256;
    u32 *aNew = sqlite3_realloc(p->aLock, nNew*sizeof(u32));
    if( aNew==0 ) return SQLITE_NOMEM_BKPT;
    memset(&aNew[p->nLock], 0, sizeof(u32) * (nNew - p->nLock));
    p->nAlloc = nNew;
    p->aLock = aNew;
  }

  assert( p->iTransId>=0 );

  sqlite3_mutex_enter(pDb->mutex);
  pSlot = &pDb->aSlot[pgno % HMA_PAGELOCK_SLOTS];
  iWriter = slotGetWriter(*pSlot);
  if( iWriter==p->iTransId || (bWrite==0 && (*pSlot & (1<<p->iTransId))) ){
    bSkip = 1;
  }else if( iWriter>=0 ){
    rc = SQLITE_BUSY_DEADLOCK;
  }else if( bWrite ){
    if( (slotReaderMask(*pSlot) & ~(1 << p->iTransId))==0 ){
      *pSlot += ((p->iTransId + 1) << HMA_MAX_TRANSACTIONID);
    }else{
      rc = SQLITE_BUSY_DEADLOCK;
    }
  }else{
    *pSlot |= (1 << p->iTransId);
  }

  assert( slotGetWriter(*pSlot)<0 
       || slotReaderMask(*pSlot)==0 
       || slotReaderMask(*pSlot)==(1 << slotGetWriter(*pSlot))
  );

  sqlite3_mutex_leave(pDb->mutex);

  if( bSkip==0 ){
    p->aLock[p->nLock++] = pgno;
  }
  return rc;
}

int sqlite3ServerHasLock(Server *p, Pgno pgno, int bWrite){
  assert( 0 );
  return 0;
}

#endif /* ifdef SQLITE_SERVER_EDITION */
