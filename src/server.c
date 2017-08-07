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

#ifdef SQLITE_SERVER_EDITION

/*
** Page-locking slot format:
**
**   Assuming HMA_MAX_TRANSACTIONID is set to 16.
**
**   The least-significant 16 bits are used for read locks. When a read
**   lock is taken, the client sets the bit associated with its 
**   transaction-id.
**
**   The next 5 bits are set to 0 if no client currently holds a write
**   lock. Or to (transaction-id + 1) if a write lock is held.
**
**   The next 8 bits are set to the number of transient-read locks 
**   currently held on the page.
*/
#define HMA_SLOT_RL_BITS 16       /* bits for Read Locks */
#define HMA_SLOT_WL_BITS 5        /* bits for Write Locks */
#define HMA_SLOT_TR_BITS 8        /* bits for Transient Reader locks */

#define HMA_SLOT_RLWL_BITS (HMA_SLOT_RL_BITS + HMA_SLOT_WL_BITS)


#define HMA_SLOT_RL_MASK ((1 << HMA_SLOT_RL_BITS)-1)
#define HMA_SLOT_WL_MASK (((1 << HMA_SLOT_WL_BITS)-1) << HMA_SLOT_RL_BITS)
#define HMA_SLOT_TR_MASK (((1 << HMA_SLOT_TR_BITS)-1) << HMA_SLOT_RLWL_BITS)


/* Number of page-locking slots */
#define HMA_PAGELOCK_SLOTS (256*1024)

/* Maximum concurrent read/write transactions */
#define HMA_MAX_TRANSACTIONID 16


#define HMA_HASH_SIZE 512

/*
** The argument to this macro is the value of a locking slot. It returns
** -1 if no client currently holds the write lock, or the transaction-id
** of the locker otherwise.
*/
#define slotGetWriter(v) ((((int)(v)&HMA_SLOT_WL_MASK) >> HMA_SLOT_RL_BITS) - 1)

/*
** The argument to this macro is the value of a locking slot. This macro
** returns the current number of slow reader clients reading the page.
*/
#define slotGetSlowReaders(v) (((v) & HMA_SLOT_TR_MASK) >> HMA_SLOT_RLWL_BITS)

#define slotReaderMask(v) ((v) & HMA_SLOT_RL_MASK)

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

  int iNextCommit;                /* Commit id for next pre-commit call */ 
  Server *pCommit;                /* List of connections currently commiting */
  Server *pReader;                /* Connections in slower-reader transaction */
  ServerPage *pPgFirst;           /* First (oldest) in list of pages */
  ServerPage *pPgLast;            /* Last (newest) in list of pages */
  ServerPage *apPg[HMA_HASH_SIZE];

  ServerPage *pFree;              /* List of free page buffers */
};

/*
** Once instance for each client connection open on a server mode database
** in this process.
*/
struct Server {
  ServerDb *pDb;                  /* Database object */
  Pager *pPager;                  /* Associated pager object */
  int eTrans;                     /* One of the SERVER_TRANS_xxx values */ 
  int iTransId;                   /* Current transaction id (or -1) */
  int iCommitId;                  /* Current commit id (or 0) */
  int nAlloc;                     /* Allocated size of aLock[] array */
  int nLock;                      /* Number of entries in aLock[] */
  u32 *aLock;                     /* Mapped lock file */
  Server *pNext;                  /* Next in pCommit or pReader list */
};

/*
** Possible values for Server.eTrans.
*/
#define SERVER_TRANS_NONE      0
#define SERVER_TRANS_READONLY  1
#define SERVER_TRANS_READWRITE 2

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
        p->iNextCommit = 1;
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
      pJ->zJournal = sqlite3_mprintf("%s-journal/%d-journal", zFilename, i);
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
    ServerPage *pFree;
    ServerDb **pp;
    serverShutdownDatabase(pDb);
    for(pp=&g_server.pDb; *pp!=pDb; pp=&((*pp)->pNext));
    *pp = pDb->pNext;
    sqlite3_mutex_free(pDb->mutex);
    while( (pFree = pDb->pFree) ){
      pDb->pFree = pFree->pNext;
      sqlite3_free(pFree);
    }
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
int sqlite3ServerBegin(Server *p, int bReadonly){
  int rc = SQLITE_OK;

  if( p->eTrans==SERVER_TRANS_NONE ){
    int id;
    ServerDb *pDb = p->pDb;
    u32 t;

    assert( p->iTransId<0 );
    assert( p->pNext==0 );
    sqlite3_mutex_enter(pDb->mutex);

    if( bReadonly ){
      Server *pIter;
      p->iCommitId = pDb->iNextCommit;
      for(pIter=pDb->pCommit; pIter; pIter=pIter->pNext){
        if( pIter->iCommitId<p->iCommitId ){
          p->iCommitId = pIter->iCommitId;
        }
      }
      p->pNext = pDb->pReader;
      pDb->pReader = p;
      p->eTrans = SERVER_TRANS_READONLY;
    }else{
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
      p->eTrans = SERVER_TRANS_READWRITE;
    }

    sqlite3_mutex_leave(pDb->mutex);

    if( rc==SQLITE_OK && bReadonly==0 ){
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
  if( p->eTrans!=SERVER_TRANS_NONE ){
    Server **pp;
    ServerDb *pDb = p->pDb;
    ServerPage *pPg = 0;

    sqlite3_mutex_enter(pDb->mutex);

    if( p->eTrans==SERVER_TRANS_READONLY ){
      /* Remove the connection from the readers list */
      for(pp=&pDb->pReader; *pp!=p; pp = &((*pp)->pNext));
      *pp = p->pNext;
    }else{
      serverReleaseLocks(p);

      /* Clear the bit in the transaction mask. */
      pDb->transmask &= ~((u32)1 << p->iTransId);

      /* If this connection is in the committers list, remove it. */
      for(pp=&pDb->pCommit; *pp; pp = &((*pp)->pNext)){
        if( *pp==p ){
          *pp = p->pNext;
          break;
        }
      }
    }

    /* See if it is possible to free any ServerPage records. If so, remove
    ** them from the linked list and hash table, but do not call sqlite3_free()
    ** on them until the mutex has been released.  */
    if( pDb->pPgFirst ){
      ServerPage *pLast = 0;
      Server *pIter;
      int iOldest = 0x7FFFFFFF;
      for(pIter=pDb->pReader; pIter; pIter=pIter->pNext){
        iOldest = MIN(iOldest, pIter->iCommitId);
      }
      for(pIter=pDb->pCommit; pIter; pIter=pIter->pNext){
        iOldest = MIN(iOldest, pIter->iCommitId);
      }

      for(pPg=pDb->pPgFirst; pPg && pPg->iCommitId<iOldest; pPg=pPg->pNext){
        if( pPg->pHashPrev ){
          pPg->pHashPrev->pHashNext = pPg->pHashNext;
        }else{
          int iHash = pPg->pgno % HMA_HASH_SIZE;
          assert( pDb->apPg[iHash]==pPg );
          pDb->apPg[iHash] = pPg->pHashNext;
        }
        if( pPg->pHashNext ){
          pPg->pHashNext->pHashPrev = pPg->pHashPrev;
        }
        pLast = pPg;
      }

      if( pLast ){
        assert( pLast->pNext==pPg );
        pLast->pNext = pDb->pFree;
        pDb->pFree = pDb->pPgFirst;
      }

      if( pPg==0 ){
        pDb->pPgFirst = pDb->pPgLast = 0;
      }else{
        pDb->pPgFirst = pPg;
      }
    }

    sqlite3_mutex_leave(pDb->mutex);

    p->pNext = 0;
    p->eTrans = SERVER_TRANS_NONE;
    p->iTransId = -1;
    p->iCommitId = 0;
  }
  return rc;
}

int sqlite3ServerPreCommit(Server *p, ServerPage *pPg){
  ServerDb *pDb = p->pDb;
  int rc = SQLITE_OK;
  ServerPage *pIter;

  if( pPg==0 ) return SQLITE_OK;

  sqlite3_mutex_enter(pDb->mutex);

  /* Assign a commit id to this transaction */
  assert( p->iCommitId==0 );
  assert( p->eTrans==SERVER_TRANS_READWRITE );
  assert( p->iTransId>=0 );

  p->iCommitId = pDb->iNextCommit++;

  /* Iterate through all pages. For each:
  **
  **   1. Set the iCommitId field.
  **   2. Add the page to the hash table.
  **   3. Wait until all slow-reader locks have cleared.
  */
  for(pIter=pPg; pIter; pIter=pIter->pNext){
    u32 *pSlot = &pDb->aSlot[pIter->pgno % HMA_PAGELOCK_SLOTS];
    int iHash = pIter->pgno % HMA_HASH_SIZE;
    pIter->iCommitId = p->iCommitId;
    pIter->pHashNext = pDb->apPg[iHash];
    if( pIter->pHashNext ){
      pIter->pHashNext->pHashPrev = pIter;
    }
    pDb->apPg[iHash] = pIter;

    /* TODO: Something better than this! */
    while( slotGetSlowReaders(*pSlot)>0 ){
      sqlite3_mutex_leave(pDb->mutex);
      sqlite3_mutex_enter(pDb->mutex);
    }

    /* If pIter is the last element in the list, append the new list to
    ** the ServerDb.pPgFirst/pPgLast list at this point.  */
    if( pIter->pNext==0 ){
      if( pDb->pPgLast ){
        assert( pDb->pPgFirst );
        pDb->pPgLast->pNext = pPg;
      }else{
        assert( pDb->pPgFirst==0 );
        pDb->pPgFirst = pPg;
      }
      pDb->pPgLast = pIter;
    }
  }

  /* Add this connection to the list of current committers */
  assert( p->pNext==0 );
  p->pNext = pDb->pCommit;
  pDb->pCommit = p;

  sqlite3_mutex_leave(pDb->mutex);
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

  assert( p->eTrans==SERVER_TRANS_READWRITE 
       || p->eTrans==SERVER_TRANS_READONLY 
  );
  if( p->eTrans==SERVER_TRANS_READWRITE ){
    ServerDb *pDb = p->pDb;
    int iWriter;
    int bSkip = 0;
    u32 *pSlot;

    assert( p->iTransId>=0 );
    assert( p->nLock<=p->nAlloc );
    if( p->nLock==p->nAlloc ){
      int nNew = p->nLock ? p->nLock*2 : 256;
      u32 *aNew = sqlite3_realloc(p->aLock, nNew*sizeof(u32));
      if( aNew==0 ) return SQLITE_NOMEM_BKPT;
      memset(&aNew[p->nLock], 0, sizeof(u32) * (nNew - p->nLock));
      p->nAlloc = nNew;
      p->aLock = aNew;
    }

    sqlite3_mutex_enter(pDb->mutex);

    pSlot = &pDb->aSlot[pgno % HMA_PAGELOCK_SLOTS];
    assert( slotGetWriter(*pSlot)<0 
        || slotReaderMask(*pSlot)==0 
        || slotReaderMask(*pSlot)==(1 << slotGetWriter(*pSlot))
        );

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
  }

  return rc;
}

int sqlite3ServerHasLock(Server *p, Pgno pgno, int bWrite){
  assert( 0 );
  return 0;
}

static void serverIncrSlowReader(u32 *pSlot, int n){
  assert( n==1 || n==-1 );
  *pSlot += (n * (1 << HMA_SLOT_RLWL_BITS));
}

void sqlite3ServerReadPage(Server *p, Pgno pgno, u8 **ppData){
  if( p->eTrans==SERVER_TRANS_READONLY ){
    ServerDb *pDb = p->pDb;
    ServerPage *pIter;
    ServerPage *pBest = 0;
    int iHash = pgno % HMA_HASH_SIZE;

    sqlite3_mutex_enter(pDb->mutex);

    /* Search the hash table for the oldest version of page pgno with
    ** a commit-id greater than or equal to Server.iCommitId.  */
    for(pIter=pDb->apPg[iHash]; pIter; pIter=pIter->pHashNext){
      if( pIter->pgno==pgno 
       && pIter->iCommitId>=p->iCommitId 
       && (pBest==0 || pIter->iCommitId<pBest->iCommitId) 
      ){
        pBest = pIter;
      }
    }

    if( pBest ){
      *ppData = pBest->aData;
    }else{
      u32 *pSlot = &pDb->aSlot[pgno % HMA_PAGELOCK_SLOTS];
      serverIncrSlowReader(pSlot, 1);
    }

    sqlite3_mutex_leave(pDb->mutex);
  }
}

void sqlite3ServerEndReadPage(Server *p, Pgno pgno){
  if( p->eTrans==SERVER_TRANS_READONLY ){
    ServerDb *pDb = p->pDb;
    u32 *pSlot = &pDb->aSlot[pgno % HMA_PAGELOCK_SLOTS];
    sqlite3_mutex_enter(pDb->mutex);
    serverIncrSlowReader(pSlot, -1);
    assert( slotGetSlowReaders(*pSlot)>=0 );
    sqlite3_mutex_leave(pDb->mutex);
  }
}

ServerPage *sqlite3ServerBuffer(Server *p){
  ServerDb *pDb = p->pDb;
  ServerPage *pRet = 0;
  sqlite3_mutex_enter(pDb->mutex);
  if( pDb->pFree ){
    pRet = pDb->pFree;
    pDb->pFree = pRet->pNext;
    pRet->pNext = 0;
  }
  sqlite3_mutex_leave(pDb->mutex);
  return pRet;
}

/*
** Return true if the handle passed as the only argument is not NULL and
** currently has an open readonly transaction (one started with BEGIN
** READONLY). Return false if the argument is NULL, if there is no open
** transaction, or if the open transaction is read/write.
*/
int sqlite3ServerIsReadonly(Server *p){
  return (p && p->eTrans==SERVER_TRANS_READONLY);
}

#endif /* ifdef SQLITE_SERVER_EDITION */
