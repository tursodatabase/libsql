/*
** 2011-12-03
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
** Unix-specific run-time environment implementation for LSM.
*/

#ifdef WIN32

#if defined(__GNUC__) || defined(__TINYC__)
/* workaround for ftruncate() visibility on gcc. */
# ifndef _XOPEN_SOURCE
#  define _XOPEN_SOURCE 500
# endif
#endif

#include <unistd.h>
#include <sys/types.h>

#include <sys/stat.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ctype.h>

#include <unistd.h>
#include <errno.h>

#include "lsmInt.h"

/*
** An open file is an instance of the following object
*/
typedef struct Win32File Win32File;
struct Win32File {
  lsm_env *pEnv;                  /* The run-time environment */
  const char *zName;              /* Full path to file */

  HANDLE h;                       /* Open file descriptor */
  HANDLE shmh;                    /* File descriptor for *-shm file */

  void *pMap;                     /* Pointer to mapping of file fd */
  off_t nMap;                     /* Size of mapping at pMap in bytes */
  int nShm;                       /* Number of entries in array apShm[] */
  void **apShm;                   /* Array of 32K shared memory segments */
};

static char *win32ShmFile(Win32File *p){
  char *zShm;
  int nName = strlen(p->zName);
  zShm = (char *)lsmMalloc(p->pEnv, nName+4+1);
  if( zShm ){
    memcpy(zShm, p->zName, nName);
    memcpy(&zShm[nName], "-shm", 5);
  }
  return zShm;
}

static int lsmWin32OsOpen(
  lsm_env *pEnv,
  const char *zFile,
  int flags,
  lsm_file **ppFile
){
  int rc = LSM_OK;
  Win32File *p;

  p = lsm_malloc(pEnv, sizeof(Win32File));
  if( p==0 ){
    rc = LSM_NOMEM;
  }else{
    int bReadonly = (flags & LSM_OPEN_READONLY);
    int oflags = (bReadonly ? O_RDONLY : (O_RDWR|O_CREAT));
    memset(p, 0, sizeof(Win32File));
    p->zName = zFile;
    p->pEnv = pEnv;

    CreateFile((LPCWSTR)zConverted,
                              dwDesiredAccess,
                              dwShareMode, NULL,
                              dwCreationDisposition,
                              dwFlagsAndAttributes,
                              NULL))==INVALID_HANDLE_VALUE &&
                              winRetryIoerr(&cnt, &lastErrno) ){


    p->fd = open(zFile, oflags, 0644);
    if( p->fd<0 ){
      lsm_free(pEnv, p);
      p = 0;
      if( errno==ENOENT ){
        rc = lsmErrorBkpt(LSM_IOERR_NOENT);
      }else{
        rc = LSM_IOERR_BKPT;
      }
    }
  }

  *ppFile = (lsm_file *)p;
  return rc;
}

static int lsmWin32OsWrite(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 iOff,                   /* Offset to write to */
  void *pData,                    /* Write data from this buffer */
  int nData                       /* Bytes of data to write */
){
  int rc = LSM_OK;
  Win32File *p = (Win32File *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = LSM_IOERR_BKPT;
  }else{
    ssize_t prc = write(p->fd, pData, (size_t)nData);
    if( prc<0 ) rc = LSM_IOERR_BKPT;
  }

  return rc;
}

static int lsmWin32OsTruncate(
  lsm_file *pFile,                /* File to write to */
  lsm_i64 nSize                   /* Size to truncate file to */
){
  Win32File *p = (Win32File *)pFile;
  int rc = LSM_OK;                /* Return code */
  int prc;                        /* Posix Return Code */
  struct stat sStat;              /* Result of fstat() invocation */
  
  prc = fstat(p->fd, &sStat);
  if( prc==0 && sStat.st_size>nSize ){
    prc = ftruncate(p->fd, (off_t)nSize);
  }
  if( prc<0 ) rc = LSM_IOERR_BKPT;

  return rc;
}

static int lsmWin32OsRead(
  lsm_file *pFile,                /* File to read from */
  lsm_i64 iOff,                   /* Offset to read from */
  void *pData,                    /* Read data into this buffer */
  int nData                       /* Bytes of data to read */
){
  int rc = LSM_OK;
  Win32File *p = (Win32File *)pFile;
  off_t offset;

  offset = lseek(p->fd, (off_t)iOff, SEEK_SET);
  if( offset!=iOff ){
    rc = LSM_IOERR_BKPT;
  }else{
    ssize_t prc = read(p->fd, pData, (size_t)nData);
    if( prc<0 ){ 
      rc = LSM_IOERR_BKPT;
    }else if( prc<nData ){
      memset(&((u8 *)pData)[prc], 0, nData - prc);
    }

  }

  return rc;
}

static int lsmWin32OsSync(lsm_file *pFile){
  int rc = LSM_OK;

#ifndef LSM_NO_SYNC
  Win32File *p = (Win32File *)pFile;
  int prc = 0;

  if( p->pMap ){
    prc = msync(p->pMap, p->nMap, MS_SYNC);
  }
  if( prc==0 ) prc = fdatasync(p->fd);
  if( prc<0 ) rc = LSM_IOERR_BKPT;
#else
  (void)pFile;
#endif

  return rc;
}

static int lsmWin32OsSectorSize(lsm_file *pFile){
  return 512;
}

static int lsmWin32OsRemap(
  lsm_file *pFile, 
  lsm_i64 iMin, 
  void **ppOut,
  lsm_i64 *pnOut
){
  off_t iSz;
  int prc;
  Win32File *p = (Win32File *)pFile;
  struct stat buf;

  /* If the file is between 0 and 2MB in size, extend it in chunks of 256K.
  ** Thereafter, in chunks of 1MB at a time.  */
  const int aIncrSz[] = {256*1024, 1024*1024};
  int nIncrSz = aIncrSz[iMin>(2*1024*1024)];

  if( p->pMap ){
    munmap(p->pMap, p->nMap);
    *ppOut = p->pMap = 0;
    *pnOut = p->nMap = 0;
  }

  if( iMin>=0 ){
    memset(&buf, 0, sizeof(buf));
    prc = fstat(p->fd, &buf);
    if( prc!=0 ) return LSM_IOERR_BKPT;
    iSz = buf.st_size;
    if( iSz<iMin ){
      iSz = ((iMin + nIncrSz-1) / nIncrSz) * nIncrSz;
      prc = ftruncate(p->fd, iSz);
      if( prc!=0 ) return LSM_IOERR_BKPT;
    }

    p->pMap = mmap(0, iSz, PROT_READ|PROT_WRITE, MAP_SHARED, p->fd, 0);
    p->nMap = iSz;
  }

  *ppOut = p->pMap;
  *pnOut = p->nMap;
  return LSM_OK;
}

static int lsmWin32OsFullpath(
  lsm_env *pEnv,
  const char *zName,
  char *zOut,
  int *pnOut
){
  int nBuf = *pnOut;
  int nReq;

  if( zName[0]!='/' ){
    char *z;
    char *zTmp;
    int nTmp = 512;
    zTmp = lsmMalloc(pEnv, nTmp);
    while( zTmp ){
      z = getcwd(zTmp, nTmp);
      if( z || errno!=ERANGE ) break;
      nTmp = nTmp*2;
      zTmp = lsmReallocOrFree(pEnv, zTmp, nTmp);
    }
    if( zTmp==0 ) return LSM_NOMEM_BKPT;
    if( z==0 ) return LSM_IOERR_BKPT;
    assert( z==zTmp );

    nTmp = strlen(zTmp);
    nReq = nTmp + 1 + strlen(zName) + 1;
    if( nReq<=nBuf ){
      memcpy(zOut, zTmp, nTmp);
      zOut[nTmp] = '/';
      memcpy(&zOut[nTmp+1], zName, strlen(zName)+1);
    }
    lsmFree(pEnv, zTmp);
  }else{
    nReq = strlen(zName)+1;
    if( nReq<=nBuf ){
      memcpy(zOut, zName, strlen(zName)+1);
    }
  }

  *pnOut = nReq;
  return LSM_OK;
}

static int lsmWin32OsFileid(
  lsm_file *pFile, 
  void *pBuf,
  int *pnBuf
){
  int prc;
  int nBuf;
  int nReq;
  Win32File *p = (Win32File *)pFile;
  struct stat buf;

  nBuf = *pnBuf;
  nReq = (sizeof(buf.st_dev) + sizeof(buf.st_ino));
  *pnBuf = nReq;
  if( nReq>nBuf ) return LSM_OK;

  memset(&buf, 0, sizeof(buf));
  prc = fstat(p->fd, &buf);
  if( prc!=0 ) return LSM_IOERR_BKPT;

  memcpy(pBuf, &buf.st_dev, sizeof(buf.st_dev));
  memcpy(&(((u8 *)pBuf)[sizeof(buf.st_dev)]), &buf.st_ino, sizeof(buf.st_ino));
  return LSM_OK;
}

static int lsmWin32OsUnlink(lsm_env *pEnv, const char *zFile){
  int prc = unlink(zFile);
  return prc ? LSM_IOERR_BKPT : LSM_OK;
}

int lsmWin32OsLock(lsm_file *pFile, int iLock, int eType){
  int rc = LSM_OK;
  Win32File *p = (Win32File *)pFile;
  static const short aType[3] = { F_UNLCK, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( aType[LSM_LOCK_UNLOCK]==F_UNLCK );
  assert( aType[LSM_LOCK_SHARED]==F_RDLCK );
  assert( aType[LSM_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<array_size(aType) );
  assert( iLock>0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = 1;
  lock.l_type = aType[eType];
  lock.l_start = (4096-iLock);

  if( fcntl(p->fd, F_SETLK, &lock) ){
    int e = errno;
    if( e==EACCES || e==EAGAIN ){
      rc = LSM_BUSY;
    }else{
      rc = LSM_IOERR_BKPT;
    }
  }

  return rc;
}

int lsmWin32OsTestLock(lsm_file *pFile, int iLock, int nLock, int eType){
  int rc = LSM_OK;
  Win32File *p = (Win32File *)pFile;
  static const short aType[3] = { 0, F_RDLCK, F_WRLCK };
  struct flock lock;

  assert( eType==LSM_LOCK_SHARED || eType==LSM_LOCK_EXCL );
  assert( aType[LSM_LOCK_SHARED]==F_RDLCK );
  assert( aType[LSM_LOCK_EXCL]==F_WRLCK );
  assert( eType>=0 && eType<array_size(aType) );
  assert( iLock>0 && iLock<=32 );

  memset(&lock, 0, sizeof(lock));
  lock.l_whence = SEEK_SET;
  lock.l_len = nLock;
  lock.l_type = aType[eType];
  lock.l_start = (4096-iLock);

  if( fcntl(p->fd, F_GETLK, &lock) ){
    rc = LSM_IOERR_BKPT;
  }else if( lock.l_type!=F_UNLCK ){
    rc = LSM_BUSY;
  }

  return rc;
}

int lsmWin32OsShmMap(lsm_file *pFile, int iChunk, int sz, void **ppShm){
  Win32File *p = (Win32File *)pFile;

  *ppShm = 0;
  assert( sz==LSM_SHM_CHUNK_SIZE );
  if( iChunk>=p->nShm ){
    int i;
    void **apNew;
    int nNew = iChunk+1;
    off_t nReq = nNew * LSM_SHM_CHUNK_SIZE;
    struct stat sStat;

    /* If the shared-memory file has not been opened, open it now. */
    if( p->shmfd<=0 ){
      char *zShm = win32ShmFile(p);
      if( !zShm ) return LSM_NOMEM_BKPT;
      p->shmfd = open(zShm, O_RDWR|O_CREAT, 0644);
      lsmFree(p->pEnv, zShm);
      if( p->shmfd<0 ){ 
        return LSM_IOERR_BKPT;
      }
    }

    /* If the shared-memory file is not large enough to contain the 
    ** requested chunk, cause it to grow.  */
    if( fstat(p->shmfd, &sStat) ){
      return LSM_IOERR_BKPT;
    }
    if( sStat.st_size<nReq ){
      if( ftruncate(p->shmfd, nReq) ){
        return LSM_IOERR_BKPT;
      }
    }

    apNew = (void **)lsmRealloc(p->pEnv, p->apShm, sizeof(void *) * nNew);
    if( !apNew ) return LSM_NOMEM_BKPT;
    for(i=p->nShm; i<nNew; i++){
      apNew[i] = 0;
    }
    p->apShm = apNew;
    p->nShm = nNew;
  }

  if( p->apShm[iChunk]==0 ){
    p->apShm[iChunk] = mmap(0, LSM_SHM_CHUNK_SIZE, 
        PROT_READ|PROT_WRITE, MAP_SHARED, p->shmfd, iChunk*LSM_SHM_CHUNK_SIZE
    );
    if( p->apShm[iChunk]==0 ) return LSM_IOERR_BKPT;
  }

  *ppShm = p->apShm[iChunk];
  return LSM_OK;
}

void lsmWin32OsShmBarrier(void){
}

int lsmWin32OsShmUnmap(lsm_file *pFile, int bDelete){
  Win32File *p = (Win32File *)pFile;
  if( p->shmfd>0 ){
    int i;
    for(i=0; i<p->nShm; i++){
      if( p->apShm[i] ){
        munmap(p->apShm[i], LSM_SHM_CHUNK_SIZE);
        p->apShm[i] = 0;
      }
    }
    close(p->shmfd);
    p->shmfd = 0;
    if( bDelete ){
      char *zShm = win32ShmFile(p);
      if( zShm ) unlink(zShm);
      lsmFree(p->pEnv, zShm);
    }
  }
  return LSM_OK;
}


static int lsmWin32OsClose(lsm_file *pFile){
  Win32File *p = (Win32File *)pFile;
  lsmWin32OsShmUnmap(pFile, 0);
  if( p->pMap ) munmap(p->pMap, p->nMap);
  close(p->fd);
  lsm_free(p->pEnv, p->apShm);
  lsm_free(p->pEnv, p);
  return LSM_OK;
}

static int lsmWin32OsSleep(lsm_env *pEnv, int us){
  usleep(us);
  return LSM_OK;
}

/****************************************************************************
** Memory allocation routines.
*/
#define BLOCK_HDR_SIZE ROUND8( sizeof(size_t) )

static void *lsmWin32OsMalloc(lsm_env *pEnv, size_t N){
  unsigned char * m;
  N += BLOCK_HDR_SIZE;
  m = (unsigned char *)malloc(N);
  *((size_t*)m) = N;
  return m + BLOCK_HDR_SIZE;
}

static void lsmWin32OsFree(lsm_env *pEnv, void *p){
  if(p){
    free( ((unsigned char *)p) - BLOCK_HDR_SIZE );
  }
}

static void *lsmWin32OsRealloc(lsm_env *pEnv, void *p, size_t N){
  unsigned char * m = (unsigned char *)p;
  if(1>N){
    lsmWin32OsFree( pEnv, p );
    return NULL;
  }else if(NULL==p){
    return lsmWin32OsMalloc(pEnv, N);
  }else{
    void * re = NULL;
    m -= BLOCK_HDR_SIZE;
#if 0 /* arguable: don't shrink */
    size_t * sz = (size_t*)m;
    if(*sz >= (size_t)N){
      return p;
    }
#endif
    re = realloc( m, N + BLOCK_HDR_SIZE );
    if(re){
      m = (unsigned char *)re;
      *((size_t*)m) = N;
      return m + BLOCK_HDR_SIZE;
    }else{
      return NULL;
    }
  }
}

static size_t lsmWin32OsMSize(lsm_env *pEnv, void *p){
  unsigned char * m = (unsigned char *)p;
  return *((size_t*)(m-BLOCK_HDR_SIZE));
}
#undef BLOCK_HDR_SIZE


#ifdef LSM_MUTEX_WIN32 
/*************************************************************************
** Mutex methods for pthreads based systems.  If LSM_MUTEX_WIN32 is
** missing then a no-op implementation of mutexes found below will be 
** used instead.
*/
#include <pthread.h>

typedef struct PthreadMutex PthreadMutex;
struct PthreadMutex {
  lsm_env *pEnv;
  pthread_mutex_t mutex;
#ifdef LSM_DEBUG
  pthread_t owner;
#endif
};

#ifdef LSM_DEBUG
# define LSM_PTHREAD_STATIC_MUTEX { 0, PTHREAD_MUTEX_INITIALIZER, 0 }
#else
# define LSM_PTHREAD_STATIC_MUTEX { 0, PTHREAD_MUTEX_INITIALIZER }
#endif

static int lsmWin32OsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  static PthreadMutex sMutex[2] = {
    LSM_PTHREAD_STATIC_MUTEX,
    LSM_PTHREAD_STATIC_MUTEX
  };

  assert( iMutex==LSM_MUTEX_GLOBAL || iMutex==LSM_MUTEX_HEAP );
  assert( LSM_MUTEX_GLOBAL==1 && LSM_MUTEX_HEAP==2 );

  *ppStatic = (lsm_mutex *)&sMutex[iMutex-1];
  return LSM_OK;
}

static int lsmWin32OsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  PthreadMutex *pMutex;           /* Pointer to new mutex */
  pthread_mutexattr_t attr;       /* Attributes object */

  pMutex = (PthreadMutex *)lsmMallocZero(pEnv, sizeof(PthreadMutex));
  if( !pMutex ) return LSM_NOMEM_BKPT;

  pMutex->pEnv = pEnv;
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&pMutex->mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  *ppNew = (lsm_mutex *)pMutex;
  return LSM_OK;
}

static void lsmWin32OsMutexDel(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_destroy(&pMutex->mutex);
  lsmFree(pMutex->pEnv, pMutex);
}

static void lsmWin32OsMutexEnter(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  pthread_mutex_lock(&pMutex->mutex);

#ifdef LSM_DEBUG
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = pthread_self();
  assert( pthread_equal(pMutex->owner, pthread_self()) );
#endif
}

static int lsmWin32OsMutexTry(lsm_mutex *p){
  int ret;
  PthreadMutex *pMutex = (PthreadMutex *)p;
  ret = pthread_mutex_trylock(&pMutex->mutex);
#ifdef LSM_DEBUG
  if( ret==0 ){
    assert( !pthread_equal(pMutex->owner, pthread_self()) );
    pMutex->owner = pthread_self();
    assert( pthread_equal(pMutex->owner, pthread_self()) );
  }
#endif
  return ret;
}

static void lsmWin32OsMutexLeave(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
#ifdef LSM_DEBUG
  assert( pthread_equal(pMutex->owner, pthread_self()) );
  pMutex->owner = 0;
  assert( !pthread_equal(pMutex->owner, pthread_self()) );
#endif
  pthread_mutex_unlock(&pMutex->mutex);
}

#ifdef LSM_DEBUG
static int lsmWin32OsMutexHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? pthread_equal(pMutex->owner, pthread_self()) : 1;
}
static int lsmWin32OsMutexNotHeld(lsm_mutex *p){
  PthreadMutex *pMutex = (PthreadMutex *)p;
  return pMutex ? !pthread_equal(pMutex->owner, pthread_self()) : 1;
}
#endif
/*
** End of pthreads mutex implementation.
*************************************************************************/
#else
/*************************************************************************
** Noop mutex implementation
*/
typedef struct NoopMutex NoopMutex;
struct NoopMutex {
  lsm_env *pEnv;                  /* Environment handle (for xFree()) */
  int bHeld;                      /* True if mutex is held */
  int bStatic;                    /* True for a static mutex */
};
static NoopMutex aStaticNoopMutex[2] = {
  {0, 0, 1},
  {0, 0, 1},
};

static int lsmWin32OsMutexStatic(
  lsm_env *pEnv,
  int iMutex,
  lsm_mutex **ppStatic
){
  assert( iMutex>=1 && iMutex<=(int)array_size(aStaticNoopMutex) );
  *ppStatic = (lsm_mutex *)&aStaticNoopMutex[iMutex-1];
  return LSM_OK;
}
static int lsmWin32OsMutexNew(lsm_env *pEnv, lsm_mutex **ppNew){
  NoopMutex *p;
  p = (NoopMutex *)lsmMallocZero(pEnv, sizeof(NoopMutex));
  if( p ) p->pEnv = pEnv;
  *ppNew = (lsm_mutex *)p;
  return (p ? LSM_OK : LSM_NOMEM_BKPT);
}
static void lsmWin32OsMutexDel(lsm_mutex *pMutex)  { 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bStatic==0 && p->pEnv );
  lsmFree(p->pEnv, p);
}
static void lsmWin32OsMutexEnter(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
}
static int lsmWin32OsMutexTry(lsm_mutex *pMutex){
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==0 );
  p->bHeld = 1;
  return 0;
}
static void lsmWin32OsMutexLeave(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  assert( p->bHeld==1 );
  p->bHeld = 0;
}
#ifdef LSM_DEBUG
static int lsmWin32OsMutexHeld(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? p->bHeld : 1;
}
static int lsmWin32OsMutexNotHeld(lsm_mutex *pMutex){ 
  NoopMutex *p = (NoopMutex *)pMutex;
  return p ? !p->bHeld : 1;
}
#endif
/***************************************************************************/
#endif /* else LSM_MUTEX_NONE */

/* Without LSM_DEBUG, the MutexHeld tests are never called */
#ifndef LSM_DEBUG
# define lsmWin32OsMutexHeld    0
# define lsmWin32OsMutexNotHeld 0
#endif

lsm_env *lsm_default_env(void){
  static lsm_env win32_env = {
    sizeof(lsm_env),         /* nByte */
    1,                       /* iVersion */
    /***** file i/o ******************/
    0,                       /* pVfsCtx */
    lsmWin32OsFullpath,      /* xFullpath */
    lsmWin32OsOpen,          /* xOpen */
    lsmWin32OsRead,          /* xRead */
    lsmWin32OsWrite,         /* xWrite */
    lsmWin32OsTruncate,      /* xTruncate */
    lsmWin32OsSync,          /* xSync */
    lsmWin32OsSectorSize,    /* xSectorSize */
    lsmWin32OsRemap,         /* xRemap */
    lsmWin32OsFileid,        /* xFileid */
    lsmWin32OsClose,         /* xClose */
    lsmWin32OsUnlink,        /* xUnlink */
    lsmWin32OsLock,          /* xLock */
    lsmWin32OsTestLock,      /* xTestLock */
    lsmWin32OsShmMap,        /* xShmMap */
    lsmWin32OsShmBarrier,    /* xShmBarrier */
    lsmWin32OsShmUnmap,      /* xShmUnmap */
    /***** memory allocation *********/
    0,                       /* pMemCtx */
    lsmWin32OsMalloc,        /* xMalloc */
    lsmWin32OsRealloc,       /* xRealloc */
    lsmWin32OsFree,          /* xFree */
    lsmWin32OsMSize,         /* xSize */
    /***** mutexes *********************/
    0,                       /* pMutexCtx */
    lsmWin32OsMutexStatic,   /* xMutexStatic */
    lsmWin32OsMutexNew,      /* xMutexNew */
    lsmWin32OsMutexDel,      /* xMutexDel */
    lsmWin32OsMutexEnter,    /* xMutexEnter */
    lsmWin32OsMutexTry,      /* xMutexTry */
    lsmWin32OsMutexLeave,    /* xMutexLeave */
    lsmWin32OsMutexHeld,     /* xMutexHeld */
    lsmWin32OsMutexNotHeld,  /* xMutexNotHeld */
    /***** other *********************/
    lsmWin32OsSleep,         /* xSleep */
  };
  return &win32_env;
}

#endif
