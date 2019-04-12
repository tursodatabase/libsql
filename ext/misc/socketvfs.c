/*
** 2019 April 12
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
** A read-only VFS that reads data from a server instead of a file
** using a custom protocol over a tcp/ip socket. The VFS is named 
** "socket". The filename passed to sqlite3_open() is of the
** form "host:portnumber". For example, to connect to the server
** on port 23456 on the localhost:
**
**   sqlite3_open_v2("localhost:23456", &db, SQLITE_OPEN_READONLY, "socket");
**
** Or, if using URIs:
**
**   sqlite3_open("file:localhost:23456?vfs=socket", &db);
** 
** The protocol is:
**
**   * Client connects to tcp/ip server. Server immediately sends the
**     database file-size in bytes as a 64-bit big-endian integer.
**
**   * To read from the file, client sends the byte offset and amount
**     of data required in bytes, both as 64-bit big-endian integers
**     (i.e. a 16-byte message). Server sends back the requested data.
**
** As well as the usual SQLite loadable extension entry point, this file
** exports one more function:
**
**     sqlite3_vfs *sqlite3_socketvfs(void);
**
** To install the "socket" VFS without loading the extension, link this file
** into the application and invoke:
**
**     int bDefault = 0;          // Do not make "socket" the default VFS
**     sqlite3_vfs_register(sqlite3_socketvfs(), bDefault);
**
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <assert.h>
#include <string.h>

#if defined(_WIN32)
# if defined(_WIN32_WINNT)
#  undef _WIN32_WINNT
# endif
# define _WIN32_WINNT 0x501
#endif
#ifndef __EXTENSIONS__
# define __EXTENSIONS__ 1  /* IPv6 won't compile on Solaris without this */
#endif
#if defined(_WIN32)
#  include <winsock2.h>
#  include <ws2tcpip.h>
#  include <Windows.h>
#  include <time.h>
#else
#  include <netinet/in.h>
#  include <arpa/inet.h>
#  include <sys/socket.h>
#  include <netdb.h>
#  include <time.h>
#endif
#include <assert.h>
#include <sys/types.h>
#include <signal.h>

#if !defined(_WIN32)
# include <unistd.h>
#endif

/*
** When using this VFS, the sqlite3_file* handles that SQLite uses are
** actually pointers to instances of type SocketFile.
*/
typedef struct SocketFile SocketFile;
struct SocketFile {
  sqlite3_file base;              /* Base class. Must be first. */
  int iSocket;                    /* Socket used to talk to server. */
  sqlite3_int64 szFile;           /* Size of file in bytes */
};

static sqlite3_uint64 socketGetU64(const unsigned char *a){
  return (((sqlite3_uint64)(a[0])) << 56)
       + (((sqlite3_uint64)(a[1])) << 48)
       + (((sqlite3_uint64)(a[2])) << 40)
       + (((sqlite3_uint64)(a[3])) << 32)
       + (((sqlite3_uint64)(a[4])) << 24)
       + (((sqlite3_uint64)(a[5])) << 16)
       + (((sqlite3_uint64)(a[6])) <<  8)
       + (((sqlite3_uint64)(a[7])) <<  0);
}

static void socketPutU64(unsigned char *a, sqlite3_int64 i){
  a[0] = ((i >> 56) & 0xFF);
  a[1] = ((i >> 48) & 0xFF);
  a[2] = ((i >> 40) & 0xFF);
  a[3] = ((i >> 32) & 0xFF);
  a[4] = ((i >> 24) & 0xFF);
  a[5] = ((i >> 16) & 0xFF);
  a[6] = ((i >>  8) & 0xFF);
  a[7] = ((i >>  0) & 0xFF);
}

static void socket_close(int iSocket){
  if( iSocket>=0 ){
#if defined(_WIN32)
    if( shutdown(iSocket,1)==0 ) shutdown(iSocket,0);
    closesocket(iSocket);
#else
    close(iSocket);
#endif
  }
}

/*
** Write nData bytes of data from buffer aData to socket iSocket. If
** successful, return SQLITE_OK. Otherwise, SQLITE_IOERR_WRITE.
*/
static int socket_send(int iSocket, const unsigned char *aData, int nData){
  int nWrite = 0;
  do{
    int res = send(iSocket, (const char*)&aData[nWrite], nData-nWrite, 0);
    if( res<=0 ) return SQLITE_IOERR_WRITE;
    nWrite += res;
  }while( nWrite<nData );
  return SQLITE_OK;
}

/*
** Read nData bytes of data from socket iSocket into buffer aData. If
** successful, return SQLITE_OK. Otherwise, SQLITE_IOERR_READ.
*/
static int socket_recv(int iSocket, unsigned char *aData, int nData){
  int nRead = 0;
  do{
    int res = recv(iSocket, (char*)&aData[nRead], nData-nRead, 0);
    if( res<=0 ) return SQLITE_IOERR_READ;
    nRead += res;
  }while( nRead<nData );
  return SQLITE_OK;
}

/*
** Close a SocketFile file.
*/
static int socketClose(sqlite3_file *pFile){
  SocketFile *pSock = (SocketFile*)pFile;
  socket_close(pSock->iSocket);
  pSock->iSocket = -1;
  return SQLITE_OK;
}

/*
** Read data from a SocketFile file.
*/
static int socketRead(
  sqlite3_file *pFile, 
  void *zBuf, 
  int iAmt, 
  sqlite3_int64 iOfst
){
  SocketFile *pSock = (SocketFile*)pFile;
  unsigned char aRequest[16];
  int rc = SQLITE_OK;
  int nRead = iAmt;

  if( iOfst+nRead>pSock->szFile ){
    nRead = (int)(pSock->szFile - iOfst);
    memset(zBuf, 0, iAmt);
    rc = SQLITE_IOERR_SHORT_READ;
  }

  if( nRead>0 ){
    socketPutU64(&aRequest[0], (sqlite3_uint64)iOfst);
    socketPutU64(&aRequest[8], (sqlite3_uint64)nRead);
    rc = socket_send(pSock->iSocket, aRequest, sizeof(aRequest));
    if( rc==SQLITE_OK ){
      rc = socket_recv(pSock->iSocket, zBuf, nRead);
    }
  }

  return rc;
}

/*
** Write to a file. This is a no-op, as this VFS is always opens files
** read-only.
*/
static int socketWrite(
  sqlite3_file *pFile, 
  const void *zBuf, 
  int iAmt, 
  sqlite3_int64 iOfst
){
  return SQLITE_IOERR_WRITE;
}

/*
** Truncate a file. This is a no-op, as this VFS is always opens files
** read-only.
*/
static int socketTruncate(sqlite3_file *pFile, sqlite3_int64 size){
  return SQLITE_IOERR_TRUNCATE;
}

/*
** Synk a file. This is a no-op, as this VFS is always opens files
** read-only.
*/
static int socketSync(sqlite3_file *pFile, int flags){
  return SQLITE_IOERR_FSYNC;
}

/*
** Write the size of the file in bytes to *pSize.
*/
static int socketFileSize(sqlite3_file *pFile, sqlite3_int64 *pSize){
  SocketFile *pSock = (SocketFile*)pFile;
  *pSize = pSock->szFile;
  return SQLITE_OK;
}

/*
** Locking functions. All no-ops.
*/
static int socketLock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}
static int socketUnlock(sqlite3_file *pFile, int eLock){
  return SQLITE_OK;
}
static int socketCheckReservedLock(sqlite3_file *pFile, int *pResOut){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** No xFileControl() verbs are implemented by this VFS.
*/
static int socketFileControl(sqlite3_file *pFile, int op, void *pArg){
  return SQLITE_OK;
}

/*
** The xSectorSize() and xDeviceCharacteristics() methods. These two
** may return special values allowing SQLite to optimize file-system 
** access to some extent. But it is also safe to simply return 0.
*/
static int socketSectorSize(sqlite3_file *pFile){
  return 0;
}
static int socketDeviceCharacteristics(sqlite3_file *pFile){
  return 0;
}

/*
** Open a SocketFile file.
*/
static int socketOpen(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zName,              /* File to open, or 0 for a temp file */
  sqlite3_file *pFile,            /* Pointer to SocketFile struct to populate */
  int flags,                      /* Input SQLITE_OPEN_XXX flags */
  int *pOutFlags                  /* Output SQLITE_OPEN_XXX flags (or NULL) */
){
  static const sqlite3_io_methods socketio = {
    1,                            /* iVersion */
    socketClose,                  /* xClose */
    socketRead,                   /* xRead */
    socketWrite,                  /* xWrite */
    socketTruncate,               /* xTruncate */
    socketSync,                   /* xSync */
    socketFileSize,               /* xFileSize */
    socketLock,                   /* xLock */
    socketUnlock,                 /* xUnlock */
    socketCheckReservedLock,      /* xCheckReservedLock */
    socketFileControl,            /* xFileControl */
    socketSectorSize,             /* xSectorSize */
    socketDeviceCharacteristics   /* xDeviceCharacteristics */
  };

  SocketFile *pSock = (SocketFile*)pFile;

  char zHost[1024];
  const char *zPort;
  int i;

  struct addrinfo hints;
  struct addrinfo *ai = 0;
  struct addrinfo *pInfo;
  unsigned char aFileSize[8];

  pSock->iSocket = -1;
  if( (flags & SQLITE_OPEN_MAIN_DB)==0 ) return SQLITE_CANTOPEN;

  /* Parse the argument and copy the results to zHost and zPort. It should be
  ** "hostname:port". Anything else is an error.  */
  assert( sizeof(zHost)>=pVfs->mxPathname );
  if( zName==0 ) return SQLITE_CANTOPEN;
  for(i=0; zName[i] && zName[i]!=':'; i++);
  if( zName[i]==0 ) return SQLITE_CANTOPEN;
  memcpy(zHost, zName, i);
  zHost[i] = '\0';
  zPort = &zName[i+1];

  /* Resolve the address */
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  if( getaddrinfo(zHost, zPort, &hints, &ai) ){
    return SQLITE_CANTOPEN;
  }

  /* Connect to the resolved address. Set SocketFile.iSocket to the tcp/ip
  ** socket and return SQLITE_OK.  */
  for(pInfo=ai; pInfo; pInfo=pInfo->ai_next){
    int sd = socket(pInfo->ai_family, pInfo->ai_socktype, pInfo->ai_protocol);
    if( sd<0 ) continue;
    if( connect(sd, pInfo->ai_addr, pInfo->ai_addrlen)<0 ){
      socket_close(sd);
      continue;
    }
    pSock->iSocket = sd;
    break;
  }

  if( ai ) freeaddrinfo(ai);
  if( pSock->iSocket<0 ) return SQLITE_CANTOPEN;

  /* The server sends back the file size as a 64-bit big-endian */
  if( socket_recv(pSock->iSocket, aFileSize, 8) ){
    socket_close(pSock->iSocket);
    return SQLITE_CANTOPEN;
  }
  pSock->szFile = (sqlite3_int64)socketGetU64(aFileSize);

  *pOutFlags = flags & ~(SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
  *pOutFlags |= SQLITE_OPEN_READONLY;
  pSock->base.pMethods = &socketio;
  return SQLITE_OK;
}

/*
** Another no-op. This is a read-only VFS.
*/
static int socketDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
  return SQLITE_IOERR_DELETE;
}

/*
** This is used by SQLite to detect journal and wal files. Which cannot
** exist for this VFS. So always set the output to false and return 
** SQLITE_OK.
*/
static int socketAccess(
  sqlite3_vfs *pVfs, 
  const char *zPath, 
  int flags, 
  int *pResOut
){
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** A no-op. Copy the input to the output.
*/
static int socketFullPathname(
  sqlite3_vfs *pVfs,              /* VFS */
  const char *zPath,              /* Input path (possibly a relative path) */
  int nPathOut,                   /* Size of output buffer in bytes */
  char *zPathOut                  /* Pointer to output buffer */
){
  int nByte = strlen(zPath);
  if( nByte>=pVfs->mxPathname ) return SQLITE_IOERR;
  memcpy(zPathOut, zPath, nByte+1);
  return SQLITE_OK;
}

/*
** The following four VFS methods:
**
**   xDlOpen
**   xDlError
**   xDlSym
**   xDlClose
**
** are supposed to implement the functionality needed by SQLite to load
** extensions compiled as shared objects. This simple VFS does not support
** this functionality, so the following functions are no-ops.
*/
static void *socketDlOpen(sqlite3_vfs *pVfs, const char *zPath){
  return 0;
}
static void socketDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
  sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
  zErrMsg[nByte-1] = '\0';
}
static void (*socketDlSym(sqlite3_vfs *pVfs, void *pH, const char *z))(void){
  return 0;
}
static void socketDlClose(sqlite3_vfs *pVfs, void *pHandle){
  return;
}

/*
** Parameter zByte points to a buffer nByte bytes in size. Populate this
** buffer with pseudo-random data.
*/
static int socketRandomness(sqlite3_vfs *pVfs, int nByte, char *zByte){
  memset(zByte, 0, nByte);
  return SQLITE_OK;
}

/*
** Sleep for at least nMicro microseconds. Return the (approximate) number 
** of microseconds slept for.
*/
static int socketSleep(sqlite3_vfs *pVfs, int nMicro){
#ifdef _WIN32
  Sleep(nMicro/1000);
#else
  sleep(nMicro / 1000000);
  usleep(nMicro % 1000000);
#endif
  return nMicro;
}

/*
** Set *pTime to the current UTC time expressed as a Julian day. Return
** SQLITE_OK if successful, or an error code otherwise.
**
**   http://en.wikipedia.org/wiki/Julian_day
**
** This implementation is not very good. The current time is rounded to
** an integer number of seconds. Also, assuming time_t is a signed 32-bit 
** value, it will stop working some time in the year 2038 AD (the so-called
** "year 2038" problem that afflicts systems that store time this way). 
*/
static int socketCurrentTime(sqlite3_vfs *pVfs, double *pTime){
  time_t t = time(0);
  *pTime = t/86400.0 + 2440587.5; 
  return SQLITE_OK;
}

/*
** This function returns a pointer to the VFS implemented in this file.
** To make the VFS available to SQLite:
**
**   sqlite3_vfs_register(sqlite3_socketvfs(), 0);
*/
sqlite3_vfs *sqlite3_socketvfs(void){
  static sqlite3_vfs socketvfs = {
    1,                            /* iVersion */
    sizeof(SocketFile),           /* szOsFile */
    512,                          /* mxPathname */
    0,                            /* pNext */
    "socket",                     /* zName */
    0,                            /* pAppData */
    socketOpen,                   /* xOpen */
    socketDelete,                 /* xDelete */
    socketAccess,                 /* xAccess */
    socketFullPathname,           /* xFullPathname */
    socketDlOpen,                 /* xDlOpen */
    socketDlError,                /* xDlError */
    socketDlSym,                  /* xDlSym */
    socketDlClose,                /* xDlClose */
    socketRandomness,             /* xRandomness */
    socketSleep,                  /* xSleep */
    socketCurrentTime,            /* xCurrentTime */
  };
  return &socketvfs;
}

/*
** Register the amatch virtual table
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_socketvfs_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Not used */
  sqlite3_vfs_register(sqlite3_socketvfs(), 0);
  return SQLITE_OK_LOAD_PERMANENTLY;
}

