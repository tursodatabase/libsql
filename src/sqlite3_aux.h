/*
** 2006 January 05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines auxiliary interfaces to the SQLite library.
** This header file is a companion to the official "sqlite.h" interface
** file.  The difference is that the extraordinary efforts are made to
** insure that the interface defined in "sqlite.h" is always backwards
** compatible.  No such guarantees are made for the auxiliary interfaces
** defined in this header file.  The interfaces defined here are subject
** to change in future releases of SQLite.
**
** We justify the volitility of the interfaces defined here by noting that
** these interfaces are designed not for users of the SQLite library but
** by code that wishes to expand and extend the SQLite library.  Some
** knowledge of what SQLite is doing internally is necessary to use these
** interfaces.  
**
** We have no intention of changing the interfaces defined in this file
** gratuitously.  No interfaces will be changed without good reason.  But
** on the other hand, if the quality and functionality of SQLite can be
** enhanced by modifying the interfaces found here, then we will do so.
**
** Since these interfaces are variable, it is suggested that they not
** be accessed as a shared library.  Users of these interfaces should
** statically link.
**
** @(#) $Id: sqlite3_aux.h,v 1.1 2006/01/06 03:29:58 drh Exp $
*/
/*
** Forward declarations
*/
typedef struct OsFile OsFile;
typedef struct IoMethod IoMethod;

/*
** An instance of the following structure contains pointers to all
** methods on an OsFile object.
*/
struct IoMethod {
  int (*xClose)(OsFile**);
  int (*xOpenDirectory)(OsFile*, const char*);
  int (*xRead)(OsFile*, void*, int amt);
  int (*xWrite)(OsFile*, const void*, int amt);
  int (*xSeek)(OsFile*, i64 offset);
  int (*xTruncate)(OsFile*, i64 size);
  int (*xSync)(OsFile*, int);
  void (*xSetFullSync)(OsFile *id, int setting);
  int (*xFileHandle)(OsFile *id);
  int (*xFileSize)(OsFile*, i64 *pSize);
  int (*xLock)(OsFile*, int);
  int (*xUnlock)(OsFile*, int);
  int (*xLockState)(OsFile *id);
  int (*xCheckReservedLock)(OsFile *id);
};

/*
** The OsFile object describes an open disk file in an OS-dependent way.
** The version of OsFile defined here is a generic version.  Each OS
** implementation defines its own subclass of this structure that contains
** additional information needed to handle file I/O.  But the pMethod
** entry (pointing to the virtual function table) always occurs first
** so that we can always find the appropriate methods.
*/
struct OsFile {
  IoMethod const *pMethod;
};

/*
** The following values may be passed as the second argument to
** sqlite3OsLock(). The various locks exhibit the following semantics:
**
** SHARED:    Any number of processes may hold a SHARED lock simultaneously.
** RESERVED:  A single process may hold a RESERVED lock on a file at
**            any time. Other processes may hold and obtain new SHARED locks.
** PENDING:   A single process may hold a PENDING lock on a file at
**            any one time. Existing SHARED locks may persist, but no new
**            SHARED locks may be obtained by other processes.
** EXCLUSIVE: An EXCLUSIVE lock precludes all other locks.
**
** PENDING_LOCK may not be passed directly to sqlite3OsLock(). Instead, a
** process that requests an EXCLUSIVE lock may actually obtain a PENDING
** lock. This can be upgraded to an EXCLUSIVE lock by a subsequent call to
** sqlite3OsLock().
*/
#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

/*
** File Locking Notes:  (Mostly about windows but also some info for Unix)
**
** We cannot use LockFileEx() or UnlockFileEx() on Win95/98/ME because
** those functions are not available.  So we use only LockFile() and
** UnlockFile().
**
** LockFile() prevents not just writing but also reading by other processes.
** A SHARED_LOCK is obtained by locking a single randomly-chosen 
** byte out of a specific range of bytes. The lock byte is obtained at 
** random so two separate readers can probably access the file at the 
** same time, unless they are unlucky and choose the same lock byte.
** An EXCLUSIVE_LOCK is obtained by locking all bytes in the range.
** There can only be one writer.  A RESERVED_LOCK is obtained by locking
** a single byte of the file that is designated as the reserved lock byte.
** A PENDING_LOCK is obtained by locking a designated byte different from
** the RESERVED_LOCK byte.
**
** On WinNT/2K/XP systems, LockFileEx() and UnlockFileEx() are available,
** which means we can use reader/writer locks.  When reader/writer locks
** are used, the lock is placed on the same range of bytes that is used
** for probabilistic locking in Win95/98/ME.  Hence, the locking scheme
** will support two or more Win95 readers or two or more WinNT readers.
** But a single Win95 reader will lock out all WinNT readers and a single
** WinNT reader will lock out all other Win95 readers.
**
** The following #defines specify the range of bytes used for locking.
** SHARED_SIZE is the number of bytes available in the pool from which
** a random byte is selected for a shared lock.  The pool of bytes for
** shared locks begins at SHARED_FIRST. 
**
** These #defines are available in sqlite_aux.h so that adaptors for
** connecting SQLite to other operating systems can use the same byte
** ranges for locking.  In particular, the same locking strategy and
** byte ranges are used for Unix.  This leaves open the possiblity of having
** clients on win95, winNT, and unix all talking to the same shared file
** and all locking correctly.  To do so would require that samba (or whatever
** tool is being used for file sharing) implements locks correctly between
** windows and unix.  I'm guessing that isn't likely to happen, but by
** using the same locking range we are at least open to the possibility.
**
** Locking in windows is manditory.  For this reason, we cannot store
** actual data in the bytes used for locking.  The pager never allocates
** the pages involved in locking therefore.  SHARED_SIZE is selected so
** that all locks will fit on a single page even at the minimum page size.
** PENDING_BYTE defines the beginning of the locks.  By default PENDING_BYTE
** is set high so that we don't have to allocate an unused page except
** for very large databases.  But one should test the page skipping logic 
** by setting PENDING_BYTE low and running the entire regression suite.
**
** Changing the value of PENDING_BYTE results in a subtly incompatible
** file format.  Depending on how it is changed, you might not notice
** the incompatibility right away, even running a full regression test.
** The default location of PENDING_BYTE is the first byte past the
** 1GB boundary.
**
*/
#ifndef SQLITE_TEST
#define PENDING_BYTE      0x40000000  /* First byte past the 1GB boundary */
#else
extern unsigned int sqlite3_pending_byte;
#define PENDING_BYTE sqlite3_pending_byte
#endif

#define RESERVED_BYTE     (PENDING_BYTE+1)
#define SHARED_FIRST      (PENDING_BYTE+2)
#define SHARED_SIZE       510

/*
** A single global instance of the following structure holds pointers to 
** the routines that SQLite uses to talk with the underlying operating
** system.  Clever programmers can substitute alternative implementations
** of these routine (prior to using any SQLite API!) in order to modify
** the way SQLite interacts with its environment.  For example, modifications
** could be supplied that allow SQLite to talk to a virtual file system.
*/
extern struct sqlite3OsVtbl {
  int (*xOpenReadWrite)(const char*, OsFile**, int*);
  int (*xOpenExclusive)(const char*, OsFile**, int);
  int (*xOpenReadOnly)(const char*, OsFile**);

  int (*xDelete)(const char*);
  int (*xFileExists)(const char*);
  char *(*xFullPathname)(const char*);
  int (*xIsDirWritable)(char*);
  int (*xSyncDirectory)(const char*);
  int (*xTempFileName)(char*);

  int (*xRandomSeed)(char*);
  int (*xSleep)(int ms);
  int (*xCurrentTime)(double*);

  void (*xEnterMutex)(void);
  void (*xLeaveMutex)(void);
  int (*xInMutex)(void);
  void *(*xThreadSpecificData)(int);

  void *(*xMalloc)(int);
  void *(*xRealloc)(void *, int);
  void (*xFree)(void *);
  int (*xAllocationSize)(void *);
} sqlite3Os;

/*
** The following API routine returns a pointer to the sqlite3Os global
** variable.  It is probably easier just to reference the global variable
** directly.  This routine is provided for backwards compatibility with
** an older interface design.
*/
struct sqlite3OsVtbl *sqlite3_os_switch(void);


/*
** The following are prototypes of convenience routines that simply
** call the corresponding routines in the OsFile.pMethod virtual
** function table.
*/
int sqlite3OsClose(OsFile**);
int sqlite3OsOpenDirectory(OsFile*, const char*);
int sqlite3OsRead(OsFile*, void*, int amt);
int sqlite3OsWrite(OsFile*, const void*, int amt);
int sqlite3OsSeek(OsFile*, i64 offset);
int sqlite3OsTruncate(OsFile*, i64 size);
int sqlite3OsSync(OsFile*, int);
void sqlite3OsSetFullSync(OsFile *id, int setting);
int sqlite3OsFileHandle(OsFile *id);
int sqlite3OsFileSize(OsFile*, i64 *pSize);
int sqlite3OsLock(OsFile*, int);
int sqlite3OsUnlock(OsFile*, int);
int sqlite3OsLockState(OsFile *id);
int sqlite3OsCheckReservedLock(OsFile *id);
