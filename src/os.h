/*
** 2001 September 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This header file (together with is companion C source-code file
** "os.c") attempt to abstract the underlying operating system so that
** the SQLite library will work on both POSIX and windows systems.
*/
#ifndef _SQLITE_OS_H_
#define _SQLITE_OS_H_

#ifndef OS_UNIX
# ifndef OS_WIN
#  if defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#    define OS_WIN 1
#    define OS_UNIX 0
#  else
#    define OS_WIN 0
#    define OS_UNIX 1
#  endif
# else
#  define OS_UNIX 0
# endif
#endif
#ifndef OS_WIN
# define OS_WIN 0
#endif

/*
** A handle for an open file is stored in an OsFile object.
*/
#if OS_UNIX
  typedef struct OsFile OsFile;
  struct OsFile {
    struct lockInfo *pLock;  /* Information about locks on this inode */
    int fd;                  /* The file descriptor */
    int locked;              /* True if this user holds the lock */
  };
# define SQLITE_TEMPNAME_SIZE 200
# if defined(HAVE_USLEEP) && HAVE_USLEEP
#  define SQLITE_MIN_SLEEP_MS 1
# else
#  define SQLITE_MIN_SLEEP_MS 1000
# endif
#endif

#if OS_WIN
#include <windows.h>
#include <winbase.h>
  typedef struct OsFile OsFile;
  struct OsFile {
    HANDLE h;               /* Handle for accessing the file */
    int locked;             /* 0: unlocked, <0: write lock, >0: read lock */
  };
# define SQLITE_TEMPNAME_SIZE (MAX_PATH+50)
# define SQLITE_MIN_SLEEP_MS 1
#endif

int sqliteOsDelete(const char*);
int sqliteOsFileExists(const char*);
int sqliteOsOpenReadWrite(const char*, OsFile*, int*);
int sqliteOsOpenExclusive(const char*, OsFile*, int);
int sqliteOsOpenReadOnly(const char*, OsFile*);
int sqliteOsTempFileName(char*);
int sqliteOsClose(OsFile*);
int sqliteOsRead(OsFile*, void*, int amt);
int sqliteOsWrite(OsFile*, const void*, int amt);
int sqliteOsSeek(OsFile*, int offset);
int sqliteOsSync(OsFile*);
int sqliteOsTruncate(OsFile*, int size);
int sqliteOsFileSize(OsFile*, int *pSize);
int sqliteOsReadLock(OsFile*);
int sqliteOsWriteLock(OsFile*);
int sqliteOsUnlock(OsFile*);
int sqliteOsRandomSeed(char*);
int sqliteOsSleep(int ms);
void sqliteOsEnterMutex(void);
void sqliteOsLeaveMutex(void);



#endif /* _SQLITE_OS_H_ */
