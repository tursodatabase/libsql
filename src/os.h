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

/*
** Figure out if we are dealing with Unix, Windows or MacOS.
**
** N.B. MacOS means Mac Classic (or Carbon). Treat Darwin (OS X) as Unix.
**      The MacOS build is designed to use CodeWarrior (tested with v8)
*/
#ifndef OS_UNIX
# ifndef OS_WIN
#  ifndef OS_MAC
#    if defined(__MACOS__)
#      define OS_MAC 1
#      define OS_WIN 0
#      define OS_UNIX 0
#    elif defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#      define OS_MAC 0
#      define OS_WIN 1
#      define OS_UNIX 0
#    else
#      define OS_MAC 0
#      define OS_WIN 0
#      define OS_UNIX 1
#    endif
#  else
#    define OS_WIN 0
#    define OS_UNIX 0
#  endif
# else
#  define OS_MAC 0
#  define OS_UNIX 0
# endif
#else
# define OS_MAC 0
# ifndef OS_WIN
#  define OS_WIN 0
# endif
#endif

/*
** Invoke the appropriate operating-system specific header file.
*/
#if OS_UNIX
# include "os_unix.h"
#endif
#if OS_WIN
# include "os_win.h"
#endif
#if OS_MAC
# include "os_mac.h"
#endif

/*
** Temporary files are named starting with this prefix followed by 16 random
** alphanumeric characters, and no file extension. They are stored in the
** OS's standard temporary file directory, and are deleted prior to exit.
** If sqlite is being embedded in another program, you may wish to change the
** prefix to reflect your program's name, so that if your program exits
** prematurely, old temporary files can be easily identified. This can be done
** using -DTEMP_FILE_PREFIX=myprefix_ on the compiler command line.
*/
#ifndef TEMP_FILE_PREFIX
# define TEMP_FILE_PREFIX "sqlite_"
#endif

/*
** The following values may be passed as the second argument to
** sqlite3OsLock().
*/
#define NO_LOCK         0
#define SHARED_LOCK     1
#define RESERVED_LOCK   2
#define PENDING_LOCK    3
#define EXCLUSIVE_LOCK  4

int sqlite3OsDelete(const char*);
int sqlite3OsFileExists(const char*);
int sqliteOsFileRename(const char*, const char*);
int sqlite3OsOpenReadWrite(const char*, OsFile*, int*);
int sqlite3OsOpenExclusive(const char*, OsFile*, int);
int sqlite3OsOpenReadOnly(const char*, OsFile*);
int sqlite3OsOpenDirectory(const char*, OsFile*);
int sqlite3OsTempFileName(char*);
int sqlite3OsClose(OsFile*);
int sqlite3OsRead(OsFile*, void*, int amt);
int sqlite3OsWrite(OsFile*, const void*, int amt);
int sqlite3OsSeek(OsFile*, off_t offset);
int sqlite3OsSync(OsFile*);
int sqlite3OsTruncate(OsFile*, off_t size);
int sqlite3OsFileSize(OsFile*, off_t *pSize);
int sqlite3OsReadLock(OsFile*);
int sqlite3OsWriteLock(OsFile*);
int sqlite3OsUnlock(OsFile*);
int sqlite3OsRandomSeed(char*);
int sqlite3OsSleep(int ms);
int sqlite3OsCurrentTime(double*);
void sqlite3OsEnterMutex(void);
void sqlite3OsLeaveMutex(void);
char *sqlite3OsFullPathname(const char*);
int sqlite3OsLock(OsFile*, int);
int sqlite3OsCheckWriteLock(OsFile *id);

#endif /* _SQLITE_OS_H_ */
