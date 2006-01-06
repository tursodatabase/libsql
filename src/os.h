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
#include "sqlite3_aux.h"

/*
** Figure out if we are dealing with Unix, Windows or MacOS.
**
** N.B. MacOS means Mac Classic (or Carbon). Treat Darwin (OS X) as Unix.
**      The MacOS build is designed to use CodeWarrior (tested with v8)
*/
#if !defined(OS_UNIX) && !defined(OS_ALT)
# define OS_OTHER 0
# ifndef OS_WIN
#   if defined(_WIN32) || defined(WIN32) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#     define OS_WIN 1
#     define OS_UNIX 0
#   else
#     define OS_WIN 0
#     define OS_UNIX 1
#  endif
# else
#  define OS_UNIX 0
# endif
#else
# ifndef OS_WIN
#  define OS_WIN 0
# endif
#endif


/*
** Define the maximum size of a temporary filename
*/
#if OS_WIN
# include <windows.h>
# define SQLITE_TEMPNAME_SIZE (MAX_PATH+50)
#else
# define SQLITE_TEMPNAME_SIZE 200
#endif

/* If the SET_FULLSYNC macro is not defined above, then make it
** a no-op
*/
#ifndef SET_FULLSYNC
# define SET_FULLSYNC(x,y)
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



#endif /* _SQLITE_OS_H_ */
