/*
** 2015 November 30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains declarations for most of the opendir() family of
** POSIX functions on Win32 using the MSVCRT.
*/

#if defined(_WIN32) && defined(_MSC_VER)

/*
** We need several data types from the Windows SDK header.
*/

#define WIN32_LEAN_AND_MEAN
#include "windows.h"

/*
** We need several support functions from the SQLite core.
*/

#include "sqlite3.h"

/*
** We need several things from the ANSI and MSVCRT headers.
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <io.h>
#include <limits.h>

/*
** We may need to provide the "ino_t" type.
*/

#ifndef INO_T_DEFINED
  #define INO_T_DEFINED
  typedef unsigned short ino_t;
#endif

/*
** We need to define "NAME_MAX" if it was not present in "limits.h".
*/

#ifndef NAME_MAX
#  ifdef FILENAME_MAX
#    define NAME_MAX (FILENAME_MAX)
#  else
#    define NAME_MAX (260)
#  endif
#endif

/*
** We need to define "NULL_INTPTR_T" and "BAD_INTPTR_T".
*/

#ifndef NULL_INTPTR_T
#  define NULL_INTPTR_T ((intptr_t)(0))
#endif

#ifndef BAD_INTPTR_T
#  define BAD_INTPTR_T ((intptr_t)(-1))
#endif

/*
** We need to provide the necessary structures and related types.
*/

typedef struct DIRENT DIRENT;
typedef struct DIR DIR;
typedef DIRENT *LPDIRENT;
typedef DIR *LPDIR;

struct DIRENT {
  ino_t d_ino;               /* Sequence number, do not use. */
  unsigned d_attributes;     /* Win32 file attributes. */
  char d_name[NAME_MAX + 1]; /* Name within the directory. */
};

struct DIR {
  intptr_t d_handle; /* Value returned by "_findfirst". */
  DIRENT d_first;    /* DIRENT constructed based on "_findfirst". */
  DIRENT d_next;     /* DIRENT constructed based on "_findnext". */
};

/*
** Provide the function prototype for the POSIX compatiable getenv()
** function.  This function is not thread-safe.
*/

extern const char *windirent_getenv(const char *name);

/*
** Finally, we can provide the function prototypes for the opendir(),
** readdir(), readdir_r(), and closedir() POSIX functions.
*/

extern LPDIR opendir(const char *dirname);
extern LPDIRENT readdir(LPDIR dirp);
extern INT readdir_r(LPDIR dirp, LPDIRENT entry, LPDIRENT *result);
extern INT closedir(LPDIR dirp);

#endif /* defined(WIN32) && defined(_MSC_VER) */
