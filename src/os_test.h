/*
** 2004 May 22
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
*/
#ifndef _SQLITE_OS_TEST_H_
#define _SQLITE_OS_TEST_H_

#define OsFile OsRealFile
#define OS_UNIX 1
#include "os_unix.h"
#undef OS_UNIX
#undef OsFile

/* Include sqliteInt.h now to get the type u8. */
#include "sqliteInt.h"

typedef struct OsFile OsFile;
struct OsFile {
  u8 **apBlk;       /* Array of blocks that have been written to. */
  int nBlk;         /* Size of apBlock. */
  int nMaxWrite;    /* Largest offset written to. */
  OsRealFile fd;
};

void sqlite3SetCrashseed(int seed);

#endif /* _SQLITE_OS_UNIX_H_ */
