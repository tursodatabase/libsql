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
** A handle for an open file is stored in an OsFile object.
*/
#if OS_UNIX
  typedef int OsFile;
# define SQLITE_TEMPNAME_SIZE 200
#endif

#if OS_WIN
  typedef HANDLE OsFile;
# define SQLITE_TEMPNAME_SIZE (MAX_PATH+1)
#endif

int sqliteOsOpenReadWrite(char*, OsFile*, int*);
int sqliteOsOpenExclusive(char*, OsFile*);
int sqliteOsTempFileName(char*);
int sqliteOsClose(OsFile);
int sqliteOsRead(OsFile, int amt, void*);
int sqliteOsWrite(OsFile, int amt, void*);
int sqliteOsSeek(OsFile, int offset);
int sqliteOsSync(OsFile);
int sqliteOsTruncate(OsFile, int size);
int sqliteOsFileSize(OsFile, int *pSize);
int sqliteOsLock(OsFile, int wrlock);
int sqliteOsUnlock(OsFile);
int sqliteOsRandomSeed(int amt, char*);
int sqliteSleep(int ms);



#endif /* _SQLITE_OS_H_ */
