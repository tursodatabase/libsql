/*
** 2025-06-05
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
** An implementation of opendir(), readdir(), and closedir() for Windows,
** based on the FindFirstFile(), FindNextFile(), and FindClose() APIs
** of Win32.
**
** #include this file inside any C-code module that needs to use
** opendir()/readdir()/closedir().  This file is a no-op on non-Windows
** machines.  On Windows, static functions are defined that implement
** those standard interfaces.
*/
#if defined(_WIN32) && defined(_MSC_VER) && !defined(SQLITE_WINDIRENT_H)
#define SQLITE_WINDIRENT_H

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <io.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#ifndef FILENAME_MAX
# define FILENAME_MAX (260)
#endif
#ifndef S_ISREG
#define S_ISREG(m) (((m) & S_IFMT) == S_IFREG)
#endif
#ifndef S_ISDIR
#define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
#endif
#ifndef S_ISLNK
#define S_ISLNK(m) (0)
#endif
typedef unsigned short mode_t;

/* The dirent object for Windows is abbreviated.  The only field really
** usable by applications is d_name[].
*/
struct dirent {
 int d_ino;                  /* Inode number (synthesized) */
 unsigned d_attributes;      /* File attributes */
 char d_name[FILENAME_MAX];  /* Null-terminated filename */
};

/* The internals of DIR are opaque according to standards.  So it
** does not matter what we put here. */
typedef struct DIR DIR;
struct DIR {
  intptr_t d_handle;         /* Handle for findfirst()/findnext() */
  struct dirent cur;         /* Current entry */
};

/* Ignore hidden and system files */
#define WindowsFileToIgnore(a) \
    ((((a).attrib)&_A_HIDDEN) || (((a).attrib)&_A_SYSTEM))

/*
** Close a previously opened directory
*/
static int closedir(DIR *pDir){
  int rc = 0;
  if( pDir==0 ){
    return EINVAL;
  }
  if( pDir->d_handle!=0 && pDir->d_handle!=(-1) ){
    rc = _findclose(pDir->d_handle);
  }
  sqlite3_free(pDir);
  return rc;
}

/*
** Open a new directory.  The directory name should be UTF-8 encoded.
** appropriate translations happen automatically.
*/
static DIR *opendir(const char *zDirName){
  DIR *pDir;
  wchar_t *b1;
  sqlite3_int64 sz;
  struct _wfinddata_t data;

  pDir = sqlite3_malloc64( sizeof(DIR) );
  if( pDir==0 ) return 0;
  memset(pDir, 0, sizeof(DIR));
  memset(&data, 0, sizeof(data));
  sz = strlen(zDirName);
  b1 = sqlite3_malloc64( (sz+3)*sizeof(b1[0]) );
  if( b1==0 ){
    closedir(pDir);
    return NULL;
  }
  sz = MultiByteToWideChar(CP_UTF8, 0, zDirName, sz, b1, sz);
  b1[sz++] = '\\';
  b1[sz++] = '*';
  b1[sz] = 0;
  if( sz+1>sizeof(data.name)/sizeof(data.name[0]) ){
    closedir(pDir);
    sqlite3_free(b1);
    return NULL;
  }
  memcpy(data.name, b1, (sz+1)*sizeof(b1[0]));
  sqlite3_free(b1);
  pDir->d_handle = _wfindfirst(data.name, &data);
  if( pDir->d_handle<0 ){
    closedir(pDir);
    return NULL;
  }
  while( WindowsFileToIgnore(data) ){
    memset(&data, 0, sizeof(data));
    if( _wfindnext(pDir->d_handle, &data)==-1 ){
      closedir(pDir);
      return NULL;
    }
  }
  pDir->cur.d_ino = 0;
  pDir->cur.d_attributes = data.attrib;
  WideCharToMultiByte(CP_UTF8, 0, data.name, -1,
                      pDir->cur.d_name, FILENAME_MAX, 0, 0);
  return pDir;
}

/*
** Read the next entry from a directory.
**
** The returned struct-dirent object is managed by DIR.  It is only
** valid until the next readdir() or closedir() call.  Only the
** d_name[] field is meaningful.  The d_name[] value has been
** translated into UTF8.
*/
static struct dirent *readdir(DIR *pDir){
  struct _wfinddata_t data;
  if( pDir==0 ) return 0;
  if( (pDir->cur.d_ino++)==0 ){
    return &pDir->cur;
  }
  do{
    memset(&data, 0, sizeof(data));
    if( _wfindnext(pDir->d_handle, &data)==-1 ){
      return NULL;
    }
  }while( WindowsFileToIgnore(data) );
  pDir->cur.d_attributes = data.attrib;
  WideCharToMultiByte(CP_UTF8, 0, data.name, -1,
                      pDir->cur.d_name, FILENAME_MAX, 0, 0);
  return &pDir->cur;
}

#endif /* defined(_WIN32) && defined(_MSC_VER) */
