/*
** 2016-05-05
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
** This file implements a utility function (and a utility program) that
** makes a copy of an SQLite database while simultaneously zeroing out all
** deleted content.
**
** Normally (when PRAGMA secure_delete=OFF, which is the default) when SQLite
** deletes content, it does not overwrite the deleted content but rather marks
** the region of the file that held that content as being reusable.  This can
** cause deleted content to recoverable from the database file.  This stale
** content is removed by the VACUUM command, but VACUUM can be expensive for
** large databases.  When in PRAGMA secure_delete=ON mode, the deleted content
** is zeroed, but secure_delete=ON has overhead as well.
**
** This utility attempts to make a copy of a complete SQLite database where
** all of the deleted content is zeroed out in the copy, and it attempts to
** do so while being faster than running VACUUM.
**
** Usage:
**
**   int sqlite3_scrub_backup(
**       const char *zSourceFile,   // Source database filename
**       const char *zDestFile,     // Destination database filename
**       char **pzErrMsg            // Write error message here
**   );
**
** Simply call the API above specifying the filename of the source database
** and the name of the backup copy.  The source database must already exist
** and can be in active use. (A read lock is held during the backup.)  The
** destination file should not previously exist.  If the pzErrMsg parameter
** is non-NULL and if an error occurs, then an error message might be written
** into memory obtained from sqlite3_malloc() and *pzErrMsg made to point to
** that error message.  But if the error is an OOM, the error might not be
** reported.  The routine always returns non-zero if there is an error.
**
** If compiled with -DSCRUB_STANDALONE then a main() procedure is added and
** this file becomes a standalone program that can be run as follows:
**
**      ./sqlite3scrub SOURCE DEST
*/
#include "sqlite3.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>

typedef struct ScrubState ScrubState;
typedef unsigned char u8;

/* State information for a scrub-and-backup operation */
struct ScrubState {
  const char *zSrcFile;    /* Name of the source file */
  const char *zDestFile;   /* Name of the destination file */
  int rcErr;               /* Error code */
  char *zErr;              /* Error message text */
  sqlite3 *dbSrc;          /* Source database connection */
  sqlite3_file *pSrc;      /* Source file handle */
  sqlite3 *dbDest;         /* Destination database connection */
  sqlite3_file *pDest;     /* Destination file handle */
  unsigned int szPage;     /* Page size */
  unsigned int nPage;      /* Number of pages */
  u8 *page1;               /* Content of page 1 */
};

/* Store an error message */
static void scrubBackupErr(ScrubState *p, const char *zFormat, ...){
  va_list ap;
  sqlite3_free(p->zErr);
  va_start(ap, zFormat);
  p->zErr = sqlite3_vmprintf(zFormat, ap);
  va_end(ap);
  if( p->rcErr==0 ) p->rcErr = SQLITE_ERROR;
}

/* Allocate memory to hold a single page of content */
static u8 *scrubBackupAllocPage(ScrubState *p){
  u8 *pPage;
  if( p->rcErr ) return 0;
  pPage = sqlite3_malloc( p->szPage );
  if( pPage==0 ) p->rcErr = SQLITE_NOMEM;
  return pPage;
}

/* Read a page from the source database into memory.  Use the memory
** provided by pBuf if not NULL or allocate a new page if pBuf==NULL.
*/
static u8 *scrubBackupRead(ScrubState *p, int pgno, u8 *pBuf){
  int rc;
  sqlite3_int64 iOff;
  u8 *pOut = pBuf;
  if( p->rcErr ) return 0;
  if( pOut==0 ){
    pOut = scrubBackupAllocPage(p);
    if( pOut==0 ) return 0;
  }
  iOff = (pgno-1)*(sqlite3_int64)p->szPage;
  rc = p->pSrc->pMethods->xRead(p->pSrc, pOut, p->szPage, iOff);
  if( rc!=SQLITE_OK ){
    if( pBuf==0 ) sqlite3_free(pOut);
    pOut = 0;
    scrubBackupErr(p, "read failed for page %d", pgno);
    p->rcErr = SQLITE_IOERR;
  }
  return pOut;  
}

/* Write a page to the destination database */
static void scrubBackupWrite(ScrubState *p, int pgno, u8 *pData){
  int rc;
  sqlite3_int64 iOff;
  if( p->rcErr ) return;
  iOff = (pgno-1)*(sqlite3_int64)p->szPage;
  rc = p->pDest->pMethods->xWrite(p->pDest, pData, p->szPage, iOff);
  if( rc!=SQLITE_OK ){
    scrubBackupErr(p, "write failed for page %d", pgno);
    p->rcErr = SQLITE_IOERR;
  }
}

/* Prepare a statement against the "db" database. */
static sqlite3_stmt *scrubBackupPrepare(
  ScrubState *p,      /* Backup context */
  sqlite3 *db,        /* Database to prepare against */
  const char *zSql    /* SQL statement */
){
  sqlite3_stmt *pStmt;
  if( p->rcErr ) return 0;
  p->rcErr = sqlite3_prepare_v2(db, zSql, -1, &pStmt, 0);
  if( p->rcErr ){
    scrubBackupErr(p, "SQL error \"%s\" on \"%s\"",
                   sqlite3_errmsg(db), zSql);
    sqlite3_finalize(pStmt);
    return 0;
  }
  return pStmt;
}


/* Open the source database file */
static void scrubBackupOpenSrc(ScrubState *p){
  sqlite3_stmt *pStmt;
  int rc;
  /* Open the source database file */
  p->rcErr = sqlite3_open_v2(p->zSrcFile, &p->dbSrc,
                 SQLITE_OPEN_READONLY |
                 SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE, 0);
  if( p->rcErr ){
    scrubBackupErr(p, "cannot open source database: %s",
                      sqlite3_errmsg(p->dbSrc));
    return;
  }
  p->rcErr = sqlite3_exec(p->dbSrc, "BEGIN", 0, 0, 0);
  if( p->rcErr ){
    scrubBackupErr(p,
       "cannot start a read transaction on the source database: %s",
       sqlite3_errmsg(p->dbSrc));
    return;
  }
  pStmt = scrubBackupPrepare(p, p->dbSrc, "PRAGMA page_size");
  if( pStmt==0 ) return;
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    p->szPage = sqlite3_column_int(pStmt, 0);
  }else{
    scrubBackupErr(p, "unable to determine the page size");
  }
  sqlite3_finalize(pStmt);
  if( p->rcErr ) return;
  pStmt = scrubBackupPrepare(p, p->dbSrc, "PRAGMA page_count");
  if( pStmt==0 ) return;
  rc = sqlite3_step(pStmt);
  if( rc==SQLITE_ROW ){
    p->nPage = sqlite3_column_int(pStmt, 0);
  }else{
    scrubBackupErr(p, "unable to determine the size of the source database");
  }
  sqlite3_finalize(pStmt);
  sqlite3_file_control(p->dbSrc, "main", SQLITE_FCNTL_FILE_POINTER, &p->pSrc);
  if( p->pSrc==0 || p->pSrc->pMethods==0 ){
    scrubBackupErr(p, "cannot get the source file handle");
    p->rcErr = SQLITE_ERROR;
  }
}

/* Create and open the destination file */
static void scrubBackupOpenDest(ScrubState *p){
  sqlite3_stmt *pStmt;
  int rc;
  char *zSql;
  if( p->rcErr ) return;
  p->rcErr = sqlite3_open_v2(p->zDestFile, &p->dbDest,
                 SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                 SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE, 0);
  if( p->rcErr ){
    scrubBackupErr(p, "cannot open destination database: %s",
                      sqlite3_errmsg(p->dbDest));
    return;
  }
  zSql = sqlite3_mprintf("PRAGMA page_size(%u);", p->szPage);
  if( zSql==0 ){
    p->rcErr = SQLITE_NOMEM;
    return;
  }
  p->rcErr = sqlite3_exec(p->dbDest, zSql, 0, 0, 0);
  sqlite3_free(zSql);
  if( p->rcErr ){
    scrubBackupErr(p,
       "cannot set the page size on the destination database: %s",
       sqlite3_errmsg(p->dbDest));
    return;
  }
  sqlite3_exec(p->dbDest, "PRAGMA journal_mode=OFF;", 0, 0, 0);
  p->rcErr = sqlite3_exec(p->dbDest, "BEGIN EXCLUSIVE;", 0, 0, 0);
  if( p->rcErr ){
    scrubBackupErr(p,
       "cannot start a write transaction on the destination database: %s",
       sqlite3_errmsg(p->dbDest));
    return;
  }
  pStmt = scrubBackupPrepare(p, p->dbDest, "PRAGMA page_count;");
  if( pStmt==0 ) return;
  rc = sqlite3_step(pStmt);
  if( rc!=SQLITE_ROW ){
    scrubBackupErr(p, "cannot measure the size of the destination");
  }else if( sqlite3_column_int(pStmt, 0)>1 ){
    scrubBackupErr(p, "destination database is not empty - holds %d pages",
                   sqlite3_column_int(pStmt, 0));
  }
  sqlite3_finalize(pStmt);
  sqlite3_file_control(p->dbDest, "main", SQLITE_FCNTL_FILE_POINTER, &p->pDest);
  if( p->pDest==0 || p->pDest->pMethods==0 ){
    scrubBackupErr(p, "cannot get the destination file handle");
    p->rcErr = SQLITE_ERROR;
  }
}

int sqlite3_scrub_backup(
  const char *zSrcFile,    /* Source file */
  const char *zDestFile,   /* Destination file */
  char **pzErr             /* Write error here if non-NULL */
){
  ScrubState s;
  unsigned int i;
  u8 *pBuf = 0;
  u8 *pData;

  memset(&s, 0, sizeof(s));
  s.zSrcFile = zSrcFile;
  s.zDestFile = zDestFile;

  scrubBackupOpenSrc(&s);
  scrubBackupOpenDest(&s);
  pBuf = scrubBackupAllocPage(&s);

  for(i=1; s.rcErr==0 && i<=s.nPage; i++){
    pData = scrubBackupRead(&s, i, pBuf);
    scrubBackupWrite(&s, i, pData);
  }

  /* Close the destination database without closing the transaction. If we
  ** commit, page zero will be overwritten. */
  sqlite3_close(s.dbDest);

  sqlite3_close(s.dbSrc);
  sqlite3_free(s.page1);
  if( pzErr ){
    *pzErr = s.zErr;
  }else{
    sqlite3_free(s.zErr);
  }
  return s.rcErr;
}   

#ifdef SCRUB_STANDALONE
/* The main() routine when this utility is run as a stand-alone program */
int main(int argc, char **argv){
  char *zErr = 0;
  int rc;
  if( argc!=3 ){
    fprintf(stderr,"Usage: %s SOURCE DESTINATION\n", argv[0]);
    exit(1);
  }
  rc = sqlite3_scrub_backup(argv[1], argv[2], &zErr);
  if( rc==SQLITE_NOMEM ){
    fprintf(stderr, "%s: out of memory\n", argv[0]);
    exit(1);
  }
  if( zErr ){
    fprintf(stderr, "%s: %s\n", argv[0], zErr);
    sqlite3_free(zErr);
    exit(1);
  }
  return 0;
}
#endif
