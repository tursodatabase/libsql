/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This header file defines the interface that the sqlite page cache
** subsystem.  The page cache subsystem reads and writes a file a page
** at a time and provides a journal for rollback.
**
** @(#) $Id: pager.h,v 1.35 2004/06/26 08:38:25 danielk1977 Exp $
*/

/*
** The size of a page.
**
** You can change this value to another (reasonable) value you want.
** It need not be a power of two, though the interface to the disk
** will likely be faster if it is.
**
** Experiments show that a page size of 1024 gives the best speed
** for common usages.  The speed differences for different sizes
** such as 512, 2048, 4096, an so forth, is minimal.  Note, however,
** that changing the page size results in a completely imcompatible
** file format.
*/
#ifndef SQLITE_PAGE_SIZE
#define SQLITE_PAGE_SIZE 1024
#endif

/*
** Number of extra bytes of data allocated at the end of each page and
** stored on disk but not used by the higher level btree layer.  Changing
** this value results in a completely incompatible file format.
*/
#ifndef SQLITE_PAGE_RESERVE
#define SQLITE_PAGE_RESERVE 0
#endif

/*
** The total number of usable bytes stored on disk for each page.
** The usable bytes come at the beginning of the page and the reserve
** bytes come at the end.
*/
#define SQLITE_USABLE_SIZE (SQLITE_PAGE_SIZE-SQLITE_PAGE_RESERVE)

/*
** Maximum number of pages in one database.
*/
#define SQLITE_MAX_PAGE 1073741823

/*
** The type used to represent a page number.  The first page in a file
** is called page 1.  0 is used to represent "not a page".
*/
typedef unsigned int Pgno;

/*
** Each open file is managed by a separate instance of the "Pager" structure.
*/
typedef struct Pager Pager;

/*
** See source code comments for a detailed description of the following
** routines:
*/
int sqlite3pager_open(Pager **ppPager, const char *zFilename,
                     int nPage, int nExtra, int useJournal,
                     void *pBusyHandler);
void sqlite3pager_set_destructor(Pager*, void(*)(void*,int));
void sqlite3pager_set_reiniter(Pager*, void(*)(void*,int));
void sqlite3pager_set_cachesize(Pager*, int);
int sqlite3pager_close(Pager *pPager);
int sqlite3pager_get(Pager *pPager, Pgno pgno, void **ppPage);
void *sqlite3pager_lookup(Pager *pPager, Pgno pgno);
int sqlite3pager_ref(void*);
int sqlite3pager_unref(void*);
Pgno sqlite3pager_pagenumber(void*);
int sqlite3pager_write(void*);
int sqlite3pager_iswriteable(void*);
int sqlite3pager_overwrite(Pager *pPager, Pgno pgno, void*);
int sqlite3pager_pagecount(Pager*);
int sqlite3pager_truncate(Pager*,Pgno);
int sqlite3pager_begin(void*);
int sqlite3pager_commit(Pager*);
int sqlite3pager_sync(Pager*,const char *zMaster);
int sqlite3pager_rollback(Pager*);
int sqlite3pager_isreadonly(Pager*);
int sqlite3pager_stmt_begin(Pager*);
int sqlite3pager_stmt_commit(Pager*);
int sqlite3pager_stmt_rollback(Pager*);
void sqlite3pager_dont_rollback(void*);
void sqlite3pager_dont_write(Pager*, Pgno);
int *sqlite3pager_stats(Pager*);
void sqlite3pager_set_safety_level(Pager*,int);
const char *sqlite3pager_filename(Pager*);
const char *sqlite3pager_dirname(Pager*);
const char *sqlite3pager_journalname(Pager*);
int sqlite3pager_rename(Pager*, const char *zNewName);
void sqlite3pager_set_codec(Pager*,void(*)(void*,void*,Pgno,int),void*);

#ifdef SQLITE_DEBUG
int sqlite3pager_lockstate(Pager*);
#endif

#ifdef SQLITE_TEST
void sqlite3pager_refdump(Pager*);
int pager3_refinfo_enable;
#endif
