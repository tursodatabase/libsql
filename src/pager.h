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
** @(#) $Id: pager.h,v 1.12 2001/11/10 13:51:09 drh Exp $
*/

/*
** The size of one page
**
** You can change this value to another (reasonable) power of two
** such as 512, 2048, 4096, or 8192 and things will still work.  But
** experiments show that a page size of 1024 gives the best speed.
** (The speed differences are minimal.)
*/
#define SQLITE_PAGE_SIZE 1024

/*
** Maximum number of pages in one database.  (This is a limitation of
** imposed by 4GB files size limits.)
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

int sqlitepager_open(Pager **ppPager,const char *zFilename,int nPage,int nEx);
void sqlitepager_set_destructor(Pager*, void(*)(void*));
void sqlitepager_set_cachesize(Pager*, int);
int sqlitepager_close(Pager *pPager);
int sqlitepager_get(Pager *pPager, Pgno pgno, void **ppPage);
void *sqlitepager_lookup(Pager *pPager, Pgno pgno);
int sqlitepager_ref(void*);
int sqlitepager_unref(void*);
Pgno sqlitepager_pagenumber(void*);
int sqlitepager_write(void*);
int sqlitepager_iswriteable(void*);
int sqlitepager_pagecount(Pager*);
int sqlitepager_commit(Pager*);
int sqlitepager_rollback(Pager*);
int sqlitepager_isreadonly(Pager*);
int *sqlitepager_stats(Pager*);

#ifdef SQLITE_TEST
void sqlitepager_refdump(Pager*);
int pager_refinfo_enable;
#endif
