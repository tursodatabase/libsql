/*
** Copyright (c) 2001 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** This header file defines the interface that the sqlite page cache
** subsystem.  The page cache subsystem reads and writes a file a page
** at a time and provides a journal for rollback.
**
** @(#) $Id: pager.h,v 1.9 2001/09/14 18:54:09 drh Exp $
*/

/*
** The size of one page
*/
#define SQLITE_PAGE_SIZE 1024

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
