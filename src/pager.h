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
** @(#) $Id: pager.h,v 1.1 2001/04/03 16:53:22 drh Exp $
*/
#include "sqliteInt.h"

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

int sqlite_pager_open(Pager **ppPager, const char *zFilename);
int sqlite_pager_close(Pager *pPager);
int sqlite_pager_get(Pager *pPager, Pgno pgno, void **ppPage);
int sqlite_pager_unref(void*);
Pgno sqlite_pager_pagenumber(void*);
int sqlite_pager_write(void*);
int sqlite_pager_pagecount(Pager*);
int sqlite_pager_commit(Pager*);
int sqlite_pager_rollback(Pager*);
