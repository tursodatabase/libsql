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
** $Id: pg.h,v 1.3 2001/01/21 00:58:09 drh Exp $
*/

typedef struct Pgr Pgr;
#define SQLITE_PAGE_SIZE 1024


int sqlitePgOpen(const char *filename, Pgr **pp);
int sqlitePgClose(Pgr*);
int sqlitePgBeginTransaction(Pgr*);
int sqlitePgCommit(Pgr*);
int sqlitePgRollback(Pgr*);
int sqlitePgGet(Pgr*, u32 pgno, void **);
int sqlitePgUnref(void*);
int sqlitePgTouch(void*);
int sqlitePgCount(Pgr*, u32*);
u32 sqlitePgNum(void*);
