/*
** Copyright (c) 1999, 2000 D. Richard Hipp
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
** This file defines the interface to the database backend (Dbbe).
**
** The database backend is designed to be as general as possible
** so that it can easily be replaced by a different backend.
** This library was originally designed to support the following
** backends: GDBM, NDBM, SDBM, Berkeley DB.
**
** $Id: dbbe.h,v 1.7 2000/08/17 09:50:00 drh Exp $
*/
#ifndef _SQLITE_DBBE_H_
#define _SQLITE_DBBE_H_
#include <stdio.h>

/*
** The database backend supports two opaque structures.  A Dbbe is
** a context for the entire set of tables forming a complete
** database.  A DbbeCursor is a pointer into a single single table.
**
** Note that at this level, the term "table" can mean either an
** SQL table or an SQL index.  In this module, a table stores a
** single arbitrary-length key and corresponding arbitrary-length
** data.  The differences between tables and indices, and the
** segregation of data into various fields or columns is handled
** by software at higher layers.
**
** The DbbeCursor structure holds some state information, such as
** the key and data from the last retrieval.  For this reason, 
** the backend must allow the creation of multiple independent
** DbbeCursor structures for each table in the database.
*/
typedef struct Dbbe Dbbe;
typedef struct DbbeCursor DbbeCursor;

/*
** The 18 interface routines.
*/

/* Open a complete database */
Dbbe *sqliteDbbeOpen(const char *zName, int write, int create, char **pzErr);

/* Close the whole database. */
void sqliteDbbeClose(Dbbe*);

/* Open a cursor into particular file of a previously opened database.
** Create the file if it doesn't already exist and writeable!=0.  zName
** is the base name of the file to be opened.  This routine will add
** an appropriate path and extension to the filename to locate the 
** actual file.
**
** If zName is 0 or "", then a temporary file is created that
** will be deleted when closed.
*/
int sqliteDbbeOpenCursor(Dbbe*, const char *zName, int writeable, DbbeCursor**);

/* Delete a table from the database */
void sqliteDbbeDropTable(Dbbe*, const char *zTableName);

/* Reorganize a table to speed access or reduce its disk usage */
int sqliteDbbeReorganizeTable(Dbbe*, const char *zTableName);

/* Close a cursor */
void sqliteDbbeCloseCursor(DbbeCursor*);

/* Fetch an entry from a table with the given key.  Return 1 if
** successful and 0 if no such entry exists.
*/
int sqliteDbbeFetch(DbbeCursor*, int nKey, char *pKey);

/* Return 1 if the given key is already in the table.  Return 0
** if it is not.
*/
int sqliteDbbeTest(DbbeCursor*, int nKey, char *pKey);

/* Retrieve the key or data used for the last fetch.  Only size
** bytes are read beginning with the offset-th byte.  The return
** value is the actual number of bytes read.
*/
int sqliteDbbeCopyKey(DbbeCursor*, int offset, int size, char *zBuf);
int sqliteDbbeCopyData(DbbeCursor*, int offset, int size, char *zBuf);

/* Retrieve the key or data.  The result is ephemeral.  In other words,
** the result is stored in a buffer that might be overwritten on the next
** call to any DBBE routine.  If the results are needed for longer than
** that, you must make a copy.
*/
char *sqliteDbbeReadKey(DbbeCursor*, int offset);
char *sqliteDbbeReadData(DbbeCursor*, int offset);

/* Return the length of the most recently fetched key or data. */
int sqliteDbbeKeyLength(DbbeCursor*);
int sqliteDbbeDataLength(DbbeCursor*);

/* Retrieve the next entry in the table.  The first key is retrieved
** the first time this routine is called, or after a call to
** sqliteDbbeRewind().  The return value is 1 if there is another
** entry, or 0 if there are no more entries. */
int sqliteDbbeNextKey(DbbeCursor*);

/* Make it so that the next call to sqliteDbbeNextKey() returns
** the first entry of the table. */
int sqliteDbbeRewind(DbbeCursor*);

/* Get a new integer key for this table. */
int sqliteDbbeNew(DbbeCursor*);

/* Write an entry into a table.  If another entry already exists with
** the same key, the old entry is discarded first.
*/
int sqliteDbbePut(DbbeCursor*, int nKey, char *pKey, int nData, char *pData);

/* Remove an entry from the table */
int sqliteDbbeDelete(DbbeCursor*, int nKey, char *pKey);

/* Open a file suitable for temporary storage */
int sqliteDbbeOpenTempFile(Dbbe*, FILE**);

/* Close a temporary file */
void sqliteDbbeCloseTempFile(Dbbe *, FILE *);

#endif /* defined(_SQLITE_DBBE_H_) */
