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
** $Id: dbbe.h,v 1.14 2001/08/19 18:19:46 drh Exp $
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
typedef struct DbbeMethods DbbeMethods;

/*
** Open a complete database.
**
** If the database name begins with "gdbm:" the GDBM driver is used.
** If the name begins with "memory:" the in-memory driver is used.
** The default driver is GDBM.
*/
Dbbe *sqliteDbbeOpen(const char *zName, int write, int create, char **pzErr);

/*
** Each of the various SQLite backends defines a set of methods for
** accessing the database.  Pointers to the methods are contained in
** an instance of the following structure.  A pointer to a static instance
** of this structure is assigned to the Dbbe structure that sqlileDbbeOpen
** returns.
*/
struct DbbeMethods {
  /* Close the whole database. */
  void (*Close)(Dbbe*);

  /* Open a cursor into a particular table of a previously opened database.
  ** Create the table if it doesn't already exist and writeable!=0.  zName
  ** is the base name of the table to be opened.  If the database is 
  ** implement as one file per table, then this routine will add an
  ** appropriate path and extension to the table name to locate the 
  ** actual file.
  **
  ** The intKeyOnly parameter is TRUE if this table will only be accessed
  ** using integer keys.  This parameter allows the database backend to
  ** use a faster algorithm for the special case of integer keys, if it
  ** wants to.
  **
  ** If zName is 0 or "", then a temporary table is created that
  ** will be deleted when closed.
  */
  int (*OpenCursor)(Dbbe*, const char *zName, int writeable, 
                    int intKeyOnly, DbbeCursor**);

  /* Delete a table from the database */
  void (*DropTable)(Dbbe*, const char *zTableName);

  /* Reorganize a table to speed access or reduce its disk usage */
  int (*ReorganizeTable)(Dbbe*, const char *zTableName);

  /* Close a cursor */
  void (*CloseCursor)(DbbeCursor*);

  /* Fetch an entry from a table with the given key.  Return 1 if
  ** successful and 0 if no such entry exists.
  */
  int (*Fetch)(DbbeCursor*, int nKey, char *pKey);

  /* Return 1 if the given key is already in the table.  Return 0
  ** if it is not.
  */
  int (*Test)(DbbeCursor*, int nKey, char *pKey);

  /* Retrieve the key or data used for the last fetch.  Only size
  ** bytes are read beginning with the offset-th byte.  The return
  ** value is the actual number of bytes read.
  */
  int (*CopyKey)(DbbeCursor*, int offset, int size, char *zBuf);
  int (*CopyData)(DbbeCursor*, int offset, int size, char *zBuf);

  /* Retrieve the key or data.  The result is ephemeral.  In other words,
  ** the result is stored in a buffer that might be overwritten on the next
  ** call to any DBBE routine.  If the results are needed for longer than
  ** that, you must make a copy.
  */
  char *(*ReadKey)(DbbeCursor*, int offset);
  char *(*ReadData)(DbbeCursor*, int offset);

  /* Return the length of the most recently fetched key or data. */
  int (*KeyLength)(DbbeCursor*);
  int (*DataLength)(DbbeCursor*);

  /* Retrieve the next entry in the table.  The first key is retrieved
  ** the first time this routine is called, or after a call to
  ** Dbbe.Rewind().  The return value is 1 if there is another
  ** entry, or 0 if there are no more entries. */
  int (*NextKey)(DbbeCursor*);

  /* Make it so that the next call to Dbbe.NextKey() returns
  ** the first entry of the table. */
  int (*Rewind)(DbbeCursor*);

  /* Get a new integer key for this table. */
  int (*New)(DbbeCursor*);

  /* Write an entry into a table.  If another entry already exists with
  ** the same key, the old entry is discarded first.
  */
  int (*Put)(DbbeCursor*, int nKey, char *pKey, int nData, char *pData);

  /* Remove an entry from the table */
  int (*Delete)(DbbeCursor*, int nKey, char *pKey);

  /* Begin a transaction. */
  int (*BeginTransaction)(Dbbe*);

  /* Commit a transaction. */
  int (*Commit)(Dbbe*);

  /* Rollback a transaction. */
  int (*Rollback)(Dbbe*);

  /* Begin searching an index where the key is given. */
  int (*BeginIndex)(DbbeCursor*, int nKey, char *pKey);

  /* Return the integer key for the next index entry, or return 0 if
  ** there are no more index entries. */
  int (*NextIndex)(DbbeCursor*);

  /* Add a new index entry to the file.  The key and record number are
  ** given. */
  int (*PutIndex)(DbbeCursor*, int nKey, char *pKey, int recno);

  /* Delete an index entry from the file.  The key and record number are
  ** given. */
  int (*DeleteIndex)(DbbeCursor*, int nKey, char *pKey, int recno);
};

/*
** This is the structure returned by sqliteDbbeOpen().  It contains
** information common to all the different backend drivers.
**
** The information in this structure (with the exception the method
** pointers in the Dbbe.x field) is intended to be visible to
** the backend drivers only.  Users should not access or modify
** this structure in any way other than the read the method pointers
** in Dbbe.x.
*/
struct Dbbe {
  struct DbbeMethods *x; /* Backend-specific methods for database access */
  /* There used to be other information here, but it has since
  ** been removed.  We'll keep the same design, though, in case we
  ** ever want to add some new fields in the future. */
};

#endif /* defined(_SQLITE_DBBE_H_) */
