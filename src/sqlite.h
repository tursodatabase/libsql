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
** This header file defines the interface that the sqlite library
** presents to client programs.
**
** @(#) $Id: sqlite.h,v 1.1 2000/05/29 14:26:01 drh Exp $
*/
#ifndef _SQLITE_H_
#define _SQLITE_H_

/*
** Each open sqlite database is represented by an instance of the
** following opaque structure.
*/
typedef struct sqlite sqlite;

/*
** A function to open a new sqlite database.  
**
** If the database does not exist and mode indicates write
** permission, then a new database is created.  If the database
** does not exist and mode does not indicate write permission,
** then the open fails, an error message generated (if errmsg!=0)
** and the function returns 0.
** 
** If mode does not indicates user write permission, then the 
** database is opened read-only.
**
** The Truth:  As currently implemented, all databases are opened
** for writing all the time.  Maybe someday we will provide the
** ability to open a database readonly.  The mode parameters is
** provide in anticipation of that enhancement.
*/
sqlite *sqlite_open(const char *filename, int mode, char **errmsg);

/*
** A function to close the database.
**
** Call this function with a pointer to a structure that was previously
** returned from sqlite_open() and the corresponding database will by closed.
*/
void sqlite_close(sqlite *);

/*
** The type for a callback function.
*/
typedef int (*sqlite_callback)(void*,int,char**, char**);

/*
** A function to executes one or more statements of SQL.
**
** If one or more of the SQL statements are queries, then
** the callback function specified by the 3rd parameter is
** invoked once for each row of the query result.  This callback
** should normally return 0.  If the callback returns a non-zero
** value then the query is aborted, all subsequent SQL statements
** are skipped and the sqlite_exec() function returns the same
** value that the callback returned.
**
** The 4th parameter is an arbitrary pointer that is passed
** to the callback function as its first parameter.
**
** The 2nd parameter to the callback function is the number of
** columns in the query result.  The 3rd parameter is an array
** of string holding the values for each column.  The 4th parameter
** is an array of strings holding the names of each column.
**
** The callback function may be NULL, even for queries.  A NULL
** callback is not an error.  It just means that no callback
** will be invoked.
**
** If an error occurs while parsing or evaluating the SQL (but
** not while executing the callback) then an appropriate error
** message is written into memory obtained from malloc() and
** *errmsg is made to point to that message.  If errmsg==NULL,
** then no error message is ever written.  The return value is
** non-zero if an error occurs.
*/
int sqlite_exec(
  sqlite*,                      /* An open database */
  char *sql,                    /* SQL to be executed */
  sqlite_callback,              /* Callback function */
  void *,                       /* 1st argument to callback function */
  char **errmsg                 /* Error msg written here */
);


/* This function returns true if the given input string comprises
** one or more complete SQL statements.
**
** The algorithm is simple.  If the last token other than spaces
** and comments is a semicolon, then return true.  otherwise return
** false.
*/
int sqlite_complete(const char *sql);

#endif /* _SQLITE_H_ */
