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
** This file contains code to implement the database backend (DBBE)
** for sqlite.  The database backend is the interface between
** sqlite and the code that does the actually reading and writing
** of information to the disk.
**
** This file uses GDBM as the database backend.  It should be
** relatively simple to convert to a different database such
** as NDBM, SDBM, or BerkeleyDB.
**
** $Id: dbbe.c,v 1.26 2001/04/04 21:22:14 drh Exp $
*/
#include "sqliteInt.h"
#include <unistd.h>
#include <ctype.h>

/*
** This routine opens a new database.  It looks at the first
** few characters of the database name to try to determine what
** kind of database to open.  If the first characters are "gdbm:",
** then it uses the GDBM driver.  If the first few characters are
** "memory:" then it uses the in-memory driver.  If there is no
** match, the default to the GDBM driver.
**
** If successful, a pointer to the Dbbe structure is returned.
** If there are errors, an appropriate error message is left
** in *pzErrMsg and NULL is returned.
*/
Dbbe *sqliteDbbeOpen(
  const char *zName,     /* The name of the database */
  int writeFlag,         /* True if we will be writing to the database */
  int createFlag,        /* True to create database if it doesn't exist */
  char **pzErrMsg        /* Write error messages (if any) here */
){
  extern Dbbe *sqliteGdbmOpen(const char*,int,int,char**);
  if( strncmp(zName, "gdbm:", 5)==0 ){
    return sqliteGdbmOpen(&zName[5], writeFlag, createFlag, pzErrMsg);
  }
  if( strncmp(zName, "memory:", 7)==0 ){
    extern Dbbe *sqliteMemOpen(const char*,int,int,char**);
    return sqliteMemOpen(&zName[7], writeFlag, createFlag, pzErrMsg);
  }
  return sqliteGdbmOpen(zName, writeFlag, createFlag, pzErrMsg);
}

/*
** Translate the name of an SQL table (or index) into the name 
** of a file that holds the key/data pairs for that table or
** index.  Space to hold the filename is obtained from
** sqliteMalloc() and must be freed by the calling function.
**
** zDir is the name of the directory in which the file should
** be located.  zSuffix is the filename extension to use for
** the file.
*/
char *sqliteDbbeNameToFile(
  const char *zDir,      /* Directory containing the file */
  const char *zTable,    /* Name of the SQL table that the file contains */
  const char *zSuffix    /* Suffix for the file.  Includes the "." */
){
  char *zFile = 0;
  int i, k, c;
  int nChar = 0;

  for(i=0; (c = zTable[i])!=0; i++){
    if( !isalnum(c) && c!='_' ){
      nChar += 3;
    }else{
      nChar ++;
    }
  }
  nChar += strlen(zDir) + strlen(zSuffix) + 2;
  zFile = sqliteMalloc( nChar );
  if( zFile==0 ) return 0;
  for(i=0; (c = zDir[i])!=0; i++){
    zFile[i] = c;
  }
  zFile[i++] = '/';
  for(k=0; (c = zTable[k])!=0; k++){
    if( isupper(c) ){
      zFile[i++] = tolower(c);
    }else if( isalnum(c) || c=='_' ){
      zFile[i++] = c;
    }else{
      zFile[i++] = '~';
      zFile[i++] = "0123456789abcdef"[c & 0xf];
      zFile[i++] = "0123456789abcdef"[(c>>8)&0xf];
    }
  }
  for(k=0; (c = zSuffix[k])!=0; k++){
    zFile[i++] = c;
  }
  zFile[i] = 0;
  return zFile;
}
