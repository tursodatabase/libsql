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
** $Id: dbbe.c,v 1.22 2001/01/13 14:34:06 drh Exp $
*/
#include "sqliteInt.h"

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
** Open a temporary file.  The file should be deleted when closed.
**
** Note that we can't use the old Unix trick of opening the file
** and then immediately unlinking the file.  That works great
** under Unix, but fails when we try to port to Windows.
*/
int sqliteDbbeOpenTempFile(Dbbe *pBe, FILE **ppFile){
  char *zFile;         /* Full name of the temporary file */
  char zBuf[50];       /* Base name of the temporary file */
  int i;               /* Loop counter */
  int limit;           /* Prevent an infinite loop */
  int rc = SQLITE_OK;  /* Value returned by this function */
  char *zDir;          /* Directory to hold the file */

  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]==0 ) break;
  }
  if( i>=pBe->nTemp ){
    pBe->nTemp++;
    pBe->apTemp = sqliteRealloc(pBe->apTemp, pBe->nTemp*sizeof(FILE*) );
    pBe->azTemp = sqliteRealloc(pBe->azTemp, pBe->nTemp*sizeof(char*) );
  }
  if( pBe->apTemp==0 ){
    *ppFile = 0;
    return SQLITE_NOMEM;
  }
  limit = 4;
  zFile = 0;
  zDir = pBe->zDir;
  if( zDir==0 ){
    zDir = "./";
  }
  do{
    sqliteRandomName(zBuf, "/_temp_file_");
    sqliteFree(zFile);
    zFile = 0;
    sqliteSetString(&zFile, zDir, zBuf, 0);
  }while( access(zFile,0)==0 && limit-- >= 0 );
  *ppFile = pBe->apTemp[i] = fopen(zFile, "w+");
  if( pBe->apTemp[i]==0 ){
    rc = SQLITE_ERROR;
    sqliteFree(zFile);
    pBe->azTemp[i] = 0;
  }else{
    pBe->azTemp[i] = zFile;
  }
  return rc;
}

/*
** Close a temporary file opened using sqliteGdbmOpenTempFile()
*/
void sqliteDbbeCloseTempFile(Dbbe *pBe, FILE *f){
  int i;
  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]==f ){
      unlink(pBe->azTemp[i]);
      sqliteFree(pBe->azTemp[i]);
      pBe->apTemp[i] = 0;
      pBe->azTemp[i] = 0;
      break;
    }
  }
  fclose(f);
}

/*
** Close all temporary files that happen to still be open.
** This routine is called when the database is being closed.
*/
void sqliteDbbeCloseAllTempFiles(Dbbe *pBe){
  int i;
  for(i=0; i<pBe->nTemp; i++){
    if( pBe->apTemp[i]!=0 ){
      unlink(pBe->azTemp[i]);
      fclose(pBe->apTemp[i]);
      sqliteFree(pBe->azTemp[i]);
      pBe->apTemp[i] = 0;
      pBe->azTemp[i] = 0;
      break;
    }
  }
  sqliteFree(pBe->azTemp);
  sqliteFree(pBe->apTemp);
}
