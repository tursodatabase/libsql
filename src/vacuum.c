/*
** 2003 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used to implement the VACUUM command.
**
** Most of the code in this file may be omitted by defining the
** SQLITE_OMIT_VACUUM macro.
**
** $Id: vacuum.c,v 1.1 2003/04/06 21:08:24 drh Exp $
*/
#include "sqliteInt.h"


/*
** The non-standard VACUUM command is used to clean up the database,
** collapse free space, etc.  It is modelled after the VACUUM command
** in PostgreSQL.
**
** In version 1.0.x of SQLite, the VACUUM command would call
** gdbm_reorganize() on all the database tables.  But beginning
** with 2.0.0, SQLite no longer uses GDBM so this command has
** become a no-op.
*/
void sqliteVacuum(Parse *pParse, Token *pTableName){
#ifndef SQLITE_OMIT_VACUUM
  /* Do nothing */
#endif
}
