/*
** 2005 July 8
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code associated with the ANALYZE command.
**
** @(#) $Id: analyze.c,v 1.1 2005/07/08 12:13:05 drh Exp $
*/
#ifndef SQLITE_OMIT_ANALYZE
#include "sqliteInt.h"

/*
** Generate code for the ANALYZE command
**
**        ANALYZE                            -- 1
**        ANALYZE  <database >               -- 2
**        ANALYZE  ?<database>.?<tablename>  -- 3
**
** Form 1 causes all indices in all attached databases to be analyzed.
** Form 2 analyzes all indices the single database named.
** Form 3 analyzes all indices associated with the named table.
*/
void sqlite3Analyze(Parse *pParse, Token *pName1, Token *pName2){
}


#endif /* SQLITE_OMIT_ANALYZE */
