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
** $Id: vacuum.c,v 1.2 2003/04/15 01:19:49 drh Exp $
*/
#include "sqliteInt.h"

#define SQLITE_OMIT_VACUUM 1

/*
** A structure for holding a dynamic string - a string that can grow
** without bound.
*/
typedef struct dynStr dynStr;
struct dynStr {
  char *z;        /* Text of the string in space obtained from sqliteMalloc() */
  int nAlloc;     /* Amount of space allocated to z[] */
  int nUsed;      /* Next unused slot in z[] */
};

#ifndef SQLITE_OMIT_VACUUM
/*
** Append text to a dynamic string
*/
static void appendText(dynStr *p, const char *zText, int nText){
  if( nText<0 ) nText = strlen(zText);
  if( p->z==0 || p->nUsed + nText + 1 >= p->nAlloc ){
    char *zNew;
    p->nAlloc = p->nUsed + nText + 1000;
    zNew = sqliteRealloc(p->z, p->nAlloc);
    if( zNew==0 ){
      sqliteFree(p->z);
      memset(p, 0, sizeof(*p));
      return;
    }
    p->z = zNew;
  }
  memcpy(&p->z[p->nUsed], zText, nText+1);
  p->nUsed += nText;
}

/*
** Append text to a dynamic string, having first put the text in quotes.
*/
static void appendQuoted(dynStr *p, const char *zText){
  int i, j;
  appendText(p, "'", 1);
  for(i=j=0; zText[i]; i++){
    if( zText[i]='\'' ){
      appendText(p, &zText[j], i-j+1);
      j = i + 1;
      appendText(p, "'", 1);
    }
  }
  if( j<i ){
    appendText(p, &zText[j], i-j);
  }
  appendText(p, "'", 1);
}

/*
** This is an SQLite callback that is invoked once for each row in
** the SQLITE_MASTER table of the database being vacuumed.  The three
** parameters are the type of entry, the name of the entry, and the SQL
** text for the entry.
**
** Append SQL text to the dynStr that will make a copy of the structure
** identified by this row.
*/
static int vacuumCallback(void *pArg, int argc, char **argv, char **NotUsed){
  dynStr *p = (dynStr*)pArg;
  assert( argc==3 );
  assert( argv[0]!=0 );
  assert( argv[1]!=0 );
  assert( argv[2]!=0 );
  appendText(p, argv[2], -1);
  appendText(p, ";\n", 2);
  if( strcmp(argv[0],"table")==0 ){
    appendText(p, "INSERT INTO ", -1);
    appendQuoted(p, argv[1]);
    appendText(p, " SELECT * FROM ", -1);
    appendQuoted(p, argv[1]);
    appendText(p, ";\n");
  }
  return 0;
}

/*
** Generate a random name of 20 character in length.
*/
static void randomName(char *zBuf){
  static const char zChars[] =
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789";
  int i;
  for(i=0; i<20; i++){
    int n = sqliteRandomByte() % (sizeof(zChars)-1);
    zBuf[i] = zChars[n];
  }
}
#endif

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
  const char *zFilename;  /* full pathname of the database file */
  int nFilename;          /* number of characters  in zFilename[] */
  char *zTemp = 0;        /* a temporary file in same directory as zFilename */
  char *zTemp2;           /* Another temp file in the same directory */
  sqlite *dbNew = 0;      /* The new vacuumed database */
  sqlite *dbOld = 0;      /* Alternative connection to original database */
  sqlite *db;             /* The original database */
  int rc;
  char *zErrMsg = 0;
  char *zSql = 0;
  dynStr sStr;

  /* Initial error checks
  */
  if( pParse->explain ){
    return;
  }
  db = pParse->db;
  if( db->flags & SQLITE_InTrans ){
    sqliteErrorMsg(pParse, "cannot VACUUM from within a transaction");
    return;
  }
  memset(&sStr, 0, sizeof(sStr));

  /* Get the full pathname of the database file and create two
  ** temporary filenames in the same directory as the original file.
  */
  zFilename = sqliteBtreeGetFilename(db->aDb[0].pBt);
  if( zFilename==0 ){
    /* This only happens with the in-memory database.  VACUUM is a no-op
    ** there, so just return */
    return;
  }
  nFilename = strlen(zFilename);
  zTemp = sqliteMalloc( 2*(nFilename+40) );
  if( zTemp==0 ) return;
  zTemp2 = &zTemp[nFilename+40];
  strcpy(zTemp, zFilename);
  strcpy(zTemp2, zFilename);
  for(i=0; i<10; i++){
    zTemp[nFilename] = '-';
    randomName(&zTemp[nFilename+1]);
    randomName(&zTemp2[nFilename+1]);
    if( !sqliteOsFileExists(zTemp) && !sqliteOsFileExists(zTemp2) ) break;
  }
  if( i>=10 ){
    sqliteErrorMsg(pParse, "unable to create a temporary database files "
       "in the same directory as the original database");
    goto end_of_vacuum;
  }

  
  dbNew = sqlite_open(zTemp, 0, &zErrMsg);
  if( dbNew==0 ){
    sqliteErrorMsg(pParse, "unable to open a temporary database at %s - %s",
       zTemp, zErrMsg);
    goto end_of_vacuum;
  }
  appendText(&sStr, "ATTACH DATABASE ", -1);
  appendQuoted(&sStr, zFilename);
  appendText(&sStr, " AS orig;\nBEGIN;\n", -1);
  if( execsql(pParse, dbNew, sStr.z) ) goto end_of_vacuum;
  sStr.nUsed = 0;
  rc = sqlite_exec(dbNew, "SELECT type, name, sql FROM sqlite_master "
           "WHERE sql NOT NULL", vacuumCallback, &sStr, &zErrMsg);
  if( rc ){
    sqliteErrorMsg(pParse, "unable to vacuum database - %s", zErrMsg);
    goto end_of_vacuum;
  }
  appendText(&sStr, "COMMIT;\n", -1);
  if( execsql(pParse, dbNew, sStr.z) ) goto end_of_vacuum;


  
end_of_vacuum:
  sqliteFree(zTemp);
  sqliteFree(zSql);
  sqliteFree(sStr.z);
  if( zErrMsg ) sqlite_freemem(zErrMsg);
  if( dbNew ) sqlite_close(dbNew);
#endif
}
