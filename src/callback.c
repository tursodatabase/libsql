/*
** 2005 May 23 
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains functions used to access the internal hash tables
** of user defined functions and collation sequences.
**
** $Id: callback.c,v 1.1 2005/05/24 12:01:02 danielk1977 Exp $
*/

#include "sqliteInt.h"

/*
** Locate and return an entry from the db.aCollSeq hash table. If the entry
** specified by zName and nName is not found and parameter 'create' is
** true, then create a new entry. Otherwise return NULL.
**
** Each pointer stored in the sqlite3.aCollSeq hash table contains an
** array of three CollSeq structures. The first is the collation sequence
** prefferred for UTF-8, the second UTF-16le, and the third UTF-16be.
**
** Stored immediately after the three collation sequences is a copy of
** the collation sequence name. A pointer to this string is stored in
** each collation sequence structure.
*/
static CollSeq * findCollSeqEntry(
  sqlite3 *db,
  const char *zName,
  int nName,
  int create
){
  CollSeq *pColl;
  if( nName<0 ) nName = strlen(zName);
  pColl = sqlite3HashFind(&db->aCollSeq, zName, nName);

  if( 0==pColl && create ){
    pColl = sqliteMalloc( 3*sizeof(*pColl) + nName + 1 );
    if( pColl ){
      CollSeq *pDel = 0;
      pColl[0].zName = (char*)&pColl[3];
      pColl[0].enc = SQLITE_UTF8;
      pColl[1].zName = (char*)&pColl[3];
      pColl[1].enc = SQLITE_UTF16LE;
      pColl[2].zName = (char*)&pColl[3];
      pColl[2].enc = SQLITE_UTF16BE;
      memcpy(pColl[0].zName, zName, nName);
      pColl[0].zName[nName] = 0;
      pDel = sqlite3HashInsert(&db->aCollSeq, pColl[0].zName, nName, pColl);

      /* If a malloc() failure occured in sqlite3HashInsert(), it will 
      ** return the pColl pointer to be deleted (because it wasn't added
      ** to the hash table).
      */
      assert( !pDel || (sqlite3_malloc_failed && pDel==pColl) );
      sqliteFree(pDel);
    }
  }
  return pColl;
}

/*
** Parameter zName points to a UTF-8 encoded string nName bytes long.
** Return the CollSeq* pointer for the collation sequence named zName
** for the encoding 'enc' from the database 'db'.
**
** If the entry specified is not found and 'create' is true, then create a
** new entry.  Otherwise return NULL.
*/
CollSeq *sqlite3FindCollSeq(
  sqlite3 *db,
  u8 enc,
  const char *zName,
  int nName,
  int create
){
  CollSeq *pColl = findCollSeqEntry(db, zName, nName, create);
  assert( SQLITE_UTF8==1 && SQLITE_UTF16LE==2 && SQLITE_UTF16BE==3 );
  assert( enc>=SQLITE_UTF8 && enc<=SQLITE_UTF16BE );
  if( pColl ) pColl += enc-1;
  return pColl;
}

/*
** Locate a user function given a name, a number of arguments and a flag
** indicating whether the function prefers UTF-16 over UTF-8.  Return a
** pointer to the FuncDef structure that defines that function, or return
** NULL if the function does not exist.
**
** If the createFlag argument is true, then a new (blank) FuncDef
** structure is created and liked into the "db" structure if a
** no matching function previously existed.  When createFlag is true
** and the nArg parameter is -1, then only a function that accepts
** any number of arguments will be returned.
**
** If createFlag is false and nArg is -1, then the first valid
** function found is returned.  A function is valid if either xFunc
** or xStep is non-zero.
**
** If createFlag is false, then a function with the required name and
** number of arguments may be returned even if the eTextRep flag does not
** match that requested.
*/
FuncDef *sqlite3FindFunction(
  sqlite3 *db,       /* An open database */
  const char *zName, /* Name of the function.  Not null-terminated */
  int nName,         /* Number of characters in the name */
  int nArg,          /* Number of arguments.  -1 means any number */
  u8 enc,            /* Preferred text encoding */
  int createFlag     /* Create new entry if true and does not otherwise exist */
){
  FuncDef *p;         /* Iterator variable */
  FuncDef *pFirst;    /* First function with this name */
  FuncDef *pBest = 0; /* Best match found so far */
  int bestmatch = 0;  


  assert( enc==SQLITE_UTF8 || enc==SQLITE_UTF16LE || enc==SQLITE_UTF16BE );
  if( nArg<-1 ) nArg = -1;

  pFirst = (FuncDef*)sqlite3HashFind(&db->aFunc, zName, nName);
  for(p=pFirst; p; p=p->pNext){
    /* During the search for the best function definition, bestmatch is set
    ** as follows to indicate the quality of the match with the definition
    ** pointed to by pBest:
    **
    ** 0: pBest is NULL. No match has been found.
    ** 1: A variable arguments function that prefers UTF-8 when a UTF-16
    **    encoding is requested, or vice versa.
    ** 2: A variable arguments function that uses UTF-16BE when UTF-16LE is
    **    requested, or vice versa.
    ** 3: A variable arguments function using the same text encoding.
    ** 4: A function with the exact number of arguments requested that
    **    prefers UTF-8 when a UTF-16 encoding is requested, or vice versa.
    ** 5: A function with the exact number of arguments requested that
    **    prefers UTF-16LE when UTF-16BE is requested, or vice versa.
    ** 6: An exact match.
    **
    ** A larger value of 'matchqual' indicates a more desirable match.
    */
    if( p->nArg==-1 || p->nArg==nArg || nArg==-1 ){
      int match = 1;          /* Quality of this match */
      if( p->nArg==nArg || nArg==-1 ){
        match = 4;
      }
      if( enc==p->iPrefEnc ){
        match += 2;
      }
      else if( (enc==SQLITE_UTF16LE && p->iPrefEnc==SQLITE_UTF16BE) ||
               (enc==SQLITE_UTF16BE && p->iPrefEnc==SQLITE_UTF16LE) ){
        match += 1;
      }

      if( match>bestmatch ){
        pBest = p;
        bestmatch = match;
      }
    }
  }

  /* If the createFlag parameter is true, and the seach did not reveal an
  ** exact match for the name, number of arguments and encoding, then add a
  ** new entry to the hash table and return it.
  */
  if( createFlag && bestmatch<6 && 
      (pBest = sqliteMalloc(sizeof(*pBest)+nName+1)) ){
    pBest->nArg = nArg;
    pBest->pNext = pFirst;
    pBest->zName = (char*)&pBest[1];
    pBest->iPrefEnc = enc;
    memcpy(pBest->zName, zName, nName);
    pBest->zName[nName] = 0;
    if( pBest==sqlite3HashInsert(&db->aFunc,pBest->zName,nName,(void*)pBest) ){
      sqliteFree(pBest);
      return 0;
    }
  }

  if( pBest && (pBest->xStep || pBest->xFunc || createFlag) ){
    return pBest;
  }
  return 0;
}
