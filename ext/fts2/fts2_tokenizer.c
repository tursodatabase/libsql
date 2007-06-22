
#include "sqlite3.h"
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include "fts2_hash.h"
#include "fts2_tokenizer.h"
#include <assert.h>

/*
** Implementation of the SQL scalar function for accessing the underlying 
** hash table. This function may be called as follows:
**
**   SELECT <function-name>(<key-name>);
**   SELECT <function-name>(<key-name>, <pointer>);
**
** where <function-name> is the name passed as the second argument
** to the sqlite3Fts2InitHashTable() function (e.g. 'fts2_tokenizer').
**
** If the <pointer> argument is specified, it must be a blob value
** containing a pointer to be stored as the hash data corresponding
** to the string <key-name>. If <pointer> is not specified, then
** the string <key-name> must already exist in the has table. Otherwise,
** an error is returned.
**
** Whether or not the <pointer> argument is specified, the value returned
** is a blob containing the pointer stored as the hash data corresponding
** to string <key-name> (after the hash-table is updated, if applicable).
*/
static void scalarFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  fts2Hash *pHash;
  void *pPtr = 0;
  const unsigned char *zName;
  int nName;

  assert( argc==1 || argc==2 );

  pHash = (fts2Hash *)sqlite3_user_data(context);

  zName = sqlite3_value_text(argv[0]);
  nName = sqlite3_value_bytes(argv[0])+1;

  if( argc==2 ){
    void *pOld;
    int n = sqlite3_value_bytes(argv[1]);
    if( n!=sizeof(pPtr) ){
      sqlite3_result_error(context, "argument type mismatch", -1);
      return;
    }
    pPtr = *(void **)sqlite3_value_blob(argv[1]);
    pOld = sqlite3Fts2HashInsert(pHash, (void *)zName, nName, pPtr);
    if( pOld==pPtr ){
      sqlite3_result_error(context, "out of memory", -1);
      return;
    }
  }else{
    pPtr = sqlite3Fts2HashFind(pHash, zName, nName);
    if( !pPtr ){
      char *zErr = sqlite3_mprintf("unknown tokenizer: %s", zName);
      sqlite3_result_error(context, zErr, -1);
      sqlite3_free(zErr);
      return;
    }
  }

  sqlite3_result_blob(context, (void *)&pPtr, sizeof(pPtr), SQLITE_TRANSIENT);
}

#ifdef SQLITE_TEST

#include <tcl.h>

/*
** Implementation of a special SQL scalar function for testing tokenizers 
** designed to be used in concert with the Tcl testing framework. This
** function must be called with two arguments:
**
**   SELECT <function-name>(<key-name>, <input-string>);
**   SELECT <function-name>(<key-name>, <pointer>);
**
** where <function-name> is the name passed as the second argument
** to the sqlite3Fts2InitHashTable() function (e.g. 'fts2_tokenizer')
** concatenated with the string '_test' (e.g. 'fts2_tokenizer_test').
**
** The return value is a string that may be interpreted as a Tcl
** list. For each token in the <input-string>, three elements are
** added to the returned list. The first is the token position, the 
** second is the token text (folded, stemmed, etc.) and the third is the
** substring of <input-string> associated with the token. For example, 
** using the built-in "simple" tokenizer:
**
**   SELECT fts_tokenizer_test('simple', 'I don't see how');
**
** will return the string:
**
**   "{0 i I 1 dont don't 2 see see 3 how how}"
**   
*/
static void testFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  fts2Hash *pHash;
  sqlite3_tokenizer_module *p;
  sqlite3_tokenizer *pTokenizer = 0;
  sqlite3_tokenizer_cursor *pCsr = 0;

  const char *zErr = 0;

  const char *zName;
  int nName;
  const char *zInput;
  int nInput;

  const char *zToken;
  int nToken;
  int iStart;
  int iEnd;
  int iPos;

  Tcl_Obj *pRet;

  assert( argc==2 );

  nName = sqlite3_value_bytes(argv[0]);
  zName = (const char *)sqlite3_value_text(argv[0]);
  nInput = sqlite3_value_bytes(argv[1]);
  zInput = (const char *)sqlite3_value_text(argv[1]);

  pHash = (fts2Hash *)sqlite3_user_data(context);
  p = (sqlite3_tokenizer_module *)sqlite3Fts2HashFind(pHash, zName, nName+1);

  if( !p ){
    char *zErr = sqlite3_mprintf("unknown tokenizer: %s", zName);
    sqlite3_result_error(context, zErr, -1);
    sqlite3_free(zErr);
    return;
  }

  pRet = Tcl_NewObj();
  Tcl_IncrRefCount(pRet);

  if( SQLITE_OK!=p->xCreate(0, 0, &pTokenizer) ){
    zErr = "error in xCreate()";
    goto finish;
  }
  pTokenizer->pModule = p;
  if( SQLITE_OK!=p->xOpen(pTokenizer, zInput, nInput, &pCsr) ){
    zErr = "error in xOpen()";
    goto finish;
  }
  pCsr->pTokenizer = pTokenizer;

  while( SQLITE_OK==p->xNext(pCsr, &zToken, &nToken, &iStart, &iEnd, &iPos) ){
    Tcl_ListObjAppendElement(0, pRet, Tcl_NewIntObj(iPos));
    Tcl_ListObjAppendElement(0, pRet, Tcl_NewStringObj(zToken, nToken));
    zToken = &zInput[iStart];
    nToken = iEnd-iStart;
    Tcl_ListObjAppendElement(0, pRet, Tcl_NewStringObj(zToken, nToken));
  }

  if( SQLITE_OK!=p->xClose(pCsr) ){
    zErr = "error in xClose()";
    goto finish;
  }
  if( SQLITE_OK!=p->xDestroy(pTokenizer) ){
    zErr = "error in xDestroy()";
    goto finish;
  }

finish:
  if( zErr ){
    sqlite3_result_error(context, zErr, -1);
  }else{
    sqlite3_result_text(context, Tcl_GetString(pRet), -1, SQLITE_TRANSIENT);
  }
  Tcl_DecrRefCount(pRet);
}
#endif

/*
** Set up SQL objects in database db used to access the contents of
** the hash table pointed to by argument pHash. The hash table must
** been initialised to use string keys, and to take a private copy 
** of the key when a value is inserted. i.e. by a call similar to:
**
**    sqlite3Fts2HashInit(pHash, FTS2_HASH_STRING, 1);
**
** This function adds a scalar function (see header comment above
** scalarFunc() in this file for details) and, if ENABLE_TABLE is
** defined at compilation time, a temporary virtual table (see header 
** comment above struct HashTableVtab) to the database schema. Both 
** provide read/write access to the contents of *pHash.
**
** The third argument to this function, zName, is used as the name
** of both the scalar and, if created, the virtual table.
*/
int sqlite3Fts2InitHashTable(
  sqlite3 *db, 
  fts2Hash *pHash, 
  const char *zName
){
  int rc;
  void *p = (void *)pHash;
  const int any = SQLITE_ANY;
  char *zTest = 0;

#ifdef SQLITE_TEST
  zTest = sqlite3_mprintf("%s_test", zName);
  if( !zTest ){
    return SQLITE_NOMEM;
  }
#endif

  if( (rc = sqlite3_create_function(db, zName, 1, any, p, scalarFunc, 0, 0))
   || (rc = sqlite3_create_function(db, zName, 2, any, p, scalarFunc, 0, 0))
#ifdef SQLITE_TEST
   || (rc = sqlite3_create_function(db, zTest, 2, any, p, testFunc, 0, 0))
#endif
  );

  sqlite3_free(zTest);
  return rc;
}

