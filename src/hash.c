/*
** 2001 September 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This is the implementation of generic hash-tables
** used in SQLite.
**
** $Id: hash.c,v 1.33 2009/01/09 01:12:28 drh Exp $
*/
#include "sqliteInt.h"
#include <assert.h>

/* Turn bulk memory into a hash table object by initializing the
** fields of the Hash structure.
**
** "pNew" is a pointer to the hash table that is to be initialized.
** "copyKey" is true if the hash table should make its own private
** copy of keys and false if it should just use the supplied pointer.
*/
void sqlite3HashInit(Hash *pNew, int copyKey){
  assert( pNew!=0 );
  pNew->copyKey = copyKey!=0;
  pNew->first = 0;
  pNew->count = 0;
  pNew->htsize = 0;
  pNew->ht = 0;
}

/* Remove all entries from a hash table.  Reclaim all memory.
** Call this routine to delete a hash table or to reset a hash table
** to the empty state.
*/
void sqlite3HashClear(Hash *pH){
  HashElem *elem;         /* For looping over all elements of the table */

  assert( pH!=0 );
  elem = pH->first;
  pH->first = 0;
  sqlite3_free(pH->ht);
  pH->ht = 0;
  pH->htsize = 0;
  while( elem ){
    HashElem *next_elem = elem->next;
    if( pH->copyKey ){
      sqlite3_free(elem->pKey);
    }
    sqlite3_free(elem);
    elem = next_elem;
  }
  pH->count = 0;
}

/*
** Hash and comparison functions when the mode is SQLITE_HASH_STRING
*/
static int strHash(const void *pKey, int nKey){
  const char *z = (const char *)pKey;
  int h = 0;
  if( nKey<=0 ) nKey = sqlite3Strlen30(z);
  while( nKey > 0  ){
    h = (h<<3) ^ h ^ sqlite3UpperToLower[(unsigned char)*z++];
    nKey--;
  }
  return h & 0x7fffffff;
}
static int strCompare(const void *pKey1, int n1, const void *pKey2, int n2){
  if( n1!=n2 ) return 1;
  return sqlite3StrNICmp((const char*)pKey1,(const char*)pKey2,n1);
}


/* Link an element into the hash table
*/
static void insertElement(
  Hash *pH,              /* The complete hash table */
  struct _ht *pEntry,    /* The entry into which pNew is inserted */
  HashElem *pNew         /* The element to be inserted */
){
  HashElem *pHead;       /* First element already in pEntry */
  pHead = pEntry->chain;
  if( pHead ){
    pNew->next = pHead;
    pNew->prev = pHead->prev;
    if( pHead->prev ){ pHead->prev->next = pNew; }
    else             { pH->first = pNew; }
    pHead->prev = pNew;
  }else{
    pNew->next = pH->first;
    if( pH->first ){ pH->first->prev = pNew; }
    pNew->prev = 0;
    pH->first = pNew;
  }
  pEntry->count++;
  pEntry->chain = pNew;
}


/* Resize the hash table so that it cantains "new_size" buckets.
** "new_size" must be a power of 2.  The hash table might fail 
** to resize if sqlite3_malloc() fails.
*/
static void rehash(Hash *pH, int new_size){
  struct _ht *new_ht;            /* The new hash table */
  HashElem *elem, *next_elem;    /* For looping over existing elements */

#ifdef SQLITE_MALLOC_SOFT_LIMIT
  if( new_size*sizeof(struct _ht)>SQLITE_MALLOC_SOFT_LIMIT ){
    new_size = SQLITE_MALLOC_SOFT_LIMIT/sizeof(struct _ht);
  }
  if( new_size==pH->htsize ) return;
#endif

  /* There is a call to sqlite3_malloc() inside rehash(). If there is
  ** already an allocation at pH->ht, then if this malloc() fails it
  ** is benign (since failing to resize a hash table is a performance
  ** hit only, not a fatal error).
  */
  if( pH->htsize>0 ) sqlite3BeginBenignMalloc();
  new_ht = (struct _ht *)sqlite3MallocZero( new_size*sizeof(struct _ht) );
  if( pH->htsize>0 ) sqlite3EndBenignMalloc();

  if( new_ht==0 ) return;
  sqlite3_free(pH->ht);
  pH->ht = new_ht;
  pH->htsize = new_size;
  for(elem=pH->first, pH->first=0; elem; elem = next_elem){
    int h = strHash(elem->pKey, elem->nKey) & (new_size-1);
    next_elem = elem->next;
    insertElement(pH, &new_ht[h], elem);
  }
}

/* This function (for internal use only) locates an element in an
** hash table that matches the given key.  The hash for this key has
** already been computed and is passed as the 4th parameter.
*/
static HashElem *findElementGivenHash(
  const Hash *pH,     /* The pH to be searched */
  const void *pKey,   /* The key we are searching for */
  int nKey,
  int h               /* The hash for this key. */
){
  HashElem *elem;                /* Used to loop thru the element list */
  int count;                     /* Number of elements left to test */

  if( pH->ht ){
    struct _ht *pEntry = &pH->ht[h];
    elem = pEntry->chain;
    count = pEntry->count;
    while( count-- && elem ){
      if( strCompare(elem->pKey,elem->nKey,pKey,nKey)==0 ){ 
        return elem;
      }
      elem = elem->next;
    }
  }
  return 0;
}

/* Remove a single entry from the hash table given a pointer to that
** element and a hash on the element's key.
*/
static void removeElementGivenHash(
  Hash *pH,         /* The pH containing "elem" */
  HashElem* elem,   /* The element to be removed from the pH */
  int h             /* Hash value for the element */
){
  struct _ht *pEntry;
  if( elem->prev ){
    elem->prev->next = elem->next; 
  }else{
    pH->first = elem->next;
  }
  if( elem->next ){
    elem->next->prev = elem->prev;
  }
  pEntry = &pH->ht[h];
  if( pEntry->chain==elem ){
    pEntry->chain = elem->next;
  }
  pEntry->count--;
  if( pEntry->count<=0 ){
    pEntry->chain = 0;
  }
  if( pH->copyKey ){
    sqlite3_free(elem->pKey);
  }
  sqlite3_free( elem );
  pH->count--;
  if( pH->count<=0 ){
    assert( pH->first==0 );
    assert( pH->count==0 );
    sqlite3HashClear(pH);
  }
}

/* Attempt to locate an element of the hash table pH with a key
** that matches pKey,nKey.  Return a pointer to the corresponding 
** HashElem structure for this element if it is found, or NULL
** otherwise.
*/
HashElem *sqlite3HashFindElem(const Hash *pH, const void *pKey, int nKey){
  int h;             /* A hash on key */
  HashElem *elem;    /* The element that matches key */

  if( pH==0 || pH->ht==0 ) return 0;
  h = strHash(pKey,nKey);
  elem = findElementGivenHash(pH,pKey,nKey, h % pH->htsize);
  return elem;
}

/* Attempt to locate an element of the hash table pH with a key
** that matches pKey,nKey.  Return the data for this element if it is
** found, or NULL if there is no match.
*/
void *sqlite3HashFind(const Hash *pH, const void *pKey, int nKey){
  HashElem *elem;    /* The element that matches key */
  elem = sqlite3HashFindElem(pH, pKey, nKey);
  return elem ? elem->data : 0;
}

/* Insert an element into the hash table pH.  The key is pKey,nKey
** and the data is "data".
**
** If no element exists with a matching key, then a new
** element is created.  A copy of the key is made if the copyKey
** flag is set.  NULL is returned.
**
** If another element already exists with the same key, then the
** new data replaces the old data and the old data is returned.
** The key is not copied in this instance.  If a malloc fails, then
** the new data is returned and the hash table is unchanged.
**
** If the "data" parameter to this function is NULL, then the
** element corresponding to "key" is removed from the hash table.
*/
void *sqlite3HashInsert(Hash *pH, const void *pKey, int nKey, void *data){
  int hraw;             /* Raw hash value of the key */
  int h;                /* the hash of the key modulo hash table size */
  HashElem *elem;       /* Used to loop thru the element list */
  HashElem *new_elem;   /* New element added to the pH */

  assert( pH!=0 );
  hraw = strHash(pKey, nKey);
  if( pH->htsize ){
    h = hraw % pH->htsize;
    elem = findElementGivenHash(pH,pKey,nKey,h);
    if( elem ){
      void *old_data = elem->data;
      if( data==0 ){
        removeElementGivenHash(pH,elem,h);
      }else{
        elem->data = data;
        if( !pH->copyKey ){
          elem->pKey = (void *)pKey;
        }
        assert(nKey==elem->nKey);
      }
      return old_data;
    }
  }
  if( data==0 ) return 0;
  new_elem = (HashElem*)sqlite3Malloc( sizeof(HashElem) );
  if( new_elem==0 ) return data;
  if( pH->copyKey && pKey!=0 ){
    new_elem->pKey = sqlite3Malloc( nKey );
    if( new_elem->pKey==0 ){
      sqlite3_free(new_elem);
      return data;
    }
    memcpy((void*)new_elem->pKey, pKey, nKey);
  }else{
    new_elem->pKey = (void*)pKey;
  }
  new_elem->nKey = nKey;
  pH->count++;
  if( pH->htsize==0 ){
    rehash(pH, 128/sizeof(pH->ht[0]));
    if( pH->htsize==0 ){
      pH->count = 0;
      if( pH->copyKey ){
        sqlite3_free(new_elem->pKey);
      }
      sqlite3_free(new_elem);
      return data;
    }
  }
  if( pH->count > pH->htsize ){
    rehash(pH,pH->htsize*2);
  }
  assert( pH->htsize>0 );
  h = hraw % pH->htsize;
  insertElement(pH, &pH->ht[h], new_elem);
  new_elem->data = data;
  return 0;
}
