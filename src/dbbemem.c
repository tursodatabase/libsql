/*
** Copyright (c) 2000 D. Richard Hipp
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
** This file uses an in-memory hash table as the database backend. 
**
** $Id: dbbemem.c,v 1.10 2001/03/20 12:57:57 drh Exp $
*/
#include "sqliteInt.h"
#include <sys/stat.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>


typedef struct Array Array;
typedef struct ArrayElem ArrayElem;
typedef struct Datum Datum;

/* A complete associative array is an instance of the following structure.
** The internals of this structure are intended to be opaque -- client
** code should not attempt to access or modify the fields of this structure
** directly.  Change this structure only by using the routines below.
** However, many of the "procedures" and "functions" for modifying and
** accessing this structure are really macros, so we can't really make
** this structure opaque.
*/
struct Array {
  int count;               /* Number of entries in the array */
  ArrayElem *first;        /* The first element of the array */
  int htsize;              /* Number of buckets in the hash table */
  struct _Array_ht {         /* the hash table */
    int count;               /* Number of entries with this hash */
    ArrayElem *chain;        /* Pointer to first entry with this hash */
  } *ht;
};

/*
** An instance of the following structure stores a single key or
** data element.
*/
struct Datum {
  int n;
  void *p;
};

/* Each element in the associative array is an instance of the following 
** structure.  All elements are stored on a single doubly-linked list.
**
** Again, this structure is intended to be opaque, but it can't really
** be opaque because it is used by macros.
*/
struct ArrayElem {
  ArrayElem *next, *prev;  /* Next and previous elements in the array */
  Datum key, data;         /* Key and data for this element */
};

/* Some routines are so simple that they can be implemented as macros
** These are given first. */

/* Return the number of entries in the array */
#define ArrayCount(X)    ((X)->count)

/* Return a pointer to the first element of the array */
#define ArrayFirst(X)    ((X)->first)

/* Return a pointer to the next (or previous) element of the array */
#define ArrayNext(X)     ((X)->next)
#define ArrayPrev(X)     ((X)->prev)

/* Return TRUE if the element given is the last element in the array */
#define ArrayIsLast(X)   ((X)->next==0)
#define ArrayIsFirst(X)  ((X)->prev==0)

/* Return the data or key for an element of the array */
#define ArrayData(X)     ((X)->data.p)
#define ArrayDataSize(X) ((X)->data.n)
#define ArrayKey(X)      ((X)->key.p)
#define ArrayKeySize(X)  ((X)->key.n)

/* Turn bulk memory into an associative array object by initializing the
** fields of the Array structure.
*/
static void ArrayInit(Array *new){
  new->first = 0;
  new->count = 0;
  new->htsize = 0;
  new->ht = 0;
}

/* Remove all entries from an associative array.  Reclaim all memory.
** This is the opposite of ArrayInit().
*/
static void ArrayClear(Array *array){
  ArrayElem *elem;         /* For looping over all elements of the array */

  elem = array->first;
  array->first = 0;
  array->count = 0;
  if( array->ht ) sqliteFree(array->ht);
  array->ht = 0;
  array->htsize = 0;
  while( elem ){
    ArrayElem *next_elem = elem->next;
    sqliteFree(elem);
    elem = next_elem;
  }
}

/*
** Generate a hash from an N-byte key
*/
static int ArrayHash(Datum d){
  int h = 0;
  while( d.n-- > 0 ){
    /* The funky case "*(char**)&d.p" is to work around a bug the
    ** c89 compiler of HPUX. */
    h = (h<<9) ^ (h<<3) ^ h ^ *((*(char**)&d.p)++);
  }
  if( h<0 ) h = -h; 
  return h;
}

/* Resize the hash table for a Array array
*/
static void ArrayRehash(Array *array, int new_size){
  struct _Array_ht *new_ht;       /* The new hash table */
  ArrayElem *elem, *next_elem;    /* For looping over existing elements */
  int i;                          /* Loop counter */
  ArrayElem *x;                   /* Element being copied to new hash table */

  new_ht = sqliteMalloc( new_size*sizeof(struct _Array_ht) );
  if( array->ht ) sqliteFree(array->ht);
  array->ht = new_ht;
  array->htsize = new_size;
  for(i=new_size-1; i>=0; i--){ 
    new_ht[i].count = 0;
    new_ht[i].chain = 0;
  }
  for(elem=array->first, array->first=0; elem; elem = next_elem){
    int h = ArrayHash(elem->key) & (new_size-1);
    next_elem = elem->next;
    x = new_ht[h].chain;
    if( x ){
      elem->next = x;
      elem->prev = x->prev;
      if( x->prev ) x->prev->next = elem;
      else          array->first = elem;
      x->prev = elem;
    }else{
      elem->next = array->first;
      if( array->first ) array->first->prev = elem;
      elem->prev = 0;
      array->first = elem;
    }
    new_ht[h].chain = elem;
    new_ht[h].count++;
  }
}

/* This function (for internal use only) locates an element in an
** array that matches the given key.  The hash for this key has
** already been computed and is passed as the 3rd parameter.
*/
static ArrayElem *ArrayFindElementGivenHash(
  const Array *array,    /* The array to be searched */
  const Datum key,       /* The key we are searching for */
  int h                  /* The hash for this key. */
){
  ArrayElem *elem;                /* Used to loop thru the element list */
  int count;                      /* Number of elements left to test */

  if( array->count ){
    elem = array->ht[h].chain;
    count = array->ht[h].count;
    while( count-- && elem ){
      if( elem->key.n==key.n && memcmp(elem->key.p,key.p,key.n)==0 ){ 
        return elem;
      }
      elem = elem->next;
    }
  }
  return 0;
}


/* Attempt to locate an element of the associative array with a key
** that matches "key".  Return the ArrayElement if found and NULL if
** if no match.
*/
static ArrayElem *ArrayFindElement(const Array *array, Datum key){
  int h;             /* A hash on key */
  if( array->count==0 ) return 0;
  h = ArrayHash(key);
  return ArrayFindElementGivenHash(array, key, h & (array->htsize-1));
}

/* Remove a single entry from the array given a pointer to that
** element and a hash on the element's key.
*/
static void ArrayRemoveElementGivenHash(
  Array *array,        /* The array containing "elem" */
  ArrayElem* elem,     /* The element to be removed from the array */
  int h                /* Hash value for the element */
){
  if( elem->prev ){
    elem->prev->next = elem->next; 
  }else{
    array->first = elem->next;
  }
  if( elem->next ){
    elem->next->prev = elem->prev;
  }
  if( array->ht[h].chain==elem ){
    array->ht[h].chain = elem->next;
  }
  array->ht[h].count--;
  if( array->ht[h].count<=0 ){
    array->ht[h].chain = 0;
  }
  sqliteFree( elem );
  array->count--;
}

/* Attempt to locate an element of the associative array with a key
** that matches "key".  Return the data for this element if it is
** found, or NULL if no match is found.
*/
static Datum ArrayFind(const Array *array, Datum key){
  int h;             /* A hash on key */
  ArrayElem *elem;   /* The element that matches key */
  static Datum nil = {0, 0};

  if( array->count==0 ) return nil;
  h = ArrayHash(key);
  elem = ArrayFindElementGivenHash(array, key, h & (array->htsize-1));
  return elem ? elem->data : nil;
}

/* Insert an element into the array.  The key will be "key" and
** the data will be "data".
**
** If no array element exists with a matching key, then a new
** array element is created.  The key is copied into the new element.
** But only a pointer to the data is stored.  NULL is returned.
**
** If another element already exists with the same key, then the
** new data replaces the old data and the old data is returned.
** The key is not copied in this instance.
**
** If the "data" parameter to this function is NULL, then the
** element corresponding to "key" is removed from the array.
*/
static Datum ArrayInsert(Array *array, Datum key, Datum data){
  int hraw;              /* Raw hash value of the key */
  int h;                 /* the hash of the key modulo hash table size */
  ArrayElem *elem;       /* Used to loop thru the element list */
  ArrayElem *new_elem;   /* New element added to the array */
  Datum rv;              /* Return value */
  static Datum nil = {0, 0};

  hraw = ArrayHash(key);
  h = hraw & (array->htsize-1);
  elem = ArrayFindElementGivenHash(array,key,h);
  if( elem ){
    Datum old_data = elem->data;
    if( data.p==0 ){
      ArrayRemoveElementGivenHash(array,elem,h);
    }else{
      elem->data = data;
    }
    return old_data;
  }
  if( data.p==0 ) return nil;
  new_elem = (ArrayElem*)sqliteMalloc( sizeof(ArrayElem) + key.n );
  if( new_elem==0 ) return nil;
  new_elem->key.n = key.n;
  new_elem->key.p = (void*)&new_elem[1];
  memcpy(new_elem->key.p, key.p, key.n);
  array->count++;
  if( array->htsize==0 ) ArrayRehash(array,4);
  if( array->count > array->htsize ){
    ArrayRehash(array,array->htsize*2);
  }
  h = hraw & (array->htsize-1);
  elem = array->ht[h].chain;
  if( elem ){
    new_elem->next = elem;
    new_elem->prev = elem->prev;
    if( elem->prev ){ elem->prev->next = new_elem; }
    else            { array->first = new_elem; }
    elem->prev = new_elem;
  }else{
    new_elem->next = array->first;
    new_elem->prev = 0;
    if( array->first ){ array->first->prev = new_elem; }
    array->first = new_elem;
  }
  array->ht[h].count++;
  array->ht[h].chain = new_elem;
  new_elem->data = data;
  rv.p = 0;
  rv.n = 0;
  return rv;
}

/*
** Information about each open database table is an instance of this 
** structure.  There will only be one such structure for each
** table.  If the VDBE opens the same table twice (as will happen
** for a self-join, for example) then two DbbeCursor structures are
** created but there is only a single MTable structure.
*/
typedef struct MTable MTable;
struct MTable {
  char *zName;            /* Name of the table */
  int delOnClose;         /* Delete when closing */
  int intKeyOnly;         /* Use only integer keys on this table */
  Array data;             /* The data in this stable */
};

/*
** The following structure contains all information used by GDBM
** database driver.  This is a subclass of the Dbbe structure.
*/
typedef struct Dbbex Dbbex;
struct Dbbex {
  Dbbe dbbe;         /* The base class */
  Array tables;      /* All tables of the database */
};

/*
** An cursor into a database file is an instance of the following structure.
** There can only be a single MTable structure for each disk file, but
** there can be multiple DbbeCursor structures.  Each DbbeCursor represents
** a cursor pointing to a particular part of the open MTable.  The
** MTable.nRef field hold a count of the number of DbbeCursor structures
** associated with the same disk file.
*/
struct DbbeCursor {
  Dbbex *pBe;        /* The database of which this record is a part */
  MTable *pTble;     /* The database file for this table */
  ArrayElem *elem;   /* Most recently accessed record */
  int needRewind;    /* Next key should be the first */
};

/*
** Forward declaration
*/
static void sqliteMemCloseCursor(DbbeCursor *pCursr);

/*
** Erase all the memory of an MTable
*/
static void deleteMTable(MTable *p){
  ArrayElem *i;
  for(i=ArrayFirst(&p->data); i; i=ArrayNext(i)){
    void *data = ArrayData(i);
    sqliteFree(data);
  }
  ArrayClear(&p->data);
  sqliteFree(p->zName);
  sqliteFree(p);
}

/*
** Completely shutdown the given database.  Close all files.  Free all memory.
*/
static void sqliteMemClose(Dbbe *pDbbe){
  Dbbex *pBe = (Dbbex*)pDbbe;
  MTable *pTble;
  ArrayElem *j;
  for(j=ArrayFirst(&pBe->tables); j; j=ArrayNext(j)){
    pTble = ArrayData(j);
    deleteMTable(pTble);
  }
  ArrayClear(&pBe->tables);
  sqliteDbbeCloseAllTempFiles(pDbbe);
  memset(pBe, 0, sizeof(*pBe));
  sqliteFree(pBe);
}

/*
** Translate the name of an SQL table (or index) into its
** canonical name.
** 
** Space to hold the canonical name is obtained from
** sqliteMalloc() and must be freed by the calling function.
*/
static char *sqliteNameOfTable(const char *zTable){
  char *zNew = 0;
  int i, c;
  sqliteSetString(&zNew, zTable, 0);
  if( zNew==0 ) return 0;
  for(i=0; (c = zNew[i])!=0; i++){
    if( isupper(c) ){
      zNew[i] = tolower(c);
    }
  }
  return zNew;
}

/*
** Open a new table cursor.  Write a pointer to the corresponding
** DbbeCursor structure into *ppCursr.  Return an integer success
** code:
**
**    SQLITE_OK          It worked!
**
**    SQLITE_NOMEM       sqliteMalloc() failed
**
**    SQLITE_PERM        Attempt to access a file for which file
**                       access permission is denied
**
**    SQLITE_BUSY        Another thread or process is already using
**                       the corresponding file and has that file locked.
**
**    SQLITE_READONLY    The current thread already has this file open
**                       readonly but you are trying to open for writing.
**                       (This can happen if a SELECT callback tries to
**                       do an UPDATE or DELETE.)
**
** If zTable is 0 or "", then a temporary database file is created and
** a cursor to that temporary file is opened.  The temporary file
** will be deleted from the disk when it is closed.
*/
static int sqliteMemOpenCursor(
  Dbbe *pDbbe,            /* The database the table belongs to */
  const char *zTable,     /* The SQL name of the file to be opened */
  int writeable,          /* True to open for writing */
  int intKeyOnly,         /* True if only integer keys are used */
  DbbeCursor **ppCursr    /* Write the resulting table pointer here */
){
  DbbeCursor *pCursr;     /* The new table cursor */
  char *zName;            /* Canonical table name */
  MTable *pTble;          /* The underlying data file for this table */
  int rc = SQLITE_OK;     /* Return value */
  Dbbex *pBe = (Dbbex*)pDbbe;

  *ppCursr = 0;
  pCursr = sqliteMalloc( sizeof(*pCursr) );
  if( pCursr==0 ) return SQLITE_NOMEM;
  if( zTable ){
    Datum key;
    zName = sqliteNameOfTable(zTable);
    key.p = zName;
    key.n = strlen(zName);
    pTble = ArrayFind(&pBe->tables, key).p;
  }else{
    zName = 0;
    pTble = 0;
  }
  if( pTble==0 ){
    pTble = sqliteMalloc( sizeof(*pTble) );
    if( pTble==0 ){
      sqliteFree(zName);
      return SQLITE_NOMEM;
    }
    if( zName ){
      Datum ins_key, ins_data;
      pTble->zName = zName;
      pTble->delOnClose = 0;
      ins_data.p = pTble;
      ins_data.n = sizeof( *pTble );
      ins_key.p = zName;
      ins_key.n = strlen(zName);
      ArrayInsert(&pBe->tables, ins_key, ins_data);
    }else{
      pTble->zName = 0;
      pTble->delOnClose = 1;
    }
    pTble->intKeyOnly = intKeyOnly;
    ArrayInit(&pTble->data);
  }else{
    assert( pTble->intKeyOnly==intKeyOnly );
    sqliteFree(zName);
  }
  pCursr->pBe = pBe;
  pCursr->pTble = pTble;
  pCursr->needRewind = 1;
  *ppCursr = pCursr;
  return rc;
}

/*
** Drop a table from the database.  The file on the disk that corresponds
** to this table is deleted.
*/
static void sqliteMemDropTable(Dbbe *pDbbe, const char *zTable){
  char *zName;            /* Name of the table file */
  Datum key, data;
  MTable *pTble;
  Dbbex *pBe = (Dbbex*)pDbbe;

  zName = sqliteNameOfTable(zTable);
  key.p = zName;
  key.n = strlen(zName);
  pTble = ArrayFind(&pBe->tables, key).p;
  if( pTble ){
    data.p = 0;
    data.n = 0;
    ArrayInsert(&pBe->tables, key, data);
    deleteMTable(pTble);
  }
  sqliteFree(zName);
}

/*
** Close a cursor previously opened by sqliteMemOpenCursor().
**
** There can be multiple cursors pointing to the same open file.
** The underlying file is not closed until all cursors have been
** closed.  This routine decrements the MTable.nref field of the
** underlying file and closes the file when nref reaches 0.
*/
static void sqliteMemCloseCursor(DbbeCursor *pCursr){
  MTable *pTble;
  Dbbex *pBe;
  if( pCursr==0 ) return;
  pTble = pCursr->pTble;
  pBe = pCursr->pBe;
  if( pTble->delOnClose ){
    deleteMTable(pTble);
  }
  sqliteFree(pCursr);
}

/*
** Reorganize a table to reduce search times and disk usage.
*/
static int sqliteMemReorganizeTable(Dbbe *pBe, const char *zTable){
  /* Do nothing */
  return SQLITE_OK;
}

/*
** Fetch a single record from an open cursor.  Return 1 on success
** and 0 on failure.
*/
static int sqliteMemFetch(DbbeCursor *pCursr, int nKey, char *pKey){
  Datum key;
  key.n = nKey;
  key.p = pKey;
  assert( nKey==4 || pCursr->pTble->intKeyOnly==0 );
  pCursr->elem = ArrayFindElement(&pCursr->pTble->data, key);
  return pCursr->elem!=0;
}

/*
** Return 1 if the given key is already in the table.  Return 0
** if it is not.
*/
static int sqliteMemTest(DbbeCursor *pCursr, int nKey, char *pKey){
  return sqliteMemFetch(pCursr, nKey, pKey);
}

/*
** Copy bytes from the current key or data into a buffer supplied by
** the calling function.  Return the number of bytes copied.
*/
static
int sqliteMemCopyKey(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  if( pCursr->elem==0 ) return 0;
  if( offset>=ArrayKeySize(pCursr->elem) ) return 0;
  if( offset+size>ArrayKeySize(pCursr->elem) ){
    n = ArrayKeySize(pCursr->elem) - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, &((char*)ArrayKey(pCursr->elem))[offset], n);
  return n;
}
static
int sqliteMemCopyData(DbbeCursor *pCursr, int offset, int size, char *zBuf){
  int n;
  if( pCursr->elem==0 ) return 0;
  if( offset>=ArrayDataSize(pCursr->elem) ) return 0;
  if( offset+size>ArrayDataSize(pCursr->elem) ){
    n = ArrayDataSize(pCursr->elem) - offset;
  }else{
    n = size;
  }
  memcpy(zBuf, &((char*)ArrayData(pCursr->elem))[offset], n);
  return n;
}

/*
** Return a pointer to bytes from the key or data.  The data returned
** is ephemeral.
*/
static char *sqliteMemReadKey(DbbeCursor *pCursr, int offset){
  if( pCursr->elem==0 || offset<0 || offset>=ArrayKeySize(pCursr->elem) ){
    return "";
  }
  return &((char*)ArrayKey(pCursr->elem))[offset];
}
static char *sqliteMemReadData(DbbeCursor *pCursr, int offset){
  if( pCursr->elem==0 || offset<0 || offset>=ArrayDataSize(pCursr->elem) ){
    return "";
  }
  return &((char*)ArrayData(pCursr->elem))[offset];
}

/*
** Return the total number of bytes in either data or key.
*/
static int sqliteMemKeyLength(DbbeCursor *pCursr){
  return pCursr->elem ? ArrayKeySize(pCursr->elem) : 0;
}
static int sqliteMemDataLength(DbbeCursor *pCursr){
  return pCursr->elem ? ArrayDataSize(pCursr->elem) : 0;
}

/*
** Make is so that the next call to sqliteNextKey() finds the first
** key of the table.
*/
static int sqliteMemRewind(DbbeCursor *pCursr){
  pCursr->needRewind = 1;
  return SQLITE_OK;
}

/*
** Read the next key from the table.  Return 1 on success.  Return
** 0 if there are no more keys.
*/
static int sqliteMemNextKey(DbbeCursor *pCursr){
  if( pCursr->needRewind || pCursr->elem==0 ){
    pCursr->elem = ArrayFirst(&pCursr->pTble->data);
    pCursr->needRewind = 0;
  }else{
    pCursr->elem = ArrayNext(pCursr->elem);
  }
  return pCursr->elem!=0;
}

/*
** Get a new integer key.
*/
static int sqliteMemNew(DbbeCursor *pCursr){
  int iKey;
  Datum key;
  int go = 1;

  while( go ){
    iKey = sqliteRandomInteger();
    if( iKey==0 ) continue;
    key.p = (char*)&iKey;
    key.n = 4;
    go = ArrayFindElement(&pCursr->pTble->data, key)!=0;
  }
  return iKey;
}   

/*
** Write an entry into the table.  Overwrite any prior entry with the
** same key.
*/
static int sqliteMemPut(
  DbbeCursor *pCursr,       /* Write new entry into this database table */
  int nKey, char *pKey,     /* The key of the new entry */
  int nData, char *pData    /* The data of the new entry */
){
  Datum data, key;
  data.n = nData;
  data.p = sqliteMalloc( data.n );
  memcpy(data.p, pData, data.n);
  key.n = nKey;
  key.p = pKey;
  assert( nKey==4 || pCursr->pTble->intKeyOnly==0 );
  data = ArrayInsert(&pCursr->pTble->data, key, data);
  if( data.p ){
    sqliteFree(data.p);
  }
  return SQLITE_OK;
}

/*
** Remove an entry from a table, if the entry exists.
*/
static int sqliteMemDelete(DbbeCursor *pCursr, int nKey, char *pKey){
  Datum key, data;
  key.n = nKey;
  key.p = pKey;
  data.p = 0;
  data.n = 0;
  data = ArrayInsert(&pCursr->pTble->data, key, data);
  if( data.p ){
    sqliteFree(data.p);
  }
  return SQLITE_OK;
}

/*
** Open a temporary file.  The file is located in the current working
** directory.
*/
static int sqliteMemOpenTempFile(Dbbe *pDbbe, FILE **ppFile){
#if OS_UNIX
  const char *zTemps[] = { "/usr/tmp", "/var/tmp", "/tmp", "/temp", 0};
#endif
#if OS_WIN
  const char *zTemps[] = { "/temp", "c:/temp", "c:", "d:", "e:", 0};
#endif
  const char *zDir;
  int i;
  struct stat statbuf;
  for(i=0; zTemps[i]; i++){
    zDir = zTemps[i];
    if( stat(zDir, &statbuf)==0 && S_ISDIR(statbuf.st_mode) 
      && access(zDir, W_OK|X_OK)==0 ){
        break;
    }
  }
  if( zDir==0 ) zDir = ".";
  return sqliteDbbeOpenTempFile(zDir, pDbbe, ppFile);
}

/*
** This variable contains pointers to all of the access methods
** used to implement the MEMORY backend.
*/
static struct DbbeMethods memoryMethods = {
  /* n         Close */   sqliteMemClose,
  /*      OpenCursor */   sqliteMemOpenCursor,
  /*       DropTable */   sqliteMemDropTable,
  /* ReorganizeTable */   sqliteMemReorganizeTable,
  /*     CloseCursor */   sqliteMemCloseCursor,
  /*           Fetch */   sqliteMemFetch,
  /*            Test */   sqliteMemTest,
  /*         CopyKey */   sqliteMemCopyKey,
  /*        CopyData */   sqliteMemCopyData,
  /*         ReadKey */   sqliteMemReadKey,
  /*        ReadData */   sqliteMemReadData,
  /*       KeyLength */   sqliteMemKeyLength,
  /*      DataLength */   sqliteMemDataLength,
  /*         NextKey */   sqliteMemNextKey,
  /*          Rewind */   sqliteMemRewind,
  /*             New */   sqliteMemNew,
  /*             Put */   sqliteMemPut,
  /*          Delete */   sqliteMemDelete,
  /*    OpenTempFile */   sqliteMemOpenTempFile,
  /*   CloseTempFile */   sqliteDbbeCloseTempFile
};

/*
** This routine opens a new database.  For the GDBM driver
** implemented here, the database name is the name of the directory
** containing all the files of the database.
**
** If successful, a pointer to the Dbbe structure is returned.
** If there are errors, an appropriate error message is left
** in *pzErrMsg and NULL is returned.
*/
Dbbe *sqliteMemOpen(
  const char *zName,     /* The name of the database */
  int writeFlag,         /* True if we will be writing to the database */
  int createFlag,        /* True to create database if it doesn't exist */
  char **pzErrMsg        /* Write error messages (if any) here */
){
  Dbbex *pNew;

  pNew = sqliteMalloc( sizeof(*pNew) );
  if( pNew==0 ){
    sqliteSetString(pzErrMsg, "out of memory", 0);
    return 0;
  }
  ArrayInit(&pNew->tables);
  pNew->dbbe.x = &memoryMethods;
  return &pNew->dbbe;
}
