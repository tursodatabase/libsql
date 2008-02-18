/*
** 2008 February 16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file implements an object that represents a fixed-length
** bitmap.  Bits are numbered starting with 1.
**
** A bitmap is used to record what pages a database file have been
** journalled during a transaction.  Usually only a few pages are
** journalled.  So the bitmap is usually sparse and has low cardinality.
** But sometimes (for example when during a DROP of a large table) most
** or all of the pages get journalled.  In those cases, the bitmap becomes
** dense.  The algorithm needs to handle both cases well.
**
** The size of the bitmap is fixed when the object is created.
**
** All bits are clear when the bitmap is created.  Individual bits
** may be set or cleared one at a time.
**
** Test operations are about 100 times more common that set operations.
** Clear operations are exceedingly rare.  There are usually between
** 5 and 500 set operations per Bitvec object, though the number of sets can
** sometimes grow into tens of thousands or larger.  The size of the
** Bitvec object is the number of pages in the database file at the
** start of a transaction, and is thus usually less than a few thousand,
** but can be as large as 2 billion for a really big database.
**
** @(#) $Id: bitvec.c,v 1.1 2008/02/18 14:47:34 drh Exp $
*/
#include "sqliteInt.h"

#define BITVEC_SZ        512
#define BITVEC_NCHAR     (BITVEC_SZ-12)
#define BITVEC_NBIT      (BITVEC_NCHAR*8)
#define BITVEC_NINT      ((BITVEC_SZ-12)/4)
#define BITVEC_MXHASH    (BITVEC_NINT/2)
#define BITVEC_NPTR      ((BITVEC_SZ-12)/8)

#define BITVEC_HASH(X)   (((X)*37)%BITVEC_NINT)

/*
** A bitmap is an instance of the following structure.
**
** This bitmap records the existance of zero or more bits
** with values between 1 and iSize, inclusive.
**
** There are three possible representations of the bitmap.
** If iSize<=BITVEC_NBIT, then Bitvec.u.aBitmap[] is a straight
** bitmap.  The least significant bit is bit 1.
**
** If iSize>BITVEC_NBIT and iDivisor==0 then Bitvec.u.aHash[] is
** a hash table that will hold up to BITVEC_MXHASH distinct values.
**
** Otherwise, the value i is redirected into one of BITVEC_NPTR
** sub-bitmaps pointed to by Bitvec.u.apSub[].  Each subbitmap
** handles up to iDivisor separate values of i.  apSub[0] holds
** values between 1 and iDivisor.  apSub[1] holds values between
** iDivisor+1 and 2*iDivisor.  apSub[N] holds values between
** N*iDivisor+1 and (N+1)*iDivisor.  Each subbitmap is normalized
** to hold deal with values between 1 and iDivisor.
*/
struct Bitvec {
  u32 iSize;      /* Maximum bit index */
  u32 nSet;       /* Number of bits that are set */
  u32 iDivisor;   /* Number of bits handled by each apSub[] entry */
  union {
    u8 aBitmap[BITVEC_NCHAR];    /* Bitmap representation */
    u32 aHash[BITVEC_NINT];      /* Hash table representation */
    Bitvec *apSub[BITVEC_NPTR];  /* Recursive representation */
  } u;
};

/*
** Create a new bitmap object able to handle bits between 0 and iSize,
** inclusive.  Return a pointer to the new object.  Return NULL if 
** malloc fails.
*/
Bitvec *sqlite3BitvecCreate(u32 iSize){
  Bitvec *p;
  assert( sizeof(*p)==BITVEC_SZ );
  p = sqlite3MallocZero( sizeof(*p) );
  if( p ){
    p->iSize = iSize;
  }
  return p;
}

/*
** Check to see if the i-th bit is set.  Return true or false.
** If p is NULL (if the bitmap has not been created) or if
** i is out of range, then return false.
*/
int sqlite3BitvecTest(Bitvec *p, u32 i){
  assert( i>0 );
  if( p==0 ) return 0;
  if( i>p->iSize ) return 0;
  if( p->iSize<=BITVEC_NBIT ){
    i--;
    return (p->u.aBitmap[i/8] & (1<<(i&7)))!=0;
  }
  if( p->iDivisor>0 ){
    u32 bin = (i-1)/p->iDivisor;
    i = (i-1)%p->iDivisor + 1;
    return sqlite3BitvecTest(p->u.apSub[bin], i);
  }else{
    u32 h = BITVEC_HASH(i);
    while( p->u.aHash[h] ){
      if( p->u.aHash[h]==i ) return 1;
      h++;
      if( h>=BITVEC_NINT ) h = 0;
    }
    return 0;
  }
}

/*
** Set the i-th bit.  Return 0 on success and an error code if
** anything goes wrong.
*/
int sqlite3BitvecSet(Bitvec *p, u32 i){
  u32 h;
  assert( p!=0 );
  if( p->iSize<=BITVEC_NBIT ){
    i--;
    p->u.aBitmap[i/8] |= 1 << (i&7);
    return SQLITE_OK;
  }
  if( p->iDivisor ){
    u32 bin = (i-1)/p->iDivisor;
    i = (i-1)%p->iDivisor + 1;
    if( p->u.apSub[bin]==0 ){
      sqlite3FaultBenign(SQLITE_FAULTINJECTOR_MALLOC, 1);
      p->u.apSub[bin] = sqlite3BitvecCreate( p->iDivisor );
      sqlite3FaultBenign(SQLITE_FAULTINJECTOR_MALLOC, 0);
      if( p->u.apSub[bin]==0 ) return SQLITE_NOMEM;
    }
    return sqlite3BitvecSet(p->u.apSub[bin], i);
  }
  h = BITVEC_HASH(i);
  while( p->u.aHash[h] ){
    if( p->u.aHash[h]==i ) return SQLITE_OK;
    h++;
    if( h==BITVEC_NINT ) h = 0;
  }
  p->nSet++;
  if( p->nSet>=BITVEC_MXHASH ){
    int j, rc;
    u32 aiValues[BITVEC_NINT];
    memcpy(aiValues, p->u.aHash, sizeof(aiValues));
    memset(p->u.apSub, 0, sizeof(p->u.apSub[0])*BITVEC_NPTR);
    p->iDivisor = (p->iSize + BITVEC_NPTR - 1)/BITVEC_NPTR;
    sqlite3BitvecSet(p, i);
    for(rc=j=0; j<BITVEC_NINT; j++){
      if( aiValues[j] ) rc |= sqlite3BitvecSet(p, aiValues[j]);
    }
    return rc;
  }
  p->u.aHash[h] = i;
  return SQLITE_OK;
}

/*
** Clear the i-th bit.  Return 0 on success and an error code if
** anything goes wrong.
*/
void sqlite3BitvecClear(Bitvec *p, u32 i){
  assert( p!=0 );
  if( p->iSize<=BITVEC_NBIT ){
    i--;
    p->u.aBitmap[i/8] &= ~(1 << (i&7));
  }else if( p->iDivisor ){
    u32 bin = (i-1)/p->iDivisor;
    i = (i-1)%p->iDivisor + 1;
    if( p->u.apSub[bin] ){
      sqlite3BitvecClear(p->u.apSub[bin], i);
    }
  }else{
    int j;
    u32 aiValues[BITVEC_NINT];
    memcpy(aiValues, p->u.aHash, sizeof(aiValues));
    memset(p->u.aHash, 0, sizeof(p->u.aHash[0])*BITVEC_NINT);
    p->nSet = 0;
    for(j=0; j<BITVEC_NINT; j++){
      if( aiValues[j] && aiValues[j]!=i ) sqlite3BitvecSet(p, aiValues[j]);
    }
  }
}

/*
** Destroy a bitmap object.  Reclaim all memory used.
*/
void sqlite3BitvecDestroy(Bitvec *p){
  if( p==0 ) return;
  if( p->iDivisor ){
    int i;
    for(i=0; i<BITVEC_NPTR; i++){
      sqlite3BitvecDestroy(p->u.apSub[i]);
    }
  }
  sqlite3_free(p);
}
