/*
** 2004 May 26
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
** This file contains code use to manipulate "Mem" structure.  A "Mem"
** stores a single value in the VDBE.  Mem is an opaque structure visible
** only within the VDBE.  Interface routines refer to a Mem using the
** name sqlite_value
*/
#include "sqliteInt.h"
#include "os.h"
#include <ctype.h>
#include "vdbeInt.h"

/*
** Given a Mem.flags value, return TEXT_Utf8, TEXT_Utf16le, or TEXT_Utf16be
** as appropriate.
*/
#define flagsToEnc(F) \
    (((F)&MEM_Utf8)?TEXT_Utf8: \
       ((F)&MEM_Utf16be)?TEXT_Utf16be:TEXT_Utf16le)

/*
** If pMem is a string object, this routine sets the encoding of the string
** (to one of UTF-8 or UTF16) and whether or not the string is
** nul-terminated. If pMem is not a string object, then this routine is
** a no-op.
**
** The second argument, "flags" consists of one of MEM_Utf8, MEM_Utf16le
** or MEM_Utf16be, possible ORed with MEM_Term. If necessary this function 
** manipulates the value stored by pMem so that it matches the flags passed
** in "flags".
**
** SQLITE_OK is returned if the conversion is successful (or not required).
** SQLITE_NOMEM may be returned if a malloc() fails during conversion
** between formats.
*/
int sqlite3VdbeSetEncoding(Mem *pMem, int flags){
  u8 enc1;    /* Current string encoding (TEXT_Utf* value) */
  u8 enc2;    /* Required string encoding (TEXT_Utf* value) */

  /* If this is not a string, do nothing. */
  if( !(pMem->flags&MEM_Str) ){
    return SQLITE_OK;
  }

  enc1 = flagsToEnc(pMem->flags);
  enc2 = flagsToEnc(flags);

  if( enc1!=enc2 ){
    if( enc1==TEXT_Utf8 || enc2==TEXT_Utf8 ){
      /* If the current encoding does not match the desired encoding, then
      ** we will need to do some translation between encodings.
      */
      char *z;
      int n;
      int rc = sqlite3utfTranslate(pMem->z,pMem->n,enc1,(void **)&z,&n,enc2);
      if( rc!=SQLITE_OK ){
        return rc;
      }
  
      /* Result of sqlite3utfTranslate is currently always dynamically
      ** allocated and nul terminated. This might be altered as a performance
      ** enhancement later.
      */
      pMem->z = z;
      pMem->n = n;
      pMem->flags = (MEM_Str | MEM_Dyn | MEM_Term | flags);
    }else{
      /* Must be translating between UTF-16le and UTF-16be. */
      int i;
      if( pMem->flags&MEM_Static ){
        Dynamicify(pMem, enc1);
      }
      for(i=0; i<pMem->n; i+=2){
        char c = pMem->z[i];
        pMem->z[i] = pMem->z[i+1];
        pMem->z[i+1] = c;
      }
      SetEncodingFlags(pMem, enc2);
    }
  }

  if( (flags&MEM_Term) && !(pMem->flags&MEM_Term) ){
    /* If we did not do any translation, but currently the string is
    ** not nul terminated (and is required to be), then we add the
    ** nul terminator now. We never have to do this if we translated
    ** the encoding of the string, as the translation functions return
    ** nul terminated values.
    */
    int f = pMem->flags;
    int nulTermLen = 2;     /* The number of 0x00 bytes to append */
    if( enc2==MEM_Utf8 ){
      nulTermLen = 1;
    }

    if( pMem->n+nulTermLen<=NBFS ){
      /* If the string plus the nul terminator will fit in the Mem.zShort
      ** buffer, and it is not already stored there, copy it there.
      */
      if( !(f&MEM_Short) ){
        memcpy(pMem->z, pMem->zShort, pMem->n);
        if( f&MEM_Dyn ){
          sqliteFree(pMem->z);
        }
        pMem->z = pMem->zShort;
        pMem->flags &= ~(MEM_Static|MEM_Ephem|MEM_Dyn);
        pMem->flags |= MEM_Short;
      }
    }else{
      /* Otherwise we have to malloc for memory. If the string is already
      ** dynamic, use sqliteRealloc(). Otherwise sqliteMalloc() enough
      ** space for the string and the nul terminator, and copy the string
      ** data there.
      */
      if( f&MEM_Dyn ){
        pMem->z = (char *)sqliteRealloc(pMem->z, pMem->n+nulTermLen);
        if( !pMem->z ){
          return SQLITE_NOMEM;
        }
      }else{
        char *z = (char *)sqliteMallocRaw(pMem->n+nulTermLen);
        memcpy(z, pMem->z, pMem->n);
        pMem->z = z;
        pMem->flags &= ~(MEM_Static|MEM_Ephem|MEM_Short);
        pMem->flags |= MEM_Dyn;
      }
    }

    /* pMem->z now points at the string data, with enough space at the end
    ** to insert the nul nul terminator. pMem->n has not yet been updated.
    */
    memcpy(&pMem->z[pMem->n], "\0\0", nulTermLen);
    pMem->n += nulTermLen;
    pMem->flags |= MEM_Term;
  }
  return SQLITE_OK;
}

static void releaseMem(Mem *p){
  if( p->flags & MEM_Dyn ){
    sqliteFree(p);
  }
}

/*
** Delete any previous value and set the value stored in *pMem to NULL.
*/
void sqlite3VdbeMemSetNull(Mem *pMem){
  releaseMem(pMem);
  pMem->flags = MEM_Null;
}

/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type INTEGER.
*/
void sqlite3VdbeMemSetInt(Mem *pMem, i64 val){
  releaseMem(pMem);
  pMem->i = val;
  pMem->flags = MEM_Int;
}

/*
** Delete any previous value and set the value stored in *pMem to val,
** manifest type REAL.
*/
void sqlite3VdbeMemSetReal(Mem *pMem, double val){
  releaseMem(pMem);
  pMem->r = val;
  pMem->flags = MEM_Real;
}

/*
** Copy the contents of memory cell pFrom into pTo.
*/
int sqlite3VdbeMemCopy(Mem *pTo, const Mem *pFrom){
  releaseMem(pTo);
  memcpy(pTo, pFrom, sizeof(*pFrom));
  if( pTo->flags&MEM_Short ){
    pTo->z = pTo->zShort;
  }else if( pTo->flags&(MEM_Ephem|MEM_Dyn) ){
    pTo->flags = pTo->flags&(~(MEM_Static|MEM_Ephem|MEM_Short|MEM_Dyn));
    if( pTo->n>NBFS ){
      pTo->z = sqliteMalloc(pTo->n);
      if( !pTo->z ) return SQLITE_NOMEM;
      pTo->flags |= MEM_Dyn;
    }else{
      pTo->z = pTo->zShort;
      pTo->flags |= MEM_Short;
    }
    memcpy(pTo->z, pFrom->z, pTo->n);
  }
  return SQLITE_OK;
}

int sqlite3VdbeMemSetStr(
  Mem *pMem,          /* Memory cell to set to string value */
  const char *z,      /* String pointer */
  int n,              /* Bytes in string, or negative */
  u8 enc,             /* Encoding of z */
  int eCopy           /* True if this function should make a copy of z */
){
  Mem tmp;

  releaseMem(pMem);
  if( !z ){
    /* If z is NULL, just set *pMem to contain NULL. */
    return SQLITE_OK;
  }

  pMem->z = (char *)z;
  if( eCopy ){
    pMem->flags = MEM_Ephem|MEM_Str;
  }else{
    pMem->flags = MEM_Static|MEM_Str;
  }
  pMem->flags |= encToFlags(enc);
  pMem->n = n;
  switch( enc ){
    case 0:
      pMem->flags |= MEM_Blob;
      break;

    case TEXT_Utf8:
      pMem->flags |= MEM_Utf8;
      if( n<0 ){
        pMem->n = strlen(z)+1;
        pMem->flags |= MEM_Term;
      }else if( z[pMem->n-1]==0 ){
        pMem->flags |= MEM_Term;
      }
      break;

    case TEXT_Utf16le:
    case TEXT_Utf16be:
      pMem->flags |= (enc==TEXT_Utf16le?MEM_Utf16le:MEM_Utf16be);
      if( n<0 ){
        pMem->n = sqlite3utf16ByteLen(z,-1)+1;
        pMem->flags |= MEM_Term;
      }else if( z[pMem->n-1]==0 && z[pMem->n-2]==0 ){
        pMem->flags |= MEM_Term;
      }
      break;

    default:
      assert(0);
  }
  Deephemeralize(pMem);
}

int sqlite3VdbeMemNulTerminate(Mem *pMem){
  int nulTermLen;
  int f = pMem->flags;

  assert( pMem->flags&MEM_Str && !pMem->flags&MEM_Term );
  assert( flagsToEnc(pMem->flags) );

  nulTermLen = (flagsToEnc(f)==TEXT_Utf8?1:2);

  if( pMem->n+nulTermLen<=NBFS ){
    /* If the string plus the nul terminator will fit in the Mem.zShort
    ** buffer, and it is not already stored there, copy it there.
    */
    if( !(f&MEM_Short) ){
      memcpy(pMem->z, pMem->zShort, pMem->n);
      if( f&MEM_Dyn ){
        sqliteFree(pMem->z);
      }
      pMem->z = pMem->zShort;
      pMem->flags &= ~(MEM_Static|MEM_Ephem|MEM_Dyn);
      pMem->flags |= MEM_Short;
    }
  }else{
    /* Otherwise we have to malloc for memory. If the string is already
    ** dynamic, use sqliteRealloc(). Otherwise sqliteMalloc() enough
    ** space for the string and the nul terminator, and copy the string
    ** data there.
    */
    if( f&MEM_Dyn ){
      pMem->z = (char *)sqliteRealloc(pMem->z, pMem->n+nulTermLen);
      if( !pMem->z ){
        return SQLITE_NOMEM;
      }
    }else{
      char *z = (char *)sqliteMalloc(pMem->n+nulTermLen);
      memcpy(z, pMem->z, pMem->n);
      pMem->z = z;
      pMem->flags &= ~(MEM_Static|MEM_Ephem|MEM_Short);
      pMem->flags |= MEM_Dyn;
    }
  }

  /* pMem->z now points at the string data, with enough space at the end
  ** to insert the nul nul terminator. pMem->n has not yet been updated.
  */
  memcpy(&pMem->z[pMem->n], "\0\0", nulTermLen);
  pMem->n += nulTermLen;
  pMem->flags |= MEM_Term;
}

/*
** Compare the values contained by the two memory cells, returning
** negative, zero or positive if pMem1 is less than, equal to, or greater
** than pMem2. Sorting order is NULL's first, followed by numbers (integers
** and reals) sorted numerically, followed by text ordered by the collating
** sequence pColl and finally blob's ordered by memcmp().
**
** Two NULL values are considered equal by this function.
*/
int sqlite3MemCompare(const Mem *pMem1, const Mem *pMem2, const CollSeq *pColl){
  int rc;
  int f1, f2;
  int combined_flags;

  /* Interchange pMem1 and pMem2 if the collating sequence specifies
  ** DESC order.
  */
  f1 = pMem1->flags;
  f2 = pMem2->flags;
  combined_flags = f1|f2;
 
  /* If one value is NULL, it is less than the other. If both values
  ** are NULL, return 0.
  */
  if( combined_flags&MEM_Null ){
    return (f2&MEM_Null) - (f1&MEM_Null);
  }

  /* If one value is a number and the other is not, the number is less.
  ** If both are numbers, compare as reals if one is a real, or as integers
  ** if both values are integers.
  */
  if( combined_flags&(MEM_Int|MEM_Real) ){
    if( !(f1&(MEM_Int|MEM_Real)) ){
      return 1;
    }
    if( !(f2&(MEM_Int|MEM_Real)) ){
      return -1;
    }
    if( (f1 & f2 & MEM_Int)==0 ){
      double r1, r2;
      if( (f1&MEM_Real)==0 ){
        r1 = pMem1->i;
      }else{
        r1 = pMem1->r;
      }
      if( (f2&MEM_Real)==0 ){
        r2 = pMem2->i;
      }else{
        r2 = pMem2->r;
      }
      if( r1<r2 ) return -1;
      if( r1>r2 ) return 1;
      return 0;
    }else{
      assert( f1&MEM_Int );
      assert( f2&MEM_Int );
      if( pMem1->i < pMem2->i ) return -1;
      if( pMem1->i > pMem2->i ) return 1;
      return 0;
    }
  }

  /* If one value is a string and the other is a blob, the string is less.
  ** If both are strings, compare using the collating functions.
  */
  if( combined_flags&MEM_Str ){
    if( (f1 & MEM_Str)==0 ){
      return 1;
    }
    if( (f2 & MEM_Str)==0 ){
      return -1;
    }
    if( pColl && pColl->xCmp ){
      return pColl->xCmp(pColl->pUser, pMem1->n, pMem1->z, pMem2->n, pMem2->z);
    }else{
      /* If no collating sequence is defined, fall through into the
      ** blob case below and use memcmp() for the comparison. */
    }
  }
 
  /* Both values must be blobs.  Compare using memcmp().
  */
  rc = memcmp(pMem1->z, pMem2->z, (pMem1->n>pMem2->n)?pMem2->n:pMem1->n);
  if( rc==0 ){
    rc = pMem1->n - pMem2->n;
  }
  return rc;
}
