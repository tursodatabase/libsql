/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** The code in this file implements execution method of the 
** Virtual Database Engine (VDBE).  A separate file ("vdbeaux.c")
** handles housekeeping details such as creating and deleting
** VDBE instances.  This file is solely interested in executing
** the VDBE program.
**
** In the external interface, an "sqlite3_stmt*" is an opaque pointer
** to a VDBE.
**
** The SQL parser generates a program which is then executed by
** the VDBE to do the work of the SQL statement.  VDBE programs are 
** similar in form to assembly language.  The program consists of
** a linear sequence of operations.  Each operation has an opcode 
** and 3 operands.  Operands P1 and P2 are integers.  Operand P3 
** is a null-terminated string.   The P2 operand must be non-negative.
** Opcodes will typically ignore one or more operands.  Many opcodes
** ignore all three operands.
**
** Computation results are stored on a stack.  Each entry on the
** stack is either an integer, a null-terminated string, a floating point
** number, or the SQL "NULL" value.  An inplicit conversion from one
** type to the other occurs as necessary.
** 
** Most of the code in this file is taken up by the sqlite3VdbeExec()
** function which does the work of interpreting a VDBE program.
** But other routines are also provided to help in building up
** a program instruction by instruction.
**
** Various scripts scan this source file in order to generate HTML
** documentation, headers files, or other derived files.  The formatting
** of the code in this file is, therefore, important.  See other comments
** in this file for details.  If in doubt, do not deviate from existing
** commenting and indentation practices when changing or adding code.
**
** $Id: vdbe.c,v 1.334 2004/05/26 13:27:00 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "os.h"
#include <ctype.h>
#include "vdbeInt.h"

/*
** The following global variable is incremented every time a cursor
** moves, either by the OP_MoveXX, OP_Next, or OP_Prev opcodes.  The test
** procedures use this information to make sure that indices are
** working correctly.  This variable has no function other than to
** help verify the correct operation of the library.
*/
int sqlite3_search_count = 0;

/*
** When this global variable is positive, it gets decremented once before
** each instruction in the VDBE.  When reaches zero, the SQLITE_Interrupt
** of the db.flags field is set in order to simulate and interrupt.
**
** This facility is used for testing purposes only.  It does not function
** in an ordinary build.
*/
int sqlite3_interrupt_count = 0;

/*
** This macro takes a single parameter, a pointer to a Mem structure.
** It returns the string encoding for the Mem structure, one of TEXT_Utf8
** TEXT_Utf16le or TEXT_Utf16be.
*/
#define MemEnc(p) ( \
   p->flags&MEM_Utf16le?TEXT_Utf16le: \
  (p->flags&MEM_Utf16le?TEXT_Utf16be:TEXT_Utf8) )

/*
** The following macros each take one parameter, a pointer to a Mem
** structure. The value returned is non-zero if the value stored in 
** the Mem structure is of or can be losslessly converted to the
** type implicit in the macro name.
** 
** MemIsNull     # NULL values
** MemIsInt      # Ints and reals and strings that can be converted to ints.
** MemIsReal     # Reals, ints and strings that look like numbers
** MemIsStr      # Strings, reals and ints.
** MemIsBlob     # Blobs.
**
** These macros do not alter the contents of the Mem structure.
*/
#define MemIsNull(p) ((p)->flags&Mem_Null)
#define MemIsBlob(p) ((p)->flags&Mem_Blob)
#define MemIsStr(p) ((p)->flags&(MEM_Int|MEM_Real|MEM_Str))
#define MemIsInt(p) ((p)->flags&(MEM_Int|MEM_Real) || hardMemIsInt(p))
#define MemIsReal(p) ((p)->flags&(MEM_Int|MEM_Real) || hardMemIsReal(p))
static int hardMemIsInt(Mem *p){
  assert( !(p->flags&(MEM_Int|MEM_Real)) );
  if( p->flags&MEM_Str ){
    int realnum = 0;
    if( sqlite3IsNumber(p->z, &realnum, MemEnc(p)) && !realnum ){
      return 1;
    }
  }
  return 0;
}
static int hardMemIsReal(Mem *p){
  assert( !(p->flags&(MEM_Int|MEM_Real)) );
  if( p->flags&MEM_Str && sqlite3IsNumber(p->z, 0, MemEnc(p)) ){
    return 1;
  }
  return 0;
}

/*
** The following two macros each take one parameter, a pointer to a Mem
** structure. They return the value stored in the Mem structure coerced
** to a 64-bit integer or real, respectively.
**
** MemInt
** MemReal
**
** These macros do not alter the contents of the Mem structure, although
** they may cache the integer or real value cast of the value.
*/
#define MemInt(p) (((p)->flags&MEM_Int)?(p)->i:hardMemInt(p))
#define MemReal(p) (((p)->flags&MEM_Real)?(p)->r:hardMemReal(p))
static i64 hardMemInt(Mem *p){
  assert( !(p->flags&MEM_Int) );
  if( !MemIsInt(p) ) return 0;

  if( p->flags&MEM_Real ){
    p->i = p->r;
  }else{
    assert( p->flags&MEM_Str );
    sqlite3atoi64(p->z, &(p->i), MemEnc(p));
  }
  p->flags |= MEM_Int;
  return p->i;
}
static double hardMemReal(Mem *p){
  assert( !(p->flags&MEM_Real) );
  if( !MemIsReal(p) ) return 0.0;

  if( p->flags&MEM_Int ){
    p->r = p->i;
  }else{
    assert( p->flags&MEM_Str );
    /* p->r = sqlite3AtoF(p->z, 0, MemEnc(p)); */
    p->r = sqlite3AtoF(p->z, 0);
  }
  p->flags |= MEM_Real;
  return p->r;
}


#if 0
/*
** MemStr(Mem *pMem)
** MemBlob(Mem *pMem)
** MemBloblen(Mem *pMem)
**
** MemType(Mem *pMem)
**
** MemSetBlob
** MemSetStr
**
** MemSetEnc
** MemSetType
**
** MemCopy
*/
struct MemRecord {
  char *zData;    /* Serialized record */
  int nField;     /* Number of fields in the header */
  int nHeader;    /* Number of bytes in the entire header */
  u64 *aType;     /* Type values for all entries in the record */
};
typedef struct MemRecord MemRecord;

/*
** Transform the value stored in pMem, which must be a blob into a
** MemRecord. An Mem cell used to store a MemRecord works as follows:
**
** Mem.z points at a MemRecord struct
*/
static int Recordify(Mem *pMem){
  return 0;
}
#endif

/*
** Release the memory associated with the given stack level.  This
** leaves the Mem.flags field in an inconsistent state.
*/
#define Release(P) if((P)->flags&MEM_Dyn){ sqliteFree((P)->z); }

/*
** Parmameter "flags" is the value of the flags for a string Mem object.
** Return one of TEXT_Utf8, TEXT_Utf16le or TEXT_Utf16be, depending
** on the encoding indicated by the flags value.
*/
static u8 flagsToEnc(int flags){
  if( flags&MEM_Utf8 ){
    assert( !(flags&(MEM_Utf16be|MEM_Utf16le)) );
    return TEXT_Utf8;
  }
  if( flags&MEM_Utf16le ){
    assert( !(flags&(MEM_Utf8|MEM_Utf16be)) );
    return TEXT_Utf16le;
  }
  assert( flags&MEM_Utf16be );
  assert( !(flags&(MEM_Utf8|MEM_Utf16le)) );
  return TEXT_Utf16be;
}

/*
** Parameter "enc" is one of TEXT_Utf8, TEXT_Utf16le or TEXT_Utf16be.
** Return the corresponding MEM_Utf* value.
*/
static int encToFlags(u8 enc){
  switch( enc ){
    case TEXT_Utf8: return MEM_Utf8;
    case TEXT_Utf16be: return MEM_Utf16be;
    case TEXT_Utf16le: return MEM_Utf16le;
  }
  assert(0);
}

/*
** Set the encoding flags of memory cell "pMem" to the correct values
** for the database encoding "enc" (one of TEXT_Utf8, TEXT_Utf16le or
** TEXT_Utf16be).
*/
#define SetEncodingFlags(pMem, enc) ((pMem)->flags = \
((pMem->flags & ~(MEM_Utf8|MEM_Utf16le|MEM_Utf16be))) | encToFlags(enc))
static int SetEncoding(Mem*, int);

/*
** Set the MEM_TypeStr, MEM_TypeReal or MEM_TypeInt flags in pMem if
** required.
*/
static void MemSetTypeFlags(Mem *pMem){
  int f = pMem->flags;
  if( f&MEM_Int ) pMem->flags |= MEM_TypeInt;
  else if( f&MEM_Real ) pMem->flags |= MEM_TypeReal;
  else if( f&MEM_Str ) pMem->flags |= MEM_TypeStr;
}


/*
** Convert the given stack entity into a string if it isn't one
** already. Return non-zero if a malloc() fails.
*/
#define Stringify(P, enc) \
if( !((P)->flags&(MEM_Str|MEM_Blob)) ) hardStringify(P, enc);
static int hardStringify(Mem *pStack, u8 enc){
  int rc = SQLITE_OK;
  int fg = pStack->flags;

  assert( !(fg&(MEM_Str|MEM_Blob)) );
  assert( fg&(MEM_Int|MEM_Real|MEM_Null) );

  if( fg & MEM_Null ){      
    /* A NULL value is converted to a zero length string */
    pStack->zShort[0] = 0;
    pStack->zShort[1] = 0;
    pStack->flags = MEM_Str | MEM_Short | MEM_Term;
    pStack->z = pStack->zShort;
    pStack->n = (enc==TEXT_Utf8?1:2);
  }else{
    /* For a Real or Integer, use sqlite3_snprintf() to produce the UTF-8
    ** string representation of the value. Then, if the required encoding
    ** is UTF-16le or UTF-16be do a translation.
    ** 
    ** FIX ME: It would be better if sqlite3_snprintf() could do UTF-16.
    */
    if( fg & MEM_Real ){
      sqlite3_snprintf(NBFS, pStack->zShort, "%.15g", pStack->r);
    }else if( fg & MEM_Int ){
      sqlite3_snprintf(NBFS, pStack->zShort, "%lld", pStack->i);
    }
    pStack->n = strlen(pStack->zShort) + 1;
    pStack->z = pStack->zShort;
    pStack->flags = MEM_Str | MEM_Short | MEM_Term;

    /* Flip the string to UTF-16 if required */
    SetEncodingFlags(pStack, TEXT_Utf8);
    rc = SetEncoding(pStack, encToFlags(enc)|MEM_Term);
  }

  return rc;
}

/*
** Convert the given stack entity into a string that has been obtained
** from sqliteMalloc().  This is different from Stringify() above in that
** Stringify() will use the NBFS bytes of static string space if the string
** will fit but this routine always mallocs for space.
** Return non-zero if we run out of memory.
*/
#define Dynamicify(P, enc) \
(((P)->flags & MEM_Dyn)==0 ? hardDynamicify(P, enc):0)
static int hardDynamicify(Mem *pStack, u8 enc){
  int fg = pStack->flags;
  char *z;
  if( (fg & MEM_Str)==0 ){
    hardStringify(pStack, enc);
  }
  assert( (fg & MEM_Dyn)==0 );
  z = sqliteMallocRaw( pStack->n );
  if( z==0 ) return 1;
  memcpy(z, pStack->z, pStack->n);
  pStack->z = z;
  pStack->flags |= MEM_Dyn;
  return 0;
}

/*
** An ephemeral string value (signified by the MEM_Ephem flag) contains
** a pointer to a dynamically allocated string where some other entity
** is responsible for deallocating that string.  Because the stack entry
** does not control the string, it might be deleted without the stack
** entry knowing it.
**
** This routine converts an ephemeral string into a dynamically allocated
** string that the stack entry itself controls.  In other words, it
** converts an MEM_Ephem string into an MEM_Dyn string.
*/
#define Deephemeralize(P) \
   if( ((P)->flags&MEM_Ephem)!=0 && hardDeephem(P) ){ goto no_mem;}
static int hardDeephem(Mem *pStack){
  char *z;
  assert( (pStack->flags & MEM_Ephem)!=0 );
  z = sqliteMallocRaw( pStack->n );
  if( z==0 ) return 1;
  memcpy(z, pStack->z, pStack->n);
  pStack->z = z;
  pStack->flags &= ~MEM_Ephem;
  pStack->flags |= MEM_Dyn;
  return 0;
}

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
int SetEncoding(Mem *pMem, int flags){
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
      pMem->flags &= ~(MEM_Utf8|MEM_Utf16le|MEM_Utf16be);
      pMem->flags &= ~(MEM_Static|MEM_Short|MEM_Ephem);
      pMem->flags |= (MEM_Dyn|MEM_Term|flags);
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
  return SQLITE_OK;
}

/*
** Convert the given stack entity into a integer if it isn't one
** already.
**
** Any prior string or real representation is invalidated.  
** NULLs are converted into 0.
*/
#define Integerify(P, enc) \
if(((P)->flags&MEM_Int)==0){ hardIntegerify(P, enc); }
static void hardIntegerify(Mem *pStack, u8 enc){
  pStack->i = 0;
  if( pStack->flags & MEM_Real ){
    pStack->i = (int)pStack->r;
    Release(pStack);
  }else if( pStack->flags & MEM_Str ){
    if( pStack->z ){
      sqlite3atoi64(pStack->z, &pStack->i, enc);
    }
  }
  pStack->flags = MEM_Int;
}

/*
** Get a valid Real representation for the given stack element.
**
** Any prior string or integer representation is retained.
** NULLs are converted into 0.0.
*/
#define Realify(P,enc) if(((P)->flags&MEM_Real)==0){ hardRealify(P,enc); }
static void hardRealify(Mem *pStack, u8 enc){
  if( pStack->flags & MEM_Str ){
    SetEncodingFlags(pStack, enc);
    SetEncoding(pStack, MEM_Utf8|MEM_Term);
    pStack->r = sqlite3AtoF(pStack->z, 0);
  }else if( pStack->flags & MEM_Int ){
    pStack->r = pStack->i;
  }else{
    pStack->r = 0.0;
  }
/*  pStack->flags |= MEM_Real; */
  pStack->flags = MEM_Real;
}

/*
** Execute the statement pStmt, either until a row of data is ready, the
** statement is completely executed or an error occurs.
*/
int sqlite3_step(sqlite3_stmt *pStmt){
  Vdbe *p = (Vdbe*)pStmt;
  sqlite *db;
  int rc;

  if( p->magic!=VDBE_MAGIC_RUN ){
    return SQLITE_MISUSE;
  }
  db = p->db;
  if( sqlite3SafetyOn(db) ){
    p->rc = SQLITE_MISUSE;
    return SQLITE_MISUSE;
  }
  if( p->explain ){
    rc = sqlite3VdbeList(p);
  }else{
    rc = sqlite3VdbeExec(p);
  }

  if( sqlite3SafetyOff(db) ){
    rc = SQLITE_MISUSE;
  }

  sqlite3Error(p->db, rc, p->zErrMsg);
  return rc;
}

/*
** Return the number of columns in the result set for the statement pStmt.
*/
int sqlite3_column_count(sqlite3_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  return pVm->nResColumn;
}

/*
** Return the number of values available from the current row of the
** currently executing statement pStmt.
*/
int sqlite3_data_count(sqlite3_stmt *pStmt){
  Vdbe *pVm = (Vdbe *)pStmt;
  if( !pVm->resOnStack ) return 0;
  return pVm->nResColumn;
}

/*
** Return the value of the 'i'th column of the current row of the currently
** executing statement pStmt.
*/
const unsigned char *sqlite3_column_data(sqlite3_stmt *pStmt, int i){
  int vals;
  Vdbe *pVm = (Vdbe *)pStmt;
  Mem *pVal;

  vals = sqlite3_data_count(pStmt);
  if( i>=vals || i<0 ){
    sqlite3Error(pVm->db, SQLITE_RANGE, 0);
    return 0;
  }

  pVal = &pVm->pTos[(1-vals)+i];
  return sqlite3_value_data((sqlite3_value *)pVal);
}

/*
** pVal is a Mem* cast to an sqlite_value* value. Return a pointer to
** the nul terminated UTF-8 string representation if the value is 
** not a blob or NULL. If the value is a blob, then just return a pointer
** to the blob of data. If it is a NULL, return a NULL pointer.
**
** This function may translate the encoding of the string stored by
** pVal. The MEM_Utf8, MEM_Utf16le and MEM_Utf16be flags must be set
** correctly when this function is called. If a translation occurs,
** the flags are set to reflect the new encoding of the string.
**
** If a translation fails because of a malloc() failure, a NULL pointer
** is returned.
*/
const unsigned char *sqlite3_value_data(sqlite3_value *pVal){
  int flags = pVal->flags;

  if( flags&MEM_Null ){
    /* For a NULL return a NULL Pointer */
    return 0;
  }

  if( flags&MEM_Str ){
    /* If there is already a string representation, make sure it is in
    ** encoded in UTF-8.
    */
    SetEncoding(pVal, MEM_Utf8|MEM_Term);
  }else if( !(flags&MEM_Blob) ){
    if( flags&MEM_Int ){
      sqlite3_snprintf(NBFS, pVal->zShort, "%lld", pVal->i);
    }else{
      assert( flags&MEM_Real );
      sqlite3_snprintf(NBFS, pVal->zShort, "%.15g", pVal->r);
    }
    pVal->z = pVal->zShort;
    pVal->n = strlen(pVal->z)+1;
    pVal->flags |= (MEM_Str|MEM_Short);
  }

  return pVal->z;
}

/*
** pVal is a Mem* cast to an sqlite_value* value. Return a pointer to
** the nul terminated UTF-16 string representation if the value is 
** not a blob or NULL. If the value is a blob, then just return a pointer
** to the blob of data. If it is a NULL, return a NULL pointer.
**
** The byte-order of the returned string data is the machines native byte
** order.
**
** This function may translate the encoding of the string stored by
** pVal. The MEM_Utf8, MEM_Utf16le and MEM_Utf16be flags must be set
** correctly when this function is called. If a translation occurs,
** the flags are set to reflect the new encoding of the string.
**
** If a translation fails because of a malloc() failure, a NULL pointer
** is returned.
*/
const void *sqlite3_value_data16(sqlite3_value* pVal){
  if( pVal->flags&MEM_Null ){
    /* For a NULL return a NULL Pointer */
    return 0;
  }

  if( pVal->flags&MEM_Str ){
    /* If there is already a string representation, make sure it is in
    ** encoded in UTF-16 machine byte order.
    */
    SetEncoding(pVal, encToFlags(TEXT_Utf16)|MEM_Term);
  }else if( !(pVal->flags&MEM_Blob) ){
    sqlite3_value_data(pVal);
    SetEncoding(pVal, encToFlags(TEXT_Utf16)|MEM_Term);
  }

  return (const void *)(pVal->z);
}

/*
** Return the value of the 'i'th column of the current row of the currently
** executing statement pStmt.
*/
const void *sqlite3_column_data16(sqlite3_stmt *pStmt, int i){
  int vals;
  Vdbe *pVm = (Vdbe *)pStmt;
  Mem *pVal;

  vals = sqlite3_data_count(pStmt);
  if( i>=vals || i<0 ){
    sqlite3Error(pVm->db, SQLITE_RANGE, 0);
    return 0;
  }

  pVal = &pVm->pTos[(1-vals)+i];
  return sqlite3_value_data16((sqlite3_value *)pVal);
}

/*
** Return the number of bytes of data that will be returned by the
** equivalent sqlite3_value_data() call.
*/
int sqlite3_value_bytes(sqlite3_value *pVal){
  if( sqlite3_value_data(pVal) ){
    return ((Mem *)pVal)->n;
  }
  return 0;
}

/*
** Return the number of bytes of data that will be returned by the
** equivalent sqlite3_value_data16() call.
*/
int sqlite3_value_bytes16(sqlite3_value *pVal){
  if( sqlite3_value_data16(pVal) ){
    return ((Mem *)pVal)->n;
  }
  return 0;
}

/*
** Return the value of the sqlite_value* argument coerced to a 64-bit
** integer.
*/
long long int sqlite3_value_int(sqlite3_value *pVal){
  Mem *pMem = (Mem *)pVal;
  return MemInt(pMem);
}

/*
** Return the value of the sqlite_value* argument coerced to a 64-bit
** IEEE float.
*/
double sqlite3_value_float(sqlite3_value *pVal){
  Mem *pMem = (Mem *)pVal;
  return MemReal(pMem);
}

/*
** Return the number of bytes of data that will be returned by the
** equivalent sqlite3_column_data() call.
*/
int sqlite3_column_bytes(sqlite3_stmt *pStmt, int i){
  Vdbe *pVm = (Vdbe *)pStmt;

  if( sqlite3_column_data(pStmt, i) ){
    int vals = sqlite3_data_count(pStmt);
    return pVm->pTos[(1-vals)+i].n;
  }
  return 0;
}

/*
** Return the number of bytes of data that will be returned by the
** equivalent sqlite3_column_data16() call.
*/
int sqlite3_column_bytes16(sqlite3_stmt *pStmt, int i){
  Vdbe *pVm = (Vdbe *)pStmt;

  if( sqlite3_column_data16(pStmt, i) ){
    int vals = sqlite3_data_count(pStmt);
    return pVm->pTos[(1-vals)+i].n;
  }
  return 0;
}

/*
** Return the value of the 'i'th column of the current row of the currently
** executing statement pStmt.
*/
long long int sqlite3_column_int(sqlite3_stmt *pStmt, int i){
  int vals;
  Vdbe *pVm = (Vdbe *)pStmt;
  Mem *pVal;

  vals = sqlite3_data_count(pStmt);
  if( i>=vals || i<0 ){
    sqlite3Error(pVm->db, SQLITE_RANGE, 0);
    return 0;
  }

  pVal = &pVm->pTos[(1-vals)+i];
  return sqlite3_value_int(pVal);
}

/*
** Return the value of the 'i'th column of the current row of the currently
** executing statement pStmt.
*/
double sqlite3_column_float(sqlite3_stmt *pStmt, int i){
  int vals;
  Vdbe *pVm = (Vdbe *)pStmt;
  Mem *pVal;

  vals = sqlite3_data_count(pStmt);
  if( i>=vals || i<0 ){
    sqlite3Error(pVm->db, SQLITE_RANGE, 0);
    return 0;
  }

  pVal = &pVm->pTos[(1-vals)+i];
  return sqlite3_value_float(pVal);
}

/*
** Return the name of the Nth column of the result set returned by SQL
** statement pStmt.
*/
const char *sqlite3_column_name(sqlite3_stmt *pStmt, int N){
  Vdbe *p = (Vdbe *)pStmt;
  Mem *pColName;

  if( N>=sqlite3_column_count(pStmt) || N<0 ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return 0;
  }

  pColName = &(p->aColName[N]);
  return sqlite3_value_data(pColName);
}

/*
** Return the name of the 'i'th column of the result set of SQL statement
** pStmt, encoded as UTF-16.
*/
const void *sqlite3_column_name16(sqlite3_stmt *pStmt, int N){
  Vdbe *p = (Vdbe *)pStmt;
  Mem *pColName;

  if( N>=sqlite3_column_count(pStmt) || N<0 ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return 0;
  }

  pColName = &(p->aColName[N]);
  return sqlite3_value_data16(pColName);
}


/*
** Return the type of the value stored in the sqlite_value* object.
*/
int sqlite3_value_type(sqlite3_value* pVal){
  int f = ((Mem *)pVal)->flags;
  if( f&MEM_Null ){
    return SQLITE3_NULL;
  }
  if( f&MEM_TypeInt ){
    return SQLITE3_INTEGER;
  }
  if( f&MEM_TypeReal ){
    return SQLITE3_FLOAT;
  }
  if( f&MEM_TypeStr ){
    return SQLITE3_TEXT;
  }
  if( f&MEM_Blob ){
    return SQLITE3_BLOB;
  }
  assert(0);
}

/*
** Return the type of the 'i'th column of the current row of the currently
** executing statement pStmt.
*/
int sqlite3_column_type(sqlite3_stmt *pStmt, int i){
  int vals;
  Vdbe *p = (Vdbe *)pStmt;

  vals = sqlite3_data_count(pStmt);
  if( i>=vals || i<0 ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return 0;
  }

  return sqlite3_value_type(&(p->pTos[(1-vals)+i]));
}

/*
** This routine returns either the column name, or declaration type (see
** sqlite3_column_decltype16() ) of the 'i'th column of the result set of
** SQL statement pStmt. The returned string is UTF-16 encoded.
**
** The declaration type is returned if 'decltype' is true, otherwise
** the column name.
*/
static const void *columnName16(sqlite3_stmt *pStmt, int i, int decltype){
  Vdbe *p = (Vdbe *)pStmt;

  if( i>=sqlite3_column_count(pStmt) || i<0 ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return 0;
  }

  if( decltype ){
    i += p->nResColumn;
  }

  if( !p->azColName16 ){
    p->azColName16 = (void **)sqliteMalloc(sizeof(void *)*p->nResColumn*2);
    if( !p->azColName16 ){
      sqlite3Error(p->db, SQLITE_NOMEM, 0);
      return 0;
    }
  }
  if( !p->azColName16[i] ){
    if( SQLITE3_BIGENDIAN ){
      p->azColName16[i] = sqlite3utf8to16be(p->azColName[i], -1);
    }
    if( !p->azColName16[i] ){
      sqlite3Error(p->db, SQLITE_NOMEM, 0);
      return 0;
    }
  }
  return p->azColName16[i];
}

/*
** Return the column declaration type (if applicable) of the 'i'th column
** of the result set of SQL statement pStmt, encoded as UTF-8.
*/
const char *sqlite3_column_decltype(sqlite3_stmt *pStmt, int i){
  Vdbe *p = (Vdbe *)pStmt;

  if( i>=sqlite3_column_count(pStmt) || i<0 ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return 0;
  }

  return p->azColName[i+p->nResColumn];
}

/*
** Return the column declaration type (if applicable) of the 'i'th column
** of the result set of SQL statement pStmt, encoded as UTF-16.
*/
const void *sqlite3_column_decltype16(sqlite3_stmt *pStmt, int i){
  return columnName16(pStmt, i, 1);
}

/*
** Unbind the value bound to variable $i in virtual machine p. This is the 
** the same as binding a NULL value to the column. If the "i" parameter is
** out of range, then SQLITE_RANGE is returned. Othewise SQLITE_OK.
**
** The error code stored in database p->db is overwritten with the return
** value in any case.
*/
static int vdbeUnbind(Vdbe *p, int i){
  Mem *pVar;
  if( p->magic!=VDBE_MAGIC_RUN || p->pc!=0 ){
    sqlite3Error(p->db, SQLITE_MISUSE, 0);
    return SQLITE_MISUSE;
  }
  if( i<1 || i>p->nVar ){
    sqlite3Error(p->db, SQLITE_RANGE, 0);
    return SQLITE_RANGE;
  }
  i--;
  pVar = &p->apVar[i];
  if( pVar->flags&MEM_Dyn ){
    sqliteFree(pVar->z);
  }
  pVar->flags = MEM_Null;
  sqlite3Error(p->db, SQLITE_OK, 0);
  return SQLITE_OK;
}

/*
** This routine is used to bind text or blob data to an SQL variable (a ?).
** It may also be used to bind a NULL value, by setting zVal to 0. Any
** existing value is unbound.
**
** The error code stored in p->db is overwritten with the return value in
** all cases.
*/
static int vdbeBindBlob(
  Vdbe *p,           /* Virtual machine */
  int i,             /* Var number to bind (numbered from 1 upward) */
  const char *zVal,  /* Pointer to blob of data */
  int bytes,         /* Number of bytes to copy */
  int copy,          /* True to copy the memory, false to copy a pointer */
  int flags          /* Valid combination of MEM_Blob, MEM_Str, MEM_Term */
){
  Mem *pVar;
  int rc;

  rc = vdbeUnbind(p, i);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pVar = &p->apVar[i-1];

  if( zVal ){
    pVar->n = bytes;
    pVar->flags = flags;
    if( !copy ){
      pVar->z = (char *)zVal;
      pVar->flags |= MEM_Static;
    }else{
      if( bytes>NBFS ){
        pVar->z = (char *)sqliteMalloc(bytes);
        if( !pVar->z ){
          sqlite3Error(p->db, SQLITE_NOMEM, 0);
          return SQLITE_NOMEM;
        }
        pVar->flags |= MEM_Dyn;
      }else{
        pVar->z = pVar->zShort;
        pVar->flags |= MEM_Short;
      }
      memcpy(pVar->z, zVal, bytes);
    }
  }

  return SQLITE_OK;
}

/*
** Bind a 64 bit integer to an SQL statement variable.
*/
int sqlite3_bind_int64(sqlite3_stmt *p, int i, long long int iValue){
  int rc;
  Vdbe *v = (Vdbe *)p;
  rc = vdbeUnbind(v, i);
  if( rc==SQLITE_OK ){
    Mem *pVar = &v->apVar[i-1];
    pVar->flags = MEM_Int;
    pVar->i = iValue;
  }
  return rc;
}

/*
** Bind a 32 bit integer to an SQL statement variable.
*/
int sqlite3_bind_int32(sqlite3_stmt *p, int i, int iValue){
  return sqlite3_bind_int64(p, i, (long long int)iValue);
}

/*
** Bind a double (real) to an SQL statement variable.
*/
int sqlite3_bind_double(sqlite3_stmt *p, int i, double iValue){
  int rc;
  Vdbe *v = (Vdbe *)p;
  rc = vdbeUnbind(v, i);
  if( rc==SQLITE_OK ){
    Mem *pVar = &v->apVar[i-1];
    pVar->flags = MEM_Real;
    pVar->r = iValue;
  }
  return SQLITE_OK;
}

/*
** Bind a NULL value to an SQL statement variable.
*/
int sqlite3_bind_null(sqlite3_stmt* p, int i){
  return vdbeUnbind((Vdbe *)p, i);
}

/*
** Bind a UTF-8 text value to an SQL statement variable.
*/
int sqlite3_bind_text( 
  sqlite3_stmt *pStmt, 
  int i, 
  const char *zData, 
  int nData, 
  int eCopy
){
  Mem *pVar;
  Vdbe *p = (Vdbe *)pStmt;
  int rc = SQLITE_OK;
  u8 db_enc = p->db->enc;            /* Text encoding of the database */

  /* Unbind any previous variable value */
  rc = vdbeUnbind(p, i);
  if( rc==SQLITE_OK ){
    pVar = &p->apVar[i-1];

    if( !zData ){
      /* If zData is NULL, then bind an SQL NULL value */
      pVar->flags = MEM_Null;
    }else{
      if( zData && nData<0 ){
        nData = strlen(zData) + 1;
      }
      pVar->z = (char *)zData;
      pVar->n = nData;
      pVar->flags = MEM_Utf8|MEM_Str|(zData[nData-1]?0:MEM_Term);
      if( !eCopy || db_enc!=TEXT_Utf8 ){
        pVar->flags |= MEM_Static;
        rc = SetEncoding(pVar, encToFlags(db_enc)|MEM_Term);
      }else{
        pVar->flags |= MEM_Ephem;
        Deephemeralize(pVar);
      }
    }
  }

  sqlite3Error(p->db, rc, 0);
  return rc;

no_mem:
  sqlite3Error(p->db, SQLITE_NOMEM, 0);
  return SQLITE_NOMEM;
}

/*
** Bind a UTF-16 text value to an SQL statement variable.
*/
int sqlite3_bind_text16(
  sqlite3_stmt *pStmt, 
  int i, 
  const void *zData, 
  int nData, 
  int eCopy
){
  Vdbe *p = (Vdbe *)pStmt;
  Mem *pVar;
  u8 db_enc = p->db->enc;            /* Text encoding of the database */
  u8 txt_enc;
  int null_term = 0;
  int rc;

  rc = vdbeUnbind(p, i);
  if( rc!=SQLITE_OK ){
    return rc;
  }
  pVar = &p->apVar[i-1];

  /* If zData is NULL, then bind an SQL NULL value */
  if( !zData ){
    pVar->flags = MEM_Null;
    return SQLITE_OK;
  }

  if( db_enc==TEXT_Utf8 ){
    /* If the database encoding is UTF-8, then do a translation. */
    pVar->z = sqlite3utf16to8(zData, nData, SQLITE3_BIGENDIAN);
    if( !pVar->z ) return SQLITE_NOMEM;
    pVar->n = strlen(pVar->z)+1;
    pVar->flags = MEM_Str|MEM_Term|MEM_Dyn;
    return SQLITE_OK;
  }
 
  /* There may or may not be a byte order mark at the start of the UTF-16.
  ** Either way set 'txt_enc' to the TEXT_Utf16* value indicating the 
  ** actual byte order used by this string. If the string does happen
  ** to contain a BOM, then move zData so that it points to the first
  ** byte after the BOM.
  */
  txt_enc = sqlite3UtfReadBom(zData, nData);
  if( txt_enc ){
    zData = (void *)(((u8 *)zData) + 2);
  }else{
    txt_enc = SQLITE3_BIGENDIAN?TEXT_Utf16be:TEXT_Utf16le;
  }

  if( nData<0 ){
    nData = sqlite3utf16ByteLen(zData, -1) + 2;
    null_term = 1;
  }else if( nData>1 && !((u8*)zData)[nData-1] && !((u8*)zData)[nData-2] ){
    null_term = 1;
  }

  if( db_enc==txt_enc && !eCopy ){
    /* If the byte order of the string matches the byte order of the
    ** database and the eCopy parameter is not set, then the string can
    ** be used without making a copy.
    */
    pVar->z = (char *)zData;
    pVar->n = nData;
    pVar->flags = MEM_Str|MEM_Static|(null_term?MEM_Term:0);
  }else{
    /* Make a copy. Swap the byte order if required */
    pVar->n = nData + (null_term?0:2);
    pVar->z = sqliteMalloc(pVar->n);
    pVar->flags = MEM_Str|MEM_Dyn|MEM_Term;
    if( db_enc==txt_enc ){
      memcpy(pVar->z, zData, nData);
    }else{
      swab(zData, pVar->z, nData);
    }
    pVar->z[pVar->n-1] = '\0';
    pVar->z[pVar->n-2] = '\0';
  }

  return SQLITE_OK;
}

/*
** Bind a blob value to an SQL statement variable.
*/
int sqlite3_bind_blob(
  sqlite3_stmt *p, 
  int i, 
  const void *zData, 
  int nData, 
  int eCopy
){
  return vdbeBindBlob((Vdbe *)p, i, zData, nData, eCopy, MEM_Blob);
}


/*
** Insert a new aggregate element and make it the element that
** has focus.
**
** Return 0 on success and 1 if memory is exhausted.
*/
static int AggInsert(Agg *p, char *zKey, int nKey){
  AggElem *pElem, *pOld;
  int i;
  Mem *pMem;
  pElem = sqliteMalloc( sizeof(AggElem) + nKey +
                        (p->nMem-1)*sizeof(pElem->aMem[0]) );
  if( pElem==0 ) return 1;
  pElem->zKey = (char*)&pElem->aMem[p->nMem];
  memcpy(pElem->zKey, zKey, nKey);
  pElem->nKey = nKey;
  pOld = sqlite3HashInsert(&p->hash, pElem->zKey, pElem->nKey, pElem);
  if( pOld!=0 ){
    assert( pOld==pElem );  /* Malloc failed on insert */
    sqliteFree(pOld);
    return 0;
  }
  for(i=0, pMem=pElem->aMem; i<p->nMem; i++, pMem++){
    pMem->flags = MEM_Null;
  }
  p->pCurrent = pElem;
  return 0;
}

/*
** Get the AggElem currently in focus
*/
#define AggInFocus(P)   ((P).pCurrent ? (P).pCurrent : _AggInFocus(&(P)))
static AggElem *_AggInFocus(Agg *p){
  HashElem *pElem = sqliteHashFirst(&p->hash);
  if( pElem==0 ){
    AggInsert(p,"",1);
    pElem = sqliteHashFirst(&p->hash);
  }
  return pElem ? sqliteHashData(pElem) : 0;
}

/*
** Pop the stack N times.
*/
static void popStack(Mem **ppTos, int N){
  Mem *pTos = *ppTos;
  while( N>0 ){
    N--;
    Release(pTos);
    pTos--;
  }
  *ppTos = pTos;
}

/*
** The parameters are pointers to the head of two sorted lists
** of Sorter structures.  Merge these two lists together and return
** a single sorted list.  This routine forms the core of the merge-sort
** algorithm.
**
** In the case of a tie, left sorts in front of right.
*/
static Sorter *Merge(Sorter *pLeft, Sorter *pRight, KeyInfo *pKeyInfo){
  Sorter sHead;
  Sorter *pTail;
  pTail = &sHead;
  pTail->pNext = 0;
  while( pLeft && pRight ){
    int c = sqlite3VdbeKeyCompare(pKeyInfo, pLeft->nKey, pLeft->zKey,
                                  pRight->nKey, pRight->zKey);
    /* int c = sqlite3SortCompare(pLeft->zKey, pRight->zKey); */
    if( c<=0 ){
      pTail->pNext = pLeft;
      pLeft = pLeft->pNext;
    }else{
      pTail->pNext = pRight;
      pRight = pRight->pNext;
    }
    pTail = pTail->pNext;
  }
  if( pLeft ){
    pTail->pNext = pLeft;
  }else if( pRight ){
    pTail->pNext = pRight;
  }
  return sHead.pNext;
}

/*
** The following routine works like a replacement for the standard
** library routine fgets().  The difference is in how end-of-line (EOL)
** is handled.  Standard fgets() uses LF for EOL under unix, CRLF
** under windows, and CR under mac.  This routine accepts any of these
** character sequences as an EOL mark.  The EOL mark is replaced by
** a single LF character in zBuf.
*/
static char *vdbe_fgets(char *zBuf, int nBuf, FILE *in){
  int i, c;
  for(i=0; i<nBuf-1 && (c=getc(in))!=EOF; i++){
    zBuf[i] = c;
    if( c=='\r' || c=='\n' ){
      if( c=='\r' ){
        zBuf[i] = '\n';
        c = getc(in);
        if( c!=EOF && c!='\n' ) ungetc(c, in);
      }
      i++;
      break;
    }
  }
  zBuf[i]  = 0;
  return i>0 ? zBuf : 0;
}

/*
** Make sure there is space in the Vdbe structure to hold at least
** mxCursor cursors.  If there is not currently enough space, then
** allocate more.
**
** If a memory allocation error occurs, return 1.  Return 0 if
** everything works.
*/
static int expandCursorArraySize(Vdbe *p, int mxCursor){
  if( mxCursor>=p->nCursor ){
    p->apCsr = sqliteRealloc( p->apCsr, (mxCursor+1)*sizeof(Cursor*) );
    if( p->apCsr==0 ) return 1;
    while( p->nCursor<=mxCursor ){
      Cursor *pC;
      p->apCsr[p->nCursor++] = pC = sqliteMalloc( sizeof(Cursor) );
      if( pC==0 ) return 1;
    }
  }
  return 0;
}

/*
** Apply any conversion required by the supplied column affinity to
** memory cell pRec. affinity may be one of:
**
** SQLITE_AFF_NUMERIC
** SQLITE_AFF_TEXT
** SQLITE_AFF_NONE
** SQLITE_AFF_INTEGER
**
*/
static void applyAffinity(Mem *pRec, char affinity, u8 enc){
  switch( affinity ){
    case SQLITE_AFF_INTEGER:
    case SQLITE_AFF_NUMERIC:
      if( 0==(pRec->flags&(MEM_Real|MEM_Int)) ){
        /* pRec does not have a valid integer or real representation. 
        ** Attempt a conversion if pRec has a string representation and
        ** it looks like a number.
        */
        int realnum;
        if( pRec->flags&MEM_Str && sqlite3IsNumber(pRec->z, &realnum, enc) ){
          if( realnum ){
            Realify(pRec, enc);
          }else{
            Integerify(pRec, enc);
          }
        }
      }

      if( affinity==SQLITE_AFF_INTEGER ){
        /* For INTEGER affinity, try to convert a real value to an int */
        if( pRec->flags&MEM_Real ){
          pRec->i = pRec->r;
          if( ((double)pRec->i)==pRec->r ){
            pRec->flags |= MEM_Int;
          }
        }
      }
      break;

    case SQLITE_AFF_TEXT:
      /* Only attempt the conversion if there is an integer or real
      ** representation (blob and NULL do not get converted) but no string
      ** representation.
      */
      if( 0==(pRec->flags&MEM_Str) && (pRec->flags&(MEM_Real|MEM_Int)) ){
        Stringify(pRec, enc);
      }
      pRec->flags &= ~(MEM_Real|MEM_Int);

      break;

    case SQLITE_AFF_NONE:
      /* Affinity NONE. Do nothing. */
      break;

    default:
      assert(0);
  }
}

#ifndef NDEBUG
/*
** Write a nice string representation of the contents of cell pMem
** into buffer zBuf, length nBuf.
*/
void prettyPrintMem(Mem *pMem, char *zBuf, int nBuf){
  char *zCsr = zBuf;
  int f = pMem->flags;

  if( f&MEM_Blob ){
    int i;
    char c;
    if( f & MEM_Dyn ){
      c = 'z';
      assert( (f & (MEM_Static|MEM_Ephem))==0 );
    }else if( f & MEM_Static ){
      c = 't';
      assert( (f & (MEM_Dyn|MEM_Ephem))==0 );
    }else if( f & MEM_Ephem ){
      c = 'e';
      assert( (f & (MEM_Static|MEM_Dyn))==0 );
    }else{
      c = 's';
    }

    zCsr += sprintf(zCsr, "%c", c);
    zCsr += sprintf(zCsr, "%d[", pMem->n);
    for(i=0; i<16 && i<pMem->n; i++){
      zCsr += sprintf(zCsr, "%02X ", ((int)pMem->z[i] & 0xFF));
    }
    for(i=0; i<16 && i<pMem->n; i++){
      char z = pMem->z[i];
      if( z<32 || z>126 ) *zCsr++ = '.';
      else *zCsr++ = z;
    }

    zCsr += sprintf(zCsr, "]");
    *zCsr = '\0';
  }else if( f & MEM_Str ){
    int j, k;
    zBuf[0] = ' ';
    if( f & MEM_Dyn ){
      zBuf[1] = 'z';
      assert( (f & (MEM_Static|MEM_Ephem))==0 );
    }else if( f & MEM_Static ){
      zBuf[1] = 't';
      assert( (f & (MEM_Dyn|MEM_Ephem))==0 );
    }else if( f & MEM_Ephem ){
      zBuf[1] = 'e';
      assert( (f & (MEM_Static|MEM_Dyn))==0 );
    }else{
      zBuf[1] = 's';
    }
    k = 2;
    k += sprintf(&zBuf[k], "%d", pMem->n);
    zBuf[k++] = '[';
    for(j=0; j<15 && j<pMem->n; j++){
      u8 c = pMem->z[j];
/*
      if( c==0 && j==pMem->n-1 ) break;
            zBuf[k++] = "0123456789ABCDEF"[c>>4];
            zBuf[k++] = "0123456789ABCDEF"[c&0xf];
*/
      if( c>=0x20 && c<0x7f ){
        zBuf[k++] = c;
      }else{
        zBuf[k++] = '.';
      }
    }
    zBuf[k++] = ']';
    zBuf[k++] = 0;
  }
}

/* Temporary - this is useful in conjunction with prettyPrintMem whilst
** debugging. 
*/
char zGdbBuf[100];
#endif

/*
** Move data out of a btree key or data field and into a Mem structure.
** The data or key is taken from the entry that pCur is currently pointing
** to.  offset and amt determine what portion of the data or key to retrieve.
** key is true to get the key or false to get data.  The result is written
** into the pMem element.
*/
static int getBtreeMem(
  BtCursor *pCur,   /* Cursor pointing at record to retrieve. */
  int offset,       /* Offset from the start of data to return bytes from. */
  int amt,          /* Number of bytes to return. */
  int key,          /* If true, retrieve from the btree key, not data. */
  Mem *pMem         /* OUT: Return data in this Mem structure. */
){
  char *zData;

  if( key ){
    zData = (char *)sqlite3BtreeKeyFetch(pCur, offset+amt);
  }else{
    zData = (char *)sqlite3BtreeDataFetch(pCur, offset+amt);
  }

  if( zData ){
    pMem->z = &zData[offset];
    pMem->n = amt;
    pMem->flags = MEM_Blob|MEM_Ephem;
  }else{
    int rc;
    if( amt>NBFS ){
      zData = (char *)sqliteMallocRaw(amt);
      if( !zData ){
        return SQLITE_NOMEM;
      }
      pMem->flags = MEM_Blob|MEM_Dyn;
    }else{
      zData = &(pMem->zShort[0]);
      pMem->flags = MEM_Blob|MEM_Short;
    }
    pMem->z = zData;

    if( key ){
      rc = sqlite3BtreeKey(pCur, offset, amt, zData);
    }else{
      rc = sqlite3BtreeData(pCur, offset, amt, zData);
    }

    if( rc!=SQLITE_OK ){
      if( amt>NBFS ){
        sqliteFree(zData);
      }
      return rc;
    }
  }

  return SQLITE_OK;
}


#ifdef VDBE_PROFILE
/*
** The following routine only works on pentium-class processors.
** It uses the RDTSC opcode to read cycle count value out of the
** processor and returns that value.  This can be used for high-res
** profiling.
*/
__inline__ unsigned long long int hwtime(void){
  unsigned long long int x;
  __asm__("rdtsc\n\t"
          "mov %%edx, %%ecx\n\t"
          :"=A" (x));
  return x;
}
#endif

/*
** The CHECK_FOR_INTERRUPT macro defined here looks to see if the
** sqlite3_interrupt() routine has been called.  If it has been, then
** processing of the VDBE program is interrupted.
**
** This macro added to every instruction that does a jump in order to
** implement a loop.  This test used to be on every single instruction,
** but that meant we more testing that we needed.  By only testing the
** flag on jump instructions, we get a (small) speed improvement.
*/
#define CHECK_FOR_INTERRUPT \
   if( db->flags & SQLITE_Interrupt ) goto abort_due_to_interrupt;


/*
** Execute as much of a VDBE program as we can then return.
**
** sqlite3VdbeMakeReady() must be called before this routine in order to
** close the program with a final OP_Halt and to set up the callbacks
** and the error message pointer.
**
** Whenever a row or result data is available, this routine will either
** invoke the result callback (if there is one) or return with
** SQLITE_ROW.
**
** If an attempt is made to open a locked database, then this routine
** will either invoke the busy callback (if there is one) or it will
** return SQLITE_BUSY.
**
** If an error occurs, an error message is written to memory obtained
** from sqliteMalloc() and p->zErrMsg is made to point to that memory.
** The error code is stored in p->rc and this routine returns SQLITE_ERROR.
**
** If the callback ever returns non-zero, then the program exits
** immediately.  There will be no error message but the p->rc field is
** set to SQLITE_ABORT and this routine will return SQLITE_ERROR.
**
** A memory allocation error causes p->rc to be set to SQLITE_NOMEM and this
** routine to return SQLITE_ERROR.
**
** Other fatal errors return SQLITE_ERROR.
**
** After this routine has finished, sqlite3VdbeFinalize() should be
** used to clean up the mess that was left behind.
*/
int sqlite3VdbeExec(
  Vdbe *p                    /* The VDBE */
){
  int pc;                    /* The program counter */
  Op *pOp;                   /* Current operation */
  int rc = SQLITE_OK;        /* Value to return */
  sqlite *db = p->db;        /* The database */
  Mem *pTos;                 /* Top entry in the operand stack */
  char zBuf[100];            /* Space to sprintf() an integer */
#ifdef VDBE_PROFILE
  unsigned long long start;  /* CPU clock count at start of opcode */
  int origPc;                /* Program counter at start of opcode */
#endif
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
  int nProgressOps = 0;      /* Opcodes executed since progress callback. */
#endif

  if( p->magic!=VDBE_MAGIC_RUN ) return SQLITE_MISUSE;
  assert( db->magic==SQLITE_MAGIC_BUSY );
  assert( p->rc==SQLITE_OK || p->rc==SQLITE_BUSY );
  p->rc = SQLITE_OK;
  assert( p->explain==0 );
  if( sqlite3_malloc_failed ) goto no_mem;
  pTos = p->pTos;
  if( p->popStack ){
    popStack(&pTos, p->popStack);
    p->popStack = 0;
  }
  p->resOnStack = 0;
  CHECK_FOR_INTERRUPT;
  for(pc=p->pc; rc==SQLITE_OK; pc++){
    assert( pc>=0 && pc<p->nOp );
    assert( pTos<=&p->aStack[pc] );
#ifdef VDBE_PROFILE
    origPc = pc;
    start = hwtime();
#endif
    pOp = &p->aOp[pc];

    /* Only allow tracing if NDEBUG is not defined.
    */
#ifndef NDEBUG
    if( p->trace ){
      sqlite3VdbePrintOp(p->trace, pc, pOp);
    }
#endif

    /* Check to see if we need to simulate an interrupt.  This only happens
    ** if we have a special test build.
    */
#ifdef SQLITE_TEST
    if( sqlite3_interrupt_count>0 ){
      sqlite3_interrupt_count--;
      if( sqlite3_interrupt_count==0 ){
        sqlite3_interrupt(db);
      }
    }
#endif

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
    /* Call the progress callback if it is configured and the required number
    ** of VDBE ops have been executed (either since this invocation of
    ** sqlite3VdbeExec() or since last time the progress callback was called).
    ** If the progress callback returns non-zero, exit the virtual machine with
    ** a return code SQLITE_ABORT.
    */
    if( db->xProgress ){
      if( db->nProgressOps==nProgressOps ){
        if( db->xProgress(db->pProgressArg)!=0 ){
          rc = SQLITE_ABORT;
          continue; /* skip to the next iteration of the for loop */
        }
        nProgressOps = 0;
      }
      nProgressOps++;
    }
#endif

    switch( pOp->opcode ){

/*****************************************************************************
** What follows is a massive switch statement where each case implements a
** separate instruction in the virtual machine.  If we follow the usual
** indentation conventions, each case should be indented by 6 spaces.  But
** that is a lot of wasted space on the left margin.  So the code within
** the switch statement will break with convention and be flush-left. Another
** big comment (similar to this one) will mark the point in the code where
** we transition back to normal indentation.
**
** The formatting of each case is important.  The makefile for SQLite
** generates two C files "opcodes.h" and "opcodes.c" by scanning this
** file looking for lines that begin with "case OP_".  The opcodes.h files
** will be filled with #defines that give unique integer values to each
** opcode and the opcodes.c file is filled with an array of strings where
** each string is the symbolic name for the corresponding opcode.
**
** Documentation about VDBE opcodes is generated by scanning this file
** for lines of that contain "Opcode:".  That line and all subsequent
** comment lines are used in the generation of the opcode.html documentation
** file.
**
** SUMMARY:
**
**     Formatting is important to scripts that scan this file.
**     Do not deviate from the formatting style currently in use.
**
*****************************************************************************/

/* Opcode:  Goto * P2 *
**
** An unconditional jump to address P2.
** The next instruction executed will be 
** the one at index P2 from the beginning of
** the program.
*/
case OP_Goto: {
  CHECK_FOR_INTERRUPT;
  pc = pOp->p2 - 1;
  break;
}

/* Opcode:  Gosub * P2 *
**
** Push the current address plus 1 onto the return address stack
** and then jump to address P2.
**
** The return address stack is of limited depth.  If too many
** OP_Gosub operations occur without intervening OP_Returns, then
** the return address stack will fill up and processing will abort
** with a fatal error.
*/
case OP_Gosub: {
  if( p->returnDepth>=sizeof(p->returnStack)/sizeof(p->returnStack[0]) ){
    sqlite3SetString(&p->zErrMsg, "return address stack overflow", (char*)0);
    p->rc = SQLITE_INTERNAL;
    return SQLITE_ERROR;
  }
  p->returnStack[p->returnDepth++] = pc+1;
  pc = pOp->p2 - 1;
  break;
}

/* Opcode:  Return * * *
**
** Jump immediately to the next instruction after the last unreturned
** OP_Gosub.  If an OP_Return has occurred for all OP_Gosubs, then
** processing aborts with a fatal error.
*/
case OP_Return: {
  if( p->returnDepth<=0 ){
    sqlite3SetString(&p->zErrMsg, "return address stack underflow", (char*)0);
    p->rc = SQLITE_INTERNAL;
    return SQLITE_ERROR;
  }
  p->returnDepth--;
  pc = p->returnStack[p->returnDepth] - 1;
  break;
}

/* Opcode:  Halt P1 P2 *
**
** Exit immediately.  All open cursors, Lists, Sorts, etc are closed
** automatically.
**
** P1 is the result code returned by sqlite3_exec().  For a normal
** halt, this should be SQLITE_OK (0).  For errors, it can be some
** other value.  If P1!=0 then P2 will determine whether or not to
** rollback the current transaction.  Do not rollback if P2==OE_Fail.
** Do the rollback if P2==OE_Rollback.  If P2==OE_Abort, then back
** out all changes that have occurred during this execution of the
** VDBE, but do not rollback the transaction. 
**
** There is an implied "Halt 0 0 0" instruction inserted at the very end of
** every program.  So a jump past the last instruction of the program
** is the same as executing Halt.
*/
case OP_Halt: {
  p->magic = VDBE_MAGIC_HALT;
  p->pTos = pTos;
  if( pOp->p1!=SQLITE_OK ){
    p->rc = pOp->p1;
    p->errorAction = pOp->p2;
    if( pOp->p3 ){
      sqlite3SetString(&p->zErrMsg, pOp->p3, (char*)0);
    }
    return SQLITE_ERROR;
  }else{
    p->rc = SQLITE_OK;
    return SQLITE_DONE;
  }
}

/* Opcode: String * * P3
**
** The string value P3 is pushed onto the stack.  If P3==0 then a
** NULL is pushed onto the stack.
*/
/* Opcode: Real * * P3
**
** The string value P3 is converted to a real and pushed on to the stack.
*/
/* Opcode: Integer P1 * P3
**
** The integer value P1 is pushed onto the stack.  If P3 is not zero
** then it is assumed to be a string representation of the same integer.
** If P1 is zero and P3 is not zero, then the value is derived from P3.
*/
case OP_Integer:
case OP_Real:
case OP_String: {
  char *z = pOp->p3;
  u8 op = pOp->opcode;

  pTos++;
  pTos->flags = 0;
 
  if( z ){
    /* FIX ME: For now the code in expr.c always puts UTF-8 in P3. It
    ** should transform text to the native encoding before doing so.
    */
    MemSetStr(pTos, z, -1, TEXT_Utf8, 0);
    SetEncoding(pTos, encToFlags(db->enc)|MEM_Term);
  }else if( op==OP_String ){
    pTos->flags = MEM_Null;
  }

  /* If this is an OP_Real or OP_Integer opcode, set the pTos->r or pTos->i
  ** values respectively.
  */
  if( op==OP_Real ){
    assert( z );
    assert( sqlite3IsNumber(z, 0, TEXT_Utf8) );
    pTos->r = sqlite3AtoF(z, 0);
    pTos->flags |= MEM_Real;
  }else if( op==OP_Integer ){
    pTos->i = pOp->p1;
    if( pTos->i==0 && pOp->p3 ){
      sqlite3GetInt64(z, &pTos->i);
    }
    pTos->flags |= MEM_Int;
  }

  break;
}

/* Opcode: Variable P1 * *
**
** Push the value of variable P1 onto the stack.  A variable is
** an unknown in the original SQL string as handed to sqlite3_compile().
** Any occurance of the '?' character in the original SQL is considered
** a variable.  Variables in the SQL string are number from left to
** right beginning with 1.  The values of variables are set using the
** sqlite3_bind() API.
*/
case OP_Variable: {
  int j = pOp->p1 - 1;
  assert( j>=0 && j<p->nVar );

  pTos++;
  memcpy(pTos, &p->apVar[j], sizeof(*pTos)-NBFS);
  if( pTos->flags&(MEM_Str|MEM_Blob) ){
    pTos->flags &= ~(MEM_Dyn|MEM_Ephem|MEM_Short);
    pTos->flags |= MEM_Static;
  }
  break;
}

/* Opcode: Utf16le_8 * * *
**
** The element on the top of the stack must be a little-endian UTF-16
** encoded string. It is translated in-place to UTF-8.
*/
case OP_Utf16le_8: {
  rc = SQLITE_INTERNAL;
  break;
}

/* Opcode: Utf16be_8 * * *
**
** The element on the top of the stack must be a big-endian UTF-16
** encoded string. It is translated in-place to UTF-8.
*/
case OP_Utf16be_8: {
  rc = SQLITE_INTERNAL;
  break;
}

/* Opcode: Utf8_16be * * *
**
** The element on the top of the stack must be a UTF-8 encoded
** string. It is translated to big-endian UTF-16.
*/
case OP_Utf8_16be: {
  rc = SQLITE_INTERNAL;
  break;
}

/* Opcode: Utf8_16le * * *
**
** The element on the top of the stack must be a UTF-8 encoded
** string. It is translated to little-endian UTF-16.
*/
case OP_Utf8_16le: {
  rc = SQLITE_INTERNAL;
  break;
}

/*
** Opcode: UtfSwab
**
** The element on the top of the stack must be an UTF-16 encoded
** string. Every second byte is exchanged, so as to translate
** the string from little-endian to big-endian or vice versa.
*/
case OP_UtfSwab: {
  rc = SQLITE_INTERNAL;
  break;
}

/* Opcode: Pop P1 * *
**
** P1 elements are popped off of the top of stack and discarded.
*/
case OP_Pop: {
  assert( pOp->p1>=0 );
  popStack(&pTos, pOp->p1);
  assert( pTos>=&p->aStack[-1] );
  break;
}

/* Opcode: Dup P1 P2 *
**
** A copy of the P1-th element of the stack 
** is made and pushed onto the top of the stack.
** The top of the stack is element 0.  So the
** instruction "Dup 0 0 0" will make a copy of the
** top of the stack.
**
** If the content of the P1-th element is a dynamically
** allocated string, then a new copy of that string
** is made if P2==0.  If P2!=0, then just a pointer
** to the string is copied.
**
** Also see the Pull instruction.
*/
case OP_Dup: {
  Mem *pFrom = &pTos[-pOp->p1];
  assert( pFrom<=pTos && pFrom>=p->aStack );
  pTos++;
  memcpy(pTos, pFrom, sizeof(*pFrom)-NBFS);
  if( pTos->flags & (MEM_Str|MEM_Blob) ){
    if( pOp->p2 && (pTos->flags & (MEM_Dyn|MEM_Ephem)) ){
      pTos->flags &= ~MEM_Dyn;
      pTos->flags |= MEM_Ephem;
    }else if( pTos->flags & MEM_Short ){
      memcpy(pTos->zShort, pFrom->zShort, pTos->n);
      pTos->z = pTos->zShort;
    }else if( (pTos->flags & MEM_Static)==0 ){
      pTos->z = sqliteMallocRaw(pFrom->n);
      if( sqlite3_malloc_failed ) goto no_mem;
      memcpy(pTos->z, pFrom->z, pFrom->n);
      pTos->flags &= ~(MEM_Static|MEM_Ephem|MEM_Short);
      pTos->flags |= MEM_Dyn;
    }
  }
  break;
}

/* Opcode: Pull P1 * *
**
** The P1-th element is removed from its current location on 
** the stack and pushed back on top of the stack.  The
** top of the stack is element 0, so "Pull 0 0 0" is
** a no-op.  "Pull 1 0 0" swaps the top two elements of
** the stack.
**
** See also the Dup instruction.
*/
case OP_Pull: {
  Mem *pFrom = &pTos[-pOp->p1];
  int i;
  Mem ts;

  ts = *pFrom;
  Deephemeralize(pTos);
  for(i=0; i<pOp->p1; i++, pFrom++){
    Deephemeralize(&pFrom[1]);
    assert( (pFrom->flags & MEM_Ephem)==0 );
    *pFrom = pFrom[1];
    if( pFrom->flags & MEM_Short ){
      assert( pFrom->flags & (MEM_Str|MEM_Blob) );
      assert( pFrom->z==pFrom[1].zShort );
      pFrom->z = pFrom->zShort;
    }
  }
  *pTos = ts;
  if( pTos->flags & MEM_Short ){
    assert( pTos->flags & (MEM_Str|MEM_Blob) );
    assert( pTos->z==pTos[-pOp->p1].zShort );
    pTos->z = pTos->zShort;
  }
  break;
}

/* Opcode: Push P1 * *
**
** Overwrite the value of the P1-th element down on the
** stack (P1==0 is the top of the stack) with the value
** of the top of the stack.  Then pop the top of the stack.
*/
case OP_Push: {
  Mem *pTo = &pTos[-pOp->p1];

  assert( pTo>=p->aStack );
  Deephemeralize(pTos);
  Release(pTo);
  *pTo = *pTos;
  if( pTo->flags & MEM_Short ){
    assert( pTo->z==pTos->zShort );
    pTo->z = pTo->zShort;
  }
  pTos--;
  break;
}


/* Opcode: ColumnName P1 P2 P3
**
** P3 becomes the P1-th column name (first is 0).  An array of pointers
** to all column names is passed as the 4th parameter to the callback.
** If P2==1 then this is the last column in the result set and thus the
** number of columns in the result set will be P1.  There must be at least
** one OP_ColumnName with a P2==1 before invoking OP_Callback and the
** number of columns specified in OP_Callback must one more than the P1
** value of the OP_ColumnName that has P2==1.
*/
case OP_ColumnName: {
  assert(0);
  assert( pOp->p1>=0 && pOp->p1<p->nOp );
  p->azColName[pOp->p1] = pOp->p3;
  p->nCallback = 0;
  assert( !pOp->p2 || p->nResColumn==(pOp->p1+1) );
  /* if( pOp->p2 ) p->nResColumn = pOp->p1+1; */
  break;
}

/* Opcode: Callback P1 * *
**
** Pop P1 values off the stack and form them into an array.  Then
** invoke the callback function using the newly formed array as the
** 3rd parameter.
*/
case OP_Callback: {
  int i;
  assert( p->nResColumn==pOp->p1 );

  for(i=0; i<pOp->p1; i++){
    Mem *pVal = &pTos[0-i];
    SetEncodingFlags(pVal, db->enc);
    MemNulTerminate(pVal);
    MemSetTypeFlags(pVal);
  }

  p->resOnStack = 1;
  p->nCallback++;
  p->popStack = pOp->p1;
  p->pc = pc + 1;
  p->pTos = pTos;
  return SQLITE_ROW;
}

/* Opcode: Concat P1 P2 P3
**
** Look at the first P1 elements of the stack.  Append them all 
** together with the lowest element first.  Use P3 as a separator.  
** Put the result on the top of the stack.  The original P1 elements
** are popped from the stack if P2==0 and retained if P2==1.  If
** any element of the stack is NULL, then the result is NULL.
**
** If P3 is NULL, then use no separator.  When P1==1, this routine
** makes a copy of the top stack element into memory obtained
** from sqliteMalloc().
*/
case OP_Concat: {
  char *zNew;
  int nByte;
  int nField;
  int i, j;
  Mem *pTerm;
  Mem zSep; /* Memory cell containing the seperator string, if any */
  int termLen;  /* Bytes in the terminator character for this encoding */

  termLen = (db->enc==TEXT_Utf8?1:2);

  /* FIX ME: Eventually, P3 will be in database native encoding. But for
  ** now it is always UTF-8. So set up zSep to hold the native encoding of
  ** P3.
  */
  if( pOp->p3 ){
    zSep.z = pOp->p3;
    zSep.n = strlen(zSep.z)+1;
    zSep.flags = MEM_Str|MEM_Static|MEM_Utf8|MEM_Term;
    SetEncoding(&zSep, encToFlags(db->enc)|MEM_Term);
  }else{
    zSep.flags = MEM_Null;
    zSep.n = 0;
  }

  /* Loop through the stack elements to see how long the result will be. */
  nField = pOp->p1;
  pTerm = &pTos[1-nField];
  nByte = termLen + (nField-1)*(zSep.n - ((zSep.flags&MEM_Term)?termLen:0));
  for(i=0; i<nField; i++, pTerm++){
    assert( pOp->p2==0 || (pTerm->flags&MEM_Str) );
    if( pTerm->flags&MEM_Null ){
      nByte = -1;
      break;
    }
    Stringify(pTerm, db->enc);
    nByte += (pTerm->n - ((pTerm->flags&MEM_Term)?termLen:0));
  }

  if( nByte<0 ){
    /* If nByte is less than zero, then there is a NULL value on the stack.
    ** In this case just pop the values off the stack (if required) and
    ** push on a NULL.
    */
    if( pOp->p2==0 ){
      popStack(&pTos, nField);
    }
    pTos++;
    pTos->flags = MEM_Null;
  }else{
    /* Otherwise malloc() space for the result and concatenate all the
    ** stack values.
    */
    zNew = sqliteMallocRaw( nByte );
    if( zNew==0 ) goto no_mem;
    j = 0;
    pTerm = &pTos[1-nField];
    for(i=j=0; i<nField; i++, pTerm++){
      int n = pTerm->n-((pTerm->flags&MEM_Term)?termLen:0);
      assert( pTerm->flags & MEM_Str );
      memcpy(&zNew[j], pTerm->z, n);
      j += n;
      if( i<nField-1 && !(zSep.flags|MEM_Null) ){
        n = zSep.n-((zSep.flags&MEM_Term)?termLen:0);
        memcpy(&zNew[j], zSep.z, n);
        j += n;
      }
    }
    zNew[j++] = 0;
    if( termLen==2 ){
      zNew[j++] = 0;
    }
    assert( j==nByte );

    if( pOp->p2==0 ){
      popStack(&pTos, nField);
    }
    pTos++;
    pTos->n = nByte;
    pTos->flags = MEM_Str|MEM_Dyn|MEM_Term|encToFlags(db->enc);
    pTos->z = zNew;
  }
  break;
}

/* Opcode: Add * * *
**
** Pop the top two elements from the stack, add them together,
** and push the result back onto the stack.  If either element
** is a string then it is converted to a double using the atof()
** function before the addition.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: Multiply * * *
**
** Pop the top two elements from the stack, multiply them together,
** and push the result back onto the stack.  If either element
** is a string then it is converted to a double using the atof()
** function before the multiplication.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: Subtract * * *
**
** Pop the top two elements from the stack, subtract the
** first (what was on top of the stack) from the second (the
** next on stack)
** and push the result back onto the stack.  If either element
** is a string then it is converted to a double using the atof()
** function before the subtraction.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: Divide * * *
**
** Pop the top two elements from the stack, divide the
** first (what was on top of the stack) from the second (the
** next on stack)
** and push the result back onto the stack.  If either element
** is a string then it is converted to a double using the atof()
** function before the division.  Division by zero returns NULL.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: Remainder * * *
**
** Pop the top two elements from the stack, divide the
** first (what was on top of the stack) from the second (the
** next on stack)
** and push the remainder after division onto the stack.  If either element
** is a string then it is converted to a double using the atof()
** function before the division.  Division by zero returns NULL.
** If either operand is NULL, the result is NULL.
*/
case OP_Add:
case OP_Subtract:
case OP_Multiply:
case OP_Divide:
case OP_Remainder: {
  Mem *pNos = &pTos[-1];
  assert( pNos>=p->aStack );
  if( ((pTos->flags | pNos->flags) & MEM_Null)!=0 ){
    Release(pTos);
    pTos--;
    Release(pTos);
    pTos->flags = MEM_Null;
  }else if( (pTos->flags & pNos->flags & MEM_Int)==MEM_Int ){
    i64 a, b;
    a = pTos->i;
    b = pNos->i;
    switch( pOp->opcode ){
      case OP_Add:         b += a;       break;
      case OP_Subtract:    b -= a;       break;
      case OP_Multiply:    b *= a;       break;
      case OP_Divide: {
        if( a==0 ) goto divide_by_zero;
        b /= a;
        break;
      }
      default: {
        if( a==0 ) goto divide_by_zero;
        b %= a;
        break;
      }
    }
    Release(pTos);
    pTos--;
    Release(pTos);
    pTos->i = b;
    pTos->flags = MEM_Int;
  }else{
    double a, b;
    Realify(pTos, db->enc);
    Realify(pNos, db->enc);
    a = pTos->r;
    b = pNos->r;
    switch( pOp->opcode ){
      case OP_Add:         b += a;       break;
      case OP_Subtract:    b -= a;       break;
      case OP_Multiply:    b *= a;       break;
      case OP_Divide: {
        if( a==0.0 ) goto divide_by_zero;
        b /= a;
        break;
      }
      default: {
        int ia = (int)a;
        int ib = (int)b;
        if( ia==0.0 ) goto divide_by_zero;
        b = ib % ia;
        break;
      }
    }
    Release(pTos);
    pTos--;
    Release(pTos);
    pTos->r = b;
    pTos->flags = MEM_Real;
  }
  break;

divide_by_zero:
  Release(pTos);
  pTos--;
  Release(pTos);
  pTos->flags = MEM_Null;
  break;
}

/* Opcode: Function P1 * P3
**
** Invoke a user function (P3 is a pointer to a Function structure that
** defines the function) with P1 string arguments taken from the stack.
** Pop all arguments from the stack and push back the result.
**
** See also: AggFunc
*/
case OP_Function: {
  int i;
  Mem *pArg;
  sqlite3_context ctx;
  sqlite3_value **apVal;
  int n = pOp->p1;

  n = pOp->p1;
  apVal = p->apArg;
  assert( apVal || n==0 );

  pArg = &pTos[1-n];
  for(i=0; i<n; i++, pArg++){
    SetEncodingFlags(pArg, db->enc);
    MemSetTypeFlags(pArg);
    apVal[i] = pArg;
  }

  ctx.pFunc = (FuncDef*)pOp->p3;
  ctx.s.flags = MEM_Null;
  ctx.s.z = 0;
  ctx.isError = 0;
  ctx.isStep = 0;
  if( sqlite3SafetyOff(db) ) goto abort_due_to_misuse;
  (*ctx.pFunc->xFunc)(&ctx, n, apVal);
  if( sqlite3SafetyOn(db) ) goto abort_due_to_misuse;
  popStack(&pTos, n);

  /* Copy the result of the function to the top of the stack */
  pTos++;
  *pTos = ctx.s;
  if( pTos->flags & MEM_Short ){
    pTos->z = pTos->zShort;
  }
  /* If the function returned an error, throw an exception */
  if( ctx.isError ){
    sqlite3SetString(&p->zErrMsg, 
       (pTos->flags & MEM_Str)!=0 ? pTos->z : "user function error", (char*)0);
    rc = SQLITE_ERROR;
  }

  if( pTos->flags&MEM_Str ){
    SetEncoding(pTos, encToFlags(db->enc)|MEM_Term);
  }

  break;
}

/* Opcode: BitAnd * * *
**
** Pop the top two elements from the stack.  Convert both elements
** to integers.  Push back onto the stack the bit-wise AND of the
** two elements.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: BitOr * * *
**
** Pop the top two elements from the stack.  Convert both elements
** to integers.  Push back onto the stack the bit-wise OR of the
** two elements.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: ShiftLeft * * *
**
** Pop the top two elements from the stack.  Convert both elements
** to integers.  Push back onto the stack the top element shifted
** left by N bits where N is the second element on the stack.
** If either operand is NULL, the result is NULL.
*/
/* Opcode: ShiftRight * * *
**
** Pop the top two elements from the stack.  Convert both elements
** to integers.  Push back onto the stack the top element shifted
** right by N bits where N is the second element on the stack.
** If either operand is NULL, the result is NULL.
*/
case OP_BitAnd:
case OP_BitOr:
case OP_ShiftLeft:
case OP_ShiftRight: {
  Mem *pNos = &pTos[-1];
  int a, b;

  assert( pNos>=p->aStack );
  if( (pTos->flags | pNos->flags) & MEM_Null ){
    popStack(&pTos, 2);
    pTos++;
    pTos->flags = MEM_Null;
    break;
  }
  Integerify(pTos, db->enc);
  Integerify(pNos, db->enc);
  a = pTos->i;
  b = pNos->i;
  switch( pOp->opcode ){
    case OP_BitAnd:      a &= b;     break;
    case OP_BitOr:       a |= b;     break;
    case OP_ShiftLeft:   a <<= b;    break;
    case OP_ShiftRight:  a >>= b;    break;
    default:   /* CANT HAPPEN */     break;
  }
  /* FIX ME: Because constant P3 values sometimes need to be translated,
  ** the following assert() can fail. When P3 is always in the native text
  ** encoding, this assert() will be valid again. Until then, the Release()
  ** is neeed instead.
  assert( (pTos->flags & MEM_Dyn)==0 );
  assert( (pNos->flags & MEM_Dyn)==0 );
  */
  Release(pTos);
  pTos--;
  Release(pTos);
  pTos->i = a;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: AddImm  P1 * *
** 
** Add the value P1 to whatever is on top of the stack.  The result
** is always an integer.
**
** To force the top of the stack to be an integer, just add 0.
*/
case OP_AddImm: {
  assert( pTos>=p->aStack );
  Integerify(pTos, db->enc);
  pTos->i += pOp->p1;
  break;
}

/* Opcode: ForceInt P1 P2 *
**
** Convert the top of the stack into an integer.  If the current top of
** the stack is not numeric (meaning that is is a NULL or a string that
** does not look like an integer or floating point number) then pop the
** stack and jump to P2.  If the top of the stack is numeric then
** convert it into the least integer that is greater than or equal to its
** current value if P1==0, or to the least integer that is strictly
** greater than its current value if P1==1.
*/
case OP_ForceInt: {
  int v;
  assert( pTos>=p->aStack );
  if( (pTos->flags & (MEM_Int|MEM_Real))==0 && ((pTos->flags & MEM_Str)==0 
      || sqlite3IsNumber(pTos->z, 0, db->enc)==0) ){
    Release(pTos);
    pTos--;
    pc = pOp->p2 - 1;
    break;
  }
  if( pTos->flags & MEM_Int ){
    v = pTos->i + (pOp->p1!=0);
  }else{
    Realify(pTos, db->enc);
    v = (int)pTos->r;
    if( pTos->r>(double)v ) v++;
    if( pOp->p1 && pTos->r==(double)v ) v++;
  }
  Release(pTos);
  pTos->i = v;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: MustBeInt P1 P2 *
** 
** Force the top of the stack to be an integer.  If the top of the
** stack is not an integer and cannot be converted into an integer
** with out data loss, then jump immediately to P2, or if P2==0
** raise an SQLITE_MISMATCH exception.
**
** If the top of the stack is not an integer and P2 is not zero and
** P1 is 1, then the stack is popped.  In all other cases, the depth
** of the stack is unchanged.
*/
case OP_MustBeInt: {
  assert( pTos>=p->aStack );
  if( pTos->flags & MEM_Int ){
    /* Do nothing */
  }else if( pTos->flags & MEM_Real ){
    int i = (int)pTos->r;
    double r = (double)i;
    if( r!=pTos->r ){
      goto mismatch;
    }
    pTos->i = i;
  }else if( pTos->flags & MEM_Str ){
    i64 v;
    if( !sqlite3atoi64(pTos->z, &v, db->enc) ){
      double r;
      if( !sqlite3IsNumber(pTos->z, 0, db->enc) ){
        goto mismatch;
      }
      Realify(pTos, db->enc);
      v = (int)pTos->r;
      r = (double)v;
      if( r!=pTos->r ){
        goto mismatch;
      }
    }
    pTos->i = v;
  }else{
    goto mismatch;
  }
  Release(pTos);
  pTos->flags = MEM_Int;
  break;

mismatch:
  if( pOp->p2==0 ){
    rc = SQLITE_MISMATCH;
    goto abort_due_to_error;
  }else{
    if( pOp->p1 ) popStack(&pTos, 1);
    pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: Eq P1 P2 P3
**
** Pop the top two elements from the stack.  If they are equal, then
** jump to instruction P2.  Otherwise, continue to the next instruction.
**
** The least significant byte of P1 may be either 0x00 or 0x01. If either
** operand is NULL (and thus if the result is unknown) then take the jump
** only if the least significant byte of P1 is 0x01.
**
** The second least significant byte of P1 must be an affinity character -
** 'n', 't', 'i' or 'o' - or 0x00. An attempt is made to coerce both values
** according to the affinity before the comparison is made. If the byte is
** 0x00, then numeric affinity is used.
**
** Once any conversions have taken place, and neither value is NULL, 
** the values are compared. If both values are blobs, or both are text,
** then memcmp() is used to determine the results of the comparison. If
** both values are numeric, then a numeric comparison is used. If the
** two values are of different types, then they are inequal.
**
** If P2 is zero, do not jump.  Instead, push an integer 1 onto the
** stack if the jump would have been taken, or a 0 if not.  Push a
** NULL if either operand was NULL.
**
** If P3 is not NULL it is a pointer to a collating sequence (a CollSeq
** structure) that defines how to compare text.
*/
/* Opcode: Ne P1 P2 P3
**
** This works just like the Eq opcode except that the jump is taken if
** the operands from the stack are not equal.  See the Eq opcode for
** additional information.
*/
/* Opcode: Lt P1 P2 P3
**
** This works just like the Eq opcode except that the jump is taken if
** the 2nd element down on the task is less than the top of the stack.
** See the Eq opcode for additional information.
*/
/* Opcode: Le P1 P2 P3
**
** This works just like the Eq opcode except that the jump is taken if
** the 2nd element down on the task is less than or equal to the
** top of the stack.  See the Eq opcode for additional information.
*/
/* Opcode: Gt P1 P2 P3
**
** This works just like the Eq opcode except that the jump is taken if
** the 2nd element down on the task is greater than the top of the stack.
** See the Eq opcode for additional information.
*/
/* Opcode: Ge P1 P2 P3
**
** This works just like the Eq opcode except that the jump is taken if
** the 2nd element down on the task is greater than or equal to the
** top of the stack.  See the Eq opcode for additional information.
*/
case OP_Eq:
case OP_Ne:
case OP_Lt:
case OP_Le:
case OP_Gt:
case OP_Ge: {
  Mem *pNos;
  int flags;
  int res;
  char affinity;

  pNos = &pTos[-1];
  flags = pTos->flags|pNos->flags;

  /* If either value is a NULL P2 is not zero, take the jump if the least
  ** significant byte of P1 is true. If P2 is zero, then push a NULL onto
  ** the stack.
  */
  if( flags&MEM_Null ){
    popStack(&pTos, 2);
    if( pOp->p2 ){
      if( (pOp->p1&0xFF) ) pc = pOp->p2-1;
    }else{
      pTos++;
      pTos->flags = MEM_Null;
    }
    break;
  }

  affinity = (pOp->p1>>8)&0xFF;
  if( affinity=='\0' ) affinity = 'n';
  applyAffinity(pNos, affinity, db->enc);
  applyAffinity(pTos, affinity, db->enc);

  assert( pOp->p3type==P3_COLLSEQ || pOp->p3==0 );
  res = sqlite3MemCompare(pNos, pTos, (CollSeq*)pOp->p3);
  switch( pOp->opcode ){
    case OP_Eq:    res = res==0;     break;
    case OP_Ne:    res = res!=0;     break;
    case OP_Lt:    res = res<0;      break;
    case OP_Le:    res = res<=0;     break;
    case OP_Gt:    res = res>0;      break;
    default:       res = res>=0;     break;
  }

  popStack(&pTos, 2);
  if( pOp->p2 ){
    if( res ){
      pc = pOp->p2-1;
    }
  }else{
    pTos++;
    pTos->flags = MEM_Int;
    pTos->i = res;
  }
  break;
}

/* Opcode: And * * *
**
** Pop two values off the stack.  Take the logical AND of the
** two values and push the resulting boolean value back onto the
** stack. 
*/
/* Opcode: Or * * *
**
** Pop two values off the stack.  Take the logical OR of the
** two values and push the resulting boolean value back onto the
** stack. 
*/
case OP_And:
case OP_Or: {
  Mem *pNos = &pTos[-1];
  int v1, v2;    /* 0==TRUE, 1==FALSE, 2==UNKNOWN or NULL */

  assert( pNos>=p->aStack );
  if( pTos->flags & MEM_Null ){
    v1 = 2;
  }else{
    Integerify(pTos, db->enc);
    v1 = pTos->i==0;
  }
  if( pNos->flags & MEM_Null ){
    v2 = 2;
  }else{
    Integerify(pNos, db->enc);
    v2 = pNos->i==0;
  }
  if( pOp->opcode==OP_And ){
    static const unsigned char and_logic[] = { 0, 1, 2, 1, 1, 1, 2, 1, 2 };
    v1 = and_logic[v1*3+v2];
  }else{
    static const unsigned char or_logic[] = { 0, 0, 0, 0, 1, 2, 0, 2, 2 };
    v1 = or_logic[v1*3+v2];
  }
  popStack(&pTos, 2);
  pTos++;
  if( v1==2 ){
    pTos->flags = MEM_Null;
  }else{
    pTos->i = v1==0;
    pTos->flags = MEM_Int;
  }
  break;
}

/* Opcode: Negative * * *
**
** Treat the top of the stack as a numeric quantity.  Replace it
** with its additive inverse.  If the top of the stack is NULL
** its value is unchanged.
*/
/* Opcode: AbsValue * * *
**
** Treat the top of the stack as a numeric quantity.  Replace it
** with its absolute value. If the top of the stack is NULL
** its value is unchanged.
*/
case OP_Negative:
case OP_AbsValue: {
  assert( pTos>=p->aStack );
  if( pTos->flags & MEM_Real ){
    Release(pTos);
    if( pOp->opcode==OP_Negative || pTos->r<0.0 ){
      pTos->r = -pTos->r;
    }
    pTos->flags = MEM_Real;
  }else if( pTos->flags & MEM_Int ){
    Release(pTos);
    if( pOp->opcode==OP_Negative || pTos->i<0 ){
      pTos->i = -pTos->i;
    }
    pTos->flags = MEM_Int;
  }else if( pTos->flags & MEM_Null ){
    /* Do nothing */
  }else{
    Realify(pTos, db->enc);
    Release(pTos);
    if( pOp->opcode==OP_Negative || pTos->r<0.0 ){
      pTos->r = -pTos->r;
    }
    pTos->flags = MEM_Real;
  }
  break;
}

/* Opcode: Not * * *
**
** Interpret the top of the stack as a boolean value.  Replace it
** with its complement.  If the top of the stack is NULL its value
** is unchanged.
*/
case OP_Not: {
  assert( pTos>=p->aStack );
  if( pTos->flags & MEM_Null ) break;  /* Do nothing to NULLs */
  Integerify(pTos, db->enc);
  Release(pTos);
  pTos->i = !pTos->i;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: BitNot * * *
**
** Interpret the top of the stack as an value.  Replace it
** with its ones-complement.  If the top of the stack is NULL its
** value is unchanged.
*/
case OP_BitNot: {
  assert( pTos>=p->aStack );
  if( pTos->flags & MEM_Null ) break;  /* Do nothing to NULLs */
  Integerify(pTos, db->enc);
  Release(pTos);
  pTos->i = ~pTos->i;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: Noop * * *
**
** Do nothing.  This instruction is often useful as a jump
** destination.
*/
case OP_Noop: {
  break;
}

/* Opcode: If P1 P2 *
**
** Pop a single boolean from the stack.  If the boolean popped is
** true, then jump to p2.  Otherwise continue to the next instruction.
** An integer is false if zero and true otherwise.  A string is
** false if it has zero length and true otherwise.
**
** If the value popped of the stack is NULL, then take the jump if P1
** is true and fall through if P1 is false.
*/
/* Opcode: IfNot P1 P2 *
**
** Pop a single boolean from the stack.  If the boolean popped is
** false, then jump to p2.  Otherwise continue to the next instruction.
** An integer is false if zero and true otherwise.  A string is
** false if it has zero length and true otherwise.
**
** If the value popped of the stack is NULL, then take the jump if P1
** is true and fall through if P1 is false.
*/
case OP_If:
case OP_IfNot: {
  int c;
  assert( pTos>=p->aStack );
  if( pTos->flags & MEM_Null ){
    c = pOp->p1;
  }else{
    Integerify(pTos, db->enc);
    c = pTos->i;
    if( pOp->opcode==OP_IfNot ) c = !c;
  }
  /* FIX ME: Because constant P3 values sometimes need to be translated,
  ** the following assert() can fail. When P3 is always in the native text
  ** encoding, this assert() will be valid again. Until then, the Release()
  ** is neeed instead.
  assert( (pTos->flags & MEM_Dyn)==0 ); 
  */
  Release(pTos);
  pTos--;
  if( c ) pc = pOp->p2-1;
  break;
}

/* Opcode: IsNull P1 P2 *
**
** If any of the top abs(P1) values on the stack are NULL, then jump
** to P2.  Pop the stack P1 times if P1>0.   If P1<0 leave the stack
** unchanged.
*/
case OP_IsNull: {
  int i, cnt;
  Mem *pTerm;
  cnt = pOp->p1;
  if( cnt<0 ) cnt = -cnt;
  pTerm = &pTos[1-cnt];
  assert( pTerm>=p->aStack );
  for(i=0; i<cnt; i++, pTerm++){
    if( pTerm->flags & MEM_Null ){
      pc = pOp->p2-1;
      break;
    }
  }
  if( pOp->p1>0 ) popStack(&pTos, cnt);
  break;
}

/* Opcode: NotNull P1 P2 *
**
** Jump to P2 if the top P1 values on the stack are all not NULL.  Pop the
** stack if P1 times if P1 is greater than zero.  If P1 is less than
** zero then leave the stack unchanged.
*/
case OP_NotNull: {
  int i, cnt;
  cnt = pOp->p1;
  if( cnt<0 ) cnt = -cnt;
  assert( &pTos[1-cnt] >= p->aStack );
  for(i=0; i<cnt && (pTos[1+i-cnt].flags & MEM_Null)==0; i++){}
  if( i>=cnt ) pc = pOp->p2-1;
  if( pOp->p1>0 ) popStack(&pTos, cnt);
  break;
}

/* Opcode: Class * * *
**
** Pop a single value from the top of the stack and push on one of the
** following strings, according to the storage class of the value just
** popped:
**
** "NULL", "INTEGER", "REAL", "TEXT", "BLOB"
**
** This opcode is probably temporary.
*/
case OP_Class: {
  int flags = pTos->flags;
  int i;

  struct {
    int mask;
    char * zClass;
    char * zClass16;
  } classes[] = {
    {MEM_Null, "NULL", "\0N\0U\0L\0L\0\0\0"},
    {MEM_Int, "INTEGER", "\0I\0N\0T\0E\0G\0E\0R\0\0\0"},
    {MEM_Real, "REAL", "\0R\0E\0A\0L\0\0\0"},
    {MEM_Str, "TEXT", "\0T\0E\0X\0T\0\0\0"},
    {MEM_Blob, "BLOB", "\0B\0L\0O\0B\0\0\0"}
  };

  Release(pTos);
  pTos->flags = MEM_Str|MEM_Static|MEM_Term;

  for(i=0; i<5; i++){
    if( classes[i].mask&flags ){
      switch( db->enc ){
        case TEXT_Utf8: 
          pTos->z = classes[i].zClass;
          break;
        case TEXT_Utf16be: 
          pTos->z = classes[i].zClass16;
          break;
        case TEXT_Utf16le: 
          pTos->z = &(classes[i].zClass16[1]);
          break;
        default:
          assert(0);
      }
      break;
    }
  }
  assert( i<5 );

  if( db->enc==TEXT_Utf8 ){
    pTos->n = strlen(pTos->z) + 1;
  }else{
    pTos->n = sqlite3utf16ByteLen(pTos->z, -1) + 2;
  }

  break;
}

/* Opcode: SetNumColumns P1 P2 *
**
** Before the OP_Column opcode can be executed on a cursor, this
** opcode must be called to set the number of fields in the table.
**
** This opcode sets the number of columns for cursor P1 to P2.
*/
case OP_SetNumColumns: {
  assert( (pOp->p1)<p->nCursor );
  p->apCsr[pOp->p1]->nField = pOp->p2;
  break;
}

/* Opcode: Column P1 P2 *
**
** Interpret the data that cursor P1 points to as a structure built using
** the MakeRecord instruction.  (See the MakeRecord opcode for additional
** information about the format of the data.) Push onto the stack the value
** of the P2-th column contained in the data.
**
** If the KeyAsData opcode has previously executed on this cursor, then the
** field might be extracted from the key rather than the data.
**
** If P1 is negative, then the record is stored on the stack rather than in
** a table.  For P1==-1, the top of the stack is used.  For P1==-2, the
** next on the stack is used.  And so forth.  The value pushed is always
** just a pointer into the record which is stored further down on the
** stack.  The column value is not copied. The number of columns in the
** record is stored on the stack just above the record itself.
*/
case OP_Column: {
  int payloadSize;   /* Number of bytes in the record */
  int i = pOp->p1;
  int p2 = pOp->p2;  /* column number to retrieve */
  Cursor *pC = 0;
  char *zRec;        /* Pointer to record-data from stack or pseudo-table. */
  BtCursor *pCrsr;

  u64 nField;        /* number of fields in the record */
  int len;           /* The length of the serialized data for the column */
  int offset = 0;
  int nn;

  char *zData;       
  Mem sMem;
  sMem.flags = 0;

  assert( i<p->nCursor );
  pTos++;

  /* If the record is coming from the stack, not from a cursor, then there
  ** is nowhere to cache the record header infomation. This simplifies
  ** things greatly, so deal with this case seperately.
  */
  if( i<0 ){
    char *zRec;     /* Pointer to record data from the stack. */
    int off = 0;    /* Offset in zRec to start of the columns data. */
    int off2 = 0;   /* Offset in zRec to the next serial type to read */
    u64 colType;    /* The serial type of the value being read. */

    assert( &pTos[i-1]>=p->aStack );

    /* FIX ME: I don't understand this either. How is it related to
    ** OP_SortNext? (I thought it would be the commented out assert())
    */
    /* assert( pTos[i].flags & MEM_Blob ); */
    assert( pTos[i].flags & (MEM_Blob|MEM_Str) );
    assert( pTos[i-1].flags & MEM_Int );

    if( pTos[i].n==0 ){
      pTos->flags = MEM_Null;
      break;
    }

    zRec = pTos[i].z;
    nField = pTos[i-1].i;
     
    for( nn=0; nn<nField; nn++ ){
      u64 v;
      off2 += sqlite3GetVarint(&zRec[off2], &v);
      if( nn==p2 ){
        colType = v;
      }else if( nn<p2 ){
        off += sqlite3VdbeSerialTypeLen(v);
      }
    }
    off += off2;
    
    sqlite3VdbeSerialGet(&zRec[off], colType, pTos, p->db->enc);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
    break;
  }


  /* This block sets the variable payloadSize, and if the data is coming
  ** from the stack or from a pseudo-table zRec. If the data is coming
  ** from a real cursor, then zRec is left as NULL.
  */
  if( (pC = p->apCsr[i])->pCursor!=0 ){
    sqlite3VdbeCursorMoveto(pC);
    zRec = 0;
    pCrsr = pC->pCursor;
    if( pC->nullRow ){
      payloadSize = 0;
    }else if( pC->cacheValid ){
      payloadSize = pC->payloadSize;
    }else if( pC->keyAsData ){
      i64 payloadSize64;
      sqlite3BtreeKeySize(pCrsr, &payloadSize64);
      payloadSize = payloadSize64;
    }else{
      sqlite3BtreeDataSize(pCrsr, &payloadSize);
    }
  }else if( pC->pseudoTable ){
    payloadSize = pC->nData;
    zRec = pC->pData;
    pC->cacheValid = 0;
    assert( payloadSize==0 || zRec!=0 );
  }else{
    payloadSize = 0;
  }

  /* If payloadSize is 0, then just push a NULL onto the stack. */
  if( payloadSize==0 ){
    pTos->flags = MEM_Null;
    break;
  }

  /* If the row data is coming from a cursor, then OP_SetNumColumns must of
  ** been executed on that cursor. Also, p2 (the column to read) must be
  ** less than nField.
  */
  assert( !pC || pC->nField>0 );
  assert( p2<pC->nField );
  nField = pC->nField;

  /* Read and parse the table header.  Store the results of the parse
  ** into the record header cache fields of the cursor.
  */
  if( !pC || !pC->cacheValid ){
    pC->payloadSize = payloadSize;
    if( !pC->aType ){
      pC->aType = sqliteMallocRaw( nField*sizeof(pC->aType[0]) );
      if( pC->aType==0 ){
        goto no_mem;
      }
    }

    if( zRec ){
      zData = zRec;
    }else{
      /* Estimate the maximum space required by the nField varints by
      ** assuming the maximum space for each is the length required to store:
      **
      **     (<record length> * 2) + 13
      **
      ** This is the serial-type for a text object as long as the record
      ** itself. In almost all cases the length required to store this is
      ** three bytes or less. 
      */
      int max_space = sqlite3VarintLen((((u64)payloadSize)<<1)+13)*nField;
      if( max_space>payloadSize ){
        max_space = payloadSize;
      }

      rc = getBtreeMem(pCrsr, 0, max_space, pC->keyAsData, &sMem);
      if( rc!=SQLITE_OK ){
        goto abort_due_to_error;
      }
      zData = sMem.z;
    }

    /* Read all the serial types for the record.  At the end of this block
    ** variable offset is set to the offset to the start of Data0 in the record.
    */
    for(nn=0; nn<nField; nn++){
      offset += sqlite3GetVarint(&zData[offset], &pC->aType[nn]);
    }
    pC->nHeader = offset;
    pC->cacheValid = 1;

    Release(&sMem);
    sMem.flags = 0;
  }

  /* Compute the offset from the beginning of the record to the beginning
  ** of the data.  And get the length of the data.
  */
  offset = pC->nHeader;
  for(nn=0; nn<p2; nn++){
    offset += sqlite3VdbeSerialTypeLen(pC->aType[nn]);
  }

  if( zRec ){
    zData = &zRec[offset];
  }else{
    len = sqlite3VdbeSerialTypeLen(pC->aType[p2]);
    getBtreeMem(pCrsr, offset, len, pC->keyAsData, &sMem);
    zData = sMem.z;
  }
  sqlite3VdbeSerialGet(zData, pC->aType[p2], pTos, p->db->enc);
  if( rc!=SQLITE_OK ){
    goto abort_due_to_error;
  }

  Release(&sMem);
  break;
}

/* Opcode MakeRecord P1 * P3
**
** This opcode (not yet in use) is a replacement for the current
** OP_MakeRecord that supports the SQLite3 manifest typing feature.
** It drops the (P2==1) option that was never use.
**
** Convert the top P1 entries of the stack into a single entry
** suitable for use as a data record in a database table.  The
** details of the format are irrelavant as long as the OP_Column
** opcode can decode the record later.  Refer to source code
** comments for the details of the record format.
**
** P3 may be a string that is P1 characters long.  The nth character of the
** string indicates the column affinity that should be used for the nth
** field of the index key (i.e. the first character of P3 corresponds to the
** lowest element on the stack).
**
**  Character      Column affinity
**  ------------------------------
**  'n'            NUMERIC
**  'i'            INTEGER
**  't'            TEXT
**  'o'            NONE
**
** If P3 is NULL then all index fields have the affinity NONE.
*/
case OP_MakeRecord: {
  /* Assuming the record contains N fields, the record format looks
  ** like this:
  **
  ** --------------------------------------------------------------------------
  ** | num-fields | type 0 | type 1 | ... | type N-1 | data0 | ... | data N-1 | 
  ** --------------------------------------------------------------------------
  **
  ** Data(0) is taken from the lowest element of the stack and data(N-1) is
  ** the top of the stack.
  **
  ** Each type field is a varint representing the serial type of the 
  ** corresponding data element (see sqlite3VdbeSerialType()). The
  ** num-fields field is also a varint storing N.
  ** 
  ** TODO: Even when the record is short enough for Mem::zShort, this opcode
  **   allocates it dynamically.
  */
  int nField = pOp->p1;
  unsigned char *zNewRecord;
  unsigned char *zCsr;
  char *zAffinity;
  Mem *pRec;
  int nBytes = 0;    /* Space required for this record */

  Mem *pData0 = &pTos[1-nField];
  assert( pData0>=p->aStack );
  zAffinity = pOp->p3;

  /* Loop through the elements that will make up the record to figure
  ** out how much space is required for the new record.
  */
  for(pRec=pData0; pRec<=pTos; pRec++){
    u64 serial_type;
    if( zAffinity ){
      applyAffinity(pRec, zAffinity[pRec-pData0], db->enc);
    }
    serial_type = sqlite3VdbeSerialType(pRec);
    nBytes += sqlite3VdbeSerialTypeLen(serial_type);
    nBytes += sqlite3VarintLen(serial_type);
  }

  if( nBytes>MAX_BYTES_PER_ROW ){
    rc = SQLITE_TOOBIG;
    goto abort_due_to_error;
  }

  /* Allocate space for the new record. */
  zNewRecord = sqliteMallocRaw(nBytes);
  if( !zNewRecord ){
    goto no_mem;
  }

  /* Write the record */
  zCsr = zNewRecord;
  for(pRec=pData0; pRec<=pTos; pRec++){
    u64 serial_type = sqlite3VdbeSerialType(pRec);
    zCsr += sqlite3PutVarint(zCsr, serial_type);      /* serial type */
  }
  for(pRec=pData0; pRec<=pTos; pRec++){
    zCsr += sqlite3VdbeSerialPut(zCsr, pRec);  /* serial data */
  }

  /* If zCsr has not been advanced exactly nBytes bytes, then one
  ** of the sqlite3PutVarint() or sqlite3VdbeSerialPut() calls above
  ** failed. This indicates a corrupted memory cell or code bug.
  */
  if( zCsr!=(zNewRecord+nBytes) ){
    rc = SQLITE_INTERNAL;
    goto abort_due_to_error;
  }

  /* Pop nField entries from the stack and push the new entry on */
  popStack(&pTos, nField);
  pTos++;
  pTos->n = nBytes;
  pTos->z = zNewRecord;
  pTos->flags = MEM_Blob | MEM_Dyn;

  break;
}

/* Opcode: MakeKey P1 P2 P3
**
** Convert the top P1 entries of the stack into a single entry suitable
** for use as the key in an index. If P2 is zero, then the original 
** entries are popped off the stack. If P2 is not zero, the original 
** entries remain on the stack.
**
** P3 is interpreted in the same way as for MakeIdxKey.
*/
/* Opcode: MakeIdxKey P1 P2 P3
**
** Convert the top P1 entries of the stack into a single entry suitable
** for use as the key in an index.  In addition, take one additional integer
** off of the stack, treat that integer as an eight-byte record number, and
** append the integer to the key as a varint.  Thus a total of P1+1 entries
** are popped from the stack for this instruction and a single entry is
** pushed back.  
**
** If P2 is not zero and one or more of the P1 entries that go into the
** generated key is NULL, then jump to P2 after the new key has been
** pushed on the stack.  In other words, jump to P2 if the key is
** guaranteed to be unique.  This jump can be used to skip a subsequent
** uniqueness test.
**
** P3 may be a string that is P1 characters long.  The nth character of the
** string indicates the column affinity that should be used for the nth
** field of the index key (i.e. the first character of P3 corresponds to the
** lowest element on the stack).
**
**  Character      Column affinity
**  ------------------------------
**  'n'            NUMERIC
**  'i'            INTEGER
**  't'            TEXT
**  'o'            NONE
**
** If P3 is NULL then datatype coercion occurs.
*/
case OP_MakeKey:
case OP_MakeIdxKey: {
  Mem *pRec;
  Mem *pData0;
  int nField;
  u64 rowid;
  int nByte = 0;
  int addRowid;
  int containsNull = 0;
  char *zKey;      /* The new key */
  int offset = 0;
  char *zAffinity = pOp->p3;
 
  nField = pOp->p1;
  assert( zAffinity==0 || strlen(zAffinity)>=nField );
  pData0 = &pTos[1-nField];
  assert( pData0>=p->aStack );

  addRowid = ((pOp->opcode==OP_MakeIdxKey)?1:0);

  /* Loop through the P1 elements that will make up the new index
  ** key. Call applyAffinity() to perform any conversion required
  ** the column affinity string P3 to modify stack elements in place.
  ** Set containsNull to 1 if a NULL value is encountered.
  **
  ** Once the value has been coerced, figure out how much space is required
  ** to store the coerced values serial-type and blob, and add this
  ** quantity to nByte.
  **
  ** TODO: Figure out if the in-place coercion causes a problem for
  ** OP_MakeKey when P2 is 0 (used by DISTINCT).
  */
  for(pRec=pData0; pRec<=pTos; pRec++){
    u64 serial_type;
    if( zAffinity ){
      applyAffinity(pRec, zAffinity[pRec-pData0], db->enc);
    }
    if( pRec->flags&MEM_Null ){
      containsNull = 1;
    }
    serial_type = sqlite3VdbeSerialType(pRec);
    nByte += sqlite3VarintLen(serial_type);
    nByte += sqlite3VdbeSerialTypeLen(serial_type);
  }

  /* If we have to append a varint rowid to this record, set 'rowid'
  ** to the value of the rowid and increase nByte by the amount of space
  ** required to store it and the 0x00 seperator byte.
  */
  if( addRowid ){
    pRec = &pTos[0-nField];
    assert( pRec>=p->aStack );
    Integerify(pRec, db->enc);
    rowid = pRec->i;
    nByte += sqlite3VarintLen(rowid);
    nByte++;
  }
  
  if( nByte>MAX_BYTES_PER_ROW ){
    rc = SQLITE_TOOBIG;
    goto abort_due_to_error;
  }

  /* Allocate space for the new key */
  zKey = (char *)sqliteMallocRaw(nByte);
  if( !zKey ){
    goto no_mem;
  }
  
  /* Build the key in the buffer pointed to by zKey. */
  for(pRec=pData0; pRec<=pTos; pRec++){
    u64 serial_type = sqlite3VdbeSerialType(pRec);
    offset += sqlite3PutVarint(&zKey[offset], serial_type);
    offset += sqlite3VdbeSerialPut(&zKey[offset], pRec);
  }
  if( addRowid ){
    zKey[offset++] = '\0';
    offset += sqlite3PutVarint(&zKey[offset], rowid);
  }
  assert( offset==nByte );

  /* Pop the consumed values off the stack and push on the new key. */
  if( addRowid||(pOp->p2==0) ){
    popStack(&pTos, nField+addRowid);
  }
  pTos++;
  pTos->flags = MEM_Blob|MEM_Dyn; /* TODO: should eventually be MEM_Blob */
  pTos->z = zKey;
  pTos->n = nByte;

  /* If P2 is non-zero, and if the key contains a NULL value, and if this
  ** was an OP_MakeIdxKey instruction, not OP_MakeKey, jump to P2.
  */
  if( pOp->p2 && containsNull && addRowid ){
    pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: Statement P1 * *
**
** Begin an individual statement transaction which is part of a larger
** BEGIN..COMMIT transaction.  This is needed so that the statement
** can be rolled back after an error without having to roll back the
** entire transaction.  The statement transaction will automatically
** commit when the VDBE halts.
**
** The statement is begun on the database file with index P1.  The main
** database file has an index of 0 and the file used for temporary tables
** has an index of 1.
*/
case OP_Statement: {
  int i = pOp->p1;
  if( i>=0 && i<db->nDb && db->aDb[i].pBt && db->aDb[i].inTrans==1 ){
    rc = sqlite3BtreeBeginStmt(db->aDb[i].pBt);
    if( rc==SQLITE_OK ) db->aDb[i].inTrans = 2;
  }
  break;
}

/* Opcode: Transaction P1 * *
**
** Begin a transaction.  The transaction ends when a Commit or Rollback
** opcode is encountered.  Depending on the ON CONFLICT setting, the
** transaction might also be rolled back if an error is encountered.
**
** P1 is the index of the database file on which the transaction is
** started.  Index 0 is the main database file and index 1 is the
** file used for temporary tables.
**
** A write lock is obtained on the database file when a transaction is
** started.  No other process can read or write the file while the
** transaction is underway.  Starting a transaction also creates a
** rollback journal.  A transaction must be started before any changes
** can be made to the database.
*/
case OP_Transaction: {
  int busy = 1;
  int i = pOp->p1;
  assert( i>=0 && i<db->nDb );
  if( db->aDb[i].inTrans ) break;
  while( db->aDb[i].pBt!=0 && busy ){
    rc = sqlite3BtreeBeginTrans(db->aDb[i].pBt);
    switch( rc ){
      case SQLITE_BUSY: {
        if( db->xBusyCallback==0 ){
          p->pc = pc;
          p->undoTransOnError = 1;
          p->rc = SQLITE_BUSY;
          p->pTos = pTos;
          return SQLITE_BUSY;
        }else if( (*db->xBusyCallback)(db->pBusyArg, "", busy++)==0 ){
          sqlite3SetString(&p->zErrMsg, sqlite3_error_string(rc), (char*)0);
          busy = 0;
        }
        break;
      }
      case SQLITE_READONLY: {
        rc = SQLITE_OK;
        /* Fall thru into the next case */
      }
      case SQLITE_OK: {
        p->inTempTrans = 0;
        busy = 0;
        break;
      }
      default: {
        goto abort_due_to_error;
      }
    }
  }
  db->aDb[i].inTrans = 1;
  p->undoTransOnError = 1;
  break;
}

/* Opcode: Commit * * *
**
** Cause all modifications to the database that have been made since the
** last Transaction to actually take effect.  No additional modifications
** are allowed until another transaction is started.  The Commit instruction
** deletes the journal file and releases the write lock on the database.
** A read lock continues to be held if there are still cursors open.
*/
case OP_Commit: {
  int i;
  if( db->xCommitCallback!=0 ){
    if( sqlite3SafetyOff(db) ) goto abort_due_to_misuse; 
    if( db->xCommitCallback(db->pCommitArg)!=0 ){
      rc = SQLITE_CONSTRAINT;
    }
    if( sqlite3SafetyOn(db) ) goto abort_due_to_misuse;
  }
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
    if( db->aDb[i].inTrans ){
      rc = sqlite3BtreeCommit(db->aDb[i].pBt);
      db->aDb[i].inTrans = 0;
    }
  }
  if( rc==SQLITE_OK ){
    sqlite3CommitInternalChanges(db);
  }else{
    sqlite3RollbackAll(db);
  }
  break;
}

/* Opcode: Rollback P1 * *
**
** Cause all modifications to the database that have been made since the
** last Transaction to be undone. The database is restored to its state
** before the Transaction opcode was executed.  No additional modifications
** are allowed until another transaction is started.
**
** P1 is the index of the database file that is committed.  An index of 0
** is used for the main database and an index of 1 is used for the file used
** to hold temporary tables.
**
** This instruction automatically closes all cursors and releases both
** the read and write locks on the indicated database.
*/
case OP_Rollback: {
  sqlite3RollbackAll(db);
  break;
}

/* Opcode: ReadCookie P1 P2 *
**
** Read cookie number P2 from database P1 and push it onto the stack.
** P2==0 is the schema version.  P2==1 is the database format.
** P2==2 is the recommended pager cache size, and so forth.  P1==0 is
** the main database file and P1==1 is the database file used to store
** temporary tables.
**
** There must be a read-lock on the database (either a transaction
** must be started or there must be an open cursor) before
** executing this instruction.
*/
case OP_ReadCookie: {
  int iMeta;
  assert( pOp->p2<SQLITE_N_BTREE_META );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( db->aDb[pOp->p1].pBt!=0 );
  /* The indexing of meta values at the schema layer is off by one from
  ** the indexing in the btree layer.  The btree considers meta[0] to
  ** be the number of free pages in the database (a read-only value)
  ** and meta[1] to be the schema cookie.  The schema layer considers
  ** meta[1] to be the schema cookie.  So we have to shift the index
  ** by one in the following statement.
  */
  rc = sqlite3BtreeGetMeta(db->aDb[pOp->p1].pBt, 1 + pOp->p2, &iMeta);
  pTos++;
  pTos->i = iMeta;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: SetCookie P1 P2 *
**
** Write the top of the stack into cookie number P2 of database P1.
** P2==0 is the schema version.  P2==1 is the database format.
** P2==2 is the recommended pager cache size, and so forth.  P1==0 is
** the main database file and P1==1 is the database file used to store
** temporary tables.
**
** A transaction must be started before executing this opcode.
*/
case OP_SetCookie: {
  assert( pOp->p2<SQLITE_N_BTREE_META );
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  assert( db->aDb[pOp->p1].pBt!=0 );
  assert( pTos>=p->aStack );
  Integerify(pTos, db->enc);
  /* See note about index shifting on OP_ReadCookie */
  rc = sqlite3BtreeUpdateMeta(db->aDb[pOp->p1].pBt, 1+pOp->p2, (int)pTos->i);
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: VerifyCookie P1 P2 *
**
** Check the value of global database parameter number 0 (the
** schema version) and make sure it is equal to P2.  
** P1 is the database number which is 0 for the main database file
** and 1 for the file holding temporary tables and some higher number
** for auxiliary databases.
**
** The cookie changes its value whenever the database schema changes.
** This operation is used to detect when that the cookie has changed
** and that the current process needs to reread the schema.
**
** Either a transaction needs to have been started or an OP_Open needs
** to be executed (to establish a read lock) before this opcode is
** invoked.
*/
case OP_VerifyCookie: {
  int iMeta;
  assert( pOp->p1>=0 && pOp->p1<db->nDb );
  rc = sqlite3BtreeGetMeta(db->aDb[pOp->p1].pBt, 1, &iMeta);
  if( rc==SQLITE_OK && iMeta!=pOp->p2 ){
    sqlite3SetString(&p->zErrMsg, "database schema has changed", (char*)0);
    rc = SQLITE_SCHEMA;
  }
  break;
}

/* Opcode: OpenRead P1 P2 P3
**
** Open a read-only cursor for the database table whose root page is
** P2 in a database file.  The database file is determined by an 
** integer from the top of the stack.  0 means the main database and
** 1 means the database used for temporary tables.  Give the new 
** cursor an identifier of P1.  The P1 values need not be contiguous
** but all P1 values should be small integers.  It is an error for
** P1 to be negative.
**
** If P2==0 then take the root page number from the next of the stack.
**
** There will be a read lock on the database whenever there is an
** open cursor.  If the database was unlocked prior to this instruction
** then a read lock is acquired as part of this instruction.  A read
** lock allows other processes to read the database but prohibits
** any other process from modifying the database.  The read lock is
** released when all cursors are closed.  If this instruction attempts
** to get a read lock but fails, the script terminates with an
** SQLITE_BUSY error code.
**
** The P3 value is a pointer to a KeyInfo structure that defines the
** content and collating sequence of indices.  P3 is NULL for cursors
** that are not pointing to indices.
**
** See also OpenWrite.
*/
/* Opcode: OpenWrite P1 P2 P3
**
** Open a read/write cursor named P1 on the table or index whose root
** page is P2.  If P2==0 then take the root page number from the stack.
**
** The P3 value is a pointer to a KeyInfo structure that defines the
** content and collating sequence of indices.  P3 is NULL for cursors
** that are not pointing to indices.
**
** This instruction works just like OpenRead except that it opens the cursor
** in read/write mode.  For a given table, there can be one or more read-only
** cursors or a single read/write cursor but not both.
**
** See also OpenRead.
*/
case OP_OpenRead:
case OP_OpenWrite: {
  int busy = 0;
  int i = pOp->p1;
  int p2 = pOp->p2;
  int wrFlag;
  Btree *pX;
  int iDb;
  Cursor *pCur;
  
  assert( pTos>=p->aStack );
  Integerify(pTos, db->enc);
  iDb = pTos->i;
  pTos--;
  assert( iDb>=0 && iDb<db->nDb );
  pX = db->aDb[iDb].pBt;
  assert( pX!=0 );
  wrFlag = pOp->opcode==OP_OpenWrite;
  if( p2<=0 ){
    assert( pTos>=p->aStack );
    Integerify(pTos, db->enc);
    p2 = pTos->i;
    pTos--;
    if( p2<2 ){
      sqlite3SetString(&p->zErrMsg, "root page number less than 2", (char*)0);
      rc = SQLITE_INTERNAL;
      break;
    }
  }
  assert( i>=0 );
  if( expandCursorArraySize(p, i) ) goto no_mem;
  pCur = p->apCsr[i];
  sqlite3VdbeCleanupCursor(pCur);
  pCur->nullRow = 1;
  if( pX==0 ) break;
  do{
    /* When opening cursors, always supply the comparison function
    ** sqlite3VdbeKeyCompare(). If the table being opened is of type
    ** INTKEY, the btree layer won't call the comparison function anyway.
    */
    rc = sqlite3BtreeCursor(pX, p2, wrFlag,
             sqlite3VdbeKeyCompare, pOp->p3,
             &pCur->pCursor);
    pCur->pKeyInfo = (KeyInfo*)pOp->p3;
    if( pCur->pKeyInfo ){
      pCur->pIncrKey = &pCur->pKeyInfo->incrKey;
      pCur->pKeyInfo->enc = p->db->enc;
    }else{
      pCur->pIncrKey = &pCur->bogusIncrKey;
    }
    switch( rc ){
      case SQLITE_BUSY: {
        if( db->xBusyCallback==0 ){
          p->pc = pc;
          p->rc = SQLITE_BUSY;
          p->pTos = &pTos[1 + (pOp->p2<=0)]; /* Operands must remain on stack */
          return SQLITE_BUSY;
        }else if( (*db->xBusyCallback)(db->pBusyArg, pOp->p3, ++busy)==0 ){
          sqlite3SetString(&p->zErrMsg, sqlite3_error_string(rc), (char*)0);
          busy = 0;
        }
        break;
      }
      case SQLITE_OK: {
        int flags = sqlite3BtreeFlags(pCur->pCursor);
        pCur->intKey = (flags & BTREE_INTKEY)!=0;
        pCur->zeroData = (flags & BTREE_ZERODATA)!=0;
        busy = 0;
        break;
      }
      case SQLITE_EMPTY: {
        rc = SQLITE_OK;
        busy = 0;
        break;
      }
      default: {
        goto abort_due_to_error;
      }
    }
  }while( busy );
  break;
}

/* Opcode: OpenTemp P1 * P3
**
** Open a new cursor to a transient table.
** The transient cursor is always opened read/write even if 
** the main database is read-only.  The transient table is deleted
** automatically when the cursor is closed.
**
** The cursor points to a BTree table if P3==0 and to a BTree index
** if P3 is not 0.  If P3 is not NULL, it points to a KeyInfo structure
** that defines the format of keys in the index.
**
** This opcode is used for tables that exist for the duration of a single
** SQL statement only.  Tables created using CREATE TEMPORARY TABLE
** are opened using OP_OpenRead or OP_OpenWrite.  "Temporary" in the
** context of this opcode means for the duration of a single SQL statement
** whereas "Temporary" in the context of CREATE TABLE means for the duration
** of the connection to the database.  Same word; different meanings.
*/
case OP_OpenTemp: {
  int i = pOp->p1;
  Cursor *pCx;
  assert( i>=0 );
  if( expandCursorArraySize(p, i) ) goto no_mem;
  pCx = p->apCsr[i];
  sqlite3VdbeCleanupCursor(pCx);
  memset(pCx, 0, sizeof(*pCx));
  pCx->nullRow = 1;
  rc = sqlite3BtreeFactory(db, 0, 1, TEMP_PAGES, &pCx->pBt);

  if( rc==SQLITE_OK ){
    rc = sqlite3BtreeBeginTrans(pCx->pBt);
  }
  if( rc==SQLITE_OK ){
    /* If a transient index is required, create it by calling
    ** sqlite3BtreeCreateTable() with the BTREE_ZERODATA flag before
    ** opening it. If a transient table is required, just use the
    ** automatically created table with root-page 1 (an INTKEY table).
    */
    if( pOp->p3 ){
      int pgno;
      assert( pOp->p3type==P3_KEYINFO );
      rc = sqlite3BtreeCreateTable(pCx->pBt, &pgno, BTREE_ZERODATA); 
      if( rc==SQLITE_OK ){
        assert( pgno==MASTER_ROOT+1 );
        rc = sqlite3BtreeCursor(pCx->pBt, pgno, 1, sqlite3VdbeKeyCompare,
            pOp->p3, &pCx->pCursor);
        pCx->pKeyInfo = (KeyInfo*)pOp->p3;
        pCx->pKeyInfo->enc = p->db->enc;
        pCx->pIncrKey = &pCx->pKeyInfo->incrKey;
      }
    }else{
      rc = sqlite3BtreeCursor(pCx->pBt, MASTER_ROOT, 1, 0, 0, &pCx->pCursor);
      pCx->intKey = 1;
      pCx->pIncrKey = &pCx->bogusIncrKey;
    }
  }
  break;
}

/* Opcode: OpenPseudo P1 * *
**
** Open a new cursor that points to a fake table that contains a single
** row of data.  Any attempt to write a second row of data causes the
** first row to be deleted.  All data is deleted when the cursor is
** closed.
**
** A pseudo-table created by this opcode is useful for holding the
** NEW or OLD tables in a trigger.
*/
case OP_OpenPseudo: {
  int i = pOp->p1;
  Cursor *pCx;
  assert( i>=0 );
  if( expandCursorArraySize(p, i) ) goto no_mem;
  pCx = p->apCsr[i];
  sqlite3VdbeCleanupCursor(pCx);
  memset(pCx, 0, sizeof(*pCx));
  pCx->nullRow = 1;
  pCx->pseudoTable = 1;
  pCx->pIncrKey = &pCx->bogusIncrKey;
  break;
}

/* Opcode: Close P1 * *
**
** Close a cursor previously opened as P1.  If P1 is not
** currently open, this instruction is a no-op.
*/
case OP_Close: {
  int i = pOp->p1;
  if( i>=0 && i<p->nCursor ){
    sqlite3VdbeCleanupCursor(p->apCsr[i]);
  }
  break;
}

/* Opcode: MoveGe P1 P2 *
**
** Pop the top of the stack and use its value as a key.  Reposition
** cursor P1 so that it points to the smallest entry that is greater
** than or equal to the key that was popped ffrom the stack.
** If there are no records greater than or equal to the key and P2 
** is not zero, then jump to P2.
**
** See also: Found, NotFound, Distinct, MoveLt, MoveGt, MoveLe
*/
/* Opcode: MoveGt P1 P2 *
**
** Pop the top of the stack and use its value as a key.  Reposition
** cursor P1 so that it points to the smallest entry that is greater
** than the key from the stack.
** If there are no records greater than the key and P2 is not zero,
** then jump to P2.
**
** See also: Found, NotFound, Distinct, MoveLt, MoveGe, MoveLe
*/
/* Opcode: MoveLt P1 P2 *
**
** Pop the top of the stack and use its value as a key.  Reposition
** cursor P1 so that it points to the largest entry that is less
** than the key from the stack.
** If there are no records less than the key and P2 is not zero,
** then jump to P2.
**
** See also: Found, NotFound, Distinct, MoveGt, MoveGe, MoveLe
*/
/* Opcode: MoveLe P1 P2 *
**
** Pop the top of the stack and use its value as a key.  Reposition
** cursor P1 so that it points to the largest entry that is less than
** or equal to the key that was popped from the stack.
** If there are no records less than or eqal to the key and P2 is not zero,
** then jump to P2.
**
** See also: Found, NotFound, Distinct, MoveGt, MoveGe, MoveLt
*/
case OP_MoveLt:
case OP_MoveLe:
case OP_MoveGe:
case OP_MoveGt: {
  int i = pOp->p1;
  Cursor *pC;

  assert( pTos>=p->aStack );
  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  if( pC->pCursor!=0 ){
    int res, oc;
    oc = pOp->opcode;
    pC->nullRow = 0;
    *pC->pIncrKey = oc==OP_MoveGt || oc==OP_MoveLe;
    if( pC->intKey ){
      i64 iKey;
      assert( !pOp->p3 );
      Integerify(pTos, db->enc);
      iKey = intToKey(pTos->i);
      if( pOp->p2==0 && pOp->opcode==OP_MoveGe ){
        pC->movetoTarget = iKey;
        pC->deferredMoveto = 1;
        Release(pTos);
        pTos--;
        break;
      }
      sqlite3BtreeMoveto(pC->pCursor, 0, (u64)iKey, &res);
      pC->lastRecno = pTos->i;
      pC->recnoIsValid = res==0;
    }else{
      Stringify(pTos, db->enc);
      sqlite3BtreeMoveto(pC->pCursor, pTos->z, pTos->n, &res);
      pC->recnoIsValid = 0;
    }
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
    *pC->pIncrKey = 0;
    sqlite3_search_count++;
    if( oc==OP_MoveGe || oc==OP_MoveGt ){
      if( res<0 ){
        sqlite3BtreeNext(pC->pCursor, &res);
        pC->recnoIsValid = 0;
        if( res && pOp->p2>0 ){
          pc = pOp->p2 - 1;
        }
      }
    }else{
      assert( oc==OP_MoveLt || oc==OP_MoveLe );
      if( res>=0 ){
        sqlite3BtreePrevious(pC->pCursor, &res);
        pC->recnoIsValid = 0;
      }else{
        /* res might be negative because the table is empty.  Check to
        ** see if this is the case.
        */
        res = sqlite3BtreeEof(pC->pCursor);
      }
      if( res && pOp->p2>0 ){
        pc = pOp->p2 - 1;
      }
    }
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: Distinct P1 P2 *
**
** Use the top of the stack as a string key.  If a record with that key does
** not exist in the table of cursor P1, then jump to P2.  If the record
** does already exist, then fall thru.  The cursor is left pointing
** at the record if it exists. The key is not popped from the stack.
**
** This operation is similar to NotFound except that this operation
** does not pop the key from the stack.
**
** See also: Found, NotFound, MoveTo, IsUnique, NotExists
*/
/* Opcode: Found P1 P2 *
**
** Use the top of the stack as a string key.  If a record with that key
** does exist in table of P1, then jump to P2.  If the record
** does not exist, then fall thru.  The cursor is left pointing
** to the record if it exists.  The key is popped from the stack.
**
** See also: Distinct, NotFound, MoveTo, IsUnique, NotExists
*/
/* Opcode: NotFound P1 P2 *
**
** Use the top of the stack as a string key.  If a record with that key
** does not exist in table of P1, then jump to P2.  If the record
** does exist, then fall thru.  The cursor is left pointing to the
** record if it exists.  The key is popped from the stack.
**
** The difference between this operation and Distinct is that
** Distinct does not pop the key from the stack.
**
** See also: Distinct, Found, MoveTo, NotExists, IsUnique
*/
case OP_Distinct:
case OP_NotFound:
case OP_Found: {
  int i = pOp->p1;
  int alreadyExists = 0;
  Cursor *pC;
  assert( pTos>=p->aStack );
  assert( i>=0 && i<p->nCursor );
  if( (pC = p->apCsr[i])->pCursor!=0 ){
    int res, rx;
    assert( pC->intKey==0 );
    Stringify(pTos, db->enc);
    rx = sqlite3BtreeMoveto(pC->pCursor, pTos->z, pTos->n, &res);
    alreadyExists = rx==SQLITE_OK && res==0;
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
  }
  if( pOp->opcode==OP_Found ){
    if( alreadyExists ) pc = pOp->p2 - 1;
  }else{
    if( !alreadyExists ) pc = pOp->p2 - 1;
  }
  if( pOp->opcode!=OP_Distinct ){
    Release(pTos);
    pTos--;
  }
  break;
}

/* Opcode: IsUnique P1 P2 *
**
** The top of the stack is an integer record number.  Call this
** record number R.  The next on the stack is an index key created
** using MakeIdxKey.  Call it K.  This instruction pops R from the
** stack but it leaves K unchanged.
**
** P1 is an index.  So it has no data and its key consists of a
** record generated by OP_MakeIdxKey.  This key contains one or more
** fields followed by a varint ROWID.
**
** This instruction asks if there is an entry in P1 where the
** fields matches K but the rowid is different from R.
** If there is no such entry, then there is an immediate
** jump to P2.  If any entry does exist where the index string
** matches K but the record number is not R, then the record
** number for that entry is pushed onto the stack and control
** falls through to the next instruction.
**
** See also: Distinct, NotFound, NotExists, Found
*/
case OP_IsUnique: {
  int i = pOp->p1;
  Mem *pNos = &pTos[-1];
  Cursor *pCx;
  BtCursor *pCrsr;
  i64 R;

  /* Pop the value R off the top of the stack
  */
  assert( pNos>=p->aStack );
  Integerify(pTos, db->enc);
  R = pTos->i;
  pTos--;
  assert( i>=0 && i<=p->nCursor );
  pCx = p->apCsr[i];
  pCrsr = pCx->pCursor;
  if( pCrsr!=0 ){
    int res, rc;
    i64 v;         /* The record number on the P1 entry that matches K */
    char *zKey;    /* The value of K */
    int nKey;      /* Number of bytes in K */
    int len;       /* Number of bytes in K without the rowid at the end */

    /* Make sure K is a string and make zKey point to K
    */
    Stringify(pNos, db->enc);
    zKey = pNos->z;
    nKey = pNos->n;

    assert( nKey >= 2 );
    len = nKey-2;
    while( zKey[len] && --len );

    /* Search for an entry in P1 where all but the last four bytes match K.
    ** If there is no such entry, jump immediately to P2.
    */
    assert( pCx->deferredMoveto==0 );
    pCx->cacheValid = 0;
    rc = sqlite3BtreeMoveto(pCrsr, zKey, len, &res);
    if( rc!=SQLITE_OK ) goto abort_due_to_error;
    if( res<0 ){
      rc = sqlite3BtreeNext(pCrsr, &res);
      if( res ){
        pc = pOp->p2 - 1;
        break;
      }
    }
    rc = sqlite3VdbeIdxKeyCompare(pCx, len, zKey, &res); 
    if( rc!=SQLITE_OK ) goto abort_due_to_error;
    if( res>0 ){
      pc = pOp->p2 - 1;
      break;
    }

    /* At this point, pCrsr is pointing to an entry in P1 where all but
    ** the final varint (the rowid) matches K.  Check to see if the
    ** final varint is different from R.  If it equals R then jump
    ** immediately to P2.
    */
    rc = sqlite3VdbeIdxRowid(pCrsr, &v);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
    if( v==R ){
      pc = pOp->p2 - 1;
      break;
    }

    /* The final varint of the key is different from R.  Push it onto
    ** the stack.  (The record number of an entry that violates a UNIQUE
    ** constraint.)
    */
    pTos++;
    pTos->i = v;
    pTos->flags = MEM_Int;
  }
  break;
}

/* Opcode: NotExists P1 P2 *
**
** Use the top of the stack as a integer key.  If a record with that key
** does not exist in table of P1, then jump to P2.  If the record
** does exist, then fall thru.  The cursor is left pointing to the
** record if it exists.  The integer key is popped from the stack.
**
** The difference between this operation and NotFound is that this
** operation assumes the key is an integer and NotFound assumes it
** is a string.
**
** See also: Distinct, Found, MoveTo, NotFound, IsUnique
*/
case OP_NotExists: {
  int i = pOp->p1;
  Cursor *pC;
  BtCursor *pCrsr;
  assert( pTos>=p->aStack );
  assert( i>=0 && i<p->nCursor );
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    int res, rx;
    u64 iKey;
    assert( pTos->flags & MEM_Int );
    assert( p->apCsr[i]->intKey );
    iKey = intToKey(pTos->i);
    rx = sqlite3BtreeMoveto(pCrsr, 0, iKey, &res);
    pC->lastRecno = pTos->i;
    pC->recnoIsValid = res==0;
    pC->nullRow = 0;
    pC->cacheValid = 0;
    if( rx!=SQLITE_OK || res!=0 ){
      pc = pOp->p2 - 1;
      pC->recnoIsValid = 0;
    }
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: NewRecno P1 * *
**
** Get a new integer record number used as the key to a table.
** The record number is not previously used as a key in the database
** table that cursor P1 points to.  The new record number is pushed 
** onto the stack.
*/
case OP_NewRecno: {
  int i = pOp->p1;
  i64 v = 0;
  Cursor *pC;
  assert( i>=0 && i<p->nCursor );
  if( (pC = p->apCsr[i])->pCursor==0 ){
    /* The zero initialization above is all that is needed */
  }else{
    /* The next rowid or record number (different terms for the same
    ** thing) is obtained in a two-step algorithm.
    **
    ** First we attempt to find the largest existing rowid and add one
    ** to that.  But if the largest existing rowid is already the maximum
    ** positive integer, we have to fall through to the second
    ** probabilistic algorithm
    **
    ** The second algorithm is to select a rowid at random and see if
    ** it already exists in the table.  If it does not exist, we have
    ** succeeded.  If the random rowid does exist, we select a new one
    ** and try again, up to 1000 times.
    **
    ** For a table with less than 2 billion entries, the probability
    ** of not finding a unused rowid is about 1.0e-300.  This is a 
    ** non-zero probability, but it is still vanishingly small and should
    ** never cause a problem.  You are much, much more likely to have a
    ** hardware failure than for this algorithm to fail.
    **
    ** The analysis in the previous paragraph assumes that you have a good
    ** source of random numbers.  Is a library function like lrand48()
    ** good enough?  Maybe. Maybe not. It's hard to know whether there
    ** might be subtle bugs is some implementations of lrand48() that
    ** could cause problems. To avoid uncertainty, SQLite uses its own 
    ** random number generator based on the RC4 algorithm.
    **
    ** To promote locality of reference for repetitive inserts, the
    ** first few attempts at chosing a random rowid pick values just a little
    ** larger than the previous rowid.  This has been shown experimentally
    ** to double the speed of the COPY operation.
    */
    int res, rx, cnt;
    i64 x;
    cnt = 0;
    assert( (sqlite3BtreeFlags(pC->pCursor) & BTREE_INTKEY)!=0 );
    assert( (sqlite3BtreeFlags(pC->pCursor) & BTREE_ZERODATA)==0 );
    if( !pC->useRandomRowid ){
      if( pC->nextRowidValid ){
        v = pC->nextRowid;
      }else{
        rx = sqlite3BtreeLast(pC->pCursor, &res);
        if( res ){
          v = 1;
        }else{
          sqlite3BtreeKeySize(pC->pCursor, (u64*)&v);
          v = keyToInt(v);
          if( v==0x7fffffffffffffff ){
            pC->useRandomRowid = 1;
          }else{
            v++;
          }
        }
      }
      if( v<0x7fffffffffffffff ){
        pC->nextRowidValid = 1;
        pC->nextRowid = v+1;
      }else{
        pC->nextRowidValid = 0;
      }
    }
    if( pC->useRandomRowid ){
      v = db->priorNewRowid;
      cnt = 0;
      do{
        if( v==0 || cnt>2 ){
          sqlite3Randomness(sizeof(v), &v);
          if( cnt<5 ) v &= 0xffffff;
        }else{
          unsigned char r;
          sqlite3Randomness(1, &r);
          v += r + 1;
        }
        if( v==0 ) continue;
        x = intToKey(v);
        rx = sqlite3BtreeMoveto(pC->pCursor, 0, (u64)x, &res);
        cnt++;
      }while( cnt<1000 && rx==SQLITE_OK && res==0 );
      db->priorNewRowid = v;
      if( rx==SQLITE_OK && res==0 ){
        rc = SQLITE_FULL;
        goto abort_due_to_error;
      }
    }
    pC->recnoIsValid = 0;
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
  }
  pTos++;
  pTos->i = v;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: PutIntKey P1 P2 *
**
** Write an entry into the table of cursor P1.  A new entry is
** created if it doesn't already exist or the data for an existing
** entry is overwritten.  The data is the value on the top of the
** stack.  The key is the next value down on the stack.  The key must
** be an integer.  The stack is popped twice by this instruction.
**
** If the OPFLAG_NCHANGE flag of P2 is set, then the row change count is
** incremented (otherwise not).  If the OPFLAG_CSCHANGE flag is set,
** then the current statement change count is incremented (otherwise not).
** If the OPFLAG_LASTROWID flag of P2 is set, then rowid is
** stored for subsequent return by the sqlite3_last_insert_rowid() function
** (otherwise it's unmodified).
*/
/* Opcode: PutStrKey P1 * *
**
** Write an entry into the table of cursor P1.  A new entry is
** created if it doesn't already exist or the data for an existing
** entry is overwritten.  The data is the value on the top of the
** stack.  The key is the next value down on the stack.  The key must
** be a string.  The stack is popped twice by this instruction.
**
** P1 may not be a pseudo-table opened using the OpenPseudo opcode.
*/
case OP_PutIntKey:
case OP_PutStrKey: {
  Mem *pNos = &pTos[-1];
  int i = pOp->p1;
  Cursor *pC;
  assert( pNos>=p->aStack );
  assert( i>=0 && i<p->nCursor );
  if( ((pC = p->apCsr[i])->pCursor!=0 || pC->pseudoTable) ){
    char *zKey;
    i64 nKey; 
    i64 iKey;
    if( pOp->opcode==OP_PutStrKey ){
      Stringify(pNos, db->enc);
      nKey = pNos->n;
      zKey = pNos->z;
    }else{
      assert( pNos->flags & MEM_Int );

      /* If the table is an INTKEY table, set nKey to the value of
      ** the integer key, and zKey to NULL. Otherwise, set nKey to
      ** sizeof(i64) and point zKey at iKey. iKey contains the integer
      ** key in the on-disk byte order.
      */
      iKey = intToKey(pNos->i);
      if( pC->intKey ){
        nKey = intToKey(pNos->i);
        zKey = 0;
      }else{
        nKey = sizeof(i64);
        zKey = (char*)&iKey;
      }

      if( pOp->p2 & OPFLAG_NCHANGE ) db->nChange++;
      if( pOp->p2 & OPFLAG_LASTROWID ) db->lastRowid = pNos->i;
      if( pOp->p2 & OPFLAG_CSCHANGE ) db->csChange++;
      if( pC->nextRowidValid && pTos->i>=pC->nextRowid ){
        pC->nextRowidValid = 0;
      }
    }
    if( pTos->flags & MEM_Null ){
      pTos->z = 0;
      pTos->n = 0;
    }else{
      assert( pTos->flags & (MEM_Blob|MEM_Str) );
    }
    if( pC->pseudoTable ){
      /* PutStrKey does not work for pseudo-tables.
      ** The following assert makes sure we are not trying to use
      ** PutStrKey on a pseudo-table
      */
      assert( pOp->opcode==OP_PutIntKey );
      sqliteFree(pC->pData);
      pC->iKey = iKey;
      pC->nData = pTos->n;
      if( pTos->flags & MEM_Dyn ){
        pC->pData = pTos->z;
        pTos->flags = MEM_Null;
      }else{
        pC->pData = sqliteMallocRaw( pC->nData );
        if( pC->pData ){
          memcpy(pC->pData, pTos->z, pC->nData);
        }
      }
      pC->nullRow = 0;
    }else{
      rc = sqlite3BtreeInsert(pC->pCursor, zKey, nKey, pTos->z, pTos->n);
    }
    pC->recnoIsValid = 0;
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
  }
  popStack(&pTos, 2);
  break;
}

/* Opcode: Delete P1 P2 *
**
** Delete the record at which the P1 cursor is currently pointing.
**
** The cursor will be left pointing at either the next or the previous
** record in the table. If it is left pointing at the next record, then
** the next Next instruction will be a no-op.  Hence it is OK to delete
** a record from within an Next loop.
**
** If the OPFLAG_NCHANGE flag of P2 is set, then the row change count is
** incremented (otherwise not).  If OPFLAG_CSCHANGE flag is set,
** then the current statement change count is incremented (otherwise not).
**
** If P1 is a pseudo-table, then this instruction is a no-op.
*/
case OP_Delete: {
  int i = pOp->p1;
  Cursor *pC;
  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  if( pC->pCursor!=0 ){
    sqlite3VdbeCursorMoveto(pC);
    rc = sqlite3BtreeDelete(pC->pCursor);
    pC->nextRowidValid = 0;
    pC->cacheValid = 0;
  }
  if( pOp->p2 & OPFLAG_NCHANGE ) db->nChange++;
  if( pOp->p2 & OPFLAG_CSCHANGE ) db->csChange++;
  break;
}

/* Opcode: SetCounts * * *
**
** Called at end of statement.  Updates lsChange (last statement change count)
** and resets csChange (current statement change count) to 0.
*/
case OP_SetCounts: {
  db->lsChange=db->csChange;
  db->csChange=0;
  break;
}

/* Opcode: KeyAsData P1 P2 *
**
** Turn the key-as-data mode for cursor P1 either on (if P2==1) or
** off (if P2==0).  In key-as-data mode, the OP_Column opcode pulls
** data off of the key rather than the data.  This is used for
** processing compound selects.
**
** This opcode also instructs the cursor that the keys used will be
** serialized in the record format usually used for table data, not
** the usual index key format.
*/
case OP_KeyAsData: {
  int i = pOp->p1;
  Cursor *pC;
  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  pC->keyAsData = pOp->p2;
  sqlite3BtreeSetCompare(pC->pCursor, sqlite3VdbeRowCompare, pC->pKeyInfo);
  break;
}

/* Opcode: RowData P1 * *
**
** Push onto the stack the complete row data for cursor P1.
** There is no interpretation of the data.  It is just copied
** onto the stack exactly as it is found in the database file.
**
** If the cursor is not pointing to a valid row, a NULL is pushed
** onto the stack.
*/
/* Opcode: RowKey P1 * *
**
** Push onto the stack the complete row key for cursor P1.
** There is no interpretation of the key.  It is just copied
** onto the stack exactly as it is found in the database file.
**
** If the cursor is not pointing to a valid row, a NULL is pushed
** onto the stack.
*/
case OP_RowKey:
case OP_RowData: {
  int i = pOp->p1;
  Cursor *pC;
  int n;

  pTos++;
  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  if( pC->nullRow ){
    pTos->flags = MEM_Null;
  }else if( pC->pCursor!=0 ){
    BtCursor *pCrsr = pC->pCursor;
    sqlite3VdbeCursorMoveto(pC);
    if( pC->nullRow ){
      pTos->flags = MEM_Null;
      break;
    }else if( pC->keyAsData || pOp->opcode==OP_RowKey ){
      i64 n64;
      assert( !pC->intKey );
      sqlite3BtreeKeySize(pCrsr, &n64);
      n = n64;
    }else{
      sqlite3BtreeDataSize(pCrsr, &n);
    }
    pTos->n = n;
    if( n<=NBFS ){
      pTos->flags = MEM_Blob | MEM_Short;
      pTos->z = pTos->zShort;
    }else{
      char *z = sqliteMallocRaw( n );
      if( z==0 ) goto no_mem;
      pTos->flags = MEM_Blob | MEM_Dyn;
      pTos->z = z;
    }
    if( pC->keyAsData || pOp->opcode==OP_RowKey ){
      sqlite3BtreeKey(pCrsr, 0, n, pTos->z);
    }else{
      sqlite3BtreeData(pCrsr, 0, n, pTos->z);
    }
  }else if( pC->pseudoTable ){
    pTos->n = pC->nData;
    pTos->z = pC->pData;
    pTos->flags = MEM_Blob|MEM_Ephem;
  }else{
    pTos->flags = MEM_Null;
  }
  break;
}

/* Opcode: Recno P1 * *
**
** Push onto the stack an integer which is the first 4 bytes of the
** the key to the current entry in a sequential scan of the database
** file P1.  The sequential scan should have been started using the 
** Next opcode.
*/
case OP_Recno: {
  int i = pOp->p1;
  Cursor *pC;
  i64 v;

  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  sqlite3VdbeCursorMoveto(pC);
  pTos++;
  if( pC->recnoIsValid ){
    v = pC->lastRecno;
  }else if( pC->pseudoTable ){
    v = keyToInt(pC->iKey);
  }else if( pC->nullRow || pC->pCursor==0 ){
    pTos->flags = MEM_Null;
    break;
  }else{
    assert( pC->pCursor!=0 );
    sqlite3BtreeKeySize(pC->pCursor, (u64*)&v);
    v = keyToInt(v);
  }
  pTos->i = v;
  pTos->flags = MEM_Int;
  break;
}

/* Opcode: IdxColumn P1 * *
**
** P1 is a cursor opened on an index. Push the first field from the
** current index key onto the stack.
*/
case OP_IdxColumn: {
  char *zData;
  i64 n;
  u64 serial_type;
  int len;
  int freeZData = 0;
  BtCursor *pCsr;

  assert( 0==p->apCsr[pOp->p1]->intKey );
  pCsr = p->apCsr[pOp->p1]->pCursor;
  rc = sqlite3BtreeKeySize(pCsr, &n);
  if( rc!=SQLITE_OK ){
    goto abort_due_to_error;
  }
  if( n>10 ) n = 10;

  zData = (char *)sqlite3BtreeKeyFetch(pCsr, n);
  assert( zData );

  len = sqlite3GetVarint(zData, &serial_type);
  n = sqlite3VdbeSerialTypeLen(serial_type);

  zData = (char *)sqlite3BtreeKeyFetch(pCsr, len+n);
  if( !zData ){
    zData = (char *)sqliteMalloc(n);
    if( !zData ){
      goto no_mem;
    }
    rc = sqlite3BtreeKey(pCsr, len, n, zData);
    if( rc!=SQLITE_OK ){
      sqliteFree(zData);
      goto abort_due_to_error;
    }
    freeZData = 1;
    len = 0;
  }

  pTos++;
  sqlite3VdbeSerialGet(&zData[len], serial_type, pTos, p->db->enc);
  if( freeZData ){
    sqliteFree(zData);
  }
  break;
}

/* Opcode: FullKey P1 * *
**
** Extract the complete key from the record that cursor P1 is currently
** pointing to and push the key onto the stack as a string.
**
** Compare this opcode to Recno.  The Recno opcode extracts the first
** 4 bytes of the key and pushes those bytes onto the stack as an
** integer.  This instruction pushes the entire key as a string.
**
** This opcode may not be used on a pseudo-table.
*/
case OP_FullKey: {
  int i = pOp->p1;
  BtCursor *pCrsr;
  Cursor *pC;

  assert( p->apCsr[i]->keyAsData );
  assert( !p->apCsr[i]->pseudoTable );
  assert( i>=0 && i<p->nCursor );
  pTos++;
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    u64 amt;
    char *z;

    sqlite3VdbeCursorMoveto(pC);
    assert( pC->intKey==0 );
    sqlite3BtreeKeySize(pCrsr, &amt);
    if( amt<=0 ){
      rc = SQLITE_CORRUPT;
      goto abort_due_to_error;
    }
    if( amt>NBFS ){
      z = sqliteMallocRaw( amt );
      if( z==0 ) goto no_mem;
      pTos->flags = MEM_Blob | MEM_Dyn;
    }else{
      z = pTos->zShort;
      pTos->flags = MEM_Blob | MEM_Short;
    }
    sqlite3BtreeKey(pCrsr, 0, amt, z);
    pTos->z = z;
    pTos->n = amt;
  }
  break;
}

/* Opcode: NullRow P1 * *
**
** Move the cursor P1 to a null row.  Any OP_Column operations
** that occur while the cursor is on the null row will always push 
** a NULL onto the stack.
*/
case OP_NullRow: {
  int i = pOp->p1;
  Cursor *pC;

  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  pC->nullRow = 1;
  pC->recnoIsValid = 0;
  break;
}

/* Opcode: Last P1 P2 *
**
** The next use of the Recno or Column or Next instruction for P1 
** will refer to the last entry in the database table or index.
** If the table or index is empty and P2>0, then jump immediately to P2.
** If P2 is 0 or if the table or index is not empty, fall through
** to the following instruction.
*/
case OP_Last: {
  int i = pOp->p1;
  Cursor *pC;
  BtCursor *pCrsr;

  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  if( (pCrsr = pC->pCursor)!=0 ){
    int res;
    rc = sqlite3BtreeLast(pCrsr, &res);
    pC->nullRow = res;
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
    if( res && pOp->p2>0 ){
      pc = pOp->p2 - 1;
    }
  }else{
    pC->nullRow = 0;
  }
  break;
}

/* Opcode: Rewind P1 P2 *
**
** The next use of the Recno or Column or Next instruction for P1 
** will refer to the first entry in the database table or index.
** If the table or index is empty and P2>0, then jump immediately to P2.
** If P2 is 0 or if the table or index is not empty, fall through
** to the following instruction.
*/
case OP_Rewind: {
  int i = pOp->p1;
  Cursor *pC;
  BtCursor *pCrsr;
  int res;

  assert( i>=0 && i<p->nCursor );
  pC = p->apCsr[i];
  if( (pCrsr = pC->pCursor)!=0 ){
    rc = sqlite3BtreeFirst(pCrsr, &res);
    pC->atFirst = res==0;
    pC->deferredMoveto = 0;
    pC->cacheValid = 0;
  }else{
    res = 1;
  }
  pC->nullRow = res;
  if( res && pOp->p2>0 ){
    pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: Next P1 P2 *
**
** Advance cursor P1 so that it points to the next key/data pair in its
** table or index.  If there are no more key/value pairs then fall through
** to the following instruction.  But if the cursor advance was successful,
** jump immediately to P2.
**
** See also: Prev
*/
/* Opcode: Prev P1 P2 *
**
** Back up cursor P1 so that it points to the previous key/data pair in its
** table or index.  If there is no previous key/value pairs then fall through
** to the following instruction.  But if the cursor backup was successful,
** jump immediately to P2.
*/
case OP_Prev:
case OP_Next: {
  Cursor *pC;
  BtCursor *pCrsr;

  CHECK_FOR_INTERRUPT;
  assert( pOp->p1>=0 && pOp->p1<p->nCursor );
  pC = p->apCsr[pOp->p1];
  if( (pCrsr = pC->pCursor)!=0 ){
    int res;
    if( pC->nullRow ){
      res = 1;
    }else{
      assert( pC->deferredMoveto==0 );
      rc = pOp->opcode==OP_Next ? sqlite3BtreeNext(pCrsr, &res) :
                                  sqlite3BtreePrevious(pCrsr, &res);
      pC->nullRow = res;
      pC->cacheValid = 0;
    }
    if( res==0 ){
      pc = pOp->p2 - 1;
      sqlite3_search_count++;
    }
  }else{
    pC->nullRow = 1;
  }
  pC->recnoIsValid = 0;
  break;
}

/* Opcode: IdxPut P1 P2 P3
**
** The top of the stack holds a SQL index key made using the
** MakeIdxKey instruction.  This opcode writes that key into the
** index P1.  Data for the entry is nil.
**
** If P2==1, then the key must be unique.  If the key is not unique,
** the program aborts with a SQLITE_CONSTRAINT error and the database
** is rolled back.  If P3 is not null, then it becomes part of the
** error message returned with the SQLITE_CONSTRAINT.
*/
case OP_IdxPut: {
  int i = pOp->p1;
  Cursor *pC;
  BtCursor *pCrsr;
  assert( pTos>=p->aStack );
  assert( i>=0 && i<p->nCursor );
  assert( pTos->flags & MEM_Blob );
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    int nKey = pTos->n;
    const char *zKey = pTos->z;
    if( pOp->p2 ){
      int res;
      int len;
      u64 n;
   
      /* 'len' is the length of the key minus the rowid at the end */
      len = nKey-2;
      while( zKey[len] && --len );

      rc = sqlite3BtreeMoveto(pCrsr, zKey, len, &res);
      if( rc!=SQLITE_OK ) goto abort_due_to_error;
      while( res!=0 ){
        int c;
        sqlite3BtreeKeySize(pCrsr, &n);
        if( n==nKey && 
            sqlite3VdbeIdxKeyCompare(pC, len, zKey, &c)==SQLITE_OK
            && c==0
        ){
          rc = SQLITE_CONSTRAINT;
          if( pOp->p3 && pOp->p3[0] ){
            sqlite3SetString(&p->zErrMsg, pOp->p3, (char*)0);
          }
          goto abort_due_to_error;
        }
        if( res<0 ){
          sqlite3BtreeNext(pCrsr, &res);
          res = +1;
        }else{
          break;
        }
      }
    }
    assert( pC->intKey==0 );
    rc = sqlite3BtreeInsert(pCrsr, zKey, nKey, "", 0);
    assert( pC->deferredMoveto==0 );
    pC->cacheValid = 0;
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: IdxDelete P1 * *
**
** The top of the stack is an index key built using the MakeIdxKey opcode.
** This opcode removes that entry from the index.
*/
case OP_IdxDelete: {
  int i = pOp->p1;
  Cursor *pC;
  BtCursor *pCrsr;
  assert( pTos>=p->aStack );
  assert( pTos->flags & MEM_Blob );
  assert( i>=0 && i<p->nCursor );
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    int rx, res;
    rx = sqlite3BtreeMoveto(pCrsr, pTos->z, pTos->n, &res);
    if( rx==SQLITE_OK && res==0 ){
      rc = sqlite3BtreeDelete(pCrsr);
    }
    assert( pC->deferredMoveto==0 );
    pC->cacheValid = 0;
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: IdxRecno P1 * *
**
** Push onto the stack an integer which is the varint located at the
** end of the index key pointed to by cursor P1.  These integer should be
** the record number of the table entry to which this index entry points.
**
** See also: Recno, MakeIdxKey.
*/
case OP_IdxRecno: {
  int i = pOp->p1;
  BtCursor *pCrsr;
  Cursor *pC;

  assert( i>=0 && i<p->nCursor );
  pTos++;
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    i64 rowid;

    assert( pC->deferredMoveto==0 );
    assert( pC->intKey==0 );
    rc = sqlite3VdbeIdxRowid(pCrsr, &rowid);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }
    pTos->flags = MEM_Int;
    pTos->i = rowid;

#if 0
    /* Read the final 9 bytes of the key into buf[]. If the whole key is
    ** less than 9 bytes then just load the whole thing. Set len to the 
    ** number of bytes read.
    */
    sqlite3BtreeKeySize(pCrsr, &sz);
    len = ((sz>10)?10:sz);
    rc = sqlite3BtreeKey(pCrsr, sz-len, len, buf);
    if( rc!=SQLITE_OK ){
      goto abort_due_to_error;
    }

    len--;
    if( buf[len]&0x80 ){
      /* If the last byte read has the 0x80 bit set, then the key does
      ** not end with a varint. Push a NULL onto the stack instead.
      */
      pTos->flags = MEM_Null;
    }else{
      /* Find the start of the varint by searching backwards for a 0x00
      ** byte. If one does not exists, then intepret the whole 9 bytes as a
      ** varint.
      */
      while( len && buf[len-1] ){
        len--;
      }
      sqlite3GetVarint(&buf[len], &sz);
      pTos->flags = MEM_Int;
      pTos->i = sz;
    }
#endif
  }else{
    pTos->flags = MEM_Null;
  }
  break;
}

/* Opcode: IdxGT P1 P2 *
**
** Compare the top of the stack against the key on the index entry that
** cursor P1 is currently pointing to.  Ignore the ROWID of the
** index entry.  If the index entry is greater than the top of the stack
** then jump to P2.  Otherwise fall through to the next instruction.
** In either case, the stack is popped once.
*/
/* Opcode: IdxGE P1 P2 P3
**
** Compare the top of the stack against the key on the index entry that
** cursor P1 is currently pointing to.  Ignore the ROWID of the
** index entry.  If the index in the cursor is greater than or equal to 
** the top of the stack
** then jump to P2.  Otherwise fall through to the next instruction.
** In either case, the stack is popped once.
**
** If P3 is the "+" string (or any other non-NULL string) then the
** index taken from the top of the stack is temporarily increased by
** an epsilon prior to the comparison.  This make the opcode work
** like IdxGT except that if the key from the stack is a prefix of
** the key in the cursor, the result is false whereas it would be
** true with IdxGT.
*/
/* Opcode: IdxLT P1 P2 P3
**
** Compare the top of the stack against the key on the index entry that
** cursor P1 is currently pointing to.  Ignore the ROWID of the
** index entry.  If the index entry is less than the top of the stack
** then jump to P2.  Otherwise fall through to the next instruction.
** In either case, the stack is popped once.
**
** If P3 is the "+" string (or any other non-NULL string) then the
** index taken from the top of the stack is temporarily increased by
** an epsilon prior to the comparison.  This makes the opcode work
** like IdxLE.
*/
case OP_IdxLT:
case OP_IdxGT:
case OP_IdxGE: {
  int i= pOp->p1;
  BtCursor *pCrsr;
  Cursor *pC;

  assert( i>=0 && i<p->nCursor );
  assert( pTos>=p->aStack );
  if( (pCrsr = (pC = p->apCsr[i])->pCursor)!=0 ){
    int res, rc;
 
    Stringify(pTos, db->enc);
    assert( pC->deferredMoveto==0 );
    *pC->pIncrKey = pOp->p3!=0;
    assert( pOp->p3==0 || pOp->opcode!=OP_IdxGT );
    rc = sqlite3VdbeIdxKeyCompare(pC, pTos->n, pTos->z, &res);
    *pC->pIncrKey = 0;
    if( rc!=SQLITE_OK ){
      break;
    }
    if( pOp->opcode==OP_IdxLT ){
      res = -res;
    }else if( pOp->opcode==OP_IdxGE ){
      res++;
    }
    if( res>0 ){
      pc = pOp->p2 - 1 ;
    }
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: IdxIsNull P1 P2 *
**
** The top of the stack contains an index entry such as might be generated
** by the MakeIdxKey opcode.  This routine looks at the first P1 fields of
** that key.  If any of the first P1 fields are NULL, then a jump is made
** to address P2.  Otherwise we fall straight through.
**
** The index entry is always popped from the stack.
*/
case OP_IdxIsNull: {
  int i = pOp->p1;
  int k, n;
  const char *z;

  assert( pTos>=p->aStack );
  assert( pTos->flags & MEM_Blob );
  z = pTos->z;
  n = pTos->n;
  for(k=0; k<n && i>0; i--){
    u64 serial_type;
    k += sqlite3GetVarint(&z[k], &serial_type);
    if( serial_type==6 ){   /* Serial type 6 is a NULL */
      pc = pOp->p2-1;
      break;
    }
    k += sqlite3VdbeSerialTypeLen(serial_type);
  }
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: Destroy P1 P2 *
**
** Delete an entire database table or index whose root page in the database
** file is given by P1.
**
** The table being destroyed is in the main database file if P2==0.  If
** P2==1 then the table to be clear is in the auxiliary database file
** that is used to store tables create using CREATE TEMPORARY TABLE.
**
** See also: Clear
*/
case OP_Destroy: {
  rc = sqlite3BtreeDropTable(db->aDb[pOp->p2].pBt, pOp->p1);
  break;
}

/* Opcode: Clear P1 P2 *
**
** Delete all contents of the database table or index whose root page
** in the database file is given by P1.  But, unlike Destroy, do not
** remove the table or index from the database file.
**
** The table being clear is in the main database file if P2==0.  If
** P2==1 then the table to be clear is in the auxiliary database file
** that is used to store tables create using CREATE TEMPORARY TABLE.
**
** See also: Destroy
*/
case OP_Clear: {
  rc = sqlite3BtreeClearTable(db->aDb[pOp->p2].pBt, pOp->p1);
  break;
}

/* Opcode: CreateTable * P2 P3
**
** Allocate a new table in the main database file if P2==0 or in the
** auxiliary database file if P2==1.  Push the page number
** for the root page of the new table onto the stack.
**
** The root page number is also written to a memory location that P3
** points to.  This is the mechanism is used to write the root page
** number into the parser's internal data structures that describe the
** new table.
**
** The difference between a table and an index is this:  A table must
** have a 4-byte integer key and can have arbitrary data.  An index
** has an arbitrary key but no data.
**
** See also: CreateIndex
*/
/* Opcode: CreateIndex * P2 P3
**
** Allocate a new index in the main database file if P2==0 or in the
** auxiliary database file if P2==1.  Push the page number of the
** root page of the new index onto the stack.
**
** See documentation on OP_CreateTable for additional information.
*/
case OP_CreateIndex:
case OP_CreateTable: {
  int pgno;
  int flags;
  assert( pOp->p3!=0 && pOp->p3type==P3_POINTER );
  assert( pOp->p2>=0 && pOp->p2<db->nDb );
  assert( db->aDb[pOp->p2].pBt!=0 );
  if( pOp->opcode==OP_CreateTable ){
    /* flags = BTREE_INTKEY; */
    flags = BTREE_LEAFDATA|BTREE_INTKEY;
  }else{
    flags = BTREE_ZERODATA;
  }
  rc = sqlite3BtreeCreateTable(db->aDb[pOp->p2].pBt, &pgno, flags);
  pTos++;
  if( rc==SQLITE_OK ){
    pTos->i = pgno;
    pTos->flags = MEM_Int;
    *(u32*)pOp->p3 = pgno;
    pOp->p3 = 0;
  }else{
    pTos->flags = MEM_Null;
  }
  break;
}

/* Opcode: IntegrityCk * P2 *
**
** Do an analysis of the currently open database.  Push onto the
** stack the text of an error message describing any problems.
** If there are no errors, push a "ok" onto the stack.
**
** The root page numbers of all tables in the database are integer
** values on the stack.  This opcode pulls as many integers as it
** can off of the stack and uses those numbers as the root pages.
**
** If P2 is not zero, the check is done on the auxiliary database
** file, not the main database file.
**
** This opcode is used for testing purposes only.
*/
case OP_IntegrityCk: {
  int nRoot;
  int *aRoot;
  int j;
  char *z;

  for(nRoot=0; &pTos[-nRoot]>=p->aStack; nRoot++){
    if( (pTos[-nRoot].flags & MEM_Int)==0 ) break;
  }
  assert( nRoot>0 );
  aRoot = sqliteMallocRaw( sizeof(int*)*(nRoot+1) );
  if( aRoot==0 ) goto no_mem;
  for(j=0; j<nRoot; j++){
    Mem *pMem = &pTos[-j];
    aRoot[j] = pMem->i;
  }
  aRoot[j] = 0;
  popStack(&pTos, nRoot);
  pTos++;
  z = sqlite3BtreeIntegrityCheck(db->aDb[pOp->p2].pBt, aRoot, nRoot);
  if( z==0 || z[0]==0 ){
    if( z ) sqliteFree(z);
    pTos->z = "ok";
    pTos->n = 3;
    pTos->flags = MEM_Str | MEM_Static;
  }else{
    pTos->z = z;
    pTos->n = strlen(z) + 1;
    pTos->flags = MEM_Str | MEM_Dyn;
  }
  if( db->enc!=TEXT_Utf8 ){
    SetEncodingFlags(pTos, TEXT_Utf8);
    SetEncoding(pTos, encToFlags(db->enc)|MEM_Term);
  }
  sqliteFree(aRoot);
  break;
}

/* Opcode: ListWrite * * *
**
** Write the integer on the top of the stack
** into the temporary storage list.
*/
case OP_ListWrite: {
  Keylist *pKeylist;
  assert( pTos>=p->aStack );
  pKeylist = p->pList;
  if( pKeylist==0 || pKeylist->nUsed>=pKeylist->nKey ){
    pKeylist = sqliteMallocRaw( sizeof(Keylist)+999*sizeof(pKeylist->aKey[0]) );
    if( pKeylist==0 ) goto no_mem;
    pKeylist->nKey = 1000;
    pKeylist->nRead = 0;
    pKeylist->nUsed = 0;
    pKeylist->pNext = p->pList;
    p->pList = pKeylist;
  }
  Integerify(pTos, db->enc);
  pKeylist->aKey[pKeylist->nUsed++] = pTos->i;
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: ListRewind * * *
**
** Rewind the temporary buffer back to the beginning.
*/
case OP_ListRewind: {
  /* What this opcode codes, really, is reverse the order of the
  ** linked list of Keylist structures so that they are read out
  ** in the same order that they were read in. */
  Keylist *pRev, *pTop;
  pRev = 0;
  while( p->pList ){
    pTop = p->pList;
    p->pList = pTop->pNext;
    pTop->pNext = pRev;
    pRev = pTop;
  }
  p->pList = pRev;
  break;
}

/* Opcode: ListRead * P2 *
**
** Attempt to read an integer from the temporary storage buffer
** and push it onto the stack.  If the storage buffer is empty, 
** push nothing but instead jump to P2.
*/
case OP_ListRead: {
  Keylist *pKeylist;
  CHECK_FOR_INTERRUPT;
  pKeylist = p->pList;
  if( pKeylist!=0 ){
    assert( pKeylist->nRead>=0 );
    assert( pKeylist->nRead<pKeylist->nUsed );
    assert( pKeylist->nRead<pKeylist->nKey );
    pTos++;
    pTos->i = pKeylist->aKey[pKeylist->nRead++];
    pTos->flags = MEM_Int;
    if( pKeylist->nRead>=pKeylist->nUsed ){
      p->pList = pKeylist->pNext;
      sqliteFree(pKeylist);
    }
  }else{
    pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: ListReset * * *
**
** Reset the temporary storage buffer so that it holds nothing.
*/
case OP_ListReset: {
  if( p->pList ){
    sqlite3VdbeKeylistFree(p->pList);
    p->pList = 0;
  }
  break;
}

/* Opcode: ListPush * * * 
**
** Save the current Vdbe list such that it can be restored by a ListPop
** opcode. The list is empty after this is executed.
*/
case OP_ListPush: {
  p->keylistStackDepth++;
  assert(p->keylistStackDepth > 0);
  p->keylistStack = sqliteRealloc(p->keylistStack, 
          sizeof(Keylist *) * p->keylistStackDepth);
  if( p->keylistStack==0 ) goto no_mem;
  p->keylistStack[p->keylistStackDepth - 1] = p->pList;
  p->pList = 0;
  break;
}

/* Opcode: ListPop * * * 
**
** Restore the Vdbe list to the state it was in when ListPush was last
** executed.
*/
case OP_ListPop: {
  assert(p->keylistStackDepth > 0);
  p->keylistStackDepth--;
  sqlite3VdbeKeylistFree(p->pList);
  p->pList = p->keylistStack[p->keylistStackDepth];
  p->keylistStack[p->keylistStackDepth] = 0;
  if( p->keylistStackDepth == 0 ){
    sqliteFree(p->keylistStack);
    p->keylistStack = 0;
  }
  break;
}

/* Opcode: ContextPush * * * 
**
** Save the current Vdbe context such that it can be restored by a ContextPop
** opcode. The context stores the last insert row id, the last statement change
** count, and the current statement change count.
*/
case OP_ContextPush: {
  p->contextStackDepth++;
  assert(p->contextStackDepth > 0);
  p->contextStack = sqliteRealloc(p->contextStack, 
          sizeof(Context) * p->contextStackDepth);
  if( p->contextStack==0 ) goto no_mem;
  p->contextStack[p->contextStackDepth - 1].lastRowid = p->db->lastRowid;
  p->contextStack[p->contextStackDepth - 1].lsChange = p->db->lsChange;
  p->contextStack[p->contextStackDepth - 1].csChange = p->db->csChange;
  break;
}

/* Opcode: ContextPop * * * 
**
** Restore the Vdbe context to the state it was in when contextPush was last
** executed. The context stores the last insert row id, the last statement
** change count, and the current statement change count.
*/
case OP_ContextPop: {
  assert(p->contextStackDepth > 0);
  p->contextStackDepth--;
  p->db->lastRowid = p->contextStack[p->contextStackDepth].lastRowid;
  p->db->lsChange = p->contextStack[p->contextStackDepth].lsChange;
  p->db->csChange = p->contextStack[p->contextStackDepth].csChange;
  if( p->contextStackDepth == 0 ){
    sqliteFree(p->contextStack);
    p->contextStack = 0;
  }
  break;
}

/* Opcode: SortPut * * *
**
** The TOS is the key and the NOS is the data.  Pop both from the stack
** and put them on the sorter.  The key and data should have been
** made using SortMakeKey and SortMakeRec, respectively.
*/
case OP_SortPut: {
  Mem *pNos = &pTos[-1];
  Sorter *pSorter;
  assert( pNos>=p->aStack );
  if( Dynamicify(pTos, db->enc) || Dynamicify(pNos, db->enc) ) goto no_mem;
  pSorter = sqliteMallocRaw( sizeof(Sorter) );
  if( pSorter==0 ) goto no_mem;
  pSorter->pNext = p->pSort;
  p->pSort = pSorter;
  assert( pTos->flags & MEM_Dyn );
  pSorter->nKey = pTos->n;
  pSorter->zKey = pTos->z;
  assert( pNos->flags & MEM_Dyn );
  pSorter->nData = pNos->n;
  pSorter->pData = pNos->z;
  pTos -= 2;
  break;
}

/* Opcode: Sort * * P3
**
** Sort all elements on the sorter.  The algorithm is a
** mergesort.  The P3 argument is a pointer to a KeyInfo structure
** that describes the keys to be sorted.
*/
case OP_Sort: {
  int i;
  KeyInfo *pKeyInfo = (KeyInfo*)pOp->p3;
  Sorter *pElem;
  Sorter *apSorter[NSORT];
  pKeyInfo->enc = p->db->enc;
  for(i=0; i<NSORT; i++){
    apSorter[i] = 0;
  }
  while( p->pSort ){
    pElem = p->pSort;
    p->pSort = pElem->pNext;
    pElem->pNext = 0;
    for(i=0; i<NSORT-1; i++){
    if( apSorter[i]==0 ){
        apSorter[i] = pElem;
        break;
      }else{
        pElem = Merge(apSorter[i], pElem, pKeyInfo);
        apSorter[i] = 0;
      }
    }
    if( i>=NSORT-1 ){
      apSorter[NSORT-1] = Merge(apSorter[NSORT-1],pElem, pKeyInfo);
    }
  }
  pElem = 0;
  for(i=0; i<NSORT; i++){
    pElem = Merge(apSorter[i], pElem, pKeyInfo);
  }
  p->pSort = pElem;
  break;
}

/* Opcode: SortNext * P2 *
**
** Push the data for the topmost element in the sorter onto the
** stack, then remove the element from the sorter.  If the sorter
** is empty, push nothing on the stack and instead jump immediately 
** to instruction P2.
*/
case OP_SortNext: {
  Sorter *pSorter = p->pSort;
  CHECK_FOR_INTERRUPT;
  if( pSorter!=0 ){
    p->pSort = pSorter->pNext;
    pTos++;
    pTos->z = pSorter->pData;
    pTos->n = pSorter->nData;
    /* FIX ME: I don't understand this. What does the sorter return? 
    ** I thought it would be the commented out flags.
    */
    /* pTos->flags = MEM_Blob|MEM_Dyn; */
    pTos->flags = MEM_Str|MEM_Dyn|MEM_Utf8|MEM_Term;
    sqliteFree(pSorter->zKey);
    sqliteFree(pSorter);
  }else{
    pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: SortReset * * *
**
** Remove any elements that remain on the sorter.
*/
case OP_SortReset: {
  sqlite3VdbeSorterReset(p);
  break;
}

/* Opcode: FileOpen * * P3
**
** Open the file named by P3 for reading using the FileRead opcode.
** If P3 is "stdin" then open standard input for reading.
*/
case OP_FileOpen: {
  assert( pOp->p3!=0 );
  if( p->pFile ){
    if( p->pFile!=stdin ) fclose(p->pFile);
    p->pFile = 0;
  }
  if( sqlite3StrICmp(pOp->p3,"stdin")==0 ){
    p->pFile = stdin;
  }else{
    p->pFile = fopen(pOp->p3, "r");
  }
  if( p->pFile==0 ){
    sqlite3SetString(&p->zErrMsg,"unable to open file: ", pOp->p3, (char*)0);
    rc = SQLITE_ERROR;
  }
  break;
}

/* Opcode: FileRead P1 P2 P3
**
** Read a single line of input from the open file (the file opened using
** FileOpen).  If we reach end-of-file, jump immediately to P2.  If
** we are able to get another line, split the line apart using P3 as
** a delimiter.  There should be P1 fields.  If the input line contains
** more than P1 fields, ignore the excess.  If the input line contains
** fewer than P1 fields, assume the remaining fields contain NULLs.
**
** Input ends if a line consists of just "\.".  A field containing only
** "\N" is a null field.  The backslash \ character can be used be used
** to escape newlines or the delimiter.
*/
case OP_FileRead: {
  int n, eol, nField, i, c, nDelim;
  char *zDelim, *z;
  CHECK_FOR_INTERRUPT;
  if( p->pFile==0 ) goto fileread_jump;
  nField = pOp->p1;
  if( nField<=0 ) goto fileread_jump;
  if( nField!=p->nField || p->azField==0 ){
    char **azField = sqliteRealloc(p->azField, sizeof(char*)*nField+1);
    if( azField==0 ){ goto no_mem; }
    p->azField = azField;
    p->nField = nField;
  }
  n = 0;
  eol = 0;
  while( eol==0 ){
    if( p->zLine==0 || n+200>p->nLineAlloc ){
      char *zLine;
      p->nLineAlloc = p->nLineAlloc*2 + 300;
      zLine = sqliteRealloc(p->zLine, p->nLineAlloc);
      if( zLine==0 ){
        p->nLineAlloc = 0;
        sqliteFree(p->zLine);
        p->zLine = 0;
        goto no_mem;
      }
      p->zLine = zLine;
    }
    if( vdbe_fgets(&p->zLine[n], p->nLineAlloc-n, p->pFile)==0 ){
      eol = 1;
      p->zLine[n] = 0;
    }else{
      int c;
      while( (c = p->zLine[n])!=0 ){
        if( c=='\\' ){
          if( p->zLine[n+1]==0 ) break;
          n += 2;
        }else if( c=='\n' ){
          p->zLine[n] = 0;
          eol = 1;
          break;
        }else{
          n++;
        }
      }
    }
  }
  if( n==0 ) goto fileread_jump;
  z = p->zLine;
  if( z[0]=='\\' && z[1]=='.' && z[2]==0 ){
    goto fileread_jump;
  }
  zDelim = pOp->p3;
  if( zDelim==0 ) zDelim = "\t";
  c = zDelim[0];
  nDelim = strlen(zDelim);
  p->azField[0] = z;
  for(i=1; *z!=0 && i<=nField; i++){
    int from, to;
    from = to = 0;
    if( z[0]=='\\' && z[1]=='N' 
       && (z[2]==0 || strncmp(&z[2],zDelim,nDelim)==0) ){
      if( i<=nField ) p->azField[i-1] = 0;
      z += 2 + nDelim;
      if( i<nField ) p->azField[i] = z;
      continue;
    }
    while( z[from] ){
      if( z[from]=='\\' && z[from+1]!=0 ){
        int tx = z[from+1];
        switch( tx ){
          case 'b':  tx = '\b'; break;
          case 'f':  tx = '\f'; break;
          case 'n':  tx = '\n'; break;
          case 'r':  tx = '\r'; break;
          case 't':  tx = '\t'; break;
          case 'v':  tx = '\v'; break;
          default:   break;
        }
        z[to++] = tx;
        from += 2;
        continue;
      }
      if( z[from]==c && strncmp(&z[from],zDelim,nDelim)==0 ) break;
      z[to++] = z[from++];
    }
    if( z[from] ){
      z[to] = 0;
      z += from + nDelim;
      if( i<nField ) p->azField[i] = z;
    }else{
      z[to] = 0;
      z = "";
    }
  }
  while( i<nField ){
    p->azField[i++] = 0;
  }
  break;

  /* If we reach end-of-file, or if anything goes wrong, jump here.
  ** This code will cause a jump to P2 */
fileread_jump:
  pc = pOp->p2 - 1;
  break;
}

/* Opcode: FileColumn P1 * *
**
** Push onto the stack the P1-th column of the most recently read line
** from the input file.
*/
case OP_FileColumn: {
  int i = pOp->p1;
  char *z;
  assert( i>=0 && i<p->nField );
  if( p->azField ){
    z = p->azField[i];
  }else{
    z = 0;
  }
  pTos++;
  if( z ){
    pTos->n = strlen(z) + 1;
    pTos->z = z;
    pTos->flags = MEM_Utf8 | MEM_Str | MEM_Ephem | MEM_Term;
    SetEncoding(pTos, encToFlags(db->enc)|MEM_Term);
  }else{
    pTos->flags = MEM_Null;
  }
  break;
}

/* Opcode: MemStore P1 P2 *
**
** Write the top of the stack into memory location P1.
** P1 should be a small integer since space is allocated
** for all memory locations between 0 and P1 inclusive.
**
** After the data is stored in the memory location, the
** stack is popped once if P2 is 1.  If P2 is zero, then
** the original data remains on the stack.
*/
case OP_MemStore: {
  int i = pOp->p1;
  Mem *pMem;
  assert( pTos>=p->aStack );
  if( i>=p->nMem ){
    int nOld = p->nMem;
    Mem *aMem;
    p->nMem = i + 5;
    aMem = sqliteRealloc(p->aMem, p->nMem*sizeof(p->aMem[0]));
    if( aMem==0 ) goto no_mem;
    if( aMem!=p->aMem ){
      int j;
      for(j=0; j<nOld; j++){
        if( aMem[j].flags & MEM_Short ){
          aMem[j].z = aMem[j].zShort;
        }
      }
    }
    p->aMem = aMem;
    if( nOld<p->nMem ){
      memset(&p->aMem[nOld], 0, sizeof(p->aMem[0])*(p->nMem-nOld));
    }
  }
  Deephemeralize(pTos);
  pMem = &p->aMem[i];
  Release(pMem);
  *pMem = *pTos;
  if( pMem->flags & MEM_Dyn ){
    if( pOp->p2 ){
      pTos->flags = MEM_Null;
    }else{
      pMem->z = sqliteMallocRaw( pMem->n );
      if( pMem->z==0 ) goto no_mem;
      memcpy(pMem->z, pTos->z, pMem->n);
    }
  }else if( pMem->flags & MEM_Short ){
    pMem->z = pMem->zShort;
  }
  if( pOp->p2 ){
    Release(pTos);
    pTos--;
  }
  break;
}

/* Opcode: MemLoad P1 * *
**
** Push a copy of the value in memory location P1 onto the stack.
**
** If the value is a string, then the value pushed is a pointer to
** the string that is stored in the memory location.  If the memory
** location is subsequently changed (using OP_MemStore) then the
** value pushed onto the stack will change too.
*/
case OP_MemLoad: {
  int i = pOp->p1;
  assert( i>=0 && i<p->nMem );
  pTos++;
  memcpy(pTos, &p->aMem[i], sizeof(pTos[0])-NBFS);;
  if( pTos->flags & (MEM_Str|MEM_Blob) ){
    pTos->flags |= MEM_Ephem;
    pTos->flags &= ~(MEM_Dyn|MEM_Static|MEM_Short);
  }
  break;
}

/* Opcode: MemIncr P1 P2 *
**
** Increment the integer valued memory cell P1 by 1.  If P2 is not zero
** and the result after the increment is greater than zero, then jump
** to P2.
**
** This instruction throws an error if the memory cell is not initially
** an integer.
*/
case OP_MemIncr: {
  int i = pOp->p1;
  Mem *pMem;
  assert( i>=0 && i<p->nMem );
  pMem = &p->aMem[i];
  assert( pMem->flags==MEM_Int );
  pMem->i++;
  if( pOp->p2>0 && pMem->i>0 ){
     pc = pOp->p2 - 1;
  }
  break;
}

/* Opcode: AggReset * P2 *
**
** Reset the aggregator so that it no longer contains any data.
** Future aggregator elements will contain P2 values each.
*/
case OP_AggReset: {
  sqlite3VdbeAggReset(&p->agg);
  p->agg.nMem = pOp->p2;
  p->agg.apFunc = sqliteMalloc( p->agg.nMem*sizeof(p->agg.apFunc[0]) );
  if( p->agg.apFunc==0 ) goto no_mem;
  break;
}

/* Opcode: AggInit * P2 P3
**
** Initialize the function parameters for an aggregate function.
** The aggregate will operate out of aggregate column P2.
** P3 is a pointer to the FuncDef structure for the function.
*/
case OP_AggInit: {
  int i = pOp->p2;
  assert( i>=0 && i<p->agg.nMem );
  p->agg.apFunc[i] = (FuncDef*)pOp->p3;
  break;
}

/* Opcode: AggFunc * P2 P3
**
** Execute the step function for an aggregate.  The
** function has P2 arguments.  P3 is a pointer to the FuncDef
** structure that specifies the function.
**
** The top of the stack must be an integer which is the index of
** the aggregate column that corresponds to this aggregate function.
** Ideally, this index would be another parameter, but there are
** no free parameters left.  The integer is popped from the stack.
*/
case OP_AggFunc: {
  int n = pOp->p2;
  int i;
  Mem *pMem, *pRec;
  sqlite3_context ctx;
  sqlite3_value **apVal;

  assert( n>=0 );
  assert( pTos->flags==MEM_Int );
  pRec = &pTos[-n];
  assert( pRec>=p->aStack );

  apVal = p->apArg;
  assert( apVal || n==0 );

  for(i=0; i<n; i++, pRec++){
    apVal[i] = pRec;
    SetEncodingFlags(pRec, db->enc);
    MemSetTypeFlags(pRec);
  }
  i = pTos->i;
  assert( i>=0 && i<p->agg.nMem );
  ctx.pFunc = (FuncDef*)pOp->p3;
  pMem = &p->agg.pCurrent->aMem[i];
  ctx.s.z = pMem->zShort;  /* Space used for small aggregate contexts */
  ctx.pAgg = pMem->z;
  ctx.cnt = ++pMem->i;
  ctx.isError = 0;
  ctx.isStep = 1;
  (ctx.pFunc->xStep)(&ctx, n, apVal);
  pMem->z = ctx.pAgg;
  pMem->flags = MEM_AggCtx;
  popStack(&pTos, n+1);
  if( ctx.isError ){
    rc = SQLITE_ERROR;
  }
  break;
}

/* Opcode: AggFocus * P2 *
**
** Pop the top of the stack and use that as an aggregator key.  If
** an aggregator with that same key already exists, then make the
** aggregator the current aggregator and jump to P2.  If no aggregator
** with the given key exists, create one and make it current but
** do not jump.
**
** The order of aggregator opcodes is important.  The order is:
** AggReset AggFocus AggNext.  In other words, you must execute
** AggReset first, then zero or more AggFocus operations, then
** zero or more AggNext operations.  You must not execute an AggFocus
** in between an AggNext and an AggReset.
*/
case OP_AggFocus: {
  AggElem *pElem;
  char *zKey;
  int nKey;

  assert( pTos>=p->aStack );
  Stringify(pTos, db->enc);
  zKey = pTos->z;
  nKey = pTos->n;
  pElem = sqlite3HashFind(&p->agg.hash, zKey, nKey);
  if( pElem ){
    p->agg.pCurrent = pElem;
    pc = pOp->p2 - 1;
  }else{
    AggInsert(&p->agg, zKey, nKey);
    if( sqlite3_malloc_failed ) goto no_mem;
  }
  Release(pTos);
  pTos--;
  break; 
}

/* Opcode: AggSet * P2 *
**
** Move the top of the stack into the P2-th field of the current
** aggregate.  String values are duplicated into new memory.
*/
case OP_AggSet: {
  AggElem *pFocus = AggInFocus(p->agg);
  Mem *pMem;
  int i = pOp->p2;
  assert( pTos>=p->aStack );
  if( pFocus==0 ) goto no_mem;
  assert( i>=0 && i<p->agg.nMem );
  Deephemeralize(pTos);
  pMem = &pFocus->aMem[i];
  Release(pMem);
  *pMem = *pTos;
  if( pMem->flags & MEM_Dyn ){
    pTos->flags = MEM_Null;
  }else if( pMem->flags & MEM_Short ){
    pMem->z = pMem->zShort;
  }
  SetEncodingFlags(pMem, db->enc);
  SetEncoding(pMem, MEM_Utf8|MEM_Term);
  Release(pTos);
  pTos--;
  break;
}

/* Opcode: AggGet * P2 *
**
** Push a new entry onto the stack which is a copy of the P2-th field
** of the current aggregate.  Strings are not duplicated so
** string values will be ephemeral.
*/
case OP_AggGet: {
  AggElem *pFocus = AggInFocus(p->agg);
  Mem *pMem;
  int i = pOp->p2;
  if( pFocus==0 ) goto no_mem;
  assert( i>=0 && i<p->agg.nMem );
  pTos++;
  pMem = &pFocus->aMem[i];
  *pTos = *pMem;
  if( pTos->flags & (MEM_Str|MEM_Blob) ){
    pTos->flags &= ~(MEM_Dyn|MEM_Static|MEM_Short);
    pTos->flags |= MEM_Ephem;
  }
  if( pTos->flags&MEM_Str ){
    SetEncodingFlags(pTos, TEXT_Utf8);
    SetEncoding(pTos, encToFlags(db->enc)|MEM_Term);
  }
  break;
}

/* Opcode: AggNext * P2 *
**
** Make the next aggregate value the current aggregate.  The prior
** aggregate is deleted.  If all aggregate values have been consumed,
** jump to P2.
**
** The order of aggregator opcodes is important.  The order is:
** AggReset AggFocus AggNext.  In other words, you must execute
** AggReset first, then zero or more AggFocus operations, then
** zero or more AggNext operations.  You must not execute an AggFocus
** in between an AggNext and an AggReset.
*/
case OP_AggNext: {
  CHECK_FOR_INTERRUPT;
  if( p->agg.pSearch==0 ){
    p->agg.pSearch = sqliteHashFirst(&p->agg.hash);
  }else{
    p->agg.pSearch = sqliteHashNext(p->agg.pSearch);
  }
  if( p->agg.pSearch==0 ){
    pc = pOp->p2 - 1;
  } else {
    int i;
    sqlite3_context ctx;
    Mem *aMem;
    p->agg.pCurrent = sqliteHashData(p->agg.pSearch);
    aMem = p->agg.pCurrent->aMem;
    for(i=0; i<p->agg.nMem; i++){
      int freeCtx;
      if( p->agg.apFunc[i]==0 ) continue;
      if( p->agg.apFunc[i]->xFinalize==0 ) continue;
      ctx.s.flags = MEM_Null;
      ctx.s.z = aMem[i].zShort;
      ctx.pAgg = (void*)aMem[i].z;
      freeCtx = aMem[i].z && aMem[i].z!=aMem[i].zShort;
      ctx.cnt = aMem[i].i;
      ctx.isStep = 0;
      ctx.pFunc = p->agg.apFunc[i];
      (*p->agg.apFunc[i]->xFinalize)(&ctx);
      if( freeCtx ){
        sqliteFree( aMem[i].z );
      }
      aMem[i] = ctx.s;
      if( aMem[i].flags & MEM_Short ){
        aMem[i].z = aMem[i].zShort;
      }
    }
  }
  break;
}

/* Opcode: Vacuum * * *
**
** Vacuum the entire database.  This opcode will cause other virtual
** machines to be created and run.  It may not be called from within
** a transaction.
*/
case OP_Vacuum: {
  if( sqlite3SafetyOff(db) ) goto abort_due_to_misuse; 
  rc = sqlite3RunVacuum(&p->zErrMsg, db);
  if( sqlite3SafetyOn(db) ) goto abort_due_to_misuse;
  break;
}

/* An other opcode is illegal...
*/
default: {
  sqlite3_snprintf(sizeof(zBuf),zBuf,"%d",pOp->opcode);
  sqlite3SetString(&p->zErrMsg, "unknown opcode ", zBuf, (char*)0);
  rc = SQLITE_INTERNAL;
  break;
}

/*****************************************************************************
** The cases of the switch statement above this line should all be indented
** by 6 spaces.  But the left-most 6 spaces have been removed to improve the
** readability.  From this point on down, the normal indentation rules are
** restored.
*****************************************************************************/
    }

#ifdef VDBE_PROFILE
    {
      long long elapse = hwtime() - start;
      pOp->cycles += elapse;
      pOp->cnt++;
#if 0
        fprintf(stdout, "%10lld ", elapse);
        sqlite3VdbePrintOp(stdout, origPc, &p->aOp[origPc]);
#endif
    }
#endif

    /* The following code adds nothing to the actual functionality
    ** of the program.  It is only here for testing and debugging.
    ** On the other hand, it does burn CPU cycles every time through
    ** the evaluator loop.  So we can leave it out when NDEBUG is defined.
    */
#ifndef NDEBUG
    /* Sanity checking on the top element of the stack */
    if( pTos>=p->aStack ){
      assert( pTos->flags!=0 );  /* Must define some type */
      if( pTos->flags & (MEM_Str|MEM_Blob) ){
        int x = pTos->flags & (MEM_Static|MEM_Dyn|MEM_Ephem|MEM_Short);
        assert( x!=0 );            /* Strings must define a string subtype */
        assert( (x & (x-1))==0 );  /* Only one string subtype can be defined */
        assert( pTos->z!=0 );      /* Strings must have a value */
        /* Mem.z points to Mem.zShort iff the subtype is MEM_Short */
        assert( (pTos->flags & MEM_Short)==0 || pTos->z==pTos->zShort );
        assert( (pTos->flags & MEM_Short)!=0 || pTos->z!=pTos->zShort );
      }else{
        /* Cannot define a string subtype for non-string objects */
        assert( (pTos->flags & (MEM_Static|MEM_Dyn|MEM_Ephem|MEM_Short))==0 );
      }
      /* MEM_Null excludes all other types */
      assert( pTos->flags==MEM_Null || (pTos->flags&MEM_Null)==0 );
    }
    if( pc<-1 || pc>=p->nOp ){
      sqlite3SetString(&p->zErrMsg, "jump destination out of range", (char*)0);
      rc = SQLITE_INTERNAL;
    }
    if( p->trace && pTos>=p->aStack ){
      int i;
      fprintf(p->trace, "Stack:");
      for(i=0; i>-5 && &pTos[i]>=p->aStack; i--){
        if( pTos[i].flags & MEM_Null ){
          fprintf(p->trace, " NULL");
        }else if( (pTos[i].flags & (MEM_Int|MEM_Str))==(MEM_Int|MEM_Str) ){
          fprintf(p->trace, " si:%lld", pTos[i].i);
        }else if( pTos[i].flags & MEM_Int ){
          fprintf(p->trace, " i:%lld", pTos[i].i);
        }else if( pTos[i].flags & MEM_Real ){
          fprintf(p->trace, " r:%g", pTos[i].r);
        }else{
          char zBuf[100];
          prettyPrintMem(&pTos[i], zBuf, 100);
          fprintf(p->trace, " ");
          fprintf(p->trace, zBuf);
        }
      }
      if( rc!=0 ) fprintf(p->trace," rc=%d",rc);
      fprintf(p->trace,"\n");
    }
#endif
  }  /* The end of the for(;;) loop the loops through opcodes */

  /* If we reach this point, it means that execution is finished.
  */
vdbe_halt:
  if( rc ){
    p->rc = rc;
    rc = SQLITE_ERROR;
  }else{
    rc = SQLITE_DONE;
  }
  p->magic = VDBE_MAGIC_HALT;
  p->pTos = pTos;
  return rc;

  /* Jump to here if a malloc() fails.  It's hard to get a malloc()
  ** to fail on a modern VM computer, so this code is untested.
  */
no_mem:
  sqlite3SetString(&p->zErrMsg, "out of memory", (char*)0);
  rc = SQLITE_NOMEM;
  goto vdbe_halt;

  /* Jump to here for an SQLITE_MISUSE error.
  */
abort_due_to_misuse:
  rc = SQLITE_MISUSE;
  /* Fall thru into abort_due_to_error */

  /* Jump to here for any other kind of fatal error.  The "rc" variable
  ** should hold the error number.
  */
abort_due_to_error:
  if( p->zErrMsg==0 ){
    if( sqlite3_malloc_failed ) rc = SQLITE_NOMEM;
    sqlite3SetString(&p->zErrMsg, sqlite3_error_string(rc), (char*)0);
  }
  goto vdbe_halt;

  /* Jump to here if the sqlite3_interrupt() API sets the interrupt
  ** flag.
  */
abort_due_to_interrupt:
  assert( db->flags & SQLITE_Interrupt );
  db->flags &= ~SQLITE_Interrupt;
  if( db->magic!=SQLITE_MAGIC_BUSY ){
    rc = SQLITE_MISUSE;
  }else{
    rc = SQLITE_INTERRUPT;
  }
  sqlite3SetString(&p->zErrMsg, sqlite3_error_string(rc), (char*)0);
  goto vdbe_halt;
}
