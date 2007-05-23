/*
** 2004 April 13
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains routines used to translate between UTF-8, 
** UTF-16, UTF-16BE, and UTF-16LE.
**
** $Id: utf.c,v 1.51 2007/05/23 16:23:09 danielk1977 Exp $
**
** Notes on UTF-8:
**
**   Byte-0    Byte-1    Byte-2    Byte-3    Value
**  0xxxxxxx                                 00000000 00000000 0xxxxxxx
**  110yyyyy  10xxxxxx                       00000000 00000yyy yyxxxxxx
**  1110zzzz  10yyyyyy  10xxxxxx             00000000 zzzzyyyy yyxxxxxx
**  11110uuu  10uuzzzz  10yyyyyy  10xxxxxx   000uuuuu zzzzyyyy yyxxxxxx
**
**
** Notes on UTF-16:  (with wwww+1==uuuuu)
**
**      Word-0               Word-1          Value
**  110110ww wwzzzzyy   110111yy yyxxxxxx    000uuuuu zzzzyyyy yyxxxxxx
**  zzzzyyyy yyxxxxxx                        00000000 zzzzyyyy yyxxxxxx
**
**
** BOM or Byte Order Mark:
**     0xff 0xfe   little-endian utf-16 follows
**     0xfe 0xff   big-endian utf-16 follows
**
*/
#include "sqliteInt.h"
#include <assert.h>
#include "vdbeInt.h"

/*
** The following constant value is used by the SQLITE_BIGENDIAN and
** SQLITE_LITTLEENDIAN macros.
*/
const int sqlite3one = 1;

/*
** This lookup table is used to help decode the first byte of
** a multi-byte UTF8 character.
*/
const unsigned char sqlite3UtfTrans1[] = {
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
  0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
  0x00, 0x01, 0x02, 0x03, 0x00, 0x01, 0x00, 0x00,
};

#define WRITE_UTF8(zOut, c) {                          \
  if( c<0x00080 ){                                     \
    *zOut++ = (c&0xFF);                                \
  }                                                    \
  else if( c<0x00800 ){                                \
    *zOut++ = 0xC0 + ((c>>6)&0x1F);                    \
    *zOut++ = 0x80 + (c & 0x3F);                       \
  }                                                    \
  else if( c<0x10000 ){                                \
    *zOut++ = 0xE0 + ((c>>12)&0x0F);                   \
    *zOut++ = 0x80 + ((c>>6) & 0x3F);                  \
    *zOut++ = 0x80 + (c & 0x3F);                       \
  }else{                                               \
    *zOut++ = 0xF0 + ((c>>18) & 0x07);                 \
    *zOut++ = 0x80 + ((c>>12) & 0x3F);                 \
    *zOut++ = 0x80 + ((c>>6) & 0x3F);                  \
    *zOut++ = 0x80 + (c & 0x3F);                       \
  }                                                    \
}

#define WRITE_UTF16LE(zOut, c) {                                \
  if( c<=0xFFFF ){                                              \
    *zOut++ = (c&0x00FF);                                       \
    *zOut++ = ((c>>8)&0x00FF);                                  \
  }else{                                                        \
    *zOut++ = (((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (c&0x00FF);                                       \
    *zOut++ = (0x00DC + ((c>>8)&0x03));                         \
  }                                                             \
}

#define WRITE_UTF16BE(zOut, c) {                                \
  if( c<=0xFFFF ){                                              \
    *zOut++ = ((c>>8)&0x00FF);                                  \
    *zOut++ = (c&0x00FF);                                       \
  }else{                                                        \
    *zOut++ = (0x00D8 + (((c-0x10000)>>18)&0x03));              \
    *zOut++ = (((c>>10)&0x003F) + (((c-0x10000)>>10)&0x00C0));  \
    *zOut++ = (0x00DC + ((c>>8)&0x03));                         \
    *zOut++ = (c&0x00FF);                                       \
  }                                                             \
}

#define READ_UTF16LE(zIn, c){                                         \
  c = (*zIn++);                                                       \
  c += ((*zIn++)<<8);                                                 \
  if( c>=0xD800 && c<0xE000 ){                                       \
    int c2 = (*zIn++);                                                \
    c2 += ((*zIn++)<<8);                                              \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
    if( (c & 0xFFFF0000)==0 ) c = 0xFFFD;                             \
  }                                                                   \
}

#define READ_UTF16BE(zIn, c){                                         \
  c = ((*zIn++)<<8);                                                  \
  c += (*zIn++);                                                      \
  if( c>=0xD800 && c<0xE000 ){                                       \
    int c2 = ((*zIn++)<<8);                                           \
    c2 += (*zIn++);                                                   \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
    if( (c & 0xFFFF0000)==0 ) c = 0xFFFD;                             \
  }                                                                   \
}

/*
** If the TRANSLATE_TRACE macro is defined, the value of each Mem is
** printed on stderr on the way into and out of sqlite3VdbeMemTranslate().
*/ 
/* #define TRANSLATE_TRACE 1 */

#ifndef SQLITE_OMIT_UTF16
/*
** This routine transforms the internal text encoding used by pMem to
** desiredEnc. It is an error if the string is already of the desired
** encoding, or if *pMem does not contain a string value.
*/
int sqlite3VdbeMemTranslate(Mem *pMem, u8 desiredEnc){
  unsigned char zShort[NBFS]; /* Temporary short output buffer */
  int len;                    /* Maximum length of output string in bytes */
  unsigned char *zOut;                  /* Output buffer */
  unsigned char *zIn;                   /* Input iterator */
  unsigned char *zTerm;                 /* End of input */
  unsigned char *z;                     /* Output iterator */
  unsigned int c;

  assert( pMem->flags&MEM_Str );
  assert( pMem->enc!=desiredEnc );
  assert( pMem->enc!=0 );
  assert( pMem->n>=0 );

#if defined(TRANSLATE_TRACE) && defined(SQLITE_DEBUG)
  {
    char zBuf[100];
    sqlite3VdbeMemPrettyPrint(pMem, zBuf);
    fprintf(stderr, "INPUT:  %s\n", zBuf);
  }
#endif

  /* If the translation is between UTF-16 little and big endian, then 
  ** all that is required is to swap the byte order. This case is handled
  ** differently from the others.
  */
  if( pMem->enc!=SQLITE_UTF8 && desiredEnc!=SQLITE_UTF8 ){
    u8 temp;
    int rc;
    rc = sqlite3VdbeMemMakeWriteable(pMem);
    if( rc!=SQLITE_OK ){
      assert( rc==SQLITE_NOMEM );
      return SQLITE_NOMEM;
    }
    zIn = (u8*)pMem->z;
    zTerm = &zIn[pMem->n];
    while( zIn<zTerm ){
      temp = *zIn;
      *zIn = *(zIn+1);
      zIn++;
      *zIn++ = temp;
    }
    pMem->enc = desiredEnc;
    goto translate_out;
  }

  /* Set len to the maximum number of bytes required in the output buffer. */
  if( desiredEnc==SQLITE_UTF8 ){
    /* When converting from UTF-16, the maximum growth results from
    ** translating a 2-byte character to a 4-byte UTF-8 character.
    ** A single byte is required for the output string
    ** nul-terminator.
    */
    len = pMem->n * 2 + 1;
  }else{
    /* When converting from UTF-8 to UTF-16 the maximum growth is caused
    ** when a 1-byte UTF-8 character is translated into a 2-byte UTF-16
    ** character. Two bytes are required in the output buffer for the
    ** nul-terminator.
    */
    len = pMem->n * 2 + 2;
  }

  /* Set zIn to point at the start of the input buffer and zTerm to point 1
  ** byte past the end.
  **
  ** Variable zOut is set to point at the output buffer. This may be space
  ** obtained from malloc(), or Mem.zShort, if it large enough and not in
  ** use, or the zShort array on the stack (see above).
  */
  zIn = (u8*)pMem->z;
  zTerm = &zIn[pMem->n];
  if( len>NBFS ){
    zOut = sqliteMallocRaw(len);
    if( !zOut ) return SQLITE_NOMEM;
  }else{
    zOut = zShort;
  }
  z = zOut;

  if( pMem->enc==SQLITE_UTF8 ){
    unsigned int iExtra = 0xD800;

    if( 0==(pMem->flags&MEM_Term) && zTerm>zIn && (zTerm[-1]&0x80) ){
      /* This UTF8 string is not nul-terminated, and the last byte is
      ** not a character in the ascii range (codpoints 0..127). This
      ** means the SQLITE_READ_UTF8() macro might read past the end
      ** of the allocated buffer.
      **
      ** There are four possibilities:
      **
      **   1. The last byte is the first byte of a non-ASCII character,
      **
      **   2. The final N bytes of the input string are continuation bytes
      **      and immediately preceding them is the first byte of a 
      **      non-ASCII character.
      **
      **   3. The final N bytes of the input string are continuation bytes
      **      and immediately preceding them is a byte that encodes a 
      **      character in the ASCII range.
      **
      **   4. The entire string consists of continuation characters.
      **
      ** Cases (3) and (4) require no special handling. The SQLITE_READ_UTF8()
      ** macro will not overread the buffer in these cases.
      */
      unsigned char *zExtra = &zTerm[-1];
      while( zExtra>zIn && (zExtra[0]&0xC0)==0x80 ){
        zExtra--;
      }

      if( (zExtra[0]&0xC0)==0xC0 ){
        /* Make a copy of the last character encoding in the input string.
        ** Then make sure it is nul-terminated and use SQLITE_READ_UTF8()
        ** to decode the codepoint. Store the codepoint in variable iExtra,
        ** it will be appended to the output string later.
        */
        unsigned char *zFree = 0;
        unsigned char zBuf[16];
        int nExtra = (pMem->n+zIn-zExtra);
        zTerm = zExtra;
        if( nExtra>15 ){
          zExtra = sqliteMallocRaw(nExtra+1);
          if( !zExtra ){
            return SQLITE_NOMEM;
          }
          zFree = zExtra;
        }else{
          zExtra = zBuf;
        }
        memcpy(zExtra, zTerm, nExtra);
        zExtra[nExtra] = '\0';
        SQLITE_READ_UTF8(zExtra, iExtra);
        sqliteFree(zFree);
      }
    }

    if( desiredEnc==SQLITE_UTF16LE ){
      /* UTF-8 -> UTF-16 Little-endian */
      while( zIn<zTerm ){
        SQLITE_READ_UTF8(zIn, c); 
        WRITE_UTF16LE(z, c);
      }
      if( iExtra!=0xD800 ){
        WRITE_UTF16LE(z, iExtra);
      }
    }else{
      assert( desiredEnc==SQLITE_UTF16BE );
      /* UTF-8 -> UTF-16 Big-endian */
      while( zIn<zTerm ){
        SQLITE_READ_UTF8(zIn, c); 
        WRITE_UTF16BE(z, c);
      }
      if( iExtra!=0xD800 ){
        WRITE_UTF16BE(z, iExtra);
      }
    }
    pMem->n = z - zOut;
    *z++ = 0;
  }else{
    assert( desiredEnc==SQLITE_UTF8 );
    if( pMem->enc==SQLITE_UTF16LE ){
      /* UTF-16 Little-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16LE(zIn, c); 
        WRITE_UTF8(z, c);
      }
    }else{
      /* UTF-16 Little-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16BE(zIn, c); 
        WRITE_UTF8(z, c);
      }
    }
    pMem->n = z - zOut;
  }
  *z = 0;
  assert( (pMem->n+(desiredEnc==SQLITE_UTF8?1:2))<=len );

  sqlite3VdbeMemRelease(pMem);
  pMem->flags &= ~(MEM_Static|MEM_Dyn|MEM_Ephem|MEM_Short);
  pMem->enc = desiredEnc;
  if( zOut==zShort ){
    memcpy(pMem->zShort, zOut, len);
    zOut = (u8*)pMem->zShort;
    pMem->flags |= (MEM_Term|MEM_Short);
  }else{
    pMem->flags |= (MEM_Term|MEM_Dyn);
  }
  pMem->z = (char*)zOut;

translate_out:
#if defined(TRANSLATE_TRACE) && defined(SQLITE_DEBUG)
  {
    char zBuf[100];
    sqlite3VdbeMemPrettyPrint(pMem, zBuf);
    fprintf(stderr, "OUTPUT: %s\n", zBuf);
  }
#endif
  return SQLITE_OK;
}

/*
** This routine checks for a byte-order mark at the beginning of the 
** UTF-16 string stored in *pMem. If one is present, it is removed and
** the encoding of the Mem adjusted. This routine does not do any
** byte-swapping, it just sets Mem.enc appropriately.
**
** The allocation (static, dynamic etc.) and encoding of the Mem may be
** changed by this function.
*/
int sqlite3VdbeMemHandleBom(Mem *pMem){
  int rc = SQLITE_OK;
  u8 bom = 0;

  if( pMem->n<0 || pMem->n>1 ){
    u8 b1 = *(u8 *)pMem->z;
    u8 b2 = *(((u8 *)pMem->z) + 1);
    if( b1==0xFE && b2==0xFF ){
      bom = SQLITE_UTF16BE;
    }
    if( b1==0xFF && b2==0xFE ){
      bom = SQLITE_UTF16LE;
    }
  }
  
  if( bom ){
    /* This function is called as soon as a string is stored in a Mem*,
    ** from within sqlite3VdbeMemSetStr(). At that point it is not possible
    ** for the string to be stored in Mem.zShort, or for it to be stored
    ** in dynamic memory with no destructor.
    */
    assert( !(pMem->flags&MEM_Short) );
    assert( !(pMem->flags&MEM_Dyn) || pMem->xDel );
    if( pMem->flags & MEM_Dyn ){
      void (*xDel)(void*) = pMem->xDel;
      char *z = pMem->z;
      pMem->z = 0;
      pMem->xDel = 0;
      rc = sqlite3VdbeMemSetStr(pMem, &z[2], pMem->n-2, bom, SQLITE_TRANSIENT);
      xDel(z);
    }else{
      rc = sqlite3VdbeMemSetStr(pMem, &pMem->z[2], pMem->n-2, bom, 
          SQLITE_TRANSIENT);
    }
  }
  return rc;
}
#endif /* SQLITE_OMIT_UTF16 */

/*
** pZ is a UTF-8 encoded unicode string. If nByte is less than zero,
** return the number of unicode characters in pZ up to (but not including)
** the first 0x00 byte. If nByte is not less than zero, return the
** number of unicode characters in the first nByte of pZ (or up to 
** the first 0x00, whichever comes first).
*/
int sqlite3Utf8CharLen(const char *zIn, int nByte){
  int r = 0;
  const u8 *z = (const u8*)zIn;
  const u8 *zTerm;
  if( nByte>=0 ){
    zTerm = &z[nByte];
  }else{
    zTerm = (const u8*)(-1);
  }
  assert( z<=zTerm );
  while( *z!=0 && z<zTerm ){
    SQLITE_SKIP_UTF8(z);
    r++;
  }
  return r;
}

#ifndef SQLITE_OMIT_UTF16
/*
** Convert a UTF-16 string in the native encoding into a UTF-8 string.
** Memory to hold the UTF-8 string is obtained from malloc and must be
** freed by the calling function.
**
** NULL is returned if there is an allocation error.
*/
char *sqlite3Utf16to8(const void *z, int nByte){
  Mem m;
  memset(&m, 0, sizeof(m));
  sqlite3VdbeMemSetStr(&m, z, nByte, SQLITE_UTF16NATIVE, SQLITE_STATIC);
  sqlite3VdbeChangeEncoding(&m, SQLITE_UTF8);
  assert( (m.flags & MEM_Term)!=0 || sqlite3MallocFailed() );
  assert( (m.flags & MEM_Str)!=0 || sqlite3MallocFailed() );
  return (m.flags & MEM_Dyn)!=0 ? m.z : sqliteStrDup(m.z);
}

/*
** pZ is a UTF-16 encoded unicode string. If nChar is less than zero,
** return the number of bytes up to (but not including), the first pair
** of consecutive 0x00 bytes in pZ. If nChar is not less than zero,
** then return the number of bytes in the first nChar unicode characters
** in pZ (or up until the first pair of 0x00 bytes, whichever comes first).
*/
int sqlite3Utf16ByteLen(const void *zIn, int nChar){
  unsigned int c = 1;
  char const *z = zIn;
  int n = 0;
  if( SQLITE_UTF16NATIVE==SQLITE_UTF16BE ){
    /* Using an "if (SQLITE_UTF16NATIVE==SQLITE_UTF16BE)" construct here
    ** and in other parts of this file means that at one branch will
    ** not be covered by coverage testing on any single host. But coverage
    ** will be complete if the tests are run on both a little-endian and 
    ** big-endian host. Because both the UTF16NATIVE and SQLITE_UTF16BE
    ** macros are constant at compile time the compiler can determine
    ** which branch will be followed. It is therefore assumed that no runtime
    ** penalty is paid for this "if" statement.
    */
    while( c && ((nChar<0) || n<nChar) ){
      READ_UTF16BE(z, c);
      n++;
    }
  }else{
    while( c && ((nChar<0) || n<nChar) ){
      READ_UTF16LE(z, c);
      n++;
    }
  }
  return (z-(char const *)zIn)-((c==0)?2:0);
}

#if defined(SQLITE_TEST)
/*
** Translate UTF-8 to UTF-8.
**
** This has the effect of making sure that the string is well-formed
** UTF-8.  Miscoded characters are removed.
**
** The translation is done in-place (since it is impossible for the
** correct UTF-8 encoding to be longer than a malformed encoding).
*/
int sqlite3Utf8To8(unsigned char *zIn){
  unsigned char *zOut = zIn;
  unsigned char *zStart = zIn;
  int c;

  while(1){
    SQLITE_READ_UTF8(zIn, c);
    if( c==0 ) break;
    if( c!=0xfffd ){
      WRITE_UTF8(zOut, c);
    }
  }
  *zOut = 0;
  return zOut - zStart;
}
#endif

#if defined(SQLITE_TEST)
/*
** This routine is called from the TCL test function "translate_selftest".
** It checks that the primitives for serializing and deserializing
** characters in each encoding are inverses of each other.
*/
void sqlite3UtfSelfTest(){
  unsigned int i, t;
  unsigned char zBuf[20];
  unsigned char *z;
  int n;
  unsigned int c;

  for(i=0; i<0x00110000; i++){
    z = zBuf;
    WRITE_UTF8(z, i);
    n = z-zBuf;
    z[0] = 0;
    z = zBuf;
    SQLITE_READ_UTF8(z, c);
    t = i;
    if( i>=0xD800 && i<=0xDFFF ) t = 0xFFFD;
    if( (i&0xFFFFFFFE)==0xFFFE ) t = 0xFFFD;
    assert( c==t );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16LE(z, i);
    n = z-zBuf;
    z[0] = 0;
    z = zBuf;
    READ_UTF16LE(z, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16BE(z, i);
    n = z-zBuf;
    z[0] = 0;
    z = zBuf;
    READ_UTF16BE(z, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
}
#endif /* SQLITE_TEST */
#endif /* SQLITE_OMIT_UTF16 */
