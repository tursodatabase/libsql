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
** $Id: utf.c,v 1.25 2004/06/23 13:46:32 danielk1977 Exp $
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
**
** Handling of malformed strings:
**
** SQLite accepts and processes malformed strings without an error wherever
** possible. However this is not possible when converting between UTF-8 and
** UTF-16.
**
** When converting malformed UTF-8 strings to UTF-16, one instance of the
** replacement character U+FFFD for each byte that cannot be interpeted as
** part of a valid unicode character.
**
** When converting malformed UTF-16 strings to UTF-8, one instance of the
** replacement character U+FFFD for each pair of bytes that cannot be
** interpeted as part of a valid unicode character.
**
** This file contains the following public routines:
**
** sqlite3VdbeMemTranslate() - Translate the encoding used by a Mem* string.
** sqlite3VdbeMemHandleBom() - Handle byte-order-marks in UTF16 Mem* strings.
** sqlite3utf16ByteLen()     - Calculate byte-length of a void* UTF16 string.
** sqlite3utf8CharLen()      - Calculate char-length of a char* UTF8 string.
** sqlite3utf8LikeCompare()  - Do a LIKE match given two UTF8 char* strings.
**
*/
#include <assert.h>
#include "sqliteInt.h"
#include "vdbeInt.h"

/*
** The following macro, LOWERCASE(x), takes an integer representing a
** unicode code point. The value returned is the same code point folded to
** lower case, if applicable. SQLite currently understands the upper/lower
** case relationship between the 26 characters used in the English
** language only.
**
** This means that characters with umlauts etc. will not be folded
** correctly (unless they are encoded as composite characters, which would
** doubtless cause much trouble).
*/
#define LOWERCASE(x) (x<91?(int)(UpperToLower[x]):x)
static unsigned char UpperToLower[91] = {
      0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17,
     18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
     36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
     54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 97, 98, 99,100,101,102,103,
    104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,
    122,
};

/*
** This table maps from the first byte of a UTF-8 character to the number
** of trailing bytes expected. A value '255' indicates that the table key
** is not a legal first byte for a UTF-8 character.
*/
static const u8 xtra_utf8_bytes[256]  = {
/* 0xxxxxxx */
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,
0, 0, 0, 0, 0, 0, 0, 0,     0, 0, 0, 0, 0, 0, 0, 0,

/* 10wwwwww */
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,

/* 110yyyyy */
1, 1, 1, 1, 1, 1, 1, 1,     1, 1, 1, 1, 1, 1, 1, 1,
1, 1, 1, 1, 1, 1, 1, 1,     1, 1, 1, 1, 1, 1, 1, 1,

/* 1110zzzz */
2, 2, 2, 2, 2, 2, 2, 2,     2, 2, 2, 2, 2, 2, 2, 2,

/* 11110yyy */
3, 3, 3, 3, 3, 3, 3, 3,     255, 255, 255, 255, 255, 255, 255, 255,
};

/*
** This table maps from the number of trailing bytes in a UTF-8 character
** to an integer constant that is effectively calculated for each character
** read by a naive implementation of a UTF-8 character reader. The code
** in the READ_UTF8 macro explains things best.
*/
static const int xtra_utf8_bits[4] =  {
0,
12416,          /* (0xC0 << 6) + (0x80) */
925824,         /* (0xE0 << 12) + (0x80 << 6) + (0x80) */
63447168        /* (0xF0 << 18) + (0x80 << 12) + (0x80 << 6) + 0x80 */
};

#define READ_UTF8(zIn, c) { \
  int xtra;                                            \
  c = *(zIn)++;                                        \
  xtra = xtra_utf8_bytes[c];                           \
  switch( xtra ){                                      \
    case 255: c = (int)0xFFFD; break;                  \
    case 3: c = (c<<6) + *(zIn)++;                     \
    case 2: c = (c<<6) + *(zIn)++;                     \
    case 1: c = (c<<6) + *(zIn)++;                     \
    c -= xtra_utf8_bits[xtra];                         \
  }                                                    \
}

#define SKIP_UTF8(zIn) {                               \
  zIn += (xtra_utf8_bytes[*(u8 *)zIn] + 1);            \
}

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
  if( c>=0xD800 && c<=0xE000 ){                                       \
    int c2 = (*zIn++);                                                \
    c2 += ((*zIn++)<<8);                                              \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

#define READ_UTF16BE(zIn, c){                                         \
  c = ((*zIn++)<<8);                                                  \
  c += (*zIn++);                                                      \
  if( c>=0xD800 && c<=0xE000 ){                                       \
    int c2 = ((*zIn++)<<8);                                           \
    c2 += (*zIn++);                                                   \
    c = (c2&0x03FF) + ((c&0x003F)<<10) + (((c&0x03C0)+0x0040)<<10);   \
  }                                                                   \
}

/*
** If the TRANSLATE_TRACE macro is defined, the value of each Mem is
** printed on stderr on the way into and out of sqlite3VdbeMemTranslate().
*/ 
/* #define TRANSLATE_TRACE 1 */

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
  int c;

  assert( pMem->flags&MEM_Str );
  assert( pMem->enc!=desiredEnc );
  assert( pMem->enc!=0 );
  assert( pMem->n>=0 );

#ifdef TRANSLATE_TRACE
  {
    char zBuf[100];
    sqlite3VdbeMemPrettyPrint(pMem, zBuf, 100);
    fprintf(stderr, "INPUT:  %s\n", zBuf);
  }
#endif

  /* If the translation is between UTF-16 little and big endian, then 
  ** all that is required is to swap the byte order. This case is handled
  ** differently from the others.
  */
  if( pMem->enc!=SQLITE_UTF8 && desiredEnc!=SQLITE_UTF8 ){
    u8 temp;
    sqlite3VdbeMemMakeWriteable(pMem);
    zIn = pMem->z;
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
    ** translating a 2-byte character to a 3-byte UTF-8 character (i.e.
    ** code-point 0xFFFC). A single byte is required for the output string
    ** nul-terminator.
    */
    len = (pMem->n/2) * 3 + 1;
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
  zIn = pMem->z;
  zTerm = &zIn[pMem->n];
  if( len>NBFS ){
    zOut = sqliteMallocRaw(len);
    if( !zOut ) return SQLITE_NOMEM;
  }else{
    zOut = zShort;
  }
  z = zOut;

  if( pMem->enc==SQLITE_UTF8 ){
    if( desiredEnc==SQLITE_UTF16LE ){
      /* UTF-8 -> UTF-16 Little-endian */
      while( zIn<zTerm ){
        READ_UTF8(zIn, c); 
        WRITE_UTF16LE(z, c);
      }
      WRITE_UTF16LE(z, 0);
      pMem->n = (z-zOut)-2;
    }else if( desiredEnc==SQLITE_UTF16BE ){
      /* UTF-8 -> UTF-16 Big-endian */
      while( zIn<zTerm ){
        READ_UTF8(zIn, c); 
        WRITE_UTF16BE(z, c);
      }
      WRITE_UTF16BE(z, 0);
      pMem->n = (z-zOut)-2;
    }
  }else{
    assert( desiredEnc==SQLITE_UTF8 );
    if( pMem->enc==SQLITE_UTF16LE ){
      /* UTF-16 Little-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16LE(zIn, c); 
        WRITE_UTF8(z, c);
      }
      WRITE_UTF8(z, 0);
      pMem->n = (z-zOut)-1;
    }else{
      /* UTF-16 Little-endian -> UTF-8 */
      while( zIn<zTerm ){
        READ_UTF16BE(zIn, c); 
        WRITE_UTF8(z, c);
      }
      WRITE_UTF8(z, 0);
      pMem->n = (z-zOut)-1;
    }
  }
  assert( (pMem->n+(desiredEnc==SQLITE_UTF8?1:2))<=len );

  sqlite3VdbeMemRelease(pMem);
  pMem->flags &= ~(MEM_Static|MEM_Dyn|MEM_Ephem|MEM_Short);
  pMem->enc = desiredEnc;
  if( zOut==zShort ){
    memcpy(pMem->zShort, zOut, len);
    zOut = pMem->zShort;
    pMem->flags |= (MEM_Term|MEM_Short);
  }else{
    pMem->flags |= (MEM_Term|MEM_Dyn);
  }
  pMem->z = zOut;

translate_out:
#ifdef TRANSLATE_TRACE
  {
    char zBuf[100];
    sqlite3VdbeMemPrettyPrint(pMem, zBuf, 100);
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

/*
** pZ is a UTF-8 encoded unicode string. If nByte is less than zero,
** return the number of unicode characters in pZ up to (but not including)
** the first 0x00 byte. If nByte is not less than zero, return the
** number of unicode characters in the first nByte of pZ (or up to 
** the first 0x00, whichever comes first).
*/
int sqlite3utf8CharLen(const char *z, int nByte){
  int r = 0;
  const char *zTerm;
  if( nByte>=0 ){
    zTerm = &z[nByte];
  }else{
    zTerm = (const char *)(-1);
  }
  assert( z<=zTerm );
  while( *z!=0 && z<zTerm ){
    SKIP_UTF8(z);
    r++;
  }
  return r;
}

/*
** pZ is a UTF-16 encoded unicode string. If nChar is less than zero,
** return the number of bytes up to (but not including), the first pair
** of consecutive 0x00 bytes in pZ. If nChar is not less than zero,
** then return the number of bytes in the first nChar unicode characters
** in pZ (or up until the first pair of 0x00 bytes, whichever comes first).
*/
int sqlite3utf16ByteLen(const void *zIn, int nChar){
  int c = 1;
  char const *z = zIn;
  int n = 0;
  if( SQLITE_UTF16NATIVE==SQLITE_UTF16BE ){
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

/*
** Compare two UTF-8 strings for equality using the "LIKE" operator of
** SQL.  The '%' character matches any sequence of 0 or more
** characters and '_' matches any single character.  Case is
** not significant.
*/
int sqlite3utf8LikeCompare(
  const unsigned char *zPattern, 
  const unsigned char *zString
){
  register int c;
  int c2;

  while( (c = LOWERCASE(*zPattern))!=0 ){
    switch( c ){
      case '%': {
        while( (c=zPattern[1]) == '%' || c == '_' ){
          if( c=='_' ){
            if( *zString==0 ) return 0;
            SKIP_UTF8(zString);
          }
          zPattern++;
        }
        if( c==0 ) return 1;
        c = LOWERCASE(c);
        while( (c2=LOWERCASE(*zString))!=0 ){
          while( c2 != 0 && c2 != c ){ 
            zString++;
            c2 = LOWERCASE(*zString); 
          }
          if( c2==0 ) return 0;
          if( sqlite3utf8LikeCompare(&zPattern[1],zString) ) return 1;
          SKIP_UTF8(zString);
        }
        return 0;
      }
      case '_': {
        if( *zString==0 ) return 0;
        SKIP_UTF8(zString);
        zPattern++;
        break;
      }
      default: {
        if( c != LOWERCASE(*zString) ) return 0;
        zPattern++;
        zString++;
        break;
      }
    }
  }
  return *zString==0;
}

#if defined(SQLITE_TEST)
/*
** This routine is called from the TCL test function "translate_selftest".
** It checks that the primitives for serializing and deserializing
** characters in each encoding are inverses of each other.
*/
void sqlite3utfSelfTest(){
  int i;
  unsigned char zBuf[20];
  unsigned char *z;
  int n;
  int c;

  for(i=0; i<0x00110000; i++){
    z = zBuf;
    WRITE_UTF8(z, i);
    n = z-zBuf;
    z = zBuf;
    READ_UTF8(z, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<=0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16LE(z, i);
    n = z-zBuf;
    z = zBuf;
    READ_UTF16LE(z, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
  for(i=0; i<0x00110000; i++){
    if( i>=0xD800 && i<=0xE000 ) continue;
    z = zBuf;
    WRITE_UTF16BE(z, i);
    n = z-zBuf;
    z = zBuf;
    READ_UTF16BE(z, c);
    assert( c==i );
    assert( (z-zBuf)==n );
  }
}
#endif
