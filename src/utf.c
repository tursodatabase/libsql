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
** $Id: utf.c,v 1.15 2004/05/31 18:51:58 drh Exp $
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
*/
#include <assert.h>
#include "sqliteInt.h"

typedef struct UtfString UtfString;
struct UtfString {
  unsigned char *pZ;    /* Raw string data */
  int n;                /* Allocated length of pZ in bytes */
  int c;                /* Number of pZ bytes already read or written */
};

/* TODO: Implement this macro in os.h. It should be 1 on big-endian
** machines, and 0 on little-endian.
*/
#define SQLITE_NATIVE_BIGENDIAN 0

#if SQLITE_NATIVE_BIGENDIAN == 1
#define BOM_BIGENDIAN 0x0000FFFE
#define BOM_LITTLEENDIAN 0x0000FEFF
#else
#define BOM_BIGENDIAN 0x0000FEFF
#define BOM_LITTLEENDIAN 0x0000FFFE
#endif

/*
** These two macros are used to interpret the first two bytes of the 
** unsigned char array pZ as a 16-bit unsigned int. BE16() for a big-endian
** interpretation, LE16() for little-endian.
*/
#define BE16(pZ) (((u16)((pZ)[0])<<8) + (u16)((pZ)[1]))
#define LE16(pZ) (((u16)((pZ)[1])<<8) + (u16)((pZ)[0]))

/*
** READ_16 interprets the first two bytes of the unsigned char array pZ 
** as a 16-bit unsigned int. If big_endian is non-zero the intepretation
** is big-endian, otherwise little-endian.
*/
#define READ_16(pZ,big_endian) (big_endian?BE16(pZ):LE16(pZ))

/*
** Read the BOM from the start of *pStr, if one is present. Return zero
** for little-endian, non-zero for big-endian. If no BOM is present, return
** the value of the parameter "big_endian".
**
** Return values:
**     1 -> big-endian string
**     0 -> little-endian string
*/
static int readUtf16Bom(UtfString *pStr, int big_endian){
  /* The BOM must be the first thing read from the string */
  assert( pStr->c==0 );

  /* If the string data consists of 1 byte or less, the BOM will make no
  ** difference anyway. In this case just fall through to the default case
  ** and return the native byte-order for this machine.
  **
  ** Otherwise, check the first 2 bytes of the string to see if a BOM is
  ** present.
  */
  if( pStr->n>1 ){
    u32 bom = BE16(pStr->pZ);
    if( bom==BOM_BIGENDIAN ){
      pStr->c = 2;
      return 1;
    }
    if( bom==BOM_LITTLEENDIAN ){
      pStr->c = 2;
      return 0;
    }
  }

  return big_endian;
}

/*
** zData is a UTF-16 encoded string, nData bytes in length. This routine
** checks if there is a byte-order mark at the start of zData. If no
** byte order mark is found 0 is returned. Otherwise TEXT_Utf16be or
** TEXT_Utf16le is returned, depending on whether The BOM indicates that
** the text is big-endian or little-endian.
*/
u8 sqlite3UtfReadBom(const void *zData, int nData){
  if( nData<0 || nData>1 ){
    u8 b1 = *(u8 *)zData;
    u8 b2 = *(((u8 *)zData) + 1);
    if( b1==0xFE && b2==0xFF ){
      return TEXT_Utf16be;
    }
    if( b1==0xFF && b2==0xFE ){
      return TEXT_Utf16le;
    }
  }
  return 0;
}


/*
** Read a single unicode character from the UTF-8 encoded string *pStr. The
** value returned is a unicode scalar value. In the case of malformed
** strings, the unicode replacement character U+FFFD may be returned.
*/
static u32 readUtf8(UtfString *pStr){
  struct Utf8TblRow {
    u8 b1_mask;
    u8 b1_masked_val;
    u8 b1_value_mask;
    int trailing_bytes;
  };
  static const struct Utf8TblRow utf8tbl[] = {
    { 0x80, 0x00, 0x7F, 0 },
    { 0xE0, 0xC0, 0x1F, 1 },
    { 0xF0, 0xE0, 0x0F, 2 },
    { 0xF8, 0xF0, 0x0E, 3 },
    { 0, 0, 0, 0}
  };

  u8 b1;       /* First byte of the potentially multi-byte utf-8 character */
  u32 ret = 0; /* Return value */
  int ii;
  struct Utf8TblRow const *pRow;

  pRow = &(utf8tbl[0]);

  b1 = pStr->pZ[pStr->c];
  pStr->c++;
  while( pRow->b1_mask && (b1&pRow->b1_mask)!=pRow->b1_masked_val ){
    pRow++;
  }
  if( !pRow->b1_mask ){
    return 0xFFFD;
  }
  
  ret = (u32)(b1&pRow->b1_value_mask);
  for( ii=0; ii<pRow->trailing_bytes; ii++ ){
    u8 b = pStr->pZ[pStr->c+ii];
    if( (b&0xC0)!=0x80 ){
      return 0xFFFD;
    }
    ret = (ret<<6) + (u32)(b&0x3F);
  }
  
  pStr->c += pRow->trailing_bytes;
  return ret;
}

/*
** Write the unicode character 'code' to the string pStr using UTF-8
** encoding. SQLITE_NOMEM may be returned if sqlite3Malloc() fails.
*/
static int writeUtf8(UtfString *pStr, u32 code){
  struct Utf8WriteTblRow {
    u32 max_code;
    int trailing_bytes;
    u8 b1_and_mask;
    u8 b1_or_mask;
  };
  static const struct Utf8WriteTblRow utf8tbl[] = {
    {0x0000007F, 0, 0x7F, 0x00},
    {0x000007FF, 1, 0xDF, 0xC0},
    {0x0000FFFF, 2, 0xEF, 0xE0},
    {0x0010FFFF, 3, 0xF7, 0xF0},
    {0x00000000, 0, 0x00, 0x00}
  };
  const struct Utf8WriteTblRow *pRow = &utf8tbl[0];

  while( code>pRow->max_code ){
    assert( pRow->max_code );
    pRow++;
  }

  /* Ensure there is enough room left in the output buffer to write
  ** this UTF-8 character. 
  */
  assert( (pStr->n-pStr->c)>=(pRow->trailing_bytes+1) );

  /* Write the UTF-8 encoded character to pStr. All cases below are
  ** intentionally fall-through.
  */
  switch( pRow->trailing_bytes ){
    case 3:
      pStr->pZ[pStr->c+3] = (((u8)code)&0x3F)|0x80;
      code = code>>6;
    case 2:
      pStr->pZ[pStr->c+2] = (((u8)code)&0x3F)|0x80;
      code = code>>6;
    case 1:
      pStr->pZ[pStr->c+1] = (((u8)code)&0x3F)|0x80;
      code = code>>6;
    case 0:
      pStr->pZ[pStr->c] = (((u8)code)&(pRow->b1_and_mask))|(pRow->b1_or_mask);
  }
  pStr->c += (pRow->trailing_bytes + 1);

  return 0;
}

/*
** Read a single unicode character from the UTF-16 encoded string *pStr. The
** value returned is a unicode scalar value. In the case of malformed
** strings, the unicode replacement character U+FFFD may be returned.
**
** If big_endian is true, the string is assumed to be UTF-16BE encoded.
** Otherwise, it is UTF-16LE encoded.
*/
static u32 readUtf16(UtfString *pStr, int big_endian){
  u32 code_point;   /* the first code-point in the character */

  /* If there is only one byte of data left in the string, return the 
  ** replacement character.
  */
  if( (pStr->n-pStr->c)==1 ){
    pStr->c++;
    return (int)0xFFFD;
  }

  code_point = READ_16(&(pStr->pZ[pStr->c]), big_endian);
  pStr->c += 2;

  /* If this is a non-surrogate code-point, just cast it to an int and
  ** return the code-point value.
  */
  if( code_point<0xD800 || code_point>0xE000 ){
    return code_point;
  }

  /* If this is a trailing surrogate code-point, then the string is
  ** malformed; return the replacement character.
  */
  if( code_point>0xDBFF ){
    return 0xFFFD;
  }

  /* The code-point just read is a leading surrogate code-point. If their
  ** is not enough data left or the next code-point is not a trailing
  ** surrogate, return the replacement character.
  */
  if( (pStr->n-pStr->c)>1 ){
    u32 code_point2 = READ_16(&pStr->pZ[pStr->c], big_endian);
    if( code_point2<0xDC00 || code_point>0xDFFF ){
      return 0xFFFD;
    }
    pStr->c += 2;

    return ( 
        (((code_point&0x03C0)+0x0040)<<16) +   /* uuuuu */
        ((code_point&0x003F)<<10) +            /* xxxxxx */
        (code_point2&0x03FF)                   /* yy yyyyyyyy */
    );

  }else{
    return (int)0xFFFD;
  }
  
  /* not reached */
}

static int writeUtf16(UtfString *pStr, int code, int big_endian){
  int bytes;
  unsigned char *hi_byte;
  unsigned char *lo_byte;

  bytes = (code>0x0000FFFF?4:2);

  /* Ensure there is enough room left in the output buffer to write
  ** this UTF-8 character.
  */
  assert( (pStr->n-pStr->c)>=bytes );
  
  /* Initialise hi_byte and lo_byte to point at the locations into which
  ** the MSB and LSB of the (first) 16-bit unicode code-point written for
  ** this character.
  */
  hi_byte = (big_endian?&pStr->pZ[pStr->c]:&pStr->pZ[pStr->c+1]);
  lo_byte = (big_endian?&pStr->pZ[pStr->c+1]:&pStr->pZ[pStr->c]);

  if( bytes==2 ){
    *hi_byte = (u8)((code&0x0000FF00)>>8);
    *lo_byte = (u8)(code&0x000000FF);
  }else{
    u32 wrd;
    wrd = ((((code&0x001F0000)-0x00010000)+(code&0x0000FC00))>>10)|0x0000D800;
    *hi_byte = (u8)((wrd&0x0000FF00)>>8);
    *lo_byte = (u8)(wrd&0x000000FF);

    wrd = (code&0x000003FF)|0x0000DC00;
    *(hi_byte+2) = (u8)((wrd&0x0000FF00)>>8);
    *(lo_byte+2) = (u8)(wrd&0x000000FF);
  }

  pStr->c += bytes;
  
  return 0;
}

/*
** pZ is a UTF-8 encoded unicode string. If nByte is less than zero,
** return the number of unicode characters in pZ up to (but not including)
** the first 0x00 byte. If nByte is not less than zero, return the
** number of unicode characters in the first nByte of pZ (or up to 
** the first 0x00, whichever comes first).
*/
int sqlite3utf8CharLen(const char *pZ, int nByte){
  UtfString str;
  int ret = 0;
  u32 code = 1;

  str.pZ = (char *)pZ;
  str.n = nByte;
  str.c = 0;

  while( (nByte<0 || str.c<str.n) && code!=0 ){
    code = readUtf8(&str);
    ret++;
  }
  if( code==0 ) ret--;

  return ret;
}

/*
** pZ is a UTF-16 encoded unicode string. If nChar is less than zero,
** return the number of bytes up to (but not including), the first pair
** of consecutive 0x00 bytes in pZ. If nChar is not less than zero,
** then return the number of bytes in the first nChar unicode characters
** in pZ (or up until the first pair of 0x00 bytes, whichever comes first).
*/
int sqlite3utf16ByteLen(const void *pZ, int nChar){
  if( nChar<0 ){
    const unsigned char *pC1 = (unsigned char *)pZ;
    const unsigned char *pC2 = (unsigned char *)pZ+1;
    while( *pC1 || *pC2 ){
      pC1 += 2;
      pC2 += 2;
    }
    return pC1-(unsigned char *)pZ;
  }else{
    UtfString str;
    u32 code = 1;
    int big_endian;
    int nRead = 0;
    int ret;

    str.pZ = (char *)pZ;
    str.c = 0;
    str.n = -1;

    /* Check for a BOM. We just ignore it if there is one, it's only read
    ** so that it is not counted as a character. 
    */
    big_endian = readUtf16Bom(&str, 0);
    ret = 0-str.c;

    while( code!=0 && nRead<nChar ){
      code = readUtf16(&str, big_endian);
      nRead++;
    }
    if( code==0 ){
      ret -= 2;
    }
    return str.c + ret;
  }
}

/*
** Convert a string in UTF-16 native byte (or with a Byte-order-mark or
** "BOM") into a UTF-8 string.  The UTF-8 string is written into space 
** obtained from sqlite3Malloc() and must be released by the calling function.
**
** The parameter N is the number of bytes in the UTF-16 string.  If N is
** negative, the entire string up to the first \u0000 character is translated.
**
** The returned UTF-8 string is always \000 terminated.
*/
unsigned char *sqlite3utf16to8(const void *pData, int N, int big_endian){
  UtfString in;
  UtfString out;

  out.pZ = 0;

  in.pZ = (unsigned char *)pData;
  in.n = N;
  in.c = 0;

  if( in.n<0 ){
    in.n = sqlite3utf16ByteLen(in.pZ, -1);
  }

  /* A UTF-8 encoding of a unicode string can require at most 1.5 times as
  ** much space to store as the same string encoded using UTF-16. Allocate
  ** this now.
  */
  out.n = (in.n*1.5) + 1;
  out.pZ = sqliteMalloc(out.n);
  if( !out.pZ ){
    return 0;
  }
  out.c = 0;

  big_endian = readUtf16Bom(&in, big_endian);
  while( in.c<in.n ){
    writeUtf8(&out, readUtf16(&in, big_endian));
  }

  /* Add the NULL-terminator character */
  assert( out.c<out.n );
  out.pZ[out.c] = 0x00;

  return out.pZ;
}

static void *utf8toUtf16(const unsigned char *pIn, int N, int big_endian){
  UtfString in;
  UtfString out;

  in.pZ = (unsigned char *)pIn;
  in.n = N;
  in.c = 0;

  if( in.n<0 ){
    in.n = strlen(in.pZ);
  }

  /* A UTF-16 encoding of a unicode string can require at most twice as
  ** much space to store as the same string encoded using UTF-8. Allocate
  ** this now.
  */
  out.n = (in.n*2) + 2;
  out.pZ = sqliteMalloc(out.n);
  if( !out.pZ ){
    return 0;
  }
  out.c = 0;

  while( in.c<in.n ){
    writeUtf16(&out, readUtf8(&in), big_endian);
  }

  /* Add the NULL-terminator character */
  assert( (out.c+1)<out.n );
  out.pZ[out.c] = 0x00;
  out.pZ[out.c+1] = 0x00;

  return out.pZ;
}

/*
** Translate UTF-8 to UTF-16BE or UTF-16LE
*/
void *sqlite3utf8to16be(const unsigned char *pIn, int N){
  return utf8toUtf16(pIn, N, 1);
}

void *sqlite3utf8to16le(const unsigned char *pIn, int N){
  return utf8toUtf16(pIn, N, 0);
}

/* 
** This routine does the work for sqlite3utf16to16le() and
** sqlite3utf16to16be(). If big_endian is 1 the input string is
** transformed in place to UTF-16BE encoding. If big_endian is 0 then
** the input is transformed to UTF-16LE.
**
** Unless the first two bytes of the input string is a BOM, the input is
** assumed to be UTF-16 encoded using the machines native byte ordering.
*/
static void utf16to16(void *pData, int N, int big_endian){
  UtfString inout;
  inout.pZ = (unsigned char *)pData;
  inout.c = 0;
  inout.n = N;

  if( inout.n<0 ){
    inout.n = sqlite3utf16ByteLen(inout.pZ, -1);
  }

  if( readUtf16Bom(&inout, SQLITE_BIGENDIAN)!=big_endian ){
    /* swab(&inout.pZ[inout.c], inout.pZ, inout.n-inout.c); */
    int i;
    for(i=0; i<(inout.n-inout.c); i += 2){
      char c1 = inout.pZ[i+inout.c];
      char c2 = inout.pZ[i+inout.c+1];
      inout.pZ[i] = c2;
      inout.pZ[i+1] = c1;
    }
  }else if( inout.c ){
    memmove(inout.pZ, &inout.pZ[inout.c], inout.n-inout.c);
  }

  inout.pZ[inout.n-inout.c] = 0x00;
  inout.pZ[inout.n-inout.c+1] = 0x00;
}

/*
** Convert a string in UTF-16 native byte or with a BOM into a UTF-16LE
** string.  The conversion occurs in-place.  The output overwrites the
** input.  N bytes are converted.  If N is negative everything is converted
** up to the first \u0000 character.
**
** If the native byte order is little-endian and there is no BOM, then
** this routine is a no-op.  If there is a BOM at the start of the string,
** it is removed.
**
** Translation from UTF-16LE to UTF-16BE and back again is accomplished
** using the library function swab().
*/
void sqlite3utf16to16le(void *pData, int N){
  utf16to16(pData, N, 0);
}

/*
** Convert a string in UTF-16 native byte or with a BOM into a UTF-16BE
** string.  The conversion occurs in-place.  The output overwrites the
** input.  N bytes are converted.  If N is negative everything is converted
** up to the first \u0000 character.
**
** If the native byte order is little-endian and there is no BOM, then
** this routine is a no-op.  If there is a BOM at the start of the string,
** it is removed.
**
** Translation from UTF-16LE to UTF-16BE and back again is accomplished
** using the library function swab().
*/
void sqlite3utf16to16be(void *pData, int N){
  utf16to16(pData, N, 1);
}

/*
** This function is used to translate between UTF-8 and UTF-16. The
** result is returned in dynamically allocated memory.
*/
int sqlite3utfTranslate(
  const void *zData, int nData,  /* Input string */
  u8 enc1,                       /* Encoding of zData */
  void **zOut, int *nOut,        /* Output string */
  u8 enc2                        /* Desired encoding of output */
){
  assert( enc1==TEXT_Utf8 || enc1==TEXT_Utf16le || enc1==TEXT_Utf16be );
  assert( enc2==TEXT_Utf8 || enc2==TEXT_Utf16le || enc2==TEXT_Utf16be );
  assert( 
    (enc1==TEXT_Utf8 && (enc2==TEXT_Utf16le || enc2==TEXT_Utf16be)) ||
    (enc2==TEXT_Utf8 && (enc1==TEXT_Utf16le || enc1==TEXT_Utf16be))
  );

  if( enc1==TEXT_Utf8 ){
    if( enc2==TEXT_Utf16le ){
      *zOut = sqlite3utf8to16le(zData, nData);
    }else{
      *zOut = sqlite3utf8to16be(zData, nData);
    }
    if( !(*zOut) ) return SQLITE_NOMEM;
    *nOut = sqlite3utf16ByteLen(*zOut, -1);
  }else{
    *zOut = sqlite3utf16to8(zData, nData, enc1==TEXT_Utf16be);
    if( !(*zOut) ) return SQLITE_NOMEM;
    *nOut = strlen(*zOut);
  }
  return SQLITE_OK;
}
