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
** $Id: utf.c,v 1.1 2004/05/04 15:00:47 drh Exp $
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
**      Word-0            Word-1             Value
**  110110wwwwxxxxxx 110111yyyyyyyyyy        000uuuuu xxxxxxyy yyyyyyyy
**  xxxxxxxxyyyyyyyy                         00000000 xxxxxxxx yyyyyyyy
**
** BOM or Byte Order Mark:
**     0xff 0xfe   little-endian utf-16 follows
**     0xfe 0xff   big-endian utf-16 follows
*/

/*
** Convert a string in UTF-16 native byte (or with a Byte-order-mark or
** "BOM") into a UTF-8 string.  The UTF-8 string is written into space 
** obtained from sqlit3Malloc() and must be released by the calling function.
**
** The parameter N is the number of bytes in the UTF-16 string.  If N is
** negative, the entire string up to the first \u0000 character is translated.
**
** The returned UTF-8 string is always \000 terminated.
*/
unsigned char *sqlite3utf16to8(const void *pData, int N){
  unsigned char *in = (unsigned char *)pData;
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
*/
void sqlite3utf16to16le(void *pData, int N){
}
void sqlite3utf16to16be(void *pData, int N){
}

/*
** Translation from UTF-16LE to UTF-16BE and back again is accomplished
** using the library function swab().
*/

/*
** Translate UTF-8 to UTF-16BE or UTF-16LE
*/
void *sqlite3utf8to16be(const unsigned char *pIn, int N){
}
void *sqlite3utf8to16le(const unsigned char *pIn, int N){
}
