/*
** 2007 April 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Code for testing all sorts of SQLite interfaces.  This code
** implements TCL commands for reading and writing the binary
** database files and displaying the content of those files as
** hexadecimal.  We could, in theory, use the built-in "binary"
** command of TCL to do a lot of this, but there are some issues
** with historical versions of the "binary" command.  So it seems
** easier and safer to build our own mechanism.
**
** $Id: test_hexio.c,v 1.2 2007/04/09 20:30:11 drh Exp $
*/
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
hexio_read  filename offset amt
hexio_write filename offset hexdata
hexio_get_int  hexdata
hexio_get_varint  hexdata
hexio_render_int8  integer
hexio_render_int16  integer
hexio_render_int38  integer
hexio_render_varint  integer
*/

/*
** Convert binary to hex.  The input zBuf[] contains N bytes of
** binary data.  zBuf[] is 2*n+1 bytes long.  Overwrite zBuf[]
** with a hexadecimal representation of its original binary input.
*/
static void binToHex(unsigned char *zBuf, int N){
  const unsigned char zHex[] = "0123456789ABCDEF";
  int i, j;
  unsigned char c;
  i = N*2;
  zBuf[i--] = 0;
  for(j=N-1; j>=0; j--){
    c = zBuf[j];
    zBuf[i--] = zHex[c&0xf];
    zBuf[i--] = zHex[c>>4];
  }
  assert( i==-1 );
}

/*
** Convert hex to binary.  The input zIn[] contains N bytes of
** hexadecimal.  Convert this into binary and write aOut[] with
** the binary data.  Spaces in the original input are ignored.
** Return the number of bytes of binary rendered.
*/
static int hexToBin(const unsigned char *zIn, int N, unsigned char *aOut){
  const unsigned char aMap[] = {
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     1, 2, 3, 4, 5, 6, 7, 8,  9,10, 0, 0, 0, 0, 0, 0,
     0,11,12,13,14,15,16, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0,11,12,13,14,15,16, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
     0, 0, 0, 0, 0, 0, 0, 0,  0, 0, 0, 0, 0, 0, 0, 0,
  };
  int i, j;
  int hi=1;
  unsigned char c;

  for(i=j=0; i<N; i++){
    c = aMap[zIn[i]];
    if( c==0 ) continue;
    if( hi ){
      aOut[j] = (c-1)<<4;
      hi = 0;
    }else{
      aOut[j++] |= c-1;
      hi = 1;
    }
  }
  return j;
}


/*
** Usage:   hexio_read  FILENAME  OFFSET  AMT
**
** Read AMT bytes from file FILENAME beginning at OFFSET from the
** beginning of the file.  Convert that information to hexadecimal
** and return the resulting HEX string.
*/
static int hexio_read(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int offset;
  int amt, got;
  const char *zFile;
  unsigned char *zBuf;
  FILE *in;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "FILENAME OFFSET AMT");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &offset) ) return TCL_ERROR;
  if( Tcl_GetIntFromObj(interp, objv[3], &amt) ) return TCL_ERROR;
  zFile = Tcl_GetString(objv[1]);
  zBuf = malloc( amt*2+1 );
  if( zBuf==0 ){
    return TCL_ERROR;
  }
  in = fopen(zFile, "r");
  if( in==0 ){
    Tcl_AppendResult(interp, "cannot open input file ", zFile, 0);
    return TCL_ERROR;
  }
  fseek(in, offset, SEEK_SET);
  got = fread(zBuf, 1, amt, in);
  fclose(in);
  if( got<0 ){
    got = 0;
  }
  binToHex(zBuf, got);
  Tcl_AppendResult(interp, zBuf, 0);
  free(zBuf);
  return TCL_OK;
}


/*
** Usage:   hexio_write  FILENAME  OFFSET  DATA
**
** Write DATA into file FILENAME beginning at OFFSET from the
** beginning of the file.  DATA is expressed in hexadecimal.
*/
static int hexio_write(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int offset;
  int nIn, nOut, written;
  const char *zFile;
  const unsigned char *zIn;
  unsigned char *aOut;
  FILE *out;

  if( objc!=4 ){
    Tcl_WrongNumArgs(interp, 1, objv, "FILENAME OFFSET HEXDATA");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[2], &offset) ) return TCL_ERROR;
  zFile = Tcl_GetString(objv[1]);
  zIn = (const unsigned char *)Tcl_GetStringFromObj(objv[3], &nIn);
  aOut = malloc( nIn/2 );
  if( aOut==0 ){
    return TCL_ERROR;
  }
  nOut = hexToBin(zIn, nIn, aOut);
  out = fopen(zFile, "r+");
  if( out==0 ){
    Tcl_AppendResult(interp, "cannot open output file ", zFile, 0);
    return TCL_ERROR;
  }
  fseek(out, offset, SEEK_SET);
  written = fwrite(aOut, 1, nOut, out);
  free(aOut);
  fclose(out);
  Tcl_SetObjResult(interp, Tcl_NewIntObj(written));
  return TCL_OK;
}

/*
** USAGE:   hexio_get_int   HEXDATA
**
** Interpret the HEXDATA argument as a big-endian integer.  Return
** the value of that integer.  HEXDATA can contain between 2 and 8
** hexadecimal digits.
*/
static int hexio_get_int(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int val;
  int nIn, nOut;
  const unsigned char *zIn;
  unsigned char *aOut;
  unsigned char aNum[4];

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "HEXDATA");
    return TCL_ERROR;
  }
  zIn = (const unsigned char *)Tcl_GetStringFromObj(objv[1], &nIn);
  aOut = malloc( nIn/2 );
  if( aOut==0 ){
    return TCL_ERROR;
  }
  nOut = hexToBin(zIn, nIn, aOut);
  if( nOut>=4 ){
    memcpy(aNum, aOut, 4);
  }else{
    memset(aNum, 0, sizeof(aNum));
    memcpy(&aNum[4-nOut], aOut, nOut);
  }
  free(aOut);
  val = (aNum[0]<<24) | (aNum[1]<<16) | (aNum[2]<<8) | aNum[3];
  Tcl_SetObjResult(interp, Tcl_NewIntObj(val));
  return TCL_OK;
}


/*
** USAGE:   hexio_render_int16   INTEGER
**
** Render INTEGER has a 16-bit big-endian integer in hexadecimal.
*/
static int hexio_render_int16(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int val;
  unsigned char aNum[10];

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "INTEGER");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &val) ) return TCL_ERROR;
  aNum[0] = val>>8;
  aNum[1] = val;
  binToHex(aNum, 2);
  Tcl_SetObjResult(interp, Tcl_NewStringObj((char*)aNum, 4));
  return TCL_OK;
}


/*
** USAGE:   hexio_render_int32   INTEGER
**
** Render INTEGER has a 32-bit big-endian integer in hexadecimal.
*/
static int hexio_render_int32(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int val;
  unsigned char aNum[10];

  if( objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "INTEGER");
    return TCL_ERROR;
  }
  if( Tcl_GetIntFromObj(interp, objv[1], &val) ) return TCL_ERROR;
  aNum[0] = val>>24;
  aNum[1] = val>>16;
  aNum[2] = val>>8;
  aNum[3] = val;
  binToHex(aNum, 4);
  Tcl_SetObjResult(interp, Tcl_NewStringObj((char*)aNum, 8));
  return TCL_OK;
}



/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_hexio_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aObjCmd[] = {
     { "hexio_read",                   hexio_read            },
     { "hexio_write",                  hexio_write           },
     { "hexio_get_int",                hexio_get_int         },
     { "hexio_render_int16",           hexio_render_int16    },
     { "hexio_render_int32",           hexio_render_int32    },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, 0, 0);
  }
  return TCL_OK;
}
