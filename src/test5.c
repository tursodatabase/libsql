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
** Code for testing the utf.c module in SQLite.  This code
** is not included in the SQLite library.  It is used for automated
** testing of the SQLite library. Specifically, the code in this file
** is used for testing the SQLite routines for converting between
** the various supported unicode encodings.
**
** $Id: test5.c,v 1.5 2004/05/22 03:05:34 danielk1977 Exp $
*/
#include "sqliteInt.h"
#include "os.h"         /* to get SQLITE3_BIGENDIAN */
#include "tcl.h"
#include <stdlib.h>
#include <string.h>

/*
** Return the number of bytes up to and including the first pair of
** 0x00 bytes in *pStr.
*/
static int utf16_length(const unsigned char *pZ){
  const unsigned char *pC1 = pZ;
  const unsigned char *pC2 = pZ+1;
  while( *pC1 || *pC2 ){
    pC1 += 2;
    pC2 += 2;
  }
  return (pC1-pZ)+2;
}

static int sqlite_utf8to16le(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  unsigned char *out;
  unsigned char *in;
  Tcl_Obj *res;

  if( objc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        Tcl_GetStringFromObj(objv[0], 0), "<utf-8 encoded-string>", 0);
    return TCL_ERROR;
  }

  in = Tcl_GetString(objv[1]);
  out = (unsigned char *)sqlite3utf8to16le(in, -1);
  res = Tcl_NewByteArrayObj(out, utf16_length(out));
  sqliteFree(out); 

  Tcl_SetObjResult(interp, res);

  return TCL_OK;
}

static int sqlite_utf8to16be(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  unsigned char *out;
  unsigned char *in;
  Tcl_Obj *res;

  if( objc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        Tcl_GetStringFromObj(objv[0], 0), "<utf-8 encoded-string>", 0);
    return TCL_ERROR;
  }

  in = Tcl_GetByteArrayFromObj(objv[1], 0);
  in = Tcl_GetString(objv[1]);
  out = (unsigned char *)sqlite3utf8to16be(in, -1);
  res = Tcl_NewByteArrayObj(out, utf16_length(out));
  sqliteFree(out);

  Tcl_SetObjResult(interp, res);

  return TCL_OK;
}

static int sqlite_utf16to16le(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  unsigned char *out;
  unsigned char *in;
  int in_len;
  Tcl_Obj *res;

  if( objc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        Tcl_GetStringFromObj(objv[0], 0), "<utf-16 encoded-string>", 0);
    return TCL_ERROR;
  }

  in = Tcl_GetByteArrayFromObj(objv[1], &in_len);
  out = (unsigned char *)sqliteMalloc(in_len);
  memcpy(out, in, in_len);
  
  sqlite3utf16to16le(out, -1);
  res = Tcl_NewByteArrayObj(out, utf16_length(out));
  sqliteFree(out);

  Tcl_SetObjResult(interp, res);

  return TCL_OK;
}

static int sqlite_utf16to16be(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  unsigned char *out;
  unsigned char *in;
  int in_len;
  Tcl_Obj *res;

  if( objc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        Tcl_GetStringFromObj(objv[0], 0), "<utf-16 encoded-string>", 0);
    return TCL_ERROR;
  }

  in = Tcl_GetByteArrayFromObj(objv[1], &in_len);
  out = (unsigned char *)sqliteMalloc(in_len);
  memcpy(out, in, in_len);
  
  sqlite3utf16to16be(out, -1);
  res = Tcl_NewByteArrayObj(out, utf16_length(out));
  sqliteFree(out);

  Tcl_SetObjResult(interp, res);

  return TCL_OK;
}

static int sqlite_utf16to8(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  unsigned char *out;
  unsigned char *in;
  Tcl_Obj *res;

  if( objc!=2 ){
    Tcl_AppendResult(interp, "wrong # args: should be \"",
        Tcl_GetStringFromObj(objv[0], 0), " <utf-16 encoded-string>", 0);
    return TCL_ERROR;
  }

  in = Tcl_GetByteArrayFromObj(objv[1], 0);
  out = sqlite3utf16to8(in, -1, SQLITE3_BIGENDIAN);
  res = Tcl_NewByteArrayObj(out, strlen(out)+1);
  sqliteFree(out);

  Tcl_SetObjResult(interp, res);

  return TCL_OK;
}

/*
** The first argument is a TCL UTF-8 string. Return the byte array
** object with the encoded representation of the string, including
** the NULL terminator.
*/
static int binarize(
  void * clientData,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *CONST objv[]
){
  int len;
  char *bytes;
  Tcl_Obj *pRet;
  assert(objc==2);

  bytes = Tcl_GetStringFromObj(objv[1], &len);
  pRet = Tcl_NewByteArrayObj(bytes, len+1);
  Tcl_SetObjResult(interp, pRet);
  return TCL_OK;
}


/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest5_Init(Tcl_Interp *interp){
  static struct {
    char *zName;
    Tcl_ObjCmdProc *xProc;
  } aCmd[] = {
    { "sqlite_utf16to8",         (Tcl_ObjCmdProc*)sqlite_utf16to8    },
    { "sqlite_utf8to16le",       (Tcl_ObjCmdProc*)sqlite_utf8to16le  },
    { "sqlite_utf8to16be",       (Tcl_ObjCmdProc*)sqlite_utf8to16be  },
    { "sqlite_utf16to16le",      (Tcl_ObjCmdProc*)sqlite_utf16to16le },
    { "sqlite_utf16to16be",      (Tcl_ObjCmdProc*)sqlite_utf16to16be },
    { "binarize",                (Tcl_ObjCmdProc*)binarize }
  };
  int i;
  for(i=0; i<sizeof(aCmd)/sizeof(aCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aCmd[i].zName, aCmd[i].xProc, 0, 0);
  }

  return TCL_OK;
}



