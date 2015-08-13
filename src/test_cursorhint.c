/*
** 2008 March 19
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
** implements new SQL functions used by the test scripts.
*/
#include "sqlite3.h"
#include "tcl.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "sqliteInt.h"
#include "vdbeInt.h"

struct CursorHintGlobal {
  Tcl_Interp *interp;
  Tcl_Obj *pScript;
} cursorhintglobal;

static char *exprToString(Mem *aMem, Expr *pExpr){
  char *zRet = 0;
  char *zBinOp = 0;

  switch( pExpr->op ){
    case TK_STRING:
      zRet = sqlite3_mprintf("%Q", pExpr->u.zToken);
      break;

    case TK_INTEGER:
      zRet = sqlite3_mprintf("%d", pExpr->u.iValue);
      break;

    case TK_NULL:
      zRet = sqlite3_mprintf("%s", "NULL");
      break;

    case TK_REGISTER: {
      Mem *pMem = &aMem[pExpr->iTable];
      if( pMem->flags & MEM_Int ){
        zRet = sqlite3_mprintf("%lld", pMem->u.i);
      }
      else if( pMem->flags & MEM_Real ){
        zRet = sqlite3_mprintf("%f", pMem->u.r);
      }
      else if( pMem->flags & MEM_Str ){
        zRet = sqlite3_mprintf("%.*Q", pMem->n, pMem->z);
      }
      else if( pMem->flags & MEM_Blob ){
      }
      else{
        zRet = sqlite3_mprintf("%s", "NULL");
      }
      break;
    }

    case TK_COLUMN: {
      zRet = sqlite3_mprintf("col(%d)", (int)pExpr->iColumn);
      break;
    }

    case TK_LT:      zBinOp = "<";      break;
    case TK_LE:      zBinOp = "<=";     break;
    case TK_GT:      zBinOp = ">";      break;
    case TK_GE:      zBinOp = ">=";     break;
    case TK_NE:      zBinOp = "!=";     break;
    case TK_EQ:      zBinOp = "==";     break;
    case TK_IS:      zBinOp = "IS";     break;
    case TK_ISNOT:   zBinOp = "IS NOT"; break;
    case TK_AND:     zBinOp = "AND";    break;
    case TK_OR:      zBinOp = "OR";     break;
    case TK_PLUS:    zBinOp = "+";      break;
    case TK_STAR:    zBinOp = "*";      break;
    case TK_MINUS:   zBinOp = "-";      break;
    case TK_REM:     zBinOp = "%";      break;
    case TK_BITAND:  zBinOp = "&";      break;
    case TK_BITOR:   zBinOp = "|";      break;
    case TK_SLASH:   zBinOp = "/";      break;
    case TK_LSHIFT:  zBinOp = "<<";     break;
    case TK_RSHIFT:  zBinOp = ">>";     break;
    case TK_CONCAT:  zBinOp = "||";     break;

    default:
      zRet = sqlite3_mprintf("%s", "expr");
      break;
  }

  if( zBinOp ){
    zRet = sqlite3_mprintf("(%z %s %z)", 
        exprToString(aMem, pExpr->pLeft),
        zBinOp,
        exprToString(aMem, pExpr->pRight)
    );
  }

  return zRet;
}

void sqlite3BtreeCursorHintTest(Mem *aMem, Expr *pExpr){
  if( cursorhintglobal.pScript ){
    Tcl_Obj *pEval = Tcl_DuplicateObj(cursorhintglobal.pScript);
    char *zExpr;
    Tcl_Obj *pObj;
    Tcl_IncrRefCount(pEval);
    zExpr = exprToString(aMem, pExpr);
    pObj = Tcl_NewStringObj(zExpr, -1);
    sqlite3_free(zExpr);
    Tcl_ListObjAppendElement(cursorhintglobal.interp, pEval, pObj);
    Tcl_EvalObjEx(cursorhintglobal.interp, pEval, TCL_GLOBAL_ONLY);
    Tcl_DecrRefCount(pEval);
  }
}

/*
** Usage: cursorhint_hook SCRIPT
*/
static int install_cursorhint_hook(
  ClientData clientData, /* Pointer to sqlite3_enable_XXX function */
  Tcl_Interp *interp,    /* The TCL interpreter that invoked this command */
  int objc,              /* Number of arguments */
  Tcl_Obj *CONST objv[]  /* Command arguments */
){
  if( objc!=1 && objc!=2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "?SCRIPT?");
    return TCL_ERROR;
  }
  if( cursorhintglobal.pScript ){
    Tcl_DecrRefCount(cursorhintglobal.pScript);
    memset(&cursorhintglobal, 0, sizeof(cursorhintglobal));
  }
  if( objc==2 ){
    cursorhintglobal.interp = interp;
    cursorhintglobal.pScript = Tcl_DuplicateObj(objv[1]);
  }
  return TCL_OK;
}

/*
** Register commands with the TCL interpreter.
*/
int Sqlitetest_cursorhint_Init(Tcl_Interp *interp){
  static struct {
     char *zName;
     Tcl_ObjCmdProc *xProc;
  } aObjCmd[] = {
     { "cursorhint_hook",    install_cursorhint_hook },
  };
  int i;
  for(i=0; i<sizeof(aObjCmd)/sizeof(aObjCmd[0]); i++){
    Tcl_CreateObjCommand(interp, aObjCmd[i].zName, aObjCmd[i].xProc, 0, 0);
  }
  sqlite3_initialize();
  return TCL_OK;
}
