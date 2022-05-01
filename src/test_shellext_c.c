/*
** 2022 Feb 28
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Test extension for testing the shell's .load -shellext ... function.
** To build from the SQLite project root:
** gcc -shared -fPIC -Wall -I. -g src/test_shellext_c.c -o test_shellext_c.so
*/
#include <stdio.h>
#include "shx_link.h"

SQLITE_EXTENSION_INIT1;

SHELL_EXTENSION_INIT1(pShExtApi, pExtHelpers, shextLinkFetcher);
#define SHX_API(entry) pShExtApi->entry
#define SHX_HELPER(entry) pExtHelpers->entry
#define oprintf pExtHelpers->utf8CurrentOutPrintf

typedef struct BatBeing BatBeing;
static void sayHowMany( BatBeing *pbb, ShellExState *psx );

/* These DERIVED_METHOD(...) macro calls' arguments were copied and
 * pasted from the DotCommand interface declaration in shext_linkage.h ,
 * but with "Interface,Derived" substituted for the interface typename.
 * The function bodies are not so easily written, of course. */

DERIVED_METHOD(void, destruct, DotCommand,BatBeing, 0, ());

DERIVED_METHOD(const char *, name, DotCommand,BatBeing, 0,()){
  (void)(pThis);
  return "bat_being";
}

DERIVED_METHOD(const char *, help, DotCommand,BatBeing, 1,(const char *zHK)){
  (void)(pThis);
  if( !zHK )
    return ".bat_being ?whatever?    Demonstrates vigilantism weekly\n";
  if( !*zHK )
    return "   Options summon side-kick and villains.\n";
  return 0;
}

DERIVED_METHOD(DotCmdRC, argsCheck, DotCommand,BatBeing, 3,
             (char **pzErrMsg, int nArgs, char *azArgs[])){
  (void)(pThis);
  (void)(pzErrMsg);
  (void)(nArgs);
  (void)(azArgs);
  return DCR_Ok;
}

DERIVED_METHOD(DotCmdRC, execute, DotCommand,BatBeing, 4,
           (ShellExState *psx, char **pzErrMsg, int nArgs, char *azArgs[]));

/* Define a DotCommand v-table initialized to reference above methods. */
DotCommand_IMPLEMENT_VTABLE(BatBeing, batty_methods);

/* Define/initialize BatBeing as a DotCommand subclass using above v-table. 
 * This compiles in a type-safe manner because the batty_methods v-table
 * and methods it incorporates strictly match the DotCommand interface.
 */
INSTANCE_BEGIN(BatBeing);
  int numCalls;
  DotCommand * pPrint;
  DotCommand * pPrior;
  ShellExState *pSXS;
INSTANCE_END(BatBeing) batty = {
  &batty_methods,
  0, 0, 0, 0
};

DERIVED_METHOD(void, destruct, DotCommand,BatBeing, 0, ()){
  BatBeing *pBB = (BatBeing*)(pThis);
  if( pBB->pSXS ) oprintf(pBB->pSXS, "BatBeing unbecoming.\n");
}

DERIVED_METHOD(DotCmdRC, execute, DotCommand,BatBeing, 4,
             (ShellExState *psx, char **pzErrMsg, int nArgs, char *azArgs[])){
  BatBeing *pbb = (BatBeing*)pThis;
  pbb->pSXS = psx;
  switch( nArgs ){
  default:
    {
      if( pbb->pPrior ){
        char *az1 = azArgs[1];
        for( int i=2; i<nArgs; ++i ) azArgs[i-1] = azArgs[i];
        azArgs[nArgs-1] = az1;
        return pbb->pPrior->pMethods->execute(pbb->pPrior, psx,
                                              pzErrMsg, nArgs, azArgs);
      }else{
        int cix;
        SHX_HELPER(setColumnWidths)(psx, azArgs+1, nArgs-1);
        oprintf(psx, "Column widths:");
        for( cix=0; cix<psx->numWidths; ++cix ){
          oprintf(psx, " %d", psx->pSpecWidths[cix]);
        }
        oprintf(psx, "\n");
      }
    }
    break;
  case 3:
    oprintf(psx, "The Penguin, Joker and Riddler have teamed up!\n");
  case 2: oprintf(psx, "The Dynamic Duo arrives, and ... ");
  case 1: oprintf(psx, "@#$ KaPow! $#@\n");
  }
  sayHowMany((BatBeing *)pThis, psx);
  return DCR_Ok;
}

static void sayHowMany( BatBeing *pbb, ShellExState *psx ){
  if( pbb->pPrint ){
    char *az[] = { "print", 0 };
    char *zErr = 0;
    DotCommand * pdcPrint = pbb->pPrint;
    DotCmdRC rc;
    az[1] = sqlite3_mprintf("This execute has been called %d times.",
                            ++pbb->numCalls);
    rc = pdcPrint->pMethods->execute(pdcPrint, psx, &zErr, 2, az);
    sqlite3_free(az[1]);
    if( rc!= DCR_Ok ){
      oprintf(psx, "print() failed: %d\n", rc);
    }
  }
}

static int shellEventHandle(void *pv, NoticeKind nk,
                            void *pvSubject, ShellExState *psx){
  if( nk==NK_ShutdownImminent ){
    BatBeing *pbb = (BatBeing *)pv;
    oprintf(psx, "Bat cave meteor strike detected after %d calls.\n",
            pbb->numCalls);
  }else if( nk==NK_Unsubscribe ){
    oprintf(psx, "BatBeing incommunicado.\n");
  }else if( nk==NK_DbUserAppeared || nk==NK_DbUserVanishing ){
    const char *zWhat = (nk==NK_DbUserAppeared)? "appeared" : "vanishing";
    int isDbu = pvSubject==psx->dbUser;
    oprintf(psx, "db%s %s\n", isDbu? "User" : "?", zWhat);
    if( psx->dbUser != pvSubject ) oprintf(psx, "not dbx(%p)\n", psx->dbUser);
  }else if( nk==NK_DbAboutToClose ){
    const char *zdb = (pvSubject==psx->dbUser)? "User"
      : (pvSubject==psx->dbShell)? "Shell" : "?";
    oprintf(psx, "db%s closing\n", zdb);
  }
  return 0;
}

/*
** Extension load function.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_testshellextc_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int nErr = 0;
  int iLdErr;
  SQLITE_EXTENSION_INIT2(pApi);
  SHELL_EXTENSION_INIT2(pShExtLink, shextLinkFetcher, db);

  SHELL_EXTENSION_INIT3(pShExtApi, pExtHelpers, pShExtLink);
  iLdErr = SHELL_EXTENSION_LOADFAIL_WHY(pShExtLink, 5, 13);
  if( iLdErr!=EXLD_Ok ){
    *pzErrMsg = sqlite3_mprintf("Load failed, cause %d\n", iLdErr);
    return SQLITE_ERROR;
  }else{
    ShellExState *psx = pShExtLink->pSXS;
    DotCommand *pdc = (DotCommand *)&batty;
    int rc;
    char *zLoadArgs = sqlite3_mprintf("Load arguments:");
    int ila;

    for( ila=0; ila<pShExtLink->nLoadArgs; ++ila ){
      zLoadArgs = sqlite3_mprintf("%z %s", zLoadArgs,
                                  pShExtLink->azLoadArgs[ila]);
    }
    if( ila ) oprintf(psx, "%s\n", zLoadArgs);
    sqlite3_free(zLoadArgs);
    SHX_API(subscribeEvents)(psx, sqlite3_testshellextc_init, &batty,
                             NK_CountOf, shellEventHandle);
    batty.pPrint = SHX_HELPER(findDotCommand)("print", psx, &rc);
    batty.pPrior = SHX_HELPER(findDotCommand)("bat_being", psx, &rc);
    rc = SHX_API(registerDotCommand)(psx, sqlite3_testshellextc_init, pdc);
    if( rc!=0 ) ++nErr;
    pShExtLink->eid = sqlite3_testshellextc_init;
  }
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}
