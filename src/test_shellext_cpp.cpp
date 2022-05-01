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
** Test extension for testing the shell's .load <extName> -shext function.
** To build from the SQLite project root:
     g++ -shared -fPIC -Wall -I. -g src/test_shellext_cpp.cpp \
      -o test_shellext_cpp.so
*/
#include <stdio.h>
#include "shx_link.h"

SQLITE_EXTENSION_INIT1;

SHELL_EXTENSION_INIT1(pShExtApi, pExtHelpers, shextLinkFetcher);
#define SHX_API(entry) pShExtApi->entry
#define SHX_HELPER(entry) pExtHelpers->entry
#define oprintf pExtHelpers->utf8CurrentOutPrintf

struct BatBeing : DotCommand {

  ~BatBeing() {
  }; // No held resources; copy/assign is fine and dying is easy.

  void destruct() {
    if( pSXS ) oprintf(pSXS, "BatBeing unbecoming.\n");
  }

  const char *name() { return "bat_being"; };

  const char *help(const char *zHK) {
    if( !zHK )
      return ".bat_being ?whatever?    Demonstrates vigilantism weekly\n";
    if( !*zHK )
      return "   Options summon side-kick and villains.\n";
    return 0;
  };

  DotCmdRC argsCheck(char **pzErrMsg, int nArgs, char *azArgs[]) {
    (void)(pzErrMsg);
    (void)(nArgs);
    (void)(azArgs);
    return DCR_Ok;
  };
  DotCmdRC execute(ShellExState *psx, char **pzErrMsg,
                   int nArgs, char *azArgs[]);

  BatBeing(DotCommand *pp = 0) {
    numCalls = 0;
    pPrint = pp;
    pPrior = 0;
    pSXS = 0;
  };

  // Default copy/assign are fine; nothing held.

  int numCalls;
  DotCommand * pPrint;
  DotCommand * pPrior;
  ShellExState *pSXS;
};

static void sayHowMany( BatBeing *pbb, ShellExState *psx ){
  if( pbb->pPrint ){
    static char cmd[] =  "print";
    char *az[] = { cmd, 0 };
    char *zErr = 0;
    DotCmdRC rc;
    az[1] = sqlite3_mprintf("This execute has been called %d times.",
                            ++pbb->numCalls);
    rc = pbb->pPrint->execute(psx, &zErr, 2, az);
    sqlite3_free(az[1]);
    if( rc!= DCR_Ok ){
      oprintf(psx, "print() failed: %d\n", rc);
    }
  }
}

DotCmdRC BatBeing::execute(ShellExState *psx, char **pzErrMsg,
                           int nArgs, char *azArgs[]) {
  pSXS = psx;
  switch( nArgs ){
  default:
    {
      if( pPrior ){
        char *az1 = azArgs[1];
        for( int i=2; i<nArgs; ++i ) azArgs[i-1] = azArgs[i];
        azArgs[nArgs-1] = az1;
        return pPrior->execute(psx, pzErrMsg, nArgs, azArgs);
      }else{
        SHX_HELPER(setColumnWidths)(psx, azArgs+1, nArgs-1);
        oprintf(psx, "Column widths:");
        for( int cix=0; cix<psx->numWidths; ++cix ){
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
  sayHowMany(this, psx);
  return DCR_Ok;
}

/* Define/initialize BatBeing as a DotCommand subclass using above v-table.
 * This compiles in a type-safe manner because the batty_methods v-table
 * and methods it incorporates strictly match the DotCommand interface.
 */
static BatBeing batty(0);

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
extern "C"
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_testshellextcpp_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int iLdErr;
  int nErr = 0;
  SQLITE_EXTENSION_INIT2(pApi);
  SHELL_EXTENSION_INIT2(pShExtLink, shextLinkFetcher, db);

  SHELL_EXTENSION_INIT3(pShExtApi, pExtHelpers, pShExtLink);
  iLdErr = SHELL_EXTENSION_LOADFAIL_WHY(pShExtLink, 5, 14);
  if( iLdErr!=EXLD_Ok ){
    *pzErrMsg = sqlite3_mprintf("Load failed, cause %d\n", iLdErr);
    return SQLITE_ERROR;
  }else{
    ShellExState *psx = pShExtLink->pSXS;
    int rc;
    char *zLoadArgs = sqlite3_mprintf("Load arguments:");
    int ila;

    for( ila=0; ila<pShExtLink->nLoadArgs; ++ila ){
      zLoadArgs = sqlite3_mprintf("%z %s", zLoadArgs,
                                  pShExtLink->azLoadArgs[ila]);
    }
    if( ila ) fprintf(SHX_HELPER(currentOutputFile)(psx), "%s\n", zLoadArgs);
    sqlite3_free(zLoadArgs);
    SHX_API(subscribeEvents)(psx, sqlite3_testshellextcpp_init, &batty,
                             NK_CountOf, shellEventHandle);
    batty.pPrint = SHX_HELPER(findDotCommand)("print", psx, &rc);
    batty.pPrior = SHX_HELPER(findDotCommand)(batty.name(), psx, &rc);
    rc = SHX_API(registerDotCommand)(psx,
                                     sqlite3_testshellextcpp_init, &batty);
    if( rc!=0 ) ++nErr;
    pShExtLink->eid = sqlite3_testshellextcpp_init;
  }
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}
