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
** g++ -shared -fPIC -Wall -I. -g src/test_shellext.cpp -o test_shellext.so
*/
#include <stdio.h>
#include "shx_link.h"

SQLITE_EXTENSION_INIT1;

SHELL_EXTENSION_INIT1(pShExtApi, pExtHelpers, shextLinkFetcher);
#define SHX_API(entry) pShExtApi->entry
#define SHX_HELPER(entry) pExtHelpers->entry

struct BatBeing : MetaCommand {

  ~BatBeing() {}; // No held resources; copy/assign is fine and dying is easy.

  void destruct() { this->~BatBeing(); }

  const char *name() { return "bat_being"; };

  const char *help(const char *zHK) {
    if( !zHK )
      return ".bat_being ?whatever?    Demonstrates vigilantism weekly\n";
    if( !*zHK )
      return "   Options summon side-kick and villains.\n";
    return 0;
  };

  DotCmdRC argsCheck(char **pzErrMsg, int nArgs, char *azArgs[]) {
    return DCR_Ok;
  };
  DotCmdRC execute(ShellExState *psx, char **pzErrMsg,
                   int nArgs, char *azArgs[]);

  BatBeing(MetaCommand *pp = 0) {
    numCalls = 0;
    pPrint = pp;
  };

  // Default copy/assign are fine; nothing held.

  int numCalls;
  MetaCommand * pPrint;
};

static void sayHowMany( BatBeing *pbb, FILE *out, ShellExState *psx ){
  if( pbb->pPrint ){
    static char cmd[] =  "print";
    char *az[] = { cmd, 0 };
    char *zErr = 0;
    DotCmdRC rc;
    az[1] = sqlite3_mprintf("This execute has been called %d times.\n",
                            ++pbb->numCalls);
    rc = pbb->pPrint->execute(psx, &zErr, 2, az);
    sqlite3_free(az[1]);
    if( rc!= DCR_Ok ){
      fprintf(out, "print() failed: %d\n", rc);
    }
  }
}

DotCmdRC BatBeing::execute(ShellExState *psx, char **pzErrMsg,
                           int nArgs, char *azArgs[]) {
  FILE *out = SHX_HELPER(currentOutputFile)(psx);
  switch( nArgs ){
  default: fprintf(out, "The Penguin, Joker and Riddler have teamed up!\n");
  case 2: fprintf(out, "The Dynamic Duo arrives, and ... ");
  case 1: fprintf(out, "@#$ KaPow! $#@\n");
  }
  sayHowMany(this, out, psx);
  return DCR_Ok;
}

/* Define/initialize BatBeing as a MetaCommand subclass using above v-table. 
 * This compiles in a type-safe manner because the batty_methods v-table
 * and methods it incorporates strictly match the MetaCommand interface.
 */
static BatBeing batty(0);

static int shellEventHandle(void *pv, NoticeKind nk,
                            void *pvSubject, ShellExState *psx){
  FILE *out = SHX_HELPER(currentOutputFile)(psx);
  if( nk==NK_ShutdownImminent ){
    BatBeing *pbb = (BatBeing *)pv;
    fprintf(out, "Bat cave meteor strike detected after %d calls.\n",
            pbb->numCalls);
  }else if( nk==NK_Unsubscribe ){
    fprintf(out, "BatBeing incommunicado.\n");
  }else if( nk==NK_DbUserAppeared || nk==NK_DbUserVanishing ){
    const char *zWhat = (nk==NK_DbUserAppeared)? "appeared" : "vanishing";
    fprintf(out, "dbUser(%p) %s\n", pvSubject, zWhat);
    if( psx->dbUser != pvSubject ) fprintf(out, "not dbx(%p)\n", psx->dbUser);
  }else if( nk==NK_DbAboutToClose ){
    fprintf(out, "db(%p) closing\n", pvSubject);
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
int sqlite3_testshellext_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int iLdErr;
  int nErr = 0;
  SQLITE_EXTENSION_INIT2(pApi);
  SHELL_EXTENSION_INIT2(pShExtLink, shextLinkFetcher, db);

  SHELL_EXTENSION_INIT3(pShExtApi, pExtHelpers, pShExtLink);
  iLdErr = SHELL_EXTENSION_LOADFAIL_WHY(pShExtLink, 5, 5);
  if( iLdErr!=EXLD_Ok ){
    *pzErrMsg = sqlite3_mprintf("Load failed, cause %d\n", iLdErr);
    return SQLITE_ERROR;
  }else{
    ShellExState *psx = pShExtLink->pSXS;
    int rc;

    SHX_API(subscribeEvents)(psx, sqlite3_testshellext_init, &batty,
                             NK_CountOf, shellEventHandle);
    batty.pPrint = SHX_HELPER(findMetaCommand)("print", psx, &rc);
    rc = SHX_API(registerMetaCommand)(psx, sqlite3_testshellext_init, &batty);
    if( rc!=0 ) ++nErr;
    pShExtLink->eid = sqlite3_testshellext_init;
  }
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}
