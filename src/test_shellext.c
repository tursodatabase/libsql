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
** gcc -shared -fPIC -Wall -I$srcdir -I.. -g test_shellext.c -o test_shellext.so
*/
#include <stdio.h>
#include "shext_linkage.h"

SQLITE_EXTENSION_INIT1;

static struct ShExtAPI *pShExtApi = 0;
static struct ExtHelpers *pExtHelpers = 0;

typedef struct BatBeing BatBeing;
static void sayHowMany( BatBeing *pbb, FILE *out, ShellExState *psx );

/* These DERIVED_METHOD(...) macro calls' arguments were copied and
 * pasted from the MetaCommand interface declaration in shext_linkage.h ,
 * but with "Interface,Derived" substituted for the interface typename.
 * The function bodies are not so easily written, of course. */

DERIVED_METHOD(void, destruct, MetaCommand,BatBeing, 0, ()){
  fprintf(stderr, "BatBeing unbecoming.\n");
}

DERIVED_METHOD(const char *, name, MetaCommand,BatBeing, 0,()){
  return "bat_being";
}

DERIVED_METHOD(const char *, help, MetaCommand,BatBeing, 1,(int more)){
  switch( more ){
  case 0: return
      ".bat_being ?whatever?    Demonstrates vigilantism weekly\n";
  case 1: return "   Options summon side-kick and villains.\n";
  default: return 0;
  }
}

DERIVED_METHOD(DotCmdRC, argsCheck, MetaCommand,BatBeing, 3,
             (char **pzErrMsg, int nArgs, char *azArgs[])){
  return DCR_Ok;
}

DERIVED_METHOD(DotCmdRC, execute, MetaCommand,BatBeing, 4,
             (ShellExState *psx, char **pzErrMsg, int nArgs, char *azArgs[])){
  FILE *out = pExtHelpers->currentOutputFile(psx);
  switch( nArgs ){
  default: fprintf(out, "The Penguin, Joker and Riddler have teamed up!\n");
  case 2: fprintf(out, "The Dynamic Duo arrives, and ... ");
  case 1: fprintf(out, "@#$ KaPow! $#@\n");
  }
  sayHowMany((BatBeing *)pThis, out, psx);
  return DCR_Ok;
}

/* Define a MetaCommand v-table initialized to reference above methods. */
MetaCommand_IMPLEMENT_VTABLE(BatBeing, batty_methods);

/* Define/initialize BatBeing as a MetaCommand subclass using above v-table. 
 * This compiles in a type-safe manner because the batty_methods v-table
 * and methods it incorporates strictly match the MetaCommand interface.
 */
INSTANCE_BEGIN(BatBeing);
  int numCalls;
  MetaCommand * pPrint;
INSTANCE_END(BatBeing) batty = {
  &batty_methods,
  0, 0
};

static void sayHowMany( BatBeing *pbb, FILE *out, ShellExState *psx ){
  if( pbb->pPrint ){
    char *az[] = { "print", 0 };
    char *zErr = 0;
    MetaCommand * pmcPrint = pbb->pPrint;
    DotCmdRC rc;
    az[1] = sqlite3_mprintf("This execute has been called %d times.\n",
                            ++pbb->numCalls);
    rc = pmcPrint->pMethods->execute(pmcPrint, psx, &zErr, 2, az);
    sqlite3_free(az[1]);
    if( rc!= DCR_Ok ){
      fprintf(out, "print() failed: %d\n", rc);
    }
  }
}

static int shellEventHandle(void *pv, NoticeKind nk,
                            void *pvSubject, ShellExState *psx){
  FILE *out = pExtHelpers->currentOutputFile(psx);
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

DEFINE_SHDB_TO_SHEXTLINK(shext_link);

/*
** Extension load function.
*/
#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_testshellext_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int nErr = 0;
  ShellExtensionLink *pShExtLink;
  SQLITE_EXTENSION_INIT2(pApi);
  pShExtLink = shext_link(db);
  if( pShExtLink && pShExtLink->pShellExtensionAPI->numRegistrars>=1 ){
    ShellExState *psx = pShExtLink->pSXS;
    MetaCommand *pmc = (MetaCommand *)&batty;
    int rc;

    pShExtApi = & pShExtLink->pShellExtensionAPI->api.named;
    pExtHelpers = & pShExtLink->pShellExtensionAPI->pExtHelpers->helpers.named;
    pShExtApi->subscribeEvents(psx, sqlite3_testshellext_init, &batty,
                               NK_CountOf, shellEventHandle);
    batty.pPrint = pExtHelpers->findMetaCommand("print", psx, &rc);
    rc = pShExtApi->registerMetaCommand(psx, sqlite3_testshellext_init,  pmc);
    if( rc!=0 ) ++nErr;
    pShExtLink->eid = sqlite3_testshellext_init;
  }
  else{
    printf("No ShellExtensionLink pointer or registration API.\n");
    ++nErr;
  }
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}
