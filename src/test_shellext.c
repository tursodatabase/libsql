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

/* These DERIVED_METHOD(...) macro calls' arguments were copied and
 * pasted from the MetaCommand interface declaration in shext_linkage.h ,
 * but with Interface,Derived substituted for the interface typename.
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

DERIVED_METHOD(int, argsCheck, MetaCommand,BatBeing, 3,
             (char **pzErrMsg, int nArgs, char *azArgs[])){
  return 0;
}

typedef struct BatBeing BatBeing;
static void sayHowMany( struct BatBeing *pbb, FILE *out );

DERIVED_METHOD(int, execute, MetaCommand,BatBeing, 4,
             (ShellExState *psx, char **pzErrMsg, int nArgs, char *azArgs[])){
  FILE *out = pExtHelpers->currentOutputFile(psx);
  switch( nArgs ){
  default: fprintf(out, "The Penguin, Joker and Riddler have teamed up!\n");
  case 2: fprintf(out, "The Dynamic Duo arrives, and ... ");
  case 1: fprintf(out, "@#$ KaPow! $#@\n");
  }
  sayHowMany((struct BatBeing *)pThis, out);
  return 0;
}

/* Note that these CONCRETE_METHOD... macro calls' arguments were copied and
 * pasted from the MetaCommand interface declaration in shext_linkage.h .
 * In a future version of shext_linkage.h, this will all be a mondo maco. */
CONCRETE_BEGIN(MetaCommand, BatBeing);
CONCRETE_METHOD(const char *, name, MetaCommand, 0,());
CONCRETE_METHOD(const char *, help, MetaCommand, 1,(int more));
CONCRETE_METHOD(int, argsCheck, MetaCommand, 3,
                 (char **pzErrMsg, int nArgs, char *azArgs[]));
CONCRETE_METHOD(int, execute, MetaCommand, 4,
                 (ShellExState *, char **pzErrMsg, int nArgs, char *azArgs[]));
CONCRETE_END(BatBeing) batty_methods = {
  DECORATE_METHOD(BatBeing,destruct),
  DECORATE_METHOD(BatBeing,name),
  DECORATE_METHOD(BatBeing,help),
  DECORATE_METHOD(BatBeing,argsCheck),
  DECORATE_METHOD(BatBeing,execute)
};

INSTANCE_BEGIN(BatBeing);
  int numCalls;
INSTANCE_END(BatBeing) batty = {
  &batty_methods,
  0
};

static void sayHowMany( struct BatBeing *pbb, FILE *out ){
  fprintf(out, "This execute has been called %d times.\n", ++pbb->numCalls);
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
    rc = pShExtApi->registerMetaCommand(psx, sqlite3_testshellext_init,pmc);
    if( rc!=0 ) ++nErr;
  }
  else{
    printf("No ShellExtensionLink pointer or registration API.\n");
    ++nErr;
  }
  return nErr ? SQLITE_ERROR : SQLITE_OK;
}
