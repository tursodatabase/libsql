/*
** 2014 August 30
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains a command-line application that uses the OTA 
** extension. See the usage() function below for an explanation.
*/

#include "sqlite3ota.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
** Print a usage message and exit.
*/
void usage(const char *zArgv0){
  fprintf(stderr, 
"Usage: %s [-step NSTEP] TARGET-DB OTA-DB\n"
"\n"
"  Argument OTA-DB must be an OTA database containing an update suitable for\n"
"  target database TARGET-DB. If NSTEP is set to less than or equal to zero\n"
"  (the default value), this program attempts to apply the entire update to\n"
"  the target database.\n"
"\n"
"  If NSTEP is greater than zero, then a maximum of NSTEP calls are made\n"
"  to sqlite3ota_step(). If the OTA update has not been completely applied\n"
"  after the NSTEP'th call is made, the state is saved in the database OTA-DB\n"
"  and the program exits. Subsequent invocations of this (or any other OTA)\n"
"  application will use this state to resume applying the OTA update to the\n"
"  target db.\n"
"\n"
, zArgv0);
  exit(1);
}

void report_default_vfs(){
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  fprintf(stdout, "default vfs is \"%s\"\n", pVfs->zName);
}

void report_ota_vfs(sqlite3ota *pOta){
  sqlite3 *db = sqlite3ota_db(pOta, 0);
  if( db ){
    char *zName = 0;
    sqlite3_file_control(db, "main", SQLITE_FCNTL_VFSNAME, &zName);
    if( zName ){
      fprintf(stdout, "using vfs \"%s\"\n", zName);
    }else{
      fprintf(stdout, "vfs name not available\n");
    }
    sqlite3_free(zName);
  }
}

int main(int argc, char **argv){
  int i;
  const char *zTarget;            /* Target database to apply OTA to */
  const char *zOta;               /* Database containing OTA */
  char *zErrmsg;                  /* Error message, if any */
  sqlite3ota *pOta;               /* OTA handle */
  int nStep = 0;                  /* Maximum number of step() calls */
  int rc;
  sqlite3_int64 nProgress = 0;

  /* Process command line arguments. Following this block local variables 
  ** zTarget, zOta and nStep are all set. */
  if( argc==5 ){
    int nArg1 = strlen(argv[1]);
    if( nArg1>5 || nArg1<2 || memcmp("-step", argv[1], nArg1) ) usage(argv[0]);
    nStep = atoi(argv[2]);
  }else if( argc!=3 ){
    usage(argv[0]);
  }
  zTarget = argv[argc-2];
  zOta = argv[argc-1];

  report_default_vfs();

  /* Open an OTA handle. If nStep is less than or equal to zero, call
  ** sqlite3ota_step() until either the OTA has been completely applied
  ** or an error occurs. Or, if nStep is greater than zero, call
  ** sqlite3ota_step() a maximum of nStep times.  */
  pOta = sqlite3ota_open(zTarget, zOta);
  report_ota_vfs(pOta);
  for(i=0; (nStep<=0 || i<nStep) && sqlite3ota_step(pOta)==SQLITE_OK; i++);
  nProgress = sqlite3ota_progress(pOta);
  rc = sqlite3ota_close(pOta, &zErrmsg);

  /* Let the user know what happened. */
  switch( rc ){
    case SQLITE_OK:
      fprintf(stdout, 
          "SQLITE_OK: ota update incomplete (%lld operations so far)\n",
          nProgress
      );
      break;

    case SQLITE_DONE:
      fprintf(stdout, 
          "SQLITE_DONE: ota update completed (%lld operations)\n",
          nProgress
      );
      break;

    default:
      fprintf(stderr, "error=%d: %s\n", rc, zErrmsg);
      break;
  }

  sqlite3_free(zErrmsg);
  return (rc==SQLITE_OK || rc==SQLITE_DONE) ? 0 : 1;
}

