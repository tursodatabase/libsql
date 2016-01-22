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
** This file contains a command-line application that uses the RBU 
** extension. See the usage() function below for an explanation.
*/

#include "sqlite3rbu.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
** Print a usage message and exit.
*/
void usage(const char *zArgv0){
  fprintf(stderr, 
"Usage: %s [-step NSTEP] TARGET-DB RBU-DB\n"
"\n"
"  Argument RBU-DB must be an RBU database containing an update suitable for\n"
"  target database TARGET-DB. If NSTEP is set to less than or equal to zero\n"
"  (the default value), this program attempts to apply the entire update to\n"
"  the target database.\n"
"\n"
"  If NSTEP is greater than zero, then a maximum of NSTEP calls are made\n"
"  to sqlite3rbu_step(). If the RBU update has not been completely applied\n"
"  after the NSTEP'th call is made, the state is saved in the database RBU-DB\n"
"  and the program exits. Subsequent invocations of this (or any other RBU)\n"
"  application will use this state to resume applying the RBU update to the\n"
"  target db.\n"
"\n"
, zArgv0);
  exit(1);
}

void report_default_vfs(){
  sqlite3_vfs *pVfs = sqlite3_vfs_find(0);
  fprintf(stdout, "default vfs is \"%s\"\n", pVfs->zName);
}

void report_rbu_vfs(sqlite3rbu *pRbu){
  sqlite3 *db = sqlite3rbu_db(pRbu, 0);
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
  const char *zTarget;            /* Target database to apply RBU to */
  const char *zRbu;               /* Database containing RBU */
  char zBuf[200];                 /* Buffer for printf() */
  char *zErrmsg;                  /* Error message, if any */
  sqlite3rbu *pRbu;               /* RBU handle */
  int nStep = 0;                  /* Maximum number of step() calls */
  int rc;
  sqlite3_int64 nProgress = 0;

  /* Process command line arguments. Following this block local variables 
  ** zTarget, zRbu and nStep are all set. */
  if( argc==5 ){
    size_t nArg1 = strlen(argv[1]);
    if( nArg1>5 || nArg1<2 || memcmp("-step", argv[1], nArg1) ) usage(argv[0]);
    nStep = atoi(argv[2]);
  }else if( argc!=3 ){
    usage(argv[0]);
  }
  zTarget = argv[argc-2];
  zRbu = argv[argc-1];

  report_default_vfs();

  /* Open an RBU handle. If nStep is less than or equal to zero, call
  ** sqlite3rbu_step() until either the RBU has been completely applied
  ** or an error occurs. Or, if nStep is greater than zero, call
  ** sqlite3rbu_step() a maximum of nStep times.  */
  pRbu = sqlite3rbu_open(zTarget, zRbu, 0);
  report_rbu_vfs(pRbu);
  for(i=0; (nStep<=0 || i<nStep) && sqlite3rbu_step(pRbu)==SQLITE_OK; i++);
  nProgress = sqlite3rbu_progress(pRbu);
  rc = sqlite3rbu_close(pRbu, &zErrmsg);

  /* Let the user know what happened. */
  switch( rc ){
    case SQLITE_OK:
      sqlite3_snprintf(sizeof(zBuf), zBuf,
          "SQLITE_OK: rbu update incomplete (%lld operations so far)\n",
          nProgress
      );
      fprintf(stdout, "%s", zBuf);
      break;

    case SQLITE_DONE:
      sqlite3_snprintf(sizeof(zBuf), zBuf,
          "SQLITE_DONE: rbu update completed (%lld operations)\n",
          nProgress
      );
      fprintf(stdout, "%s", zBuf);
      break;

    default:
      fprintf(stderr, "error=%d: %s\n", rc, zErrmsg);
      break;
  }

  sqlite3_free(zErrmsg);
  return (rc==SQLITE_OK || rc==SQLITE_DONE) ? 0 : 1;
}
