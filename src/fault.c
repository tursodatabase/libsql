/*
** 2008 Jan 22
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code to implement a fault-injector used for
** testing and verification of SQLite.
**
** Subsystems within SQLite can call sqlite3FaultStep() to see if
** they should simulate a fault.  sqlite3FaultStep() normally returns
** zero but will return non-zero if a fault should be simulated.
** Fault injectors can be used, for example, to simulate memory
** allocation failures or I/O errors.
**
** The fault injector is omitted from the code if SQLite is
** compiled with -DSQLITE_OMIT_BUILTIN_TEST=1.  There is a very
** small performance hit for leaving the fault injector in the code.
** Commerical products will probably want to omit the fault injector
** from production builds.  But safety-critical systems who work
** under the motto "fly what you test and test what you fly" may
** choose to leave the fault injector enabled even in production.
**
** $Id: fault.c,v 1.7 2008/06/19 18:17:50 danielk1977 Exp $
*/
#include "sqliteInt.h"

/*
** There can be various kinds of faults.  For example, there can be
** a memory allocation failure.  Or an I/O failure.  For each different
** fault type, there is a separate FaultInjector structure to keep track
** of the status of that fault.
*/
static struct MemFault {
  int iCountdown;   /* Number of pending successes before we hit a failure */
  int nRepeat;      /* Number of times to repeat the failure */
  int nBenign;      /* Number of benign failures seen since last config */
  int nFail;        /* Number of failures seen since last config */
  u8 enable;        /* True if enabled */
  i16 benign;       /* Positive if next failure will be benign */

  int isInstalled;
  sqlite3_mem_methods m;         /* 'Real' malloc implementation */
} memfault;

/*
** This routine exists as a place to set a breakpoint that will
** fire on any simulated malloc() failure.
*/
static void sqlite3Fault(void){
  static int cnt = 0;
  cnt++;
}

/*
** Check to see if a fault should be simulated.  Return true to simulate
** the fault.  Return false if the fault should not be simulated.
*/
static int faultsimStep(){
  if( likely(!memfault.enable) ){
    return 0;
  }
  if( memfault.iCountdown>0 ){
    memfault.iCountdown--;
    return 0;
  }
  sqlite3Fault();
  memfault.nFail++;
  if( memfault.benign>0 ){
    memfault.nBenign++;
  }
  memfault.nRepeat--;
  if( memfault.nRepeat<=0 ){
    memfault.enable = 0;
  }
  return 1;  
}

static void *faultsimMalloc(int n){
  void *p = 0;
  if( !faultsimStep() ){
    p = memfault.m.xMalloc(n);
  }
  return p;
}


static void *faultsimRealloc(void *pOld, int n){
  void *p = 0;
  if( !faultsimStep() ){
    p = memfault.m.xRealloc(pOld, n);
  }
  return p;
}

/* 
** The following method calls are passed directly through to the underlying
** malloc system:
**
**     xFree
**     xSize
**     xRoundup
**     xInit
**     xShutdown
*/
static void faultsimFree(void *p){
  memfault.m.xFree(p);
}
static int faultsimSize(void *p){
  return memfault.m.xSize(p);
}
static int faultsimRoundup(int n){
  return memfault.m.xRoundup(n);
}
static int faultsimInit(void *p){
  return memfault.m.xInit(memfault.m.pAppData);
}
static void faultsimShutdown(void *p){
  memfault.m.xShutdown(memfault.m.pAppData);
}

/*
** This routine configures and enables a fault injector.  After
** calling this routine, a FaultStep() will return false (zero)
** nDelay times, then it will return true nRepeat times,
** then it will again begin returning false.
*/
void sqlite3FaultConfig(int id, int nDelay, int nRepeat){
  memfault.iCountdown = nDelay;
  memfault.nRepeat = nRepeat;
  memfault.nBenign = 0;
  memfault.nFail = 0;
  memfault.enable = nDelay>=0;
  memfault.benign = 0;
}

/*
** Return the number of faults (both hard and benign faults) that have
** occurred since the injector was last configured.
*/
int sqlite3FaultFailures(int id){
  assert( id>=0 && id<SQLITE_FAULTINJECTOR_COUNT );
  return memfault.nFail;
}

/*
** Return the number of benign faults that have occurred since the
** injector was last configured.
*/
int sqlite3FaultBenignFailures(int id){
  return memfault.nBenign;
}

/*
** Return the number of successes that will occur before the next failure.
** If no failures are scheduled, return -1.
*/
int sqlite3FaultPending(int id){
  if( memfault.enable ){
    return memfault.iCountdown;
  }else{
    return -1;
  }
}

/* 
** After this routine causes subsequent faults to be either benign
** or hard (not benign), according to the "enable" parameter.
**
** Most faults are hard.  In other words, most faults cause
** an error to be propagated back up to the application interface.
** However, sometimes a fault is easily recoverable.  For example,
** if a malloc fails while resizing a hash table, this is completely
** recoverable simply by not carrying out the resize.  The hash table
** will continue to function normally.  So a malloc failure during
** a hash table resize is a benign fault.  
*/
void sqlite3FaultBeginBenign(int id){
  if( id<0 ){
    for(id=0; id<SQLITE_FAULTINJECTOR_COUNT; id++){
      memfault.benign++;
    }
  }else{
    assert( id>=0 && id<SQLITE_FAULTINJECTOR_COUNT );
    memfault.benign++;
  }
}
void sqlite3FaultEndBenign(int id){
  if( id<0 ){
    for(id=0; id<SQLITE_FAULTINJECTOR_COUNT; id++){
      assert( memfault.benign>0 );
      memfault.benign--;
    }
  }else{
    assert( memfault.benign>0 );
    memfault.benign--;
  }
}

int sqlite3FaultsimInstall(int install){
  static struct sqlite3_mem_methods m = {
    faultsimMalloc,                   /* xMalloc */
    faultsimFree,                     /* xFree */
    faultsimRealloc,                  /* xRealloc */
    faultsimSize,                     /* xSize */
    faultsimRoundup,                  /* xRoundup */
    faultsimInit,                     /* xInit */
    faultsimShutdown,                 /* xShutdown */
    0                                 /* pAppData */
  };
  int rc;

  assert(install==1 || install==0);
  assert(memfault.isInstalled==1 || memfault.isInstalled==0);

  if( install==memfault.isInstalled ){
    return SQLITE_ERROR;
  }

  rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &memfault.m);
  assert(memfault.m.xMalloc);
  if( rc==SQLITE_OK ){
    rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &m);
  }

  if( rc==SQLITE_OK ){
    memfault.isInstalled = 1;
  }
  return rc;
}

