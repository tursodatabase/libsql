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
** This file contains code to implement a pseudo-random number
** generator (PRNG) for SQLite.
**
** Random numbers are used by some of the database backends in order
** to generate random integer keys for tables or random filenames.
**
** $Id: random.c,v 1.27 2008/10/07 15:25:48 drh Exp $
*/
#include "sqliteInt.h"


/* All threads share a single random number generator.
** This structure is the current state of the generator.
*/
static SQLITE_WSD struct sqlite3PrngType {
  unsigned char isInit;          /* True if initialized */
  unsigned char i, j;            /* State variables */
  unsigned char s[256];          /* State variables */
} sqlite3Prng = { 0, };

/*
** Get a single 8-bit random value from the RC4 PRNG.  The Mutex
** must be held while executing this routine.
**
** Why not just use a library random generator like lrand48() for this?
** Because the OP_NewRowid opcode in the VDBE depends on having a very
** good source of random numbers.  The lrand48() library function may
** well be good enough.  But maybe not.  Or maybe lrand48() has some
** subtle problems on some systems that could cause problems.  It is hard
** to know.  To minimize the risk of problems due to bad lrand48()
** implementations, SQLite uses this random number generator based
** on RC4, which we know works very well.
**
** (Later):  Actually, OP_NewRowid does not depend on a good source of
** randomness any more.  But we will leave this code in all the same.
*/
static int randomByte(void){
  unsigned char t;


  /* The "wsdPrng" macro will resolve to the pseudo-random number generator
  ** state vector.  If writable static data is unsupported on the target,
  ** we have to locate the state vector at run-time.  In the more common
  ** case where writable static data is supported, wsdPrng can refer directly
  ** to the "sqlite3Prng" state vector declared above.
  */
#ifdef SQLITE_OMIT_WSD
  struct sqlite3PrngType *p = &GLOBAL(struct sqlite3PrngType, sqlite3Prng);
# define wsdPrng p[0]
#else
# define wsdPrng sqlite3Prng
#endif


  /* Initialize the state of the random number generator once,
  ** the first time this routine is called.  The seed value does
  ** not need to contain a lot of randomness since we are not
  ** trying to do secure encryption or anything like that...
  **
  ** Nothing in this file or anywhere else in SQLite does any kind of
  ** encryption.  The RC4 algorithm is being used as a PRNG (pseudo-random
  ** number generator) not as an encryption device.
  */
  if( !wsdPrng.isInit ){
    int i;
    char k[256];
    wsdPrng.j = 0;
    wsdPrng.i = 0;
    sqlite3OsRandomness(sqlite3_vfs_find(0), 256, k);
    for(i=0; i<256; i++){
      wsdPrng.s[i] = i;
    }
    for(i=0; i<256; i++){
      wsdPrng.j += wsdPrng.s[i] + k[i];
      t = wsdPrng.s[wsdPrng.j];
      wsdPrng.s[wsdPrng.j] = wsdPrng.s[i];
      wsdPrng.s[i] = t;
    }
    wsdPrng.isInit = 1;
  }

  /* Generate and return single random byte
  */
  wsdPrng.i++;
  t = wsdPrng.s[wsdPrng.i];
  wsdPrng.j += t;
  wsdPrng.s[wsdPrng.i] = wsdPrng.s[wsdPrng.j];
  wsdPrng.s[wsdPrng.j] = t;
  t += wsdPrng.s[wsdPrng.i];
  return wsdPrng.s[t];
}

/*
** Return N random bytes.
*/
void sqlite3_randomness(int N, void *pBuf){
  unsigned char *zBuf = pBuf;
#if SQLITE_THREADSAFE
  sqlite3_mutex *mutex = sqlite3MutexAlloc(SQLITE_MUTEX_STATIC_PRNG);
#endif
  sqlite3_mutex_enter(mutex);
  while( N-- ){
    *(zBuf++) = randomByte();
  }
  sqlite3_mutex_leave(mutex);
}

#ifndef SQLITE_OMIT_BUILTIN_TEST
/*
** For testing purposes, we sometimes want to preserve the state of
** PRNG and restore the PRNG to its saved state at a later time, or
** to reset the PRNG to its initial state.  These routines accomplish
** those tasks.
**
** The sqlite3_test_control() interface calls these routines to
** control the PRNG.
*/
static SQLITE_WSD struct sqlite3PrngType sqlite3SavedPrng = { 0, };
void sqlite3PrngSaveState(void){
  memcpy(
    &GLOBAL(struct sqlite3PrngType, sqlite3SavedPrng),
    &GLOBAL(struct sqlite3PrngType, sqlite3Prng),
    sizeof(sqlite3Prng)
  );
}
void sqlite3PrngRestoreState(void){
  memcpy(
    &GLOBAL(struct sqlite3PrngType, sqlite3Prng),
    &GLOBAL(struct sqlite3PrngType, sqlite3SavedPrng),
    sizeof(sqlite3Prng)
  );
}
void sqlite3PrngResetState(void){
  GLOBAL(struct sqlite3PrngType, sqlite3Prng).isInit = 0;
}
#endif /* SQLITE_OMIT_BUILTIN_TEST */
