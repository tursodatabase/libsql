/*
** 2019-10-02
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Header file for the lookaside allocator
**
** This header defines the interface to the lookaside allocator.
** The lookaside allocator implements a two-size memory allocator using a
** buffer provided at initialization-time to exploit the fact that 75% of
** SQLite's allocations are <=128B.
*/
#ifndef SQLITE_LOOKASIDE_H
#define SQLITE_LOOKASIDE_H

/*
** Count the number of slots of lookaside memory that are outstanding
*/
int sqlite3LookasideUsed(Lookaside *pLookaside, int *pHighwater);

void sqlite3LookasideResetUsed(Lookaside *pLookaside);

#define sqlite3LookasideDisable(pLookaside) do{(pLookaside)->bDisable++;\
  (pLookaside)->sz=0;}while(0)
#define sqlite3LookasideEnable(pLookaside) do{(pLookaside)->bDisable--;\
  (pLookaside)->sz=(pLookaside)->bDisable?0:(pLookaside)->szTrue;} while(0)
#define sqlite3LookasideEnableCnt(pLookaside, CNT) do{(pLookaside)->bDisable -= (CNT);\
  (pLookaside)->sz=(pLookaside)->bDisable?0:(pLookaside)->szTrue;} while(0)
#define sqlite3LookasideDisabled(pLookaside) ((pLookaside)->bDisable)

# ifndef SQLITE_OMIT_LOOKASIDE

/*
** Set up a lookaside allocator.
** Returns SQLITE_OK on success.
** If lookaside is already active, return SQLITE_BUSY.
**
** If pStart is NULL the space for the lookaside memory is obtained from
** sqlite3_malloc(). If pStart is not NULL then it is sz*cnt bytes of memory
** to use for the lookaside memory.
*/
int sqlite3LookasideOpen(
  void *pBuf,           /* NULL or sz*cnt bytes of memory */
  int sz,               /* Number of bytes in each lookaside slot */
  int cnt,              /* Number of slots */
  Lookaside *pLookaside /* Preallocated space for the Lookaside */
);

/* Reset and close the lookaside object */
void sqlite3LookasideClose(Lookaside *pLookaside);

/*
** Returns TRUE if p is a lookaside memory allocation from db
*/
int sqlite3IsLookaside(Lookaside *pLookaside, void *p);

/*
** Returns a pointer to a region at least n bytes in size, or NULL if the
** lookaside allocator has exhausted its available memory.
*/
void *sqlite3LookasideAlloc(Lookaside *pLookaside, u64 n);

/*
** Free memory previously obtained from sqlite3LookasideAlloc().
*/
void sqlite3LookasideFree(Lookaside *pLookaside, void *p);

/*
** Return the size of a memory allocation previously obtained from
** sqlite3LookasideAlloc().
*/
int sqlite3LookasideSize(Lookaside *pLookaside, void *p);

# else
#  define sqlite3LookasideOpen(A, B, C, D) SQLITE_OK
#  define sqlite3LookasideClose(A)
#  define sqlite3IsLookaside(A, B) 0
#  define sqlite3LookasideAlloc(A, B) 0
#  define sqlite3LookasideFree(A, B) assert(0);
#  define sqlite3LookasideSize(A, B) -1
# endif

#endif /* SQLITE_LOOKASIDE_H */
