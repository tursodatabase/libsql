// SPDX-License-Identifier: MIT

#ifndef LIBSQL_PAGE_HEADER_H
#define LIBSQL_PAGE_HEADER_H

typedef struct sqlite3_pcache_page sqlite3_pcache_page;
typedef struct Pager Pager;
typedef struct PgHdr PgHdr;
typedef struct PCache PCache;

/*
** Every page in the cache is controlled by an instance of the following
** structure.
*/
struct PgHdr {
  sqlite3_pcache_page *pPage;    /* Pcache object page handle */
  void *pData;                   /* Page data */
  void *pExtra;                  /* Extra content */
  PCache *pCache;                /* PRIVATE: Cache that owns this page */
  PgHdr *pDirty;                 /* Transient list of dirty sorted by pgno */
  Pager *pPager;                 /* The pager this page is part of */
  unsigned int pgno;             /* Page number for this page */
#ifdef SQLITE_CHECK_PAGES
  unsigned int pageHash;         /* Hash of page content */
#endif
  unsigned short flags;          /* PGHDR flags defined below */

  /**********************************************************************
  ** Elements above, except pCache, are public.  All that follow are 
  ** private to pcache.c and should not be accessed by other modules.
  ** pCache is grouped with the public elements for efficiency.
  */
  short nRef;                    /* Number of users of this page */
  PgHdr *pDirtyNext;             /* Next element in list of dirty pages */
  PgHdr *pDirtyPrev;             /* Previous element in list of dirty pages */
                          /* NB: pDirtyNext and pDirtyPrev are undefined if the
                          ** PgHdr object is not dirty */
};

#endif // LIBSQL_PAGE_HEADER_H
