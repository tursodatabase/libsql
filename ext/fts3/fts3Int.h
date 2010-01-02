/*
** 2009 Nov 12
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
*/

#ifndef _FTSINT_H
#define _FTSINT_H

#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif

#include "sqlite3.h"
#include "fts3_tokenizer.h"
#include "fts3_hash.h"

/*
** This constant controls how often segments are merged. Once there are
** FTS3_MERGE_COUNT segments of level N, they are merged into a single
** segment of level N+1.
*/
#define FTS3_MERGE_COUNT 16

/*
** This is the maximum amount of data (in bytes) to store in the 
** Fts3Table.pendingTerms hash table. Normally, the hash table is
** populated as documents are inserted/updated/deleted in a transaction
** and used to create a new segment when the transaction is committed.
** However if this limit is reached midway through a transaction, a new 
** segment is created and the hash table cleared immediately.
*/
#define FTS3_MAX_PENDING_DATA (1*1024*1024)

/*
** Macro to return the number of elements in an array. SQLite has a
** similar macro called ArraySize(). Use a different name to avoid
** a collision when building an amalgamation with built-in FTS3.
*/
#define SizeofArray(X) ((int)(sizeof(X)/sizeof(X[0])))

/*
** Maximum length of a varint encoded integer. The varint format is different
** from that used by SQLite, so the maximum length is 10, not 9.
*/
#define FTS3_VARINT_MAX 10

/*
** This section provides definitions to allow the
** FTS3 extension to be compiled outside of the 
** amalgamation.
*/
#ifndef SQLITE_AMALGAMATION
/*
** Macros indicating that conditional expressions are always true or
** false.
*/
# define ALWAYS(x) (x)
# define NEVER(X)  (x)
/*
** Internal types used by SQLite.
*/
typedef unsigned char u8;         /* 1-byte (or larger) unsigned integer */
typedef short int i16;            /* 2-byte (or larger) signed integer */
typedef unsigned int u32;         /* 4-byte unsigned integer */
typedef sqlite3_uint64 u64;       /* 8-byte unsigned integer */
/*
** Macro used to suppress compiler warnings for unused parameters.
*/
#define UNUSED_PARAMETER(x) (void)(x)
#endif

typedef struct Fts3Table Fts3Table;
typedef struct Fts3Cursor Fts3Cursor;
typedef struct Fts3Expr Fts3Expr;
typedef struct Fts3Phrase Fts3Phrase;
typedef struct Fts3SegReader Fts3SegReader;
typedef struct Fts3SegFilter Fts3SegFilter;

/*
** A connection to a fulltext index is an instance of the following
** structure. The xCreate and xConnect methods create an instance
** of this structure and xDestroy and xDisconnect free that instance.
** All other methods receive a pointer to the structure as one of their
** arguments.
*/
struct Fts3Table {
  sqlite3_vtab base;              /* Base class used by SQLite core */
  sqlite3 *db;                    /* The database connection */
  const char *zDb;                /* logical database name */
  const char *zName;              /* virtual table name */
  int nColumn;                    /* number of named columns in virtual table */
  char **azColumn;                /* column names.  malloced */
  sqlite3_tokenizer *pTokenizer;  /* tokenizer for inserts and queries */

  /* Precompiled statements used by the implementation. Each of these 
  ** statements is run and reset within a single virtual table API call. 
  */
  sqlite3_stmt *aStmt[18];

  /* Pointer to string containing the SQL:
  **
  ** "SELECT block FROM %_segments WHERE blockid BETWEEN ? AND ? 
  **    ORDER BY blockid"
  */
  char *zSelectLeaves;
  int nLeavesStmt;                /* Valid statements in aLeavesStmt */
  int nLeavesTotal;               /* Total number of prepared leaves stmts */
  int nLeavesAlloc;               /* Allocated size of aLeavesStmt */
  sqlite3_stmt **aLeavesStmt;     /* Array of prepared zSelectLeaves stmts */

  int nNodeSize;                  /* Soft limit for node size */

  /* The following hash table is used to buffer pending index updates during
  ** transactions. Variable nPendingData estimates the memory size of the 
  ** pending data, including hash table overhead, but not malloc overhead. 
  ** When nPendingData exceeds nMaxPendingData, the buffer is flushed 
  ** automatically. Variable iPrevDocid is the docid of the most recently
  ** inserted record.
  */
  int nMaxPendingData;
  int nPendingData;
  sqlite_int64 iPrevDocid;
  Fts3Hash pendingTerms;
};

/*
** When the core wants to read from the virtual table, it creates a
** virtual table cursor (an instance of the following structure) using
** the xOpen method. Cursors are destroyed using the xClose method.
*/
struct Fts3Cursor {
  sqlite3_vtab_cursor base;       /* Base class used by SQLite core */
  i16 eSearch;                    /* Search strategy (see below) */
  u8 isEof;                       /* True if at End Of Results */
  u8 isRequireSeek;               /* True if must seek pStmt to %_content row */
  sqlite3_stmt *pStmt;            /* Prepared statement in use by the cursor */
  Fts3Expr *pExpr;                /* Parsed MATCH query string */
  sqlite3_int64 iPrevId;          /* Previous id read from aDoclist */
  char *pNextId;                  /* Pointer into the body of aDoclist */
  char *aDoclist;                 /* List of docids for full-text queries */
  int nDoclist;                   /* Size of buffer at aDoclist */
  int isMatchinfoOk;              /* True when aMatchinfo[] matches iPrevId */
  u32 *aMatchinfo;
};

/*
** The Fts3Cursor.eSearch member is always set to one of the following.
** Actualy, Fts3Cursor.eSearch can be greater than or equal to
** FTS3_FULLTEXT_SEARCH.  If so, then Fts3Cursor.eSearch - 2 is the index
** of the column to be searched.  For example, in
**
**     CREATE VIRTUAL TABLE ex1 USING fts3(a,b,c,d);
**     SELECT docid FROM ex1 WHERE b MATCH 'one two three';
** 
** Because the LHS of the MATCH operator is 2nd column "b",
** Fts3Cursor.eSearch will be set to FTS3_FULLTEXT_SEARCH+1.  (+0 for a,
** +1 for b, +2 for c, +3 for d.)  If the LHS of MATCH were "ex1" 
** indicating that all columns should be searched,
** then eSearch would be set to FTS3_FULLTEXT_SEARCH+4.
*/
#define FTS3_FULLSCAN_SEARCH 0    /* Linear scan of %_content table */
#define FTS3_DOCID_SEARCH    1    /* Lookup by rowid on %_content table */
#define FTS3_FULLTEXT_SEARCH 2    /* Full-text index search */

/*
** A "phrase" is a sequence of one or more tokens that must match in
** sequence.  A single token is the base case and the most common case.
** For a sequence of tokens contained in "...", nToken will be the number
** of tokens in the string.
*/
struct Fts3Phrase {
  int nToken;                /* Number of tokens in the phrase */
  int iColumn;               /* Index of column this phrase must match */
  int isNot;                 /* Phrase prefixed by unary not (-) operator */
  struct PhraseToken {
    char *z;                 /* Text of the token */
    int n;                   /* Number of bytes in buffer pointed to by z */
    int isPrefix;            /* True if token ends in with a "*" character */
  } aToken[1];               /* One entry for each token in the phrase */
};

/*
** A tree of these objects forms the RHS of a MATCH operator.
**
** If Fts3Expr.eType is either FTSQUERY_NEAR or FTSQUERY_PHRASE and isLoaded
** is true, then aDoclist points to a malloced buffer, size nDoclist bytes, 
** containing the results of the NEAR or phrase query in FTS3 doclist
** format. As usual, the initial "Length" field found in doclists stored
** on disk is omitted from this buffer.
**
** Variable pCurrent always points to the start of a docid field within
** aDoclist. Since the doclist is usually scanned in docid order, this can
** be used to accelerate seeking to the required docid within the doclist.
*/
struct Fts3Expr {
  int eType;                 /* One of the FTSQUERY_XXX values defined below */
  int nNear;                 /* Valid if eType==FTSQUERY_NEAR */
  Fts3Expr *pParent;         /* pParent->pLeft==this or pParent->pRight==this */
  Fts3Expr *pLeft;           /* Left operand */
  Fts3Expr *pRight;          /* Right operand */
  Fts3Phrase *pPhrase;       /* Valid if eType==FTSQUERY_PHRASE */

  int isLoaded;              /* True if aDoclist/nDoclist are initialized. */
  char *aDoclist;            /* Buffer containing doclist */
  int nDoclist;              /* Size of aDoclist in bytes */

  sqlite3_int64 iCurrent;
  char *pCurrent;
};

/*
** Candidate values for Fts3Query.eType. Note that the order of the first
** four values is in order of precedence when parsing expressions. For 
** example, the following:
**
**   "a OR b AND c NOT d NEAR e"
**
** is equivalent to:
**
**   "a OR (b AND (c NOT (d NEAR e)))"
*/
#define FTSQUERY_NEAR   1
#define FTSQUERY_NOT    2
#define FTSQUERY_AND    3
#define FTSQUERY_OR     4
#define FTSQUERY_PHRASE 5


/* fts3_init.c */
int sqlite3Fts3DeleteVtab(int, sqlite3_vtab *);
int sqlite3Fts3InitVtab(int, sqlite3*, void*, int, const char*const*, 
                        sqlite3_vtab **, char **);

/* fts3_write.c */
int sqlite3Fts3UpdateMethod(sqlite3_vtab*,int,sqlite3_value**,sqlite3_int64*);
int sqlite3Fts3PendingTermsFlush(Fts3Table *);
void sqlite3Fts3PendingTermsClear(Fts3Table *);
int sqlite3Fts3Optimize(Fts3Table *);
int sqlite3Fts3SegReaderNew(Fts3Table *,int, sqlite3_int64,
  sqlite3_int64, sqlite3_int64, const char *, int, Fts3SegReader**);
int sqlite3Fts3SegReaderPending(Fts3Table*,const char*,int,int,Fts3SegReader**);
void sqlite3Fts3SegReaderFree(Fts3Table *, Fts3SegReader *);
int sqlite3Fts3SegReaderIterate(
  Fts3Table *, Fts3SegReader **, int, Fts3SegFilter *,
  int (*)(Fts3Table *, void *, char *, int, char *, int),  void *
);
int sqlite3Fts3ReadBlock(Fts3Table*, sqlite3_int64, char const**, int*);
int sqlite3Fts3AllSegdirs(Fts3Table*, sqlite3_stmt **);

/* Flags allowed as part of the 4th argument to SegmentReaderIterate() */
#define FTS3_SEGMENT_REQUIRE_POS   0x00000001
#define FTS3_SEGMENT_IGNORE_EMPTY  0x00000002
#define FTS3_SEGMENT_COLUMN_FILTER 0x00000004
#define FTS3_SEGMENT_PREFIX        0x00000008

/* Type passed as 4th argument to SegmentReaderIterate() */
struct Fts3SegFilter {
  const char *zTerm;
  int nTerm;
  int iCol;
  int flags;
};

/* fts3.c */
int sqlite3Fts3PutVarint(char *, sqlite3_int64);
int sqlite3Fts3GetVarint(const char *, sqlite_int64 *);
int sqlite3Fts3GetVarint32(const char *, int *);
int sqlite3Fts3VarintLen(sqlite3_uint64);
void sqlite3Fts3Dequote(char *);

char *sqlite3Fts3FindPositions(Fts3Expr *, sqlite3_int64, int);
int sqlite3Fts3ExprLoadDoclist(Fts3Table *, Fts3Expr *);

/* fts3_tokenizer.c */
const char *sqlite3Fts3NextToken(const char *, int *);
int sqlite3Fts3InitHashTable(sqlite3 *, Fts3Hash *, const char *);
int sqlite3Fts3InitTokenizer(Fts3Hash *pHash, 
  const char *, sqlite3_tokenizer **, const char **, char **
);

/* fts3_snippet.c */
void sqlite3Fts3Offsets(sqlite3_context*, Fts3Cursor*);
void sqlite3Fts3Snippet(sqlite3_context*, Fts3Cursor*, 
  const char *, const char *, const char *
);
void sqlite3Fts3Snippet2(sqlite3_context *, Fts3Cursor *, const char *,
  const char *, const char *, int, int
);
void sqlite3Fts3Matchinfo(sqlite3_context *, Fts3Cursor *);

/* fts3_expr.c */
int sqlite3Fts3ExprParse(sqlite3_tokenizer *, 
  char **, int, int, const char *, int, Fts3Expr **
);
void sqlite3Fts3ExprFree(Fts3Expr *);
#ifdef SQLITE_TEST
int sqlite3Fts3ExprInitTestInterface(sqlite3 *db);
#endif

#endif /* _FTSINT_H */
