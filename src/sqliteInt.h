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
** Internal interface definitions for SQLite.
**
** @(#) $Id: sqliteInt.h,v 1.84 2002/02/02 18:49:21 drh Exp $
*/
#include "sqlite.h"
#include "hash.h"
#include "vdbe.h"
#include "parse.h"
#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

/*
** The maximum number of in-memory pages to use for the main database
** table and for temporary tables.
*/
#define MAX_PAGES   100
#define TEMP_PAGES   25

/*
** File format version number
*/
#define FILE_FORMAT 1

/*
** Integers of known sizes.  These typedefs might change for architectures
** where the sizes very.  Preprocessor macros are available so that the
** types can be conveniently redefined at compile-type.  Like this:
**
**         cc '-DUINTPTR_TYPE=long long int' ...
*/
#ifndef UINT32_TYPE
# define UINT32_TYPE unsigned int
#endif
#ifndef UINT16_TYPE
# define UINT16_TYPE unsigned short int
#endif
#ifndef UINT8_TYPE
# define UINT8_TYPE unsigned char
#endif
#ifndef INTPTR_TYPE
# define INTPTR_TYPE int
#endif
typedef UINT32_TYPE u32;           /* 4-byte unsigned integer */
typedef UINT16_TYPE u16;           /* 2-byte unsigned integer */
typedef UINT8_TYPE u8;             /* 1-byte unsigned integer */
typedef INTPTR_TYPE ptr;           /* Big enough to hold a pointer */
typedef unsigned INTPTR_TYPE uptr; /* Big enough to hold a pointer */

/*
** This macro casts a pointer to an integer.  Useful for doing
** pointer arithmetic.
*/
#define Addr(X)  ((uptr)X)

/*
** The maximum number of bytes of data that can be put into a single
** row of a single table.  The upper bound on this limit is 16777215
** bytes (or 16MB-1).  We have arbitrarily set the limit to just 1MB
** here because the overflow page chain is inefficient for really big
** records and we want to discourage people from thinking that 
** multi-megabyte records are OK.  If your needs are different, you can
** change this define and recompile to increase or decrease the record
** size.
*/
#define MAX_BYTES_PER_ROW  1048576

/*
** If memory allocation problems are found, recompile with
**
**      -DMEMORY_DEBUG=1
**
** to enable some sanity checking on malloc() and free().  To
** check for memory leaks, recompile with
**
**      -DMEMORY_DEBUG=2
**
** and a line of text will be written to standard error for
** each malloc() and free().  This output can be analyzed
** by an AWK script to determine if there are any leaks.
*/
#ifdef MEMORY_DEBUG
# define sqliteMalloc(X)    sqliteMalloc_(X,__FILE__,__LINE__)
# define sqliteFree(X)      sqliteFree_(X,__FILE__,__LINE__)
# define sqliteRealloc(X,Y) sqliteRealloc_(X,Y,__FILE__,__LINE__)
# define sqliteStrDup(X)    sqliteStrDup_(X,__FILE__,__LINE__)
# define sqliteStrNDup(X,Y) sqliteStrNDup_(X,Y,__FILE__,__LINE__)
  void sqliteStrRealloc(char**);
#else
# define sqliteStrRealloc(X)
#endif

/*
** This variable gets set if malloc() ever fails.  After it gets set,
** the SQLite library shuts down permanently.
*/
extern int sqlite_malloc_failed;

/*
** The following global variables are used for testing and debugging
** only.  They only work if MEMORY_DEBUG is defined.
*/
#ifdef MEMORY_DEBUG
extern int sqlite_nMalloc;       /* Number of sqliteMalloc() calls */
extern int sqlite_nFree;         /* Number of sqliteFree() calls */
extern int sqlite_iMallocFail;   /* Fail sqliteMalloc() after this many calls */
#endif

/*
** Name of the master database table.  The master database table
** is a special table that holds the names and attributes of all
** user tables and indices.
*/
#define MASTER_NAME   "sqlite_master"

/*
** A convenience macro that returns the number of elements in
** an array.
*/
#define ArraySize(X)    (sizeof(X)/sizeof(X[0]))

/*
** Integer identifiers for built-in SQL functions.
*/
#define FN_Unknown    0
#define FN_Count      1
#define FN_Min        2
#define FN_Max        3
#define FN_Sum        4
#define FN_Avg        5
#define FN_Fcnt       6
#define FN_Length     7
#define FN_Substr     8
#define FN_Abs        9
#define FN_Round      10

/*
** Forward references to structures
*/
typedef struct Column Column;
typedef struct Table Table;
typedef struct Index Index;
typedef struct Instruction Instruction;
typedef struct Expr Expr;
typedef struct ExprList ExprList;
typedef struct Parse Parse;
typedef struct Token Token;
typedef struct IdList IdList;
typedef struct WhereInfo WhereInfo;
typedef struct WhereLevel WhereLevel;
typedef struct Select Select;
typedef struct AggExpr AggExpr;

/*
** Each database is an instance of the following structure
*/
struct sqlite {
  Btree *pBe;                   /* The B*Tree backend */
  Btree *pBeTemp;               /* Backend for session temporary tables */
  int flags;                    /* Miscellanous flags. See below */
  int file_format;              /* What file format version is this database? */
  int schema_cookie;            /* Magic number that changes with the schema */
  int next_cookie;              /* Value of schema_cookie after commit */
  int nTable;                   /* Number of tables in the database */
  void *pBusyArg;               /* 1st Argument to the busy callback */
  int (*xBusyCallback)(void *,const char*,int);  /* The busy callback */
  Hash tblHash;                 /* All tables indexed by name */
  Hash idxHash;                 /* All (named) indices indexed by name */
  Hash tblDrop;                 /* Uncommitted DROP TABLEs */
  Hash idxDrop;                 /* Uncommitted DROP INDEXs */
  int lastRowid;                /* ROWID of most recent insert */
  int nextRowid;                /* Next generated rowID */
  int onError;                  /* Default conflict algorithm */
};

/*
** Possible values for the sqlite.flags.
*/
#define SQLITE_VdbeTrace      0x00000001  /* True to trace VDBE execution */
#define SQLITE_Initialized    0x00000002  /* True after initialization */
#define SQLITE_Interrupt      0x00000004  /* Cancel current operation */
#define SQLITE_InTrans        0x00000008  /* True if in a transaction */
#define SQLITE_InternChanges  0x00000010  /* Uncommitted Hash table changes */
#define SQLITE_FullColNames   0x00000020  /* Show full column names on SELECT */
#define SQLITE_CountRows      0x00000040  /* Count rows changed by INSERT, */
                                          /*   DELETE, or UPDATE and return */
                                          /*   the count using a callback. */
#define SQLITE_NullCallback   0x00000080  /* Invoke the callback once if the */
                                          /*   result set is empty */
#define SQLITE_ResultDetails  0x00000100  /* Details added to result set */

/*
** Current file format version
*/
#define SQLITE_FileFormat 2

/*
** information about each column of an SQL table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;     /* Name of this column */
  char *zDflt;     /* Default value of this column */
  char *zType;     /* Data type for this column */
  u8 notNull;      /* True if there is a NOT NULL constraint */
  u8 isPrimKey;    /* True if this column is an INTEGER PRIMARY KEY */
};

/*
** Each SQL table is represented in memory by
** an instance of the following structure.
*/
struct Table {
  char *zName;     /* Name of the table */
  int nCol;        /* Number of columns in this table */
  Column *aCol;    /* Information about each column */
  int iPKey;       /* If not less then 0, use aCol[iPKey] as the primary key */
  Index *pIndex;   /* List of SQL indexes on this table. */
  int tnum;        /* Page containing root for this table */
  u8 readOnly;     /* True if this table should not be written by the user */
  u8 isCommit;     /* True if creation of this table has been committed */
  u8 isTemp;       /* True if stored in db->pBeTemp instead of db->pBe */
  u8 hasPrimKey;   /* True if there exists a primary key */
  u8 keyConf;      /* What to do in case of uniqueness conflict on iPKey */
};

/*
** SQLite supports 4 or 5 different ways to resolve a contraint
** error.  (Only 4 are implemented as of this writing.  The fifth method
** "ABORT" is planned.)  ROLLBACK processing means that a constraint violation
** causes the operation in proces to fail and for the current transaction
** to be rolled back.  ABORT processing means the operation in process
** fails and any prior changes from that one operation are backed out,
** but the transaction is not rolled back.  FAIL processing means that
** the operation in progress stops and returns an error code.  But prior
** changes due to the same operation are not backed out and no rollback
** occurs.  IGNORE means that the particular row that caused the constraint
** error is not inserted or updated.  Processing continues and no error
** is returned.  REPLACE means that preexisting database rows that caused
** a UNIQUE constraint violation are removed so that the new insert or
** update can proceed.  Processing continues and no error is reported.
** 
** The following there symbolic values are used to record which type
** of action to take.
*/
#define OE_None     0   /* There is no constraint to check */
#define OE_Rollback 1   /* Fail the operation and rollback the transaction */
#define OE_Abort    2   /* Back out changes but do no rollback transaction */
#define OE_Fail     3   /* Stop the operation but leave all prior changes */
#define OE_Ignore   4   /* Ignore the error. Do not do the INSERT or UPDATE */
#define OE_Replace  5   /* Delete existing record, then do INSERT or UPDATE */
#define OE_Default  9   /* Do whatever the default action is */

/*
** Each SQL index is represented in memory by an
** instance of the following structure.
**
** The columns of the table that are to be indexed are described
** by the aiColumn[] field of this structure.  For example, suppose
** we have the following table and index:
**
**     CREATE TABLE Ex1(c1 int, c2 int, c3 text);
**     CREATE INDEX Ex2 ON Ex1(c3,c1);
**
** In the Table structure describing Ex1, nCol==3 because there are
** three columns in the table.  In the Index structure describing
** Ex2, nColumn==2 since 2 of the 3 columns of Ex1 are indexed.
** The value of aiColumn is {2, 0}.  aiColumn[0]==2 because the 
** first column to be indexed (c3) has an index of 2 in Ex1.aCol[].
** The second column to be indexed (c1) has an index of 0 in
** Ex1.aCol[], hence Ex2.aiColumn[1]==0.
*/
struct Index {
  char *zName;     /* Name of this index */
  int nColumn;     /* Number of columns in the table used by this index */
  int *aiColumn;   /* Which columns are used by this index.  1st is 0 */
  Table *pTable;   /* The SQL table being indexed */
  int tnum;        /* Page containing root of this index in database file */
  u8 isUnique;     /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  u8 isCommit;     /* True if creation of this index has been committed */
  u8 isDropped;    /* True if a DROP INDEX has executed on this index */
  u8 onError;      /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
  Index *pNext;    /* The next index associated with the same table */
};

/*
** Each token coming out of the lexer is an instance of
** this structure.
*/
struct Token {
  const char *z;      /* Text of the token.  Not NULL-terminated! */
  int n;              /* Number of characters in this token */
};

/*
** Each node of an expression in the parse tree is an instance
** of this structure
*/
struct Expr {
  int op;                /* Operation performed by this node */
  Expr *pLeft, *pRight;  /* Left and right subnodes */
  ExprList *pList;       /* A list of expressions used as a function argument */
  Token token;           /* An operand token */
  Token span;            /* Complete text of the expression */
  int iTable, iColumn;   /* When op==TK_COLUMN, then this expr node means the
                         ** iColumn-th field of the iTable-th table.  When
                         ** op==TK_FUNCTION, iColumn holds the function id */
  int iAgg;              /* When op==TK_COLUMN and pParse->useAgg==TRUE, pull
                         ** result from the iAgg-th element of the aggregator */
  Select *pSelect;       /* When the expression is a sub-select */
};

/*
** A list of expressions.  Each expression may optionally have a
** name.  An expr/name combination can be used in several ways, such
** as the list of "expr AS ID" fields following a "SELECT" or in the
** list of "ID = expr" items in an UPDATE.  A list of expressions can
** also be used as the argument to a function, in which case the azName
** field is not used.
*/
struct ExprList {
  int nExpr;             /* Number of expressions on the list */
  struct ExprList_item {
    Expr *pExpr;           /* The list of expressions */
    char *zName;           /* Token associated with this expression */
    char sortOrder;        /* 1 for DESC or 0 for ASC */
    char isAgg;            /* True if this is an aggregate like count(*) */
    char done;             /* A flag to indicate when processing is finished */
  } *a;                  /* One entry for each expression */
};

/*
** A list of identifiers.
*/
struct IdList {
  int nId;         /* Number of identifiers on the list */
  struct IdList_item {
    char *zName;      /* Text of the identifier. */
    char *zAlias;     /* The "B" part of a "A AS B" phrase.  zName is the "A" */
    int idx;          /* Index in some Table.aCol[] of a column named zName */
    Table *pTab;      /* An SQL table corresponding to zName */
    Select *pSelect;  /* A SELECT statement used in place of a table name */
  } *a;            /* One entry for each identifier on the list */
};

/*
** For each nested loop in a WHERE clause implementation, the WhereInfo
** structure contains a single instance of this structure.  This structure
** is intended to be private the the where.c module and should not be
** access or modified by other modules.
*/
struct WhereLevel {
  int iMem;            /* Memory cell used by this level */
  Index *pIdx;         /* Index used */
  int iCur;            /* Cursor number used for this index */
  int score;           /* How well this indexed scored */
  int brk;             /* Jump here to break out of the loop */
  int cont;            /* Jump here to continue with the next loop cycle */
  int op, p1, p2;      /* Opcode used to terminate the loop */
};

/*
** The WHERE clause processing routine has two halves.  The
** first part does the start of the WHERE loop and the second
** half does the tail of the WHERE loop.  An instance of
** this structure is returned by the first half and passed
** into the second half to give some continuity.
*/
struct WhereInfo {
  Parse *pParse;
  IdList *pTabList;    /* List of tables in the join */
  int iContinue;       /* Jump here to continue with next record */
  int iBreak;          /* Jump here to break out of the loop */
  int base;            /* Index of first Open opcode */
  int nLevel;          /* Number of nested loop */
  WhereLevel a[1];     /* Information about each nest loop in the WHERE */
};

/*
** An instance of the following structure contains all information
** needed to generate code for a single SELECT statement.
*/
struct Select {
  int isDistinct;        /* True if the DISTINCT keyword is present */
  ExprList *pEList;      /* The fields of the result */
  IdList *pSrc;          /* The FROM clause */
  Expr *pWhere;          /* The WHERE clause */
  ExprList *pGroupBy;    /* The GROUP BY clause */
  Expr *pHaving;         /* The HAVING clause */
  ExprList *pOrderBy;    /* The ORDER BY clause */
  int op;                /* One of: TK_UNION TK_ALL TK_INTERSECT TK_EXCEPT */
  Select *pPrior;        /* Prior select in a compound select statement */
  int nLimit, nOffset;   /* LIMIT and OFFSET values.  -1 means not used */
};

/*
** The results of a select can be distributed in several ways.
*/
#define SRT_Callback     1  /* Invoke a callback with each row of result */
#define SRT_Mem          2  /* Store result in a memory cell */
#define SRT_Set          3  /* Store result as unique keys in a table */
#define SRT_Union        5  /* Store result as keys in a table */
#define SRT_Except       6  /* Remove result from a UNION table */
#define SRT_Table        7  /* Store result as data with a unique key */

/*
** When a SELECT uses aggregate functions (like "count(*)" or "avg(f1)")
** we have to do some additional analysis of expressions.  An instance
** of the following structure holds information about a single subexpression
** somewhere in the SELECT statement.  An array of these structures holds
** all the information we need to generate code for aggregate
** expressions.
**
** Note that when analyzing a SELECT containing aggregates, both
** non-aggregate field variables and aggregate functions are stored
** in the AggExpr array of the Parser structure.
**
** The pExpr field points to an expression that is part of either the
** field list, the GROUP BY clause, the HAVING clause or the ORDER BY
** clause.  The expression will be freed when those clauses are cleaned
** up.  Do not try to delete the expression attached to AggExpr.pExpr.
**
** If AggExpr.pExpr==0, that means the expression is "count(*)".
*/
struct AggExpr {
  int isAgg;        /* if TRUE contains an aggregate function */
  Expr *pExpr;      /* The expression */
};

/*
** An SQL parser context.  A copy of this structure is passed through
** the parser and down into all the parser action routine in order to
** carry around information that is global to the entire parse.
*/
struct Parse {
  sqlite *db;          /* The main database structure */
  Btree *pBe;          /* The database backend */
  int rc;              /* Return code from execution */
  sqlite_callback xCallback;  /* The callback function */
  void *pArg;          /* First argument to the callback function */
  char *zErrMsg;       /* An error message */
  Token sErrToken;     /* The token at which the error occurred */
  Token sFirstToken;   /* The first token parsed */
  Token sLastToken;    /* The last token parsed */
  Table *pNewTable;    /* A table being constructed by CREATE TABLE */
  Vdbe *pVdbe;         /* An engine for executing database bytecode */
  int colNamesSet;     /* TRUE after OP_ColumnCount has been issued to pVdbe */
  int explain;         /* True if the EXPLAIN flag is found on the query */
  int initFlag;        /* True if reparsing CREATE TABLEs */
  int nameClash;       /* A permanent table name clashes with temp table name */
  int newTnum;         /* Table number to use when reparsing CREATE TABLEs */
  int nErr;            /* Number of errors seen */
  int nTab;            /* Number of previously allocated cursors */
  int nMem;            /* Number of memory cells used so far */
  int nSet;            /* Number of sets used so far */
  int nAgg;            /* Number of aggregate expressions */
  AggExpr *aAgg;       /* An array of aggregate expressions */
  int iAggCount;       /* Index of the count(*) aggregate in aAgg[] */
  int useAgg;          /* If true, extract field values from the aggregator
                       ** while generating expressions.  Normally false */
  int schemaVerified;  /* True if an OP_VerifySchema has been coded someplace
                       ** other than after an OP_Transaction */
};

/*
** Internal function prototypes
*/
int sqliteStrICmp(const char *, const char *);
int sqliteStrNICmp(const char *, const char *, int);
int sqliteHashNoCase(const char *, int);
int sqliteCompare(const char *, const char *);
int sqliteSortCompare(const char *, const char *);
void sqliteRealToSortable(double r, char *);
#ifdef MEMORY_DEBUG
  void *sqliteMalloc_(int,char*,int);
  void sqliteFree_(void*,char*,int);
  void *sqliteRealloc_(void*,int,char*,int);
  char *sqliteStrDup_(const char*,char*,int);
  char *sqliteStrNDup_(const char*, int,char*,int);
#else
  void *sqliteMalloc(int);
  void sqliteFree(void*);
  void *sqliteRealloc(void*,int);
  char *sqliteStrDup(const char*);
  char *sqliteStrNDup(const char*, int);
#endif
void sqliteSetString(char **, const char *, ...);
void sqliteSetNString(char **, ...);
void sqliteDequote(char*);
int sqliteRunParser(Parse*, const char*, char **);
void sqliteExec(Parse*);
Expr *sqliteExpr(int, Expr*, Expr*, Token*);
void sqliteExprSpan(Expr*,Token*,Token*);
Expr *sqliteExprFunction(ExprList*, Token*);
void sqliteExprDelete(Expr*);
ExprList *sqliteExprListAppend(ExprList*,Expr*,Token*);
void sqliteExprListDelete(ExprList*);
void sqlitePragma(Parse*,Token*,Token*,int);
void sqliteCommitInternalChanges(sqlite*);
void sqliteRollbackInternalChanges(sqlite*);
void sqliteStartTable(Parse*,Token*,Token*,int);
void sqliteAddColumn(Parse*,Token*);
void sqliteAddNotNull(Parse*, int);
void sqliteAddPrimaryKey(Parse*, IdList*, int);
void sqliteAddColumnType(Parse*,Token*,Token*);
void sqliteAddDefaultValue(Parse*,Token*,int);
void sqliteEndTable(Parse*,Token*);
void sqliteDropTable(Parse*, Token*);
void sqliteDeleteTable(sqlite*, Table*);
void sqliteInsert(Parse*, Token*, ExprList*, Select*, IdList*, int);
IdList *sqliteIdListAppend(IdList*, Token*);
void sqliteIdListAddAlias(IdList*, Token*);
void sqliteIdListDelete(IdList*);
void sqliteCreateIndex(Parse*, Token*, Token*, IdList*, int, Token*, Token*);
void sqliteDropIndex(Parse*, Token*);
int sqliteSelect(Parse*, Select*, int, int);
Select *sqliteSelectNew(ExprList*,IdList*,Expr*,ExprList*,Expr*,ExprList*,
                        int,int,int);
void sqliteSelectDelete(Select*);
void sqliteDeleteFrom(Parse*, Token*, Expr*);
void sqliteUpdate(Parse*, Token*, ExprList*, Expr*, int);
WhereInfo *sqliteWhereBegin(Parse*, IdList*, Expr*, int);
void sqliteWhereEnd(WhereInfo*);
void sqliteExprCode(Parse*, Expr*);
void sqliteExprIfTrue(Parse*, Expr*, int);
void sqliteExprIfFalse(Parse*, Expr*, int);
Table *sqliteFindTable(sqlite*,char*);
Index *sqliteFindIndex(sqlite*,char*);
void sqliteUnlinkAndDeleteIndex(sqlite*,Index*);
void sqliteCopy(Parse*, Token*, Token*, Token*, int);
void sqliteVacuum(Parse*, Token*);
int sqliteGlobCompare(const unsigned char*,const unsigned char*);
int sqliteLikeCompare(const unsigned char*,const unsigned char*);
char *sqliteTableNameFromToken(Token*);
int sqliteExprCheck(Parse*, Expr*, int, int*);
int sqliteExprCompare(Expr*, Expr*);
int sqliteFuncId(Token*);
int sqliteExprResolveIds(Parse*, IdList*, ExprList*, Expr*);
void sqliteExprResolveInSelect(Parse*, Expr*);
int sqliteExprAnalyzeAggregates(Parse*, Expr*);
void sqliteParseInfoReset(Parse*);
Vdbe *sqliteGetVdbe(Parse*);
int sqliteRandomByte(void);
int sqliteRandomInteger(void);
void sqliteBeginTransaction(Parse*, int);
void sqliteCommitTransaction(Parse*);
void sqliteRollbackTransaction(Parse*);
char *sqlite_mprintf(const char *, ...);
int sqliteExprIsConstant(Expr*);
void sqliteGenerateRowDelete(Vdbe*, Table*, int);
void sqliteGenerateRowIndexDelete(Vdbe*, Table*, int, char*);
void sqliteGenerateConstraintChecks(Parse*,Table*,int,char*,int,int,int,int);
void sqliteCompleteInsertion(Parse*, Table*, int, char*, int, int);
void sqliteBeginWriteOperation(Parse*);
void sqliteBeginMultiWriteOperation(Parse*);
void sqliteEndWriteOperation(Parse*);
