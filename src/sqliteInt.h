/*
** Copyright (c) 1999, 2000 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License as published by the Free Software Foundation; either
** version 2 of the License, or (at your option) any later version.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*************************************************************************
** Internal interface definitions for SQLite.
**
** @(#) $Id: sqliteInt.h,v 1.29 2000/08/02 13:47:42 drh Exp $
*/
#include "sqlite.h"
#include "dbbe.h"
#include "vdbe.h"
#include "parse.h"
#include <gdbm.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

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
** The following global variables are used for testing and debugging
** only.  Thy only work if MEMORY_DEBUG is defined.
*/
#ifdef MEMORY_DEBUG
int sqlite_nMalloc;         /* Number of sqliteMalloc() calls */
int sqlite_nFree;           /* Number of sqliteFree() calls */
int sqlite_iMallocFail;     /* Fail sqliteMalloc() after this many calls */
#endif

/*
** The number of entries in the in-memory hash array holding the
** database schema.
*/
#define N_HASH        51

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
typedef struct Select Select;
typedef struct AggExpr AggExpr;

/*
** Each database is an instance of the following structure
*/
struct sqlite {
  Dbbe *pBe;                 /* The backend driver */
  int flags;                 /* Miscellanous flags */
  int file_format;           /* What file format version is this database? */
  int nTable;                /* Number of tables in the database */
  void *pBusyArg;            /* 1st Argument to the busy callback */
  int (*xBusyCallback)(void *,const char*,int);  /* The busy callback */
  Table *apTblHash[N_HASH];  /* All tables of the database */
  Index *apIdxHash[N_HASH];  /* All indices of the database */
};

/*
** Possible values for the sqlite.flags.
*/
#define SQLITE_VdbeTrace    0x00000001
#define SQLITE_Initialized  0x00000002

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
  int notNull;     /* True if there is a NOT NULL constraint */
};

/*
** Each SQL table is represented in memory by
** an instance of the following structure.
*/
struct Table {
  char *zName;     /* Name of the table */
  Table *pHash;    /* Next table with same hash on zName */
  int nCol;        /* Number of columns in this table */
  Column *aCol;    /* Information about each column */
  int readOnly;    /* True if this table should not be written by the user */
  Index *pIndex;   /* List of SQL indexes on this table. */
};

/*
** Each SQL index is represented in memory by and
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
  Index *pHash;    /* Next index with the same hash on zName */
  int nColumn;     /* Number of columns in the table used by this index */
  int *aiColumn;   /* Which columns are used by this index.  1st is 0 */
  Table *pTable;   /* The SQL table being indexed */
  int isUnique;    /* True if keys must all be unique */
  Index *pNext;    /* The next index associated with the same table */
};

/*
** Each token coming out of the lexer is an instance of
** this structure.
*/
struct Token {
  char *z;      /* Text of the token */
  int n;        /* Number of characters in this token */
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
  struct {
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
  struct {
    char *zName;      /* Text of the identifier. */
    char *zAlias;     /* The "B" part of a "A AS B" phrase.  zName is the "A" */
    Table *pTab;      /* An SQL table corresponding to zName */
    int idx;          /* Index in some Table.aCol[] of a column named zName */
  } *a;            /* One entry for each identifier on the list */
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
  Index *aIdx[32];     /* Indices used for each table */
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
** An SQL parser context
*/
struct Parse {
  sqlite *db;          /* The main database structure */
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
  int nErr;            /* Number of errors seen */
  int nTab;            /* Number of previously allocated cursors */
  int nMem;            /* Number of memory cells used so far */
  int nSet;            /* Number of sets used so far */
  int nAgg;            /* Number of aggregate expressions */
  AggExpr *aAgg;       /* An array of aggregate expressions */
  int iAggCount;       /* Index of the count(*) aggregate in aAgg[] */
  int useAgg;          /* If true, extract field values from the aggregator
                       ** while generating expressions.  Normally false */
};

/*
** Internal function prototypes
*/
int sqliteStrICmp(const char *, const char *);
int sqliteStrNICmp(const char *, const char *, int);
int sqliteHashNoCase(const char *, int);
int sqliteCompare(const char *, const char *);
int sqliteSortCompare(const char *, const char *);
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
int sqliteGetToken(const char*, int *);
void sqliteSetString(char **, const char *, ...);
void sqliteSetNString(char **, ...);
void sqliteDequote(char*);
int sqliteRunParser(Parse*, char*, char **);
void sqliteExec(Parse*);
Expr *sqliteExpr(int, Expr*, Expr*, Token*);
void sqliteExprSpan(Expr*,Token*,Token*);
Expr *sqliteExprFunction(ExprList*, Token*);
void sqliteExprDelete(Expr*);
ExprList *sqliteExprListAppend(ExprList*,Expr*,Token*);
void sqliteExprListDelete(ExprList*);
void sqliteStartTable(Parse*,Token*,Token*);
void sqliteAddColumn(Parse*,Token*);
void sqliteAddDefaultValue(Parse*,Token*,int);
void sqliteEndTable(Parse*,Token*);
void sqliteDropTable(Parse*, Token*);
void sqliteDeleteTable(sqlite*, Table*);
void sqliteInsert(Parse*, Token*, ExprList*, Select*, IdList*);
IdList *sqliteIdListAppend(IdList*, Token*);
void sqliteIdListAddAlias(IdList*, Token*);
void sqliteIdListDelete(IdList*);
void sqliteCreateIndex(Parse*, Token*, Token*, IdList*, Token*, Token*);
void sqliteDropIndex(Parse*, Token*);
int sqliteSelect(Parse*, Select*, int, int);
Select *sqliteSelectNew(ExprList*,IdList*,Expr*,ExprList*,Expr*,ExprList*,int);
void sqliteSelectDelete(Select*);
void sqliteDeleteFrom(Parse*, Token*, Expr*);
void sqliteUpdate(Parse*, Token*, ExprList*, Expr*);
WhereInfo *sqliteWhereBegin(Parse*, IdList*, Expr*, int);
void sqliteWhereEnd(WhereInfo*);
void sqliteExprCode(Parse*, Expr*);
void sqliteExprIfTrue(Parse*, Expr*, int);
void sqliteExprIfFalse(Parse*, Expr*, int);
Table *sqliteFindTable(sqlite*,char*);
void sqliteCopy(Parse*, Token*, Token*, Token*);
void sqliteVacuum(Parse*, Token*);
int sqliteGlobCompare(const char*,const char*);
int sqliteLikeCompare(const unsigned char*,const unsigned char*);
char *sqliteTableNameFromToken(Token*);
int sqliteExprCheck(Parse*, Expr*, int, int*);
int sqliteExprCompare(Expr*, Expr*);
int sqliteFuncId(Token*);
int sqliteExprResolveIds(Parse*, IdList*, Expr*);
void sqliteExprResolveInSelect(Parse*, Expr*);
int sqliteExprAnalyzeAggregates(Parse*, Expr*);
void sqliteParseInfoReset(Parse*);
Vdbe *sqliteGetVdbe(Parse*);
