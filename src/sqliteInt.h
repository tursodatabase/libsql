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
** @(#) $Id: sqliteInt.h,v 1.11 2000/06/03 18:06:53 drh Exp $
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

/* #define MEMORY_DEBUG 2 */
#ifdef MEMORY_DEBUG
# define sqliteMalloc(X)    sqliteMalloc_(X,__FILE__,__LINE__)
# define sqliteFree(X)      sqliteFree_(X,__FILE__,__LINE__)
# define sqliteRealloc(X,Y) sqliteRealloc_(X,Y,__FILE__,__LINE__)
  void sqliteStrRealloc(char**);
#else
# define sqliteStrRealloc(X)
#endif

/*
** The number of entries in the in-memory hash table holding the
** schema.
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
** Integer identifiers for functions.
*/
#define FN_Unknown    0
#define FN_Count      1
#define FN_Min        2
#define FN_Max        3
#define FN_Sum        4
#define FN_Avg        5

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

/*
** Each database is an instance of the following structure
*/
struct sqlite {
  Dbbe *pBe;                 /* The backend driver */
  int flags;                 /* Miscellanous flags */
  Table *apTblHash[N_HASH];  /* All tables of the database */
  Index *apIdxHash[N_HASH];  /* All indices of the database */
};

/*
** Possible values for the flags field of sqlite
*/
#define SQLITE_VdbeTrace    0x00000001
#define SQLITE_Initialized  0x00000002

/*
** information about each column of a table is held in an instance
** of this structure.
*/
struct Column {
  char *zName;        /* Name of this column */
  char *zDflt;        /* Default value of this column */
  int notNull;        /* True if there is a NOT NULL constraing */
};

/*
** Each table is represented in memory by
** an instance of the following structure
*/
struct Table {
  char *zName;        /* Name of the table */
  Table *pHash;       /* Next table with same hash on zName */
  int nCol;           /* Number of columns in this table */
  Column *aCol;       /* Information about each column */
  int readOnly;       /* True if this table should not be written by the user */
  Index *pIndex;      /* List of indices on this table. */
};

/*
** Each index is represented in memory by and
** instance of the following structure.
*/
struct Index {
  char *zName;        /* Name of this index */
  Index *pHash;       /* Next index with the same hash on zName */
  int nField;         /* Number of fields in the table indexed by this index */
  int *aiField;       /* Indices of fields used by this index.  1st is 0 */
  Table *pTable;      /* The table being indexed */
  int isUnique;       /* True if keys must all be unique */
  Index *pNext;       /* The next index associated with the same table */
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
  int iTable, iField;    /* When op==TK_FIELD, then this node means the
                         ** iField-th field of the iTable-th table */
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
    int idx;               /* ... */
    int isAgg;             /* True if this is an aggregate like count(*) */
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
    Table *pTab;      /* Table corresponding to zName */
    int idx;          /* Index of a field name in the table */
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
  IdList *pTabList;
  int iContinue;
  int iBreak;
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
  int explain;         /* True if the EXPLAIN flag is found on the query */
  int initFlag;        /* True if reparsing CREATE TABLEs */
  int nErr;            /* Number of errors seen */
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
#else
  void *sqliteMalloc(int);
  void sqliteFree(void*);
  void *sqliteRealloc(void*,int);
#endif
int sqliteGetToken(const char*, int *);
void sqliteSetString(char **, const char *, ...);
void sqliteSetNString(char **, ...);
void sqliteDequote(char*);
int sqliteRunParser(Parse*, char*, char **);
void sqliteExec(Parse*);
Expr *sqliteExpr(int, Expr*, Expr*, Token*);
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
void sqliteInsert(Parse*, Token*, ExprList*, IdList*);
IdList *sqliteIdListAppend(IdList*, Token*);
void sqliteIdListAddAlias(IdList*, Token*);
void sqliteIdListDelete(IdList*);
void sqliteCreateIndex(Parse*, Token*, Token*, IdList*, Token*, Token*);
void sqliteDropIndex(Parse*, Token*);
void sqliteSelect(Parse*, ExprList*, IdList*, Expr*, ExprList*, int);
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
int sqliteFuncId(Token*);
int sqliteExprResolveIds(Parse*, IdList*, Expr*);
