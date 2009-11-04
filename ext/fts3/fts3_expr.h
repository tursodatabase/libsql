/*
** 2008 Nov 28
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

#include "fts3_tokenizer.h"
#include "sqlite3.h"

/*
** The following describes the syntax supported by the fts3 MATCH
** operator in a similar format to that used by the lemon parser
** generator. This module does not use actually lemon, it uses a
** custom parser.
**
**   query ::= andexpr (OR andexpr)*.
**
**   andexpr ::= notexpr (AND? notexpr)*.
**
**   notexpr ::= nearexpr (NOT nearexpr|-TOKEN)*.
**   notexpr ::= LP query RP.
**
**   nearexpr ::= phrase (NEAR distance_opt nearexpr)*.
**
**   distance_opt ::= .
**   distance_opt ::= / INTEGER.
**
**   phrase ::= TOKEN.
**   phrase ::= COLUMN:TOKEN.
**   phrase ::= "TOKEN TOKEN TOKEN...".
*/

typedef struct Fts3Expr Fts3Expr;
typedef struct Fts3Phrase Fts3Phrase;

/*
** A "phrase" is a sequence of one or more tokens that must match in
** sequence.  A single token is the base case and the most common case.
** For a sequence of tokens contained in "...", nToken will be the number
** of tokens in the string.
*/
struct Fts3Phrase {
  int nToken;          /* Number of tokens in the phrase */
  int iColumn;         /* Index of column this phrase must match */
  int isNot;           /* Phrase prefixed by unary not (-) operator */
  struct PhraseToken {
    char *z;              /* Text of the token */
    int n;                /* Number of bytes in buffer pointed to by z */
    int isPrefix;         /* True if token ends in with a "*" character */
  } aToken[1];         /* One entry for each token in the phrase */
};

/*
** A tree of these objects forms the RHS of a MATCH operator.
*/
struct Fts3Expr {
  int eType;                 /* One of the FTSQUERY_XXX values defined below */
  int nNear;                 /* Valid if eType==FTSQUERY_NEAR */
  Fts3Expr *pParent;         /* pParent->pLeft==this or pParent->pRight==this */
  Fts3Expr *pLeft;           /* Left operand */
  Fts3Expr *pRight;          /* Right operand */
  Fts3Phrase *pPhrase;       /* Valid if eType==FTSQUERY_PHRASE */
};

int sqlite3Fts3ExprParse(sqlite3_tokenizer *, char **, int, int, 
                         const char *, int, Fts3Expr **);
void sqlite3Fts3ExprFree(Fts3Expr *);

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

#ifdef SQLITE_TEST
void sqlite3Fts3ExprInitTestInterface(sqlite3 *db);
#endif
