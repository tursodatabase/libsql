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
** This file contains SQLite's grammar for SQL.  Process this file
** using the lemon parser generator to generate C code that runs
** the parser.  Lemon will also generate a header file containing
** numeric codes for all of the tokens.
**
** @(#) $Id: parse.y,v 1.28 2001/04/11 14:28:42 drh Exp $
*/
%token_prefix TK_
%token_type {Token}
%extra_argument {Parse *pParse}
%syntax_error {
  sqliteSetString(&pParse->zErrMsg,"syntax error",0);
  pParse->sErrToken = TOKEN;
}
%name sqliteParser
%include {
#include "sqliteInt.h"
#include "parse.h"
}

// These are extra tokens used by the lexer but never seen by the
// parser.  We put them in a rule so that the parser generator will
// add them to the parse.h output file.
//
%nonassoc END_OF_FILE ILLEGAL SPACE UNCLOSED_STRING COMMENT FUNCTION
          COLUMN AGG_FUNCTION.

// Input is zero or more commands.
input ::= cmdlist.

// A list of commands is zero or more commands
//
cmdlist ::= ecmd.
cmdlist ::= cmdlist SEMI ecmd.
ecmd ::= explain cmd.  {sqliteExec(pParse);}
ecmd ::= cmd.          {sqliteExec(pParse);}
ecmd ::= .
explain ::= EXPLAIN.    {pParse->explain = 1;}

// Begin and end transactions.  Transaction support is sparse.
// Some backends support only COMMIT and not ROLLBACK.  There can
// be only a single active transaction at a time.
//
cmd ::= BEGIN trans_opt.       {sqliteBeginTransaction(pParse);}
trans_opt ::= .
trans_opt ::= TRANSACTION.
trans_opt ::= TRANSACTION ids.
cmd ::= COMMIT trans_opt.      {sqliteCommitTransaction(pParse);}
cmd ::= END trans_opt.         {sqliteCommitTransaction(pParse);}
cmd ::= ROLLBACK trans_opt.    {sqliteRollbackTransaction(pParse);}

// The first form of a command is a CREATE TABLE statement.
//
cmd ::= create_table create_table_args.
create_table ::= CREATE(X) TABLE ids(Y).   {sqliteStartTable(pParse,&X,&Y);}
create_table_args ::= LP columnlist conslist_opt RP(X).
                                           {sqliteEndTable(pParse,&X);}
columnlist ::= columnlist COMMA column.
columnlist ::= column.

// About the only information used for a column is the name of the
// column.  The type is always just "text".  But the code will accept
// an elaborate typename.  Perhaps someday we'll do something with it.
//
column ::= columnid type carglist. 
columnid ::= ids(X).                {sqliteAddColumn(pParse,&X);}

// An IDENTIFIER can be a generic identifier, or one of several
// keywords.  Any non-standard keyword can also be an identifier.
// We also make DESC and identifier since it comes up so often.
//
%type id {Token}
id(A) ::= DESC(X).       {A = X;}
id(A) ::= ASC(X).        {A = X;}
id(A) ::= DELIMITERS(X). {A = X;}
id(A) ::= EXPLAIN(X).    {A = X;}
id(A) ::= VACUUM(X).     {A = X;}
id(A) ::= BEGIN(X).      {A = X;}
id(A) ::= END(X).        {A = X;}
id(A) ::= ID(X).         {A = X;}

// And "ids" is an identifer-or-string.
//
%type ids {Token}
ids(A) ::= id(X).        {A = X;}
ids(A) ::= STRING(X).    {A = X;}

type ::= typename.
type ::= typename LP signed RP.
type ::= typename LP signed COMMA signed RP.
typename ::= ids.
typename ::= typename ids.
signed ::= INTEGER.
signed ::= PLUS INTEGER.
signed ::= MINUS INTEGER.
carglist ::= carglist carg.
carglist ::= .
carg ::= CONSTRAINT ids ccons.
carg ::= ccons.
carg ::= DEFAULT STRING(X).          {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT ID(X).              {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT INTEGER(X).         {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT PLUS INTEGER(X).    {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT MINUS INTEGER(X).   {sqliteAddDefaultValue(pParse,&X,1);}
carg ::= DEFAULT FLOAT(X).           {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT PLUS FLOAT(X).      {sqliteAddDefaultValue(pParse,&X,0);}
carg ::= DEFAULT MINUS FLOAT(X).     {sqliteAddDefaultValue(pParse,&X,1);}
carg ::= DEFAULT NULL. 

// In addition to the type name, we also care about the primary key.
//
ccons ::= NOT NULL.
ccons ::= PRIMARY KEY sortorder.     {sqliteCreateIndex(pParse,0,0,0,0,0);}
ccons ::= UNIQUE.
ccons ::= CHECK LP expr RP.

// For the time being, the only constraint we care about is the primary
// key.
//
conslist_opt ::= .
conslist_opt ::= COMMA conslist.
conslist ::= conslist COMMA tcons.
conslist ::= conslist tcons.
conslist ::= tcons.
tcons ::= CONSTRAINT ids.
tcons ::= PRIMARY KEY LP idxlist(X) RP. {sqliteCreateIndex(pParse,0,0,X,0,0);}
tcons ::= UNIQUE LP idlist RP.
tcons ::= CHECK expr.
idlist ::= idlist COMMA ids.
idlist ::= ids.

// The next command format is dropping tables.
//
cmd ::= DROP TABLE ids(X).          {sqliteDropTable(pParse,&X);}

// The select statement
//
cmd ::= select(X).  {
  sqliteSelect(pParse, X, SRT_Callback, 0);
  sqliteSelectDelete(X);
}

%type select {Select*}
%destructor select {sqliteSelectDelete($$);}
%type oneselect {Select*}
%destructor oneselect {sqliteSelectDelete($$);}

select(A) ::= oneselect(X).                      {A = X;}
select(A) ::= select(X) joinop(Y) oneselect(Z).  {
  if( Z ){
    Z->op = Y;
    Z->pPrior = X;
  }
  A = Z;
}
%type joinop {int}
joinop(A) ::= UNION.      {A = TK_UNION;}
joinop(A) ::= UNION ALL.  {A = TK_ALL;}
joinop(A) ::= INTERSECT.  {A = TK_INTERSECT;}
joinop(A) ::= EXCEPT.     {A = TK_EXCEPT;}
oneselect(A) ::= SELECT distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z). {
  A = sqliteSelectNew(W,X,Y,P,Q,Z,D);
}

// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {int}
distinct(A) ::= DISTINCT.   {A = 1;}
distinct(A) ::= ALL.        {A = 0;}
distinct(A) ::= .           {A = 0;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  In the case of "SELECT * FROM ..."
// the selcollist value is NULL.  
//
%type selcollist {ExprList*}
%destructor selcollist {sqliteExprListDelete($$);}
%type sclp {ExprList*}
%destructor sclp {sqliteExprListDelete($$);}
sclp(A) ::= selcollist(X) COMMA.             {A = X;}
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= STAR.                      {A = 0;}
selcollist(A) ::= sclp(P) expr(X).           {A = sqliteExprListAppend(P,X,0);}
selcollist(A) ::= sclp(P) expr(X) as ids(Y). {A = sqliteExprListAppend(P,X,&Y);}
as ::= .
as ::= AS.


%type seltablist {IdList*}
%destructor seltablist {sqliteIdListDelete($$);}
%type stl_prefix {IdList*}
%destructor stl_prefix {sqliteIdListDelete($$);}
%type from {IdList*}
%destructor from {sqliteIdListDelete($$);}

from(A) ::= FROM seltablist(X).               {A = X;}
stl_prefix(A) ::= seltablist(X) COMMA.        {A = X;}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(X) ids(Y).       {A = sqliteIdListAppend(X,&Y);}
seltablist(A) ::= stl_prefix(X) ids(Y) as ids(Z). {
  A = sqliteIdListAppend(X,&Y);
  sqliteIdListAddAlias(A,&Z);
}

%type orderby_opt {ExprList*}
%destructor orderby_opt {sqliteExprListDelete($$);}
%type sortlist {ExprList*}
%destructor sortlist {sqliteExprListDelete($$);}
%type sortitem {Expr*}
%destructor sortitem {sqliteExprDelete($$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}
sortlist(A) ::= sortlist(X) COMMA sortitem(Y) sortorder(Z). {
  A = sqliteExprListAppend(X,Y,0);
  if( A ) A->a[A->nExpr-1].sortOrder = Z;  /* 0=ascending, 1=decending */
}
sortlist(A) ::= sortitem(Y) sortorder(Z). {
  A = sqliteExprListAppend(0,Y,0);
  if( A ) A->a[0].sortOrder = Z;
}
sortitem(A) ::= expr(X).   {A = X;}

%type sortorder {int}

sortorder(A) ::= ASC.      {A = 0;}
sortorder(A) ::= DESC.     {A = 1;}
sortorder(A) ::= .         {A = 0;}

%type groupby_opt {ExprList*}
%destructor groupby_opt {sqliteExprListDelete($$);}
groupby_opt(A) ::= .                      {A = 0;}
groupby_opt(A) ::= GROUP BY exprlist(X).  {A = X;}

%type having_opt {Expr*}
%destructor having_opt {sqliteExprDelete($$);}
having_opt(A) ::= .                {A = 0;}
having_opt(A) ::= HAVING expr(X).  {A = X;}


cmd ::= DELETE FROM ids(X) where_opt(Y).
    {sqliteDeleteFrom(pParse, &X, Y);}

%type where_opt {Expr*}
%destructor where_opt {sqliteExprDelete($$);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X;}

%type setlist {ExprList*}
%destructor setlist {sqliteExprListDelete($$);}

cmd ::= UPDATE ids(X) SET setlist(Y) where_opt(Z).
    {sqliteUpdate(pParse,&X,Y,Z);}

setlist(A) ::= setlist(Z) COMMA ids(X) EQ expr(Y).
    {A = sqliteExprListAppend(Z,Y,&X);}
setlist(A) ::= ids(X) EQ expr(Y).   {A = sqliteExprListAppend(0,Y,&X);}

cmd ::= INSERT INTO ids(X) inscollist_opt(F) VALUES LP itemlist(Y) RP.
               {sqliteInsert(pParse, &X, Y, 0, F);}
cmd ::= INSERT INTO ids(X) inscollist_opt(F) select(S).
               {sqliteInsert(pParse, &X, 0, S, F);}


%type itemlist {ExprList*}
%destructor itemlist {sqliteExprListDelete($$);}
%type item {Expr*}
%destructor item {sqliteExprDelete($$);}

itemlist(A) ::= itemlist(X) COMMA item(Y).  {A = sqliteExprListAppend(X,Y,0);}
itemlist(A) ::= item(X).     {A = sqliteExprListAppend(0,X,0);}
item(A) ::= INTEGER(X).      {A = sqliteExpr(TK_INTEGER, 0, 0, &X);}
item(A) ::= PLUS INTEGER(X). {A = sqliteExpr(TK_INTEGER, 0, 0, &X);}
item(A) ::= MINUS INTEGER(X). {
  A = sqliteExpr(TK_UMINUS, 0, 0, 0);
  if( A ) A->pLeft = sqliteExpr(TK_INTEGER, 0, 0, &X);
}
item(A) ::= FLOAT(X).        {A = sqliteExpr(TK_FLOAT, 0, 0, &X);}
item(A) ::= PLUS FLOAT(X).   {A = sqliteExpr(TK_FLOAT, 0, 0, &X);}
item(A) ::= MINUS FLOAT(X).  {
  A = sqliteExpr(TK_UMINUS, 0, 0, 0);
  if( A ) A->pLeft = sqliteExpr(TK_FLOAT, 0, 0, &X);
}
item(A) ::= STRING(X).       {A = sqliteExpr(TK_STRING, 0, 0, &X);}
item(A) ::= NULL.            {A = sqliteExpr(TK_NULL, 0, 0, 0);}

%type inscollist_opt {IdList*}
%destructor inscollist_opt {sqliteIdListDelete($$);}
%type inscollist {IdList*}
%destructor inscollist {sqliteIdListDelete($$);}

inscollist_opt(A) ::= .                       {A = 0;}
inscollist_opt(A) ::= LP inscollist(X) RP.    {A = X;}
inscollist(A) ::= inscollist(X) COMMA ids(Y). {A = sqliteIdListAppend(X,&Y);}
inscollist(A) ::= ids(Y).                     {A = sqliteIdListAppend(0,&Y);}

%left OR.
%left AND.
%right NOT.
%left EQ NE ISNULL NOTNULL IS LIKE GLOB BETWEEN IN.
%left GT GE LT LE.
%left PLUS MINUS.
%left STAR SLASH.
%left CONCAT.
%right UMINUS.

%type expr {Expr*}
%destructor expr {sqliteExprDelete($$);}

expr(A) ::= LP(B) expr(X) RP(E). {A = X; sqliteExprSpan(A,&B,&E);}
expr(A) ::= NULL(X).             {A = sqliteExpr(TK_NULL, 0, 0, &X);}
expr(A) ::= id(X).               {A = sqliteExpr(TK_ID, 0, 0, &X);}
expr(A) ::= ids(X) DOT ids(Y). {
  Expr *temp1 = sqliteExpr(TK_ID, 0, 0, &X);
  Expr *temp2 = sqliteExpr(TK_ID, 0, 0, &Y);
  A = sqliteExpr(TK_DOT, temp1, temp2, 0);
}
expr(A) ::= INTEGER(X).      {A = sqliteExpr(TK_INTEGER, 0, 0, &X);}
expr(A) ::= FLOAT(X).        {A = sqliteExpr(TK_FLOAT, 0, 0, &X);}
expr(A) ::= STRING(X).       {A = sqliteExpr(TK_STRING, 0, 0, &X);}
expr(A) ::= ID(X) LP exprlist(Y) RP(E). {
  A = sqliteExprFunction(Y, &X);
  sqliteExprSpan(A,&X,&E);
}
expr(A) ::= ID(X) LP STAR RP(E). {
  A = sqliteExprFunction(0, &X);
  sqliteExprSpan(A,&X,&E);
}
expr(A) ::= expr(X) AND expr(Y).   {A = sqliteExpr(TK_AND, X, Y, 0);}
expr(A) ::= expr(X) OR expr(Y).    {A = sqliteExpr(TK_OR, X, Y, 0);}
expr(A) ::= expr(X) LT expr(Y).    {A = sqliteExpr(TK_LT, X, Y, 0);}
expr(A) ::= expr(X) GT expr(Y).    {A = sqliteExpr(TK_GT, X, Y, 0);}
expr(A) ::= expr(X) LE expr(Y).    {A = sqliteExpr(TK_LE, X, Y, 0);}
expr(A) ::= expr(X) GE expr(Y).    {A = sqliteExpr(TK_GE, X, Y, 0);}
expr(A) ::= expr(X) NE expr(Y).    {A = sqliteExpr(TK_NE, X, Y, 0);}
expr(A) ::= expr(X) EQ expr(Y).    {A = sqliteExpr(TK_EQ, X, Y, 0);}
expr(A) ::= expr(X) LIKE expr(Y).  {A = sqliteExpr(TK_LIKE, X, Y, 0);}
expr(A) ::= expr(X) NOT LIKE expr(Y).  {
  A = sqliteExpr(TK_LIKE, X, Y, 0);
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&X->span,&Y->span);
}
expr(A) ::= expr(X) GLOB expr(Y).  {A = sqliteExpr(TK_GLOB,X,Y,0);}
expr(A) ::= expr(X) NOT GLOB expr(Y).  {
  A = sqliteExpr(TK_GLOB, X, Y, 0);
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&X->span,&Y->span);
}
expr(A) ::= expr(X) PLUS expr(Y).  {A = sqliteExpr(TK_PLUS, X, Y, 0);}
expr(A) ::= expr(X) MINUS expr(Y). {A = sqliteExpr(TK_MINUS, X, Y, 0);}
expr(A) ::= expr(X) STAR expr(Y).  {A = sqliteExpr(TK_STAR, X, Y, 0);}
expr(A) ::= expr(X) SLASH expr(Y). {A = sqliteExpr(TK_SLASH, X, Y, 0);}
expr(A) ::= expr(X) CONCAT expr(Y). {A = sqliteExpr(TK_CONCAT, X, Y, 0);}
expr(A) ::= expr(X) ISNULL(E). {
  A = sqliteExpr(TK_ISNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) NOTNULL(E). {
  A = sqliteExpr(TK_NOTNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= NOT(B) expr(X). {
  A = sqliteExpr(TK_NOT, X, 0, 0);
  sqliteExprSpan(A,&B,&X->span);
}
expr(A) ::= MINUS(B) expr(X). [UMINUS] {
  A = sqliteExpr(TK_UMINUS, X, 0, 0);
  sqliteExprSpan(A,&B,&X->span);
}
expr(A) ::= PLUS(B) expr(X). [UMINUS] {
  A = X;
  sqliteExprSpan(A,&B,&X->span);
}
expr(A) ::= LP(B) select(X) RP(E). {
  A = sqliteExpr(TK_SELECT, 0, 0, 0);
  if( A ) A->pSelect = X;
  sqliteExprSpan(A,&B,&E);
}
expr(A) ::= expr(W) BETWEEN expr(X) AND expr(Y). {
  ExprList *pList = sqliteExprListAppend(0, X, 0);
  pList = sqliteExprListAppend(pList, Y, 0);
  A = sqliteExpr(TK_BETWEEN, W, 0, 0);
  if( A ) A->pList = pList;
  sqliteExprSpan(A,&W->span,&Y->span);
}
expr(A) ::= expr(W) NOT BETWEEN expr(X) AND expr(Y). {
  ExprList *pList = sqliteExprListAppend(0, X, 0);
  pList = sqliteExprListAppend(pList, Y, 0);
  A = sqliteExpr(TK_BETWEEN, W, 0, 0);
  if( A ) A->pList = pList;
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&W->span,&Y->span);
}
expr(A) ::= expr(X) IN LP exprlist(Y) RP(E).  {
  A = sqliteExpr(TK_IN, X, 0, 0);
  if( A ) A->pList = Y;
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) IN LP select(Y) RP(E).  {
  A = sqliteExpr(TK_IN, X, 0, 0);
  if( A ) A->pSelect = Y;
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) NOT IN LP exprlist(Y) RP(E).  {
  A = sqliteExpr(TK_IN, X, 0, 0);
  if( A ) A->pList = Y;
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) NOT IN LP select(Y) RP(E).  {
  A = sqliteExpr(TK_IN, X, 0, 0);
  if( A ) A->pSelect = Y;
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}



%type exprlist {ExprList*}
%destructor exprlist {sqliteExprListDelete($$);}
%type expritem {Expr*}
%destructor expritem {sqliteExprDelete($$);}

exprlist(A) ::= exprlist(X) COMMA expritem(Y). 
   {A = sqliteExprListAppend(X,Y,0);}
exprlist(A) ::= expritem(X).            {A = sqliteExprListAppend(0,X,0);}
expritem(A) ::= expr(X).                {A = X;}
expritem(A) ::= .                       {A = 0;}


cmd ::= CREATE(S) uniqueflag INDEX ids(X) ON ids(Y) LP idxlist(Z) RP(E).
    {sqliteCreateIndex(pParse, &X, &Y, Z, &S, &E);}
uniqueflag ::= UNIQUE.
uniqueflag ::= .

%type idxlist {IdList*}
%destructor idxlist {sqliteIdListDelete($$);}
%type idxitem {Token}

idxlist(A) ::= idxlist(X) COMMA idxitem(Y).  
     {A = sqliteIdListAppend(X,&Y);}
idxlist(A) ::= idxitem(Y).
     {A = sqliteIdListAppend(0,&Y);}
idxitem(A) ::= ids(X).          {A = X;}

cmd ::= DROP INDEX ids(X).      {sqliteDropIndex(pParse, &X);}

cmd ::= COPY ids(X) FROM ids(Y) USING DELIMITERS STRING(Z).
    {sqliteCopy(pParse,&X,&Y,&Z);}
cmd ::= COPY ids(X) FROM ids(Y).
    {sqliteCopy(pParse,&X,&Y,0);}

cmd ::= VACUUM.                {sqliteVacuum(pParse,0);}
cmd ::= VACUUM ids(X).         {sqliteVacuum(pParse,&X);}
