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
** @(#) $Id: parse.y,v 1.3 2000/05/31 02:27:49 drh Exp $
*/
%token_prefix TK_
%token_type {Token}
%extra_argument {Parse *pParse}
%syntax_error {
  sqliteSetNString(&pParse->zErrMsg,"syntax error near \"",0,TOKEN.z,TOKEN.n,
                   "\"", 1, 0);
  pParse->sErrToken = TOKEN;
}
%name sqliteParser
%include {
#include "sqliteInt.h"
#include "parse.h"
}


// Input is zero or more commands.
input ::= cmdlist.

// These are extra tokens used by the lexer but never seen by the
// parser.  We put them in a rule so that the parser generator will
// add them to the sqliteTokens.h output file.
//
input ::= END_OF_FILE ILLEGAL SPACE UNCLOSED_STRING COMMENT FUNCTION
          UMINUS FIELD.

// A list of commands is zero or more commands
//
cmdlist ::= ecmd.
cmdlist ::= cmdlist SEMI ecmd.
ecmd ::= explain cmd.  {sqliteExec(pParse);}
ecmd ::= cmd.          {sqliteExec(pParse);}
ecmd ::= .
explain ::= EXPLAIN.    {pParse->explain = 1;}

// The first form of a command is a CREATE TABLE statement.
//
cmd ::= create_table create_table_args.
create_table ::= CREATE(X) TABLE id(Y).    {sqliteStartTable(pParse,&X,&Y);}
create_table_args ::= LP columnlist conslist_opt RP(X).
                                           {sqliteEndTable(pParse,&X);}
columnlist ::= columnlist COMMA column.
columnlist ::= column.

// About the only information used for a column is the name of the
// column.  The type is always just "text".  But the code will accept
// an elaborate typename.  Perhaps someday we'll do something with it.
//
column ::= columnid type carglist. 
columnid ::= id(X).                {sqliteAddColumn(pParse,&X);}
%type id {Token}
id(A) ::= ID(X).     {A = X;}
id(A) ::= STRING(X). {A = X;}
type ::= typename.
type ::= typename LP signed RP.
type ::= typename LP signed COMMA signed RP.
typename ::= ID.
typename ::= typename ID.
signed ::= INTEGER.
signed ::= PLUS INTEGER.
signed ::= MINUS INTEGER.
carglist ::= carglist carg.
carglist ::= .
carg ::= CONSTRAINT ID ccons.
carg ::= ccons.
carg ::= DEFAULT expr.

// In addition to the type name, we also care about the primary key.
//
ccons ::= NOT NULL.
ccons ::= PRIMARY KEY sortorder.  
   {sqliteCreateIndex(pParse,0,0,0,0,0);}
ccons ::= UNIQUE.
ccons ::= CHECK expr.

// For the time being, the only constraint we care about is the primary
// key.
//
conslist_opt ::= .
conslist_opt ::= COMMA conslist.
conslist ::= conslist COMMA tcons.
conslist ::= tcons.
tcons ::= CONSTRAINT ID tcons2.
tcons ::= tcons2.
tcons2 ::= PRIMARY KEY LP idxlist(X) RP.  
      {sqliteCreateIndex(pParse,0,0,X,0,0);}
tcons2 ::= UNIQUE LP idlist RP.
tcons2 ::= CHECK expr.
idlist ::= idlist COMMA id.
idlist ::= id.

// The next command format is dropping tables.
//
cmd ::= DROP TABLE id(X).          {sqliteDropTable(pParse,&X);}

// The select statement
//
cmd ::= select.
select ::= SELECT selcollist(W) from(X) where_opt(Y) orderby_opt(Z).
     {sqliteSelect(pParse, W, X, Y, Z);}
select ::= SELECT STAR from(X) where_opt(Y) orderby_opt(Z).
     {sqliteSelect(pParse, 0, X, Y, Z);}

%type selcollist {ExprList*}
%destructor selcollist {sqliteExprListDelete($$);}
%type sclp {ExprList*}
%destructor sclp {sqliteExprListDelete($$);}

sclp(A) ::= selcollist(X) COMMA.             {A = X;}
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(P) expr(X).           {A = sqliteExprListAppend(P,X,0);}
selcollist(A) ::= sclp(P) expr(X) AS ID(Y).  {A = sqliteExprListAppend(P,X,&Y);}
selcollist(A) ::= sclp(P) expr(X) AS STRING(Y).
  {A = sqliteExprListAppend(P,X,&Y);}

%type seltablist {IdList*}
%destructor seltablist {sqliteIdListDelete($$);}
%type stl_prefix {IdList*}
%destructor stl_prefix {sqliteIdListDelete($$);}
%type from {IdList*}
%destructor from {sqliteIdListDelete($$);}

from(A) ::= FROM seltablist(X).               {A = X;}
stl_prefix(A) ::= seltablist(X) COMMA.        {A = X;}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(X) id(Y).        {A = sqliteIdListAppend(X,&Y);}
seltablist(A) ::= stl_prefix(X) id(Y) AS id(Z).
   {A = sqliteIdListAppend(X,&Y);
    sqliteIdListAddAlias(A,&Z);}

%type orderby_opt {ExprList*}
%destructor orderby_opt {sqliteExprListDelete($$);}
%type sortlist {ExprList*}
%destructor sortlist {sqliteExprListDelete($$);}
%type sortitem {Expr*}
%destructor sortitem {sqliteExprDelete($$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}
sortlist(A) ::= sortlist(X) COMMA sortitem(Y) sortorder(Z).  
  {
    A = sqliteExprListAppend(X,Y,0);
    A->a[A->nExpr-1].idx = Z;
  }
sortlist(A) ::= sortitem(Y) sortorder(Z).
  {
    A = sqliteExprListAppend(0,Y,0);
    A->a[0].idx = Z;
  }
sortitem(A) ::= ID(X).           {A = sqliteExpr(TK_ID, 0, 0, &X);}
sortitem(A) ::= ID(X) DOT ID(Y). 
  {
     Expr *temp1 = sqliteExpr(TK_ID, 0, 0, &X);
     Expr *temp2 = sqliteExpr(TK_ID, 0, 0, &Y);
     A = sqliteExpr(TK_DOT, temp1, temp2, 0);
  }

%type sortorder {int}

sortorder(A) ::= ASC.      {A = 0;}
sortorder(A) ::= DESC.     {A = 1;}
sortorder(A) ::= .         {A = 0;}

cmd ::= DELETE FROM ID(X) where_opt(Y).
    {sqliteDeleteFrom(pParse, &X, Y);}

%type where_opt {Expr*}
%destructor where_opt {sqliteExprDelete($$);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X;}

%type setlist {ExprList*}
%destructor setlist {sqliteExprListDelete($$);}

cmd ::= UPDATE ID(X) SET setlist(Y) where_opt(Z).
    {sqliteUpdate(pParse,&X,Y,Z);}

setlist(A) ::= ID(X) EQ expr(Y) COMMA setlist(Z).
    {A = sqliteExprListAppend(Z,Y,&X);}
setlist(A) ::= ID(X) EQ expr(Y).   {A = sqliteExprListAppend(0,Y,&X);}

cmd ::= INSERT INTO ID(X) fieldlist_opt(F) VALUES LP itemlist(Y) RP.
               {sqliteInsert(pParse, &X, Y, F);}


%type itemlist {ExprList*}
%destructor itemlist {sqliteExprListDelete($$);}
%type item {Expr*}
%destructor item {sqliteExprDelete($$);}

itemlist(A) ::= itemlist(X) COMMA item(Y).  {A = sqliteExprListAppend(X,Y,0);}
itemlist(A) ::= item(X).     {A = sqliteExprListAppend(0,X,0);}
item(A) ::= INTEGER(X).      {A = sqliteExpr(TK_INTEGER, 0, 0, &X);}
item(A) ::= FLOAT(X).        {A = sqliteExpr(TK_FLOAT, 0, 0, &X);}
item(A) ::= STRING(X).       {A = sqliteExpr(TK_STRING, 0, 0, &X);}

%type fieldlist_opt {IdList*}
%destructor fieldlist_opt {sqliteIdListDelete($$);}
%type fieldlist {IdList*}
%destructor fieldlist {sqliteIdListDelete($$);}

fieldlist_opt(A) ::= .                    {A = 0;}
fieldlist_opt(A) ::= LP fieldlist(X) RP.  {A = X;}
fieldlist(A) ::= fieldlist(X) COMMA ID(Y). {A = sqliteIdListAppend(X,&Y);}
fieldlist(A) ::= ID(Y).                    {A = sqliteIdListAppend(0,&Y);}

%left OR.
%left AND.
%left EQ NE ISNULL NOTNULL IS LIKE GLOB.
%left GT GE LT LE.
%left PLUS MINUS.
%left STAR SLASH PERCENT.
%right NOT.

%type expr {Expr*}
%destructor expr {sqliteExprDelete($$);}

expr(A) ::= LP expr(X) RP.   {A = X;}
expr(A) ::= ID(X).           {A = sqliteExpr(TK_ID, 0, 0, &X);}
expr(A) ::= NULL.            {A = sqliteExpr(TK_NULL, 0, 0, 0);}
expr(A) ::= ID(X) DOT ID(Y). {Expr *temp1 = sqliteExpr(TK_ID, 0, 0, &X);
                              Expr *temp2 = sqliteExpr(TK_ID, 0, 0, &Y);
                              A = sqliteExpr(TK_DOT, temp1, temp2, 0);}
expr(A) ::= INTEGER(X).      {A = sqliteExpr(TK_INTEGER, 0, 0, &X);}
expr(A) ::= FLOAT(X).        {A = sqliteExpr(TK_FLOAT, 0, 0, &X);}
expr(A) ::= STRING(X).       {A = sqliteExpr(TK_STRING, 0, 0, &X);}
// expr(A) ::= ID(X) LP exprlist(Y) RP.  {A = sqliteExprFunction(Y, &X);}
// expr(A) ::= ID(X) LP STAR RP.         {A = sqliteExprFunction(0, &X);}
expr(A) ::= expr(X) AND expr(Y).   {A = sqliteExpr(TK_AND, X, Y, 0);}
expr(A) ::= expr(X) OR expr(Y).    {A = sqliteExpr(TK_OR, X, Y, 0);}
expr(A) ::= expr(X) LT expr(Y).    {A = sqliteExpr(TK_LT, X, Y, 0);}
expr(A) ::= expr(X) GT expr(Y).    {A = sqliteExpr(TK_GT, X, Y, 0);}
expr(A) ::= expr(X) LE expr(Y).    {A = sqliteExpr(TK_LE, X, Y, 0);}
expr(A) ::= expr(X) GE expr(Y).    {A = sqliteExpr(TK_GE, X, Y, 0);}
expr(A) ::= expr(X) NE expr(Y).    {A = sqliteExpr(TK_NE, X, Y, 0);}
expr(A) ::= expr(X) EQ expr(Y).    {A = sqliteExpr(TK_EQ, X, Y, 0);}
expr(A) ::= expr(X) LIKE expr(Y).  {A = sqliteExpr(TK_LIKE, X, Y, 0);}
expr(A) ::= expr(X) GLOB expr(Y).   {A = sqliteExpr(TK_GLOB,X,Y,0);}
// expr(A) ::= expr(X) IS expr(Y).    {A = sqliteExpr(TK_EQ, X, Y, 0);}
expr(A) ::= expr(X) PLUS expr(Y).  {A = sqliteExpr(TK_PLUS, X, Y, 0);}
expr(A) ::= expr(X) MINUS expr(Y). {A = sqliteExpr(TK_MINUS, X, Y, 0);}
expr(A) ::= expr(X) STAR expr(Y).  {A = sqliteExpr(TK_STAR, X, Y, 0);}
expr(A) ::= expr(X) SLASH expr(Y). {A = sqliteExpr(TK_SLASH, X, Y, 0);}
expr(A) ::= expr(X) ISNULL.        {A = sqliteExpr(TK_ISNULL, X, 0, 0);}
expr(A) ::= expr(X) NOTNULL.       {A = sqliteExpr(TK_NOTNULL, X, 0, 0);}
expr(A) ::= NOT expr(X).           {A = sqliteExpr(TK_NOT, X, 0, 0);}
expr(A) ::= MINUS expr(X). [NOT]   {A = sqliteExpr(TK_UMINUS, X, 0, 0);}
expr(A) ::= PLUS expr(X). [NOT]    {A = X;}

%type exprlist {ExprList*}
%destructor exprlist {sqliteExprListDelete($$);}
%type expritem {Expr*}
%destructor expritem {sqliteExprDelete($$);}

/*
exprlist(A) ::= exprlist(X) COMMA expritem(Y). 
   {A = sqliteExprListAppend(X,Y,0);}
exprlist(A) ::= expritem(X).            {A = sqliteExprListAppend(0,X,0);}
expritem(A) ::= expr(X).                {A = X;}
expritem(A) ::= .                       {A = 0;}
*/

cmd ::= CREATE(S) uniqueflag INDEX ID(X) ON ID(Y) LP idxlist(Z) RP(E).
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
idxitem(A) ::= ID(X).           {A = X;}

cmd ::= DROP INDEX id(X).       {sqliteDropIndex(pParse, &X);}

cmd ::= COPY id(X) FROM id(Y) USING DELIMITERS STRING(Z).
    {sqliteCopy(pParse,&X,&Y,&Z);}
cmd ::= COPY id(X) FROM id(Y).
    {sqliteCopy(pParse,&X,&Y,0);}

cmd ::= VACUUM.                {sqliteVacuum(pParse,0);}
cmd ::= VACUUM id(X).          {sqliteVacuum(pParse,&X);}
