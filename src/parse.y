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
** This file contains SQLite's grammar for SQL.  Process this file
** using the lemon parser generator to generate C code that runs
** the parser.  Lemon will also generate a header file containing
** numeric codes for all of the tokens.
**
** @(#) $Id: parse.y,v 1.66 2002/05/21 11:38:12 drh Exp $
*/
%token_prefix TK_
%token_type {Token}
%default_type {Token}
%extra_argument {Parse *pParse}
%syntax_error {
  sqliteSetString(&pParse->zErrMsg,"syntax error",0);
  pParse->sErrToken = TOKEN;
}
%name sqliteParser
%include {
#include "sqliteInt.h"
#include "parse.h"

/*
** A structure for holding two integers
*/
struct twoint { int a,b; };

/*
** A structure for holding an integer and an IdList
*/
struct int_idlist { int a; IdList * b; };
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
cmdlist ::= cmdlist ecmd.
ecmd ::= explain cmd SEMI.  {sqliteExec(pParse);}
ecmd ::= cmd SEMI.          {sqliteExec(pParse);}
ecmd ::= SEMI.
explain ::= EXPLAIN.    {pParse->explain = 1;}

///////////////////// Begin and end transactions. ////////////////////////////
//

cmd ::= BEGIN trans_opt onconf(R).  {sqliteBeginTransaction(pParse,R);}
trans_opt ::= .
trans_opt ::= TRANSACTION.
trans_opt ::= TRANSACTION ids.
cmd ::= COMMIT trans_opt.      {sqliteCommitTransaction(pParse);}
cmd ::= END trans_opt.         {sqliteCommitTransaction(pParse);}
cmd ::= ROLLBACK trans_opt.    {sqliteRollbackTransaction(pParse);}

///////////////////// The CREATE TABLE statement ////////////////////////////
//
cmd ::= create_table create_table_args.
create_table ::= CREATE(X) temp(T) TABLE ids(Y). {
   sqliteStartTable(pParse,&X,&Y,T);
}
%type temp {int}
temp(A) ::= TEMP.  {A = 1;}
temp(A) ::= .      {A = 0;}
create_table_args ::= LP columnlist conslist_opt RP(X). {
  sqliteEndTable(pParse,&X,0);
}
create_table_args ::= AS select(S). {
  sqliteEndTable(pParse,0,S);
  sqliteSelectDelete(S);
}
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
//
%type id {Token}
id(A) ::= ABORT(X).      {A = X;}
id(A) ::= AFTER(X).      {A = X;}
id(A) ::= ASC(X).        {A = X;}
id(A) ::= BEFORE(X).     {A = X;}
id(A) ::= BEGIN(X).      {A = X;}
id(A) ::= CLUSTER(X).    {A = X;}
id(A) ::= CONFLICT(X).   {A = X;}
id(A) ::= COPY(X).       {A = X;}
id(A) ::= DELIMITERS(X). {A = X;}
id(A) ::= DESC(X).       {A = X;}
id(A) ::= EACH(X).       {A = X;}
id(A) ::= END(X).        {A = X;}
id(A) ::= EXPLAIN(X).    {A = X;}
id(A) ::= FAIL(X).       {A = X;}
id(A) ::= FOR(X).        {A = X;}
id(A) ::= ID(X).         {A = X;}
id(A) ::= IGNORE(X).     {A = X;}
id(A) ::= INSTEAD(X).    {A = X;}
id(A) ::= KEY(X).        {A = X;}
id(A) ::= OF(X).         {A = X;}
id(A) ::= OFFSET(X).     {A = X;}
id(A) ::= PRAGMA(X).     {A = X;}
id(A) ::= REPLACE(X).    {A = X;}
id(A) ::= ROW(X).        {A = X;}
id(A) ::= TEMP(X).       {A = X;}
id(A) ::= TRIGGER(X).    {A = X;}
id(A) ::= VACUUM(X).     {A = X;}
id(A) ::= VIEW(X).       {A = X;}

// And "ids" is an identifer-or-string.
//
%type ids {Token}
ids(A) ::= id(X).        {A = X;}
ids(A) ::= STRING(X).    {A = X;}

type ::= .
type ::= typename(X).                    {sqliteAddColumnType(pParse,&X,&X);}
type ::= typename(X) LP signed RP(Y).    {sqliteAddColumnType(pParse,&X,&Y);}
type ::= typename(X) LP signed COMMA signed RP(Y).
                                         {sqliteAddColumnType(pParse,&X,&Y);}
%type typename {Token}
typename(A) ::= ids(X).           {A = X;}
typename(A) ::= typename(X) ids.  {A = X;}
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

// In addition to the type name, we also care about the primary key and
// UNIQUE constraints.
//
ccons ::= NOT NULL onconf(R).               {sqliteAddNotNull(pParse, R);}
ccons ::= PRIMARY KEY sortorder onconf(R).  {sqliteAddPrimaryKey(pParse,0,R);}
ccons ::= UNIQUE onconf(R).            {sqliteCreateIndex(pParse,0,0,0,R,0,0);}
ccons ::= CHECK LP expr RP onconf.

// For the time being, the only constraint we care about is the primary
// key and UNIQUE.  Both create indices.
//
conslist_opt ::= .
conslist_opt ::= COMMA conslist.
conslist ::= conslist COMMA tcons.
conslist ::= conslist tcons.
conslist ::= tcons.
tcons ::= CONSTRAINT ids.
tcons ::= PRIMARY KEY LP idxlist(X) RP onconf(R).
                                             {sqliteAddPrimaryKey(pParse,X,R);}
tcons ::= UNIQUE LP idxlist(X) RP onconf(R).
                                       {sqliteCreateIndex(pParse,0,0,X,R,0,0);}
tcons ::= CHECK expr onconf.

// The following is a non-standard extension that allows us to declare the
// default behavior when there is a constraint conflict.
//
%type onconf {int}
%type orconf {int}
%type resolvetype {int}
onconf(A) ::= .                              { A = OE_Default; }
onconf(A) ::= ON CONFLICT resolvetype(X).    { A = X; }
orconf(A) ::= .                              { A = OE_Default; }
orconf(A) ::= OR resolvetype(X).             { A = X; }
resolvetype(A) ::= ROLLBACK.                 { A = OE_Rollback; }
resolvetype(A) ::= ABORT.                    { A = OE_Abort; }
resolvetype(A) ::= FAIL.                     { A = OE_Fail; }
resolvetype(A) ::= IGNORE.                   { A = OE_Ignore; }
resolvetype(A) ::= REPLACE.                  { A = OE_Replace; }

////////////////////////// The DROP TABLE /////////////////////////////////////
//
cmd ::= DROP TABLE ids(X).          {sqliteDropTable(pParse,&X,0);}

///////////////////// The CREATE VIEW statement /////////////////////////////
//
cmd ::= CREATE(X) VIEW ids(Y) AS select(S). {
  sqliteCreateView(pParse, &X, &Y, S);
}
cmd ::= DROP VIEW ids(X). {
  sqliteDropTable(pParse, &X, 1);
}

//////////////////////// The SELECT statement /////////////////////////////////
//
cmd ::= select(X).  {
  sqliteSelect(pParse, X, SRT_Callback, 0, 0, 0, 0);
  sqliteSelectDelete(X);
}

%type select {Select*}
%destructor select {sqliteSelectDelete($$);}
%type oneselect {Select*}
%destructor oneselect {sqliteSelectDelete($$);}

select(A) ::= oneselect(X).                      {A = X;}
select(A) ::= select(X) multiselect_op(Y) oneselect(Z).  {
  if( Z ){
    Z->op = Y;
    Z->pPrior = X;
  }
  A = Z;
}
%type multiselect_op {int}
multiselect_op(A) ::= UNION.      {A = TK_UNION;}
multiselect_op(A) ::= UNION ALL.  {A = TK_ALL;}
multiselect_op(A) ::= INTERSECT.  {A = TK_INTERSECT;}
multiselect_op(A) ::= EXCEPT.     {A = TK_EXCEPT;}
oneselect(A) ::= SELECT distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
  A = sqliteSelectNew(W,X,Y,P,Q,Z,D,L.a,L.b);
}

// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {int}
distinct(A) ::= DISTINCT.   {A = 1;}
distinct(A) ::= ALL.        {A = 0;}
distinct(A) ::= .           {A = 0;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an
// opcode of TK_ALL.
//
%type selcollist {ExprList*}
%destructor selcollist {sqliteExprListDelete($$);}
%type sclp {ExprList*}
%destructor sclp {sqliteExprListDelete($$);}
sclp(A) ::= selcollist(X) COMMA.             {A = X;}
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(P) expr(X).           {A = sqliteExprListAppend(P,X,0);}
selcollist(A) ::= sclp(P) expr(X) as ids(Y). {A = sqliteExprListAppend(P,X,&Y);}
selcollist(A) ::= sclp(P) STAR. {
  A = sqliteExprListAppend(P, sqliteExpr(TK_ALL, 0, 0, 0), 0);
}
selcollist(A) ::= sclp(P) ids(X) DOT STAR. {
  Expr *pRight = sqliteExpr(TK_ALL, 0, 0, 0);
  Expr *pLeft = sqliteExpr(TK_ID, 0, 0, &X);
  A = sqliteExprListAppend(P, sqliteExpr(TK_DOT, pLeft, pRight, 0), 0);
}
as ::= .
as ::= AS.


%type seltablist {IdList*}
%destructor seltablist {sqliteIdListDelete($$);}
%type stl_prefix {IdList*}
%destructor stl_prefix {sqliteIdListDelete($$);}
%type from {IdList*}
%destructor from {sqliteIdListDelete($$);}

from(A) ::= .                                 {A = sqliteMalloc(sizeof(*A));}
from(A) ::= FROM seltablist(X).               {A = X;}
stl_prefix(A) ::= seltablist(X) COMMA.        {A = X;}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(X) ids(Y).       {A = sqliteIdListAppend(X,&Y);}
seltablist(A) ::= stl_prefix(X) ids(Y) as ids(Z). {
  A = sqliteIdListAppend(X,&Y);
  sqliteIdListAddAlias(A,&Z);
}
seltablist(A) ::= stl_prefix(X) LP select(S) RP. {
  A = sqliteIdListAppend(X,0);
  A->a[A->nId-1].pSelect = S;
  if( S->pOrderBy ){
    sqliteExprListDelete(S->pOrderBy);
    S->pOrderBy = 0;
  }
}
seltablist(A) ::= stl_prefix(X) LP select(S) RP as ids(Z). {
  A = sqliteIdListAppend(X,0);
  A->a[A->nId-1].pSelect = S;
  if( S->pOrderBy ){
    sqliteExprListDelete(S->pOrderBy);
    S->pOrderBy = 0;
  }
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

%type limit_opt {struct twoint}
limit_opt(A) ::= .                  {A.a = -1; A.b = 0;}
limit_opt(A) ::= LIMIT INTEGER(X).  {A.a = atoi(X.z); A.b = 0;}
limit_opt(A) ::= LIMIT INTEGER(X) limit_sep INTEGER(Y). 
                                    {A.a = atoi(X.z); A.b = atoi(Y.z);}
limit_sep ::= OFFSET.
limit_sep ::= COMMA.

/////////////////////////// The DELETE statement /////////////////////////////
//
cmd ::= DELETE FROM ids(X) where_opt(Y).
    {sqliteDeleteFrom(pParse, &X, Y);}

%type where_opt {Expr*}
%destructor where_opt {sqliteExprDelete($$);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X;}

%type setlist {ExprList*}
%destructor setlist {sqliteExprListDelete($$);}

////////////////////////// The UPDATE command ////////////////////////////////
//
cmd ::= UPDATE orconf(R) ids(X) SET setlist(Y) where_opt(Z).
    {sqliteUpdate(pParse,&X,Y,Z,R);}

setlist(A) ::= setlist(Z) COMMA ids(X) EQ expr(Y).
    {A = sqliteExprListAppend(Z,Y,&X);}
setlist(A) ::= ids(X) EQ expr(Y).   {A = sqliteExprListAppend(0,Y,&X);}

////////////////////////// The INSERT command /////////////////////////////////
//
cmd ::= insert_cmd(R) INTO ids(X) inscollist_opt(F) VALUES LP itemlist(Y) RP.
               {sqliteInsert(pParse, &X, Y, 0, F, R);}
cmd ::= insert_cmd(R) INTO ids(X) inscollist_opt(F) select(S).
               {sqliteInsert(pParse, &X, 0, S, F, R);}

%type insert_cmd {int}
insert_cmd(A) ::= INSERT orconf(R).   {A = R;}
insert_cmd(A) ::= REPLACE.            {A = OE_Replace;}


%type itemlist {ExprList*}
%destructor itemlist {sqliteExprListDelete($$);}

itemlist(A) ::= itemlist(X) COMMA expr(Y).  {A = sqliteExprListAppend(X,Y,0);}
itemlist(A) ::= expr(X).                    {A = sqliteExprListAppend(0,X,0);}

%type inscollist_opt {IdList*}
%destructor inscollist_opt {sqliteIdListDelete($$);}
%type inscollist {IdList*}
%destructor inscollist {sqliteIdListDelete($$);}

inscollist_opt(A) ::= .                       {A = 0;}
inscollist_opt(A) ::= LP inscollist(X) RP.    {A = X;}
inscollist(A) ::= inscollist(X) COMMA ids(Y). {A = sqliteIdListAppend(X,&Y);}
inscollist(A) ::= ids(Y).                     {A = sqliteIdListAppend(0,&Y);}

/////////////////////////// Expression Processing /////////////////////////////
//
%left OR.
%left AND.
%right NOT.
%left EQ NE ISNULL NOTNULL IS LIKE GLOB BETWEEN IN.
%left GT GE LT LE.
%left BITAND BITOR LSHIFT RSHIFT.
%left PLUS MINUS.
%left STAR SLASH REM.
%left CONCAT.
%right UMINUS BITNOT.

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
expr(A) ::= expr(X) BITAND expr(Y). {A = sqliteExpr(TK_BITAND, X, Y, 0);}
expr(A) ::= expr(X) BITOR expr(Y).  {A = sqliteExpr(TK_BITOR, X, Y, 0);}
expr(A) ::= expr(X) LSHIFT expr(Y). {A = sqliteExpr(TK_LSHIFT, X, Y, 0);}
expr(A) ::= expr(X) RSHIFT expr(Y). {A = sqliteExpr(TK_RSHIFT, X, Y, 0);}
expr(A) ::= expr(X) likeop(OP) expr(Y).  [LIKE]  {
  ExprList *pList = sqliteExprListAppend(0, Y, 0);
  pList = sqliteExprListAppend(pList, X, 0);
  A = sqliteExprFunction(pList, &OP);
  sqliteExprSpan(A, &X->span, &Y->span);
}
expr(A) ::= expr(X) NOT likeop(OP) expr(Y). [LIKE] {
  ExprList *pList = sqliteExprListAppend(0, Y, 0);
  pList = sqliteExprListAppend(pList, X, 0);
  A = sqliteExprFunction(pList, &OP);
  A = sqliteExpr(TK_NOT, A, 0, 0);
  sqliteExprSpan(A,&X->span,&Y->span);
}
likeop(A) ::= LIKE(X). {A = X;}
likeop(A) ::= GLOB(X). {A = X;}
expr(A) ::= expr(X) PLUS expr(Y).  {A = sqliteExpr(TK_PLUS, X, Y, 0);}
expr(A) ::= expr(X) MINUS expr(Y). {A = sqliteExpr(TK_MINUS, X, Y, 0);}
expr(A) ::= expr(X) STAR expr(Y).  {A = sqliteExpr(TK_STAR, X, Y, 0);}
expr(A) ::= expr(X) SLASH expr(Y). {A = sqliteExpr(TK_SLASH, X, Y, 0);}
expr(A) ::= expr(X) REM expr(Y).   {A = sqliteExpr(TK_REM, X, Y, 0);}
expr(A) ::= expr(X) CONCAT expr(Y). {A = sqliteExpr(TK_CONCAT, X, Y, 0);}
expr(A) ::= expr(X) ISNULL(E). {
  A = sqliteExpr(TK_ISNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) IS NULL(E). {
  A = sqliteExpr(TK_ISNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) NOTNULL(E). {
  A = sqliteExpr(TK_NOTNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) NOT NULL(E). {
  A = sqliteExpr(TK_NOTNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= expr(X) IS NOT NULL(E). {
  A = sqliteExpr(TK_NOTNULL, X, 0, 0);
  sqliteExprSpan(A,&X->span,&E);
}
expr(A) ::= NOT(B) expr(X). {
  A = sqliteExpr(TK_NOT, X, 0, 0);
  sqliteExprSpan(A,&B,&X->span);
}
expr(A) ::= BITNOT(B) expr(X). {
  A = sqliteExpr(TK_BITNOT, X, 0, 0);
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

/* CASE expressions */
expr(A) ::= CASE(C) case_operand(X) case_exprlist(Y) case_else(Z) END(E). {
  A = sqliteExpr(TK_CASE, X, Z, 0);
  if( A ) A->pList = Y;
  sqliteExprSpan(A, &C, &E);
}
%type case_exprlist {ExprList*}
%destructor case_exprlist {sqliteExprListDelete($$);}
case_exprlist(A) ::= case_exprlist(X) WHEN expr(Y) THEN expr(Z). {
  A = sqliteExprListAppend(X, Y, 0);
  A = sqliteExprListAppend(A, Z, 0);
}
case_exprlist(A) ::= WHEN expr(Y) THEN expr(Z). {
  A = sqliteExprListAppend(0, Y, 0);
  A = sqliteExprListAppend(A, Z, 0);
}
%type case_else {Expr*}
case_else(A) ::=  ELSE expr(X).         {A = X;}
case_else(A) ::=  .                     {A = 0;} 
%type case_operand {Expr*}
case_operand(A) ::= expr(X).            {A = X;} 
case_operand(A) ::= .                   {A = 0;} 

%type exprlist {ExprList*}
%destructor exprlist {sqliteExprListDelete($$);}
%type expritem {Expr*}
%destructor expritem {sqliteExprDelete($$);}

exprlist(A) ::= exprlist(X) COMMA expritem(Y). 
   {A = sqliteExprListAppend(X,Y,0);}
exprlist(A) ::= expritem(X).            {A = sqliteExprListAppend(0,X,0);}
expritem(A) ::= expr(X).                {A = X;}
expritem(A) ::= .                       {A = 0;}

///////////////////////////// The CREATE INDEX command ///////////////////////
//
cmd ::= CREATE(S) uniqueflag(U) INDEX ids(X)
        ON ids(Y) LP idxlist(Z) RP(E) onconf(R). {
  if( U!=OE_None ) U = R;
  if( U==OE_Default) U = OE_Abort;
  sqliteCreateIndex(pParse, &X, &Y, Z, U, &S, &E);
}

%type uniqueflag {int}
uniqueflag(A) ::= UNIQUE.  { A = OE_Abort; }
uniqueflag(A) ::= .        { A = OE_None; }

%type idxlist {IdList*}
%destructor idxlist {sqliteIdListDelete($$);}
%type idxitem {Token}

idxlist(A) ::= idxlist(X) COMMA idxitem(Y).  
     {A = sqliteIdListAppend(X,&Y);}
idxlist(A) ::= idxitem(Y).
     {A = sqliteIdListAppend(0,&Y);}
idxitem(A) ::= ids(X).          {A = X;}

///////////////////////////// The DROP INDEX command /////////////////////////
//

cmd ::= DROP INDEX ids(X).      {sqliteDropIndex(pParse, &X);}


///////////////////////////// The COPY command ///////////////////////////////
//
cmd ::= COPY orconf(R) ids(X) FROM ids(Y) USING DELIMITERS STRING(Z).
    {sqliteCopy(pParse,&X,&Y,&Z,R);}
cmd ::= COPY orconf(R) ids(X) FROM ids(Y).
    {sqliteCopy(pParse,&X,&Y,0,R);}

///////////////////////////// The VACUUM command /////////////////////////////
//
cmd ::= VACUUM.                {sqliteVacuum(pParse,0);}
cmd ::= VACUUM ids(X).         {sqliteVacuum(pParse,&X);}

///////////////////////////// The PRAGMA command /////////////////////////////
//
cmd ::= PRAGMA ids(X) EQ ids(Y).         {sqlitePragma(pParse,&X,&Y,0);}
cmd ::= PRAGMA ids(X) EQ ON(Y).          {sqlitePragma(pParse,&X,&Y,0);}
cmd ::= PRAGMA ids(X) EQ plus_num(Y).    {sqlitePragma(pParse,&X,&Y,0);}
cmd ::= PRAGMA ids(X) EQ minus_num(Y).   {sqlitePragma(pParse,&X,&Y,1);}
cmd ::= PRAGMA ids(X) LP ids(Y) RP.      {sqlitePragma(pParse,&X,&Y,0);}
cmd ::= PRAGMA ids(X).                   {sqlitePragma(pParse,&X,&X,0);}
plus_num(A) ::= plus_opt number(X).   {A = X;}
minus_num(A) ::= MINUS number(X).     {A = X;}
number(A) ::= INTEGER(X).  {A = X;}
number(A) ::= FLOAT(X).    {A = X;}
plus_opt ::= PLUS.
plus_opt ::= .

//////////////////////////// The CREATE TRIGGER command /////////////////////
cmd ::= CREATE(A) TRIGGER ids(B) trigger_time(C) trigger_event(D) ON ids(E) 
                  foreach_clause(F) when_clause(G)
                  BEGIN trigger_cmd_list(S) END(Z). {
  sqliteCreateTrigger(pParse, &B, C, D.a, D.b, &E, F, G, S, 
      A.z, (int)(Z.z - A.z) + Z.n );
}

%type trigger_time  {int}
trigger_time(A) ::= BEFORE.      { A = TK_BEFORE; }
trigger_time(A) ::= AFTER.       { A = TK_AFTER;  }
trigger_time(A) ::= INSTEAD OF.  { A = TK_INSTEAD;}
trigger_time(A) ::= .            { A = TK_BEFORE; }

%type trigger_event {struct int_idlist}
trigger_event(A) ::= DELETE. { A.a = TK_DELETE; A.b = 0; }
trigger_event(A) ::= INSERT. { A.a = TK_INSERT; A.b = 0; }
trigger_event(A) ::= UPDATE. { A.a = TK_UPDATE; A.b = 0;}
trigger_event(A) ::= UPDATE OF inscollist(X). {A.a = TK_UPDATE; A.b = X; }

%type foreach_clause {int}
foreach_clause(A) ::= .                   { A = TK_ROW; }
foreach_clause(A) ::= FOR EACH ROW.       { A = TK_ROW; }
foreach_clause(A) ::= FOR EACH STATEMENT. { A = TK_STATEMENT; }

%type when_clause {Expr *}
when_clause(A) ::= .             { A = 0; }
when_clause(A) ::= WHEN expr(X). { A = X; }

%type trigger_cmd_list {TriggerStep *}
trigger_cmd_list(A) ::= trigger_cmd(X) SEMI trigger_cmd_list(Y). {
  X->pNext = Y ; A = X; }
trigger_cmd_list(A) ::= . { A = 0; }

%type trigger_cmd {TriggerStep *}
// UPDATE 
trigger_cmd(A) ::= UPDATE orconf(R) ids(X) SET setlist(Y) where_opt(Z).  
               { A = sqliteTriggerUpdateStep(&X, Y, Z, R); }

// INSERT
trigger_cmd(A) ::= INSERT orconf(R) INTO ids(X) inscollist_opt(F) 
  VALUES LP itemlist(Y) RP.  
{A = sqliteTriggerInsertStep(&X, F, Y, 0, R);}

trigger_cmd(A) ::= INSERT orconf(R) INTO ids(X) inscollist_opt(F) select(S).
               {A = sqliteTriggerInsertStep(&X, F, 0, S, R);}

// DELETE
trigger_cmd(A) ::= DELETE FROM ids(X) where_opt(Y).
               {A = sqliteTriggerDeleteStep(&X, Y);}

// SELECT
trigger_cmd(A) ::= select(X).  {A = sqliteTriggerSelectStep(X); }

////////////////////////  DROP TRIGGER statement //////////////////////////////
cmd ::= DROP TRIGGER ids(X). {
    sqliteDropTrigger(pParse,&X,0);
}
