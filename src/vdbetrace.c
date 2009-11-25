/*
** 2009 November 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used to insert the values of host parameters
** (aka "wildcards") into the SQL text output by sqlite3_trace().
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_TRACE

/*
** zSql is a zero-terminated string of UTF-8 SQL text.  Return the number of
** bytes in this text up to but excluding the first character in
** a host parameter.  If the text contains no host parameters, return
** the total number of bytes in the text.
*/
static int findNextHostParameter(const char *zSql){
  int tokenType;
  int nTotal = 0;
  int n;

  while( zSql[0] ){
    n = sqlite3GetToken((u8*)zSql, &tokenType);
    assert( n>0 && tokenType!=TK_ILLEGAL );
    if( tokenType==TK_VARIABLE ) break;
    nTotal += n;
    zSql += n;
  }
  return nTotal;
}

/*
** Return a pointer to a string in memory obtained form sqlite3Malloc() which
** holds a copy of zRawSql but with host parameters expanded to their
** current values.
**
** The calling function is responsible for making sure the memory returned
** is eventually freed.
**
** ALGORITHM:  Scan the input string looking for host parameters in any of
** these forms:  ?, ?N, $A, @A, :A.  Take care to avoid text within
** string literals, quoted identifier names, and comments.  For text forms,
** the host parameter index is found by scanning the perpared
** statement for the corresponding OP_Variable opcode.  Once the host
** parameter index is known, locate the value in p->aVar[].  Then render
** the value as a literal in place of the host parameter name.
*/
char *sqlite3VdbeExpandSql(
  Vdbe *p,                 /* The prepared statement being evaluated */
  const char *zRawSql      /* Raw text of the SQL statement */
){
  sqlite3 *db;             /* The database connection */
  int idx;                 /* Index of a host parameter */
  int nextIndex = 1;       /* Index of next ? host parameter */
  int n;                   /* Length of a token prefix */
  int i;                   /* Loop counter */
  int dummy;               /* For holding a unused return value */
  Mem *pVar;               /* Value of a host parameter */
  VdbeOp *pOp;             /* For looping over opcodes */
  StrAccum out;            /* Accumulate the output here */
  char zBase[100];         /* Initial working space */

  db = p->db;
  sqlite3StrAccumInit(&out, zBase, sizeof(zBase), 
                      db->aLimit[SQLITE_LIMIT_LENGTH]);
  out.db = db;
  while( zRawSql[0] ){
    n = findNextHostParameter(zRawSql);
    assert( n>0 );
    sqlite3StrAccumAppend(&out, zRawSql, n);
    zRawSql += n;
    if( zRawSql[0]==0 ) break;
    if( zRawSql[0]=='?' ){
      zRawSql++;
      if( sqlite3Isdigit(zRawSql[0]) ){
        idx = 0;
        while( sqlite3Isdigit(zRawSql[0]) ){
          idx = idx*10 + zRawSql[0] - '0';
          zRawSql++;
        }
      }else{
        idx = nextIndex;
      }
    }else{
      assert( zRawSql[0]==':' || zRawSql[0]=='$' || zRawSql[0]=='@' );
      testcase( zRawSql[0]==':' );
      testcase( zRawSql[0]=='$' );
      testcase( zRawSql[0]=='@' );
      n = sqlite3GetToken((u8*)zRawSql, &dummy);
      idx = 0;
      for(i=0, pOp=p->aOp; ALWAYS(i<p->nOp); i++, pOp++){
        if( pOp->opcode!=OP_Variable ) continue;
        if( pOp->p3>1 ) continue;
        if( pOp->p4.z==0 ) continue;
        if( memcmp(pOp->p4.z, zRawSql, n)==0 && pOp->p4.z[n]==0 ){
          idx = pOp->p1;
          break;
        }
      }
      assert( idx>0 );
      zRawSql += n;
    }
    nextIndex = idx + 1;
    assert( idx>0 && idx<=p->nVar );
    pVar = &p->aVar[idx-1];
    if( pVar->flags & MEM_Null ){
      sqlite3StrAccumAppend(&out, "NULL", 4);
    }else if( pVar->flags & MEM_Int ){
      sqlite3XPrintf(&out, "%lld", pVar->u.i);
    }else if( pVar->flags & MEM_Real ){
      sqlite3XPrintf(&out, "%!.15g", pVar->r);
    }else if( pVar->flags & MEM_Str ){
#ifndef SQLITE_OMIT_UTF16
      if( ENC(db)!=SQLITE_UTF8 ){
        Mem utf8;
        memset(&utf8, 0, sizeof(utf8));
        utf8.db = db;
        sqlite3VdbeMemSetStr(&utf8, pVar->z, pVar->n, ENC(db), SQLITE_STATIC);
        sqlite3VdbeChangeEncoding(&utf8, SQLITE_UTF8);
        sqlite3XPrintf(&out, "'%.*q'", utf8.n, utf8.z);
        sqlite3VdbeMemRelease(&utf8);
      }else
#endif
      {
        sqlite3XPrintf(&out, "'%.*q'", pVar->n, pVar->z);
      }
    }else if( pVar->flags & MEM_Zero ){
      sqlite3XPrintf(&out, "zeroblob(%d)", pVar->u.nZero);
    }else{
      assert( pVar->flags & MEM_Blob );
      sqlite3StrAccumAppend(&out, "x'", 2);
      for(i=0; i<pVar->n; i++){
        sqlite3XPrintf(&out, "%02x", pVar->z[i]&0xff);
      }
      sqlite3StrAccumAppend(&out, "'", 1);
    }
  }
  return sqlite3StrAccumFinish(&out);
}

#endif /* #ifndef SQLITE_OMIT_TRACE */
