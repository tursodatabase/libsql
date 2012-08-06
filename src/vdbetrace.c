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
**
** The Vdbe parse-tree explainer is also found here.
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
static int findNextHostParameter(const char *zSql, int *pnToken){
  int tokenType;
  int nTotal = 0;
  int n;

  *pnToken = 0;
  while( zSql[0] ){
    n = sqlite3GetToken((u8*)zSql, &tokenType);
    assert( n>0 && tokenType!=TK_ILLEGAL );
    if( tokenType==TK_VARIABLE ){
      *pnToken = n;
      break;
    }
    nTotal += n;
    zSql += n;
  }
  return nTotal;
}

/*
** This function returns a pointer to a nul-terminated string in memory
** obtained from sqlite3DbMalloc(). If sqlite3.vdbeExecCnt is 1, then the
** string contains a copy of zRawSql but with host parameters expanded to 
** their current bindings. Or, if sqlite3.vdbeExecCnt is greater than 1, 
** then the returned string holds a copy of zRawSql with "-- " prepended
** to each line of text.
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
  int idx = 0;             /* Index of a host parameter */
  int nextIndex = 1;       /* Index of next ? host parameter */
  int n;                   /* Length of a token prefix */
  int nToken;              /* Length of the parameter token */
  int i;                   /* Loop counter */
  Mem *pVar;               /* Value of a host parameter */
  StrAccum out;            /* Accumulate the output here */
  char zBase[100];         /* Initial working space */

  db = p->db;
  sqlite3StrAccumInit(&out, zBase, sizeof(zBase), 
                      db->aLimit[SQLITE_LIMIT_LENGTH]);
  out.db = db;
  if( db->vdbeExecCnt>1 ){
    while( *zRawSql ){
      const char *zStart = zRawSql;
      while( *(zRawSql++)!='\n' && *zRawSql );
      sqlite3StrAccumAppend(&out, "-- ", 3);
      sqlite3StrAccumAppend(&out, zStart, (int)(zRawSql-zStart));
    }
  }else{
    while( zRawSql[0] ){
      n = findNextHostParameter(zRawSql, &nToken);
      assert( n>0 );
      sqlite3StrAccumAppend(&out, zRawSql, n);
      zRawSql += n;
      assert( zRawSql[0] || nToken==0 );
      if( nToken==0 ) break;
      if( zRawSql[0]=='?' ){
        if( nToken>1 ){
          assert( sqlite3Isdigit(zRawSql[1]) );
          sqlite3GetInt32(&zRawSql[1], &idx);
        }else{
          idx = nextIndex;
        }
      }else{
        assert( zRawSql[0]==':' || zRawSql[0]=='$' || zRawSql[0]=='@' );
        testcase( zRawSql[0]==':' );
        testcase( zRawSql[0]=='$' );
        testcase( zRawSql[0]=='@' );
        idx = sqlite3VdbeParameterIndex(p, zRawSql, nToken);
        assert( idx>0 );
      }
      zRawSql += nToken;
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
        u8 enc = ENC(db);
        if( enc!=SQLITE_UTF8 ){
          Mem utf8;
          memset(&utf8, 0, sizeof(utf8));
          utf8.db = db;
          sqlite3VdbeMemSetStr(&utf8, pVar->z, pVar->n, enc, SQLITE_STATIC);
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
  }
  return sqlite3StrAccumFinish(&out);
}

#endif /* #ifndef SQLITE_OMIT_TRACE */

/*****************************************************************************
** The following code implements the data-structure explaining logic
** for the Vdbe.
*/

#if defined(SQLITE_ENABLE_TREE_EXPLAIN)

/*
** Allocate a new Explain object
*/
void sqlite3ExplainBegin(Vdbe *pVdbe){
  if( pVdbe ){
    Explain *p;
    sqlite3BeginBenignMalloc();
    p = (Explain *)sqlite3MallocZero( sizeof(Explain) );
    if( p ){
      p->pVdbe = pVdbe;
      sqlite3_free(pVdbe->pExplain);
      pVdbe->pExplain = p;
      sqlite3StrAccumInit(&p->str, p->zBase, sizeof(p->zBase),
                          SQLITE_MAX_LENGTH);
      p->str.useMalloc = 2;
    }else{
      sqlite3EndBenignMalloc();
    }
  }
}

/*
** Return true if the Explain ends with a new-line.
*/
static int endsWithNL(Explain *p){
  return p && p->str.zText && p->str.nChar
           && p->str.zText[p->str.nChar-1]=='\n';
}
    
/*
** Append text to the indentation
*/
void sqlite3ExplainPrintf(Vdbe *pVdbe, const char *zFormat, ...){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 ){
    va_list ap;
    if( p->nIndent && endsWithNL(p) ){
      int n = p->nIndent;
      if( n>ArraySize(p->aIndent) ) n = ArraySize(p->aIndent);
      sqlite3AppendSpace(&p->str, p->aIndent[n-1]);
    }   
    va_start(ap, zFormat);
    sqlite3VXPrintf(&p->str, 1, zFormat, ap);
    va_end(ap);
  }
}

/*
** Append a '\n' if there is not already one.
*/
void sqlite3ExplainNL(Vdbe *pVdbe){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 && !endsWithNL(p) ){
    sqlite3StrAccumAppend(&p->str, "\n", 1);
  }
}

/*
** Push a new indentation level.  Subsequent lines will be indented
** so that they begin at the current cursor position.
*/
void sqlite3ExplainPush(Vdbe *pVdbe){
  Explain *p;
  if( pVdbe && (p = pVdbe->pExplain)!=0 ){
    if( p->str.zText && p->nIndent<ArraySize(p->aIndent) ){
      const char *z = p->str.zText;
      int i = p->str.nChar-1;
      int x;
      while( i>=0 && z[i]!='\n' ){ i--; }
      x = (p->str.nChar - 1) - i;
      if( p->nIndent && x<p->aIndent[p->nIndent-1] ){
        x = p->aIndent[p->nIndent-1];
      }
      p->aIndent[p->nIndent] = x;
    }
    p->nIndent++;
  }
}

/*
** Pop the indentation stack by one level.
*/
void sqlite3ExplainPop(Vdbe *p){
  if( p && p->pExplain ) p->pExplain->nIndent--;
}

/*
** Free the indentation structure
*/
void sqlite3ExplainFinish(Vdbe *pVdbe){
  if( pVdbe && pVdbe->pExplain ){
    sqlite3_free(pVdbe->zExplain);
    sqlite3ExplainNL(pVdbe);
    pVdbe->zExplain = sqlite3StrAccumFinish(&pVdbe->pExplain->str);
    sqlite3_free(pVdbe->pExplain);
    pVdbe->pExplain = 0;
    sqlite3EndBenignMalloc();
  }
}

/*
** Return the explanation of a virtual machine.
*/
const char *sqlite3VdbeExplanation(Vdbe *pVdbe){
  return (pVdbe && pVdbe->zExplain) ? pVdbe->zExplain : 0;
}
#endif /* defined(SQLITE_DEBUG) */
