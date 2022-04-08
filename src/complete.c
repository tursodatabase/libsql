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
** An tokenizer for SQL
**
** This file contains C code that implements the sqlite3_complete() API.
** This code used to be part of the tokenizer.c source file.  But by
** separating it out, the code will be automatically omitted from
** static links that do not use it.
*/
#include "sqliteInt.h"
#ifndef SQLITE_OMIT_COMPLETE

/*
** This is defined in tokenize.c.  We just have to import the definition.
*/
#ifndef SQLITE_AMALGAMATION
#ifdef SQLITE_ASCII
#define IdChar(C)  ((sqlite3CtypeMap[(unsigned char)C]&0x46)!=0)
#endif
#ifdef SQLITE_EBCDIC
extern const char sqlite3IsEbcdicIdChar[];
#define IdChar(C)  (((c=C)>=0x42 && sqlite3IsEbcdicIdChar[c-0x40]))
#endif
#endif /* SQLITE_AMALGAMATION */


/*
** Token types used by the sqlite3_complete() routine.  See the header
** comments on that procedure for additional information.
*/
#define tkSEMI    0      /* ; */
#define tkWS      1      /* whitespace */
#define tkOTHER   2      /* other puntuation, literal-value, or quoted ID */
#define tkEXPLAIN 3      /* EXPLAIN keyword */
#define tkCREATE  4      /* CREATE keyword */
#define tkTRIGGER 5      /* TRIGGER keyword */
#define tkEND     6      /* END keyword */
#define tkFOR     7      /* FOR keyword */
#define tkEACH    8      /* EACH keyword */
#define tkROW     9      /* ROW keyword */
#define tkKYWD    10     /* Any other keyword */

/*
** Return TRUE if the given SQL string ends in a semicolon.
**
** Special handling is require for CREATE TRIGGER statements.
** Whenever the CREATE TRIGGER keywords are seen, the statement
** must end with ";END;".
**
** This implementation uses a state machine with 8 states:
**
**   (1) START     At the beginning or end of an SQL statement.  This routine
**                 returns 1 if it ends in the START state and 0 if it ends
**                 in any other state.
**
**   (2) NORMAL    We are in the middle of statement which ends with a single
**                 semicolon.
**
**   (2) EXPLAIN   The keyword EXPLAIN has been seen at the beginning of 
**                 a statement.
**
**   (3) CREATE    The keyword CREATE has been seen at the beginning of a
**                 statement, possibly preceded by EXPLAIN and/or followed by
**                 TEMP or TEMPORARY
**
**   (4) TRIGGER   We are in the middle of a trigger definition or FOR EACH ROW
**                 statement that must be ended by ";END:"
**
**   (5) SEMI      We've seen the first semicolon in the ";END;" that occurs at
**                 the end of a trigger definition.
**
**   (6) END       We've seen the ";END" of the ";END;" that occurs at the end
**                 of a trigger definition.
**
**   (7) FOR       We've seen the "FOR" keyword near the beginning of a 
**                 statement.  Awaiting a following "EACH".
**
**   (8) EACH      We've seen keywrods "FOR" and "EACH" near the beginning of
**                 of a statement and are awaiting a "ROW" token.
**
**   (9) INIT      No non-whitespace input seen so far.
**
**
** Transitions between states above are determined by tokens extracted
** from the input.  The following tokens are significant:
**
**   (0) tkSEMI      A semicolon.
**   (1) tkWS        Whitespace.
**   (2) tkOTHER     Any other token.
**   (3) tkEXPLAIN   The "explain" keyword.
**   (4) tkCREATE    The "create" keyword.
**   (5) tkTRIGGER   The "trigger" keyword.
**   (6) tkEND       The "end" keyword.
**   (7) tkFOR       The "for" keyword.
**   (8) tkEACH      The "each" keyword.
**   (9) tkROW       The "row" keyword.
**  (10) tkKYWD      Any other keyword-like token.
**
** Whitespace never causes a state transition and is always ignored.
** This means that a SQL string of all whitespace is invalid.
**
** If we compile with SQLITE_OMIT_TRIGGER, all of the computation needed
** to recognize the end of a trigger can be omitted.  All we have to do
** is look for a semicolon that is not part of an string or comment.
*/
int sqlite3_complete(const char *zSql){
  u8 state = 9;   /* Current state, using numbers defined in header comment */
  u8 token;       /* Value of the next token */

  /* A complex statement machine used to detect the end of a CREATE TRIGGER
  ** statement.  This is the normal case.
  */
  static const u8 trans[10][11] = {
              /* Token:                                                 */
              /* 0    1  2     3     4      5     6   7   8    9   10   */
/* State:     ** SEMI WS OTHER EXPLN CREATE TRIGR END FOR EACH ROW KYWD */
/* 0   START: */{  0, 0,    1,    2,     3,    1,  1,  7,   1,  1,   1 },
/* 1  NORMAL: */{  0, 1,    1,    1,     1,    1,  1,  1,   1,  1,   1 },
/* 2 EXPLAIN: */{  0, 2,    1,    1,     3,    1,  1,  7,   1,  1,   2 },
/* 3  CREATE: */{  0, 3,    1,    1,     1,    4,  1,  1,   1,  1,   3 },
/* 4 TRIGGER: */{  5, 4,    4,    4,     4,    4,  4,  4,   4,  4,   4 },
/* 5    SEMI: */{  5, 5,    4,    4,     4,    4,  6,  4,   4,  4,   4 },
/* 6     END: */{  0, 6,    4,    4,     4,    4,  4,  4,   4,  4,   4 },
/* 7     FOR: */{  0, 7,    1,    1,     1,    1,  1,  1,   1,  1,   1 },
/* 8    EACH: */{  0, 8,    1,    1,     1,    1,  1,  1,   1,  4,   1 },
/* 9    INIT: */{  0, 9,    1,    2,     3,    1,  1,  7,   1,  1,   1 }
  };


#ifdef SQLITE_ENABLE_API_ARMOR
  if( zSql==0 ){
    (void)SQLITE_MISUSE_BKPT;
    return 0;
  }
#endif

  while( *zSql ){
    switch( *zSql ){
      case ';': {  /* A semicolon */
        token = tkSEMI;
        break;
      }
      case ' ':
      case '\r':
      case '\t':
      case '\n':
      case '\f': {  /* White space is ignored */
        token = tkWS;
        break;
      }
      case '/': {   /* C-style comments */
        if( zSql[1]!='*' ){
          token = tkOTHER;
          break;
        }
        zSql += 2;
        while( zSql[0] && (zSql[0]!='*' || zSql[1]!='/') ){ zSql++; }
        if( zSql[0]==0 ) return 0;
        zSql++;
        token = tkWS;
        break;
      }
      case '-': {   /* SQL-style comments from "--" to end of line */
        if( zSql[1]!='-' ){
          token = tkOTHER;
          break;
        }
        while( *zSql && *zSql!='\n' ){ zSql++; }
        if( *zSql==0 ) return state==1;
        token = tkWS;
        break;
      }
      case '[': {   /* Microsoft-style identifiers in [...] */
        zSql++;
        while( *zSql && *zSql!=']' ){ zSql++; }
        if( *zSql==0 ) return 0;
        token = tkOTHER;
        break;
      }
      case '`':     /* Grave-accent quoted symbols used by MySQL */
      case '"':     /* single- and double-quoted strings */
      case '\'': {
        int c = *zSql;
        zSql++;
        while( *zSql && *zSql!=c ){ zSql++; }
        if( *zSql==0 ) return 0;
        token = tkOTHER;
        break;
      }
      default: {
#ifdef SQLITE_EBCDIC
        unsigned char c;
#endif
        if( IdChar((u8)*zSql) ){
          /* Keywords and unquoted identifiers */
          int nId;
          for(nId=1; IdChar(zSql[nId]); nId++){}
          switch( *zSql ){
            case 'c': case 'C': {
              if( nId==6 && sqlite3StrNICmp(zSql, "create", 6)==0 ){
                token = tkCREATE;
              }else{
                token = tkKYWD;
              }
              break;
            }
            case 'e':  case 'E': {
              if( nId==3 && sqlite3StrNICmp(zSql, "end", 3)==0 ){
                token = tkEND;
              }else
              if( nId==7 && sqlite3StrNICmp(zSql, "explain", 7)==0 ){
                token = tkEXPLAIN;
              }else
              if( nId==4 && sqlite3StrNICmp(zSql, "each", 4)==0 ){
                token = tkEACH;
              }else{
                token = tkKYWD;
              }
              break;
            }
            case 'f': case 'F': {
              if( nId==3 && sqlite3StrNICmp(zSql, "for", 3)==0 ){
                token = tkFOR;
              }else{
                token = tkKYWD;
              }
              break;
            }
            case 'r': case 'R': {
              if( nId==3 && sqlite3StrNICmp(zSql, "row", 3)==0 ){
                token = tkROW;
              }else{
                token = tkKYWD;
              }
              break;
            }
            case 't': case 'T': {
              if( nId==7 && sqlite3StrNICmp(zSql, "trigger", 7)==0 ){
                token = tkTRIGGER;
              }else{
                token = tkKYWD;
              }
              break;
            }
            default: {
              token = tkKYWD;
              break;
            }
          }
          zSql += nId-1;
        }else{
          /* Operators and special symbols */
          token = tkOTHER;
        }
        break;
      }
    }
#if 0
    printf("COMPLETER:  %d.%d -> %d\n", state, token, trans[state][token]);
#endif
    state = trans[state][token];
    zSql++;
  }
  return state==0;
}

#ifndef SQLITE_OMIT_UTF16
/*
** This routine is the same as the sqlite3_complete() routine described
** above, except that the parameter is required to be UTF-16 encoded, not
** UTF-8.
*/
int sqlite3_complete16(const void *zSql){
  sqlite3_value *pVal;
  char const *zSql8;
  int rc;

#ifndef SQLITE_OMIT_AUTOINIT
  rc = sqlite3_initialize();
  if( rc ) return rc;
#endif
  pVal = sqlite3ValueNew(0);
  sqlite3ValueSetStr(pVal, -1, zSql, SQLITE_UTF16NATIVE, SQLITE_STATIC);
  zSql8 = sqlite3ValueText(pVal, SQLITE_UTF8);
  if( zSql8 ){
    rc = sqlite3_complete(zSql8);
  }else{
    rc = SQLITE_NOMEM_BKPT;
  }
  sqlite3ValueFree(pVal);
  return rc & 0xff;
}
#endif /* SQLITE_OMIT_UTF16 */
#endif /* SQLITE_OMIT_COMPLETE */
