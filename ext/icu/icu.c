
/*
** This file implements an integration between the ICU library 
** ("International Components for Unicode", an open-source library 
** for handling unicode data) and SQLite. The integration uses 
** ICU to provide the following to SQLite:
**
**   * Implementations of the SQL scalar upper() and lower() 
**     functions for case mapping, 
**
**   * Collation sequences
**
**   * Implementation of the SQL regexp() function (and hence REGEXP
**     operator) using the ICU uregex_XX() APIs.
**
**   * LIKE
*/

#if !defined(SQLITE_CORE) || defined(SQLITE_ENABLE_ICU)

#include <unicode/utypes.h>
#include <unicode/uregex.h>
#include <unicode/ustring.h>

#include <assert.h>
#include "sqlite3.h"

#ifndef SQLITE_CORE
  #include "sqlite3ext.h"
  SQLITE_EXTENSION_INIT1
#endif

/*
** Collation sequences:
**
**   ucol_open()
**   ucol_strcoll()
**   ucol_close()
*/

/*
** Version of sqlite3_free() that is always a function, never a macro.
*/
static void xFree(void *p){
  sqlite3_free(p);
}

/*
** LIKE operator.
**
** http://unicode.org/reports/tr21/tr21-5.html#Caseless_Matching
*/

/*
** Function to delete compiled regexp objects. Registered as
** a destructor function with sqlite3_set_auxdata().
*/
static void icuRegexpDelete(void *p){
  URegularExpression *pExpr = (URegularExpression *)p;
  uregex_close(pExpr);
}

/*
** Implementation of SQLite REGEXP operator. This scalar function takes
** two arguments. The first is a regular expression pattern to compile
** the second is a string to match against that pattern. If either 
** argument is an SQL NULL, then NULL Is returned. Otherwise, the result
** is 1 if the string matches the pattern, or 0 otherwise.
**
** SQLite maps the regexp() function to the regexp() operator such
** that the following two are equivalent:
**
**     zString REGEXP zPattern
**     regexp(zPattern, zString)
**
** Uses the following ICU regexp APIs:
**
**     uregex_open()
**     uregex_matches()
**     uregex_close()
*/
static void icuRegexpFunc(sqlite3_context *p, int nArg, sqlite3_value **apArg){
  UErrorCode status = U_ZERO_ERROR;
  URegularExpression *pExpr;
  UBool res;
  const UChar *zString = sqlite3_value_text16(apArg[1]);

  /* If the left hand side of the regexp operator is NULL, 
  ** then the result is also NULL. 
  */
  if( !zString ){
    return;
  }

  pExpr = sqlite3_get_auxdata(p, 0);
  if( !pExpr ){
    const UChar *zPattern = sqlite3_value_text16(apArg[0]);
    if( !zPattern ){
      return;
    }
    pExpr = uregex_open(zPattern, -1, 0, 0, &status);

    if( U_SUCCESS(status) ){
      sqlite3_set_auxdata(p, 0, pExpr, icuRegexpDelete);
    }else{
      assert(!pExpr);
      sqlite3_result_error(p, "Error compiling regular expression", -1);
      return;
    }
  }

  /* Configure the text that the regular expression operates on. */
  uregex_setText(pExpr, zString, -1, &status);
  if( !U_SUCCESS(status) ){
    sqlite3_result_error(p, "Error configuring regular expression", -1);
    return;
  }

  /* Attempt the match */
  res = uregex_matches(pExpr, 0, &status);
  if( !U_SUCCESS(status) ){
    sqlite3_result_error(p, "Error matching regular expression", -1);
    return;
  }

  /* Set the text that the regular expression operates on to a NULL
  ** pointer. This is not really necessary, but it is tidier than 
  ** leaving the regular expression object configured with an invalid
  ** pointer after this function returns.
  */
  uregex_setText(pExpr, 0, 0, &status);

  /* Return 1 or 0. */
  sqlite3_result_int(p, res ? 1 : 0);
}

/*
** Implementations of scalar functions for case mapping - upper() and 
** lower(). Function upper() converts it's input to upper-case (ABC).
** Function lower() converts to lower-case (abc).
**
** ICU provides two types of case mapping, "general" case mapping and
** "language specific". Refer to ICU documentation for the differences
** between the two.
**
** To utilise "general" case mapping, the upper() or lower() scalar 
** functions are invoked with one argument:
**
**     upper('ABC') -> 'abc'
**     lower('abc') -> 'ABC'
**
** To access ICU "language specific" case mapping, upper() or lower()
** should be invoked with two arguments. The second argument is the name
** of the locale to use. Passing an empty string ("") or SQL NULL value
** as the second argument is the smae as invoking the 1 argument version
** of upper() or lower().
**
**     lower('I', 'en_us') -> 'i'
**     lower('I', 'tr_tr') -> 'ı' (small dotless i)
**
** http://www.icu-project.org/userguide/posix.html#case_mappings
*/
static void icuCaseFunc16(sqlite3_context *p, int nArg, sqlite3_value **apArg){
  const UChar *zInput;
  UChar *zOutput;
  int nInput;
  int nOutput;

  UErrorCode status = U_ZERO_ERROR;
  const char *zLocale = 0;

  assert(nArg==1 || nArg==2);
  if( nArg==2 ){
    zLocale = (const char *)sqlite3_value_text(apArg[1]);
  }

  zInput = sqlite3_value_text16(apArg[0]);
  nInput = sqlite3_value_bytes16(apArg[0]);

  nOutput = nInput * 2 + 2;
  zOutput = sqlite3_malloc(nInput*2+2);
  if( !zOutput ){
    return;
  }

  if( sqlite3_user_data(p) ){
    u_strToUpper(zOutput, nOutput/2, zInput, nInput/2, zLocale, &status);
  }else{
    u_strToLower(zOutput, nOutput/2, zInput, nInput/2, zLocale, &status);
  }

  if( !U_SUCCESS(status) ){
    sqlite3_result_error(p, "Error converting case", -1);
    return;
  }

  sqlite3_result_text16(p, zOutput, -1, xFree);
}

/*
** Register the ICU extension functions with database db.
*/
int sqlite3IcuInit(sqlite3 *db){
  struct IcuScalar {
    const char *zName;                        /* Function name */
    int nArg;                                 /* Number of arguments */
    int enc;                                  /* Optimal text encoding */
    void *pContext;                           /* sqlite3_user_data() context */
    void (*xFunc)(sqlite3_context*,int,sqlite3_value**);
  } scalars[] = {
    {"regexp", 2, SQLITE_ANY,          0, icuRegexpFunc},

    {"lower",  1, SQLITE_UTF16,        0, icuCaseFunc16},
    {"lower",  2, SQLITE_UTF16,        0, icuCaseFunc16},
    {"upper",  1, SQLITE_UTF16, (void*)1, icuCaseFunc16},
    {"upper",  2, SQLITE_UTF16, (void*)1, icuCaseFunc16},

    {"lower",  1, SQLITE_UTF8,         0, icuCaseFunc16},
    {"lower",  2, SQLITE_UTF8,         0, icuCaseFunc16},
    {"upper",  1, SQLITE_UTF8,  (void*)1, icuCaseFunc16},
    {"upper",  2, SQLITE_UTF8,  (void*)1, icuCaseFunc16},
  };

  int rc = SQLITE_OK;
  int i;

  for(i=0; rc==SQLITE_OK && i<(sizeof(scalars)/sizeof(struct IcuScalar)); i++){
    struct IcuScalar *p = &scalars[i];
    rc = sqlite3_create_function(
        db, p->zName, p->nArg, p->enc, p->pContext, p->xFunc, 0, 0
    );
  }

  return rc;
}

#if !SQLITE_CORE
int sqlite3_extension_init(
  sqlite3 *db, 
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  SQLITE_EXTENSION_INIT2(pApi)
  return sqlite3IcuInit(db);
}
#endif

#endif
