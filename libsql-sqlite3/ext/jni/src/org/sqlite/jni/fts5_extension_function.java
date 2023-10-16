/*
** 2023-08-05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file is part of the JNI bindings for the sqlite3 C API.
*/
package org.sqlite.jni;

/**
   JNI-level wrapper for C's fts5_extension_function type.

*/
public abstract class fts5_extension_function {
  // typedef void (*fts5_extension_function)(
  //   const Fts5ExtensionApi *pApi,   /* API offered by current FTS version */
  //   Fts5Context *pFts,              /* First arg to pass to pApi functions */
  //   sqlite3_context *pCtx,          /* Context for returning result/error */
  //   int nVal,                       /* Number of values in apVal[] array */
  //   sqlite3_value **apVal           /* Array of trailing arguments */
  // );

  /**
     The callback implementation, corresponding to the xFunction
     argument of C's fts5_api::xCreateFunction().
  */
  public abstract void xFunction(Fts5ExtensionApi ext, Fts5Context fCx,
                                 sqlite3_context pCx, sqlite3_value argv[]);
  //! Optionally override
  public void xDestroy(){}
}
