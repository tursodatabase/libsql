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
   INCOMPLETE AND COMPLETELY UNTESTED.

   A wrapper for communicating C-level (fts5_api*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.
*/
public final class Fts5 {
  /* Not used */
  private Fts5(){}

  //! Callback type for use with xTokenize() variants
  public static interface xTokenizeCallback {
    int xToken(int tFlags, byte txt[], int iStart, int iEnd);
  }

  public static final int FTS5_TOKENIZE_QUERY    = 0x0001;
  public static final int FTS5_TOKENIZE_PREFIX   = 0x0002;
  public static final int FTS5_TOKENIZE_DOCUMENT = 0x0004;
  public static final int FTS5_TOKENIZE_AUX      = 0x0008;
  public static final int FTS5_TOKEN_COLOCATED   = 0x0001;
}
