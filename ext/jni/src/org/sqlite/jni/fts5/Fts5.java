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
package org.sqlite.jni.fts5;

/**
   INCOMPLETE AND COMPLETELY UNTESTED.

   A utility object for holding FTS5-specific types and constants
   which are used by multiple FTS5 classes.
*/
public final class Fts5 {
  /* Not used */
  private Fts5(){}


  public static final int FTS5_TOKENIZE_QUERY    = 0x0001;
  public static final int FTS5_TOKENIZE_PREFIX   = 0x0002;
  public static final int FTS5_TOKENIZE_DOCUMENT = 0x0004;
  public static final int FTS5_TOKENIZE_AUX      = 0x0008;
  public static final int FTS5_TOKEN_COLOCATED   = 0x0001;
}
