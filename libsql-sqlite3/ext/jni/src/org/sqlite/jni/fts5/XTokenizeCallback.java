/*
** 2023-08-04
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
   Callback type for use with xTokenize() variants.
*/
public interface XTokenizeCallback {
  int call(int tFlags, byte[] txt, int iStart, int iEnd);
}
