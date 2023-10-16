/*
** 2023-07-21
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
   A wrapper for communicating C-level (sqlite3_stmt*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.
*/
public final class sqlite3_stmt extends NativePointerHolder<sqlite3_stmt> {
  // Only invoked from JNI.
  private sqlite3_stmt(){}
}
