/*
** 2023-07-22
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
*/
public abstract class Collation {
  /**
     Must compare the given byte arrays using memcmp() semantics.
  */
  public abstract int xCompare(byte[] lhs, byte[] rhs);
  /**
     Called by SQLite when the collation is destroyed. If a Collation
     requires custom cleanup, override this method.
  */
  public void xDestroy() {}
}
