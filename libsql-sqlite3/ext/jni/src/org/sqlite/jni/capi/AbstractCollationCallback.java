/*
** 2023-08-25
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
package org.sqlite.jni.capi;
import org.sqlite.jni.annotation.NotNull;

/**
   An implementation of {@link CollationCallback} which provides a
   no-op xDestroy() method.
*/
public abstract class AbstractCollationCallback
  implements CollationCallback, XDestroyCallback {
  /**
     Must compare the given byte arrays and return the result using
     {@code memcmp()} semantics.
  */
  public abstract int call(@NotNull byte[] lhs, @NotNull byte[] rhs);

  /**
     Optionally override to be notified when the UDF is finalized by
     SQLite. This implementation does nothing.
  */
  public void xDestroy(){}
}
