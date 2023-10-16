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
   Callback for use with {@link CApi#sqlite3_create_collation}.

   @see AbstractCollationCallback
*/
public interface CollationCallback
  extends CallbackProxy, XDestroyCallback {
  /**
     Must compare the given byte arrays and return the result using
     {@code memcmp()} semantics.
  */
  int call(@NotNull byte[] lhs, @NotNull byte[] rhs);

  /**
     Called by SQLite when the collation is destroyed. If a collation
     requires custom cleanup, override this method.
  */
  void xDestroy();
}
