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
package org.sqlite.jni;
import org.sqlite.jni.annotation.*;

/**
   Callback for use with {@link CApi#sqlite3_set_authorizer}.
*/
public interface AuthorizerCallback extends CallbackProxy {
  /**
     Must function as described for the C-level
     sqlite3_set_authorizer() callback.
  */
  int call(int opId, @Nullable String s1, @Nullable String s2,
           @Nullable String s3, @Nullable String s4);

}
