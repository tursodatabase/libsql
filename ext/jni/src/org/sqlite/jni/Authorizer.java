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
   A callback for use with sqlite3_set_authorizer().
*/
public interface Authorizer {
  /**
     Must functions as described for the sqlite3_set_authorizer()
     callback, with one caveat: the string values passed here were
     initially (at the C level) encoded in standard UTF-8. If they
     contained any constructs which are not compatible with MUTF-8,
     these strings will not have the expected values. For further
     details, see the documentation for the SQLite3Jni class.

     Must not throw.
  */
  int xAuth(int opId, @Nullable String s1, @Nullable String s2,
            @Nullable String s3, @Nullable String s4);
}
