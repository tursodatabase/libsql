/*
** 2023-08-05x
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

   A wrapper for communicating C-level (Fts5Tokenizer*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.

   At the C level, the Fts5Tokenizer type is essentially a void
   pointer used specifically for tokenizers.
*/
public final class Fts5Tokenizer extends NativePointerHolder<Fts5Tokenizer> {
  //! Only called from JNI.
  private Fts5Tokenizer(){}
}
