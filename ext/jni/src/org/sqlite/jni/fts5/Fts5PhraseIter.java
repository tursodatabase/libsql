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
import org.sqlite.jni.capi.NativePointerHolder;

/**
   A wrapper for C-level Fts5PhraseIter. They are only modified and
   inspected by native-level code.
*/
public final class Fts5PhraseIter extends NativePointerHolder<Fts5PhraseIter> {
  //! Updated and used only by native code.
  private long a;
  private long b;
}
