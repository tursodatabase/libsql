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
   A helper for passing pointers between JNI C code and Java, in
   particular for output pointers of high-level object types in the
   sqlite3 C API, e.g. (sqlite3**) and (sqlite3_stmt**).  This is
   intended to be subclassed and the ContextType is intended to be the
   class which is doing the subclassing. The intent of the ContextType
   is strictly to provide some level of type safety by avoiding that
   NativePointerHolder is not inadvertently passed to an incompatible
   function signature.

   These objects do not _own_ the pointer they refer to.  They are
   intended simply to communicate that pointer between C and Java.
*/
public class NativePointerHolder<ContextType> {
  //! Only set from JNI, where access permissions don't matter.
  private long nativePointer = 0;
  public final long getNativePointer(){ return nativePointer; }
}
