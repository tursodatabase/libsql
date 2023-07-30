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
   Helper classes for handling JNI output pointers for primitive
   types. Higher-level classes which use output pointers have their
   own corresponding Java class, e.g. sqlite3 and sqlite3_stmt.

   We do not use a generic OutputPointer<T> because working with those
   from the native JNI code is unduly quirky due to a lack of
   autoboxing at that level.
*/
public final class OutputPointer {
  public static final class Int32 {
    //! Only set from the JNI layer.
    private int value;
    public final int getValue(){return value;}
  }
  public static final class Int64 {
    //! Only set from the JNI layer.
    private long value;
    public final long getValue(){return value;}
  }
}
