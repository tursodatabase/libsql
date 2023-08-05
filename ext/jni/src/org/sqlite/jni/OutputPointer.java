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
   types.

   We do not use a generic OutputPointer<T> because working with those
   from the native JNI code is unduly quirky due to a lack of
   autoboxing at that level.
*/
public final class OutputPointer {
  public static final class Int32 {
    private int value;
    public Int32(){this(0);}
    public Int32(int v){value = v;}
    public final int getValue(){return value;}
    public final void setValue(int v){value = v;}
  }

  public static final class Int64 {
    private long value;
    public Int64(){this(0);}
    public Int64(long v){value = v;}
    public final long getValue(){return value;}
    public final void setValue(long v){value = v;}
  }

  public static final class String {
    private java.lang.String value;
    public String(){this(null);}
    public String(java.lang.String v){value = v;}
    public final java.lang.String getValue(){return value;}
    public final void setValue(java.lang.String v){value = v;}
  }

  public static final class ByteArray {
    private byte value[];
    public ByteArray(){this(null);}
    public ByteArray(byte v[]){value = v;}
    public final byte[] getValue(){return value;}
    public final void setValue(byte v[]){value = v;}
  }
}
