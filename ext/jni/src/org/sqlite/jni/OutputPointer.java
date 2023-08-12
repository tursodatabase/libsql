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
   Helper classes for handling JNI output pointers.

   We do not use a generic OutputPointer<T> because working with those
   from the native JNI code is unduly quirky due to a lack of
   autoboxing at that level.
*/
public final class OutputPointer {

  /**
     Output pointer for use with routines, such as sqlite3_open(),
     which return a database handle via an output pointer. These
     pointers can only be set by the JNI layer, not by client-level
     code.
  */
  public static final class sqlite3 {
    private org.sqlite.jni.sqlite3 value;
    public sqlite3(){value = null;}
    public void clear(){value = null;}
    public final org.sqlite.jni.sqlite3 getValue(){return value;}
    /** Equivalent to calling getValue() then clear(). */
    public final org.sqlite.jni.sqlite3 takeValue(){
      final org.sqlite.jni.sqlite3 v = value;
      value = null;
      return v;
    }
  }

  /**
     Output pointer for use with routines, such as sqlite3_prepare(),
     which return a statement handle via an output pointer. These
     pointers can only be set by the JNI layer, not by client-level
     code.
  */
  public static final class sqlite3_stmt {
    private org.sqlite.jni.sqlite3_stmt value;
    public sqlite3_stmt(){value = null;}
    public void clear(){value = null;}
    public final org.sqlite.jni.sqlite3_stmt getValue(){return value;}
    /** Equivalent to calling getValue() then clear(). */
    public final org.sqlite.jni.sqlite3_stmt takeValue(){
      final org.sqlite.jni.sqlite3_stmt v = value;
      value = null;
      return v;
    }
  }

  /**
     Output pointer for use with native routines which return integers via
     output pointers.
  */
  public static final class Int32 {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public int value;
    public Int32(){this(0);}
    public Int32(int v){value = v;}
    public final int getValue(){return value;}
    public final void setValue(int v){value = v;}
  }

  /**
     Output pointer for use with native routines which return 64-bit integers
     via output pointers.
  */
  public static final class Int64 {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public long value;
    public Int64(){this(0);}
    public Int64(long v){value = v;}
    public final long getValue(){return value;}
    public final void setValue(long v){value = v;}
  }

  /**
     Output pointer for use with native routines which return strings via
     output pointers.
  */
  public static final class String {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public java.lang.String value;
    public String(){this(null);}
    public String(java.lang.String v){value = v;}
    public final java.lang.String getValue(){return value;}
    public final void setValue(java.lang.String v){value = v;}
  }

  /**
     Output pointer for use with native routines which return byte
     arrays via output pointers.
  */
  public static final class ByteArray {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public byte[] value;
    public ByteArray(){this(null);}
    public ByteArray(byte[] v){value = v;}
    public final byte[] getValue(){return value;}
    public final void setValue(byte[] v){value = v;}
  }
}
