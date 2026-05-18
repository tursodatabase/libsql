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
package org.sqlite.jni.capi;

/**
   Helper classes for handling JNI output pointers.

   <p>We do not use a generic OutputPointer<T> because working with those
   from the native JNI code is unduly quirky due to a lack of
   autoboxing at that level.

   <p>The usage is similar for all of these types:

   <pre>{@code
   OutputPointer.sqlite3 out = new OutputPointer.sqlite3();
   assert( null==out.get() );
   int rc = sqlite3_open(":memory:", out);
   if( 0!=rc ) ... error;
   assert( null!=out.get() );
   sqlite3 db = out.take();
   assert( null==out.get() );
   }</pre>

   <p>With the minor exception that the primitive types permit direct
   access to the object's value via the `value` property, whereas the
   JNI-level opaque types do not permit client-level code to set that
   property.

   <p>Warning: do not share instances of these classes across
   threads. Doing so may lead to corrupting sqlite3-internal state.
*/
public final class OutputPointer {

  /**
     Output pointer for use with routines, such as sqlite3_open(),
     which return a database handle via an output pointer. These
     pointers can only be set by the JNI layer, not by client-level
     code.
  */
  public static final class sqlite3 {
    private org.sqlite.jni.capi.sqlite3 value;
    /** Initializes with a null value. */
    public sqlite3(){value = null;}
    /** Sets the current value to null. */
    public void clear(){value = null;}
    /** Returns the current value. */
    public org.sqlite.jni.capi.sqlite3 get(){return value;}
    /** Equivalent to calling get() then clear(). */
    public org.sqlite.jni.capi.sqlite3 take(){
      final org.sqlite.jni.capi.sqlite3 v = value;
      value = null;
      return v;
    }
  }

  /**
     Output pointer for sqlite3_blob_open(). These
     pointers can only be set by the JNI layer, not by client-level
     code.
  */
  public static final class sqlite3_blob {
    private org.sqlite.jni.capi.sqlite3_blob value;
    /** Initializes with a null value. */
    public sqlite3_blob(){value = null;}
    /** Sets the current value to null. */
    public void clear(){value = null;}
    /** Returns the current value. */
    public org.sqlite.jni.capi.sqlite3_blob get(){return value;}
    /** Equivalent to calling get() then clear(). */
    public org.sqlite.jni.capi.sqlite3_blob take(){
      final org.sqlite.jni.capi.sqlite3_blob v = value;
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
    private org.sqlite.jni.capi.sqlite3_stmt value;
    /** Initializes with a null value. */
    public sqlite3_stmt(){value = null;}
    /** Sets the current value to null. */
    public void clear(){value = null;}
    /** Returns the current value. */
    public org.sqlite.jni.capi.sqlite3_stmt get(){return value;}
    /** Equivalent to calling get() then clear(). */
    public org.sqlite.jni.capi.sqlite3_stmt take(){
      final org.sqlite.jni.capi.sqlite3_stmt v = value;
      value = null;
      return v;
    }
  }

  /**
     Output pointer for use with routines, such as sqlite3_prepupdate_new(),
     which return a sqlite3_value handle via an output pointer. These
     pointers can only be set by the JNI layer, not by client-level
     code.
  */
  public static final class sqlite3_value {
    private org.sqlite.jni.capi.sqlite3_value value;
    /** Initializes with a null value. */
    public sqlite3_value(){value = null;}
    /** Sets the current value to null. */
    public void clear(){value = null;}
    /** Returns the current value. */
    public org.sqlite.jni.capi.sqlite3_value get(){return value;}
    /** Equivalent to calling get() then clear(). */
    public org.sqlite.jni.capi.sqlite3_value take(){
      final org.sqlite.jni.capi.sqlite3_value v = value;
      value = null;
      return v;
    }
  }

  /**
     Output pointer for use with native routines which return booleans
     via integer output pointers.
  */
  public static final class Bool {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public boolean value;
    /** Initializes with the value 0. */
    public Bool(){this(false);}
    /** Initializes with the value v. */
    public Bool(boolean v){value = v;}
    /** Returns the current value. */
    public boolean get(){return value;}
    /** Sets the current value to v. */
    public void set(boolean v){value = v;}
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
    /** Initializes with the value 0. */
    public Int32(){this(0);}
    /** Initializes with the value v. */
    public Int32(int v){value = v;}
    /** Returns the current value. */
    public int get(){return value;}
    /** Sets the current value to v. */
    public void set(int v){value = v;}
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
    /** Initializes with the value 0. */
    public Int64(){this(0);}
    /** Initializes with the value v. */
    public Int64(long v){value = v;}
    /** Returns the current value. */
    public long get(){return value;}
    /** Sets the current value. */
    public void set(long v){value = v;}
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
    /** Initializes with a null value. */
    public String(){this(null);}
    /** Initializes with the value v. */
    public String(java.lang.String v){value = v;}
    /** Returns the current value. */
    public java.lang.String get(){return value;}
    /** Sets the current value. */
    public void set(java.lang.String v){value = v;}
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
    /** Initializes with the value null. */
    public ByteArray(){this(null);}
    /** Initializes with the value v. */
    public ByteArray(byte[] v){value = v;}
    /** Returns the current value. */
    public byte[] get(){return value;}
    /** Sets the current value. */
    public void set(byte[] v){value = v;}
  }

  /**
     Output pointer for use with native routines which return
     blobs via java.nio.ByteBuffer.

     See {@link org.sqlite.jni.capi.CApi#sqlite3_jni_supports_nio}
  */
  public static final class ByteBuffer {
    /**
       This is public for ease of use. Accessors are provided for
       consistency with the higher-level types.
    */
    public java.nio.ByteBuffer value;
    /** Initializes with the value null. */
    public ByteBuffer(){this(null);}
    /** Initializes with the value v. */
    public ByteBuffer(java.nio.ByteBuffer v){value = v;}
    /** Returns the current value. */
    public java.nio.ByteBuffer get(){return value;}
    /** Sets the current value. */
    public void set(java.nio.ByteBuffer v){value = v;}
  }
}
