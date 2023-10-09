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
   A wrapper for communicating C-level (sqlite3*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java
   and C via JNI.
*/
public final class sqlite3 extends NativePointerHolder<sqlite3>
 implements AutoCloseable {

  // Only invoked from JNI
  private sqlite3(){}

  public String toString(){
    final long ptr = getNativePointer();
    if( 0==ptr ){
      return sqlite3.class.getSimpleName()+"@null";
    }
    final String fn = CApi.sqlite3_db_filename(this, "main");
    return sqlite3.class.getSimpleName()
      +"@"+String.format("0x%08x",ptr)
      +"["+((null == fn) ? "<unnamed>" : fn)+"]"
      ;
  }

  @Override public void close(){
    CApi.sqlite3_close_v2(this.clearNativePointer());
  }
}
