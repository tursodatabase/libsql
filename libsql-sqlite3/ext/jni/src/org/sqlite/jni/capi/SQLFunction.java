/*
** 2023-07-22
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
   SQLFunction is used in conjunction with the
   sqlite3_create_function() JNI-bound API to give that native code
   access to the callback functions needed in order to implement SQL
   functions in Java.

   <p>

   This class is not used by itself, but is a marker base class. The
   three UDF types are modelled by the inner classes Scalar,
   Aggregate<T>, and Window<T>. Most simply, clients may subclass
   those, or create anonymous classes from them, to implement
   UDFs. Clients are free to create their own classes for use with
   UDFs, so long as they conform to the public interfaces defined by
   those three classes. The JNI layer only actively relies on the
   SQLFunction base class and the method names and signatures used by
   the UDF callback interfaces.
*/
public interface SQLFunction {

}
