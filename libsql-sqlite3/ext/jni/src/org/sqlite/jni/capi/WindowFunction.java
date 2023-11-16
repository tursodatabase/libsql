/*
** 2023-08-25
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
   A SQLFunction implementation for window functions.  Note that
   WindowFunction inherits from {@link AggregateFunction} and each
   instance is required to implement the inherited abstract methods
   from that class. See {@link AggregateFunction} for information on
   managing the UDF's invocation-specific state.
*/
public abstract class WindowFunction<T> extends AggregateFunction<T> {

  /**
     As for the xInverse() argument of the C API's
     sqlite3_create_window_function(). If this function throws, the
     exception is not propagated and a warning might be emitted
     to a debugging channel.
  */
  public abstract void xInverse(sqlite3_context cx, sqlite3_value[] args);

  /**
     As for the xValue() argument of the C API's sqlite3_create_window_function().
     See xInverse() for the fate of any exceptions this throws.
  */
  public abstract void xValue(sqlite3_context cx);
}
