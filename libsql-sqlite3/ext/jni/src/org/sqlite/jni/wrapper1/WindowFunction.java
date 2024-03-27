/*
** 2023-10-16
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file is part of the wrapper1 interface for sqlite3.
*/
package org.sqlite.jni.wrapper1;

/**
   A SqlFunction implementation for window functions. The T type
   represents the type of data accumulated by this function while it
   works. e.g. a SUM()-like UDF might use Integer or Long and a
   CONCAT()-like UDF might use a StringBuilder or a List<String>.
*/
public abstract class WindowFunction<T> extends AggregateFunction<T>  {

  /**
     As for the xInverse() argument of the C API's
     sqlite3_create_window_function().  If this function throws, the
     exception is reported via sqlite3_result_error().
  */
  public abstract void xInverse(SqlFunction.Arguments args);

  /**
     As for the xValue() argument of the C API's
     sqlite3_create_window_function(). If this function throws, it is
     translated into sqlite3_result_error().

     Note that the passed-in object will not actually contain any
     arguments for xValue() but will contain the context object needed
     for setting the call's result or error state.
  */
  public abstract void xValue(SqlFunction.Arguments args);

}
