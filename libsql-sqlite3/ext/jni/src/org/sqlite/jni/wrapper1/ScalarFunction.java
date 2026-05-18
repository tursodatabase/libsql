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
   The SqlFunction type for scalar SQL functions.
*/
public abstract class ScalarFunction implements SqlFunction  {
  /**
     As for the xFunc() argument of the C API's
     sqlite3_create_function(). If this function throws, it is
     translated into an sqlite3_result_error().
  */
  public abstract void xFunc(SqlFunction.Arguments args);

  /**
     Optionally override to be notified when the UDF is finalized by
     SQLite. This default implementation does nothing.
  */
  public void xDestroy() {}

}
