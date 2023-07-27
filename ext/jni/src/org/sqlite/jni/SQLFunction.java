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
package org.sqlite.jni;

/**
   SQLFunction is used in conjunction with the
   sqlite3_create_function() JNI-bound API to give that native code
   access to the callback functions needed in order to implement SQL
   functions in Java. This class is not used by itself: see the
   three inner classes.
*/
public abstract class SQLFunction {

  //! Subclass for creating scalar functions.
  public static abstract class Scalar extends SQLFunction {
    public abstract void xFunc(sqlite3_context cx, sqlite3_value[] args);
    /**
       Optionally override to be notified when the function is
       finalized by SQLite.
    */
    public void xDestroy() {}
  }

  //! Subclass for creating aggregate functions.
  public static abstract class Aggregate extends SQLFunction {
    public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xFinal(sqlite3_context cx);
    public void xDestroy() {}
  }

  //! Subclass for creating window functions.
  public static abstract class Window extends SQLFunction {
    public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xInverse(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xFinal(sqlite3_context cx);
    public abstract void xValue(sqlite3_context cx);
    public void xDestroy() {}
  }
}
