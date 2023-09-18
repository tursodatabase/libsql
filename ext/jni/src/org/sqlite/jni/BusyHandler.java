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
   Callback proxy for use with sqlite3_busy_handler().
*/
public abstract class BusyHandler {
  /**
     Must function as documented for the sqlite3_busy_handler()
     callback argument, minus the (void*) argument the C-level
     function requires.

     Any exceptions thrown by this callback are suppressed in order to
     retain the C-style API semantics of the JNI bindings.
  */
  public abstract int xCallback(int n);

  /**
     Optionally override to perform any cleanup when this busy
     handler is destroyed. It is destroyed when:

     - The associated db is passed to sqlite3_close() or
       sqlite3_close_v2().

     - sqlite3_busy_handler() is called to replace the handler,
       whether it's passed a null handler or any other instance of
       this class.

     - sqlite3_busy_timeout() is called, which implicitly installs
       a busy handler.
  */
  public void xDestroy(){}
}
