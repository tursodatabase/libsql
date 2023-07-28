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
   sqlite3_context instances are used in conjunction with user-defined
   SQL functions (a.k.a. UDFs). They are opaque pointers.

   The getAggregateContext() method corresponds to C's
   sqlite3_aggregate_context(), with a slightly different interface in
   order to account for cross-language differences. It serves the same
   purposes in a slightly different way: it provides a key which is
   stable across invocations of UDF xStep() and xFinal() pairs, to
   which a UDF may map state across such calls (e.g. a numeric result
   which is being accumulated).
*/
public class sqlite3_context extends NativePointerHolder<sqlite3_context> {
  public sqlite3_context() {
    super();
  }
  private long aggcx = 0;

  /**
     If this object is being used in the context of an aggregate or
     window UDF, the UDF binding layer will set a unique context value
     here, else this will return 0. That value will be the same across
     matching calls to the UDF callbacks. This value can be used as a
     key to map state which needs to persist across such calls, noting
     that such state should be cleaned up via xFinal().
  */
  public long getAggregateContext(){
    return aggcx;
  }

  /**
     For use only by the JNI layer. It's permitted to call this even
     though it's private.
  */
  private void setAggregateContext(long n){
    aggcx = n;
  }
}
