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
   SQL functions (a.k.a. UDFs).
*/
public final class sqlite3_context extends NativePointerHolder<sqlite3_context> {
  /**
     For use only by the JNI layer. It's permitted to set this even
     though it's private.
  */
  private long aggregateContext = 0;

  /**
     getAggregateContext() corresponds to C's
     sqlite3_aggregate_context(), with a slightly different interface
     to account for cross-language differences. It serves the same
     purposes in a slightly different way: it provides a key which is
     stable across invocations of "matching sets" of a UDF's callbacks,
     such that all calls into those callbacks can determine which "set"
     of those calls they belong to.

     If this object is being used in the context of an aggregate or
     window UDF, this function returns a non-0 value which is distinct
     for each set of UDF callbacks from a single invocation of the
     UDF, otherwise it returns 0. The returned value is only only
     valid within the context of execution of a single SQL statement,
     and may be re-used by future invocations of the UDF in different
     SQL statements.

     Consider this SQL, where MYFUNC is a user-defined aggregate function:

     SELECT MYFUNC(A), MYFUNC(B) FROM T;

     The xStep() and xFinal() methods of the callback need to be able
     to differentiate between those two invocations in order to
     perform their work properly. The value returned by
     getAggregateContext() will be distinct for each of those
     invocations of MYFUNC() and is intended to be used as a lookup
     key for mapping callback invocations to whatever client-defined
     state is needed by the UDF.

     There is one case where this will return 0 in the context of an
     aggregate or window function: if the result set has no rows,
     the UDF's xFinal() will be called without any other x...() members
     having been called. In that one case, no aggregate context key will
     have been generated. xFinal() implementations need to be prepared to
     accept that condition as legal.
  */
  public long getAggregateContext(){
    return aggregateContext;
  }
}
