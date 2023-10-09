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
  private Long aggregateContext = null;

  /**
     getAggregateContext() corresponds to C's
     sqlite3_aggregate_context(), with a slightly different interface
     to account for cross-language differences. It serves the same
     purposes in a slightly different way: it provides a key which is
     stable across invocations of a UDF's callbacks, such that all
     calls into those callbacks can determine which "set" of those
     calls they belong to.

     <p>Note that use of this method is not a requirement for proper use
     of this class. sqlite3_aggregate_context() can also be used.

     <p>If the argument is true and the aggregate context has not yet
     been set up, it will be initialized and fetched on demand, else it
     won't. The intent is that xStep(), xValue(), and xInverse()
     methods pass true and xFinal() methods pass false.

     <p>This function treats numeric 0 as null, always returning null instead
     of 0.

     <p>If this object is being used in the context of an aggregate or
     window UDF, this function returns a non-0 value which is distinct
     for each set of UDF callbacks from a single invocation of the
     UDF, otherwise it returns 0. The returned value is only only
     valid within the context of execution of a single SQL statement,
     and must not be re-used by future invocations of the UDF in
     different SQL statements.

     <p>Consider this SQL, where MYFUNC is a user-defined aggregate function:

     <pre>{@code
     SELECT MYFUNC(A), MYFUNC(B) FROM T;
     }</pre>

     <p>The xStep() and xFinal() methods of the callback need to be able
     to differentiate between those two invocations in order to
     perform their work properly. The value returned by
     getAggregateContext() will be distinct for each of those
     invocations of MYFUNC() and is intended to be used as a lookup
     key for mapping callback invocations to whatever client-defined
     state is needed by the UDF.

     <p>There is one case where this will return null in the context
     of an aggregate or window function: if the result set has no
     rows, the UDF's xFinal() will be called without any other x...()
     members having been called. In that one case, no aggregate
     context key will have been generated. xFinal() implementations
     need to be prepared to accept that condition as legal.
  */
  public synchronized Long getAggregateContext(boolean initIfNeeded){
      if( aggregateContext==null ){
        aggregateContext = CApi.sqlite3_aggregate_context(this, initIfNeeded);
        if( !initIfNeeded && null==aggregateContext ) aggregateContext = 0L;
      }
      return (null==aggregateContext || 0!=aggregateContext) ? aggregateContext : null;
  }
}
