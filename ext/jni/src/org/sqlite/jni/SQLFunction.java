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

   Note that if a given function is called multiple times in a single
   SQL statement, e.g. SELECT MYFUNC(A), MYFUNC(B)..., then the
   context object passed to each one will be different. This is most
   significant for aggregates and window functions, since they must
   assign their results to the proper context.

   TODO: add helper APIs to map sqlite3_context instances to
   func-specific state and to clear that when the aggregate or window
   function is done.
*/
public abstract class SQLFunction {

  /**
     ContextMap is a helper for use with aggregate and window
     functions, to help them manage their accumulator state across
     calls to xStep() and xFinal(). It works by mapping
     sqlite3_context::getAggregateContext() to a single piece of state
     which persists across a set of 0 or more SQLFunction.xStep()
     calls and 1 SQLFunction.xFinal() call.
   */
  public static final class ContextMap<T> {
    private java.util.Map<Long,ValueHolder<T>> map
      = new java.util.HashMap<Long,ValueHolder<T>>();

    /**
       Should be called from a UDF's xStep() method, passing it that
       method's first argument and an initial value for the persistent
       state. If there is currently no mapping for
       cx.getAggregateContext() within the map, one is created, else
       an existing one is preferred.  It returns a ValueHolder which
       can be used to modify that state directly without having to put
       a new result back in the underlying map.
    */
    public ValueHolder<T> xStep(sqlite3_context cx, T initialValue){
      ValueHolder<T> rc = map.get(cx.getAggregateContext());
      if(null == rc){
        map.put(cx.getAggregateContext(), rc = new ValueHolder<T>(initialValue));
      }
      return rc;
    }

    /**
       Should be called from a UDF's xFinal() method and passed that
       method's first argument. This function returns the value
       associated with cx.getAggregateContext(), or null if
       this.xStep() has not been called to set up such a mapping. That
       will be the case if an aggregate is used in a statement which
       has no result rows.
    */
    public T xFinal(sqlite3_context cx){
      final ValueHolder<T> h = map.remove(cx.getAggregateContext());
      return null==h ? null : h.value;
    }
  }

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
  public static abstract class Aggregate<T> extends SQLFunction {
    public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xFinal(sqlite3_context cx);
    public void xDestroy() {}

    private final ContextMap<T> map = new ContextMap<>();

    /**
       See ContextMap<T>.xStep().
    */
    public final ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
      return map.xStep(cx, initialValue);
    }

    /**
       See ContextMap<T>.xFinal().
    */
    public final T takeAggregateState(sqlite3_context cx){
      return map.xFinal(cx);
    }
  }

  //! Subclass for creating window functions.
  public static abstract class Window<T> extends Aggregate<T> {
    public Window(){
      super();
    }
    //public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xInverse(sqlite3_context cx, sqlite3_value[] args);
    //public abstract void xFinal(sqlite3_context cx);
    public abstract void xValue(sqlite3_context cx);
  }
}
