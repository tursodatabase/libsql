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
   A SQLFunction implementation for aggregate functions.  Its T is the
   data type of its "accumulator" state, an instance of which is
   intended to be be managed using the getAggregateState() and
   takeAggregateState() methods.
*/
public abstract class AggregateFunction<T> implements SQLFunction {

  /**
     As for the xStep() argument of the C API's
     sqlite3_create_function().  If this function throws, the
     exception is not propagated and a warning might be emitted to a
     debugging channel.
  */
  public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);

  /**
     As for the xFinal() argument of the C API's sqlite3_create_function().
     If this function throws, it is translated into an sqlite3_result_error().
  */
  public abstract void xFinal(sqlite3_context cx);

  /**
     Optionally override to be notified when the UDF is finalized by
     SQLite.
  */
  public void xDestroy() {}

  /**
     PerContextState assists aggregate and window functions in
     managing their accumulator state across calls to the UDF's
     callbacks.

     <p>T must be of a type which can be legally stored as a value in
     java.util.HashMap<KeyType,T>.

     <p>If a given aggregate or window function is called multiple times
     in a single SQL statement, e.g. SELECT MYFUNC(A), MYFUNC(B)...,
     then the clients need some way of knowing which call is which so
     that they can map their state between their various UDF callbacks
     and reset it via xFinal(). This class takes care of such
     mappings.

     <p>This class works by mapping
     sqlite3_context.getAggregateContext() to a single piece of
     state, of a client-defined type (the T part of this class), which
     persists across a "matching set" of the UDF's callbacks.

     <p>This class is a helper providing commonly-needed functionality
     - it is not required for use with aggregate or window functions.
     Client UDFs are free to perform such mappings using custom
     approaches. The provided {@link AggregateFunction} and {@link
     WindowFunction} classes use this.
  */
  public static final class PerContextState<T> {
    private final java.util.Map<Long,ValueHolder<T>> map
      = new java.util.HashMap<>();

    /**
       Should be called from a UDF's xStep(), xValue(), and xInverse()
       methods, passing it that method's first argument and an initial
       value for the persistent state. If there is currently no
       mapping for the given context within the map, one is created
       using the given initial value, else the existing one is used
       and the 2nd argument is ignored.  It returns a ValueHolder<T>
       which can be used to modify that state directly without
       requiring that the client update the underlying map's entry.

       <p>The caller is obligated to eventually call
       takeAggregateState() to clear the mapping.
    */
    public ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
      final Long key = cx.getAggregateContext(true);
      ValueHolder<T> rc = null==key ? null : map.get(key);
      if( null==rc ){
        map.put(key, rc = new ValueHolder<>(initialValue));
      }
      return rc;
    }

    /**
       Should be called from a UDF's xFinal() method and passed that
       method's first argument. This function removes the value
       associated with cx.getAggregateContext() from the map and
       returns it, returning null if no other UDF method has been
       called to set up such a mapping. The latter condition will be
       the case if a UDF is used in a statement which has no result
       rows.
    */
    public T takeAggregateState(sqlite3_context cx){
      final ValueHolder<T> h = map.remove(cx.getAggregateContext(false));
      return null==h ? null : h.value;
    }
  }

  /** Per-invocation state for the UDF. */
  private final PerContextState<T> map = new PerContextState<>();

  /**
     To be called from the implementation's xStep() method, as well
     as the xValue() and xInverse() methods of the {@link WindowFunction}
     subclass, to fetch the current per-call UDF state. On the
     first call to this method for any given sqlite3_context
     argument, the context is set to the given initial value. On all other
     calls, the 2nd argument is ignored.

     @see AggregateFunction.PerContextState#getAggregateState
  */
  protected final ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
    return map.getAggregateState(cx, initialValue);
  }

  /**
     To be called from the implementation's xFinal() method to fetch
     the final state of the UDF and remove its mapping.

     see AggregateFunction.PerContextState#takeAggregateState
  */
  protected final T takeAggregateState(sqlite3_context cx){
    return map.takeAggregateState(cx);
  }
}
