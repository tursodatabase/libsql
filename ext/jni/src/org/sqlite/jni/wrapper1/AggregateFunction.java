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
   EXPERIMENTAL/INCOMPLETE/UNTESTED

   A SqlFunction implementation for aggregate functions. The T type
   represents the type of data accumulated by this aggregate while it
   works. e.g. a SUM()-like UDF might use Integer or Long and a
   CONCAT()-like UDF might use a StringBuilder or a List<String>.
*/
public abstract class AggregateFunction<T> implements SqlFunction  {

  /**
     As for the xStep() argument of the C API's
     sqlite3_create_function().  If this function throws, the
     exception is reported via sqlite3_result_error().
  */
  public abstract void xStep(SqlFunction.Arguments args);

  /**
     As for the xFinal() argument of the C API's
     sqlite3_create_function(). If this function throws, it is
     translated into sqlite3_result_error().

     Note that the passed-in object will not actually contain any
     arguments for xFinal() but will contain the context object needed
     for setting the call's result or error state.
  */
  public abstract void xFinal(SqlFunction.Arguments args);

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
    public ValueHolder<T> getAggregateState(SqlFunction.Arguments args, T initialValue){
      final Long key = args.getContext().getAggregateContext(true);
      ValueHolder<T> rc = null==key ? null : map.get(key);
      if( null==rc ){
        map.put(key, rc = new ValueHolder<>(initialValue));
      }
      return rc;
    }

    /**
       Should be called from a UDF's xFinal() method and passed that
       method's first argument. This function removes the value
       associated with with the arguments' aggregate context from the
       map and returns it, returning null if no other UDF method has
       been called to set up such a mapping. The latter condition will
       be the case if a UDF is used in a statement which has no result
       rows.
    */
    public T takeAggregateState(SqlFunction.Arguments args){
      final ValueHolder<T> h = map.remove(args.getContext().getAggregateContext(false));
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

     @see SQLFunction.PerContextState#getAggregateState
  */
  protected final ValueHolder<T> getAggregateState(SqlFunction.Arguments args, T initialValue){
    return map.getAggregateState(args, initialValue);
  }

  /**
     To be called from the implementation's xFinal() method to fetch
     the final state of the UDF and remove its mapping.

     see SQLFunction.PerContextState#takeAggregateState
  */
  protected final T takeAggregateState(SqlFunction.Arguments args){
    return map.takeAggregateState(args);
  }

}
