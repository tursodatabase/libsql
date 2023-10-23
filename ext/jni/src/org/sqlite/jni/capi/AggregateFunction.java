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

  /** Per-invocation state for the UDF. */
  private final SQLFunction.PerContextState<T> map =
    new SQLFunction.PerContextState<>();

  /**
     To be called from the implementation's xStep() method, as well
     as the xValue() and xInverse() methods of the {@link WindowFunction}
     subclass, to fetch the current per-call UDF state. On the
     first call to this method for any given sqlite3_context
     argument, the context is set to the given initial value. On all other
     calls, the 2nd argument is ignored.

     @see SQLFunction.PerContextState#getAggregateState
  */
  protected final ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
    return map.getAggregateState(cx, initialValue);
  }

  /**
     To be called from the implementation's xFinal() method to fetch
     the final state of the UDF and remove its mapping.

     see SQLFunction.PerContextState#takeAggregateState
  */
  protected final T takeAggregateState(sqlite3_context cx){
    return map.takeAggregateState(cx);
  }
}
