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
import org.sqlite.jni.capi.CApi;
import org.sqlite.jni.annotation.*;
import org.sqlite.jni.capi.sqlite3_context;
import org.sqlite.jni.capi.sqlite3_value;

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

  /** Per-invocation state for the UDF. */
  private final SqlFunction.PerContextState<T> map =
    new SqlFunction.PerContextState<>();

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
