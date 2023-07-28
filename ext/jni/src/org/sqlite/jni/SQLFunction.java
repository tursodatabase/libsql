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
   inner classes Scalar, Aggregate<T>, and Window<T>.
*/
public abstract class SQLFunction {

  /**
     ContextMap is a helper for use with aggregate and window
     functions, to help them manage their accumulator state across
     calls to the UDF's callbacks.

     If a given aggregate or window function is called multiple times
     in a single SQL statement, e.g. SELECT MYFUNC(A), MYFUNC(B)...,
     then the clients need some way of knowing which call is which so
     that they can map their state between their various UDF callbacks
     and reset it (if needed) via xFinal(). This class takes care of
     such mappings.

     This class works by mapping
     sqlite3_context.getAggregateContext() to a single piece of
     state, of a client-defined type (the T part of this class), which
     persists across a "matching set" of the UDF's callbacks.

     This class is a helper providing commonly-needed functionality -
     it is not required for use with aggregate or window functions.
     Client UDFs are free to perform such mappings using custom
     approaches.
  */
  public static final class ContextMap<T> {
    private final java.util.Map<Long,ValueHolder<T>> map
      = new java.util.HashMap<>();

    /**
       Should be called from a UDF's xStep(), xValue(), and xInverse()
       methods, passing it that method's first argument and an initial
       value for the persistent state. If there is currently no
       mapping for cx.getAggregateContext() within the map, one is
       created using the given initial value, else the existing one is
       used and the 2nd argument is ignored.  It returns a
       ValueHolder<T> which can be used to modify that state directly
       without requiring that the client update the underlying map's
       entry.

       T must be of a type which can be legally stored as a value in
       java.util.HashMap<KeyType,T>.
    */
    public ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
      ValueHolder<T> rc = map.get(cx.getAggregateContext());
      if(null == rc){
        map.put(cx.getAggregateContext(), rc = new ValueHolder<>(initialValue));
      }
      return rc;
    }

    /**
       Should be called from a UDF's xFinal() method and passed that
       method's first argument. This function removes the value
       associated with cx.getAggregateContext() from the map and
       returns it, returning null if no other UDF method has been
       called to set up such a mapping. The latter condition will be
       the case if an aggregate is used in a statement which has no
       result rows.
    */
    public T takeAggregateState(sqlite3_context cx){
      final ValueHolder<T> h = map.remove(cx.getAggregateContext());
      return null==h ? null : h.value;
    }
  }

  //! Subclass for creating scalar functions.
  public static abstract class Scalar extends SQLFunction {
    public abstract void xFunc(sqlite3_context cx, sqlite3_value[] args);
    /**
       Optionally override to be notified when the UDF is finalized by
       SQLite.
    */
    public void xDestroy() {}
  }

  /**
     SQLFunction Subclass for creating aggregate functions.  Its T is
     the data type of its "accumulator" state, an instance of which is
     intended to be be managed using the getAggregateState() and
     takeAggregateState() methods.
  */
  public static abstract class Aggregate<T> extends SQLFunction {
    public abstract void xStep(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xFinal(sqlite3_context cx);

    //! @see Scalar#xDestroy()
    public void xDestroy() {}

    private final ContextMap<T> map = new ContextMap<>();

    //! @see ContextMap<T>#getAggregateState()
    protected final ValueHolder<T> getAggregateState(sqlite3_context cx, T initialValue){
      return map.getAggregateState(cx, initialValue);
    }

    //! @see ContextMap<T>#takeAggregateState()
    protected final T takeAggregateState(sqlite3_context cx){
      return map.takeAggregateState(cx);
    }
  }

  /**
     An SQLFunction subclass for creating window functions.  Note that
     Window<T> inherits from Aggregate<T> and each instance is
     required to implement the inherited abstract methods from that
     class. See Aggregate<T> for information on managing the UDF's
     invocation-specific state.
  */
  public static abstract class Window<T> extends Aggregate<T> {
    public abstract void xInverse(sqlite3_context cx, sqlite3_value[] args);
    public abstract void xValue(sqlite3_context cx);
  }
}
