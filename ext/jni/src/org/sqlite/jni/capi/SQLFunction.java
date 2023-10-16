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
package org.sqlite.jni.capi;

/**
   SQLFunction is used in conjunction with the
   sqlite3_create_function() JNI-bound API to give that native code
   access to the callback functions needed in order to implement SQL
   functions in Java.

   <p>

   This class is not used by itself, but is a marker base class. The
   three UDF types are modelled by the inner classes Scalar,
   Aggregate<T>, and Window<T>. Most simply, clients may subclass
   those, or create anonymous classes from them, to implement
   UDFs. Clients are free to create their own classes for use with
   UDFs, so long as they conform to the public interfaces defined by
   those three classes. The JNI layer only actively relies on the
   SQLFunction base class and the method names and signatures used by
   the UDF callback interfaces.
*/
public interface SQLFunction {

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

}
