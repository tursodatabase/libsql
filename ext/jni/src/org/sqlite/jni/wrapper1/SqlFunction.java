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
import org.sqlite.jni.capi.sqlite3_context;
import org.sqlite.jni.capi.sqlite3_value;

/**
   Base marker interface for SQLite's three types of User-Defined SQL
   Functions (UDFs): Scalar, Aggregate, and Window functions.
*/
public interface SqlFunction  {

  /**
     The Arguments type is an abstraction on top of the lower-level
     UDF function argument types. It provides _most_ of the functionality
     of the lower-level interface, insofar as possible without "leaking"
     those types into this API.
  */
  public final static class Arguments implements Iterable<SqlFunction.Arguments.Arg>{
    private final sqlite3_context cx;
    private final sqlite3_value args[];
    public final int length;

    /**
       Must be passed the context and arguments for the UDF call this
       object is wrapping. Intended to be used by internal proxy
       classes which "convert" the lower-level interface into this
       package's higher-level interface, e.g. ScalarAdapter and
       AggregateAdapter.

       Passing null for the args is equivalent to passing a length-0
       array.
    */
    Arguments(sqlite3_context cx, sqlite3_value args[]){
      this.cx = cx;
      this.args = args==null ? new sqlite3_value[0] : args;;
      this.length = this.args.length;
    }

    /**
       Wrapper for a single SqlFunction argument. Primarily intended
       for use with the Arguments class's Iterable interface.
    */
    public final static class Arg {
      private final Arguments a;
      private final int ndx;
      /* Only for use by the Arguments class. */
      private Arg(Arguments a, int ndx){
        this.a = a;
        this.ndx = ndx;
      }
      /** Returns this argument's index in its parent argument list. */
      public int getIndex(){return ndx;}
      public int getInt(){return a.getInt(ndx);}
      public long getInt64(){return a.getInt64(ndx);}
      public double getDouble(){return a.getDouble(ndx);}
      public byte[] getBlob(){return a.getBlob(ndx);}
      public byte[] getText(){return a.getText(ndx);}
      public String getText16(){return a.getText16(ndx);}
      public int getBytes(){return a.getBytes(ndx);}
      public int getBytes16(){return a.getBytes16(ndx);}
      public Object getObject(){return a.getObject(ndx);}
      public <T> T getObjectCasted(Class<T> type){ return a.getObjectCasted(ndx, type); }
      public int getType(){return a.getType(ndx);}
      public Object getAuxData(){return a.getAuxData(ndx);}
      public void setAuxData(Object o){a.setAuxData(ndx, o);}
    }

    @Override
    public java.util.Iterator<SqlFunction.Arguments.Arg> iterator(){
      final Arg[] proxies = new Arg[args.length];
      for( int i = 0; i < args.length; ++i ){
        proxies[i] = new Arg(this, i);
      }
      return java.util.Arrays.stream(proxies).iterator();
    }

    /**
       Returns the sqlite3_value at the given argument index or throws
       an IllegalArgumentException exception if ndx is out of range.
    */
    private sqlite3_value valueAt(int ndx){
      if(ndx<0 || ndx>=args.length){
        throw new IllegalArgumentException(
          "SQL function argument index "+ndx+" is out of range."
        );
      }
      return args[ndx];
    }

    sqlite3_context getContext(){return cx;}

    public int getArgCount(){ return args.length; }

    public int getInt(int arg){return CApi.sqlite3_value_int(valueAt(arg));}
    public long getInt64(int arg){return CApi.sqlite3_value_int64(valueAt(arg));}
    public double getDouble(int arg){return CApi.sqlite3_value_double(valueAt(arg));}
    public byte[] getBlob(int arg){return CApi.sqlite3_value_blob(valueAt(arg));}
    public byte[] getText(int arg){return CApi.sqlite3_value_text(valueAt(arg));}
    public String getText16(int arg){return CApi.sqlite3_value_text16(valueAt(arg));}
    public int getBytes(int arg){return CApi.sqlite3_value_bytes(valueAt(arg));}
    public int getBytes16(int arg){return CApi.sqlite3_value_bytes16(valueAt(arg));}
    public Object getObject(int arg){return CApi.sqlite3_value_java_object(valueAt(arg));}
    public <T> T getObjectCasted(int arg, Class<T> type){
      return CApi.sqlite3_value_java_casted(valueAt(arg), type);
    }

    public int getType(int arg){return CApi.sqlite3_value_type(valueAt(arg));}
    public int getSubtype(int arg){return CApi.sqlite3_value_subtype(valueAt(arg));}
    public int getNumericType(int arg){return CApi.sqlite3_value_numeric_type(valueAt(arg));}
    public int getNoChange(int arg){return CApi.sqlite3_value_nochange(valueAt(arg));}
    public boolean getFromBind(int arg){return CApi.sqlite3_value_frombind(valueAt(arg));}
    public int getEncoding(int arg){return CApi.sqlite3_value_encoding(valueAt(arg));}

    public void resultInt(int v){ CApi.sqlite3_result_int(cx, v); }
    public void resultInt64(long v){ CApi.sqlite3_result_int64(cx, v); }
    public void resultDouble(double v){ CApi.sqlite3_result_double(cx, v); }
    public void resultError(String msg){CApi.sqlite3_result_error(cx, msg);}
    public void resultError(Exception e){CApi.sqlite3_result_error(cx, e);}
    public void resultErrorTooBig(){CApi.sqlite3_result_error_toobig(cx);}
    public void resultErrorCode(int rc){CApi.sqlite3_result_error_code(cx, rc);}
    public void resultObject(Object o){CApi.sqlite3_result_java_object(cx, o);}
    public void resultNull(){CApi.sqlite3_result_null(cx);}
    public void resultArg(int argNdx){CApi.sqlite3_result_value(cx, valueAt(argNdx));}
    public void resultZeroBlob(long n){
      // Throw on error? If n is too big,
      // sqlite3_result_error_toobig() is automatically called.
      CApi.sqlite3_result_zeroblob64(cx, n);
    }

    public void resultBlob(byte[] blob){CApi.sqlite3_result_blob(cx, blob);}
    public void resultText(byte[] utf8){CApi.sqlite3_result_text(cx, utf8);}
    public void resultText(String txt){CApi.sqlite3_result_text(cx, txt);}
    public void resultText16(byte[] utf16){CApi.sqlite3_result_text16(cx, utf16);}
    public void resultText16(String txt){CApi.sqlite3_result_text16(cx, txt);}

    public void setAuxData(int arg, Object o){
      /* From the API docs: https://www.sqlite.org/c3ref/get_auxdata.html

         The value of the N parameter to these interfaces should be
         non-negative. Future enhancements may make use of negative N
         values to define new kinds of function caching behavior.
      */
      valueAt(arg);
      CApi.sqlite3_set_auxdata(cx, arg, o);
    }

    public Object getAuxData(int arg){
      valueAt(arg);
      return CApi.sqlite3_get_auxdata(cx, arg);
    }
  }

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

  /**
     Internal-use adapter for wrapping this package's ScalarFunction
     for use with the org.sqlite.jni.capi.ScalarFunction interface.
  */
  static final class ScalarAdapter extends org.sqlite.jni.capi.ScalarFunction {
    final ScalarFunction impl;
    ScalarAdapter(ScalarFunction impl){
      this.impl = impl;
    }
    /**
       Proxies this.impl.xFunc(), adapting the call arguments to that
       function's signature. If the proxy throws, it's translated to
       sqlite_result_error() with the exception's message.
    */
    public void xFunc(sqlite3_context cx, sqlite3_value[] args){
      try{
        impl.xFunc( new SqlFunction.Arguments(cx, args) );
      }catch(Exception e){
        CApi.sqlite3_result_error(cx, e);
      }
    }

    public void xDestroy(){
      impl.xDestroy();
    }
  }

  /**
     Internal-use adapter for wrapping this package's AggregateFunction
     for use with the org.sqlite.jni.capi.AggregateFunction interface.
  */
  static final class AggregateAdapter extends org.sqlite.jni.capi.AggregateFunction {
    final AggregateFunction impl;
    AggregateAdapter(AggregateFunction impl){
      this.impl = impl;
    }

    /**
       Proxies this.impl.xStep(), adapting the call arguments to that
       function's signature. If the proxied function throws, it is
       translated to sqlite_result_error() with the exception's
       message.
    */
    public void xStep(sqlite3_context cx, sqlite3_value[] args){
      try{
        impl.xStep( new SqlFunction.Arguments(cx, args) );
      }catch(Exception e){
        CApi.sqlite3_result_error(cx, e);
      }
    }

    /**
       As for the xFinal() argument of the C API's sqlite3_create_function().
       If the proxied function throws, it is translated into a sqlite3_result_error().
    */
    public void xFinal(sqlite3_context cx){
      try{
        impl.xFinal( new SqlFunction.Arguments(cx, null) );
      }catch(Exception e){
        CApi.sqlite3_result_error(cx, e);
      }
    }

    public void xDestroy(){
      impl.xDestroy();
    }
  }

}
