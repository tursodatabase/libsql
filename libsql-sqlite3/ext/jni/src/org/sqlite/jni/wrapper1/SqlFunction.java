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

  int DETERMINISTIC = CApi.SQLITE_DETERMINISTIC;
  int INNOCUOUS = CApi.SQLITE_INNOCUOUS;
  int DIRECTONLY = CApi.SQLITE_DIRECTONLY;
  int SUBTYPE = CApi.SQLITE_SUBTYPE;
  int RESULT_SUBTYPE = CApi.SQLITE_RESULT_SUBTYPE;
  int UTF8 = CApi.SQLITE_UTF8;
  int UTF16 = CApi.SQLITE_UTF16;

  /**
     The Arguments type is an abstraction on top of the lower-level
     UDF function argument types. It provides _most_ of the functionality
     of the lower-level interface, insofar as possible without "leaking"
     those types into this API.
  */
  final class Arguments implements Iterable<SqlFunction.Arguments.Arg>{
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
      this.args = args==null ? new sqlite3_value[0] : args;
      this.length = this.args.length;
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

    //! Returns the underlying sqlite3_context for these arguments.
    sqlite3_context getContext(){return cx;}

    /**
       Returns the Sqlite (db) object associated with this UDF call,
       or null if the UDF is somehow called without such an object or
       the db has been closed in an untimely manner (e.g. closed by a
       UDF call).
    */
    public Sqlite getDb(){
      return Sqlite.fromNative( CApi.sqlite3_context_db_handle(cx) );
    }

    public int getArgCount(){ return args.length; }

    public int getInt(int argNdx){return CApi.sqlite3_value_int(valueAt(argNdx));}
    public long getInt64(int argNdx){return CApi.sqlite3_value_int64(valueAt(argNdx));}
    public double getDouble(int argNdx){return CApi.sqlite3_value_double(valueAt(argNdx));}
    public byte[] getBlob(int argNdx){return CApi.sqlite3_value_blob(valueAt(argNdx));}
    public byte[] getText(int argNdx){return CApi.sqlite3_value_text(valueAt(argNdx));}
    public String getText16(int argNdx){return CApi.sqlite3_value_text16(valueAt(argNdx));}
    public int getBytes(int argNdx){return CApi.sqlite3_value_bytes(valueAt(argNdx));}
    public int getBytes16(int argNdx){return CApi.sqlite3_value_bytes16(valueAt(argNdx));}
    public Object getObject(int argNdx){return CApi.sqlite3_value_java_object(valueAt(argNdx));}
    public <T> T getObject(int argNdx, Class<T> type){
      return CApi.sqlite3_value_java_object(valueAt(argNdx), type);
    }

    public int getType(int argNdx){return CApi.sqlite3_value_type(valueAt(argNdx));}
    public int getSubtype(int argNdx){return CApi.sqlite3_value_subtype(valueAt(argNdx));}
    public int getNumericType(int argNdx){return CApi.sqlite3_value_numeric_type(valueAt(argNdx));}
    public int getNoChange(int argNdx){return CApi.sqlite3_value_nochange(valueAt(argNdx));}
    public boolean getFromBind(int argNdx){return CApi.sqlite3_value_frombind(valueAt(argNdx));}
    public int getEncoding(int argNdx){return CApi.sqlite3_value_encoding(valueAt(argNdx));}

    public void resultInt(int v){ CApi.sqlite3_result_int(cx, v); }
    public void resultInt64(long v){ CApi.sqlite3_result_int64(cx, v); }
    public void resultDouble(double v){ CApi.sqlite3_result_double(cx, v); }
    public void resultError(String msg){CApi.sqlite3_result_error(cx, msg);}
    public void resultError(Exception e){CApi.sqlite3_result_error(cx, e);}
    public void resultErrorTooBig(){CApi.sqlite3_result_error_toobig(cx);}
    public void resultErrorCode(int rc){CApi.sqlite3_result_error_code(cx, rc);}
    public void resultObject(Object o){CApi.sqlite3_result_java_object(cx, o);}
    public void resultNull(){CApi.sqlite3_result_null(cx);}
    /**
       Analog to sqlite3_result_value(), using the Value object at the
       given argument index.
    */
    public void resultArg(int argNdx){CApi.sqlite3_result_value(cx, valueAt(argNdx));}
    public void resultSubtype(int subtype){CApi.sqlite3_result_subtype(cx, subtype);}
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

    /**
       Callbacks should invoke this on OOM errors, instead of throwing
       OutOfMemoryError, because the latter cannot be propagated
       through the C API.
    */
    public void resultNoMem(){CApi.sqlite3_result_error_nomem(cx);}

    /**
       Analog to sqlite3_set_auxdata() but throws if argNdx is out of
       range.
    */
    public void setAuxData(int argNdx, Object o){
      /* From the API docs: https://sqlite.org/c3ref/get_auxdata.html

         The value of the N parameter to these interfaces should be
         non-negative. Future enhancements may make use of negative N
         values to define new kinds of function caching behavior.
      */
      valueAt(argNdx);
      CApi.sqlite3_set_auxdata(cx, argNdx, o);
    }

    /**
       Analog to sqlite3_get_auxdata() but throws if argNdx is out of
       range.
    */
    public Object getAuxData(int argNdx){
      valueAt(argNdx);
      return CApi.sqlite3_get_auxdata(cx, argNdx);
    }

    /**
       Represents a single SqlFunction argument. Primarily intended
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
      public <T> T getObject(Class<T> type){ return a.getObject(ndx, type); }
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

  }

  /**
     Internal-use adapter for wrapping this package's ScalarFunction
     for use with the org.sqlite.jni.capi.ScalarFunction interface.
  */
  final class ScalarAdapter extends org.sqlite.jni.capi.ScalarFunction {
    private final ScalarFunction impl;
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
  class AggregateAdapter extends org.sqlite.jni.capi.AggregateFunction {
  /*cannot be final without duplicating the whole body in WindowAdapter*/
    private final AggregateFunction impl;
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
       As for the xFinal() argument of the C API's
       sqlite3_create_function().  If the proxied function throws, it
       is translated into a sqlite3_result_error().
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

  /**
     Internal-use adapter for wrapping this package's WindowFunction
     for use with the org.sqlite.jni.capi.WindowFunction interface.
  */
  final class WindowAdapter extends AggregateAdapter {
    private final WindowFunction impl;
    WindowAdapter(WindowFunction impl){
      super(impl);
      this.impl = impl;
    }

    /**
       Proxies this.impl.xInverse(), adapting the call arguments to that
       function's signature. If the proxied function throws, it is
       translated to sqlite_result_error() with the exception's
       message.
    */
    public void xInverse(sqlite3_context cx, sqlite3_value[] args){
      try{
        impl.xInverse( new SqlFunction.Arguments(cx, args) );
      }catch(Exception e){
        CApi.sqlite3_result_error(cx, e);
      }
    }

    /**
       As for the xValue() argument of the C API's sqlite3_create_window_function().
       If the proxied function throws, it is translated into a sqlite3_result_error().
    */
    public void xValue(sqlite3_context cx){
      try{
        impl.xValue( new SqlFunction.Arguments(cx, null) );
      }catch(Exception e){
        CApi.sqlite3_result_error(cx, e);
      }
    }

    public void xDestroy(){
      impl.xDestroy();
    }
  }

}
