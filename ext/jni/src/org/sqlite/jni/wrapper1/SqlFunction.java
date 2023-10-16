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
*/
public interface SqlFunction  {

  /**
     EXPERIMENTAL/INCOMPLETE/UNTESTED. An attempt at hiding UDF-side
     uses of the sqlite3_context and sqlite3_value classes from a
     high-level wrapper.  This level of indirection requires more than
     twice as much Java code (in this API, not client-side) as using
     the lower-level API. Client-side it's roughly the same amount of
     code.
  */
  public final static class Arguments implements Iterable<SqlFunction.Arguments.Arg>{
    private final sqlite3_context cx;
    private final sqlite3_value args[];
    public final int length;

    /**
       Must be passed the context and arguments for the UDF call this
       object is wrapping.
    */
    Arguments(@NotNull sqlite3_context cx, @NotNull sqlite3_value args[]){
      this.cx = cx;
      this.args = args;
      this.length = args.length;
    }

    /**
       Wrapper for a single SqlFunction argument. Primarily intended
       for eventual use with the Arguments class's Iterable interface.
    */
    public final static class Arg {
      private final Arguments a;
      private final int ndx;
      /* Only for use by the Arguments class. */
      private Arg(@NotNull Arguments a, int ndx){
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

    //! Untested!
    @Override
    public java.util.Iterator<SqlFunction.Arguments.Arg> iterator(){
      Arg[] proxies = new Arg[args.length];
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
     Internal-use adapter for wrapping this package's ScalarFunction
     for use with the org.sqlite.jni.capi.ScalarFunction interface.
  */
  static final class ScalarAdapter extends org.sqlite.jni.capi.ScalarFunction {
    final ScalarFunction impl;
    ScalarAdapter(ScalarFunction impl){
      this.impl = impl;
    }
    /**
       Proxies this.f.xFunc(), adapting the call arguments to that
       function's signature.
    */
    public void xFunc(sqlite3_context cx, sqlite3_value[] args){
      impl.xFunc( new SqlFunction.Arguments(cx, args) );
    }

    public void xDestroy(){
      impl.xDestroy();
    }
  }

}
