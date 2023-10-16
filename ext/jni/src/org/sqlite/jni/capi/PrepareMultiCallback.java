/*
** 2023-09-13
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
   Callback for use with {@link CApi#sqlite3_prepare_multi}.
*/
public interface PrepareMultiCallback extends CallbackProxy {

  /**
     Gets passed a sqlite3_stmt which it may handle in arbitrary ways,
     transfering ownership of it to this function.

     sqlite3_prepare_multi() will _not_ finalize st - it is up
     to the call() implementation how st is handled.

     Must return 0 on success or an SQLITE_... code on error.

     See the {@link Finalize} class for a wrapper which finalizes the
     statement after calling a proxy PrepareMultiCallback.
  */
  int call(sqlite3_stmt st);

  /**
     A PrepareMultiCallback impl which wraps a separate impl and finalizes
     any sqlite3_stmt passed to its callback.
  */
  public static final class Finalize implements PrepareMultiCallback {
    private PrepareMultiCallback p;
    /**
       p is the proxy to call() when this.call() is called.
    */
    public Finalize( PrepareMultiCallback p ){
      this.p = p;
    }
    /**
       Calls the call() method of the proxied callback and either returns its
       result or propagates an exception. Either way, it passes its argument to
       sqlite3_finalize() before returning.
    */
    @Override public int call(sqlite3_stmt st){
      try {
        return this.p.call(st);
      }finally{
        CApi.sqlite3_finalize(st);
      }
    }
  }

  /**
     A PrepareMultiCallback impl which steps entirely through a result set,
     ignoring all non-error results.
  */
  public static final class StepAll implements PrepareMultiCallback {
    public StepAll(){}
    /**
       Calls sqlite3_step() on st until it returns something other than
       SQLITE_ROW. If the final result is SQLITE_DONE then 0 is returned,
       else the result of the final step is returned.
    */
    @Override public int call(sqlite3_stmt st){
      int rc = CApi.SQLITE_DONE;
      while( CApi.SQLITE_ROW == (rc = CApi.sqlite3_step(st)) ){}
      return CApi.SQLITE_DONE==rc ? 0 : rc;
    }
  }
}
