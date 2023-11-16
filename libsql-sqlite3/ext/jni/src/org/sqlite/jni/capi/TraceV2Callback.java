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
import org.sqlite.jni.annotation.Nullable;

/**
   Callback for use with {@link CApi#sqlite3_trace_v2}.
*/
public interface TraceV2Callback extends CallbackProxy {
  /**
     Called by sqlite3 for various tracing operations, as per
     sqlite3_trace_v2(). Note that this interface elides the 2nd
     argument to the native trace callback, as that role is better
     filled by instance-local state.

     <p>These callbacks may throw, in which case their exceptions are
     converted to C-level error information.

     <p>The 2nd argument to this function, if non-null, will be a an
     sqlite3 or sqlite3_stmt object, depending on the first argument
     (see below).

     <p>The final argument to this function is the "X" argument
     documented for sqlite3_trace() and sqlite3_trace_v2(). Its type
     depends on value of the first argument:

     <p>- SQLITE_TRACE_STMT: pNative is a sqlite3_stmt. pX is a String
     containing the prepared SQL.

     <p>- SQLITE_TRACE_PROFILE: pNative is a sqlite3_stmt. pX is a Long
     holding an approximate number of nanoseconds the statement took
     to run.

     <p>- SQLITE_TRACE_ROW: pNative is a sqlite3_stmt. pX is null.

     <p>- SQLITE_TRACE_CLOSE: pNative is a sqlite3. pX is null.
  */
  int call(int traceFlag, Object pNative, @Nullable Object pX);
}
