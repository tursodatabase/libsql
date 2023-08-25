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
package org.sqlite.jni;

/**
   Callback proxy for use with sqlite3_trace_v2().
*/
public interface trace_v2_callback extends sqlite3_callback_proxy {
  /**
     Called by sqlite3 for various tracing operations, as per
     sqlite3_trace_v2(). Note that this interface elides the 2nd
     argument to the native trace callback, as that role is better
     filled by instance-local state.

     <p>The 2nd argument to this function, if non-0, will be a native
     pointer to either an sqlite3 or sqlite3_stmt object, depending on
     the first argument (see below). Client code can pass it to the
     sqlite3 resp. sqlite3_stmt constructor to create a wrapping
     object, if necessary. This API does not do so by default because
     tracing can be called frequently, creating such a wrapper for
     each call is comparatively expensive, and the objects are
     probably only seldom useful.

     <p>The final argument to this function is the "X" argument
     documented for sqlite3_trace() and sqlite3_trace_v2(). Its type
     depends on value of the first argument:

     <p>- SQLITE_TRACE_STMT: pNative is a sqlite3_stmt. pX is a string
     containing the prepared SQL, with one caveat: JNI only provides
     us with the ability to convert that string to MUTF-8, as
     opposed to standard UTF-8, and is cannot be ruled out that that
     difference may be significant for certain inputs. The
     alternative would be that we first convert it to UTF-16 before
     passing it on, but there's no readily-available way to do that
     without calling back into the db to peform the conversion
     (which would lead to further tracing).

     <p>- SQLITE_TRACE_PROFILE: pNative is a sqlite3_stmt. pX is a Long
     holding an approximate number of nanoseconds the statement took
     to run.

     <p>- SQLITE_TRACE_ROW: pNative is a sqlite3_stmt. pX is null.

     <p>- SQLITE_TRACE_CLOSE: pNative is a sqlite3. pX is null.
  */
  int call(int traceFlag, Object pNative, Object pX);
}
