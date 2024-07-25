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
   This marker interface exists soley for use as a documentation and
   class-grouping tool. It should be applied to interfaces or
   classes which have a call() method implementing some specific
   callback interface on behalf of the C library.

   <p>Unless very explicitely documented otherwise, callbacks must
   never throw. Any which do throw but should not might trigger debug
   output regarding the error, but the exception will not be
   propagated.  For callback interfaces which support returning error
   info to the core, the JNI binding will convert any exceptions to
   C-level error information. For callback interfaces which do not
   support returning error information, all exceptions will
   necessarily be suppressed in order to retain the C-style no-throw
   semantics and avoid invoking undefined behavior in the C layer.

   <p>Callbacks of this style follow a common naming convention:

   <p>1) They use the UpperCamelCase form of the C function they're
   proxying for, minus the {@code sqlite3_} prefix, plus a {@code
   Callback} suffix. e.g. {@code sqlite3_busy_handler()}'s callback is
   named {@code BusyHandlerCallback}. Exceptions are made where that
   would potentially be ambiguous, e.g. {@link ConfigSqllogCallback}
   instead of {@code ConfigCallback} because the {@code
   sqlite3_config()} interface may need to support more callback types
   in the future.

   <p>2) They all have a {@code call()} method but its signature is
   callback-specific.
*/
public interface CallbackProxy {}
