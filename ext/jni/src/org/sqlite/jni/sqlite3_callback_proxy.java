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
   This marker interface exists soley for use as a documentation and
   class-grouping tool. It should be applied to interfaces or
   classes which have a call() method implementing some specific
   callback interface on behalf of the C library.

   <p>Callbacks of this style follow a common naming convention:

   <p>1) They almost all have the same class or interface name as the
   C function they are proxying a callback for, minus the sqlite3_
   prefix, plus a _callback suffix. e.g. sqlite3_busy_handler()'s
   callback is named busy_handler_callback. Exceptions are made where
   that would potentially be ambiguous, e.g. config_sqllog_callback
   instead of config_callback because the sqlite3_config() interface
   may need to support more callback types in the future.

   <p>2) They all have a call() method but its signature is
   callback-specific.
*/
public interface sqlite3_callback_proxy {}
