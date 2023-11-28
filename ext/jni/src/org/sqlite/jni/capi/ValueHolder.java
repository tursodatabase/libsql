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
** This file contains the ValueHolder utility class for the sqlite3
** JNI bindings.
*/
package org.sqlite.jni.capi;

/**
   A helper class which simply holds a single value. Its primary use
   is for communicating values out of anonymous classes, as doing so
   requires a "final" reference, as well as communicating aggregate
   SQL function state across calls to such functions.
*/
public class ValueHolder<T> {
  public T value;
  public ValueHolder(){}
  public ValueHolder(T v){value = v;}
}
