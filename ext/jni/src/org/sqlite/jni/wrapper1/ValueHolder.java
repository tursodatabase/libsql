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
** This file contains a set of tests for the sqlite3 JNI bindings.
*/
package org.sqlite.jni.wrapper1;

/**
   A helper class which simply holds a single value. Its primary use
   is for communicating values out of anonymous classes, as doing so
   requires a "final" reference.
*/
public class ValueHolder<T> {
  public T value;
  public ValueHolder(){}
  public ValueHolder(T v){value = v;}
}
