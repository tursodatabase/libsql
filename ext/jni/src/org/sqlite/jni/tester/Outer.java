/*
** 2023-08-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains a utility class for generating console output.
*/
package org.sqlite.jni.tester;

class Outer {
  public boolean isVerbose = true;

  public static void out(Object val){
    System.out.print(val);
  }

  public static void outln(Object val){
    System.out.println(val);
  }

  @SuppressWarnings("unchecked")
  public static void out(Object... vals){
    int n = 0;
    for(Object v : vals) out((n++>0 ? " " : "")+v);
  }

  @SuppressWarnings("unchecked")
  public static void outln(Object... vals){
    out(vals);
    out("\n");
  }

  @SuppressWarnings("unchecked")
  public Outer verbose(Object... vals){
    if(isVerbose) outln(vals);
    return this;
  }

  public void setVerbose(boolean b){
    isVerbose = b;
  }

  public boolean getVerbose(){
    return isVerbose;
  }

}
