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

/**
   Console output utility class.
*/
class Outer {
  public int verbosity = 0;

  public static void out(Object val){
    System.out.print(val);
  }

  public static void outln(Object val){
    System.out.println(val);
  }

  @SuppressWarnings("unchecked")
  public static void out(Object... vals){
    for(Object v : vals) out(v);
  }

  @SuppressWarnings("unchecked")
  public static void outln(Object... vals){
    out(vals);
    out("\n");
  }

  @SuppressWarnings("unchecked")
  public Outer verbose(Object... vals){
    if(verbosity>0){
      out("VERBOSE",(verbosity>1 ? "+: " : ": "));
      outln(vals);
    }
    return this;
  }

  public void setVerbosity(int level){
    verbosity = level;
  }

  public int getVerbosity(){
    return verbosity;
  }

  public boolean isVerbose(){return verbosity > 0;}

}
