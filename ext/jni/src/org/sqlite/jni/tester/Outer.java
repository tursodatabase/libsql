package org.sqlite.jni.tester;

public class Outer {
  public boolean isVerbose = true;

  public static <T> void out(T val){
    System.out.print(val);
  }

  public static <T> void outln(T val){
    System.out.println(val);
  }

  @SuppressWarnings("unchecked")
  public static <T> void out(T... vals){
    int n = 0;
    for(T v : vals) out((n++>0 ? " " : "")+v);
  }

  @SuppressWarnings("unchecked")
  public static <T> void outln(T... vals){
    out(vals);
    out("\n");
  }

  @SuppressWarnings("unchecked")
  public <T> Outer verbose(T... vals){
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
