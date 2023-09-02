package org.sqlite.jni.annotation;

/**
   This annotation is for marking functions as "canonical", meaning
   that they map directly to a function in the core sqlite3 C API. The
   intent is to distinguish them from functions added specifically to
   the Java API.

   <p>Canonical functions, unless specifically documented, have the
   same semantics as their counterparts in <a
   href="https://sqlite.org/c3ref/intro.html">the C API
   documentation</a>, despite their signatures perhaps differing
   slightly. Canonical forms may be native or implemented in Java.
   Sometimes multiple overloads are labeled as Canonical because one
   or more of them are just type- or encoding-related conversion
   wrappers but provide identical semantics (e.g. from a String to a
   byte[]).  The Java API adds a number of convenience overloads to
   simplify use, as well as a few Java-specific functions, and those
   are never flagged as @Canonical.

   <p>In some cases, the canonical version of a function is private
   and exposed to Java via public overloads.

   <p>The comment property can be used to add a comment.
*/
@java.lang.annotation.Documented
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@java.lang.annotation.Target(java.lang.annotation.ElementType.METHOD)
public @interface Canonical{
  /**
     Brief comments about the binding, e.g. noting any major
     semantic differences.
  */
  String comment() default "";
}
