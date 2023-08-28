package org.sqlite.jni.annotation;

/**
   This annotation is for marking functions as "canonical", meaning
   that they exist in the C API. The intent is to distinguish them
   from functions added specifically to the Java API.

   <p>Canonical functions, unless specifically documented, have the
   same semantics as their counterparts in @{link
   https://sqlite.org/c3ref/intro.html the C API documentation}, despite
   their signatures perhaps differing.
*/
@java.lang.annotation.Documented
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.SOURCE)
@java.lang.annotation.Target(java.lang.annotation.ElementType.METHOD)
public @interface Canonical{}
