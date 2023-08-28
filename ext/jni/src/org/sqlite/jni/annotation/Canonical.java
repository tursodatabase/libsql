package org.sqlite.jni.annotation;

/**
   This annotation is for marking functions as "canonical", meaning
   that they map directly to a function in the core sqlite3 C API. The
   intent is to distinguish them from functions added specifically to
   the Java API.

   <p>Canonical functions, unless specifically documented, have the
   same semantics as their counterparts in
   <a href="https://sqlite.org/c3ref/intro.html">the C API documentation</a>,
   despite their signatures perhaps differing. The Java API adds a
   number of overloads to simplify use, as well as a few Java-specific
   functions, and those are never flagged as @Canonical.

   <p>In some cases, the canonical version of a function is private
   and exposed to Java via public overloads.

   <p>In rare cases, the Java interface for a canonical function has a
   different name than its C counterpart. For such cases,
   (cname=the-C-side-name) is passed to this annotation and a
   Java-side implementation with a slightly different signature is
   added to with the canonical name. As of this writing, that applies
   only to {@link org.sqlite.jni.SQLite3Jni#sqlite3_value_text_utf8}
   and {@link org.sqlite.jni.SQLite3Jni#sqlite3_column_text_utf8}.

   <p>The comment property can be used to add a comment.
*/
@java.lang.annotation.Documented
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.SOURCE)
@java.lang.annotation.Target(java.lang.annotation.ElementType.METHOD)
public @interface Canonical{
  /**
     Java functions which directly map to a canonical function but
     change its name for some reason should not the original name
     in this property.
  */
  String cname() default ""/*doesn't allow null*/;
  /**
     Brief comments about the binding, e.g. noting any major
     semantic differences.
  */
  String comment() default "";
}
