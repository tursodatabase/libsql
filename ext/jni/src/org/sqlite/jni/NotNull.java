package org.sqlite.jni;

/**
   This annotation is for flagging parameters which may not legally be
   null. Note that the C-style API does not throw any
   NullPointerExceptions on its own because it has a no-throw policy
   in order to retain its C-style semantics.

   <p>This annotation is informational only. No policy is in place to
   programmatically ensure that NotNull is conformed to in client
   code.

   <p>This annotation is solely for the use by the classes in this
   package but is made public so that javadoc will link to it from the
   annotated functions. It is not part of the public API and
   client-level code must not rely on it.
*/
@java.lang.annotation.Documented
@java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.SOURCE)
@java.lang.annotation.Target(java.lang.annotation.ElementType.PARAMETER)
public @interface NotNull{}
