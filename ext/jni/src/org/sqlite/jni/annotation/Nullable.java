/*
** 2023-09-27
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file houses the Nullable annotation for the sqlite3 C API.
*/
package org.sqlite.jni.annotation;
import java.lang.annotation.*;

/**
   This annotation is for flagging parameters which may legally be
   null, noting that they may behave differently if passed null but
   are prepared to expect null as a value. When used in the context of
   callback methods which are called into from the C APIs, this
   annotation communicates that the C API may pass a null value to the
   callback.

   <p>This annotation is solely for the use by the classes in this
   package but is made public so that javadoc will link to it from the
   annotated functions. It is not part of the public API and
   client-level code must not rely on it.
*/
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface Nullable{}
