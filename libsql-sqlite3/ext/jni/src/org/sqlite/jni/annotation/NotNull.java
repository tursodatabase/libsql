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
** This file houses the NotNull annotation for the sqlite3 C API.
*/
package org.sqlite.jni.annotation;
import java.lang.annotation.*;

/**
   This annotation is for flagging parameters which may not legally be
   null or point to closed/finalized C-side resources.

   <p>In the case of Java types which map directly to C struct types
   (e.g. {@link org.sqlite.jni.capi.sqlite3}, {@link
   org.sqlite.jni.capi.sqlite3_stmt}, and {@link
   org.sqlite.jni.capi.sqlite3_context}), a closed/finalized resource
   is also considered to be null for purposes this annotation because
   the C-side effect of passing such a handle is the same as if null
   is passed.</p>

   <p>When used in the context of Java interfaces which are called
   from the C APIs, this annotation communicates that the C API will
   never pass a null value to the callback for that parameter.</p>

   <p>Passing a null, for this annotation's definition of null, for
   any parameter marked with this annoation specifically invokes
   undefined behavior (see below).</p>

   <p>Passing 0 (i.e. C NULL) or a negative value for any long-type
   parameter marked with this annoation specifically invokes undefined
   behavior (see below). Such values are treated as C pointers in the
   JNI layer.</p>

   <p><b>Undefined behaviour:</b> the JNI build uses the {@code
   SQLITE_ENABLE_API_ARMOR} build flag, meaning that the C code
   invoked with invalid NULL pointers and the like will not invoke
   undefined behavior in the conventional C sense, but may, for
   example, return result codes which are not documented for the
   affected APIs or may otherwise behave unpredictably. In no known
   cases will such arguments result in C-level code dereferencing a
   NULL pointer or accessing out-of-bounds (or otherwise invalid)
   memory. In other words, they may cause unexpected behavior but
   should never cause an outright crash or security issue.</p>

   <p>Note that the C-style API does not throw any exceptions on its
   own because it has a no-throw policy in order to retain its C-style
   semantics, but it may trigger NullPointerExceptions (or similar) if
   passed a null for a parameter flagged with this annotation.</p>

   <p>This annotation is informational only. No policy is in place to
   programmatically ensure that NotNull is conformed to in client
   code.</p>

   <p>This annotation is solely for the use by the classes in the
   org.sqlite.jni package and subpackages, but is made public so that
   javadoc will link to it from the annotated functions. It is not
   part of the public API and client-level code must not rely on
   it.</p>
*/
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.PARAMETER)
public @interface NotNull{}
