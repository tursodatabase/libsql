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
** This file houses the Experimental annotation for the sqlite3 C API.
*/
package org.sqlite.jni.annotation;
import java.lang.annotation.*;

/**
   This annotation is for flagging methods, constructors, and types
   which are expressly experimental and subject to any amount of
   change or outright removal. Client code should not rely on such
   features.
*/
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target({
    ElementType.METHOD,
    ElementType.CONSTRUCTOR,
    ElementType.TYPE
})
public @interface Experimental{}
