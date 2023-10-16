/*
** 2023-08-05
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file is part of the JNI bindings for the sqlite3 C API.
*/
package org.sqlite.jni;

/**
   INCOMPLETE AND COMPLETELY UNTESTED.

   A wrapper for communicating C-level (fts5_api*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.
*/
public final class fts5_api extends NativePointerHolder<fts5_api> {
  /* Only invoked from JNI */
  private fts5_api(){}
  public final int iVersion = 2;

  /**
     Returns the fts5_api instance associated with the given db, or
     null if something goes horribly wrong.
  */
  public static synchronized native fts5_api getInstanceForDb(@NotNull sqlite3 db);

  // int (*xCreateTokenizer)(
  //   fts5_api *pApi,
  //   const char *zName,
  //   void *pContext,
  //   fts5_tokenizer *pTokenizer,
  //   void (*xDestroy)(void*)
  // );

  // /* Find an existing tokenizer */
  // int (*xFindTokenizer)(
  //   fts5_api *pApi,
  //   const char *zName,
  //   void **ppContext,
  //   fts5_tokenizer *pTokenizer
  // );

  // /* Create a new auxiliary function */
  // int (*xCreateFunction)(
  //   fts5_api *pApi,
  //   const char *zName,
  //   void *pContext,
  //   fts5_extension_function xFunction,
  //   void (*xDestroy)(void*)
  // );

  public synchronized native int xCreateFunction(@NotNull String name,
                                    @Nullable Object userData,
                                    @NotNull fts5_extension_function xFunction);

  public int xCreateFunction(@NotNull String name,
                             @NotNull fts5_extension_function xFunction){
    return xCreateFunction(name, null, xFunction);
  }

}
