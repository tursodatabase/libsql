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
package org.sqlite.jni.fts5;
import org.sqlite.jni.capi.NativePointerHolder;
import org.sqlite.jni.annotation.NotNull;

/**
   A wrapper for communicating C-level (fts5_tokenizer*) instances with
   Java. These wrappers do not own their associated pointer, they
   simply provide a type-safe way to communicate it between Java and C
   via JNI.
*/
public final class fts5_tokenizer extends NativePointerHolder<fts5_tokenizer> {
  /* Only invoked by JNI */
  private fts5_tokenizer(){}

  // int (*xCreate)(void*, const char **azArg, int nArg, Fts5Tokenizer **ppOut);
  // void (*xDelete)(Fts5Tokenizer*);

  public native int xTokenize(@NotNull Fts5Tokenizer t, int tokFlags,
                              @NotNull byte pText[],
                              @NotNull XTokenizeCallback callback);


  // int (*xTokenize)(Fts5Tokenizer*,
  //     void *pCtx,
  //     int flags,            /* Mask of FTS5_TOKENIZE_* flags */
  //     const char *pText, int nText,
  //     int (*xToken)(
  //       void *pCtx,         /* Copy of 2nd argument to xTokenize() */
  //       int tflags,         /* Mask of FTS5_TOKEN_* flags */
  //       const char *pToken, /* Pointer to buffer containing token */
  //       int nToken,         /* Size of token in bytes */
  //       int iStart,         /* Byte offset of token within input text */
  //       int iEnd            /* Byte offset of end of token within input text */
  //     )
  // );
}
