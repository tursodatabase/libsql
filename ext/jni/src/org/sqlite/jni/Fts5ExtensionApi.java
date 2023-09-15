/*
** 2023-08-04
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
import java.nio.charset.StandardCharsets;

/**
   ALMOST COMPLETELY UNTESTED.

   FAR FROM COMPLETE and the feasibility of binding this to Java
   is still undetermined. This might be removed.
*/
public final class Fts5ExtensionApi extends NativePointerHolder<Fts5ExtensionApi> {
  //! Only called from JNI
  private Fts5ExtensionApi(){}
  private int iVersion = 2;

  /* Callback type for used by xQueryPhrase(). */
  public static interface xQueryPhraseCallback {
    int xCallback(Fts5ExtensionApi fapi, Fts5Context cx);
  }

  /**
     Returns the singleton instance of this class.
  */
  public static synchronized native Fts5ExtensionApi getInstance();

  public synchronized native int xColumnCount(@NotNull Fts5Context fcx);
  public synchronized native int xColumnSize(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.Int32 pnToken);
  public synchronized native int xColumnText(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.String txt);
  public synchronized native int xColumnTotalSize(@NotNull Fts5Context fcx, int iCol,
                                     @NotNull OutputPointer.Int64 pnToken);
  public synchronized native Object xGetAuxdata(@NotNull Fts5Context cx, boolean clearIt);
  public synchronized native int xInst(@NotNull Fts5Context cx, int iIdx,
                          @NotNull OutputPointer.Int32 piPhrase,
                          @NotNull OutputPointer.Int32 piCol,
                          @NotNull OutputPointer.Int32 piOff);
  public synchronized native int xInstCount(@NotNull Fts5Context fcx,
                               @NotNull OutputPointer.Int32 pnInst);
  public synchronized native int xPhraseCount(@NotNull Fts5Context fcx);
  public synchronized native int xPhraseFirst(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);
  public synchronized native int xPhraseFirstColumn(@NotNull Fts5Context cx, int iPhrase,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  public synchronized native void xPhraseNext(@NotNull Fts5Context cx,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);
  public synchronized native void xPhraseNextColumn(@NotNull Fts5Context cx,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  public synchronized native int xPhraseSize(@NotNull Fts5Context fcx, int iPhrase);
  public synchronized native int xQueryPhrase(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull xQueryPhraseCallback callback);
  public synchronized native int xRowCount(@NotNull Fts5Context fcx,
                              @NotNull OutputPointer.Int64 nRow);
  public synchronized native long xRowid(@NotNull Fts5Context cx);
  /* Note that the JNI binding lacks the C version's xDelete()
     callback argument. Instead, if pAux has an xDestroy() method, it
     is called if the FTS5 API finalizes the aux state (including if
     allocation of storage for the auxdata fails). Any reference to
     pAux held by the JNI layer will be relinquished regardless of
     whether pAux has an xDestroy() method. */
  public synchronized native int xSetAuxdata(@NotNull Fts5Context cx, @Nullable Object pAux);
  public synchronized native int xTokenize(@NotNull Fts5Context cx, @NotNull byte pText[],
                              @NotNull Fts5.xTokenizeCallback callback);

  public synchronized native Object xUserData(Fts5Context cx);
  //^^^ returns the pointer passed as the 3rd arg to the C-level
  // fts5_api::xCreateFunction.
}
