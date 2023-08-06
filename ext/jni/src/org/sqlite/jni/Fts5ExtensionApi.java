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
   COMPLETELY UNTESTED.

   FAR FROM COMPLETE and the feasibility of binding this to Java
   is still undetermined. This might be removed.

   Reminder to self: the native Fts5ExtensionApi is a singleton.
*/
public final class Fts5ExtensionApi extends NativePointerHolder<Fts5ExtensionApi> {
  //! Only called from JNI
  private Fts5ExtensionApi(){}
  private int iVersion = 2;

  public static interface xQueryPhraseCallback {
    int xCallback(Fts5ExtensionApi fapi, Fts5Context cx);
  }

  /**
     Returns a singleton instance of this class.
  */
  public static synchronized native Fts5ExtensionApi getInstance();

  public native int xColumnCount(@NotNull Fts5Context fcx);
  public native int xColumnSize(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.Int32 pnToken);
  public native int xColumnText(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.String txt);
  public native int xColumnTotalSize(@NotNull Fts5Context fcx, int iCol,
                                     @NotNull OutputPointer.Int64 pnToken);
  public native Object xGetAuxdata(@NotNull Fts5Context cx, boolean clearIt);
  public native int xInst(@NotNull Fts5Context cx, int iIdx,
                          @NotNull OutputPointer.Int32 piPhrase,
                          @NotNull OutputPointer.Int32 piCol,
                          @NotNull OutputPointer.Int32 piOff);
  public native int xInstCount(@NotNull Fts5Context fcx,
                               @NotNull OutputPointer.Int32 pnInst);
  public native int xPhraseCount(@NotNull Fts5Context fcx);
  public native int xPhraseFirst(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);
  public native int xPhraseFirstColumn(@NotNull Fts5Context cx, int iPhrase,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  public native void xPhraseNext(@NotNull Fts5Context cx,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);
  public native void xPhraseNextColumn(@NotNull Fts5Context cx,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  public native int xPhraseSize(@NotNull Fts5Context fcx, int iPhrase);
  public native int xQueryPhrase(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull xQueryPhraseCallback callback);
  public native int xRowCount(@NotNull Fts5Context fcx,
                              @NotNull OutputPointer.Int64 nRow);
  public native long xRowid(@NotNull Fts5Context cx);
  /* Note that this impl lacks the xDelete() callback
     argument. Instead, if pAux has an xDestroy() method, it is called
     if the FTS5 API finalizes the aux state (including if allocation
     of storage for the auxdata fails). Any reference to pAux held by
     the JNI layer will be relinquished regardless of whther pAux has
     an xDestroy() method. */
  public native int xSetAuxdata(@NotNull Fts5Context cx, @Nullable Object pAux);
  public native int xTokenize(@NotNull Fts5Context cx, @NotNull byte pText[],
                              @NotNull Fts5.xTokenizeCallback callback);

  public native Object xUserData(Fts5Context cx);
  //^^^ returns the pointer passed as the 3rd arg to the C-level
  // fts5_api::xCreateFunction.
}
