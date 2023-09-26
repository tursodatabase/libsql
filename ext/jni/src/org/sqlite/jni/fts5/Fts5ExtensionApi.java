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
package org.sqlite.jni.fts5;
import java.nio.charset.StandardCharsets;
import org.sqlite.jni.*;
import org.sqlite.jni.annotation.*;

/**
*/
public final class Fts5ExtensionApi extends NativePointerHolder<Fts5ExtensionApi> {
  //! Only called from JNI
  private Fts5ExtensionApi(){}
  private final int iVersion = 2;

  /* Callback type for used by xQueryPhrase(). */
  public static interface XQueryPhraseCallback {
    int call(Fts5ExtensionApi fapi, Fts5Context cx);
  }

  /**
     Returns the singleton instance of this class.
  */
  public static native Fts5ExtensionApi getInstance();

  @Canonical
  public native int xColumnCount(@NotNull Fts5Context fcx);

  @Canonical
  public native int xColumnSize(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.Int32 pnToken);

  @Canonical
  public native int xColumnText(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.String txt);

  @Canonical
  public native int xColumnTotalSize(@NotNull Fts5Context fcx, int iCol,
                                     @NotNull OutputPointer.Int64 pnToken);

  @Canonical
  public native Object xGetAuxdata(@NotNull Fts5Context cx, boolean clearIt);

  @Canonical
  public native int xInst(@NotNull Fts5Context cx, int iIdx,
                          @NotNull OutputPointer.Int32 piPhrase,
                          @NotNull OutputPointer.Int32 piCol,
                          @NotNull OutputPointer.Int32 piOff);

  @Canonical
  public native int xInstCount(@NotNull Fts5Context fcx,
                               @NotNull OutputPointer.Int32 pnInst);

  @Canonical
  public native int xPhraseCount(@NotNull Fts5Context fcx);

  @Canonical
  public native int xPhraseFirst(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);

  @Canonical
  public native int xPhraseFirstColumn(@NotNull Fts5Context cx, int iPhrase,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  @Canonical
  public native void xPhraseNext(@NotNull Fts5Context cx,
                                 @NotNull Fts5PhraseIter iter,
                                 @NotNull OutputPointer.Int32 iCol,
                                 @NotNull OutputPointer.Int32 iOff);
  @Canonical
  public native void xPhraseNextColumn(@NotNull Fts5Context cx,
                                       @NotNull Fts5PhraseIter iter,
                                       @NotNull OutputPointer.Int32 iCol);
  @Canonical
  public native int xPhraseSize(@NotNull Fts5Context fcx, int iPhrase);

  @Canonical
  public native int xQueryPhrase(@NotNull Fts5Context cx, int iPhrase,
                                 @NotNull XQueryPhraseCallback callback);
  @Canonical
  public native int xRowCount(@NotNull Fts5Context fcx,
                              @NotNull OutputPointer.Int64 nRow);

  @Canonical
  public native long xRowid(@NotNull Fts5Context cx);
  /* Note that the JNI binding lacks the C version's xDelete()
     callback argument. Instead, if pAux has an xDestroy() method, it
     is called if the FTS5 API finalizes the aux state (including if
     allocation of storage for the auxdata fails). Any reference to
     pAux held by the JNI layer will be relinquished regardless of
     whether pAux has an xDestroy() method. */

  @Canonical
  public native int xSetAuxdata(@NotNull Fts5Context cx, @Nullable Object pAux);

  @Canonical
  public native int xTokenize(@NotNull Fts5Context cx, @NotNull byte[] pText,
                              @NotNull XTokenizeCallback callback);

  @Canonical
  public native Object xUserData(Fts5Context cx);
  //^^^ returns the pointer passed as the 3rd arg to the C-level
  // fts5_api::xCreateFunction().
}
