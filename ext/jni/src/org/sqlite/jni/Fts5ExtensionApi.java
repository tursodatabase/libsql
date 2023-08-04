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
   FAR FROM COMPLETE and the feasibility of binding this to Java
   is still undetermined. This might be removed.

   Reminder to self: the native Fts5ExtensionApi is a singleton.
*/
public final class Fts5ExtensionApi extends NativePointerHolder<Fts5ExtensionApi> {
  //! Only called from JNI
  private Fts5ExtensionApi(){}
  private int iVersion;

  public static native Fts5ExtensionApi getInstance();

  public native int xColumnCount(@NotNull Fts5Context fcx);
  public native int xColumnSize(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.Int32 pnToken);
  public native int xColumnText(@NotNull Fts5Context cx, int iCol,
                                @NotNull OutputPointer.ByteArray txt);
  public int xColumnText(@NotNull Fts5Context cx, int iCol,
                         @NotNull OutputPointer.String txt){
    final OutputPointer.ByteArray out = new OutputPointer.ByteArray();
    int rc = xColumnText(cx, iCol, out);
    if( 0 == rc ){
      txt.setValue( new String(out.getValue(), StandardCharsets.UTF_8) );
    }
    return rc;
  }
  public native int xColumnTotalSize(@NotNull Fts5Context fcx, int iCol,
                                     @NotNull OutputPointer.Int64 pnToken);
  public native int xInst(@NotNull Fts5Context cx, int iIdx,
                          @NotNull OutputPointer.Int32 piPhrase,
                          @NotNull OutputPointer.Int32 piCol,
                          @NotNull OutputPointer.Int32 piOff);
  public native int xInstCount(@NotNull Fts5Context fcx,
                               @NotNull OutputPointer.Int32 pnInst);
  public native int xPhraseCount(@NotNull Fts5Context fcx);
  public native int xPhraseSize(@NotNull Fts5Context fcx, int iPhrase);
  public native int xRowCount(@NotNull Fts5Context fcx,
                              @NotNull OutputPointer.Int64 nRow);
  public native long xRowid(@NotNull Fts5Context cx);
/**************************************************************
  void *(*xUserData)(Fts5Context*);

  int (*xTokenize)(Fts5Context*,
    const char *pText, int nText,
    void *pCtx,
    int (*xToken)(void*, int, const char*, int, int, int)
  );


  int (*xQueryPhrase)(Fts5Context*, int iPhrase, void *pUserData,
    int(*)(const Fts5ExtensionApi*,Fts5Context*,void*)
  );
  int (*xSetAuxdata)(Fts5Context*, void *pAux, void(*xDelete)(void*));
  void *(*xGetAuxdata)(Fts5Context*, int bClear);

  int (*xPhraseFirst)(Fts5Context*, int iPhrase, Fts5PhraseIter*, int*, int*);
  void (*xPhraseNext)(Fts5Context*, Fts5PhraseIter*, int *piCol, int *piOff);

  int (*xPhraseFirstColumn)(Fts5Context*, int iPhrase, Fts5PhraseIter*, int*);
  void (*xPhraseNextColumn)(Fts5Context*, Fts5PhraseIter*, int *piCol);
  **************************************************************/
}
