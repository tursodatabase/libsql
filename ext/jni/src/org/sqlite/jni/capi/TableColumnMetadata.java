/*
** 2023-07-21
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
package org.sqlite.jni.capi;

/**
   A wrapper object for use with sqlite3_table_column_metadata().
   They are populated only via that interface.
*/
public final class TableColumnMetadata {
  OutputPointer.Bool pNotNull = new OutputPointer.Bool();
  OutputPointer.Bool pPrimaryKey = new OutputPointer.Bool();
  OutputPointer.Bool pAutoinc = new OutputPointer.Bool();
  OutputPointer.String pzCollSeq = new OutputPointer.String();
  OutputPointer.String pzDataType = new OutputPointer.String();

  public TableColumnMetadata(){
  }

  public String getDataType(){ return pzDataType.value; }
  public String getCollation(){ return pzCollSeq.value; }
  public boolean isNotNull(){ return pNotNull.value; }
  public boolean isPrimaryKey(){ return pPrimaryKey.value; }
  public boolean isAutoincrement(){ return pAutoinc.value; }
}
