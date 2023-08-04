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
** This file contains a set of tests for the sqlite3 JNI bindings.
*/
package org.sqlite.jni;
import static org.sqlite.jni.SQLite3Jni.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TesterFts5 {

  private static <T> void out(T val){
    System.out.print(val);
  }

  private static <T> void outln(T val){
    System.out.println(val);
  }

  private static int affirmCount = 0;
  private static void affirm(Boolean v){
    ++affirmCount;
    if( !v ) throw new RuntimeException("Assertion failed.");
  }

  private static void execSql(sqlite3 db, String[] sql){
    execSql(db, String.join("", sql));
  }
  private static int execSql(sqlite3 db, boolean throwOnError, String sql){
      OutputPointer.Int32 oTail = new OutputPointer.Int32();
      final byte[] sqlUtf8 = sql.getBytes(StandardCharsets.UTF_8);
      int pos = 0, n = 1;
      byte[] sqlChunk = sqlUtf8;
      sqlite3_stmt stmt = new sqlite3_stmt();
      int rc = 0;
      while(pos < sqlChunk.length){
        if(pos > 0){
          sqlChunk = Arrays.copyOfRange(sqlChunk, pos,
                                        sqlChunk.length);
        }
        if( 0==sqlChunk.length ) break;
        rc = sqlite3_prepare_v2(db, sqlChunk, stmt, oTail);
        affirm(0 == rc);
        pos = oTail.getValue();
        affirm(0 != stmt.getNativePointer());
        rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        affirm(0 == stmt.getNativePointer());
        if(0!=rc && SQLITE_ROW!=rc && SQLITE_DONE!=rc){
          if(throwOnError){
            throw new RuntimeException("db op failed with rc="+rc);
          }else{
            break;
          }
        }
      }
      if(SQLITE_ROW==rc || SQLITE_DONE==rc) rc = 0;
      return rc;
  }
  private static void execSql(sqlite3 db, String sql){
    execSql(db, true, sql);
  }


  private static sqlite3 createNewDb(){
    sqlite3 db = new sqlite3();
    affirm(0 == db.getNativePointer());
    int rc = sqlite3_open(":memory:", db);
    affirm(0 == rc);
    affirm(0 != db.getNativePointer());
    rc = sqlite3_busy_timeout(db, 2000);
    affirm( 0 == rc );
    return db;
  }

  private static void test1(){
    Fts5ExtensionApi fea = Fts5ExtensionApi.getInstance();
    affirm( null != fea );
    affirm( fea.getNativePointer() != 0 );
    affirm( fea == Fts5ExtensionApi.getInstance() )/*singleton*/;
  }

  public TesterFts5(){
    final long timeStart = System.nanoTime();
    test1();
    final long timeEnd = System.nanoTime();
    outln("FTS5 Tests done. Metrics:");
    outln("\tAssertions checked: "+affirmCount);
    outln("\tTotal time = "
          +((timeEnd - timeStart)/1000000.0)+"ms");
  }
}
