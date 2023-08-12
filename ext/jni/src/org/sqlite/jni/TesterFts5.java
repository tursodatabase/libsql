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
import static org.sqlite.jni.Tester1.*;

public class TesterFts5 {

  private static void test1(){
    Fts5ExtensionApi fea = Fts5ExtensionApi.getInstance();
    affirm( null != fea );
    affirm( fea.getNativePointer() != 0 );
    affirm( fea == Fts5ExtensionApi.getInstance() )/*singleton*/;

    sqlite3 db = createNewDb();
    fts5_api fApi = fts5_api.getInstanceForDb(db);
    affirm( fApi != null );
    affirm( fApi == fts5_api.getInstanceForDb(db) /* singleton per db */ );

    execSql(db, new String[] {
        "CREATE VIRTUAL TABLE ft USING fts5(a, b);",
        "INSERT INTO ft(rowid, a, b) VALUES(1, 'X Y', 'Y Z');",
        "INSERT INTO ft(rowid, a, b) VALUES(2, 'A Z', 'Y Y');"
      });

    final String pUserData = "This is pUserData";
    ValueHolder<Boolean> xDestroyCalled = new ValueHolder<>(false);
    ValueHolder<Integer> xFuncCount = new ValueHolder<>(0);
    final fts5_extension_function func = new fts5_extension_function(){
        public void xFunction(Fts5ExtensionApi ext, Fts5Context fCx,
                              sqlite3_context pCx, sqlite3_value argv[]){
          int nCols = ext.xColumnCount(fCx);
          affirm( 2 == nCols );
          affirm( nCols == argv.length );
          affirm( ext.xUserData(fCx) == pUserData );
          if(true){
            OutputPointer.String op = new OutputPointer.String();
            for(int i = 0; i < nCols; ++i ){
              int rc = ext.xColumnText(fCx, i, op);
              affirm( 0 == rc );
              final String val = op.value;
              affirm( val.equals(sqlite3_value_text(argv[i])) );
              //outln("xFunction col "+i+": "+val);
            }
          }
          ++xFuncCount.value;
        }
        public void xDestroy(){
          xDestroyCalled.value = true;
        }
      };

    int rc = fApi.xCreateFunction("myaux", pUserData, func);
    affirm( 0==rc );

    affirm( 0==xFuncCount.value );
    execSql(db, "select myaux(ft,a,b) from ft;");
    affirm( 2==xFuncCount.value );
    affirm( !xDestroyCalled.value );
    sqlite3_close_v2(db);
    affirm( xDestroyCalled.value );
  }

  public TesterFts5(){
    int oldAffirmCount = Tester1.affirmCount;
    Tester1.affirmCount = 0;
    final long timeStart = System.nanoTime();
    test1();
    final long timeEnd = System.nanoTime();
    outln("FTS5 Tests done. Metrics:");
    outln("\tAssertions checked: "+Tester1.affirmCount);
    outln("\tTotal time = "
          +((timeEnd - timeStart)/1000000.0)+"ms");
    Tester1.affirmCount = oldAffirmCount;
  }
}
