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
package org.sqlite.jni.fts5;
import static org.sqlite.jni.SQLite3Jni.*;
import static org.sqlite.jni.Tester1.*;
import org.sqlite.jni.*;

public class TesterFts5 {

  private static void test1(){
    final Fts5ExtensionApi fea = Fts5ExtensionApi.getInstance();
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
    final int outputs[] = {0, 0};
    final fts5_extension_function func = new fts5_extension_function(){
        @Override public void call(Fts5ExtensionApi ext, Fts5Context fCx,
                                   sqlite3_context pCx, sqlite3_value argv[]){
          final int nCols = ext.xColumnCount(fCx);
          affirm( 2 == nCols );
          affirm( nCols == argv.length );
          affirm( ext.xUserData(fCx) == pUserData );
          final OutputPointer.String op = new OutputPointer.String();
          final OutputPointer.Int32 colsz = new OutputPointer.Int32();
          final OutputPointer.Int64 colTotalSz = new OutputPointer.Int64();
          for(int i = 0; i < nCols; ++i ){
            int rc = ext.xColumnText(fCx, i, op);
            affirm( 0 == rc );
            final String val = op.value;
            affirm( val.equals(sqlite3_value_text16(argv[i])) );
            rc = ext.xColumnSize(fCx, i, colsz);
            affirm( 0==rc );
            affirm( 3==sqlite3_value_bytes(argv[i]) );
            rc = ext.xColumnTotalSize(fCx, i, colTotalSz);
            affirm( 0==rc );
          }
          ++outputs[0];
        }
        public void xDestroy(){
          outputs[1] = 1;
        }
      };

    int rc = fApi.xCreateFunction("myaux", pUserData, func);
    affirm( 0==rc );

    affirm( 0==outputs[0] );
    execSql(db, "select myaux(ft,a,b) from ft;");
    affirm( 2==outputs[0] );
    affirm( 0==outputs[1] );
    sqlite3_close_v2(db);
    affirm( 1==outputs[1] );
  }

  private static synchronized void runTests(){
    test1();
  }

  public TesterFts5(){
    runTests();
  }
}
