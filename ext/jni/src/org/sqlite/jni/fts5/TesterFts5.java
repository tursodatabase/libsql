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

import java.util.*;

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

  /* 
  ** Argument sql is a string containing one or more SQL statements
  ** separated by ";" characters. This function executes each of these
  ** statements against the database passed as the first argument. If
  ** no error occurs, the results of the SQL script are returned as
  ** an array of strings. If an error does occur, a RuntimeException is 
  ** thrown.
  */
  private static String[] sqlite3_exec(sqlite3 db, String sql) {
    List<String> aOut = new ArrayList<String>();

    /* Iterate through the list of SQL statements. For each, step through
    ** it and add any results to the aOut[] array.  */
    int rc = sqlite3_prepare_multi(db, sql, new PrepareMultiCallback() {
      @Override public int call(sqlite3_stmt pStmt){
        while( SQLITE_ROW==sqlite3_step(pStmt) ){
          int ii;
          for(ii=0; ii<sqlite3_column_count(pStmt); ii++){
            aOut.add( sqlite3_column_text16(pStmt, ii) );
          }
        }
        return sqlite3_finalize(pStmt);
      }
    });
    if( rc!=SQLITE_OK ){
      throw new RuntimeException(sqlite3_errmsg16(db));
    }

    /* Convert to array and return */
    String[] arr = new String[aOut.size()];
    return aOut.toArray(arr);
  }

  /*
  ** Execute the SQL script passed as the second parameter via 
  ** sqlite3_exec(). Then affirm() that the results, when converted to
  ** a string, match the value of the 3rd parameter. Example:
  **
  **   do_execsql_test(db, "SELECT 'abc'", "[abc]");
  **
  */
  private static void do_execsql_test(sqlite3 db, String sql, String expect) {
    String res = Arrays.toString( sqlite3_exec(db, sql) );
    affirm( res.equals(expect),
      "got {" + res + "} expected {" + expect + "}"
    );
  }
  private static void do_execsql_test(sqlite3 db, String sql){
    do_execsql_test(db, sql, "[]");
  }

  /* Test of the Fts5ExtensionApi.xRowid() API. */
  private static void test_rowid(){

    /* Open db and populate an fts5 table */
    sqlite3 db = createNewDb();
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(a, b);" +
      "INSERT INTO ft(rowid, a, b) VALUES(1, 'x y z', 'x y z');" +
      "INSERT INTO ft(rowid, a, b) VALUES(2, 'x y z', 'x y z');" +
      "INSERT INTO ft(rowid, a, b) VALUES(-9223372036854775808, 'x', 'x');" +
      "INSERT INTO ft(rowid, a, b) VALUES(0, 'x', 'x');" +
      "INSERT INTO ft(rowid, a, b) VALUES(9223372036854775807, 'x', 'x');" +
      "INSERT INTO ft(rowid, a, b) VALUES(3, 'x y z', 'x y z');"
    );

    /* Create a user-defined-function fts5_rowid() that uses xRowid() */
    fts5_extension_function fts5_rowid = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        long rowid = ext.xRowid(fCx);
        sqlite3_result_int64(pCx, rowid);
      }
      public void xDestroy(){ }
    };
    fts5_api.getInstanceForDb(db).xCreateFunction("fts5_rowid", fts5_rowid);

    /* Test that fts5_rowid() seems to work */
    do_execsql_test(db, 
      "SELECT rowid==fts5_rowid(ft) FROM ft('x')",
      "[1, 1, 1, 1, 1, 1]"
    );

    sqlite3_close_v2(db);
  }

  private static synchronized void runTests(){
    test1();
    test_rowid();
  }

  public TesterFts5(){
    runTests();
  }
}
