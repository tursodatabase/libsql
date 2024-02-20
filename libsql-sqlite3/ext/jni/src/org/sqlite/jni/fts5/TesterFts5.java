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
import static org.sqlite.jni.capi.CApi.*;
import static org.sqlite.jni.capi.Tester1.*;
import org.sqlite.jni.capi.*;
import org.sqlite.jni.fts5.*;

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
      throw new RuntimeException(sqlite3_errmsg(db));
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

  /*
  ** Create the following custom SQL functions:
  **
  **     fts5_rowid()
  **     fts5_columncount()
  */
  private static void create_test_functions(sqlite3 db){
    /* 
    ** A user-defined-function fts5_rowid() that uses xRowid()
    */
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

    /* 
    ** fts5_columncount() - xColumnCount() 
    */
    fts5_extension_function fts5_columncount = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        int nCol = ext.xColumnCount(fCx);
        sqlite3_result_int(pCx, nCol);
      }
      public void xDestroy(){ }
    };

    /* 
    ** fts5_columnsize() - xColumnSize() 
    */
    fts5_extension_function fts5_columnsize = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException("fts5_columncount: wrong number of args");
        }
        int iCol = sqlite3_value_int(argv[0]);

        OutputPointer.Int32 piSz = new OutputPointer.Int32();
        int rc = ext.xColumnSize(fCx, iCol, piSz);
        if( rc!=SQLITE_OK ){
          throw new RuntimeException( sqlite3_errstr(rc) );
        }
        sqlite3_result_int(pCx, piSz.get());
      }
      public void xDestroy(){ }
    };

    /* 
    ** fts5_columntext() - xColumnText() 
    */
    fts5_extension_function fts5_columntext = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException("fts5_columntext: wrong number of args");
        }
        int iCol = sqlite3_value_int(argv[0]);

        OutputPointer.String pzText = new OutputPointer.String();
        int rc = ext.xColumnText(fCx, iCol, pzText);
        if( rc!=SQLITE_OK ){
          throw new RuntimeException( sqlite3_errstr(rc) );
        }
        sqlite3_result_text16(pCx, pzText.get());
      }
      public void xDestroy(){ }
    };

    /* 
    ** fts5_columntotalsize() - xColumnTotalSize() 
    */
    fts5_extension_function fts5_columntsize = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException(
              "fts5_columntotalsize: wrong number of args"
          );
        }
        int iCol = sqlite3_value_int(argv[0]);

        OutputPointer.Int64 piSz = new OutputPointer.Int64();
        int rc = ext.xColumnTotalSize(fCx, iCol, piSz);
        if( rc!=SQLITE_OK ){
          throw new RuntimeException( sqlite3_errstr(rc) );
        }
        sqlite3_result_int64(pCx, piSz.get());
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_aux(<fts>, <value>);
    */
    class fts5_aux implements fts5_extension_function {
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length>1 ){
          throw new RuntimeException("fts5_aux: wrong number of args");
        }

        boolean bClear = (argv.length==1);
        Object obj = ext.xGetAuxdata(fCx, bClear);
        if( obj instanceof String ){
          sqlite3_result_text16(pCx, (String)obj);
        }

        if( argv.length==1 ){
          String val = sqlite3_value_text16(argv[0]);
          if( !val.equals("") ){
            ext.xSetAuxdata(fCx, val);
          }
        }
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_inst(<fts>);
    **
    ** This is used to test the xInstCount() and xInst() APIs. It returns a
    ** text value containing a Tcl list with xInstCount() elements. Each
    ** element is itself a list of 3 integers - the phrase number, column
    ** number and token offset returned by each call to xInst().
    */
    fts5_extension_function fts5_inst = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=0 ){
          throw new RuntimeException("fts5_inst: wrong number of args");
        }

        OutputPointer.Int32 pnInst = new OutputPointer.Int32();
        OutputPointer.Int32 piPhrase = new OutputPointer.Int32();
        OutputPointer.Int32 piCol = new OutputPointer.Int32();
        OutputPointer.Int32 piOff = new OutputPointer.Int32();
        String ret = new String();

        int rc = ext.xInstCount(fCx, pnInst);
        int nInst = pnInst.get();
        int ii;

        for(ii=0; rc==SQLITE_OK && ii<nInst; ii++){
          ext.xInst(fCx, ii, piPhrase, piCol, piOff);
          if( ii>0 ) ret += " ";
          ret += "{"+piPhrase.get()+" "+piCol.get()+" "+piOff.get()+"}";
        }

        sqlite3_result_text(pCx, ret);
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_pinst(<fts>);
    **
    ** Like SQL function fts5_inst(), except using the following
    **
    **     xPhraseCount
    **     xPhraseFirst
    **     xPhraseNext
    */
    fts5_extension_function fts5_pinst = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=0 ){
          throw new RuntimeException("fts5_pinst: wrong number of args");
        }

        OutputPointer.Int32 piCol = new OutputPointer.Int32();
        OutputPointer.Int32 piOff = new OutputPointer.Int32();
        String ret = new String();
        int rc = SQLITE_OK;

        int nPhrase = ext.xPhraseCount(fCx);
        int ii;

        for(ii=0; rc==SQLITE_OK && ii<nPhrase; ii++){
          Fts5PhraseIter pIter = new Fts5PhraseIter();
          for(rc = ext.xPhraseFirst(fCx, ii, pIter, piCol, piOff);
              rc==SQLITE_OK && piCol.get()>=0;
              ext.xPhraseNext(fCx, pIter, piCol, piOff)
          ){
            if( !ret.equals("") ) ret += " ";
            ret += "{"+ii+" "+piCol.get()+" "+piOff.get()+"}";
          }
        }

        if( rc!=SQLITE_OK ){
          throw new RuntimeException("fts5_pinst: rc=" + rc);
        }else{
          sqlite3_result_text(pCx, ret);
        }
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_pcolinst(<fts>);
    **
    ** Like SQL function fts5_pinst(), except using the following
    **
    **     xPhraseFirstColumn
    **     xPhraseNextColumn
    */
    fts5_extension_function fts5_pcolinst = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=0 ){
          throw new RuntimeException("fts5_pcolinst: wrong number of args");
        }

        OutputPointer.Int32 piCol = new OutputPointer.Int32();
        String ret = new String();
        int rc = SQLITE_OK;

        int nPhrase = ext.xPhraseCount(fCx);
        int ii;

        for(ii=0; rc==SQLITE_OK && ii<nPhrase; ii++){
          Fts5PhraseIter pIter = new Fts5PhraseIter();
          for(rc = ext.xPhraseFirstColumn(fCx, ii, pIter, piCol);
              rc==SQLITE_OK && piCol.get()>=0;
              ext.xPhraseNextColumn(fCx, pIter, piCol)
          ){
            if( !ret.equals("") ) ret += " ";
            ret += "{"+ii+" "+piCol.get()+"}";
          }
        }

        if( rc!=SQLITE_OK ){
          throw new RuntimeException("fts5_pcolinst: rc=" + rc);
        }else{
          sqlite3_result_text(pCx, ret);
        }
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_rowcount(<fts>);
    */
    fts5_extension_function fts5_rowcount = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=0 ){
          throw new RuntimeException("fts5_rowcount: wrong number of args");
        }
        OutputPointer.Int64 pnRow = new OutputPointer.Int64();

        int rc = ext.xRowCount(fCx, pnRow);
        if( rc==SQLITE_OK ){
          sqlite3_result_int64(pCx, pnRow.get());
        }else{
          throw new RuntimeException("fts5_rowcount: rc=" + rc);
        }
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_phrasesize(<fts>);
    */
    fts5_extension_function fts5_phrasesize = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException("fts5_phrasesize: wrong number of args");
        }
        int iPhrase = sqlite3_value_int(argv[0]);

        int sz = ext.xPhraseSize(fCx, iPhrase);
        sqlite3_result_int(pCx, sz);
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_phrasehits(<fts>, <phrase-number>);
    **
    ** Use the xQueryPhrase() API to determine how many hits, in total,
    ** there are for phrase <phrase-number> in the database.
    */
    fts5_extension_function fts5_phrasehits = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException("fts5_phrasesize: wrong number of args");
        }
        int iPhrase = sqlite3_value_int(argv[0]);
        int rc = SQLITE_OK;

        class MyCallback implements Fts5ExtensionApi.XQueryPhraseCallback {
          public int nRet = 0;
          public int getRet() { return nRet; }

          @Override
          public int call(Fts5ExtensionApi fapi, Fts5Context cx){
            OutputPointer.Int32 pnInst = new OutputPointer.Int32();
            int rc = fapi.xInstCount(cx, pnInst);
            nRet += pnInst.get();
            return rc;
          }
        };

        MyCallback xCall = new MyCallback();
        rc = ext.xQueryPhrase(fCx, iPhrase, xCall);
        if( rc!=SQLITE_OK ){
          throw new RuntimeException("fts5_phrasehits: rc=" + rc);
        }
        sqlite3_result_int(pCx, xCall.getRet());
      }
      public void xDestroy(){ }
    };

    /*
    ** fts5_tokenize(<fts>, <text>)
    */
    fts5_extension_function fts5_tokenize = new fts5_extension_function(){
      @Override public void call(
          Fts5ExtensionApi ext, 
          Fts5Context fCx,
          sqlite3_context pCx, 
          sqlite3_value argv[]
      ){
        if( argv.length!=1 ){
          throw new RuntimeException("fts5_tokenize: wrong number of args");
        }
        byte[] utf8 = sqlite3_value_text(argv[0]);
        int rc = SQLITE_OK;

        class MyCallback implements XTokenizeCallback {
          private List<String> myList = new ArrayList<String>();

          public String getval() {
            return String.join("+", myList);
          }

          @Override
          public int call(int tFlags, byte[] txt, int iStart, int iEnd){
            try {
              String str = new String(txt, "UTF-8");
              myList.add(str);
            } catch (Exception e) {
            }
            return SQLITE_OK;
          }
        };

        MyCallback xCall = new MyCallback();
        ext.xTokenize(fCx, utf8, xCall);
        sqlite3_result_text16(pCx, xCall.getval());

        if( rc!=SQLITE_OK ){
          throw new RuntimeException("fts5_tokenize: rc=" + rc);
        }
      }
      public void xDestroy(){ }
    };

    fts5_api api = fts5_api.getInstanceForDb(db);
    api.xCreateFunction("fts5_rowid", fts5_rowid);
    api.xCreateFunction("fts5_columncount", fts5_columncount);
    api.xCreateFunction("fts5_columnsize", fts5_columnsize);
    api.xCreateFunction("fts5_columntext", fts5_columntext);
    api.xCreateFunction("fts5_columntotalsize", fts5_columntsize);

    api.xCreateFunction("fts5_aux1", new fts5_aux());
    api.xCreateFunction("fts5_aux2", new fts5_aux());

    api.xCreateFunction("fts5_inst", fts5_inst);
    api.xCreateFunction("fts5_pinst", fts5_pinst);
    api.xCreateFunction("fts5_pcolinst", fts5_pcolinst);
    api.xCreateFunction("fts5_rowcount", fts5_rowcount);
    api.xCreateFunction("fts5_phrasesize", fts5_phrasesize);
    api.xCreateFunction("fts5_phrasehits", fts5_phrasehits);
    api.xCreateFunction("fts5_tokenize", fts5_tokenize);
  }
  /* 
  ** Test of various Fts5ExtensionApi methods 
  */
  private static void test2(){

    /* Open db and populate an fts5 table */
    sqlite3 db = createNewDb();
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(a, b);" +
      "INSERT INTO ft(rowid, a, b) VALUES(-9223372036854775808, 'x', 'x');" +
      "INSERT INTO ft(rowid, a, b) VALUES(0, 'x', 'x');" +
      "INSERT INTO ft(rowid, a, b) VALUES(1, 'x y z', 'x y z');" +
      "INSERT INTO ft(rowid, a, b) VALUES(2, 'x y z', 'x z');" +
      "INSERT INTO ft(rowid, a, b) VALUES(3, 'x y z', 'x y z');" +
      "INSERT INTO ft(rowid, a, b) VALUES(9223372036854775807, 'x', 'x');"
    );

    create_test_functions(db);

    /* Test that fts5_rowid() seems to work */
    do_execsql_test(db, 
      "SELECT rowid==fts5_rowid(ft) FROM ft('x')",
      "[1, 1, 1, 1, 1, 1]"
    );

    /* Test fts5_columncount() */
    do_execsql_test(db, 
      "SELECT fts5_columncount(ft) FROM ft('x')",
      "[2, 2, 2, 2, 2, 2]"
    );

    /* Test fts5_columnsize() */
    do_execsql_test(db, 
      "SELECT fts5_columnsize(ft, 0) FROM ft('x') ORDER BY rowid",
      "[1, 1, 3, 3, 3, 1]"
    );
    do_execsql_test(db, 
      "SELECT fts5_columnsize(ft, 1) FROM ft('x') ORDER BY rowid",
      "[1, 1, 3, 2, 3, 1]"
    );
    do_execsql_test(db, 
      "SELECT fts5_columnsize(ft, -1) FROM ft('x') ORDER BY rowid",
      "[2, 2, 6, 5, 6, 2]"
    );

    /* Test that xColumnSize() returns SQLITE_RANGE if the column number
    ** is out-of range */
    try {
      do_execsql_test(db, 
        "SELECT fts5_columnsize(ft, 2) FROM ft('x') ORDER BY rowid"
      );
    } catch( RuntimeException e ){
      affirm( e.getMessage().matches(".*column index out of range") );
    }

    /* Test fts5_columntext() */
    do_execsql_test(db, 
      "SELECT fts5_columntext(ft, 0) FROM ft('x') ORDER BY rowid",
      "[x, x, x y z, x y z, x y z, x]"
    );
    do_execsql_test(db, 
      "SELECT fts5_columntext(ft, 1) FROM ft('x') ORDER BY rowid",
      "[x, x, x y z, x z, x y z, x]"
    );
    boolean threw = false;
    try{
      /* columntext() used to return NULLs when given an out-of bounds column
         but now results in a range error. */
      do_execsql_test(db, 
        "SELECT fts5_columntext(ft, 2) FROM ft('x') ORDER BY rowid",
        "[null, null, null, null, null, null]"
      );
    }catch(Exception e){
      threw = true;
      affirm( e.getMessage().matches(".*column index out of range") );
    }
    affirm( threw );
    threw = false;

    /* Test fts5_columntotalsize() */
    do_execsql_test(db, 
      "SELECT fts5_columntotalsize(ft, 0) FROM ft('x') ORDER BY rowid",
      "[12, 12, 12, 12, 12, 12]"
    );
    do_execsql_test(db, 
      "SELECT fts5_columntotalsize(ft, 1) FROM ft('x') ORDER BY rowid",
      "[11, 11, 11, 11, 11, 11]"
    );
    do_execsql_test(db, 
      "SELECT fts5_columntotalsize(ft, -1) FROM ft('x') ORDER BY rowid",
      "[23, 23, 23, 23, 23, 23]"
    );

    /* Test that xColumnTotalSize() returns SQLITE_RANGE if the column 
    ** number is out-of range */
    try {
      do_execsql_test(db, 
        "SELECT fts5_columntotalsize(ft, 2) FROM ft('x') ORDER BY rowid"
      );
    } catch( RuntimeException e ){
      affirm( e.getMessage().matches(".*column index out of range") );
    }

    do_execsql_test(db, 
      "SELECT rowid, fts5_rowcount(ft) FROM ft('z')",
      "[1, 6, 2, 6, 3, 6]"
    );

    sqlite3_close_v2(db);
  }

  /* 
  ** Test of various Fts5ExtensionApi methods 
  */
  private static void test3(){

    /* Open db and populate an fts5 table */
    sqlite3 db = createNewDb();
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(a, b);" +
      "INSERT INTO ft(a, b) VALUES('the one', 1);" +
      "INSERT INTO ft(a, b) VALUES('the two', 2);" +
      "INSERT INTO ft(a, b) VALUES('the three', 3);" +
      "INSERT INTO ft(a, b) VALUES('the four', '');"
    );
    create_test_functions(db);

    /* Test fts5_aux1() + fts5_aux2() - users of xGetAuxdata and xSetAuxdata */
    do_execsql_test(db,
      "SELECT fts5_aux1(ft, a) FROM ft('the')",
      "[null, the one, the two, the three]"
    );
    do_execsql_test(db,
      "SELECT fts5_aux2(ft, b) FROM ft('the')",
      "[null, 1, 2, 3]"
    );
    do_execsql_test(db,
      "SELECT fts5_aux1(ft, a), fts5_aux2(ft, b) FROM ft('the')",
      "[null, null, the one, 1, the two, 2, the three, 3]"
    );
    do_execsql_test(db,
      "SELECT fts5_aux1(ft, b), fts5_aux1(ft) FROM ft('the')",
      "[null, 1, 1, 2, 2, 3, 3, null]"
    );
  }

  /* 
  ** Test of various Fts5ExtensionApi methods 
  */
  private static void test4(){

    /* Open db and populate an fts5 table */
    sqlite3 db = createNewDb();
    create_test_functions(db);
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(a, b);" +
      "INSERT INTO ft(a, b) VALUES('one two three', 'two three four');" +
      "INSERT INTO ft(a, b) VALUES('two three four', 'three four five');" +
      "INSERT INTO ft(a, b) VALUES('three four five', 'four five six');" 
    );


    do_execsql_test(db,
      "SELECT fts5_inst(ft) FROM ft('two')",
      "[{0 0 1} {0 1 0}, {0 0 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_inst(ft) FROM ft('four')",
      "[{0 1 2}, {0 0 2} {0 1 1}, {0 0 1} {0 1 0}]"
    );

    do_execsql_test(db,
      "SELECT fts5_inst(ft) FROM ft('a OR b OR four')",
      "[{2 1 2}, {2 0 2} {2 1 1}, {2 0 1} {2 1 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_inst(ft) FROM ft('two four')",
      "[{0 0 1} {0 1 0} {1 1 2}, {0 0 0} {1 0 2} {1 1 1}]"
    );

    do_execsql_test(db,
      "SELECT fts5_pinst(ft) FROM ft('two')",
      "[{0 0 1} {0 1 0}, {0 0 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pinst(ft) FROM ft('four')",
      "[{0 1 2}, {0 0 2} {0 1 1}, {0 0 1} {0 1 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pinst(ft) FROM ft('a OR b OR four')",
      "[{2 1 2}, {2 0 2} {2 1 1}, {2 0 1} {2 1 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pinst(ft) FROM ft('two four')",
      "[{0 0 1} {0 1 0} {1 1 2}, {0 0 0} {1 0 2} {1 1 1}]"
    );

    do_execsql_test(db,
      "SELECT fts5_pcolinst(ft) FROM ft('two')",
      "[{0 0} {0 1}, {0 0}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pcolinst(ft) FROM ft('four')",
      "[{0 1}, {0 0} {0 1}, {0 0} {0 1}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pcolinst(ft) FROM ft('a OR b OR four')",
      "[{2 1}, {2 0} {2 1}, {2 0} {2 1}]"
    );
    do_execsql_test(db,
      "SELECT fts5_pcolinst(ft) FROM ft('two four')",
      "[{0 0} {0 1} {1 1}, {0 0} {1 0} {1 1}]"
    );

    do_execsql_test(db,
      "SELECT fts5_phrasesize(ft, 0) FROM ft('four five six') LIMIT 1;",
      "[1]"
    );
    do_execsql_test(db,
      "SELECT fts5_phrasesize(ft, 0) FROM ft('four + five + six') LIMIT 1;",
      "[3]"
    );


    sqlite3_close_v2(db);
  }

  private static void test5(){
    /* Open db and populate an fts5 table */
    sqlite3 db = createNewDb();
    create_test_functions(db);
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(x, b);" +
      "INSERT INTO ft(x) VALUES('one two three four five six seven eight');" +
      "INSERT INTO ft(x) VALUES('one two one four one six one eight');" +
      "INSERT INTO ft(x) VALUES('one two three four five six seven eight');"
    );

    do_execsql_test(db,
      "SELECT fts5_phrasehits(ft, 0) FROM ft('one') LIMIT 1",
      "[6]"
    );

    sqlite3_close_v2(db);
  }

  private static void test6(){
    sqlite3 db = createNewDb();
    create_test_functions(db);
    do_execsql_test(db, 
      "CREATE VIRTUAL TABLE ft USING fts5(x, b);" +
      "INSERT INTO ft(x) VALUES('one two three four five six seven eight');" 
    );

    do_execsql_test(db,
      "SELECT fts5_tokenize(ft, 'abc def ghi') FROM ft('one')",
      "[abc+def+ghi]"
    );
    do_execsql_test(db,
      "SELECT fts5_tokenize(ft, 'it''s BEEN a...') FROM ft('one')",
      "[it+s+been+a]"
    );

    sqlite3_close_v2(db);
  }

  private static synchronized void runTests(){
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
  }

  public TesterFts5(){
    runTests();
  }
}
