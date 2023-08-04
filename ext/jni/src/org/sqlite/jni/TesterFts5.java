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
