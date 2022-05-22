/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js.
*/

const mainTest1 = function(namespace){
    const S = namespace.sqlite3;
    const oo = namespace.SQLite3;
    const T = self.SqliteTester;
    console.log("Loaded module:",S.sqlite3_libversion(),
                S.sqlite3_sourceid());
    const db = new oo.DB();
    const log = console.log.bind(console);
    T.assert(db.pDb);
    log("DB:",db.filename);
    log("Build options:",oo.compileOptionUsed());

    let st = db.prepare("select 1");
    T.assert(st.pStmt);
    log("statement =",st);
    T.assert(st === db._statements[st.pStmt])
        .assert(1===st.columnCount)
        .assert(0===st.parameterCount)
        .mustThrow(()=>st.bind(1,null));

    let pId = st.pStmt;
    st.finalize();
    T.assert(!st.pStmt)
        .assert(!db._statements[pId]);
    log("Test count:",T.counter);
};

self/*window or worker*/.Module.onRuntimeInitialized = function(){
    console.log("Loading sqlite3-api.js...");
    self.Module.loadSqliteAPI(mainTest1);
};
