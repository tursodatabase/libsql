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
    const S = namespace.sqlite3.api;
    const oo = namespace.sqlite3.SQLite3;
    const T = self.SqliteTester;
    console.log("Loaded module:",S.sqlite3_libversion(),
                S.sqlite3_sourceid());
    const db = new oo.DB();
    const log = console.log.bind(console);
    T.assert(db._pDb);
    log("DB:",db.filename);
    log("Build options:",oo.compileOptionUsed());
    let st = db.prepare("select 3 as a");
    log("statement =",st);
    T.assert(st._pStmt)
        .assert(!st._mayGet)
        .assert('a' === st.getColumnName(0))
        .assert(st === db._statements[st._pStmt])
        .assert(1===st.columnCount)
        .assert(0===st.parameterCount)
        .mustThrow(()=>st.bind(1,null))
        .assert(true===st.step())
        .assert(3 === st.get(0))
        .mustThrow(()=>st.get(1))
        .mustThrow(()=>st.get(0,~S.SQLITE_INTEGER))
        .assert(3 === st.get(0,S.SQLITE_INTEGER))
        .assert(3 === st.getInt(0))
        .assert('3' === st.get(0,S.SQLITE_TEXT))
        .assert('3' === st.getString(0))
        .assert(3.0 === st.get(0,S.SQLITE_FLOAT))
        .assert(3.0 === st.getFloat(0))
        .assert(st.get(0,S.SQLITE_BLOB) instanceof Uint8Array)
        .assert(st.getBlob(0) instanceof Uint8Array)
        .assert(3 === st.get([])[0])
        .assert(3 === st.get({}).a)
        .assert(3 === st.getJSON(0))
        .assert(st._mayGet)
        .assert(false===st.step())
        .assert(!st._mayGet)
    ;
    let pId = st._pStmt;
    st.finalize();
    T.assert(!st._pStmt)
        .assert(!db._statements[pId]);
    log("Test count:",T.counter);
};

self/*window or worker*/.Module.onRuntimeInitialized = function(){
    console.log("Loading sqlite3-api.js...");
    self.Module.loadSqliteAPI(mainTest1);
};
