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
(function(){
    self.Module.onRuntimeInitialized = function(){
        console.log("Loading sqlite3-api.js...");
        self.Module.loadSqliteAPI(function(S){
            console.log("Loaded module:",S.sqlite3_libversion(),
                        S.sqlite3_sourceid());
        });
    };
})(self/*window or worker*/);
