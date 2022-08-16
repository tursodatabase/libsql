/*
  2022-08-16

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic demonstration of the SQLite3 OO API #1, shorn of assertions
  and the like to improve readability.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const toss = function(...args){throw new Error(args.join(' '))};
  const debug = console.debug.bind(console),
        log = console.log.bind(console),
        warn = console.warn.bind(console),
        error = console.error.bind(console);

  const demo1 = function(sqlite3,EmModule){
    const capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;

    // If we have persistent storage, maybe init and mount it:
    const dbDir = true
          ? "" // this demo works better without persistent storage.
          : capi.sqlite3_web_persistent_dir();
            // ^^^ returns name of persistent mount point or "" if we have none

    const db = new oo.DB(dbDir+"/mydb.sqlite3");
    /**
       Never(!) rely on garbage collection to clean up DBs and
       (especially) statements. Always wrap their lifetimes in
       try/finally construct...
    */
    try {
      log("Create a table...");
      db.exec("CREATE TABLE IF NOT EXISTS t(a,b)");
      //Equivalent:
      db.exec({
        sql:"CREATE TABLE IF NOT EXISTS t(a,b)"
        // ... numerous other options ... 
      });
      // SQL can be either a string or a byte array

      log("Insert some data using exec()...");
      let i;
      for( i = 1; i <= 5; ++i ){
        db.exec({
          sql: "insert into t(a,b) values (?,?)",
          // bind by parameter index...
          bind: [i, i*2]
        });
        db.exec({
          sql: "insert into t(a,b) values ($a,$b)",
          // bind by parameter name...
          bind: {$a: i * 3, $b: i * 4}
        });
      }    

      log("Insert using a prepared statement...");
      let q = db.prepare("insert into t(a,b) values(?,?)");
      try {
        for( i = 100; i < 103; ++i ){
          q.bind( [i, i*2] ).step();
          q.reset();
        }
        // Equivalent...
        for( i = 103; i <= 105; ++i ){
          q.bind(1, i).bind(2, i*2).stepReset();
        }
      }finally{
        q.finalize();
      }

      log("Query data with exec() using rowMode 'array'...");
      db.exec({
        sql: "select a from t order by a limit 3",
        rowMode: 'array', // 'array', 'object', or 'stmt' (default)
        callback: function(row){
          log("row ",++this.counter,"=",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode 'object'...");
      db.exec({
        sql: "select a as aa, b as bb from t order by aa limit 3",
        rowMode: 'object',
        callback: function(row){
          log("row ",++this.counter,"=",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode 'stmt'...");
      db.exec({
        sql: "select a from t order by a limit 3",
        rowMode: 'stmt', // stmt === the default
        callback: function(row){
          log("row ",++this.counter,"get(0) =",row.get(0));
        }.bind({counter: 0})
      });

      log("Query data with exec() using rowMode INTEGER (result column index)...");
      db.exec({
        sql: "select a, b from t order by a limit 3",
        rowMode: 1, // === result column 1
        callback: function(row){
          log("row ",++this.counter,"b =",row);
        }.bind({counter: 0})
      });

      log("Query data with exec() without a callback...");
      let resultRows = [];
      db.exec({
        sql: "select a, b from t order by a limit 3",
        rowMode: 'object',
        resultRows: resultRows
      });
      log("Result rows:",resultRows);

      log("Create a scalar UDF...");
      db.createFunction({
        name: 'twice',
        callback: function(arg){ // note the call arg count
          return arg + arg;
        }
      });
      log("Run scalar UDF and collect result column names...");
      let columnNames = [];
      db.exec({
        sql: "select a, twice(a), twice(''||a) from t order by a desc limit 3",
        columnNames: columnNames,
        callback: function(row){
          log("a =",row.get(0), "twice(a) =", row.get(1), "twice(''||a) =",row.get(2));
        }
      });
      log("Result column names:",columnNames);

      /**
         Main differences between exec() and execMulti():

         - execMulti() traverses all statements in the input SQL

         - exec() supports a couple options not supported by execMulti(),
           and vice versa.

         - execMulti() result callback/array only activates for the
           first statement which has result columns. It is arguable
           whether it should support a callback at all, and that
           feature may be removed.

         - execMulti() column-bind data only activates for the first statement
           with bindable columns. This feature is arguable and may be removed.
       */
      
      if(0){
        warn("UDF will throw because of incorrect arg count...");
        db.exec("select twice(1,2,3)");
      }

      try {
        db.callInTransaction( function(D) {
          D.exec("delete from t");
          log("In transaction: count(*) from t =",db.selectValue("select count(*) from t"));
          throw new sqlite3.SQLite3Error("Demonstrating callInTransaction() rollback");
        });
      }catch(e){
        log("Got expected exception:",e.message);
        log("count(*) from t =",db.selectValue("select count(*) from t"));
      }

    }finally{
      db.close();
    }

    /**
       Misc. DB features:

       - get change count (total or statement-local, 32- or 64-bit)
       - get its file name
       - selectValue() takes SQL and returns first column of first row.
    
       Misc. Stmt features:

       - Various forms of bind() 
       - clearBindings()
       - reset()
       - Various forms of step()
       - Variants of get() for explicit type treatment/conversion,
         e.g. getInt(), getFloat(), getBlob(), getJSON()
       - getColumnName(ndx), getColumnNames()
       - getParamIndex(name)
    */
  }/*demo1()*/;

  const runDemos = function(Module){
    //log("Module",Module);
    const sqlite3 = Module.sqlite3,
          capi = sqlite3.capi,
          oo = sqlite3.oo1,
          wasm = capi.wasm;
    log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    try {
      [ demo1 ].forEach((f)=>f(sqlite3, Module))
    }catch(e){
      error("Exception:",e.message);
      throw e;
    }
  };

  sqlite3InitModule(self.sqlite3TestModule).then(runDemos);
})();
